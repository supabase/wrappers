use crate::stats;
use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZlibDecoder};
use aws_config::BehaviorVersion;
use aws_sdk_s3 as s3;
use http::Uri;
use pgrx::pg_sys;
use serde_json::{self, Value as JsonValue};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::io::Cursor;
use std::pin::Pin;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

use super::parquet::*;
use supabase_wrappers::prelude::*;

use super::{S3FdwError, S3FdwResult};

// record parser for a S3 file
enum Parser {
    Csv(csv::Reader<Cursor<Vec<u8>>>),
    // JSON lines text file format: https://jsonlines.org/
    JsonLine(VecDeque<JsonValue>),
    Parquet(S3Parquet),
}

#[wrappers_fdw(
    version = "0.1.3",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/s3_fdw",
    error_type = "S3FdwError"
)]
pub(crate) struct S3Fdw {
    rt: Runtime,
    client: Option<s3::Client>,
    rdr: Option<BufReader<Pin<Box<dyn AsyncRead>>>>,
    parser: Parser,
    tgt_cols: Vec<Column>,
    rows_out: i64,

    // local string buffer for CSV and JSONL
    buf: String,
}

impl S3Fdw {
    const FDW_NAME: &'static str = "S3Fdw";

    // local string line buffer size, in bytes
    // Note: this is not a hard limit, just an indication of full buffer
    const BUF_SIZE: usize = 256 * 1024;

    // fetch remote data to local string line buffer when it is empty and set
    // up record parser.
    // Returns:
    //   Some - still have records to read
    //   None - no more records
    fn refill(&mut self) -> S3FdwResult<Option<()>> {
        if !self.buf.is_empty() {
            return Ok(Some(()));
        }

        if let Some(ref mut rdr) = self.rdr {
            // fetch remote data by lines and fill in local buffer
            let mut total_lines = 0;
            let mut total_bytes = 0;
            loop {
                let num_bytes = self.rt.block_on(rdr.read_line(&mut self.buf))?;
                total_lines += 1;
                total_bytes += num_bytes;
                if num_bytes == 0 || self.buf.len() > Self::BUF_SIZE {
                    break;
                }
            }

            stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, total_lines);
            stats::inc_stats(Self::FDW_NAME, stats::Metric::BytesIn, total_bytes as i64);
        }

        if self.buf.is_empty() {
            return Ok(None);
        }

        match &mut self.parser {
            Parser::Csv(rdr) => {
                let mut buf: Vec<u8> = Vec::new();
                buf.extend(self.buf.as_bytes());
                *rdr = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(Cursor::new(buf));
            }
            Parser::JsonLine(records) => {
                // enclose json lines into a json string and then parse it
                let s = self
                    .buf
                    .split('\n')
                    .map(|s| s.trim())
                    .collect::<Vec<&str>>()
                    .join(",");
                let json_str = format!("{{ \"rows\": [{}] }}", s.trim_end_matches(','));
                let rows = serde_json::from_str::<JsonValue>(&json_str)?;
                let rows = rows
                    .get("rows")
                    .and_then(|arr| arr.as_array())
                    .ok_or(S3FdwError::ReadJsonlError(json_str))?;
                *records = VecDeque::from(rows.to_vec());
            }
            _ => unreachable!(),
        }

        Ok(Some(()))
    }
}

impl ForeignDataWrapper<S3FdwError> for S3Fdw {
    fn new(options: &HashMap<String, String>) -> S3FdwResult<Self> {
        // cannot use create_async_runtime() as the runtime needs to be created
        // for multiple threads
        let rt = tokio::runtime::Runtime::new()
            .map_err(CreateRuntimeError::FailedToCreateAsyncRuntime)?;
        let mut ret = S3Fdw {
            rt,
            client: None,
            rdr: None,
            parser: Parser::JsonLine(VecDeque::new()),
            tgt_cols: Vec::new(),
            rows_out: 0,
            buf: String::new(),
        };

        // get is_mock flag
        let is_mock: bool = options.get("is_mock") == Some(&"true".to_string());

        // get credentials
        let creds = if is_mock {
            // LocalStack uses hardcoded credentials
            Some(("test".to_string(), "test".to_string()))
        } else {
            match options.get("vault_access_key_id") {
                Some(vault_access_key_id) => {
                    // if using credentials stored in Vault
                    let vault_secret_access_key =
                        require_option("vault_secret_access_key", options)?;
                    get_vault_secret(vault_access_key_id)
                        .zip(get_vault_secret(vault_secret_access_key))
                }
                None => {
                    // if using credentials directly specified
                    let aws_access_key_id =
                        require_option("aws_access_key_id", options)?.to_string();
                    let aws_secret_access_key =
                        require_option("aws_secret_access_key", options)?.to_string();
                    Some((aws_access_key_id, aws_secret_access_key))
                }
            }
        };

        let Some(creds) = creds else {
            return Ok(ret);
        };

        // get region
        let default_region = "us-east-1".to_string();
        let region = if is_mock {
            default_region
        } else {
            options
                .get("aws_region")
                .map(|t| t.to_owned())
                .unwrap_or(default_region)
        };

        // set AWS environment variables and create shared config from them
        env::set_var("AWS_ACCESS_KEY_ID", creds.0);
        env::set_var("AWS_SECRET_ACCESS_KEY", creds.1);
        env::set_var("AWS_REGION", region);

        let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

        // endpoint_url not supported as env var in rust https://github.com/awslabs/aws-sdk-rust/issues/932
        if let Some(endpoint_url) = options.get("endpoint_url") {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }

        let config = ret.rt.block_on(config_loader.load());

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        // create S3 client
        let client = if is_mock {
            let mut s3_config_builder = s3::config::Builder::from(&config);
            s3_config_builder = s3_config_builder
                .endpoint_url("http://localhost:4566/")
                .force_path_style(true);
            s3::Client::from_conf(s3_config_builder.build())
        } else {
            s3::Client::new(&config)
        };
        ret.client = Some(client);

        Ok(ret)
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> S3FdwResult<()> {
        // extract s3 bucket and object path from uri option
        let (bucket, object) = {
            let uri = require_option("uri", options)?.parse::<Uri>()?;
            if uri.scheme_str() != Option::Some("s3")
                || uri.host().is_none()
                || uri.path().is_empty()
            {
                return Err(S3FdwError::InvalidS3Uri(uri.to_string()));
            }
            // exclude 1st "/" char in the path as s3 object path doesn't like it
            (
                uri.host()
                    .expect("host is not None as tested in if condition above")
                    .to_owned(),
                uri.path()[1..].to_string(),
            )
        };

        let has_header: bool = options.get("has_header") == Some(&"true".to_string());

        self.tgt_cols = columns.to_vec();

        if let Some(client) = &self.client {
            // initialise parser according to format option
            let format = require_option("format", options)?;
            // create dummy parser
            match format {
                "csv" => self.parser = Parser::Csv(csv::Reader::from_reader(Cursor::new(vec![0]))),
                "jsonl" => self.parser = Parser::JsonLine(VecDeque::new()),
                "parquet" => self.parser = Parser::Parquet(S3Parquet::default()),
                _ => return Err(S3FdwError::InvalidFormatOption(format.to_string())),
            }

            let stream = self
                .rt
                .block_on(client.get_object().bucket(&bucket).key(&object).send())?
                .body
                .into_async_read();

            let mut boxed_stream: Pin<Box<dyn AsyncRead>> =
                if let Some(compress) = options.get("compress") {
                    let buf_rdr = BufReader::new(stream);
                    match compress.as_str() {
                        "bzip2" => Box::pin(BzDecoder::new(buf_rdr)),
                        "gzip" => Box::pin(GzipDecoder::new(buf_rdr)),
                        "xz" => Box::pin(XzDecoder::new(buf_rdr)),
                        "zlib" => Box::pin(ZlibDecoder::new(buf_rdr)),
                        _ => return Err(S3FdwError::InvalidCompressOption(compress.to_string())),
                    }
                } else {
                    Box::pin(stream)
                };

            // deal with parquet file, read all its content to local buffer if it is
            // compressed, otherwise open async read stream for it
            if let Parser::Parquet(ref mut s3parquet) = &mut self.parser {
                if options.get("compress").is_some() {
                    // read all contents to local
                    let mut buf = Vec::new();
                    self.rt
                        .block_on(boxed_stream.read_to_end(&mut buf))
                        .expect("read compressed parquet file failed");
                    self.rt.block_on(s3parquet.open_local_stream(buf))?;
                } else {
                    // open async read stream
                    self.rt.block_on(s3parquet.open_async_stream(
                        client,
                        &bucket,
                        &object,
                        &self.tgt_cols,
                    ))?;
                }
                return Ok(());
            }

            let mut rdr: BufReader<Pin<Box<dyn AsyncRead>>> = BufReader::new(boxed_stream);

            // skip csv header line if needed
            if let Parser::Csv(_) = self.parser {
                if has_header {
                    let mut header = String::new();
                    self.rt.block_on(rdr.read_line(&mut header))?;
                }
            }

            self.rdr = Some(rdr);
        }

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> S3FdwResult<Option<()>> {
        // read parquet record
        if let Parser::Parquet(ref mut s3parquet) = &mut self.parser {
            if self.rt.block_on(s3parquet.refill())?.is_none() {
                return Ok(None);
            }
            let ret = s3parquet.read_into_row(row, &self.tgt_cols)?;
            if ret.is_some() {
                self.rows_out += 1;
            } else {
                stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, self.rows_out);
            }
            return Ok(ret);
        }

        // read csv or jsonl record
        loop {
            if self.refill()?.is_none() {
                break;
            }

            // parse local buffer data to records
            match &mut self.parser {
                Parser::Csv(rdr) => {
                    let mut record = csv::StringRecord::new();
                    let result = rdr.read_record(&mut record)?;
                    if result {
                        for col in &self.tgt_cols {
                            let cell = record.get(col.num - 1).map(|s| Cell::String(s.to_owned()));
                            row.push(&col.name, cell);
                        }
                        self.rows_out += 1;
                        return Ok(Some(()));
                    } else {
                        // no more records left in the local buffer, refill from remote
                        self.buf.clear();
                    }
                }
                Parser::JsonLine(records) => {
                    match records.pop_front() {
                        Some(record) => {
                            if let Some(obj) = record.as_object() {
                                for col in &self.tgt_cols {
                                    let cell = obj
                                        .get(&col.name)
                                        .map(|val| match val {
                                            JsonValue::Null => None,
                                            JsonValue::Bool(v) => Some(Cell::String(v.to_string())),
                                            JsonValue::Number(v) => {
                                                Some(Cell::String(v.to_string()))
                                            }
                                            JsonValue::String(v) => {
                                                Some(Cell::String(v.to_owned()))
                                            }
                                            JsonValue::Array(v) => {
                                                Some(Cell::String(format!("{:?}", v)))
                                            }
                                            JsonValue::Object(v) => {
                                                Some(Cell::String(format!("{:?}", v)))
                                            }
                                        })
                                        .unwrap_or(None);
                                    row.push(&col.name, cell);
                                }
                            }
                            self.rows_out += 1;
                            return Ok(Some(()));
                        }
                        None => {
                            // no more records left in the local buffer, refill from remote
                            self.buf.clear();
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, self.rows_out);

        Ok(None)
    }

    fn end_scan(&mut self) -> S3FdwResult<()> {
        // release local resources
        self.rdr.take();
        self.parser = Parser::JsonLine(VecDeque::new());
        Ok(())
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) -> S3FdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "uri")?;
                check_options_contain(&options, "format")?;
            }
        }

        Ok(())
    }
}
