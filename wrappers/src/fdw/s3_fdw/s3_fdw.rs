use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZlibDecoder};
use aws_sdk_s3 as s3;
use http::Uri;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use serde_json::{self, Value as JsonValue};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::io::Cursor;
use std::pin::Pin;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

use supabase_wrappers::prelude::*;

// record parser for S3 text file
enum Parser {
    Csv(csv::Reader<Cursor<Vec<u8>>>),
    // JSON lines text file format: https://jsonlines.org/
    JsonLine(VecDeque<JsonValue>),
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/s3_fdw"
)]
pub(crate) struct S3Fdw {
    rt: Runtime,
    client: Option<s3::Client>,
    rdr: Option<BufReader<Pin<Box<dyn AsyncRead>>>>,
    parser: Parser,
    tgt_cols: Vec<Column>,
    buf: String,
}

impl S3Fdw {
    // local string line buffer size, in bytes
    // Note: this is not a hard limit, just an indication of full buffer
    const BUF_SIZE: usize = 256 * 1024;

    // fetch remote data to local string line buffer when it is empty and set
    // up record parser.
    // Returns:
    //   Some - still have records to read
    //   None - no more records
    fn refill(&mut self) -> Option<()> {
        if !self.buf.is_empty() {
            return Some(());
        }

        if let Some(ref mut rdr) = self.rdr {
            // fetch remote data by lines and fill in local buffer
            loop {
                match self.rt.block_on(rdr.read_line(&mut self.buf)) {
                    Ok(num_bytes) => {
                        if num_bytes == 0 || self.buf.len() > Self::BUF_SIZE {
                            break;
                        }
                    }
                    Err(err) => {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            &format!("fetch query result failed: {}", err),
                        );
                        return None;
                    }
                }
            }
        }

        if self.buf.is_empty() {
            return None;
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
                match serde_json::from_str::<JsonValue>(&json_str) {
                    Ok(rows) => {
                        *records =
                            VecDeque::from(rows.get("rows").unwrap().as_array().unwrap().to_vec());
                    }
                    Err(err) => {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            &format!("parse json line file failed: {}", err),
                        );
                        return None;
                    }
                }
            }
        }

        Some(())
    }
}

impl ForeignDataWrapper for S3Fdw {
    fn new(options: &HashMap<String, String>) -> Self {
        let mut ret = S3Fdw {
            rt: create_async_runtime(),
            client: None,
            rdr: None,
            parser: Parser::JsonLine(VecDeque::new()),
            tgt_cols: Vec::new(),
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
                    require_option("vault_secret_access_key", options).and_then(
                        |vault_secret_access_key| {
                            get_vault_secret(vault_access_key_id)
                                .zip(get_vault_secret(&vault_secret_access_key))
                        },
                    )
                }
                None => {
                    // if using credentials directly specified
                    require_option("aws_access_key_id", options)
                        .zip(require_option("aws_secret_access_key", options))
                }
            }
        };
        if creds.is_none() {
            return ret;
        }
        let creds = creds.unwrap();

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
        let config = ret.rt.block_on(aws_config::load_from_env());

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

        ret
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) {
        // extract s3 bucket and object path from uri option
        let (bucket, object) = if let Some(uri) = require_option("uri", options) {
            match uri.parse::<Uri>() {
                Ok(uri) => {
                    if uri.scheme_str() != Option::Some("s3")
                        || uri.host().is_none()
                        || uri.path().is_empty()
                    {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            &format!("invalid s3 uri: {}", uri),
                        );
                        return;
                    }
                    // exclude 1st "/" char in the path as s3 object path doesn't like it
                    (uri.host().unwrap().to_owned(), uri.path()[1..].to_string())
                }
                Err(err) => {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("parse s3 uri failed: {}", err),
                    );
                    return;
                }
            }
        } else {
            return;
        };

        // initialise parser according to format option
        if let Some(format) = require_option("format", options) {
            // create dummy parser
            match format.as_str() {
                "csv" => self.parser = Parser::Csv(csv::Reader::from_reader(Cursor::new(vec![0]))),
                "jsonl" => self.parser = Parser::JsonLine(VecDeque::new()),
                _ => {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!(
                            "invalid format option: {}, it can only be 'csv' or 'jsonl'",
                            format
                        ),
                    );
                    return;
                }
            }
        } else {
            return;
        };

        let has_header: bool = options.get("has_header") == Some(&"true".to_string());

        if let Some(client) = &self.client {
            self.tgt_cols = columns.to_vec();

            match self
                .rt
                .block_on(client.get_object().bucket(bucket).key(object).send())
            {
                Ok(resp) => {
                    let stream = resp.body.into_async_read();

                    let boxed_stream: Pin<Box<dyn AsyncRead>> =
                        // deal with compression
                        if let Some(compress) = options.get("compress") {
                            let buf_rdr = BufReader::new(stream);
                            match compress.as_str() {
                                "bzip2" => Box::pin(BzDecoder::new(buf_rdr)),
                                "gzip" => Box::pin(GzipDecoder::new(buf_rdr)),
                                "xz" => Box::pin(XzDecoder::new(buf_rdr)),
                                "zlib" => Box::pin(ZlibDecoder::new(buf_rdr)),
                                _ => {
                                    report_error(
                                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                                        &format!("invalid compression option: {}", compress),
                                    );
                                    return;
                                }
                            }
                        } else {
                            Box::pin(stream)
                        };

                    let mut rdr: BufReader<Pin<Box<dyn AsyncRead>>> = BufReader::new(boxed_stream);

                    // skip csv header line if needed
                    if let Parser::Csv(_) = self.parser {
                        if has_header {
                            let mut header = String::new();
                            if let Err(err) = self.rt.block_on(rdr.read_line(&mut header)) {
                                report_error(
                                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                                    &format!("fetch csv file failed: {}", err),
                                );
                                return;
                            }
                        }
                    }

                    self.rdr = Some(rdr);
                }
                Err(err) => report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("request s3 failed: {}", err),
                ),
            }
        }
    }

    fn iter_scan(&mut self, row: &mut Row) -> Option<()> {
        loop {
            if self.refill().is_none() {
                break;
            }

            // parse local buffer data to records
            match &mut self.parser {
                Parser::Csv(rdr) => {
                    let mut record = csv::StringRecord::new();
                    match rdr.read_record(&mut record) {
                        Ok(result) => {
                            if result {
                                for col in &self.tgt_cols {
                                    let cell =
                                        record.get(col.num - 1).map(|s| Cell::String(s.to_owned()));
                                    row.push(&col.name, cell);
                                }
                                return Some(());
                            } else {
                                // no more records left in the local buffer, refill from remote
                                self.buf.clear();
                            }
                        }
                        Err(err) => {
                            report_error(
                                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                                &format!("read csv record failed: {}", err),
                            );
                            break;
                        }
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
                            return Some(());
                        }
                        None => {
                            // no more records left in the local buffer, refill from remote
                            self.buf.clear();
                        }
                    }
                }
            }
        }

        None
    }

    fn end_scan(&mut self) {
        self.rdr.take();
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "uri");
                check_options_contain(&options, "format");
            }
        }
    }
}
