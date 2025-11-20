use crate::stats;
use aws_config::BehaviorVersion;
use aws_sdk_s3vectors::{
    client::Client,
    error::SdkError,
    operation::list_vectors::ListVectorsError,
    operation::list_vectors::ListVectorsOutput,
    types::{PutInputVector, VectorData},
};
use aws_smithy_async::future::pagination_stream::PaginationStream;
use pgrx::{datum::JsonB, prelude::*};
use std::collections::HashMap;
use std::env;

use supabase_wrappers::prelude::*;

use super::conv::json_value_to_document;
use super::s3vec::S3Vec;
use super::{S3VectorsFdwError, S3VectorsFdwResult};

#[wrappers_fdw(
    version = "0.1.1",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/s3vectors_fdw",
    error_type = "S3VectorsFdwError"
)]
pub(crate) struct S3VectorsFdw {
    rt: Runtime,
    client: Client,
    bucket_name: Option<String>,
    index_name: Option<String>,
    tgt_cols: Vec<Column>,
    quals: Vec<Qual>,
    row_limit: Option<i64>,
    batch_size: usize,

    // for vectors selection
    vectors_stream: Option<PaginationStream<Result<ListVectorsOutput, SdkError<ListVectorsError>>>>,
    curr_vectors: Vec<S3Vec>,
    scan_initialised: bool,
    has_next_page: bool,
    row_cnt: i64,

    // for vectors insertion
    insert_vectors: Vec<PutInputVector>,
}

impl S3VectorsFdw {
    const FDW_NAME: &'static str = "S3VectorsFdw";

    fn fetch_next_page(&mut self) -> S3VectorsFdwResult<()> {
        self.curr_vectors.clear();
        self.has_next_page = false;

        if let Some(stream) = self.vectors_stream.as_mut() {
            if let Some(next_batch) = self.rt.block_on(stream.try_next())? {
                self.has_next_page = next_batch.next_token.is_some();
                self.curr_vectors = next_batch.vectors.iter().map(S3Vec::from).collect();
            }
        }

        Ok(())
    }

    // get all vectors if no quals specified
    // ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_ListVectors.html
    fn list_vectors(&mut self) -> S3VectorsFdwResult<()> {
        let stream = self
            .client
            .list_vectors()
            .set_vector_bucket_name(self.bucket_name.clone())
            .set_index_name(self.index_name.clone())
            .set_max_results(Some(self.batch_size as _))
            .set_return_data(Some(true))
            .set_return_metadata(Some(true))
            .into_paginator()
            .send();
        self.vectors_stream = Some(stream);
        self.fetch_next_page()
    }

    // get vectors for specified key(s)
    // ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_GetVectors.html
    fn get_vectors(&mut self) -> S3VectorsFdwResult<()> {
        let qual = &self.quals[0];
        let mut keys = Vec::new();

        if qual.use_or {
            // e.g. "where key in ('aa', 'bb')"
            if let Value::Array(cells) = &qual.value {
                for cell in cells {
                    if let Cell::String(key) = cell {
                        keys.push(key.clone());
                    }
                }
            }
        } else {
            // e.g. "where key = 'aa'"
            if let Value::Cell(Cell::String(key)) = &qual.value {
                keys.push(key.clone());
            }
        }

        let result = self.rt.block_on(
            self.client
                .get_vectors()
                .set_vector_bucket_name(self.bucket_name.clone())
                .set_index_name(self.index_name.clone())
                .set_keys(Some(keys))
                .set_return_data(Some(true))
                .set_return_metadata(Some(true))
                .send(),
        )?;

        self.curr_vectors = result.vectors.iter().map(S3Vec::from).collect();

        Ok(())
    }

    // get vectors by approximate nearest neighbor search query
    // ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_QueryVectors.html
    fn query_vectors(&mut self) -> S3VectorsFdwResult<()> {
        let query_vector = {
            let qual = self.quals.iter().find(|q| q.field == "data");
            if let Some(q) = qual {
                if let Value::Cell(Cell::Bytea(bytea)) = &q.value {
                    let s3vec = if let Some(param) = &q.param {
                        if let Some(Value::Cell(Cell::Bytea(b))) = *param
                            .eval_value
                            .lock()
                            .expect("parameter eval value should be locked")
                        {
                            S3Vec::try_from(b)?
                        } else {
                            S3Vec::try_from(*bytea)?
                        }
                    } else {
                        S3Vec::try_from(*bytea)?
                    };
                    Some(VectorData::Float32(s3vec.data.clone()))
                } else {
                    None
                }
            } else {
                None
            }
        };
        let metadata_filter = self.quals.iter().find_map(|q| {
            if q.field == "metadata" {
                if let Value::Cell(Cell::Json(json)) = &q.value {
                    let document = json_value_to_document(&json.0);
                    return Some(document);
                }
            }
            None
        });
        // return top 3 vectors by default
        let top_k = self.row_limit.map(|v| v as i32).or(Some(3));

        let result = self.rt.block_on(
            self.client
                .query_vectors()
                .set_vector_bucket_name(self.bucket_name.clone())
                .set_index_name(self.index_name.clone())
                .set_query_vector(query_vector)
                .set_filter(metadata_filter)
                .set_top_k(top_k)
                .set_return_distance(Some(true))
                .set_return_metadata(Some(true))
                .send(),
        )?;

        self.curr_vectors = result.vectors.iter().map(S3Vec::from).collect();

        Ok(())
    }

    fn init_scan(&mut self) -> S3VectorsFdwResult<()> {
        self.scan_initialised = true;

        if self.quals.is_empty() {
            return self.list_vectors();
        } else if self.quals.len() == 1 {
            let qual = &self.quals[0];
            if qual.field == "key" && qual.operator.as_str() == "=" {
                return self.get_vectors();
            } else if qual.field == "data" && qual.operator.as_str() == "<==>" {
                return self.query_vectors();
            }
        } else if self.quals.len() == 2
            && self.quals.iter().all(|qual| {
                (qual.field == "data" || qual.field == "metadata")
                    && qual.operator.as_str() == "<==>"
            })
        {
            return self.query_vectors();
        }

        Err(S3VectorsFdwError::QueryNotSupported)
    }

    fn flush_vectors(&mut self) -> S3VectorsFdwResult<()> {
        if self.insert_vectors.is_empty() {
            return Ok(());
        }

        let _ = self.rt.block_on(
            self.client
                .put_vectors()
                .set_vector_bucket_name(self.bucket_name.clone())
                .set_index_name(self.index_name.clone())
                .set_vectors(Some(self.insert_vectors.clone()))
                .send(),
        )?;

        self.insert_vectors.clear();

        Ok(())
    }
}

impl ForeignDataWrapper<S3VectorsFdwError> for S3VectorsFdw {
    fn new(server: ForeignServer) -> S3VectorsFdwResult<Self> {
        let rt = create_async_runtime()?;

        let batch_size = require_option_or("batch_size", &server.options, "300")
            .parse::<usize>()
            .unwrap_or(300)
            .clamp(1, 500);

        // get AWS credentials from server options
        let creds = {
            match server.options.get("vault_access_key_id") {
                Some(vault_access_key_id) => {
                    // if using credentials stored in Vault
                    let vault_secret_access_key =
                        require_option("vault_secret_access_key", &server.options)?;
                    get_vault_secret(vault_access_key_id)
                        .zip(get_vault_secret(vault_secret_access_key))
                }
                None => {
                    // if using credentials directly specified
                    let aws_access_key_id =
                        require_option("aws_access_key_id", &server.options)?.to_string();
                    let aws_secret_access_key =
                        require_option("aws_secret_access_key", &server.options)?.to_string();
                    Some((aws_access_key_id, aws_secret_access_key))
                }
            }
            .expect("AWS credentials should be provided in server options")
        };

        // get region
        let region = require_option_or("aws_region", &server.options, "us-east-1");

        // set AWS environment variables and create shared config from them
        env::set_var("AWS_ACCESS_KEY_ID", creds.0);
        env::set_var("AWS_SECRET_ACCESS_KEY", creds.1);
        env::set_var("AWS_REGION", region);

        // set endpoint URL if needed
        if let Some(endpoint_url) = server.options.get("endpoint_url") {
            let endpoint_url = if endpoint_url.ends_with('/') {
                endpoint_url.clone()
            } else {
                format!("{endpoint_url}/")
            };
            env::set_var("AWS_ENDPOINT_URL", endpoint_url);
        }

        // load AWS config and create client
        let config_loader = aws_config::defaults(BehaviorVersion::latest());
        let config = rt.block_on(config_loader.load());
        let client = Client::new(&config);

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(S3VectorsFdw {
            rt,
            client,
            bucket_name: None,
            index_name: None,
            tgt_cols: Vec::new(),
            quals: Vec::new(),
            row_limit: None,
            batch_size,
            vectors_stream: None,
            curr_vectors: Vec::new(),
            scan_initialised: false,
            has_next_page: false,
            row_cnt: 0,
            insert_vectors: Vec::new(),
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> S3VectorsFdwResult<()> {
        self.bucket_name = require_option("bucket_name", options)?.to_owned().into();
        self.index_name = require_option("index_name", options)?.to_owned().into();
        self.tgt_cols = columns.to_vec();
        self.quals = quals.to_vec();
        self.row_limit = limit.clone().map(|limit| limit.count);
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> S3VectorsFdwResult<Option<()>> {
        if !self.scan_initialised {
            self.init_scan()?;
        }

        loop {
            // end iter scan if the row limit is reached
            if let Some(limit) = self.row_limit {
                if self.row_cnt >= limit {
                    break;
                }
            }

            // convert a vector to a row
            if let Some(vector) = self.curr_vectors.pop() {
                for tgt_col in &self.tgt_cols {
                    match tgt_col.name.as_str() {
                        "key" => {
                            row.push("key", Some(Cell::String(vector.key.clone())));
                        }
                        "data" => {
                            let cbor_data = unsafe { pgrx::datum::cbor_encode(&vector) };
                            row.push("data", Some(Cell::Bytea(cbor_data.cast_mut())));
                        }
                        "metadata" => {
                            row.push(
                                "metadata",
                                vector.metadata.clone().map(|m| Cell::Json(JsonB(m))),
                            );
                        }
                        _ => {}
                    }
                }

                // increase row count
                self.row_cnt += 1;

                return Ok(Some(()));
            }

            // need to fetch next page of vectors
            if self.has_next_page {
                self.fetch_next_page()?;
                continue;
            }

            break;
        }

        Ok(None)
    }

    fn end_scan(&mut self) -> S3VectorsFdwResult<()> {
        Ok(())
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> S3VectorsFdwResult<()> {
        self.bucket_name = require_option("bucket_name", options)?.to_owned().into();
        self.index_name = require_option("index_name", options)?.to_owned().into();
        self.insert_vectors.clear();
        Ok(())
    }

    fn insert(&mut self, src: &Row) -> S3VectorsFdwResult<()> {
        let mut builder = PutInputVector::builder();

        for (col_name, cell) in src.iter() {
            match col_name.as_str() {
                "key" => {
                    if let Some(Cell::String(key)) = cell {
                        builder = builder.set_key(key.clone().into());
                    } else {
                        return Err(S3VectorsFdwError::InvalidInsertValue(format!(
                            "key: {cell:?}",
                        )));
                    }
                }
                "data" => {
                    if let Some(Cell::Bytea(vector)) = cell {
                        let s3vec = S3Vec::try_from(*vector)?;
                        let vector_data = VectorData::Float32(s3vec.data.clone());
                        builder = builder.set_data(vector_data.into());
                    } else {
                        return Err(S3VectorsFdwError::InvalidInsertValue(format!(
                            "data: {cell:?}"
                        )));
                    }
                }
                "metadata" => {
                    if let Some(c) = cell {
                        if let Cell::Json(metadata) = c {
                            let document = json_value_to_document(&metadata.0);
                            builder = builder.set_metadata(document.into());
                        } else {
                            return Err(S3VectorsFdwError::InvalidInsertValue(format!(
                                "metadata: {cell:?}"
                            )));
                        }
                    } else {
                        builder = builder.set_metadata(None);
                    }
                }
                _ => {}
            }
        }

        let vector = builder.build()?;
        self.insert_vectors.push(vector);

        if self.insert_vectors.len() >= self.batch_size {
            self.flush_vectors()?;
        }

        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> S3VectorsFdwResult<()> {
        match rowid {
            Cell::String(key) => {
                // delete the vector using the key
                let _resp = self.rt.block_on(
                    self.client
                        .delete_vectors()
                        .set_vector_bucket_name(self.bucket_name.clone())
                        .set_index_name(self.index_name.clone())
                        .set_keys(Some(vec![key.to_owned()]))
                        .send(),
                )?;
            }
            _ => return Err(S3VectorsFdwError::InvalidRowId(format!("{rowid:?}"))),
        };

        Ok(())
    }

    fn end_modify(&mut self) -> S3VectorsFdwResult<()> {
        self.flush_vectors()?;
        Ok(())
    }

    fn import_foreign_schema(
        &mut self,
        import_stmt: ImportForeignSchemaStmt,
    ) -> S3VectorsFdwResult<Vec<String>> {
        let bucket_name = require_option("bucket_name", &import_stmt.options)?;
        let mut next_token: Option<String> = None;
        let mut ret: Vec<String> = Vec::new();

        loop {
            let mut request = self
                .client
                .list_indexes()
                .set_vector_bucket_name(Some(bucket_name.to_owned()))
                .set_max_results(Some(self.batch_size as _));

            if let Some(token) = next_token {
                request = request.set_next_token(Some(token));
            }

            let resp = self.rt.block_on(request.send())?;

            for index in resp.indexes {
                // for PostgreSQL table name compatibility
                let table_name = index.index_name.replace('-', "_");

                let table_sql = format!(
                    r#"create foreign table if not exists {} (
                        key text not null,
                        data s3vec not null,
                        metadata jsonb
                    )
                    server {} options (
                        bucket_name '{}',
                        index_name '{}',
                        rowid_column 'key'
                    )"#,
                    table_name, import_stmt.server_name, bucket_name, index.index_name
                );
                ret.push(table_sql);
            }

            // check if there are more pages
            next_token = resp.next_token;
            if next_token.is_none() {
                break;
            }
        }

        Ok(ret)
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> S3VectorsFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "bucket_name")?;
                check_options_contain(&options, "index_name")?;
            }
        }

        Ok(())
    }
}
