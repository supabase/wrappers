use crate::stats;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{
    pg_sys,
    prelude::{AnyNumeric, Date, PgSqlErrorCode, Timestamp},
};
use reqwest::{
    self,
    header::{HeaderMap, HeaderName, HeaderValue},
    StatusCode, Url,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::value::Value as JsonValue;
use std::collections::HashMap;
use std::str::FromStr;

use supabase_wrappers::prelude::*;
use thiserror::Error;

fn create_client(api_key: &str) -> ClientWithMiddleware {
    let mut headers = HeaderMap::new();
    let header_name = HeaderName::from_static("x-api-key");
    let mut auth_value = HeaderValue::from_str(api_key).unwrap();
    auth_value.set_sensitive(true);
    headers.insert(header_name, auth_value);
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

macro_rules! report_request_error {
    ($err:ident) => {{
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_ERROR,
            &format!("request failed: {}", $err),
        );
        return Ok(());
    }};
}

fn extract_params(quals: &[Qual]) -> Option<Vec<Qual>> {
    let params = quals
        .iter()
        .filter_map(|q| {
            if q.field.starts_with("_param_") {
                Some(q.clone())
            } else {
                None
            }
        })
        .collect();
    Some(params)
}

macro_rules! type_mismatch {
    ($col:ident) => {
        panic!("column '{}' data type not match", $col.name)
    };
}

fn json_value_to_cell(tgt_col: &Column, v: &JsonValue) -> Cell {
    match tgt_col.type_oid {
        pg_sys::BOOLOID => Cell::Bool(v.as_bool().unwrap_or_else(|| type_mismatch!(tgt_col))),
        pg_sys::CHAROID => Cell::I8(
            v.as_i64()
                .map(|s| i8::try_from(s).unwrap_or_else(|_| type_mismatch!(tgt_col)))
                .unwrap_or_else(|| type_mismatch!(tgt_col)),
        ),
        pg_sys::INT2OID => Cell::I16(
            v.as_i64()
                .map(|s| i16::try_from(s).unwrap_or_else(|_| type_mismatch!(tgt_col)))
                .unwrap_or_else(|| type_mismatch!(tgt_col)),
        ),
        pg_sys::FLOAT4OID => Cell::F32(
            v.as_f64()
                .map(|s| s as f32)
                .unwrap_or_else(|| type_mismatch!(tgt_col)),
        ),
        pg_sys::INT4OID => Cell::I32(
            v.as_i64()
                .map(|s| i32::try_from(s).unwrap_or_else(|_| type_mismatch!(tgt_col)))
                .unwrap_or_else(|| type_mismatch!(tgt_col)),
        ),
        pg_sys::FLOAT8OID => Cell::F64(v.as_f64().unwrap_or_else(|| type_mismatch!(tgt_col))),
        pg_sys::INT8OID => Cell::I64(v.as_i64().unwrap_or_else(|| type_mismatch!(tgt_col))),
        pg_sys::NUMERICOID => Cell::Numeric(
            v.as_f64()
                .map(|s| AnyNumeric::try_from(s).unwrap_or_else(|_| type_mismatch!(tgt_col)))
                .unwrap_or_else(|| type_mismatch!(tgt_col)),
        ),
        pg_sys::TEXTOID => Cell::String(
            v.as_str()
                .map(|s| s.to_owned())
                .unwrap_or_else(|| type_mismatch!(tgt_col)),
        ),
        pg_sys::DATEOID => Cell::Date(
            v.as_str()
                .map(|s| Date::from_str(s).unwrap_or_else(|_| type_mismatch!(tgt_col)))
                .unwrap_or_else(|| type_mismatch!(tgt_col)),
        ),
        pg_sys::TIMESTAMPOID => Cell::Timestamp(
            v.as_str()
                .map(|s| Timestamp::from_str(s).unwrap_or_else(|_| type_mismatch!(tgt_col)))
                .unwrap_or_else(|| type_mismatch!(tgt_col)),
        ),
        _ => {
            // report error and return a dummy cell
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE,
                &format!(
                    "column '{}' data type oid '{}' is not supported",
                    tgt_col.name, tgt_col.type_oid
                ),
            );
            Cell::Bool(false)
        }
    }
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/logflare_fdw",
    error_type = "LogflareFdwError"
)]
pub(crate) struct LogflareFdw {
    rt: Runtime,
    base_url: Url,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
    params: Vec<Qual>,
}

impl LogflareFdw {
    const FDW_NAME: &str = "LogflareFdw";
    const BASE_URL: &str = "https://api.logflare.app/api/endpoints/query/";

    fn build_url(&self, endpoint: &str) -> Option<Url> {
        let mut url = self.base_url.join(endpoint).unwrap();
        for param in &self.params {
            // extract actual param name, e.g. "_param_foo" => "foo"
            let param_name = &param.field[7..];

            if param.operator != "=" {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("Parameter '{}' only supports '=' operator", param_name),
                );
                return None;
            }
            match &param.value {
                Value::Cell(cell) => {
                    let value = match cell {
                        Cell::String(s) => s.clone(),
                        Cell::Date(d) => d.to_string().as_str().trim_matches('\'').to_owned(),
                        Cell::Timestamp(t) => t.to_string().as_str().trim_matches('\'').to_owned(),
                        _ => cell.to_string(),
                    };
                    url.query_pairs_mut().append_pair(param_name, &value);
                }
                Value::Array(_) => {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("Parameter '{}' doesn't supports array value", param_name),
                    );
                    return None;
                }
            }
        }
        Some(url)
    }

    fn resp_to_rows(&mut self, body: &JsonValue, tgt_cols: &[Column]) -> Option<Vec<Row>> {
        body.as_object()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|record| {
                        let mut row = Row::new();
                        if let Some(r) = record.as_object() {
                            for tgt_col in tgt_cols {
                                let cell = if tgt_col.name == "_result" {
                                    // add _result meta cell
                                    Some(Cell::String(record.to_string()))
                                } else if tgt_col.name.starts_with("_param_") {
                                    // add param cell
                                    self.params.iter().find_map(|p| {
                                        if p.field == tgt_col.name {
                                            if let Value::Cell(cell) = &p.value {
                                                Some(cell.clone())
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    })
                                } else {
                                    // add normal cell
                                    r.get(&tgt_col.name).map(|v| json_value_to_cell(tgt_col, v))
                                };
                                row.push(&tgt_col.name, cell);
                            }
                        }
                        row
                    })
                    .collect::<Vec<Row>>()
            })
    }
}

#[derive(Error, Debug)]
enum LogflareFdwError {
    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),
}

impl From<LogflareFdwError> for ErrorReport {
    fn from(value: LogflareFdwError) -> Self {
        match value {
            LogflareFdwError::CreateRuntimeError(e) => e.into(),
        }
    }
}

impl ForeignDataWrapper<LogflareFdwError> for LogflareFdw {
    fn new(options: &HashMap<String, String>) -> Result<Self, LogflareFdwError> {
        let base_url = options
            .get("api_url")
            .map(|t| t.to_owned())
            .map(|s| {
                if s.ends_with('/') {
                    s
                } else {
                    format!("{}/", s)
                }
            })
            .unwrap_or_else(|| LogflareFdw::BASE_URL.to_string());
        let client = match options.get("api_key") {
            Some(api_key) => Some(create_client(api_key)),
            None => require_option("api_key_id", options)
                .and_then(|key_id| get_vault_secret(&key_id))
                .map(|api_key| create_client(&api_key)),
        };

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(LogflareFdw {
            rt: create_async_runtime()?,
            base_url: Url::parse(&base_url).unwrap(),
            client,
            scan_result: None,
            params: Vec::default(),
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> Result<(), LogflareFdwError> {
        let endpoint = if let Some(name) = require_option("endpoint", options) {
            name
        } else {
            return Ok(());
        };

        // extract params
        self.params = if let Some(params) = extract_params(quals) {
            params
        } else {
            return Ok(());
        };

        if let Some(client) = &self.client {
            // build url
            let url = self.build_url(&endpoint);
            if url.is_none() {
                return Ok(());
            }
            let url = url.unwrap();

            // make api call
            match self.rt.block_on(client.get(url).send()) {
                Ok(resp) => {
                    stats::inc_stats(
                        Self::FDW_NAME,
                        stats::Metric::BytesIn,
                        resp.content_length().unwrap_or(0) as i64,
                    );

                    if resp.status() == StatusCode::NOT_FOUND {
                        // if it is 404 error, we should treat it as an empty
                        // result rather than a request error
                        return Ok(());
                    }

                    match resp.error_for_status() {
                        Ok(resp) => {
                            let body: JsonValue = self.rt.block_on(resp.json()).unwrap();
                            let result = self.resp_to_rows(&body, columns);
                            if let Some(result) = &result {
                                stats::inc_stats(
                                    Self::FDW_NAME,
                                    stats::Metric::RowsIn,
                                    result.len() as i64,
                                );
                                stats::inc_stats(
                                    Self::FDW_NAME,
                                    stats::Metric::RowsOut,
                                    result.len() as i64,
                                );
                            }
                            self.scan_result = result;
                        }
                        Err(err) => report_request_error!(err),
                    }
                }
                Err(err) => report_request_error!(err),
            }
        }

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, LogflareFdwError> {
        if let Some(ref mut result) = self.scan_result {
            if !result.is_empty() {
                return Ok(result
                    .drain(0..1)
                    .last()
                    .map(|src_row| row.replace_with(src_row)));
            }
        }
        Ok(None)
    }

    fn end_scan(&mut self) -> Result<(), LogflareFdwError> {
        self.scan_result.take();
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), LogflareFdwError> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "endpoint");
            }
        }

        Ok(())
    }
}
