use crate::stats;
use pgrx::{
    pg_sys,
    prelude::{AnyNumeric, Date, Timestamp, TimestampWithTimeZone},
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

use super::{LogflareFdwError, LogflareFdwResult};

fn create_client(api_key: &str) -> LogflareFdwResult<ClientWithMiddleware> {
    let mut headers = HeaderMap::new();
    let header_name = HeaderName::from_static("x-api-key");
    let mut auth_value = HeaderValue::from_str(api_key)?;
    auth_value.set_sensitive(true);
    headers.insert(header_name, auth_value);
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    Ok(ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build())
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

fn json_value_to_cell(tgt_col: &Column, v: &JsonValue) -> LogflareFdwResult<Cell> {
    match tgt_col.type_oid {
        pg_sys::BOOLOID => v.as_bool().map(Cell::Bool),
        pg_sys::CHAROID => v.as_i64().and_then(|s| i8::try_from(s).ok()).map(Cell::I8),
        pg_sys::INT2OID => v
            .as_i64()
            .and_then(|s| i16::try_from(s).ok())
            .map(Cell::I16),
        pg_sys::FLOAT4OID => v.as_f64().map(|s| s as f32).map(Cell::F32),
        pg_sys::INT4OID => v
            .as_i64()
            .and_then(|s| i32::try_from(s).ok())
            .map(Cell::I32),
        pg_sys::FLOAT8OID => v.as_f64().map(Cell::F64),
        pg_sys::INT8OID => v.as_i64().map(Cell::I64),
        pg_sys::NUMERICOID => v
            .as_f64()
            .and_then(|s| AnyNumeric::try_from(s).ok())
            .map(Cell::Numeric),
        pg_sys::TEXTOID => v.as_str().map(|s| s.to_owned()).map(Cell::String),
        pg_sys::DATEOID => v
            .as_str()
            .and_then(|s| Date::from_str(s).ok())
            .map(Cell::Date),
        pg_sys::TIMESTAMPOID => v
            .as_str()
            .and_then(|s| Timestamp::from_str(s).ok())
            .map(Cell::Timestamp),
        pg_sys::TIMESTAMPTZOID => v
            .as_str()
            .and_then(|s| TimestampWithTimeZone::from_str(s).ok())
            .map(Cell::Timestamptz),
        _ => {
            return Err(LogflareFdwError::UnsupportedColumnType(
                tgt_col.name.clone(),
            ))
        }
    }
    .ok_or(LogflareFdwError::ColumnTypeNotMatch(tgt_col.name.clone()))
}

#[wrappers_fdw(
    version = "0.1.1",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/logflare_fdw",
    error_type = "LogflareFdwError"
)]
pub(crate) struct LogflareFdw {
    rt: Runtime,
    base_url: Url,
    client: Option<ClientWithMiddleware>,
    scan_result: Vec<Row>,
    params: Vec<Qual>,
}

impl LogflareFdw {
    const FDW_NAME: &'static str = "LogflareFdw";
    const BASE_URL: &'static str = "https://api.logflare.app/api/endpoints/query/";

    fn build_url(&self, endpoint: &str) -> LogflareFdwResult<Option<Url>> {
        let mut url = self.base_url.join(endpoint)?;
        for param in &self.params {
            // extract actual param name, e.g. "_param_foo" => "foo"
            let param_name = &param.field[7..];

            if param.operator != "=" {
                return Err(LogflareFdwError::NoEqualParameter(param_name.to_string()));
            }

            match &param.value {
                Value::Cell(cell) => {
                    let value = match cell {
                        Cell::String(s) => s.clone(),
                        Cell::Date(d) => d.to_string().as_str().trim_matches('\'').to_owned(),
                        Cell::Timestamp(t) => t.to_string().as_str().trim_matches('\'').to_owned(),
                        Cell::Timestamptz(t) => {
                            t.to_string().as_str().trim_matches('\'').to_owned()
                        }
                        _ => cell.to_string(),
                    };
                    url.query_pairs_mut().append_pair(param_name, &value);
                }
                Value::Array(_) => {
                    return Err(LogflareFdwError::NoArrayParameter(param_name.to_string()))
                }
            }
        }

        Ok(Some(url))
    }

    fn resp_to_rows(
        &mut self,
        body: &JsonValue,
        tgt_cols: &[Column],
    ) -> LogflareFdwResult<Vec<Row>> {
        body.as_object()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.as_array())
            .ok_or(LogflareFdwError::InvalidResponse(body.to_string()))
            .and_then(|arr| {
                arr.iter()
                    .map(|record| {
                        let mut row = Row::new();

                        if let Some(r) = record.as_object() {
                            for tgt_col in tgt_cols {
                                let cell: Option<Cell> = if tgt_col.name == "_result" {
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
                                    if let Some(s) = r.get(&tgt_col.name) {
                                        match json_value_to_cell(tgt_col, s) {
                                            Ok(cell) => Some(cell),
                                            Err(err) => return Err(err),
                                        }
                                    } else {
                                        None
                                    }
                                };
                                row.push(&tgt_col.name, cell);
                            }
                        }

                        Ok(row)
                    })
                    .collect()
            })
    }
}

impl ForeignDataWrapper<LogflareFdwError> for LogflareFdw {
    fn new(options: &HashMap<String, String>) -> LogflareFdwResult<Self> {
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
            None => {
                let key_id = require_option("api_key_id", options)?;
                get_vault_secret(key_id).map(|api_key| create_client(&api_key))
            }
        }
        .transpose()?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(LogflareFdw {
            rt: create_async_runtime()?,
            base_url: Url::parse(&base_url)?,
            client,
            scan_result: Vec::default(),
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
    ) -> LogflareFdwResult<()> {
        let endpoint = require_option("endpoint", options)?;

        // extract params
        self.params = if let Some(params) = extract_params(quals) {
            params
        } else {
            return Ok(());
        };

        if let Some(client) = &self.client {
            // build url
            let url = self.build_url(endpoint)?;
            if url.is_none() {
                return Ok(());
            }
            let url = url.unwrap();

            // make api call
            let body: JsonValue = self.rt.block_on(client.get(url).send()).and_then(|resp| {
                stats::inc_stats(
                    Self::FDW_NAME,
                    stats::Metric::BytesIn,
                    resp.content_length().unwrap_or(0) as i64,
                );

                if resp.status() == StatusCode::NOT_FOUND {
                    // if it is 404 error, we should treat it as an empty
                    // result rather than a request error
                    return Ok(JsonValue::Null);
                }

                resp.error_for_status()
                    .and_then(|resp| self.rt.block_on(resp.json()))
                    .map_err(reqwest_middleware::Error::from)
            })?;
            if body.is_null() {
                return Ok(());
            }

            let result = self.resp_to_rows(&body, columns)?;
            if !result.is_empty() {
                stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, result.len() as i64);
                stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, result.len() as i64);
            }
            self.scan_result = result;
        }

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> LogflareFdwResult<Option<()>> {
        if self.scan_result.is_empty() {
            Ok(None)
        } else {
            Ok(self
                .scan_result
                .drain(0..1)
                .last()
                .map(|src_row| row.replace_with(src_row)))
        }
    }

    fn end_scan(&mut self) -> LogflareFdwResult<()> {
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> LogflareFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "endpoint")?;
            }
        }

        Ok(())
    }
}
