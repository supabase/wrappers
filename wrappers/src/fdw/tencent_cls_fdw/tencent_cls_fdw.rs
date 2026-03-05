use crate::stats;
use chrono::Utc;
use hex;
use hmac::{Hmac, Mac};
use pgrx::pg_sys;
use reqwest::{
    self, StatusCode,
    header::{HeaderMap, HeaderValue},
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use serde_json::value::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::{TencentClsFdwError, TencentClsFdwResult};

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_MAX_RESPONSE_SIZE: usize = 10 * 1024 * 1024;
const DEFAULT_LIMIT: u64 = 100;

/// TC3-HMAC-SHA256 signing for Tencent Cloud API v3
fn tc3_sign(
    secret_id: &str,
    secret_key: &str,
    service: &str,
    host: &str,
    timestamp: i64,
    payload: &str,
) -> String {
    let date = chrono::DateTime::from_timestamp(timestamp, 0)
        .unwrap()
        .format("%Y-%m-%d")
        .to_string();

    // Step 1: Canonical request
    let hashed_payload = hex::encode(Sha256::digest(payload.as_bytes()));
    let canonical_headers = format!("content-type:application/json\nhost:{host}\n");
    let signed_headers = "content-type;host";
    let canonical_request =
        format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{hashed_payload}");

    // Step 2: String to sign
    let credential_scope = format!("{date}/{service}/tc3_request");
    let hashed_canonical = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let string_to_sign =
        format!("TC3-HMAC-SHA256\n{timestamp}\n{credential_scope}\n{hashed_canonical}");

    // Step 3: Signing key chain
    let k_date = hmac_sha256(format!("TC3{secret_key}").as_bytes(), date.as_bytes());
    let k_service = hmac_sha256(&k_date, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"tc3_request");
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    // Step 4: Authorization header
    format!(
        "TC3-HMAC-SHA256 Credential={secret_id}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn create_client() -> TencentClsFdwResult<ClientWithMiddleware> {
    let client = reqwest::Client::builder().build()?;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    Ok(ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build())
}

fn json_value_to_cell(tgt_col: &Column, v: &JsonValue) -> TencentClsFdwResult<Cell> {
    match tgt_col.type_oid {
        pg_sys::BOOLOID => v.as_bool().map(Cell::Bool),
        pg_sys::INT2OID => v
            .as_i64()
            .and_then(|s| i16::try_from(s).ok())
            .map(Cell::I16),
        pg_sys::INT4OID => v
            .as_i64()
            .and_then(|s| i32::try_from(s).ok())
            .map(Cell::I32),
        pg_sys::FLOAT8OID => v.as_f64().map(Cell::F64),
        pg_sys::INT8OID => v.as_i64().map(Cell::I64),
        pg_sys::TEXTOID => {
            if v.is_string() {
                v.as_str().map(|s| Cell::String(s.to_owned()))
            } else {
                Some(Cell::String(v.to_string()))
            }
        }
        _ => {
            return Err(TencentClsFdwError::UnsupportedColumnType(
                tgt_col.name.clone(),
            ));
        }
    }
    .ok_or(TencentClsFdwError::ColumnTypeNotMatch(tgt_col.name.clone()))
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Wener",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/tencent_cls_fdw",
    error_type = "TencentClsFdwError"
)]
pub(crate) struct TencentClsFdw {
    rt: Runtime,
    secret_id: String,
    secret_key: String,
    region: String,
    endpoint: String,
    client: Option<ClientWithMiddleware>,
    scan_result: Vec<Row>,
    max_response_size: usize,
}

impl TencentClsFdw {
    const FDW_NAME: &'static str = "TencentClsFdw";

    fn search_log(
        &self,
        topic_id: &str,
        query: &str,
        from_ms: i64,
        to_ms: i64,
        limit: u64,
        syntax_rule: u8,
        context: &str,
    ) -> TencentClsFdwResult<JsonValue> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| TencentClsFdwError::MissingParameter("client".to_string()))?;

        let mut body = serde_json::json!({
            "TopicId": topic_id,
            "Query": query,
            "From": from_ms,
            "To": to_ms,
            "Limit": limit,
            "SyntaxRule": syntax_rule,
        });
        if !context.is_empty() {
            body["Context"] = serde_json::json!(context);
        }

        let payload = serde_json::to_string(&body)?;
        let timestamp = Utc::now().timestamp();
        let host = &self.endpoint;

        let authorization = tc3_sign(
            &self.secret_id,
            &self.secret_key,
            "cls",
            host,
            timestamp,
            &payload,
        );

        let url = format!("https://{host}/");
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&authorization).unwrap(),
        );
        headers.insert("X-TC-Action", HeaderValue::from_static("SearchLog"));
        headers.insert("X-TC-Version", HeaderValue::from_static("2020-10-16"));
        headers.insert("X-TC-Region", HeaderValue::from_str(&self.region).unwrap());
        headers.insert(
            "X-TC-Timestamp",
            HeaderValue::from_str(&timestamp.to_string()).unwrap(),
        );

        let resp_text: String = self.rt.block_on(async {
            let resp = client
                .post(&url)
                .headers(headers)
                .header("Content-Type", "application/json")
                .body(payload)
                .send()
                .await
                .map_err(|e| TencentClsFdwError::RequestMiddlewareError(e))?;

            let status = resp.status();
            let text = resp
                .text()
                .await
                .map_err(|e| TencentClsFdwError::RequestError(e))?;

            if !status.is_success() {
                return Err(TencentClsFdwError::ApiError(format!(
                    "HTTP {}: {}",
                    status,
                    &text[..text.len().min(500)]
                )));
            }
            Ok::<_, TencentClsFdwError>(text)
        })?;

        if resp_text.len() > self.max_response_size {
            return Err(TencentClsFdwError::ResponseTooLarge(
                resp_text.len(),
                self.max_response_size,
            ));
        }

        let resp: JsonValue = serde_json::from_str(&resp_text)?;

        // Check for API error
        if let Some(error) = resp.pointer("/Response/Error").and_then(|e| e.as_object()) {
            let code = error
                .get("Code")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");
            let message = error.get("Message").and_then(|v| v.as_str()).unwrap_or("");
            return Err(TencentClsFdwError::ApiError(format!("{code}: {message}")));
        }

        Ok(resp)
    }

    fn resp_to_rows(
        &self,
        resp: &JsonValue,
        tgt_cols: &[Column],
        quals: &[Qual],
    ) -> TencentClsFdwResult<Vec<Row>> {
        let results = resp.pointer("/Response/Results").and_then(|v| v.as_array());

        let results = match results {
            Some(arr) => arr,
            None => return Ok(Vec::new()),
        };

        results
            .iter()
            .map(|record| {
                let mut row = Row::new();

                // Parse LogJson into a JSON object for field extraction
                let log_json: Option<JsonValue> = record
                    .get("LogJson")
                    .and_then(|v| v.as_str())
                    .and_then(|s| serde_json::from_str(s).ok());

                for tgt_col in tgt_cols {
                    let cell: Option<Cell> = match tgt_col.name.as_str() {
                        // Meta column: full result JSON
                        "_result" => Some(Cell::String(record.to_string())),
                        // Param columns: echo back WHERE values
                        name if name.starts_with("_") => quals.iter().find_map(|q| {
                            if q.field == tgt_col.name {
                                if let Value::Cell(cell) = &q.value {
                                    Some(cell.clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }),
                        // Standard CLS result fields
                        "log_time" => record.get("Time").and_then(|v| v.as_i64()).map(Cell::I64),
                        "source" => record
                            .get("Source")
                            .and_then(|v| v.as_str())
                            .map(|s| Cell::String(s.to_owned())),
                        "topic_id" => record
                            .get("TopicId")
                            .and_then(|v| v.as_str())
                            .map(|s| Cell::String(s.to_owned())),
                        "topic_name" => record
                            .get("TopicName")
                            .and_then(|v| v.as_str())
                            .map(|s| Cell::String(s.to_owned())),
                        "file_name" => record
                            .get("FileName")
                            .and_then(|v| v.as_str())
                            .map(|s| Cell::String(s.to_owned())),
                        "host_name" => record
                            .get("HostName")
                            .and_then(|v| v.as_str())
                            .map(|s| Cell::String(s.to_owned())),
                        "log_json" => record
                            .get("LogJson")
                            .and_then(|v| v.as_str())
                            .map(|s| Cell::String(s.to_owned())),
                        // Any other column: try to extract from LogJson
                        _ => {
                            if let Some(ref lj) = log_json {
                                if let Some(v) = lj.get(&tgt_col.name) {
                                    match json_value_to_cell(tgt_col, v) {
                                        Ok(cell) => Some(cell),
                                        Err(_) => None,
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                    };
                    row.push(&tgt_col.name, cell);
                }
                Ok(row)
            })
            .collect()
    }
}

impl ForeignDataWrapper<TencentClsFdwError> for TencentClsFdw {
    fn new(server: ForeignServer) -> TencentClsFdwResult<Self> {
        let secret_id = match server.options.get("secret_id") {
            Some(id) => id.to_owned(),
            None => {
                let id_key = require_option("secret_id_id", &server.options)?;
                get_vault_secret(id_key)
                    .ok_or_else(|| TencentClsFdwError::VaultSecretNotFound(id_key.to_string()))?
            }
        };
        let secret_key = match server.options.get("secret_key") {
            Some(key) => key.to_owned(),
            None => {
                let key_id = require_option("secret_key_id", &server.options)?;
                get_vault_secret(key_id)
                    .ok_or_else(|| TencentClsFdwError::VaultSecretNotFound(key_id.to_string()))?
            }
        };

        let region = server
            .options
            .get("region")
            .cloned()
            .unwrap_or_else(|| "ap-shanghai".to_string());

        let endpoint = server
            .options
            .get("endpoint")
            .cloned()
            .unwrap_or_else(|| format!("cls.{region}.tencentcloudapi.com"));

        let max_response_size = server
            .options
            .get("max_response_size")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_RESPONSE_SIZE);

        let client = Some(create_client()?);

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(TencentClsFdw {
            rt: create_async_runtime()?,
            secret_id,
            secret_key,
            region,
            endpoint,
            client,
            scan_result: Vec::new(),
            max_response_size,
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> TencentClsFdwResult<()> {
        let topic_id = require_option("topic_id", options)?;
        let query = options.get("query").map(|s| s.as_str()).unwrap_or("*");
        let syntax_rule: u8 = options
            .get("syntax_rule")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let limit: u64 = options
            .get("limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_LIMIT);

        // Extract _from/_to/_query from WHERE conditions
        let mut from_ms: i64 = Utc::now().timestamp_millis() - 3600 * 1000; // default: 1 hour ago
        let mut to_ms: i64 = Utc::now().timestamp_millis();
        let mut effective_query = query.to_string();

        for qual in quals {
            match qual.field.as_str() {
                "_from" => {
                    if let Value::Cell(Cell::I64(v)) = &qual.value {
                        from_ms = *v;
                    }
                }
                "_to" => {
                    if let Value::Cell(Cell::I64(v)) = &qual.value {
                        to_ms = *v;
                    }
                }
                "_query" => {
                    if let Value::Cell(Cell::String(v)) = &qual.value {
                        effective_query = v.clone();
                    }
                }
                _ => {}
            }
        }

        let resp = self.search_log(
            topic_id,
            &effective_query,
            from_ms,
            to_ms,
            limit,
            syntax_rule,
            "",
        )?;
        let result = self.resp_to_rows(&resp, columns, quals)?;

        if !result.is_empty() {
            stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, result.len() as i64);
            stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, result.len() as i64);
        }
        self.scan_result = result;

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> TencentClsFdwResult<Option<()>> {
        if self.scan_result.is_empty() {
            Ok(None)
        } else {
            Ok(self
                .scan_result
                .drain(0..1)
                .next_back()
                .map(|src_row| row.replace_with(src_row)))
        }
    }

    fn re_scan(&mut self) -> TencentClsFdwResult<()> {
        Ok(())
    }

    fn end_scan(&mut self) -> TencentClsFdwResult<()> {
        self.scan_result.clear();
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> TencentClsFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "topic_id")?;
            }
        }
        Ok(())
    }
}
