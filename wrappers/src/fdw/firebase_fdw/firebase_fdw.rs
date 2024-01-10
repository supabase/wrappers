use crate::stats;
use pgrx::{pg_sys, prelude::*, JsonB};
use regex::Regex;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::str::FromStr;
use yup_oauth2::AccessToken;
use yup_oauth2::ServiceAccountAuthenticator;

use supabase_wrappers::prelude::*;

use super::{FirebaseFdwError, FirebaseFdwResult};

fn get_oauth2_token(sa_key: &str, rt: &Runtime) -> FirebaseFdwResult<AccessToken> {
    let creds = yup_oauth2::parse_service_account_key(sa_key.as_bytes())?;
    let sa = rt.block_on(ServiceAccountAuthenticator::builder(creds).build())?;
    let scopes = &[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/firebase.database",
        "https://www.googleapis.com/auth/firebase.messaging",
        "https://www.googleapis.com/auth/identitytoolkit",
        "https://www.googleapis.com/auth/userinfo.email",
    ];
    Ok(rt.block_on(sa.token(scopes))?)
}

fn body_to_rows(
    resp: &JsonValue,
    obj_key: &str,
    normal_cols: Vec<(&str, &str, &str)>,
    tgt_cols: &[Column],
) -> FirebaseFdwResult<Vec<Row>> {
    let mut result = Vec::new();

    let objs = resp
        .as_object()
        .and_then(|v| v.get(obj_key))
        .and_then(|v| v.as_array())
        .ok_or(FirebaseFdwError::InvalidResponse(resp.to_string()))?;

    for obj in objs {
        let mut row = Row::new();

        // extract normal columns
        for tgt_col in tgt_cols {
            if let Some((src_name, col_name, col_type)) =
                normal_cols.iter().find(|(_, c, _)| c == &tgt_col.name)
            {
                let v = obj
                    .as_object()
                    .and_then(|v| v.get(*src_name))
                    .ok_or(FirebaseFdwError::InvalidResponse(resp.to_string()))?;
                let cell = match *col_type {
                    "bool" => v.as_bool().map(Cell::Bool),
                    "i64" => v.as_i64().map(Cell::I64),
                    "string" => v.as_str().map(|a| Cell::String(a.to_owned())),
                    "timestamp" => Some(
                        v.as_str()
                            .and_then(|a| a.parse::<i64>().ok())
                            .map(|ms| to_timestamp(ms as f64 / 1000.0).to_utc())
                            .map(Cell::Timestamp)
                            .ok_or(FirebaseFdwError::InvalidTimestampFormat(v.to_string()))?,
                    ),
                    "timestamp_iso" => Some(
                        v.as_str()
                            .and_then(|a| Timestamp::from_str(a).ok())
                            .map(Cell::Timestamp)
                            .ok_or(FirebaseFdwError::InvalidTimestampFormat(v.to_string()))?,
                    ),
                    "json" => Some(Cell::Json(JsonB(v.clone()))),
                    _ => {
                        return Err(FirebaseFdwError::UnsupportedColumnType(format!(
                            "{}({})",
                            col_name, col_type
                        )))
                    }
                };
                row.push(col_name, cell);
            }
        }

        // put all properties into 'attrs' JSON column
        if tgt_cols.iter().any(|c| &c.name == "attrs") {
            let attrs = serde_json::from_str(&obj.to_string())?;
            row.push("attrs", Some(Cell::Json(JsonB(attrs))));
        }

        result.push(row);
    }

    Ok(result)
}

// convert response body text to rows
fn resp_to_rows(obj: &str, resp: &JsonValue, tgt_cols: &[Column]) -> FirebaseFdwResult<Vec<Row>> {
    match obj {
        "auth/users" => body_to_rows(
            resp,
            "users",
            vec![
                ("localId", "uid", "string"),
                ("email", "email", "string"),
                ("createdAt", "created_at", "timestamp"),
            ],
            tgt_cols,
        ),
        _ => {
            // match firestore documents
            if obj.starts_with("firestore/") {
                body_to_rows(
                    resp,
                    "documents",
                    vec![
                        ("name", "name", "string"),
                        ("fields", "fields", "json"),
                        ("createTime", "created_at", "timestamp_iso"),
                        ("updateTime", "updated_at", "timestamp_iso"),
                    ],
                    tgt_cols,
                )
            } else {
                Err(FirebaseFdwError::ObjectNotImplemented(obj.to_string()))
            }
        }
    }
}

#[wrappers_fdw(
    version = "0.1.3",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/firebase_fdw",
    error_type = "FirebaseFdwError"
)]
pub(crate) struct FirebaseFdw {
    rt: Runtime,
    project_id: String,
    client: Option<ClientWithMiddleware>,
    scan_result: Vec<Row>,
}

impl FirebaseFdw {
    const FDW_NAME: &'static str = "FirebaseFdw";

    const DEFAULT_AUTH_BASE_URL: &'static str =
        "https://identitytoolkit.googleapis.com/v1/projects";
    const DEFAULT_FIRESTORE_BASE_URL: &'static str =
        "https://firestore.googleapis.com/v1beta1/projects";

    // maximum allowed page size
    // https://firebase.google.com/docs/reference/admin/node/firebase-admin.auth.baseauth.md#baseauthlistusers
    const PAGE_SIZE: usize = 1000;

    // default maximum row count limit
    const DEFAULT_ROWS_LIMIT: usize = 10_000;

    fn build_url(
        &self,
        obj: &str,
        next_page: &Option<String>,
        options: &HashMap<String, String>,
    ) -> String {
        match obj {
            "auth/users" => {
                // ref: https://firebase.google.com/docs/reference/admin/node/firebase-admin.auth.baseauth.md#baseauthlistusers
                let base_url = options
                    .get("base_url")
                    .map(|t| t.to_owned())
                    .unwrap_or_else(|| Self::DEFAULT_AUTH_BASE_URL.to_owned());
                let mut ret = format!(
                    "{}/{}/accounts:batchGet?maxResults={}",
                    base_url,
                    self.project_id,
                    Self::PAGE_SIZE,
                );
                if let Some(next_page_token) = next_page {
                    ret.push_str(&format!("&nextPageToken={}", next_page_token));
                }
                ret
            }
            _ => {
                // match for firestore documents
                // ref: https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases.documents/listDocuments
                let re = Regex::new(r"^firestore/(?P<collection>.+)").expect("regex is valid");
                if let Some(caps) = re.captures(obj) {
                    let base_url =
                        require_option_or("base_url", options, Self::DEFAULT_FIRESTORE_BASE_URL);
                    let collection = caps
                        .name("collection")
                        .expect("`collection` capture group always exists in a match")
                        .as_str();
                    let mut ret = format!(
                        "{}/{}/databases/(default)/documents/{}?pageSize={}",
                        base_url,
                        self.project_id,
                        collection,
                        Self::PAGE_SIZE,
                    );
                    if let Some(next_page_token) = next_page {
                        ret.push_str(&format!("&pageToken={}", next_page_token));
                    }
                    return ret;
                }

                "".to_string()
            }
        }
    }
}

impl ForeignDataWrapper<FirebaseFdwError> for FirebaseFdw {
    fn new(options: &HashMap<String, String>) -> FirebaseFdwResult<Self> {
        let mut ret = Self {
            rt: create_async_runtime()?,
            project_id: require_option("project_id", options)?.to_string(),
            client: None,
            scan_result: Vec::default(),
        };

        // get oauth2 access token if it is directly defined in options
        let token = if let Some(access_token) = options.get("access_token") {
            access_token.to_owned()
        } else {
            // otherwise, get it from the options or Vault
            let sa_key = match options.get("sa_key") {
                Some(sa_key) => sa_key.to_owned(),
                None => {
                    let sa_key_id = require_option("sa_key_id", options)?;
                    match get_vault_secret(sa_key_id) {
                        Some(sa_key) => sa_key,
                        None => return Ok(ret),
                    }
                }
            };
            let access_token = get_oauth2_token(&sa_key, &ret.rt)?;
            access_token
                .token()
                .map(|t| t.to_owned())
                .ok_or(FirebaseFdwError::NoTokenFound(access_token))?
        };

        // create client
        let mut headers = header::HeaderMap::new();
        let value = format!("Bearer {}", token);
        let mut auth_value = header::HeaderValue::from_str(&value)
            .map_err(|_| FirebaseFdwError::InvalidApiKeyHeader)?;
        auth_value.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth_value);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        ret.client = Some(client);

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(ret)
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> FirebaseFdwResult<()> {
        let obj = require_option("object", options)?;
        let row_cnt_limit = options
            .get("limit")
            .map(|n| n.parse::<usize>())
            .transpose()?
            .unwrap_or(Self::DEFAULT_ROWS_LIMIT);

        self.scan_result = Vec::new();

        if let Some(client) = &self.client {
            let mut next_page: Option<String> = None;
            let mut result = Vec::new();

            loop {
                let url = self.build_url(obj, &next_page, options);

                let body = self.rt.block_on(client.get(&url).send()).and_then(|resp| {
                    stats::inc_stats(
                        Self::FDW_NAME,
                        stats::Metric::BytesIn,
                        resp.content_length().unwrap_or(0) as i64,
                    );

                    resp.error_for_status()
                        .and_then(|resp| self.rt.block_on(resp.text()))
                        .map_err(reqwest_middleware::Error::from)
                })?;

                let json: JsonValue = serde_json::from_str(&body)?;
                let mut rows = resp_to_rows(obj, &json, columns)?;
                result.append(&mut rows);
                if result.len() >= row_cnt_limit {
                    break;
                }

                // get next page token, stop fetching if no more pages
                next_page = json
                    .get("nextPageToken")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_owned());
                if next_page.is_none() {
                    break;
                }
            }

            stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, result.len() as i64);
            stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, result.len() as i64);

            self.scan_result = result;
        }

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> FirebaseFdwResult<Option<()>> {
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

    fn end_scan(&mut self) -> FirebaseFdwResult<()> {
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> FirebaseFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "object")?;
            }
        }

        Ok(())
    }
}
