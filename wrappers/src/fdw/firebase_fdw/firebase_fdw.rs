use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::JsonB;
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

macro_rules! report_request_error {
    ($url:ident, $err:ident) => {
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_ERROR,
            &format!("fetch {} failed: {}", $url, $err),
        )
    };
}

fn get_oauth2_token(sa_key: &str, rt: &Runtime) -> Option<AccessToken> {
    let creds = match yup_oauth2::parse_service_account_key(sa_key.as_bytes()) {
        Ok(creds) => creds,
        Err(err) => {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                &format!("parse service account key JSON failed: {}", err),
            );
            return None;
        }
    };
    let sa = match rt.block_on(ServiceAccountAuthenticator::builder(creds).build()) {
        Ok(sa) => sa,
        Err(err) => {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                &format!("invalid service account key: {}", err),
            );
            return None;
        }
    };
    let scopes = &[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/firebase.database",
        "https://www.googleapis.com/auth/firebase.messaging",
        "https://www.googleapis.com/auth/identitytoolkit",
        "https://www.googleapis.com/auth/userinfo.email",
    ];
    match rt.block_on(sa.token(scopes)) {
        Ok(token) => Some(token),
        Err(err) => {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                &format!("get token failed: {}", err),
            );
            None
        }
    }
}

fn body_to_rows(
    resp: &JsonValue,
    obj_key: &str,
    normal_cols: Vec<(&str, &str, &str)>,
    tgt_cols: &[Column],
) -> Vec<Row> {
    let mut result = Vec::new();

    let objs = match resp
        .as_object()
        .and_then(|v| v.get(obj_key))
        .and_then(|v| v.as_array())
    {
        Some(objs) => objs,
        None => return result,
    };

    for obj in objs {
        let mut row = Row::new();

        // extract normal columns
        for tgt_col in tgt_cols {
            if let Some((src_name, col_name, col_type)) =
                normal_cols.iter().find(|(_, c, _)| c == &tgt_col.name)
            {
                let cell = obj
                    .as_object()
                    .and_then(|v| v.get(*src_name))
                    .and_then(|v| match *col_type {
                        "bool" => v.as_bool().map(Cell::Bool),
                        "i64" => v.as_i64().map(Cell::I64),
                        "string" => v.as_str().map(|a| Cell::String(a.to_owned())),
                        "timestamp" => v.as_str().map(|a| {
                            let secs = a.parse::<i64>().unwrap() / 1000;
                            let ts = to_timestamp(secs as f64);
                            Cell::Timestamp(ts.to_utc())
                        }),
                        "timestamp_iso" => v.as_str().map(|a| {
                            let ts = Timestamp::from_str(a).unwrap();
                            Cell::Timestamp(ts)
                        }),
                        "json" => Some(Cell::Json(JsonB(v.clone()))),
                        _ => None,
                    });
                row.push(col_name, cell);
            }
        }

        // put all properties into 'attrs' JSON column
        if tgt_cols.iter().any(|c| &c.name == "attrs") {
            let attrs = serde_json::from_str(&obj.to_string()).unwrap();
            row.push("attrs", Some(Cell::Json(JsonB(attrs))));
        }

        result.push(row);
    }

    result
}

// convert response body text to rows
fn resp_to_rows(obj: &str, resp: &JsonValue, tgt_cols: &[Column]) -> Vec<Row> {
    let mut result = Vec::new();

    match obj {
        "auth/users" => {
            result = body_to_rows(
                resp,
                "users",
                vec![
                    ("localId", "uid", "string"),
                    ("email", "email", "string"),
                    ("createdAt", "created_at", "timestamp"),
                ],
                tgt_cols,
            );
        }
        _ => {
            // match firestore documents
            if obj.starts_with("firestore/") {
                result = body_to_rows(
                    resp,
                    "documents",
                    vec![
                        ("name", "name", "string"),
                        ("fields", "fields", "json"),
                        ("createTime", "created_at", "timestamp_iso"),
                        ("updateTime", "updated_at", "timestamp_iso"),
                    ],
                    tgt_cols,
                );
            } else {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_TABLE_NOT_FOUND,
                    &format!("'{}' object is not implemented", obj),
                );
            }
        }
    }

    result
}

#[wrappers_fdw(
    version = "0.1.1",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/firebase_fdw"
)]
pub(crate) struct FirebaseFdw {
    rt: Runtime,
    project_id: String,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
}

impl FirebaseFdw {
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
                let re = Regex::new(r"^firestore/(?P<collection>[^/]+)").unwrap();
                if let Some(caps) = re.captures(obj) {
                    let base_url = require_option_or(
                        "base_url",
                        options,
                        Self::DEFAULT_FIRESTORE_BASE_URL.to_owned(),
                    );
                    let collection = caps.name("collection").unwrap().as_str();
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

impl ForeignDataWrapper for FirebaseFdw {
    fn new(options: &HashMap<String, String>) -> Self {
        let mut ret = Self {
            rt: create_async_runtime(),
            project_id: "".to_string(),
            client: None,
            scan_result: None,
        };

        ret.project_id = match require_option("project_id", options) {
            Some(project_id) => project_id,
            None => return ret,
        };

        // get oauth2 access token if it is directly defined in options
        let token = if let Some(access_token) = options.get("access_token") {
            access_token.to_owned()
        } else {
            // otherwise, get it from the options or Vault
            let sa_key = match options.get("sa_key") {
                Some(sa_key) => sa_key.to_owned(),
                None => {
                    let sa_key_id = match require_option("sa_key_id", options) {
                        Some(sa_key_id) => sa_key_id,
                        None => return ret,
                    };
                    match get_vault_secret(&sa_key_id) {
                        Some(sa_key) => sa_key,
                        None => return ret,
                    }
                }
            };
            if let Some(access_token) = get_oauth2_token(&sa_key, &ret.rt) {
                access_token.token().map(|t| t.to_owned()).unwrap()
            } else {
                return ret;
            }
        };

        // create client
        let mut headers = header::HeaderMap::new();
        let value = format!("Bearer {}", token);
        let mut auth_value = header::HeaderValue::from_str(&value).unwrap();
        auth_value.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth_value);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
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
        let obj = match require_option("object", options) {
            Some(obj) => obj,
            None => return,
        };
        let row_cnt_limit = options
            .get("limit")
            .map(|n| n.parse::<usize>().unwrap())
            .unwrap_or(Self::DEFAULT_ROWS_LIMIT);

        self.scan_result = None;

        if let Some(client) = &self.client {
            let mut next_page: Option<String> = None;
            let mut result = Vec::new();

            loop {
                let url = self.build_url(&obj, &next_page, options);

                match self.rt.block_on(client.get(&url).send()) {
                    Ok(resp) => match resp.error_for_status() {
                        Ok(resp) => {
                            let body = self.rt.block_on(resp.text()).unwrap();
                            let json: JsonValue = serde_json::from_str(&body).unwrap();
                            let mut rows = resp_to_rows(&obj, &json, columns);
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
                        Err(err) => {
                            report_request_error!(url, err);
                            break;
                        }
                    },
                    Err(err) => {
                        report_request_error!(url, err);
                        break;
                    }
                }
            }

            self.scan_result = Some(result);
        }
    }

    fn iter_scan(&mut self, row: &mut Row) -> Option<()> {
        if let Some(ref mut result) = self.scan_result {
            if !result.is_empty() {
                return result
                    .drain(0..1)
                    .last()
                    .map(|src_row| row.replace_with(src_row));
            }
        }
        None
    }

    fn end_scan(&mut self) {
        self.scan_result.take();
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "object");
            }
        }
    }
}
