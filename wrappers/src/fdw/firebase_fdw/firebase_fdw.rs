use pgx::prelude::*;
use pgx::JsonB;
use regex::Regex;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::Value;
use std::collections::HashMap;
use time::{format_description::well_known::Iso8601, PrimitiveDateTime};
use yup_oauth2::AccessToken;
use yup_oauth2::ServiceAccountAuthenticator;

use supabase_wrappers::prelude::*;

macro_rules! report_fetch_error {
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

#[wrappers_meta(
    version = "0.1.0",
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

    pub fn new(options: &HashMap<String, String>) -> Self {
        let mut ret = FirebaseFdw {
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
                    .unwrap_or(Self::DEFAULT_AUTH_BASE_URL.to_owned());
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
                    let base_url = options
                        .get("base_url")
                        .map(|t| t.to_owned())
                        .unwrap_or(Self::DEFAULT_FIRESTORE_BASE_URL.to_owned());
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

    // convert response body text to rows
    fn resp_to_rows(&self, obj: &str, resp: &Value, tgt_cols: &Vec<String>) -> Vec<Row> {
        let mut result = Vec::new();

        match obj {
            "auth/users" => {
                let users = match resp
                    .as_object()
                    .and_then(|v| v.get("users"))
                    .and_then(|v| v.as_array())
                {
                    Some(users) => users,
                    None => return result,
                };

                for user in users {
                    let mut row = Row::new();
                    if tgt_cols.iter().any(|c| c == "uid") {
                        let uid = user
                            .as_object()
                            .and_then(|v| v.get("localId"))
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_owned())
                            .unwrap_or_default();
                        row.push("uid", Some(Cell::String(uid)));
                    }
                    if tgt_cols.iter().any(|c| c == "email") {
                        let email = user
                            .as_object()
                            .and_then(|v| v.get("email"))
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_owned())
                            .unwrap_or_default();
                        row.push("email", Some(Cell::String(email)));
                    }
                    if tgt_cols.iter().any(|c| c == "fields") {
                        let fields = serde_json::from_str(&user.to_string()).unwrap();
                        row.push("fields", Some(Cell::Json(JsonB(fields))));
                    }
                    result.push(row);
                }
            }
            _ => {
                // match firestore documents
                if obj.starts_with("firestore/") {
                    let docs = match resp
                        .as_object()
                        .and_then(|v| v.get("documents"))
                        .and_then(|v| v.as_array())
                    {
                        Some(docs) => docs,
                        None => return result,
                    };

                    for doc in docs {
                        let mut row = Row::new();
                        if tgt_cols.iter().any(|c| c == "name") {
                            let name = doc
                                .as_object()
                                .and_then(|v| v.get("name"))
                                .and_then(|v| v.as_str())
                                .map(|v| v.to_owned())
                                .unwrap_or_default();
                            row.push("name", Some(Cell::String(name)));
                        }
                        if tgt_cols.iter().any(|c| c == "fields") {
                            let fields = doc.pointer("/fields").map(|v| v.clone()).unwrap();
                            row.push("fields", Some(Cell::Json(JsonB(fields))));
                        }
                        if tgt_cols.iter().any(|c| c == "create_time") {
                            let create_ts = doc
                                .as_object()
                                .and_then(|v| v.get("createTime"))
                                .and_then(|v| v.as_str())
                                .map(|v| {
                                    let dt =
                                        PrimitiveDateTime::parse(&v, &Iso8601::DEFAULT).unwrap();
                                    Timestamp::try_from(dt).unwrap()
                                })
                                .unwrap_or(Timestamp::INFINITY);
                            row.push("create_time", Some(Cell::Timestamp(create_ts)));
                        }
                        if tgt_cols.iter().any(|c| c == "update_time") {
                            let update_ts = doc
                                .as_object()
                                .and_then(|v| v.get("updateTime"))
                                .and_then(|v| v.as_str())
                                .map(|v| {
                                    let dt =
                                        PrimitiveDateTime::parse(&v, &Iso8601::DEFAULT).unwrap();
                                    Timestamp::try_from(dt).unwrap()
                                })
                                .unwrap_or(Timestamp::INFINITY);
                            row.push("update_time", Some(Cell::Timestamp(update_ts)));
                        }
                        result.push(row);
                    }
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
}

impl ForeignDataWrapper for FirebaseFdw {
    fn begin_scan(
        &mut self,
        _quals: &Vec<Qual>,
        columns: &Vec<String>,
        _sorts: &Vec<Sort>,
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
                            let json: Value = serde_json::from_str(&body).unwrap();
                            let mut rows = self.resp_to_rows(&obj, &json, columns);
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
                            report_fetch_error!(url, err);
                            break;
                        }
                    },
                    Err(err) => {
                        report_fetch_error!(url, err);
                        break;
                    }
                }
            }

            self.scan_result = Some(result);
        }
    }

    fn iter_scan(&mut self) -> Option<Row> {
        if let Some(ref mut result) = self.scan_result {
            if !result.is_empty() {
                return result.drain(0..1).last();
            }
        }
        None
    }

    fn end_scan(&mut self) {
        self.scan_result.take();
    }
}
