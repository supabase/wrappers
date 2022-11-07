use pgx::log::PgSqlErrorCode;
use pgx::JsonB;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::Value;
use std::collections::HashMap;
use supabase_wrappers::{
    create_async_runtime, report_error, require_option, Cell, ForeignDataWrapper, Limit, Qual, Row,
    Runtime, Sort,
};
use yup_oauth2::ServiceAccountAuthenticator;

macro_rules! report_fetch_error {
    ($url:ident, $err:ident) => {
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_ERROR,
            &format!("fetch {} failed: {}", $url, $err),
        )
    };
}

pub(crate) struct FirebaseFdw {
    rt: Runtime,
    project_id: String,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
}

impl FirebaseFdw {
    const BASE_URL: &'static str = "https://identitytoolkit.googleapis.com/v1/projects";

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

        // get Oauth2 access token
        let sa_key_file = match require_option("sa_key_file", options) {
            Some(sa_key_file) => sa_key_file,
            None => return ret,
        };
        let creds = match ret
            .rt
            .block_on(yup_oauth2::read_service_account_key(sa_key_file))
        {
            Ok(creds) => creds,
            Err(err) => {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("read service account key file failed: {}", err),
                );
                return ret;
            }
        };
        let sa = match ret
            .rt
            .block_on(ServiceAccountAuthenticator::builder(creds).build())
        {
            Ok(sa) => sa,
            Err(err) => {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("invalid service account key: {}", err),
                );
                return ret;
            }
        };
        let scopes = &[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/firebase.database",
            "https://www.googleapis.com/auth/firebase.messaging",
            "https://www.googleapis.com/auth/identitytoolkit",
            "https://www.googleapis.com/auth/userinfo.email",
        ];
        let token = match ret.rt.block_on(sa.token(scopes)) {
            Ok(token) => token,
            Err(err) => {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("get token failed: {}", err),
                );
                return ret;
            }
        };

        // create client
        let mut headers = header::HeaderMap::new();
        let value = format!("Bearer {}", token.token().unwrap());
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

    #[inline]
    fn build_url(&self, obj: &str) -> String {
        match obj {
            "users" => format!("{}/{}/accounts:batchGet", Self::BASE_URL, self.project_id),
            _ => "".to_string(),
        }
    }

    // convert response body text to rows
    fn resp_to_rows(&self, obj: &str, resp_body: &str) -> Vec<Row> {
        let value: Value = serde_json::from_str(resp_body).unwrap();
        let mut result = Vec::new();

        match obj {
            "users" => {
                let users = value
                    .as_object()
                    .and_then(|v| v.get("users"))
                    .and_then(|v| v.as_array())
                    .unwrap();
                for user in users {
                    let mut row = Row::new();
                    let local_id = user
                        .as_object()
                        .and_then(|v| v.get("localId"))
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_owned())
                        .unwrap();
                    let email = user
                        .as_object()
                        .and_then(|v| v.get("email"))
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_owned())
                        .unwrap();
                    let props = serde_json::from_str(&user.to_string()).unwrap();
                    row.push("local_id", Some(Cell::String(local_id)));
                    row.push("email", Some(Cell::String(email)));
                    row.push("props", Some(Cell::Json(JsonB(props))));
                    result.push(row);
                }
            }
            _ => report_error(
                PgSqlErrorCode::ERRCODE_FDW_TABLE_NOT_FOUND,
                &format!("'{}' object is not implemented", obj),
            ),
        }

        result
    }
}

impl ForeignDataWrapper for FirebaseFdw {
    fn begin_scan(
        &mut self,
        _quals: &Vec<Qual>,
        _columns: &Vec<String>,
        _sorts: &Vec<Sort>,
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) {
        let obj = match require_option("object", options) {
            Some(obj) => obj,
            None => return,
        };
        let url = self.build_url(&obj);

        if let Some(client) = &self.client {
            match self.rt.block_on(client.get(&url).send()) {
                Ok(resp) => match resp.error_for_status() {
                    Ok(resp) => {
                        let body = self.rt.block_on(resp.text()).unwrap();
                        let result = self.resp_to_rows(&obj, &body);
                        self.scan_result = Some(result);
                    }
                    Err(err) => report_fetch_error!(url, err),
                },
                Err(err) => report_fetch_error!(url, err),
            }
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
