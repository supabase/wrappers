use pgx::log::PgSqlErrorCode;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::Value;
use std::collections::HashMap;

use supabase_wrappers::{
    create_async_runtime, get_secret, report_error, require_option, wrappers_meta, Cell,
    ForeignDataWrapper, Limit, Qual, Row, Runtime, Sort,
};

#[wrappers_meta(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/stripe_fdw"
)]
pub(crate) struct StripeFdw {
    rt: Runtime,
    base_url: String,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
}

impl StripeFdw {
    pub fn new(options: &HashMap<String, String>) -> Self {
        let base_url = options
            .get("api_url")
            .map(|t| t.to_owned())
            .unwrap_or("https://api.stripe.com/v1".to_string());
        let client = require_option("api_key_id", options)
            .and_then(|key_id| get_secret(&key_id))
            .and_then(|api_key| {
                let mut headers = header::HeaderMap::new();
                let value = format!("Bearer {}", api_key);
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
                Some(client)
            });

        StripeFdw {
            rt: create_async_runtime(),
            base_url,
            client,
            scan_result: None,
        }
    }

    #[inline]
    fn build_url(&self, path: &str) -> String {
        format!("{}/{}", &self.base_url, path)
    }

    // convert response body text to rows
    fn resp_to_rows(&self, obj: &str, resp_body: &str) -> Vec<Row> {
        let value: Value = serde_json::from_str(resp_body).unwrap();
        let mut result = Vec::new();

        match obj {
            "balance" => {
                let avail = value
                    .as_object()
                    .and_then(|v| v.get("available"))
                    .and_then(|v| v.as_array())
                    .unwrap();
                for a in avail {
                    let mut row = Row::new();
                    let amt = a
                        .as_object()
                        .and_then(|v| v.get("amount"))
                        .and_then(|v| v.as_i64())
                        .unwrap();
                    let currency = a
                        .as_object()
                        .and_then(|v| v.get("currency"))
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_owned())
                        .unwrap();
                    row.push("amount", Some(Cell::I64(amt)));
                    row.push("currency", Some(Cell::String(currency)));
                    result.push(row);
                }
            }
            "customers" => {
                let customers = value
                    .as_object()
                    .and_then(|v| v.get("data"))
                    .and_then(|v| v.as_array())
                    .unwrap();
                for cust in customers {
                    let mut row = Row::new();
                    let id = cust
                        .as_object()
                        .and_then(|v| v.get("id"))
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_owned())
                        .unwrap();
                    let email = cust
                        .as_object()
                        .and_then(|v| v.get("email"))
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_owned())
                        .unwrap();
                    row.push("id", Some(Cell::String(id)));
                    row.push("email", Some(Cell::String(email)));
                    result.push(row);
                }
            }
            "subscriptions" => {
                let subscriptions = value
                    .as_object()
                    .and_then(|v| v.get("data"))
                    .and_then(|v| v.as_array())
                    .unwrap();
                for sub in subscriptions {
                    let mut row = Row::new();
                    let customer_id = sub
                        .as_object()
                        .and_then(|v| v.get("customer"))
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_owned())
                        .unwrap();
                    let currency = sub
                        .as_object()
                        .and_then(|v| v.get("currency"))
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_owned())
                        .unwrap();
                    let current_period_start = sub
                        .as_object()
                        .and_then(|v| v.get("current_period_start"))
                        .and_then(|v| v.as_i64())
                        .map(|v| v.to_owned())
                        .unwrap();
                    let current_period_end = sub
                        .as_object()
                        .and_then(|v| v.get("current_period_end"))
                        .and_then(|v| v.as_i64())
                        .map(|v| v.to_owned())
                        .unwrap();
                    row.push("customer_id", Some(Cell::String(customer_id)));
                    row.push("currency", Some(Cell::String(currency)));
                    row.push(
                        "current_period_start",
                        Some(Cell::I64(current_period_start)),
                    );
                    row.push("current_period_end", Some(Cell::I64(current_period_end)));
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

macro_rules! report_fetch_error {
    ($url:ident, $err:ident) => {
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_ERROR,
            &format!("fetch {} failed: {}", $url, $err),
        )
    };
}

impl ForeignDataWrapper for StripeFdw {
    fn begin_scan(
        &mut self,
        _quals: &Vec<Qual>,
        _columns: &Vec<String>,
        _sorts: &Vec<Sort>,
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) {
        let obj = if let Some(name) = require_option("object", options) {
            name.clone()
        } else {
            return;
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
