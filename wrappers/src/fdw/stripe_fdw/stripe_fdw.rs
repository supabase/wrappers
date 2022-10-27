use pgx::log::{elog, PgLogLevel};
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::Value;
use std::collections::HashMap;

use supabase_wrappers::{create_async_runtime, Cell, ForeignDataWrapper, Limit, Qual, Row, Runtime, Sort};

pub(crate) struct StripeFdw {
    rt: Runtime,
    client: ClientWithMiddleware,
    scan_result: Option<Vec<Row>>,
}

impl StripeFdw {
    const BASE_URL: &'static str = "https://api.stripe.com/v1";

    pub fn new(options: &HashMap<String, String>) -> Self {
        let api_key = options.get("api_key").map(|k| k.to_owned()).unwrap();

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

        StripeFdw {
            rt: create_async_runtime(),
            client,
            scan_result: None,
        }
    }

    fn build_url(&self, path: &str) -> String {
        format!("{}/{}", Self::BASE_URL, path)
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
            _ => elog(
                PgLogLevel::ERROR,
                &format!("'{}' object is not implemented", obj),
            ),
        }

        result
    }
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
        let obj = options.get("object").map(|k| k.to_owned()).unwrap();
        let url = self.build_url(&obj);

        match self.rt.block_on(self.client.get(&url).send()) {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp) => {
                    let body = self.rt.block_on(resp.text()).unwrap();
                    let result = self.resp_to_rows(&obj, &body);
                    self.scan_result = Some(result);
                }
                Err(err) => elog(PgLogLevel::ERROR, &format!("fetch {} failed: {}", url, err)),
            },
            Err(err) => elog(PgLogLevel::ERROR, &format!("fetch {} failed: {}", url, err)),
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
