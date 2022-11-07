use pgx::log::PgSqlErrorCode;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

use supabase_wrappers::{
    create_async_runtime, report_error, require_option, Cell, ForeignDataWrapper, Limit, Qual, Row,
    Runtime, Sort,
};

pub(crate) struct AirtableFdw {
    rt: Runtime,
    base_url: String,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
}

#[derive(Deserialize)]
struct AirtableResponse {
    records: Vec<AirtableRecord>,
}

#[derive(Deserialize)]
struct AirtableRecord {
    id: String,
    fields: HashMap<String, Value>,
    // XXX Incorporate the createdTime field?
}

impl AirtableRecord {
    fn value_to_cell(value: &Value) -> Option<Cell> {
        use serde_json::Value::*;
        match value {
            Null => None,
            Bool(v) => Some(Cell::Bool(*v)),
            Number(n) => n.as_i64().map_or_else(
                || n.as_f64().map_or(None, |v| Some(Cell::F64(v))),
                |v| Some(Cell::I64(v)),
            ),
            String(v) => Some(Cell::String(v.clone())),
            // XXX Handle timestamps somehow...
            // XXX Fix (probably map to JsonB)
            _ => panic!("Unsupported: Array/Object"),
        }
    }

    fn to_row(&self) -> Row {
        let mut row = Row::new();
        row.push("id", Some(Cell::String(self.id.clone())));
        row.push("name", Some(Cell::String("foo".to_string())));
        /*
        for (key, value) in self.fields.iter() {
            row.push(key, AirtableRecord::value_to_cell(value));
        }
        */
        row
    }
}

impl AirtableFdw {
    pub fn new(options: &HashMap<String, String>) -> Self {
        let base_url = options
            .get("api_url")
            .map(|t| t.to_owned())
            .unwrap_or("https://api.airtable.com/v0/app4PDEzNrArJdQ5k".to_string())
            .trim_end_matches('/')
            .to_owned();

        // TODO: Support a cache
        let client = require_option("api_key", options).map(|api_key| {
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
            client
        });

        AirtableFdw {
            rt: create_async_runtime(),
            base_url,
            client,
            scan_result: None,
        }
    }

    #[inline]
    fn build_url(&self, base_id: &str, table_name: &str) -> String {
        // XXX: Test with table that has a space in the name (do we need to encode here?)
        format!("{}/{}/{}", &self.base_url, base_id, table_name)
    }

    // convert response body text to rows
    fn resp_to_rows(&self, resp_body: &str) -> Vec<Row> {
        let response: AirtableResponse = serde_json::from_str(resp_body).unwrap();
        let mut result = Vec::new();

        for record in response.records.iter() {
            result.push(record.to_row());
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

impl ForeignDataWrapper for AirtableFdw {
    fn begin_scan(
        &mut self,
        // TODO: We should be able to propagate some of these through to airtable
        _quals: &Vec<Qual>,
        _columns: &Vec<String>,
        _sorts: &Vec<Sort>,
        _limit: &Option<Limit>, // TODO: maxRecords
        options: &HashMap<String, String>,
    ) {
        // TODO: Support specifying other options (view, maxRecords)
        let url = if let Some(url) = require_option("base_id", options).and_then(|base_id| {
            require_option("table", options).map(|table| self.build_url(&base_id, &table))
        }) {
            url
        } else {
            // XXX Should we report an error here? It doesn't seem like the Stripe one does
            // if object is empty
            return;
        };

        // XXX Implement pagination
        if let Some(client) = &self.client {
            match self.rt.block_on(client.get(&url).send()) {
                Ok(resp) => match resp.error_for_status() {
                    Ok(resp) => {
                        let body = self.rt.block_on(resp.text()).unwrap();
                        let result = self.resp_to_rows(&body);
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
