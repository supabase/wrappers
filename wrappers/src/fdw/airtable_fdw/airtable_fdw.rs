use crate::stats;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::collections::HashMap;
use url::Url;

use supabase_wrappers::prelude::*;

use super::result::AirtableResponse;

fn create_client(api_key: &str) -> ClientWithMiddleware {
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
    ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

#[wrappers_fdw(
    version = "0.1.2",
    author = "Ankur Goyal",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/airtable_fdw"
)]
pub(crate) struct AirtableFdw {
    rt: Runtime,
    client: Option<ClientWithMiddleware>,
    base_url: String,
    scan_result: Option<Vec<Row>>,
}

impl AirtableFdw {
    const FDW_NAME: &str = "AirtableFdw";

    #[inline]
    fn build_url(&self, base_id: &str, table_id: &str, view_id: Option<&String>) -> String {
        match view_id {
            Some(view_id) => format!(
                "{}/{}/{}?view={}",
                &self.base_url, base_id, table_id, view_id
            ),
            None => format!("{}/{}/{}", &self.base_url, base_id, table_id),
        }
    }

    #[inline]
    fn set_limit_offset(
        &self,
        url: &str,
        page_size: Option<usize>,
        offset: Option<&str>,
    ) -> Result<String, url::ParseError> {
        let mut params = Vec::new();
        if let Some(page_size) = page_size {
            params.push(("pageSize", format!("{}", page_size)));
        }
        if let Some(offset) = offset {
            params.push(("offset", offset.to_string()));
        }

        Url::parse_with_params(url, &params).map(|x| x.into())
    }

    // convert response body text to rows
    fn parse_resp(&self, resp_body: &str, columns: &[Column]) -> (Vec<Row>, Option<String>) {
        let response: AirtableResponse = serde_json::from_str(resp_body).unwrap();
        let mut result = Vec::new();

        for record in response.records.iter() {
            result.push(record.to_row(columns));
        }

        (result, response.offset)
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

enum AirtableFdwError {}

impl From<AirtableFdwError> for ErrorReport {
    fn from(_value: AirtableFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, "", "")
    }
}

// TODO Add support for INSERT, UPDATE, DELETE
impl ForeignDataWrapper<AirtableFdwError> for AirtableFdw {
    fn new(options: &HashMap<String, String>) -> Result<Self, AirtableFdwError> {
        let base_url = options
            .get("api_url")
            .map(|t| t.to_owned())
            .unwrap_or_else(|| "https://api.airtable.com/v0".to_string());

        let client = match options.get("api_key") {
            Some(api_key) => Some(create_client(api_key)),
            None => require_option("api_key_id", options)
                .and_then(|key_id| get_vault_secret(&key_id))
                .map(|api_key| create_client(&api_key)),
        };

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(Self {
            rt: create_async_runtime(),
            client,
            base_url,
            scan_result: None,
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual], // TODO: Propagate filters
        columns: &[Column],
        _sorts: &[Sort],        // TODO: Propagate sort
        _limit: &Option<Limit>, // TODO: maxRecords
        options: &HashMap<String, String>,
    ) -> Result<(), AirtableFdwError> {
        let url = if let Some(url) = require_option("base_id", options).and_then(|base_id| {
            require_option("table_id", options)
                .map(|table_id| self.build_url(&base_id, &table_id, options.get("view_id")))
        }) {
            url
        } else {
            return Ok(());
        };

        let mut rows = Vec::new();
        if let Some(client) = &self.client {
            let mut offset: Option<String> = None;

            loop {
                // Fetch all of the rows upfront. Arguably, this could be done in batches (and invoked each
                // time iter_scan() runs out of rows) to pipeline the I/O, but we'd have to manage more
                // state so starting with the simpler solution.
                let url = match self.set_limit_offset(&url, None, offset.as_deref()) {
                    Ok(url) => url,
                    Err(err) => {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            &format!("internal error: {}", err),
                        );
                        return Ok(());
                    }
                };

                match self.rt.block_on(client.get(&url).send()) {
                    Ok(resp) => match resp.error_for_status() {
                        Ok(resp) => {
                            stats::inc_stats(
                                Self::FDW_NAME,
                                stats::Metric::BytesIn,
                                resp.content_length().unwrap_or(0) as i64,
                            );
                            let body = self.rt.block_on(resp.text()).unwrap();
                            let (new_rows, new_offset) = self.parse_resp(&body, columns);
                            rows.extend(new_rows.into_iter());

                            if let Some(new_offset) = new_offset {
                                offset = Some(new_offset);
                            } else {
                                break;
                            }
                        }
                        Err(err) => report_fetch_error!(url, err),
                    },
                    Err(err) => report_fetch_error!(url, err),
                }
            }
        }

        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, rows.len() as i64);
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, rows.len() as i64);

        self.scan_result = Some(rows);
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, AirtableFdwError> {
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

    fn end_scan(&mut self) -> Result<(), AirtableFdwError> {
        self.scan_result.take();
        Ok(())
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "base_id");
                check_options_contain(&options, "table_id");
            }
        }
    }
}
