use crate::stats;
use pgrx::{datum::datetime_support::to_timestamp, pg_sys, JsonB};
use reqwest::{self, header, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;

use supabase_wrappers::prelude::*;
use supabase_wrappers::utils::report_info;

use super::{NotionFdwError, NotionFdwResult};

fn create_client(api_key: &str, notion_version: &str) -> NotionFdwResult<ClientWithMiddleware> {
    let mut headers = header::HeaderMap::new();
    let value = format!("Bearer {}", api_key);
    let mut auth_value = header::HeaderValue::from_str(&value)?;
    auth_value.set_sensitive(true);
    headers.insert(header::AUTHORIZATION, auth_value);

    let version_value = header::HeaderValue::from_str(notion_version)?;
    headers.insert("Notion-Version", version_value);

    // Create the basic reqwest client
    let reqwest_client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;

    // Get the retry policy
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);

    // Create the client with middleware
    let client = ClientBuilder::new(reqwest_client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    Ok(client)
}

fn body_to_rows(
    resp_body: &str,
    cols: Vec<(&str, &str)>,
    tgt_cols: &[Column],
) -> NotionFdwResult<(Vec<Row>, Option<String>, Option<bool>)> {
    let mut result = Vec::new();
    let value: JsonValue = serde_json::from_str(resp_body)?;

    let object: &str = value
        .as_object()
        .and_then(|v| v.get("object"))
        .and_then(|v| v.as_str())
        .ok_or(NotionFdwError::InvalidResponse)?;

    let single_wrapped: Vec<JsonValue> = match object {
        "list" => Vec::new(),
        _ => value
            .as_object()
            .map(|v| vec![JsonValue::Object(v.clone())])
            .ok_or(NotionFdwError::InvalidResponse)?,
    };

    let entries = match object {
        "list" => value
            .as_object()
            .and_then(|v| v.get("results"))
            .and_then(|v| v.as_array())
            .ok_or(NotionFdwError::InvalidResponse)?,
        _ => &single_wrapped,
    };

    for entry in entries {
        let mut row = Row::new();

        // Extract columns based on target columns specified
        for tgt_col in tgt_cols {
            if let Some((col_name, col_type)) = cols.iter().find(|(c, _)| c == &tgt_col.name) {
                let cell = entry
                    .as_object()
                    .and_then(|v| v.get(*col_name))
                    .and_then(|v| match *col_type {
                        "bool" => v.as_bool().map(Cell::Bool),
                        "i64" => v.as_i64().map(Cell::I64),
                        "string" => v.as_str().map(|a| Cell::String(a.to_owned())),
                        "timestamp" => v.as_i64().map(|a| {
                            let ts = to_timestamp(a as f64);
                            Cell::Timestamp(ts.to_utc())
                        }),
                        _ => None,
                    });

                row.push(col_name, cell);
            } else {
                // put the rest of the attributes in a JSONB column
                let attrs = serde_json::from_str(&entry.to_string())?;
                row.push(&tgt_col.name, Some(Cell::Json(JsonB(attrs))));
            }
        }
        result.push(row);
    }

    // Handle pagination
    let cursor = value
        .as_object()
        .and_then(|v| v.get("next_cursor"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_owned());

    let has_more = value
        .as_object()
        .and_then(|v| v.get("has_more"))
        .and_then(|v| v.as_bool());

    Ok((result, cursor, has_more))
}

fn pushdown_quals(
    url: &mut Url,
    _obj: &str,
    quals: &[Qual],
    fields: Vec<&str>,
    page_size: i64,
    cursor: &Option<String>,
) {
    // for scan with a single id query param, optimized to single object GET request
    if quals.len() == 1 {
        let qual = &quals[0];
        if qual.field == "id" && qual.operator == "=" && !qual.use_or {
            if let Value::Cell(Cell::String(id)) = &qual.value {
                let new_path = format!("{}/{}", url.path(), id);
                url.set_path(&new_path);
                url.set_query(None);
                return;
            }
        }
    }

    // pushdown quals
    for qual in quals {
        for field in &fields {
            if qual.field == *field && qual.operator == "=" && !qual.use_or {
                if let Value::Cell(cell) = &qual.value {
                    match cell {
                        Cell::Bool(b) => {
                            url.query_pairs_mut()
                                .append_pair(field, b.to_string().as_str());
                        }
                        Cell::String(s) => {
                            url.query_pairs_mut().append_pair(field, s);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    // add pagination parameters except for 'balance' object
    url.query_pairs_mut()
        .append_pair("page_size", &format!("{}", page_size));
    if let Some(ref cursor) = cursor {
        url.query_pairs_mut().append_pair("start_cursor", cursor);
    }
}

// get stats metadata
#[inline]
fn get_stats_metadata() -> JsonB {
    stats::get_metadata(NotionFdw::FDW_NAME).unwrap_or(JsonB(json!({
        "request_cnt": 0i64,
    })))
}

// save stats metadata
#[inline]
fn set_stats_metadata(stats_metadata: JsonB) {
    stats::set_metadata(NotionFdw::FDW_NAME, Some(stats_metadata));
}

// increase stats metadata 'request_cnt' by 1
#[inline]
fn inc_stats_request_cnt(stats_metadata: &mut JsonB) -> NotionFdwResult<()> {
    if let Some(v) = stats_metadata.0.get_mut("request_cnt") {
        *v = (v.as_i64().ok_or(NotionFdwError::InvalidStats(
            "`request_cnt` is not a number".to_string(),
        ))? + 1)
            .into();
    };
    Ok(())
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Romain Graux",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/notion_fdw",
    error_type = "NotionFdwError"
)]
pub(crate) struct NotionFdw {
    rt: Runtime,
    base_url: Url,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
    _obj: String,
    _rowid_col: String,
    iter_idx: usize,
}

impl NotionFdw {
    const FDW_NAME: &'static str = "NotionFdw";

    fn build_url(
        &self,
        obj: &str,
        quals: &[Qual],
        page_size: i64,
        cursor: &Option<String>,
    ) -> NotionFdwResult<Option<Url>> {
        let mut url = self.base_url.join(obj)?;

        // pushdown quals other than id
        // ref: https://notion.com/docs/api/[object]/list
        let fields = match obj {
            "users" => vec![],
            _ => {
                return Err(NotionFdwError::ObjectNotImplemented(format!("{}", obj)));
            }
        };
        pushdown_quals(&mut url, obj, quals, fields, page_size, cursor);

        Ok(Some(url))
    }

    fn resp_to_rows(
        &self,
        obj: &str,
        resp_body: &str,
        tgt_cols: &[Column],
    ) -> NotionFdwResult<(Vec<Row>, Option<String>, Option<bool>)> {
        match obj {
            "users" => body_to_rows(
                resp_body,
                vec![
                    ("id", "string"),
                    ("type", "string"),
                    ("name", "string"),
                    ("object", "string"), // Always "user"
                ],
                tgt_cols,
            ),
            _ => Err(NotionFdwError::ObjectNotImplemented(obj.to_string())),
        }
    }
}

impl ForeignDataWrapper<NotionFdwError> for NotionFdw {
    fn new(options: &HashMap<String, String>) -> NotionFdwResult<Self> {
        let base_url = options
            .get("api_url")
            .map(|t| t.to_owned())
            // Ensure trailing slash is always present, otherwise /v1 will get obliterated when
            // joined with object
            .map(|s| {
                if s.ends_with('/') {
                    s
                } else {
                    format!("{}/", s)
                }
            })
            .unwrap_or_else(|| "https://api.notion.com/v1/".to_string());

        let notion_version = options
            .get("notion_version")
            .map(|t| t.to_owned())
            .unwrap_or_else(|| "2022-06-28".to_string());

        let client = match options.get("api_key") {
            Some(api_key) => Some(create_client(api_key, &notion_version)),
            None => {
                let key_id = require_option("api_key_id", options)?;
                get_vault_secret(key_id).map(|api_key| create_client(&api_key, &notion_version))
            }
        }
        .transpose()?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(NotionFdw {
            rt: create_async_runtime()?,
            base_url: Url::parse(&base_url)?,
            client,
            scan_result: None,
            _obj: String::default(),
            _rowid_col: String::default(),
            iter_idx: 0,
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> NotionFdwResult<()> {
        let obj = require_option("object", options)?;

        self.iter_idx = 0;

        if let Some(client) = &self.client {
            let page_size = 100; // maximum page size limit for Notion API
            let page_cnt = if let Some(limit) = limit {
                if limit.count == 0 {
                    return Ok(());
                }
                (limit.offset + limit.count) / page_size + 1
            } else {
                // if no limit specified, fetch all records
                i64::MAX
            };
            let mut page = 0;
            let mut result = Vec::new();
            let mut cursor: Option<String> = None;
            let mut stats_metadata = get_stats_metadata();

            while page < page_cnt {
                let url = self.build_url(obj, quals, page_size, &cursor)?;
                let Some(url) = url else {
                    return Ok(());
                };

                inc_stats_request_cnt(&mut stats_metadata)?;

                let body = self.rt.block_on(client.get(url).send()).and_then(|resp| {
                    stats::inc_stats(
                        Self::FDW_NAME,
                        stats::Metric::BytesIn,
                        resp.content_length().unwrap_or(0) as i64,
                    );

                    resp.error_for_status()
                        .and_then(|resp| self.rt.block_on(resp.text()))
                        .map_err(reqwest_middleware::Error::from)
                })?;

                if body.is_empty() {
                    break;
                }

                // convert response body to rows
                let (rows, starting_after, has_more) = self.resp_to_rows(obj, &body, columns)?;
                if rows.is_empty() {
                    break;
                }
                result.extend(rows);
                match has_more {
                    Some(has_more) => {
                        if !has_more {
                            break;
                        }
                        // Otherwise, continue
                    }
                    None => break,
                }
                cursor = starting_after;

                page += 1;
            }

            // save stats
            stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, result.len() as i64);
            stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, result.len() as i64);

            set_stats_metadata(stats_metadata);

            self.scan_result = Some(result);
        }

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> NotionFdwResult<Option<()>> {
        report_info(format!("Iter scan for object ({})", self._obj).as_str());
        if let Some(ref mut result) = self.scan_result {
            if self.iter_idx < result.len() {
                row.replace_with(result[self.iter_idx].clone());
                self.iter_idx += 1;
                return Ok(Some(()));
            }
        }
        Ok(None)
    }

    fn re_scan(&mut self) -> NotionFdwResult<()> {
        self.iter_idx = 0;
        Ok(())
    }

    fn end_scan(&mut self) -> NotionFdwResult<()> {
        self.scan_result.take();
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> NotionFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "object")?;
            }
        }

        Ok(())
    }
}
