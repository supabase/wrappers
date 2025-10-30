use crate::stats;
use pgrx::{datum::datetime_support::to_timestamp, pg_sys, JsonB};
use reqwest::{self, header, StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::{json, Map as JsonMap, Number, Value as JsonValue};
use std::collections::{HashMap, HashSet};

use supabase_wrappers::prelude::*;

use super::{StripeFdwError, StripeFdwResult};

// config list for all the supported foreign tables
// an example:
// {
//   "invoices": (
//     // foreign table name
//     "invoices",
//
//     // supported pushdown columns other than id
//     ["customer", "status", "subscription"],
//
//     // foreign table column tuples: (column_name, type)
//     [
//       ("id", "text"),
//       ("customer", "text"),
//       ("subscription", "text"),
//       ("status", "text"),
//     ]
//   ),
// }
type TableConfig = HashMap<
    &'static str,
    (
        &'static str,
        Vec<&'static str>,
        Vec<(&'static str, &'static str)>,
    ),
>;

fn create_table_config() -> TableConfig {
    HashMap::from([
        (
            "accounts",
            (
                "accounts",
                vec![],
                vec![
                    ("id", "text"),
                    ("business_type", "text"),
                    ("country", "text"),
                    ("email", "text"),
                    ("type", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "balance",
            (
                "balance",
                vec![],
                vec![
                    ("balance_type", "text"),
                    ("amount", "bigint"),
                    ("currency", "text"),
                ],
            ),
        ),
        (
            "balance_transactions",
            (
                "balance_transactions",
                vec!["type"],
                vec![
                    ("id", "text"),
                    ("amount", "bigint"),
                    ("currency", "text"),
                    ("description", "text"),
                    ("fee", "bigint"),
                    ("net", "bigint"),
                    ("status", "text"),
                    ("type", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "charges",
            (
                "charges",
                vec!["customer"],
                vec![
                    ("id", "text"),
                    ("amount", "bigint"),
                    ("currency", "text"),
                    ("customer", "text"),
                    ("description", "text"),
                    ("invoice", "text"),
                    ("payment_intent", "text"),
                    ("status", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "customers",
            (
                "customers",
                vec!["email"],
                vec![
                    ("id", "text"),
                    ("email", "text"),
                    ("name", "text"),
                    ("description", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "disputes",
            (
                "disputes",
                vec!["charge", "payment_intent"],
                vec![
                    ("id", "text"),
                    ("amount", "bigint"),
                    ("currency", "text"),
                    ("charge", "text"),
                    ("payment_intent", "text"),
                    ("reason", "text"),
                    ("status", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "events",
            (
                "events",
                vec!["type"],
                vec![
                    ("id", "text"),
                    ("type", "text"),
                    ("api_version", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "files",
            (
                "files",
                vec!["purpose"],
                vec![
                    ("id", "text"),
                    ("filename", "text"),
                    ("purpose", "text"),
                    ("title", "text"),
                    ("size", "bigint"),
                    ("type", "text"),
                    ("url", "text"),
                    ("created", "timestamp"),
                    ("expires_at", "timestamp"),
                ],
            ),
        ),
        (
            "file_links",
            (
                "file_links",
                vec![],
                vec![
                    ("id", "text"),
                    ("file", "text"),
                    ("url", "text"),
                    ("created", "timestamp"),
                    ("expired", "bool"),
                    ("expires_at", "timestamp"),
                ],
            ),
        ),
        (
            "invoices",
            (
                "invoices",
                vec!["customer", "status", "subscription"],
                vec![
                    ("id", "text"),
                    ("customer", "text"),
                    ("subscription", "text"),
                    ("status", "text"),
                    ("total", "bigint"),
                    ("currency", "text"),
                    ("period_start", "timestamp"),
                    ("period_end", "timestamp"),
                ],
            ),
        ),
        (
            "mandates",
            (
                "mandates",
                vec![],
                vec![
                    ("id", "text"),
                    ("payment_method", "text"),
                    ("status", "text"),
                    ("type", "text"),
                ],
            ),
        ),
        (
            "payment_intents",
            (
                "payment_intents",
                vec!["customer"],
                vec![
                    ("id", "text"),
                    ("customer", "text"),
                    ("amount", "bigint"),
                    ("currency", "text"),
                    ("payment_method", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "payouts",
            (
                "payouts",
                vec!["status"],
                vec![
                    ("id", "text"),
                    ("amount", "bigint"),
                    ("currency", "text"),
                    ("arrival_date", "timestamp"),
                    ("description", "text"),
                    ("statement_descriptor", "text"),
                    ("status", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "prices",
            (
                "prices",
                vec!["active", "currency", "product", "type"],
                vec![
                    ("id", "text"),
                    ("active", "bool"),
                    ("currency", "text"),
                    ("product", "text"),
                    ("unit_amount", "bigint"),
                    ("type", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "products",
            (
                "products",
                vec!["active"],
                vec![
                    ("id", "text"),
                    ("name", "text"),
                    ("active", "bool"),
                    ("default_price", "text"),
                    ("description", "text"),
                    ("created", "timestamp"),
                    ("updated", "timestamp"),
                ],
            ),
        ),
        (
            "refunds",
            (
                "refunds",
                vec!["charge", "payment_intent"],
                vec![
                    ("id", "text"),
                    ("amount", "bigint"),
                    ("currency", "text"),
                    ("charge", "text"),
                    ("payment_intent", "text"),
                    ("reason", "text"),
                    ("status", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "setup_attempts",
            (
                "setup_attempts",
                vec!["setup_intent"],
                vec![
                    ("id", "text"),
                    ("application", "text"),
                    ("customer", "text"),
                    ("on_behalf_of", "text"),
                    ("payment_method", "text"),
                    ("setup_intent", "text"),
                    ("status", "text"),
                    ("usage", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "setup_intents",
            (
                "setup_intents",
                vec!["customer", "payment_method"],
                vec![
                    ("id", "text"),
                    ("client_secret", "text"),
                    ("customer", "text"),
                    ("description", "text"),
                    ("payment_method", "text"),
                    ("status", "text"),
                    ("usage", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "subscriptions",
            (
                "subscriptions",
                vec!["customer", "price", "status"],
                vec![
                    ("id", "text"),
                    ("customer", "text"),
                    ("currency", "text"),
                    ("current_period_start", "timestamp"),
                    ("current_period_end", "timestamp"),
                ],
            ),
        ),
        (
            "tokens",
            (
                "tokens",
                vec![],
                vec![
                    ("id", "text"),
                    ("type", "text"),
                    ("client_ip", "text"),
                    ("used", "bool"),
                    ("livemode", "bool"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "topups",
            (
                "topups",
                vec!["status"],
                vec![
                    ("id", "text"),
                    ("amount", "bigint"),
                    ("currency", "text"),
                    ("description", "text"),
                    ("status", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "transfers",
            (
                "transfers",
                vec!["destination"],
                vec![
                    ("id", "text"),
                    ("amount", "bigint"),
                    ("currency", "text"),
                    ("description", "text"),
                    ("destination", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
        (
            "billing/meters",
            (
                "billing_meters",
                vec![],
                vec![
                    ("id", "text"),
                    ("display_name", "text"),
                    ("event_name", "text"),
                    ("event_time_window", "text"),
                    ("status", "text"),
                ],
            ),
        ),
        (
            "checkout/sessions",
            (
                "checkout_sessions",
                vec!["customer", "payment_intent", "subscription"],
                vec![
                    ("id", "text"),
                    ("customer", "text"),
                    ("payment_intent", "text"),
                    ("subscription", "text"),
                    ("created", "timestamp"),
                ],
            ),
        ),
    ])
}

fn create_client(
    api_key: &str,
    api_version: Option<&str>,
) -> StripeFdwResult<ClientWithMiddleware> {
    let mut headers = header::HeaderMap::new();
    let value = format!("Bearer {api_key}");
    let mut auth_value = header::HeaderValue::from_str(&value)?;
    auth_value.set_sensitive(true);
    headers.insert(header::AUTHORIZATION, auth_value);
    if let Some(version) = api_version {
        headers.insert("Stripe-Version", header::HeaderValue::from_str(version)?);
    }
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    Ok(ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build())
}

fn body_to_rows(
    resp_body: &str,
    normal_cols: Vec<(&str, &str)>,
    tgt_cols: &[Column],
) -> StripeFdwResult<(Vec<Row>, Option<String>, Option<bool>)> {
    let mut result = Vec::new();
    let value: JsonValue = serde_json::from_str(resp_body)?;
    let is_list = value
        .as_object()
        .and_then(|v| v.get("object"))
        .and_then(|v| v.as_str())
        .map(|v| v == "list")
        .unwrap_or_default();

    let is_balance = value
        .as_object()
        .and_then(|v| v.get("object"))
        .and_then(|v| v.as_str())
        .map(|v| v == "balance")
        .unwrap_or_default();

    let single_wrapped: Vec<JsonValue> = if is_list {
        Vec::new()
    } else if is_balance {
        // specially transform balance object to 2 rows
        let mut ret = Vec::new();
        for bal_type in &["available", "pending"] {
            let mut obj = value
                .as_object()
                .and_then(|v| v.get(*bal_type))
                .and_then(|v| v.as_array())
                .map(|v| v[0].as_object().ok_or(StripeFdwError::InvalidResponse))
                .transpose()?
                .ok_or(StripeFdwError::InvalidResponse)?
                .clone();
            obj.insert(
                "balance_type".to_string(),
                JsonValue::String(bal_type.to_string()),
            );
            ret.push(JsonValue::Object(obj));
        }
        ret
    } else {
        // wrap a single object in vec
        value
            .as_object()
            .map(|v| vec![JsonValue::Object(v.clone())])
            .ok_or(StripeFdwError::InvalidResponse)?
    };
    let objs = if is_list {
        value
            .as_object()
            .and_then(|v| v.get("data"))
            .and_then(|v| v.as_array())
            .ok_or(StripeFdwError::InvalidResponse)?
    } else {
        &single_wrapped
    };
    let mut cursor: Option<String> = None;

    for obj in objs {
        let mut row = Row::new();

        // extract normal columns
        for tgt_col in tgt_cols {
            if let Some((col_name, col_type)) = normal_cols.iter().find(|(c, _)| c == &tgt_col.name)
            {
                let cell = obj
                    .as_object()
                    .and_then(|v| v.get(*col_name))
                    .and_then(|v| match *col_type {
                        "bool" => v.as_bool().map(Cell::Bool),
                        "bigint" => v.as_i64().map(Cell::I64),
                        "text" => v.as_str().map(|a| Cell::String(a.to_owned())),
                        "timestamp" => v.as_i64().map(|a| {
                            let ts = to_timestamp(a as f64);
                            Cell::Timestamp(ts.to_utc())
                        }),
                        _ => None,
                    });
                row.push(col_name, cell);
            } else if &tgt_col.name == "attrs" {
                // put all properties into 'attrs' JSON column
                let attrs = serde_json::from_str(&obj.to_string())?;
                row.push("attrs", Some(Cell::Json(JsonB(attrs))));
            }
        }

        result.push(row);
    }

    // get last object's id as cursor
    if let Some(last_obj) = objs.last() {
        cursor = last_obj
            .as_object()
            .and_then(|v| v.get("id"))
            .and_then(|v| v.as_str())
            .map(|v| v.to_owned());
    }

    // get 'has_more' attribute
    let has_more = value
        .as_object()
        .and_then(|v| v.get("has_more"))
        .and_then(|v| v.as_bool());

    Ok((result, cursor, has_more))
}

fn row_to_body(row: &Row) -> StripeFdwResult<JsonValue> {
    let mut map = JsonMap::new();

    for (col_name, cell) in row.iter() {
        if let Some(cell) = cell {
            let col_name = col_name.to_owned();

            match cell {
                Cell::Bool(v) => {
                    map.insert(col_name, JsonValue::Bool(*v));
                }
                Cell::I64(v) => {
                    map.insert(col_name, JsonValue::Number(Number::from(*v)));
                }
                Cell::String(v) => {
                    map.insert(col_name, JsonValue::String(v.to_string()));
                }
                Cell::Json(v) => {
                    if col_name == "attrs" {
                        if let Some(m) = v.0.clone().as_object_mut() {
                            map.append(m)
                        }
                    }
                }
                _ => {
                    return Err(StripeFdwError::UnsupportedColumnType(format!(
                        "{col_name:?}"
                    )));
                }
            }
        }
    }

    Ok(JsonValue::Object(map))
}

fn pushdown_quals(
    url: &mut Url,
    obj: &str,
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
    if obj != "balance" {
        url.query_pairs_mut()
            .append_pair("limit", &format!("{page_size}"));
        if let Some(ref cursor) = cursor {
            url.query_pairs_mut().append_pair("starting_after", cursor);
        }
    }
}

// get stats metadata
#[inline]
fn get_stats_metadata() -> JsonB {
    stats::get_metadata(StripeFdw::FDW_NAME).unwrap_or(JsonB(json!({
        "request_cnt": 0i64,
    })))
}

// save stats metadata
#[inline]
fn set_stats_metadata(stats_metadata: JsonB) {
    stats::set_metadata(StripeFdw::FDW_NAME, Some(stats_metadata));
}

// increase stats metadata 'request_cnt' by 1
#[inline]
fn inc_stats_request_cnt(stats_metadata: &mut JsonB) -> StripeFdwResult<()> {
    if let Some(v) = stats_metadata.0.get_mut("request_cnt") {
        *v = (v.as_i64().ok_or(StripeFdwError::InvalidStats(
            "`request_cnt` is not a number".to_string(),
        ))? + 1)
            .into();
    };
    Ok(())
}

#[wrappers_fdw(
    version = "0.1.12",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/stripe_fdw",
    error_type = "StripeFdwError"
)]
pub(crate) struct StripeFdw {
    rt: Runtime,
    base_url: Url,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
    obj: String,
    rowid_col: String,
    iter_idx: usize,
    table_config: TableConfig,
}

impl StripeFdw {
    const FDW_NAME: &'static str = "StripeFdw";

    fn build_url(
        &self,
        obj: &str,
        quals: &[Qual],
        page_size: i64,
        cursor: &Option<String>,
    ) -> StripeFdwResult<Option<Url>> {
        let mut url = self.base_url.join(obj)?;

        // pushdown quals other than id
        // ref: https://stripe.com/docs/api/[object]/list
        let fields = self
            .table_config
            .get(obj)
            .map(|v| v.1.clone())
            .ok_or_else(|| StripeFdwError::ObjectNotImplemented(obj.to_string()))?;
        pushdown_quals(&mut url, obj, quals, fields, page_size, cursor);

        Ok(Some(url))
    }

    // convert response body text to rows
    fn resp_to_rows(
        &self,
        obj: &str,
        resp_body: &str,
        tgt_cols: &[Column],
    ) -> StripeFdwResult<(Vec<Row>, Option<String>, Option<bool>)> {
        let cols = self
            .table_config
            .get(obj)
            .map(|v| v.2.clone())
            .ok_or_else(|| StripeFdwError::ObjectNotImplemented(obj.to_string()))?;
        body_to_rows(resp_body, cols, tgt_cols)
    }
}

impl ForeignDataWrapper<StripeFdwError> for StripeFdw {
    fn new(server: ForeignServer) -> StripeFdwResult<Self> {
        let base_url = server
            .options
            .get("api_url")
            .map(|t| t.to_owned())
            // Ensure trailing slash is always present, otherwise /v1 will get obliterated when
            // joined with object
            .map(|s| if s.ends_with('/') { s } else { format!("{s}/") })
            .unwrap_or_else(|| "https://api.stripe.com/v1/".to_string());
        let api_version = server.options.get("api_version").map(|t| t.as_str());
        let client = match server.options.get("api_key") {
            Some(api_key) => Some(create_client(api_key, api_version)),
            None => server
                .options
                .get("api_key_id")
                .and_then(|key_id| get_vault_secret(key_id))
                .or_else(|| {
                    server
                        .options
                        .get("api_key_name")
                        .and_then(|key_name| get_vault_secret_by_name(key_name))
                })
                .map(|api_key| create_client(&api_key, api_version))
                .or_else(|| {
                    report_error(
                        pgrx::PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        "either api_key_id or api_key_name option is required",
                    );
                    None
                }),
        }
        .transpose()?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(StripeFdw {
            rt: create_async_runtime()?,
            base_url: Url::parse(&base_url)?,
            client,
            scan_result: None,
            obj: String::default(),
            rowid_col: String::default(),
            iter_idx: 0,
            table_config: create_table_config(),
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> StripeFdwResult<()> {
        let obj = require_option("object", options)?;

        self.iter_idx = 0;

        if let Some(client) = &self.client {
            let page_size = 100; // maximum page size limit for Stripe API
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
                // build url
                let url = self.build_url(obj, quals, page_size, &cursor)?;
                let Some(url) = url else {
                    return Ok(());
                };

                inc_stats_request_cnt(&mut stats_metadata)?;

                // make api call
                let body = self.rt.block_on(client.get(url).send()).and_then(|resp| {
                    stats::inc_stats(
                        Self::FDW_NAME,
                        stats::Metric::BytesIn,
                        resp.content_length().unwrap_or(0) as i64,
                    );

                    if resp.status() == StatusCode::NOT_FOUND {
                        // if it is 404 error, we should treat it as an empty
                        // result rather than a request error
                        return Ok(String::new());
                    }

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

    fn iter_scan(&mut self, row: &mut Row) -> StripeFdwResult<Option<()>> {
        if let Some(ref mut result) = self.scan_result {
            if self.iter_idx < result.len() {
                row.replace_with(result[self.iter_idx].clone());
                self.iter_idx += 1;
                return Ok(Some(()));
            }
        }
        Ok(None)
    }

    fn re_scan(&mut self) -> StripeFdwResult<()> {
        self.iter_idx = 0;
        Ok(())
    }

    fn end_scan(&mut self) -> StripeFdwResult<()> {
        self.scan_result.take();
        Ok(())
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> StripeFdwResult<()> {
        self.obj = require_option("object", options)?.to_string();
        self.rowid_col = require_option("rowid_column", options)?.to_string();
        Ok(())
    }

    fn insert(&mut self, src: &Row) -> StripeFdwResult<()> {
        if let Some(ref mut client) = self.client {
            let url = self.base_url.join(&self.obj)?;
            let body = row_to_body(src)?;
            if body.is_null() {
                return Ok(());
            }

            let mut stats_metadata = get_stats_metadata();

            inc_stats_request_cnt(&mut stats_metadata)?;

            // call Stripe API
            let body = self
                .rt
                .block_on(client.post(url).form(&body).send())
                .and_then(|resp| {
                    resp.error_for_status()
                        .and_then(|resp| {
                            stats::inc_stats(
                                Self::FDW_NAME,
                                stats::Metric::BytesIn,
                                resp.content_length().unwrap_or(0) as i64,
                            );
                            self.rt.block_on(resp.text())
                        })
                        .map_err(reqwest_middleware::Error::from)
                })?;

            let json: JsonValue = serde_json::from_str(&body)?;
            if let Some(id) = json.get("id").and_then(|v| v.as_str()) {
                report_info(&format!("inserted {} {}", self.obj, id));
            }

            set_stats_metadata(stats_metadata);
        }
        Ok(())
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> StripeFdwResult<()> {
        if let Some(ref mut client) = self.client {
            let mut stats_metadata = get_stats_metadata();

            match rowid {
                Cell::String(rowid) => {
                    let url = self.base_url.join(&format!("{}/", self.obj))?.join(rowid)?;
                    let body = row_to_body(new_row)?;
                    if body.is_null() {
                        return Ok(());
                    }

                    inc_stats_request_cnt(&mut stats_metadata)?;

                    // call Stripe API
                    let body = self
                        .rt
                        .block_on(client.post(url).form(&body).send())
                        .and_then(|resp| {
                            resp.error_for_status()
                                .and_then(|resp| {
                                    stats::inc_stats(
                                        Self::FDW_NAME,
                                        stats::Metric::BytesIn,
                                        resp.content_length().unwrap_or(0) as i64,
                                    );
                                    self.rt.block_on(resp.text())
                                })
                                .map_err(reqwest_middleware::Error::from)
                        })?;

                    let json: JsonValue = serde_json::from_str(&body)?;
                    if let Some(id) = json.get("id").and_then(|v| v.as_str()) {
                        report_info(&format!("updated {} {}", self.obj, id));
                    }
                }
                _ => unreachable!(),
            }

            set_stats_metadata(stats_metadata);
        }
        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> StripeFdwResult<()> {
        if let Some(ref mut client) = self.client {
            let mut stats_metadata = get_stats_metadata();

            match rowid {
                Cell::String(rowid) => {
                    let url = self.base_url.join(&format!("{}/", self.obj))?.join(rowid)?;

                    inc_stats_request_cnt(&mut stats_metadata)?;

                    // call Stripe API
                    let body = self
                        .rt
                        .block_on(client.delete(url).send())
                        .and_then(|resp| {
                            resp.error_for_status()
                                .and_then(|resp| {
                                    stats::inc_stats(
                                        Self::FDW_NAME,
                                        stats::Metric::BytesIn,
                                        resp.content_length().unwrap_or(0) as i64,
                                    );
                                    self.rt.block_on(resp.text())
                                })
                                .map_err(reqwest_middleware::Error::from)
                        })?;

                    let json: JsonValue = serde_json::from_str(&body)?;
                    if let Some(id) = json.get("id").and_then(|v| v.as_str()) {
                        report_info(&format!("deleted {} {}", self.obj, id));
                    }
                }
                _ => unreachable!(),
            }

            set_stats_metadata(stats_metadata);
        }
        Ok(())
    }

    fn import_foreign_schema(
        &mut self,
        stmt: ImportForeignSchemaStmt,
    ) -> StripeFdwResult<Vec<String>> {
        let tbl_config: TableConfig = HashMap::from_iter(
            self.table_config
                .iter()
                .map(|(k, v)| (v.0, (*k, v.1.clone(), v.2.clone()))),
        );
        let all_tables: HashSet<&str> = HashSet::from_iter(tbl_config.keys().copied());
        let table_list = stmt.table_list.iter().map(|t| t.as_str()).collect();
        let selected = match stmt.list_type {
            ImportSchemaType::FdwImportSchemaAll => all_tables,
            ImportSchemaType::FdwImportSchemaLimitTo => {
                all_tables.intersection(&table_list).copied().collect()
            }
            ImportSchemaType::FdwImportSchemaExcept => {
                all_tables.difference(&table_list).copied().collect()
            }
        };
        let ret = selected
            .iter()
            .map(|tbl| {
                tbl_config
                    .get(tbl)
                    .map(|cfg| {
                        let cols = cfg
                            .2
                            .iter()
                            .map(|col| format!("{} {}", col.0, col.1))
                            .collect::<Vec<String>>()
                            .join(",");
                        format!(
                            r#"create foreign table if not exists {} ({},attrs jsonb)
                            server {} options (object '{}', rowid_column 'id')"#,
                            tbl, cols, stmt.server_name, cfg.0,
                        )
                    })
                    .unwrap_or_default()
            })
            .collect();
        Ok(ret)
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> StripeFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "object")?;
            }
        }

        Ok(())
    }
}
