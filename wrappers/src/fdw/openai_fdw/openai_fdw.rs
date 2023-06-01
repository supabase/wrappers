use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::spi::Spi;
use regex::{Captures, Regex};
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::vec::IntoIter;

use supabase_wrappers::prelude::*;

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

macro_rules! report_error {
    ($expr:expr) => {{
        report_error(PgSqlErrorCode::ERRCODE_FDW_ERROR, $expr);
        return None;
    }};
}

macro_rules! report_deparse_error {
    ($expr:expr) => {{
        report_error(PgSqlErrorCode::ERRCODE_FDW_ERROR, $expr);
        String::default()
    }};
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/openai_fdw"
)]
pub(crate) struct OpenaiFdw {
    rt: Runtime,
    client: Option<ClientWithMiddleware>,
    inputs: Option<IntoIter<HashMap<String, Cell>>>,
    tgt_cols: Vec<Column>,
    params: HashMap<String, Qual>,
}

impl OpenaiFdw {
    const BASE_URL: &str = "https://api.openai.com/v1/embeddings";
    const DEFAULT_MODEL: &str = "text-embedding-ada-002";

    fn deparse(
        &mut self,
        source: &str,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> String {
        // deparse source
        let source = if source.starts_with('(') {
            let re = Regex::new(r"\$\{([^}]+)\}").unwrap();
            re.replace_all(source, |caps: &Captures| {
                let param = &caps[1];
                match quals.iter().find(|&q| q.field == param) {
                    Some(qual) => {
                        self.params.insert(qual.field.clone(), qual.clone());
                        match &qual.value {
                            Value::Cell(cell) => match cell {
                                Cell::String(s) => s.clone(),
                                _ => report_deparse_error!("query parameter must be a string"),
                            },
                            Value::Array(_) => report_deparse_error!("invalid query parameter"),
                        }
                    }
                    None => report_deparse_error!(&format!("unmatched query parameter: {}", param)),
                }
            })
            .into_owned()
        } else {
            source.to_string()
        };
        let source = format!("{} _wrappers_src", source);

        // deparse target columns
        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            let mut tgts = columns
                .iter()
                .filter(|c| !self.params.contains_key(&c.name) && c.name != "embedding")
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .join(", ");

            // add 'input' column if it is not included in target columns
            if !columns.iter().any(|c| c.name == "input") {
                tgts.push_str(", input");
            }

            tgts
        };

        // deparse where condition
        let cond = quals
            .iter()
            .filter(|q| !self.params.contains_key(&q.field))
            .map(|q| q.deparse())
            .collect::<Vec<String>>();

        let mut sql = if cond.is_empty() {
            format!("select {} from {}", tgts, source)
        } else {
            format!(
                "select {} from {} where {}",
                tgts,
                source,
                cond.join(" and ")
            )
        };

        // deparse sort
        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|s| s.deparse())
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" order by {}", order_by));
        }

        // deparse limit
        if let Some(limit) = limit {
            let real_limit = limit.offset + limit.count;
            sql.push_str(&format!(" limit {}", real_limit));
        }

        sql
    }
}

impl ForeignDataWrapper for OpenaiFdw {
    fn new(options: &HashMap<String, String>) -> Self {
        let client = match options.get("api_key") {
            Some(api_key) => Some(create_client(api_key)),
            None => require_option("api_key_id", options)
                .and_then(|key_id| get_vault_secret(&key_id))
                .map(|api_key| create_client(&api_key)),
        };

        OpenaiFdw {
            rt: create_async_runtime(),
            client,
            inputs: None,
            tgt_cols: Vec::new(),
            params: HashMap::new(),
        }
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) {
        let source = if let Some(source) = require_option("source", options) {
            source
        } else {
            return;
        };

        self.tgt_cols = columns.to_vec();

        // read source data
        let sql = self.deparse(&source, quals, columns, sorts, limit);
        self.inputs = Spi::connect(|client| {
            let inputs = client
                .select(&sql, None, None)
                .unwrap()
                .map(|src_row| {
                    let mut row: HashMap<String, Cell> = HashMap::new();
                    row.insert(
                        "id".to_string(),
                        src_row.get_by_name::<Cell, _>("id").unwrap().unwrap(),
                    );
                    row.insert(
                        "input".to_string(),
                        src_row.get_by_name::<Cell, _>("input").unwrap().unwrap(),
                    );
                    row
                })
                .collect::<Vec<_>>()
                .into_iter();
            Some(inputs)
        });
    }

    fn iter_scan(&mut self, row: &mut Row) -> Option<()> {
        if let Some(client) = &mut self.client {
            if let Some(inputs) = &mut self.inputs {
                if let Some(src_row) = inputs.next() {
                    let mut body = HashMap::new();
                    body.insert("model", Self::DEFAULT_MODEL.to_string());

                    // find input and save it to request body
                    if let Some(input) = src_row.get("input") {
                        body.insert("input", input.to_string());
                    } else {
                        report_error!("no input column found");
                    }

                    // make api call to OpenAI
                    let embedding = match self
                        .rt
                        .block_on(client.post(Self::BASE_URL).json(&body).send())
                    {
                        Ok(resp) => match resp.error_for_status() {
                            Ok(resp) => match self.rt.block_on(resp.json::<JsonValue>()) {
                                Ok(result) => result
                                    .as_object()
                                    .map(|map| map.get("data").unwrap())
                                    .map(|arr| &arr[0])
                                    .map(|map| map.get("embedding").unwrap())
                                    .map(|arr| arr.as_array().unwrap())
                                    .map(|arr| {
                                        format!(
                                            "{:?}",
                                            arr.iter()
                                                .map(|em| em.as_f64().unwrap())
                                                .collect::<Vec<f64>>()
                                        )
                                    })
                                    .unwrap(),
                                Err(err) => {
                                    report_error!(&format!(
                                        "parse response to JSON failed: {}",
                                        err
                                    ))
                                }
                            },
                            Err(err) => {
                                report_error!(&format!("request OpenAI API failed: {}", err))
                            }
                        },
                        Err(err) => report_error!(&format!("request OpanAI API failed: {}", err)),
                    };

                    // make output data row
                    for col in &self.tgt_cols {
                        match col.name.as_str() {
                            "id" => row.push("id", src_row.get("id").cloned()),
                            "embedding" => {
                                row.push("embedding", Some(Cell::String(embedding.clone())))
                            }
                            _ => {
                                // the rest columns should all be parameters
                                let cell = self.params.get(&col.name).map(|q| match &q.value {
                                    Value::Cell(cell) => cell.clone(),
                                    _ => unreachable!(),
                                });
                                row.push(&col.name, cell)
                            }
                        }
                    }

                    return Some(());
                }
            }
        }

        None
    }

    fn end_scan(&mut self) {
        self.inputs.take();
        self.tgt_cols.clear();
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "source");
            }
        }
    }
}
