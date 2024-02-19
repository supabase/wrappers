use crate::stats;
use pgrx::{JsonB, PgBuiltInOids};
use redis::{Client, Commands, Connection};
use serde_json::json;
use serde_json::value::Value as JsonValue;
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::{RedisFdwError, RedisFdwResult};

// check target columns number and types to make sure match the spec
fn check_target_columns(
    cols: &[Column],
    expect_names: &[&str],
    expect_types: &[PgBuiltInOids],
    src_type: &str,
) -> RedisFdwResult<()> {
    assert_eq!(expect_names.len(), expect_types.len());

    if cols.len() > expect_names.len() {
        match expect_types.len() {
            1 => return Err(RedisFdwError::OnlyOneColumn(src_type.to_owned())),
            2 => return Err(RedisFdwError::OnlyTwoColumn(src_type.to_owned())),
            _ => unreachable!(),
        }
    }

    for col in cols {
        let pos = expect_names.iter().position(|&name| name == col.name);
        if let Some(pos) = pos {
            if col.type_oid != expect_types[pos].value() {
                return Err(RedisFdwError::UnsupportedColumnType(col.name.clone()));
            }
        } else {
            return Err(RedisFdwError::UnsupportedColumnName(col.name.clone()));
        }
    }

    Ok(())
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/redis_fdw",
    error_type = "RedisFdwError"
)]
pub(crate) struct RedisFdw {
    client: Client,
    conn: Option<Connection>,
    src_type: String,
    src_key: String,
    tgt_cols: Vec<Column>,

    // scan result and index
    scan_result: Vec<String>,
    iter_idx: isize,

    // scan result and index for Redis stream
    // a stream entry has an ID as key and hashmap as value, for example:
    // {
    //   "1703938286749-0": {
    //     "key1": "value1",
    //     "key2": "value2"
    //   }
    // }
    // index for stream iteration is the last ID in response
    scan_result_stream: Vec<HashMap<String, HashMap<String, String>>>,
    iter_idx_stream: String,
}

impl RedisFdw {
    const FDW_NAME: &'static str = "RedisFdw";
    const BUF_SIZE: isize = 256;

    fn reset(&mut self) {
        self.iter_idx = 0;
        self.scan_result.clear();
        self.iter_idx_stream = "-".to_string();
        self.scan_result_stream.clear();
    }

    // fetch a target row for list and zset
    fn fetch_row_list(&mut self) -> RedisFdwResult<Option<Row>> {
        if let Some(ref mut conn) = &mut self.conn {
            if self.scan_result.is_empty() {
                let start = self.iter_idx;
                let stop = self.iter_idx + Self::BUF_SIZE - 1;
                self.scan_result = if self.src_type == "list" {
                    conn.lrange(&self.src_key, start, stop)?
                } else {
                    conn.zrange(&self.src_key, start, stop)?
                };
                if self.scan_result.is_empty() {
                    return Ok(None);
                }

                stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, 1_i64);
                stats::inc_stats(
                    Self::FDW_NAME,
                    stats::Metric::RowsOut,
                    self.scan_result.len() as i64,
                );
            }

            let mut tgt_row = Row::new();
            if let Some(val) = self.scan_result.drain(0..1).last() {
                let tgt_col = &self.tgt_cols[0];
                tgt_row.push(&tgt_col.name, Some(Cell::String(val.to_owned())));
            }

            return Ok(Some(tgt_row));
        }
        Ok(None)
    }

    // fetch a target row for set
    fn fetch_row_set(&mut self) -> RedisFdwResult<Option<Row>> {
        if self.iter_idx >= self.scan_result.len() as isize {
            return Ok(None);
        }
        let val = &self.scan_result[self.iter_idx as usize];
        let mut tgt_row = Row::new();
        let tgt_col = &self.tgt_cols[0];
        tgt_row.push(&tgt_col.name, Some(Cell::String(val.to_owned())));
        Ok(Some(tgt_row))
    }

    // fetch a target row for hash
    fn fetch_row_hash(&mut self) -> RedisFdwResult<Option<Row>> {
        // check for end of iteration
        // Redis hash response is like: [key1, value1, key2, values2, ...]
        if (self.iter_idx * 2 + 1) >= self.scan_result.len() as isize {
            return Ok(None);
        }
        let key = &self.scan_result[self.iter_idx as usize * 2];
        let val = &self.scan_result[self.iter_idx as usize * 2 + 1];
        let mut tgt_row = Row::new();
        for tgt_col in &self.tgt_cols {
            if tgt_col.name == "key" {
                tgt_row.push(&tgt_col.name, Some(Cell::String(key.to_owned())));
            }
            if tgt_col.name == "value" {
                tgt_row.push(&tgt_col.name, Some(Cell::String(val.to_owned())));
            }
        }
        Ok(Some(tgt_row))
    }

    // fetch a target row for stream
    fn fetch_row_stream(&mut self) -> RedisFdwResult<Option<Row>> {
        if let Some(ref mut conn) = &mut self.conn {
            if self.iter_idx as usize >= self.scan_result_stream.len() {
                self.scan_result_stream =
                    conn.xrange_count(&self.src_key, &self.iter_idx_stream, "+", Self::BUF_SIZE)?;
                if self.scan_result_stream.is_empty() {
                    return Ok(None);
                }

                self.iter_idx = 0;

                // set cursor for next iteration, don't forget the prefix "("
                if let Some(s) = self.scan_result_stream.last() {
                    self.iter_idx_stream = format!("({}", s.keys().nth(0).unwrap());
                }

                stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, 1_i64);
                stats::inc_stats(
                    Self::FDW_NAME,
                    stats::Metric::RowsOut,
                    self.scan_result_stream.len() as i64,
                );
            }

            let ent = &self.scan_result_stream[self.iter_idx as usize];
            let id = ent.keys().nth(0).expect("stream entry id");
            let items = json!(ent.values().nth(0).expect("stream entry value"));

            let mut tgt_row = Row::new();
            for tgt_col in &self.tgt_cols {
                if tgt_col.name == "id" {
                    tgt_row.push(&tgt_col.name, Some(Cell::String(id.to_owned())));
                }
                if tgt_col.name == "items" {
                    tgt_row.push(&tgt_col.name, Some(Cell::Json(JsonB(items.clone()))));
                }
            }

            return Ok(Some(tgt_row));
        }
        Ok(None)
    }

    // fetch a target row for multi_list, multi_set, multi_zset and multi_hash
    fn fetch_row_multi(&mut self) -> RedisFdwResult<Option<Row>> {
        if let Some(ref mut conn) = &mut self.conn {
            if self.iter_idx >= self.scan_result.len() as isize {
                return Ok(None);
            }

            let key = &self.scan_result[self.iter_idx as usize];
            let items: JsonValue = match self.src_type.as_str() {
                "multi_list" => {
                    let items: Vec<String> = conn.lrange(key, 0, -1)?;
                    json!(items)
                }
                "multi_set" => {
                    let items: Vec<String> = conn.sscan(key)?.collect();
                    json!(items)
                }
                "multi_zset" => {
                    let items: Vec<String> = conn.zrange(key, 0, -1)?;
                    json!(items)
                }
                "multi_hash" => {
                    let items: Vec<String> = conn.hgetall(key)?;
                    let items: HashMap<_, _> = HashMap::from_iter(
                        items.iter().step_by(2).zip(items.iter().skip(1).step_by(2)),
                    );
                    json!(items)
                }
                _ => unreachable!(),
            };

            let mut tgt_row = Row::new();
            for tgt_col in &self.tgt_cols {
                if tgt_col.name == "key" {
                    tgt_row.push(&tgt_col.name, Some(Cell::String(key.to_owned())));
                }
                if tgt_col.name == "items" {
                    tgt_row.push(&tgt_col.name, Some(Cell::Json(JsonB(items.clone()))));
                }
            }

            return Ok(Some(tgt_row));
        }
        Ok(None)
    }
}

impl ForeignDataWrapper<RedisFdwError> for RedisFdw {
    fn new(options: &HashMap<String, String>) -> RedisFdwResult<Self> {
        let conn_url = match options.get("conn_url") {
            Some(url) => url.to_owned(),
            None => {
                let conn_url_id = require_option("conn_url_id", options)?;
                get_vault_secret(conn_url_id).unwrap_or_default()
            }
        };
        let client = Client::open(conn_url)?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(RedisFdw {
            client,
            conn: None,
            src_type: String::default(),
            src_key: String::default(),
            tgt_cols: Vec::new(),
            scan_result: Vec::new(),
            iter_idx: 0,
            scan_result_stream: Vec::new(),
            iter_idx_stream: "-".to_string(),
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> RedisFdwResult<()> {
        let src_type = require_option("src_type", options)?.to_string();
        let src_key = require_option("src_key", options)?.to_string();

        let mut conn = self.client.get_connection()?;

        self.reset();

        match src_type.as_str() {
            "list" | "zset" => {
                check_target_columns(
                    columns,
                    &["element"],
                    &[PgBuiltInOids::TEXTOID],
                    src_type.as_str(),
                )?;
            }
            "set" => {
                check_target_columns(
                    columns,
                    &["element"],
                    &[PgBuiltInOids::TEXTOID],
                    src_type.as_str(),
                )?;
                self.scan_result = conn.sscan(&src_key)?.collect::<Vec<String>>();
                stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, 1_i64);
                stats::inc_stats(
                    Self::FDW_NAME,
                    stats::Metric::RowsOut,
                    self.scan_result.len() as i64,
                );
            }
            "hash" => {
                check_target_columns(
                    columns,
                    &["key", "value"],
                    &[PgBuiltInOids::TEXTOID, PgBuiltInOids::TEXTOID],
                    src_type.as_str(),
                )?;
                self.scan_result = conn.hgetall(&src_key)?;
                stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, 1_i64);
                stats::inc_stats(
                    Self::FDW_NAME,
                    stats::Metric::RowsOut,
                    self.scan_result.len() as i64,
                );
            }
            "stream" => {
                check_target_columns(
                    columns,
                    &["id", "items"],
                    &[PgBuiltInOids::TEXTOID, PgBuiltInOids::JSONBOID],
                    src_type.as_str(),
                )?;
            }
            "multi_list" | "multi_set" | "multi_zset" | "multi_hash" => {
                check_target_columns(
                    columns,
                    &["key", "items"],
                    &[PgBuiltInOids::TEXTOID, PgBuiltInOids::JSONBOID],
                    src_type.as_str(),
                )?;
                self.scan_result = conn.scan_match(&src_key)?.collect::<Vec<String>>();
                stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, 1_i64);
                stats::inc_stats(
                    Self::FDW_NAME,
                    stats::Metric::RowsOut,
                    self.scan_result.len() as i64,
                );
            }
            _ => {
                return Err(RedisFdwError::UnsupportedSourceType(src_type.clone()));
            }
        }

        self.src_type = src_type;
        self.src_key = src_key;
        self.tgt_cols = columns.to_vec();
        self.conn = Some(conn);

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> RedisFdwResult<Option<()>> {
        let tgt_row = match self.src_type.as_str() {
            "list" | "zset" => self.fetch_row_list()?,
            "set" => self.fetch_row_set()?,
            "hash" => self.fetch_row_hash()?,
            "stream" => self.fetch_row_stream()?,
            "multi_list" | "multi_set" | "multi_zset" | "multi_hash" => self.fetch_row_multi()?,
            _ => unreachable!(),
        };

        if let Some(tgt_row) = tgt_row {
            row.replace_with(tgt_row);
            self.iter_idx += 1;
            return Ok(Some(()));
        }

        Ok(None)
    }

    fn re_scan(&mut self) -> RedisFdwResult<()> {
        self.reset();
        Ok(())
    }

    fn end_scan(&mut self) -> RedisFdwResult<()> {
        self.scan_result.clear();
        self.scan_result_stream.clear();
        Ok(())
    }
}
