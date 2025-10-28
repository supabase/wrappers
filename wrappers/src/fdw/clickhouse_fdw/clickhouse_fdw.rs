use crate::stats;
#[allow(deprecated)]
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use clickhouse_rs::{
    types,
    types::Block,
    types::SqlType,
    types::Value as ChValue,
    types::{i256, u256},
    Pool,
};
use crossbeam::channel;
use futures_util::stream::StreamExt;
use pgrx::pg_sys;
use pgrx::{datum::numeric::AnyNumeric, PgBuiltInOids};

use regex::{Captures, Regex};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
};
use uuid::Uuid;

use supabase_wrappers::prelude::*;

use super::{ClickHouseFdwError, ClickHouseFdwResult};

#[derive(Debug, Clone)]
struct ConvertedRow {
    values: Vec<Option<Cell>>,
}

fn convert_row_simple(
    src_row: &types::Row<clickhouse_rs::Simple>,
    tgt_cols: &[Column],
    _params: &[Qual],
) -> ClickHouseFdwResult<ConvertedRow> {
    let mut values = Vec::with_capacity(tgt_cols.len());

    for tgt_col in tgt_cols {
        // find the column index in the source row
        let mut src_idx = None;
        for i in 0..src_row.len() {
            if src_row.name(i)? == tgt_col.name {
                src_idx = Some(i);
                break;
            }
        }

        let cell = if let Some(idx) = src_idx {
            let sql_type = src_row.sql_type(idx)?;
            let cell = match sql_type {
                types::SqlType::Bool => {
                    let value = src_row.get::<bool, usize>(idx)?;
                    Some(Cell::Bool(value))
                }
                types::SqlType::Int8 => {
                    let value = src_row.get::<i8, usize>(idx)?;
                    Some(Cell::I8(value))
                }
                types::SqlType::UInt8 => {
                    let value = src_row.get::<u8, usize>(idx)?;
                    Some(Cell::I16(value as i16)) // up-cast UInt8 to i16
                }
                types::SqlType::Int16 => {
                    let value = src_row.get::<i16, usize>(idx)?;
                    Some(Cell::I16(value))
                }
                types::SqlType::UInt16 => {
                    let value = src_row.get::<u16, usize>(idx)?;
                    Some(Cell::I32(value as i32)) // up-cast UInt16 to i32
                }
                types::SqlType::Int32 => {
                    let value = src_row.get::<i32, usize>(idx)?;
                    Some(Cell::I32(value))
                }
                types::SqlType::UInt32 => {
                    let value = src_row.get::<u32, usize>(idx)?;
                    Some(Cell::I64(value as i64)) // up-cast UInt32 to i64
                }
                types::SqlType::Float32 => {
                    let value = src_row.get::<f32, usize>(idx)?;
                    Some(Cell::F32(value))
                }
                types::SqlType::Float64 => {
                    let value = src_row.get::<f64, usize>(idx)?;
                    Some(Cell::F64(value))
                }
                types::SqlType::Int64 => {
                    let value = src_row.get::<i64, usize>(idx)?;
                    Some(Cell::I64(value))
                }
                types::SqlType::UInt64 => {
                    let value = src_row.get::<u64, usize>(idx)?;
                    Some(Cell::I64(value as i64))
                }
                types::SqlType::Int128 => {
                    let value = src_row.get::<i128, usize>(idx)?;
                    Some(Cell::String(value.to_string()))
                }
                types::SqlType::UInt128 => {
                    let value = src_row.get::<u128, usize>(idx)?;
                    Some(Cell::String(value.to_string()))
                }
                types::SqlType::Int256 => {
                    let value = src_row.get::<i256, usize>(idx)?;
                    Some(Cell::String(value.to_string()))
                }
                types::SqlType::UInt256 => {
                    let value = src_row.get::<u256, usize>(idx)?;
                    Some(Cell::String(value.to_string()))
                }
                types::SqlType::Decimal(_u, _s) => {
                    // we cannot call PG functions here to create AnyNumeric in
                    // async worker thread, so send it as string and let receiver
                    // to do the final convertion
                    let value = src_row.get::<types::Decimal, usize>(idx)?;
                    Some(Cell::String(value.to_string()))
                }
                types::SqlType::String | types::SqlType::FixedString(_) => {
                    let value = src_row.get::<String, usize>(idx)?;
                    Some(Cell::String(value))
                }
                types::SqlType::Date => {
                    let value = src_row.get::<NaiveDate, usize>(idx)?;
                    // convert NaiveDate to days since PostgreSQL's date epoch (2000-01-01)
                    let pg_epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
                    let duration = value.signed_duration_since(pg_epoch);
                    let days_since_epoch = duration.num_days() as i32;
                    let date = pgrx::datum::Date::saturating_from_raw(days_since_epoch);
                    Some(Cell::Date(date))
                }
                types::SqlType::DateTime(_) => {
                    let value = src_row.get::<DateTime<_>, usize>(idx)?;
                    // convert from Unix timestamp (seconds since 1970-01-01) to
                    // PostgreSQL timestamp (microseconds since 2000-01-01)
                    // difference between 1970 and 2000 epoch: 946684800 seconds
                    let pg_raw_timestamp = (value.timestamp() - 946684800) * 1_000_000;
                    let pg_timestamp = pgrx::datum::Timestamp::saturating_from_raw(
                        pg_raw_timestamp as pg_sys::Timestamp,
                    );
                    Some(Cell::Timestamp(pg_timestamp))
                }
                types::SqlType::Uuid => {
                    let value = src_row.get::<Uuid, usize>(idx)?;
                    Some(Cell::Uuid(pgrx::Uuid::from_bytes(*value.as_bytes())))
                }
                types::SqlType::Array(SqlType::Bool) => {
                    let value = src_row
                        .get::<Vec<bool>, usize>(idx)?
                        .into_iter()
                        .map(Some)
                        .collect();
                    Some(Cell::BoolArray(value))
                }
                types::SqlType::Array(SqlType::Int16) => {
                    let value = src_row
                        .get::<Vec<i16>, usize>(idx)?
                        .into_iter()
                        .map(Some)
                        .collect();
                    Some(Cell::I16Array(value))
                }
                types::SqlType::Array(SqlType::Int32) => {
                    let value = src_row
                        .get::<Vec<i32>, usize>(idx)?
                        .into_iter()
                        .map(Some)
                        .collect();
                    Some(Cell::I32Array(value))
                }
                types::SqlType::Array(SqlType::Int64) => {
                    let value = src_row
                        .get::<Vec<i64>, usize>(idx)?
                        .into_iter()
                        .map(Some)
                        .collect();
                    Some(Cell::I64Array(value))
                }
                types::SqlType::Array(SqlType::Float32) => {
                    let value = src_row
                        .get::<Vec<f32>, usize>(idx)?
                        .into_iter()
                        .map(Some)
                        .collect();
                    Some(Cell::F32Array(value))
                }
                types::SqlType::Array(SqlType::Float64) => {
                    let value = src_row
                        .get::<Vec<f64>, usize>(idx)?
                        .into_iter()
                        .map(Some)
                        .collect();
                    Some(Cell::F64Array(value))
                }
                types::SqlType::Array(SqlType::String) => {
                    let value = src_row
                        .get::<Vec<String>, usize>(idx)?
                        .into_iter()
                        .map(Some)
                        .collect();
                    Some(Cell::StringArray(value))
                }
                types::SqlType::Nullable(inner) => match inner {
                    SqlType::Bool => {
                        let value = src_row.get::<Option<bool>, usize>(idx)?;
                        value.map(Cell::Bool)
                    }
                    SqlType::Int8 => {
                        let value = src_row.get::<Option<i8>, usize>(idx)?;
                        value.map(Cell::I8)
                    }
                    SqlType::UInt8 => {
                        let value = src_row.get::<Option<u8>, usize>(idx)?;
                        value.map(|v| Cell::I16(v as _))
                    }
                    SqlType::Int16 => {
                        let value = src_row.get::<Option<i16>, usize>(idx)?;
                        value.map(Cell::I16)
                    }
                    SqlType::UInt16 => {
                        let value = src_row.get::<Option<u16>, usize>(idx)?;
                        value.map(|v| Cell::I32(v as _))
                    }
                    SqlType::Int32 => {
                        let value = src_row.get::<Option<i32>, usize>(idx)?;
                        value.map(Cell::I32)
                    }
                    SqlType::UInt32 => {
                        let value = src_row.get::<Option<u32>, usize>(idx)?;
                        value.map(|v| Cell::I64(v as _))
                    }
                    SqlType::Float32 => {
                        let value = src_row.get::<Option<f32>, usize>(idx)?;
                        value.map(Cell::F32)
                    }
                    SqlType::Float64 => {
                        let value = src_row.get::<Option<f64>, usize>(idx)?;
                        value.map(Cell::F64)
                    }
                    SqlType::Int64 => {
                        let value = src_row.get::<Option<i64>, usize>(idx)?;
                        value.map(Cell::I64)
                    }
                    SqlType::UInt64 => {
                        let value = src_row.get::<Option<u64>, usize>(idx)?;
                        value.map(|v| Cell::I64(v as _))
                    }
                    SqlType::Int128 => {
                        let value = src_row.get::<Option<i128>, usize>(idx)?;
                        value.map(|v| Cell::String(v.to_string()))
                    }
                    SqlType::UInt128 => {
                        let value = src_row.get::<Option<u128>, usize>(idx)?;
                        value.map(|v| Cell::String(v.to_string()))
                    }
                    SqlType::Int256 => {
                        let value = src_row.get::<Option<i256>, usize>(idx)?;
                        value.map(|v| Cell::String(v.to_string()))
                    }
                    SqlType::UInt256 => {
                        let value = src_row.get::<Option<u256>, usize>(idx)?;
                        value.map(|v| Cell::String(v.to_string()))
                    }
                    SqlType::Decimal(_, _) => {
                        // we cannot call PG functions here to create AnyNumeric in
                        // async worker thread, so send it as string and let receiver
                        // to do the final convertion
                        let value = src_row.get::<Option<types::Decimal>, usize>(idx)?;
                        value.map(|v| Cell::String(v.to_string()))
                    }
                    SqlType::String | SqlType::FixedString(_) => {
                        let value = src_row.get::<Option<String>, usize>(idx)?;
                        value.map(Cell::String)
                    }
                    SqlType::Date => {
                        let value = src_row.get::<Option<NaiveDate>, usize>(idx)?;
                        if let Some(v) = value {
                            // convert NaiveDate to days since PostgreSQL's date epoch (2000-01-01)
                            let pg_epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
                            let duration = v.signed_duration_since(pg_epoch);
                            let days_since_epoch = duration.num_days() as i32;
                            let date = pgrx::datum::Date::saturating_from_raw(days_since_epoch);
                            Some(Cell::Date(date))
                        } else {
                            None
                        }
                    }
                    SqlType::DateTime(_) => {
                        let value = src_row.get::<Option<DateTime<_>>, usize>(idx)?;
                        value.map(|v| {
                            // convert from Unix timestamp (seconds since 1970-01-01)
                            // to PostgreSQL timestamp (microseconds since 2000-01-01)
                            // difference between 1970 and 2000 epoch: 946684800 seconds
                            let pg_raw_timestamp = (v.timestamp() - 946684800) * 1_000_000;
                            let pg_timestamp = pgrx::datum::Timestamp::saturating_from_raw(
                                pg_raw_timestamp as pg_sys::Timestamp,
                            );
                            Cell::Timestamp(pg_timestamp)
                        })
                    }
                    SqlType::Uuid => src_row
                        .get::<Option<Uuid>, usize>(idx)?
                        .map(|v| Cell::Uuid(pgrx::Uuid::from_bytes(*v.as_bytes()))),
                    _ => {
                        return Err(ClickHouseFdwError::UnsupportedColumnType(
                            sql_type.to_string().into(),
                        ))
                    }
                },
                _ => {
                    return Err(ClickHouseFdwError::UnsupportedColumnType(
                        sql_type.to_string().into(),
                    ))
                }
            };
            cell
        } else {
            None
        };

        values.push(cell);
    }

    Ok(ConvertedRow { values })
}

fn array_cell_to_clickhouse_value<T: Clone>(
    v: impl AsRef<[Option<T>]>,
    array_type: &'static SqlType,
    is_nullable: bool,
) -> ClickHouseFdwResult<ChValue>
where
    ChValue: From<T>,
{
    let v: Vec<ChValue> = v
        .as_ref()
        .iter()
        .flatten()
        .cloned()
        .map(ChValue::from)
        .collect();
    let arr = ChValue::Array(array_type, Arc::new(v));
    let val = if is_nullable {
        ChValue::Nullable(either::Either::Right(Box::new(arr)))
    } else {
        arr
    };
    Ok(val)
}

#[wrappers_fdw(
    version = "0.1.8",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/clickhouse_fdw",
    error_type = "ClickHouseFdwError"
)]
pub(crate) struct ClickHouseFdw {
    rt: Runtime,
    conn_str: String,

    // streaming components
    row_receiver: Option<channel::Receiver<ClickHouseFdwResult<Option<ConvertedRow>>>>,
    streaming_task: Option<JoinHandle<()>>,

    // current state
    current_row_data: Option<ConvertedRow>,
    is_scan_complete: bool,

    table: String,
    rowid_col: String,
    tgt_cols: Vec<Column>,
    sql_query: String,
    params: Vec<Qual>,
}

impl ClickHouseFdw {
    const FDW_NAME: &'static str = "ClickHouseFdw";

    fn replace_all_params(
        &mut self,
        re: &Regex,
        mut replacement: impl FnMut(&Captures) -> ClickHouseFdwResult<String>,
    ) -> ClickHouseFdwResult<String> {
        let mut new = String::with_capacity(self.table.len());
        let mut last_match = 0;
        for caps in re.captures_iter(&self.table) {
            let m = caps.get(0).unwrap();
            new.push_str(&self.table[last_match..m.start()]);
            new.push_str(&replacement(&caps)?);
            last_match = m.end();
        }
        new.push_str(&self.table[last_match..]);
        Ok(new)
    }

    fn deparse(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> ClickHouseFdwResult<String> {
        let table = if self.table.starts_with('(') {
            let re = Regex::new(r"\$\{(\w+)\}").unwrap();
            let mut params = Vec::new();
            let mut replacement = |caps: &Captures| -> ClickHouseFdwResult<String> {
                let param = &caps[1];
                for qual in quals.iter() {
                    if qual.field == param {
                        params.push(qual.clone());
                        match &qual.value {
                            Value::Cell(cell) => return Ok(cell.to_string()),
                            Value::Array(arr) => {
                                return Err(ClickHouseFdwError::NoArrayParameter(format!(
                                    "{:?}",
                                    arr
                                )))
                            }
                        }
                    }
                }
                Err(ClickHouseFdwError::UnmatchedParameter(param.to_owned()))
            };
            let s = self.replace_all_params(&re, &mut replacement)?;
            self.params = params;
            s
        } else {
            self.table.clone()
        };

        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .filter(|c| !self.params.iter().any(|p| p.field == c.name))
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .join(", ")
        };

        let mut sql = format!("select {} from {}", tgts, &table);

        if !quals.is_empty() {
            let cond = quals
                .iter()
                .filter(|q| {
                    let is_param = self.params.iter().any(|p| p.field == q.field);
                    let is_array = match &q.value {
                        Value::Cell(c) => c.is_array(),
                        _ => false,
                    };
                    !is_param && !is_array
                })
                .map(|q| q.deparse())
                .collect::<Vec<String>>()
                .join(" and ");

            if !cond.is_empty() {
                sql.push_str(&format!(" where {}", cond));
            }
        }

        // push down sorts
        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|sort| sort.deparse())
                .collect::<Vec<String>>()
                .join(", ");
            sql.push_str(&format!(" order by {}", order_by));
        }

        // push down limits
        // Note: Postgres will take limit and offset locally after reading rows
        // from remote, so we calculate the real limit and only use it without
        // pushing down offset.
        if let Some(limit) = limit {
            let real_limit = limit.offset + limit.count;
            sql.push_str(&format!(" limit {}", real_limit));
        }

        Ok(sql)
    }

    fn fetch_next_row(&mut self) -> ClickHouseFdwResult<()> {
        if let Some(ref rx) = self.row_receiver {
            match rx.recv() {
                Ok(row_result) => match row_result {
                    Ok(Some(mut row)) => {
                        // got a valid row from sender, and then do the numeric
                        // column conversion because sender is unable
                        // to do that conversion
                        for (idx, tgt_col) in self.tgt_cols.iter().enumerate() {
                            if tgt_col.type_oid == PgBuiltInOids::NUMERICOID.into() {
                                if let Some(Cell::String(s)) = &row.values[idx] {
                                    let num: AnyNumeric = AnyNumeric::try_from(s.as_str())?;
                                    row.values[idx] = Some(Cell::Numeric(num));
                                } else {
                                    row.values[idx] = None;
                                }
                            }
                        }

                        self.current_row_data = Some(row);

                        Ok(())
                    }
                    Ok(None) => {
                        // got end-of-stream marker - no more data
                        self.current_row_data = None;
                        self.is_scan_complete = true;
                        Ok(())
                    }
                    Err(e) => {
                        self.row_receiver = None;
                        self.streaming_task = None;
                        Err(e)
                    }
                },
                Err(_) => {
                    // channel disconnected - streaming is complete or error occurred
                    self.current_row_data = None;
                    self.is_scan_complete = true;
                    Ok(())
                }
            }
        } else {
            // no receiver available, scan is complete
            self.current_row_data = None;
            self.is_scan_complete = true;
            Ok(())
        }
    }
}

impl ForeignDataWrapper<ClickHouseFdwError> for ClickHouseFdw {
    fn new(server: ForeignServer) -> ClickHouseFdwResult<Self> {
        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(CreateRuntimeError::from)?;
        let conn_str = match server.options.get("conn_string") {
            Some(conn_str) => conn_str.to_owned(),
            None => {
                let conn_str_id = require_option("conn_string_id", &server.options)?;
                get_vault_secret(conn_str_id).unwrap_or_default()
            }
        };

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(Self {
            rt,
            conn_str,
            row_receiver: None,
            streaming_task: None,
            current_row_data: None,
            is_scan_complete: false,
            table: String::default(),
            rowid_col: String::default(),
            tgt_cols: Vec::new(),
            sql_query: String::new(),
            params: Vec::new(),
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> ClickHouseFdwResult<()> {
        self.table = require_option("table", options)?.to_string();
        self.tgt_cols = columns.to_vec();
        self.params = Vec::new();
        self.sql_query = self.deparse(quals, columns, sorts, limit)?;
        self.is_scan_complete = false;
        self.current_row_data = None;

        // create bounded channel
        let (tx, rx) = channel::bounded::<ClickHouseFdwResult<Option<ConvertedRow>>>(1024);
        self.row_receiver = Some(rx);

        // clone data needed by the async task
        let conn_str = self.conn_str.clone();
        let sql = self.sql_query.clone();
        let tgt_cols = self.tgt_cols.clone();
        let params = self.params.clone();
        let tx_clone = tx.clone();

        // spawn the async streaming task
        let streaming_task = self.rt.spawn(async move {
            stream_data_to_channel(conn_str, sql, tgt_cols, params, tx_clone).await;
        });

        self.streaming_task = Some(streaming_task);

        // fetch the first row to initialize the scan
        self.fetch_next_row()?;

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> ClickHouseFdwResult<Option<()>> {
        if self.is_scan_complete {
            return Ok(None);
        }

        if let Some(converted_row) = self.current_row_data.take() {
            // process current row
            for (i, tgt_col) in self.tgt_cols.iter().enumerate() {
                // check if this is a parameter column
                if let Some(param) = self.params.iter().find(|&p| p.field == tgt_col.name) {
                    if let Value::Cell(cell) = &param.value {
                        row.push(&tgt_col.name, Some(cell.clone()));
                    } else {
                        row.push(&tgt_col.name, None);
                    }
                    continue;
                }

                let cell = converted_row.values.get(i).unwrap_or(&None).clone();
                row.push(&tgt_col.name, cell);
            }

            // fetch next row for the next iteration
            self.fetch_next_row()?;

            Ok(Some(()))
        } else {
            Ok(None) // no more rows available
        }
    }

    fn end_scan(&mut self) -> ClickHouseFdwResult<()> {
        // clean up channel and state
        self.current_row_data = None;
        self.row_receiver = None;
        self.is_scan_complete = true;

        Ok(())
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> ClickHouseFdwResult<()> {
        self.table = require_option("table", options)?.to_string();
        self.rowid_col = require_option("rowid_column", options)?.to_string();
        Ok(())
    }

    fn insert(&mut self, src: &Row) -> ClickHouseFdwResult<()> {
        // create a client for insert operations
        let pool = Pool::new(self.conn_str.clone());
        let mut client = self.rt.block_on(pool.get_handle())?;

        // use a dummy query to probe column types
        let sql = format!("select * from {} where false", self.table);
        let probe = self.rt.block_on(client.query(&sql).fetch_all())?;

        // add row to block
        let mut row = Vec::new();
        for (col_name, cell) in src.iter() {
            let col_name = col_name.to_owned();
            let tgt_col = probe.get_column(col_name.as_ref())?;
            let tgt_type = tgt_col.sql_type();
            let is_nullable = matches!(tgt_type, SqlType::Nullable(_));

            let value = cell
                .as_ref()
                .map(|c| match c {
                    Cell::Bool(v) => {
                        let val = if is_nullable {
                            ChValue::from(Some(*v))
                        } else {
                            ChValue::from(*v)
                        };
                        Ok(val)
                    }
                    Cell::I8(v) => {
                        let val = if is_nullable {
                            ChValue::from(Some(*v))
                        } else {
                            ChValue::from(*v)
                        };
                        Ok(val)
                    }
                    Cell::I16(v) => match tgt_col.sql_type() {
                        // i16 can be converted to 2 ClickHouse types: Int16 and UInt8
                        SqlType::Int16 | SqlType::Nullable(SqlType::Int16) => {
                            let val = if is_nullable {
                                ChValue::from(Some(*v))
                            } else {
                                ChValue::from(*v)
                            };
                            Ok(val)
                        }
                        SqlType::UInt8 | SqlType::Nullable(SqlType::UInt8) => {
                            let val = if is_nullable {
                                ChValue::from(Some(*v as u8))
                            } else {
                                ChValue::from(*v as u8)
                            };
                            Ok(val)
                        }
                        _ => Err(ClickHouseFdwError::UnsupportedColumnType(
                            tgt_type.to_string().into(),
                        )),
                    },
                    Cell::F32(v) => {
                        let val = if is_nullable {
                            ChValue::from(Some(*v))
                        } else {
                            ChValue::from(*v)
                        };
                        Ok(val)
                    }
                    Cell::I32(v) => match tgt_col.sql_type() {
                        // i32 can be converted to 2 ClickHouse types: Int32 and UInt16
                        SqlType::Int32 | SqlType::Nullable(SqlType::Int32) => {
                            let val = if is_nullable {
                                ChValue::from(Some(*v))
                            } else {
                                ChValue::from(*v)
                            };
                            Ok(val)
                        }
                        SqlType::UInt16 | SqlType::Nullable(SqlType::UInt16) => {
                            let val = if is_nullable {
                                ChValue::from(Some(*v as u16))
                            } else {
                                ChValue::from(*v as u16)
                            };
                            Ok(val)
                        }
                        _ => Err(ClickHouseFdwError::UnsupportedColumnType(
                            tgt_type.to_string().into(),
                        )),
                    },
                    Cell::F64(v) => {
                        let val = if is_nullable {
                            ChValue::from(Some(*v))
                        } else {
                            ChValue::from(*v)
                        };
                        Ok(val)
                    }
                    Cell::I64(v) => match tgt_col.sql_type() {
                        // i64 can be converted to 2 ClickHouse types: Int64 and UInt32
                        SqlType::Int64 | SqlType::Nullable(SqlType::Int64) => {
                            let val = if is_nullable {
                                ChValue::from(Some(*v))
                            } else {
                                ChValue::from(*v)
                            };
                            Ok(val)
                        }
                        SqlType::UInt32 | SqlType::Nullable(SqlType::UInt32) => {
                            let val = if is_nullable {
                                ChValue::from(Some(*v as u32))
                            } else {
                                ChValue::from(*v as u32)
                            };
                            Ok(val)
                        }
                        _ => Err(ClickHouseFdwError::UnsupportedColumnType(
                            tgt_type.to_string().into(),
                        )),
                    },
                    Cell::Numeric(v) => {
                        let v = types::Decimal::from_str(v.normalize())?;
                        let val = if is_nullable {
                            ChValue::from(Some(v))
                        } else {
                            ChValue::from(v)
                        };
                        Ok(val)
                    }
                    Cell::String(v) => {
                        let s = v.as_str();

                        // i256 and u256 are saved as string in Postgres, so we parse it
                        // back to ClickHouse if target column is Int256 or UInt256
                        let val = match tgt_col.sql_type() {
                            SqlType::Int256 | SqlType::Nullable(SqlType::Int256) => {
                                let v = i256::from_str(s)?;
                                if is_nullable {
                                    ChValue::from(Some(v))
                                } else {
                                    ChValue::from(v)
                                }
                            }
                            SqlType::UInt256 | SqlType::Nullable(SqlType::UInt256) => {
                                let v = u256::from_str(s)?;
                                if is_nullable {
                                    ChValue::from(Some(v))
                                } else {
                                    ChValue::from(v)
                                }
                            }
                            _ => {
                                // other than i256 and u256, convert it to string as normal
                                if is_nullable {
                                    ChValue::from(Some(s))
                                } else {
                                    ChValue::from(s)
                                }
                            }
                        };
                        Ok(val)
                    }
                    Cell::Date(_) => {
                        let s = c.to_string().replace('\'', "");
                        let tm = NaiveDate::parse_from_str(&s, "%Y-%m-%d")?;
                        let val = if is_nullable {
                            ChValue::from(Some(tm))
                        } else {
                            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            let duration = tm - epoch;
                            let dt = duration.num_days() as u16;
                            ChValue::Date(dt)
                        };
                        Ok(val)
                    }
                    Cell::Timestamp(_) => {
                        let s = c.to_string().replace('\'', "");
                        let naive_tm = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                            .or_else(|_| {
                                NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.6f")
                            })?;
                        let tm: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive_tm, Utc);
                        let val = if is_nullable {
                            ChValue::Nullable(either::Either::Right(Box::new(tm.into())))
                        } else {
                            ChValue::from(tm)
                        };
                        Ok(val)
                    }
                    Cell::Uuid(v) => {
                        let uuid = Uuid::try_parse(&v.to_string())?;
                        let val = if is_nullable {
                            ChValue::Nullable(either::Either::Right(Box::new(ChValue::Uuid(
                                *uuid.as_bytes(),
                            ))))
                        } else {
                            ChValue::from(uuid)
                        };
                        Ok(val)
                    }
                    Cell::BoolArray(v) => {
                        array_cell_to_clickhouse_value(v, &SqlType::Bool, is_nullable)
                    }
                    Cell::I16Array(v) => {
                        array_cell_to_clickhouse_value(v, &SqlType::Int16, is_nullable)
                    }
                    Cell::I32Array(v) => {
                        array_cell_to_clickhouse_value(v, &SqlType::Int32, is_nullable)
                    }
                    Cell::I64Array(v) => {
                        array_cell_to_clickhouse_value(v, &SqlType::Int64, is_nullable)
                    }
                    Cell::F32Array(v) => {
                        array_cell_to_clickhouse_value(v, &SqlType::Float32, is_nullable)
                    }
                    Cell::F64Array(v) => {
                        array_cell_to_clickhouse_value(v, &SqlType::Float64, is_nullable)
                    }
                    Cell::StringArray(v) => {
                        array_cell_to_clickhouse_value(v, &SqlType::String, is_nullable)
                    }
                    _ => Err(ClickHouseFdwError::UnsupportedColumnType(
                        tgt_type.to_string().into(),
                    )),
                })
                .transpose()?;

            if let Some(v) = value {
                row.push((col_name, v));
            }
        }
        let mut block = Block::new();
        block.push(row)?;

        // execute query on ClickHouse
        self.rt.block_on(client.insert(&self.table, block))?;
        Ok(())
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> ClickHouseFdwResult<()> {
        // create a client for update operations
        let pool = Pool::new(self.conn_str.clone());
        let mut client = self.rt.block_on(pool.get_handle())?;

        let mut sets = Vec::new();
        for (col, cell) in new_row.iter() {
            if col == &self.rowid_col {
                continue;
            }
            if let Some(cell) = cell {
                sets.push(format!("{} = {}", col, cell));
            } else {
                sets.push(format!("{} = null", col));
            }
        }
        let sql = format!(
            "alter table {} update {} where {} = {}",
            self.table,
            sets.join(", "),
            self.rowid_col,
            rowid
        );

        // execute query on ClickHouse
        self.rt.block_on(client.execute(&sql))?;
        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> ClickHouseFdwResult<()> {
        // create a client for delete operations
        let pool = Pool::new(self.conn_str.clone());
        let mut client = self.rt.block_on(pool.get_handle())?;

        let sql = format!(
            "alter table {} delete where {} = {}",
            self.table, self.rowid_col, rowid
        );

        // execute query on ClickHouse
        self.rt.block_on(client.execute(&sql))?;
        Ok(())
    }
}

async fn stream_data_to_channel(
    conn_str: String,
    sql: String,
    tgt_cols: Vec<Column>,
    params: Vec<Qual>,
    tx: channel::Sender<ClickHouseFdwResult<Option<ConvertedRow>>>,
) {
    let client_result = async {
        let pool = Pool::new(conn_str);
        pool.get_handle().await.map_err(ClickHouseFdwError::from)
    }
    .await;

    let mut client = match client_result {
        Ok(client) => client,
        Err(e) => {
            // send the connection error through the channel
            let _ = tx.send(Err(e));
            return;
        }
    };

    let mut stream = client.query(&sql).stream();

    while let Some(row_result) = stream.next().await {
        match row_result {
            Ok(src_row) => {
                match convert_row_simple(&src_row, &tgt_cols, &params) {
                    Ok(converted_row) => {
                        if tx.send(Ok(Some(converted_row))).is_err() {
                            // receiver dropped, stop streaming
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(Err(ClickHouseFdwError::from(e)));
                return;
            }
        }
    }

    // send end-of-stream marker to signal that no more data is coming
    let _ = tx.send(Ok(None));
}
