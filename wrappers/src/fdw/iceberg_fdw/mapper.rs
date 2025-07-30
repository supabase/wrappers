use arrow_array::{array, timezone::Tz, RecordBatch};
use arrow_json::ArrayWriter;
use chrono::{DateTime, NaiveDateTime};
use iceberg::spec::{PrimitiveType, Type};
use pgrx::{
    datum::{self, datetime_support::DateTimeConversionError, JsonB},
    pg_sys, varlena,
};
use serde_json::value::Value as JsonValue;
use std::cell::RefCell;
use std::ops::Deref;
use std::str::FromStr;
use uuid::Uuid;

use super::{IcebergFdwError, IcebergFdwResult};
use supabase_wrappers::prelude::*;

// 'pg epoch' (2000-01-01 00:00:00) in microsecond
const PG_EPOCH_US: i64 = 946_684_800_000_000;

// convert NaiveDateTime to pgrx Timestamp
fn naive_datetime_to_ts(dt: NaiveDateTime) -> IcebergFdwResult<datum::Timestamp> {
    let us = dt.and_utc().timestamp_micros();
    let ts = datum::Timestamp::try_from(us - PG_EPOCH_US)
        .map_err(|_| DateTimeConversionError::OutOfRange)?;
    Ok(ts)
}

// convert DateTime with timezone to pgrx TimestampWithTimeZone
fn datetime_to_tstz(dt: DateTime<Tz>) -> IcebergFdwResult<datum::TimestampWithTimeZone> {
    let us = dt.timestamp_micros();
    let ts = datum::TimestampWithTimeZone::try_from(us - PG_EPOCH_US)
        .map_err(|_| DateTimeConversionError::OutOfRange)?;
    Ok(ts)
}

// parse string to timezone
fn parse_tz(s: Option<&str>) -> IcebergFdwResult<Tz> {
    let tz = Tz::from_str(s.unwrap_or("+00:00"))?;
    Ok(tz)
}

// Iceberg cell to Wrappers cell mapper
#[derive(Default)]
pub(super) struct Mapper {
    // record batch in JSON format
    batch_json: RefCell<Option<JsonValue>>,
}

impl Mapper {
    // convert record batch to JSON value if not done yet and save it locally as cache
    fn get_batch_json(&self, batch: &RecordBatch) -> IcebergFdwResult<&RefCell<Option<JsonValue>>> {
        if self.batch_json.borrow().is_none() {
            let mut buf = Vec::with_capacity(1024);
            let mut writer = ArrayWriter::new(&mut buf);
            writer.write(batch)?;
            writer.finish()?;
            let val = serde_json::from_slice::<JsonValue>(&buf)?;
            self.batch_json.replace(Some(val));
        }

        Ok(&self.batch_json)
    }

    pub(super) fn reset(&mut self) {
        self.batch_json.take();
    }

    // map Iceberg cell to Wrappers cell
    pub(super) fn map_cell(
        &self,
        batch: &RecordBatch,
        tgt_col: &Column,
        src_array: &array::ArrayRef,
        src_type: &Type,
        rec_offset: usize,
    ) -> IcebergFdwResult<Cell> {
        let mut cell: Option<Cell> = None;
        let col_name = &tgt_col.name;
        let array = src_array.as_any();

        // map source field to target column
        match tgt_col.type_oid {
            pg_sys::BOOLOID => {
                if let Type::Primitive(PrimitiveType::Boolean) = src_type {
                    cell = array
                        .downcast_ref::<array::BooleanArray>()
                        .map(|a| Cell::Bool(a.value(rec_offset)));
                }
            }
            pg_sys::FLOAT4OID => {
                if let Type::Primitive(PrimitiveType::Float) = src_type {
                    cell = array
                        .downcast_ref::<array::Float32Array>()
                        .map(|a| Cell::F32(a.value(rec_offset)));
                }
            }
            pg_sys::INT4OID => {
                if let Type::Primitive(PrimitiveType::Int) = src_type {
                    cell = array
                        .downcast_ref::<array::Int32Array>()
                        .map(|a| Cell::I32(a.value(rec_offset)));
                }
            }
            pg_sys::FLOAT8OID => {
                if let Type::Primitive(PrimitiveType::Double) = src_type {
                    cell = array
                        .downcast_ref::<array::Float64Array>()
                        .map(|a| Cell::F64(a.value(rec_offset)));
                }
            }
            pg_sys::INT8OID => {
                if let Type::Primitive(PrimitiveType::Long) = src_type {
                    cell = array
                        .downcast_ref::<array::Int64Array>()
                        .map(|a| Cell::I64(a.value(rec_offset)));
                }
            }
            pg_sys::NUMERICOID => {
                if let Type::Primitive(PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                }) = src_type
                {
                    if let Some(arr) = array.downcast_ref::<array::Decimal128Array>() {
                        let val_str = arr.value_as_string(rec_offset);
                        let val = pgrx::AnyNumeric::from_str(&val_str)?;
                        cell = Some(Cell::Numeric(val));
                    }
                }
            }
            pg_sys::TEXTOID => {
                if let Type::Primitive(PrimitiveType::String) = src_type {
                    cell = array
                        .downcast_ref::<array::StringArray>()
                        .map(|a| Cell::String(a.value(rec_offset).to_owned()));
                }
            }
            pg_sys::DATEOID => {
                if let Type::Primitive(PrimitiveType::Date) = src_type {
                    if let Some(dt) = array
                        .downcast_ref::<array::Date64Array>()
                        .and_then(|a| a.value_as_datetime(rec_offset))
                        .or_else(|| {
                            array
                                .downcast_ref::<array::Date32Array>()
                                .and_then(|a| a.value_as_datetime(rec_offset))
                        })
                    {
                        let ts = naive_datetime_to_ts(dt)?;
                        cell = Some(Cell::Date(datum::Date::from(ts)));
                    }
                }
            }
            pg_sys::TIMEOID => {
                if let Type::Primitive(PrimitiveType::Time) = src_type {
                    if let Some(dt) = array
                        .downcast_ref::<array::Time64NanosecondArray>()
                        .and_then(|a| a.value_as_datetime(rec_offset))
                        .or_else(|| {
                            array
                                .downcast_ref::<array::Time64MicrosecondArray>()
                                .and_then(|a| a.value_as_datetime(rec_offset))
                        })
                        .or_else(|| {
                            array
                                .downcast_ref::<array::Time32MillisecondArray>()
                                .and_then(|a| a.value_as_datetime(rec_offset))
                        })
                        .or_else(|| {
                            array
                                .downcast_ref::<array::Time32SecondArray>()
                                .and_then(|a| a.value_as_datetime(rec_offset))
                        })
                    {
                        let ts = naive_datetime_to_ts(dt)?;
                        cell = Some(Cell::Time(datum::Time::from(ts)));
                    }
                }
            }
            pg_sys::TIMESTAMPOID => {
                if let Type::Primitive(PrimitiveType::Timestamp) = src_type {
                    if let Some(dt) = array
                        .downcast_ref::<array::TimestampNanosecondArray>()
                        .and_then(|a| a.value_as_datetime(rec_offset))
                        .or_else(|| {
                            array
                                .downcast_ref::<array::TimestampMicrosecondArray>()
                                .and_then(|a| a.value_as_datetime(rec_offset))
                        })
                        .or_else(|| {
                            array
                                .downcast_ref::<array::TimestampMillisecondArray>()
                                .and_then(|a| a.value_as_datetime(rec_offset))
                        })
                        .or_else(|| {
                            array
                                .downcast_ref::<array::TimestampSecondArray>()
                                .and_then(|a| a.value_as_datetime(rec_offset))
                        })
                    {
                        let ts = naive_datetime_to_ts(dt)?;
                        cell = Some(Cell::Timestamp(ts));
                    }
                } else if let Type::Primitive(PrimitiveType::TimestampNs) = src_type {
                    if let Some(dt) = array
                        .downcast_ref::<array::TimestampNanosecondArray>()
                        .and_then(|a| a.value_as_datetime(rec_offset))
                    {
                        let ts = naive_datetime_to_ts(dt)?;
                        cell = Some(Cell::Timestamp(ts));
                    }
                }
            }
            pg_sys::TIMESTAMPTZOID => {
                if let Type::Primitive(PrimitiveType::Timestamptz) = src_type {
                    if let Some(dt) = array
                        .downcast_ref::<array::TimestampNanosecondArray>()
                        .and_then(|a| {
                            parse_tz(a.timezone())
                                .map(|tz| a.value_as_datetime_with_tz(rec_offset, tz))
                                .transpose()
                        })
                        .or_else(|| {
                            array
                                .downcast_ref::<array::TimestampMicrosecondArray>()
                                .and_then(|a| {
                                    parse_tz(a.timezone())
                                        .map(|tz| a.value_as_datetime_with_tz(rec_offset, tz))
                                        .transpose()
                                })
                        })
                        .or_else(|| {
                            array
                                .downcast_ref::<array::TimestampMillisecondArray>()
                                .and_then(|a| {
                                    parse_tz(a.timezone())
                                        .map(|tz| a.value_as_datetime_with_tz(rec_offset, tz))
                                        .transpose()
                                })
                        })
                        .or_else(|| {
                            array
                                .downcast_ref::<array::TimestampSecondArray>()
                                .and_then(|a| {
                                    parse_tz(a.timezone())
                                        .map(|tz| a.value_as_datetime_with_tz(rec_offset, tz))
                                        .transpose()
                                })
                        })
                    {
                        let ts = datetime_to_tstz(dt?)?;
                        cell = Some(Cell::Timestamptz(ts));
                    }
                } else if let Type::Primitive(PrimitiveType::TimestamptzNs) = src_type {
                    if let Some(dt) = array
                        .downcast_ref::<array::TimestampNanosecondArray>()
                        .and_then(|a| {
                            parse_tz(a.timezone())
                                .map(|tz| a.value_as_datetime_with_tz(rec_offset, tz))
                                .transpose()
                        })
                    {
                        let ts = datetime_to_tstz(dt?)?;
                        cell = Some(Cell::Timestamptz(ts));
                    }
                }
            }
            pg_sys::JSONBOID => match src_type {
                Type::Struct(_) | Type::List(_) | Type::Map(_) => {
                    if let Some(json) = self.get_batch_json(batch)?.borrow().deref() {
                        let ptr = format!("/{}/{}", rec_offset, col_name);
                        let val = json.pointer(&ptr).cloned().unwrap_or_default();
                        cell = Some(Cell::Json(JsonB(val)));
                    }
                }
                _ => {}
            },
            pg_sys::BYTEAOID => {
                if let Type::Primitive(PrimitiveType::Binary) = src_type {
                    cell = array.downcast_ref::<array::LargeBinaryArray>().map(|a| {
                        let data = a.value(rec_offset);
                        Cell::Bytea(varlena::rust_byte_slice_to_bytea(data).into_pg())
                    });
                }
            }
            pg_sys::UUIDOID => {
                if let Type::Primitive(PrimitiveType::Uuid) = src_type {
                    cell = array
                        .downcast_ref::<array::FixedSizeBinaryArray>()
                        .map(|a| {
                            Uuid::from_slice(a.value(rec_offset))
                                .map(|u| Cell::Uuid(pgrx::Uuid::from_bytes(*u.as_bytes())))
                        })
                        .transpose()?;
                } else if let Type::Primitive(PrimitiveType::String) = src_type {
                    cell = array
                        .downcast_ref::<array::StringArray>()
                        .map(|a| {
                            Uuid::try_parse(a.value(rec_offset))
                                .map(|u| Cell::Uuid(pgrx::Uuid::from_bytes(*u.as_bytes())))
                        })
                        .transpose()?;
                }
            }
            _ => {
                return Err(IcebergFdwError::UnsupportedColumnType(col_name.into()));
            }
        }

        cell.ok_or_else(|| {
            IcebergFdwError::IncompatibleColumnType(col_name.into(), (*src_type).to_string())
        })
    }
}
