use arrow_array::{array, builder::*, timezone::Tz, RecordBatch};
use arrow_json::ArrayWriter;
use arrow_schema::DataType;
use chrono::{DateTime, NaiveDateTime, Timelike};
use iceberg::spec::{NestedField, NestedFieldRef, PrimitiveType, Schema, Type};
use pgrx::{
    datum::{self, datetime_support::DateTimeConversionError, JsonB},
    pg_sys,
    prelude::*,
    varlena,
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

// fill an struct with null to all fields
fn fill_empty_struct(struct_builder: &mut StructBuilder) {
    struct_builder.append_null();
    for field_builder in struct_builder.field_builders_mut() {
        if let Some(b) = field_builder.as_any_mut().downcast_mut::<StringBuilder>() {
            b.append_null();
        } else if let Some(b) = field_builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
            b.append_null();
        } else if let Some(b) = field_builder.as_any_mut().downcast_mut::<Int32Builder>() {
            b.append_null();
        } else if let Some(b) = field_builder.as_any_mut().downcast_mut::<Int64Builder>() {
            b.append_null();
        } else if let Some(b) = field_builder.as_any_mut().downcast_mut::<Float32Builder>() {
            b.append_null();
        } else if let Some(b) = field_builder.as_any_mut().downcast_mut::<Float64Builder>() {
            b.append_null();
        }
    }
}

// iceberg cell to Wrappers cell mapper
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
                    if let Some(t) = array
                        .downcast_ref::<array::Time64MicrosecondArray>()
                        .and_then(|a| a.value_as_time(rec_offset))
                        .or_else(|| {
                            array
                                .downcast_ref::<array::Time32MillisecondArray>()
                                .and_then(|a| a.value_as_time(rec_offset))
                        })
                        .or_else(|| {
                            array
                                .downcast_ref::<array::Time32SecondArray>()
                                .and_then(|a| a.value_as_time(rec_offset))
                        })
                    {
                        let time =
                            datum::Time::new(t.hour() as _, t.minute() as _, t.second() as _)?;
                        cell = Some(Cell::Time(time));
                    }
                }
            }
            pg_sys::TIMESTAMPOID => {
                if let Type::Primitive(PrimitiveType::Timestamp) = src_type {
                    if let Some(dt) = array
                        .downcast_ref::<array::TimestampMicrosecondArray>()
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
                        .downcast_ref::<array::TimestampMicrosecondArray>()
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
                        let ptr = format!("/{rec_offset}/{col_name}");
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
                return Err(IcebergFdwError::UnsupportedType(format!(
                    "column '{col_name}', type oid '{}'",
                    tgt_col.type_oid
                )));
            }
        }

        cell.ok_or_else(|| {
            IcebergFdwError::IncompatibleColumnType(col_name.into(), (*src_type).to_string())
        })
    }

    pub(super) fn append_array_value(
        &self,
        builder: &mut Box<dyn ArrayBuilder>,
        field_type: &DataType,
        cell: Option<&Cell>,
    ) -> IcebergFdwResult<()> {
        use rust_decimal::prelude::ToPrimitive;
        use rust_decimal::Decimal;
        use std::convert::TryInto;

        let unsupported = |ty: &DataType| IcebergFdwError::UnsupportedType(format!("{ty:?}"));

        match field_type {
            DataType::Boolean => {
                if let Some(bool_builder) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
                    match cell {
                        Some(Cell::Bool(val)) => bool_builder.append_value(*val),
                        _ => bool_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Int32 => {
                if let Some(int_builder) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
                    match cell {
                        Some(Cell::I32(val)) => int_builder.append_value(*val),
                        Some(Cell::I16(val)) => int_builder.append_value((*val).into()),
                        Some(Cell::I8(val)) => int_builder.append_value((*val).into()),
                        _ => int_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Int64 => {
                if let Some(int_builder) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                    match cell {
                        Some(Cell::I64(val)) => int_builder.append_value(*val),
                        Some(Cell::I32(val)) => int_builder.append_value((*val).into()),
                        Some(Cell::I16(val)) => int_builder.append_value((*val).into()),
                        Some(Cell::I8(val)) => int_builder.append_value((*val).into()),
                        _ => int_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Float32 => {
                if let Some(float_builder) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
                    match cell {
                        Some(Cell::F32(val)) => float_builder.append_value(*val),
                        Some(Cell::F64(val)) => float_builder.append_value(*val as f32),
                        _ => float_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Float64 => {
                if let Some(float_builder) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                    match cell {
                        Some(Cell::F64(val)) => float_builder.append_value(*val),
                        Some(Cell::F32(val)) => float_builder.append_value((*val).into()),
                        _ => float_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Decimal128(_, scale) => {
                if let Some(dec_builder) = builder.as_any_mut().downcast_mut::<Decimal128Builder>()
                {
                    if let Some(Cell::Numeric(val)) = cell {
                        let decimal_str = val.to_string();
                        let mut appended = false;
                        if let Ok(mut decimal) = Decimal::from_str_exact(&decimal_str) {
                            if let Ok(scale_u32) = (*scale).try_into() {
                                decimal.rescale(scale_u32);
                                if let Some(mantissa) = decimal.mantissa().to_i128() {
                                    dec_builder.append_value(mantissa);
                                    appended = true;
                                }
                            }
                        }
                        if !appended {
                            dec_builder.append_null();
                        }
                    } else {
                        dec_builder.append_null();
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Utf8 => {
                if let Some(str_builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                    match cell {
                        Some(Cell::String(val)) => str_builder.append_value(val),
                        Some(Cell::Json(val)) => str_builder.append_value(val.0.to_string()),
                        _ => str_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::LargeUtf8 => {
                if let Some(str_builder) = builder.as_any_mut().downcast_mut::<LargeStringBuilder>()
                {
                    match cell {
                        Some(Cell::String(val)) => str_builder.append_value(val),
                        Some(Cell::Json(val)) => str_builder.append_value(val.0.to_string()),
                        _ => str_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Binary => {
                if let Some(bin_builder) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
                    match cell {
                        Some(Cell::Bytea(val)) => {
                            let bytes = unsafe { varlena::varlena_to_byte_slice(*val) };
                            bin_builder.append_value(bytes);
                        }
                        Some(Cell::Uuid(val)) => {
                            bin_builder.append_value(val.as_bytes());
                        }
                        _ => bin_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::LargeBinary => {
                if let Some(bin_builder) = builder.as_any_mut().downcast_mut::<LargeBinaryBuilder>()
                {
                    match cell {
                        Some(Cell::Bytea(val)) => {
                            let bytes = unsafe { varlena::varlena_to_byte_slice(*val) };
                            bin_builder.append_value(bytes);
                        }
                        Some(Cell::Uuid(val)) => {
                            bin_builder.append_value(val.as_bytes());
                        }
                        _ => bin_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::FixedSizeBinary(size) => {
                if let Some(bin_builder) = builder
                    .as_any_mut()
                    .downcast_mut::<FixedSizeBinaryBuilder>()
                {
                    match cell {
                        Some(Cell::Bytea(val)) => {
                            let bytes = unsafe { varlena::varlena_to_byte_slice(*val) };
                            if bytes.len() == *size as usize {
                                let _ = bin_builder.append_value(bytes);
                            } else {
                                bin_builder.append_null();
                            }
                        }
                        Some(Cell::Uuid(val)) if *size as usize == val.as_bytes().len() => {
                            let _ = bin_builder.append_value(val.as_bytes());
                        }
                        _ => bin_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Date32 => {
                if let Some(date_builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
                    match cell {
                        Some(Cell::Date(val)) => {
                            date_builder.append_value(val.to_unix_epoch_days())
                        }
                        _ => date_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Date64 => {
                if let Some(date_builder) = builder.as_any_mut().downcast_mut::<Date64Builder>() {
                    match cell {
                        Some(Cell::Date(val)) => {
                            let days = i64::from(val.to_unix_epoch_days());
                            date_builder.append_value(days * 86_400_000);
                        }
                        _ => date_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Time32(unit) => match unit {
                arrow_schema::TimeUnit::Second => {
                    if let Some(time_builder) =
                        builder.as_any_mut().downcast_mut::<Time32SecondBuilder>()
                    {
                        match cell {
                            Some(Cell::Time(val)) => {
                                let micros = i64::from(*val);
                                time_builder.append_value((micros / 1_000_000) as i32);
                            }
                            _ => time_builder.append_null(),
                        }
                    } else {
                        return Err(unsupported(field_type));
                    }
                }
                arrow_schema::TimeUnit::Millisecond => {
                    if let Some(time_builder) = builder
                        .as_any_mut()
                        .downcast_mut::<Time32MillisecondBuilder>()
                    {
                        match cell {
                            Some(Cell::Time(val)) => {
                                let micros = i64::from(*val);
                                time_builder.append_value((micros / 1_000) as i32);
                            }
                            _ => time_builder.append_null(),
                        }
                    } else {
                        return Err(unsupported(field_type));
                    }
                }
                _ => return Err(unsupported(field_type)),
            },
            DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                if let Some(time_builder) = builder
                    .as_any_mut()
                    .downcast_mut::<Time64MicrosecondBuilder>()
                {
                    match cell {
                        Some(Cell::Time(val)) => {
                            let micros = i64::from(*val);
                            time_builder.append_value(micros);
                        }
                        _ => time_builder.append_null(),
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Timestamp(unit, tz) => match unit {
                arrow_schema::TimeUnit::Second => {
                    if let Some(ts_builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampSecondBuilder>()
                    {
                        let raw = match (tz.is_some(), cell) {
                            (true, Some(Cell::Timestamptz(val))) => {
                                Some(i64::from(*val) + PG_EPOCH_US)
                            }
                            (false, Some(Cell::Timestamp(val))) => {
                                Some(i64::from(*val) + PG_EPOCH_US)
                            }
                            _ => None,
                        };
                        if let Some(micros) = raw {
                            ts_builder.append_value(micros / 1_000_000);
                        } else {
                            ts_builder.append_null();
                        }
                    } else {
                        return Err(unsupported(field_type));
                    }
                }
                arrow_schema::TimeUnit::Millisecond => {
                    if let Some(ts_builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMillisecondBuilder>()
                    {
                        let raw = match (tz.is_some(), cell) {
                            (true, Some(Cell::Timestamptz(val))) => {
                                Some(i64::from(*val) + PG_EPOCH_US)
                            }
                            (false, Some(Cell::Timestamp(val))) => {
                                Some(i64::from(*val) + PG_EPOCH_US)
                            }
                            _ => None,
                        };
                        if let Some(micros) = raw {
                            ts_builder.append_value(micros / 1_000);
                        } else {
                            ts_builder.append_null();
                        }
                    } else {
                        return Err(unsupported(field_type));
                    }
                }
                arrow_schema::TimeUnit::Microsecond => {
                    if let Some(ts_builder) = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMicrosecondBuilder>()
                    {
                        let raw = match (tz.is_some(), cell) {
                            (true, Some(Cell::Timestamptz(val))) => {
                                Some(i64::from(*val) + PG_EPOCH_US)
                            }
                            (false, Some(Cell::Timestamp(val))) => {
                                Some(i64::from(*val) + PG_EPOCH_US)
                            }
                            _ => None,
                        };
                        if let Some(micros) = raw {
                            ts_builder.append_value(micros);
                        } else {
                            ts_builder.append_null();
                        }
                    } else {
                        return Err(unsupported(field_type));
                    }
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    return Err(unsupported(field_type));
                }
            },
            DataType::Struct(fields) => {
                if let Some(struct_builder) = builder.as_any_mut().downcast_mut::<StructBuilder>() {
                    match cell {
                        Some(Cell::Json(json_val)) => {
                            if let Some(json_object) = json_val.0.as_object() {
                                // process each field in the struct
                                let field_builders = struct_builder.field_builders_mut();

                                for (i, field) in fields.iter().enumerate() {
                                    if i < field_builders.len() {
                                        let field_name = field.name();
                                        let field_value = json_object.get(field_name);

                                        // handle field insertion based on field type
                                        match field.data_type() {
                                            DataType::Boolean => {
                                                if let Some(bool_builder) = field_builders[i]
                                                    .as_any_mut()
                                                    .downcast_mut::<BooleanBuilder>()
                                                {
                                                    match field_value {
                                                        Some(JsonValue::Bool(v)) => {
                                                            bool_builder.append_value(*v)
                                                        }
                                                        _ => bool_builder.append_null(),
                                                    }
                                                }
                                            }
                                            DataType::Int32 => {
                                                if let Some(int_builder) = field_builders[i]
                                                    .as_any_mut()
                                                    .downcast_mut::<Int32Builder>()
                                                {
                                                    match field_value {
                                                        Some(JsonValue::Number(n))
                                                            if n.is_i64() =>
                                                        {
                                                            if let Some(val) = n.as_i64() {
                                                                int_builder
                                                                    .append_value(val as i32);
                                                            } else {
                                                                int_builder.append_null();
                                                            }
                                                        }
                                                        _ => int_builder.append_null(),
                                                    }
                                                }
                                            }
                                            DataType::Int64 => {
                                                if let Some(int_builder) = field_builders[i]
                                                    .as_any_mut()
                                                    .downcast_mut::<Int64Builder>()
                                                {
                                                    match field_value {
                                                        Some(JsonValue::Number(n))
                                                            if n.is_i64() =>
                                                        {
                                                            if let Some(val) = n.as_i64() {
                                                                int_builder.append_value(val);
                                                            } else {
                                                                int_builder.append_null();
                                                            }
                                                        }
                                                        _ => int_builder.append_null(),
                                                    }
                                                }
                                            }
                                            DataType::Float32 => {
                                                if let Some(float_builder) = field_builders[i]
                                                    .as_any_mut()
                                                    .downcast_mut::<Float32Builder>()
                                                {
                                                    match field_value {
                                                        Some(JsonValue::Number(n))
                                                            if n.is_f64() =>
                                                        {
                                                            if let Some(val) = n.as_f64() {
                                                                float_builder
                                                                    .append_value(val as f32);
                                                            } else {
                                                                float_builder.append_null();
                                                            }
                                                        }
                                                        _ => float_builder.append_null(),
                                                    }
                                                }
                                            }
                                            DataType::Float64 => {
                                                if let Some(float_builder) = field_builders[i]
                                                    .as_any_mut()
                                                    .downcast_mut::<Float64Builder>()
                                                {
                                                    match field_value {
                                                        Some(JsonValue::Number(n))
                                                            if n.is_f64() =>
                                                        {
                                                            if let Some(val) = n.as_f64() {
                                                                float_builder.append_value(val);
                                                            } else {
                                                                float_builder.append_null();
                                                            }
                                                        }
                                                        _ => float_builder.append_null(),
                                                    }
                                                }
                                            }
                                            DataType::Utf8 => {
                                                if let Some(str_builder) = field_builders[i]
                                                    .as_any_mut()
                                                    .downcast_mut::<StringBuilder>()
                                                {
                                                    match field_value {
                                                        Some(JsonValue::String(s)) => {
                                                            str_builder.append_value(s)
                                                        }
                                                        _ => str_builder.append_null(),
                                                    }
                                                }
                                            }
                                            _ => {
                                                return Err(unsupported(field_type));
                                            }
                                        }
                                    }
                                }

                                struct_builder.append(true);
                            } else {
                                // not a JSON object, append null for all fields
                                fill_empty_struct(struct_builder);
                            }
                        }
                        _ => {
                            // no cell data, append null for all fields
                            fill_empty_struct(struct_builder);
                        }
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::List(field) => {
                if let Some(list_builder) = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                {
                    // we only deal with primitive element list
                    match cell {
                        Some(Cell::Json(json_val)) => {
                            if let Some(json_array) = json_val.0.as_array() {
                                match field.data_type() {
                                    DataType::Boolean => {
                                        let bool_builder = list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<BooleanBuilder>()
                                            .ok_or_else(|| unsupported(field_type))?;
                                        for item in json_array {
                                            match item {
                                                JsonValue::Bool(v) => bool_builder.append_value(*v),
                                                _ => bool_builder.append_null(),
                                            }
                                        }
                                        list_builder.append(true);
                                    }
                                    DataType::Int32 => {
                                        let int_builder = list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Int32Builder>()
                                            .ok_or_else(|| unsupported(field_type))?;
                                        for item in json_array {
                                            match item {
                                                JsonValue::Number(n) if n.is_i64() => {
                                                    if let Some(val) = n.as_i64() {
                                                        int_builder.append_value(val as i32);
                                                    } else {
                                                        int_builder.append_null();
                                                    }
                                                }
                                                _ => int_builder.append_null(),
                                            }
                                        }
                                        list_builder.append(true);
                                    }
                                    DataType::Int64 => {
                                        let int_builder = list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Int64Builder>()
                                            .ok_or_else(|| unsupported(field_type))?;
                                        for item in json_array {
                                            match item {
                                                JsonValue::Number(n) if n.is_i64() => {
                                                    if let Some(val) = n.as_i64() {
                                                        int_builder.append_value(val);
                                                    } else {
                                                        int_builder.append_null();
                                                    }
                                                }
                                                _ => int_builder.append_null(),
                                            }
                                        }
                                        list_builder.append(true);
                                    }
                                    DataType::Float32 => {
                                        let float_builder = list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Float32Builder>()
                                            .ok_or_else(|| unsupported(field_type))?;
                                        for item in json_array {
                                            match item {
                                                JsonValue::Number(n) if n.is_f64() => {
                                                    if let Some(val) = n.as_f64() {
                                                        float_builder.append_value(val as f32);
                                                    } else {
                                                        float_builder.append_null();
                                                    }
                                                }
                                                _ => float_builder.append_null(),
                                            }
                                        }
                                        list_builder.append(true);
                                    }
                                    DataType::Float64 => {
                                        let float_builder = list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Float64Builder>()
                                            .ok_or_else(|| unsupported(field_type))?;
                                        for item in json_array {
                                            match item {
                                                JsonValue::Number(n) if n.is_f64() => {
                                                    if let Some(val) = n.as_f64() {
                                                        float_builder.append_value(val);
                                                    } else {
                                                        float_builder.append_null();
                                                    }
                                                }
                                                _ => float_builder.append_null(),
                                            }
                                        }
                                        list_builder.append(true);
                                    }
                                    DataType::Utf8 => {
                                        let string_builder = list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<StringBuilder>()
                                            .ok_or_else(|| unsupported(field_type))?;
                                        for item in json_array {
                                            match item {
                                                JsonValue::String(s) => {
                                                    string_builder.append_value(s)
                                                }
                                                _ => string_builder.append_null(),
                                            }
                                        }
                                        list_builder.append(true);
                                    }
                                    _ => {
                                        return Err(unsupported(field_type));
                                    }
                                }
                            } else {
                                return Err(unsupported(field_type));
                            }
                        }
                        _ => {
                            list_builder.append(false);
                        }
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            DataType::Map(field, _sorted) => {
                if let Some(map_builder) = builder
                    .as_any_mut()
                    .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                {
                    match cell {
                        Some(Cell::Json(json_val)) => {
                            if let Some(json_object) = json_val.0.as_object() {
                                // extract key and value types from the struct field
                                if let DataType::Struct(struct_fields) = field.data_type() {
                                    if struct_fields.len() >= 2 {
                                        let key_field = &struct_fields[0];
                                        let value_field = &struct_fields[1];

                                        // process each key-value pair in the JSON object
                                        // first process all keys, only accept string as key
                                        for (key, _) in json_object {
                                            match key_field.data_type() {
                                                DataType::Utf8 => {
                                                    let key_builder = map_builder.keys();
                                                    if let Some(str_builder) = key_builder
                                                        .as_any_mut()
                                                        .downcast_mut::<StringBuilder>()
                                                    {
                                                        str_builder.append_value(key);
                                                    }
                                                }
                                                _ => {
                                                    return Err(unsupported(field_type));
                                                }
                                            }
                                        }

                                        // then process all values
                                        for (_, value) in json_object {
                                            match value_field.data_type() {
                                                DataType::Boolean => {
                                                    let value_builder = map_builder.values();
                                                    if let Some(bool_builder) = value_builder
                                                        .as_any_mut()
                                                        .downcast_mut::<BooleanBuilder>()
                                                    {
                                                        match value {
                                                            JsonValue::Bool(v) => {
                                                                bool_builder.append_value(*v)
                                                            }
                                                            _ => bool_builder.append_null(),
                                                        }
                                                    }
                                                }
                                                DataType::Int32 => {
                                                    let value_builder = map_builder.values();
                                                    if let Some(int_builder) = value_builder
                                                        .as_any_mut()
                                                        .downcast_mut::<Int32Builder>()
                                                    {
                                                        match value {
                                                            JsonValue::Number(n) if n.is_i64() => {
                                                                if let Some(val) = n.as_i64() {
                                                                    int_builder
                                                                        .append_value(val as i32);
                                                                } else {
                                                                    int_builder.append_null();
                                                                }
                                                            }
                                                            _ => int_builder.append_null(),
                                                        }
                                                    }
                                                }
                                                DataType::Int64 => {
                                                    let value_builder = map_builder.values();
                                                    if let Some(int_builder) = value_builder
                                                        .as_any_mut()
                                                        .downcast_mut::<Int64Builder>()
                                                    {
                                                        match value {
                                                            JsonValue::Number(n) if n.is_i64() => {
                                                                if let Some(val) = n.as_i64() {
                                                                    int_builder.append_value(val);
                                                                } else {
                                                                    int_builder.append_null();
                                                                }
                                                            }
                                                            _ => int_builder.append_null(),
                                                        }
                                                    }
                                                }
                                                DataType::Float32 => {
                                                    let value_builder = map_builder.values();
                                                    if let Some(float_builder) = value_builder
                                                        .as_any_mut()
                                                        .downcast_mut::<Float32Builder>()
                                                    {
                                                        match value {
                                                            JsonValue::Number(n) if n.is_f64() => {
                                                                if let Some(val) = n.as_f64() {
                                                                    float_builder
                                                                        .append_value(val as f32);
                                                                } else {
                                                                    float_builder.append_null();
                                                                }
                                                            }
                                                            _ => float_builder.append_null(),
                                                        }
                                                    }
                                                }
                                                DataType::Float64 => {
                                                    let value_builder = map_builder.values();
                                                    if let Some(float_builder) = value_builder
                                                        .as_any_mut()
                                                        .downcast_mut::<Float64Builder>()
                                                    {
                                                        match value {
                                                            JsonValue::Number(n) if n.is_f64() => {
                                                                if let Some(val) = n.as_f64() {
                                                                    float_builder.append_value(val);
                                                                } else {
                                                                    float_builder.append_null();
                                                                }
                                                            }
                                                            _ => float_builder.append_null(),
                                                        }
                                                    }
                                                }
                                                DataType::Utf8 => {
                                                    let value_builder = map_builder.values();
                                                    if let Some(str_builder) = value_builder
                                                        .as_any_mut()
                                                        .downcast_mut::<StringBuilder>()
                                                    {
                                                        match value {
                                                            JsonValue::String(s) => {
                                                                str_builder.append_value(s)
                                                            }
                                                            _ => str_builder.append_null(),
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    return Err(unsupported(field_type));
                                                }
                                            }
                                        }

                                        map_builder.append(true)?;
                                    } else {
                                        map_builder.append(false)?;
                                    }
                                } else {
                                    map_builder.append(false)?;
                                }
                            } else {
                                map_builder.append(false)?;
                            }
                        }
                        _ => {
                            map_builder.append(false)?;
                        }
                    }
                } else {
                    return Err(unsupported(field_type));
                }
            }
            _ => return Err(unsupported(field_type)),
        }

        Ok(())
    }

    // map Postgres table schema to Iceberg table schema
    pub(super) fn map_table_schema(&self, ftable_oid: u32) -> IcebergFdwResult<Schema> {
        let schema_info = Spi::connect(|client| {
            let mut schema_info = Vec::new();

            let rows = client.select(
                "SELECT
                   a.attname::text,
                   t.typname::text,
                   CASE
                     WHEN t.typname = 'numeric' AND a.atttypmod > -1
                     THEN ((a.atttypmod - 4) >> 16) & 65535
                     ELSE NULL
                   END AS precision,
                   CASE
                     WHEN t.typname = 'numeric' AND a.atttypmod > -1
                     THEN (a.atttypmod - 4) & 65535
                     ELSE NULL
                   END AS scale,
                   a.attnotnull
                 FROM pg_attribute a
                 JOIN pg_type t ON a.atttypid = t.oid
                 WHERE a.attrelid = $1
                   AND a.attnum > 0
                   AND NOT a.attisdropped
                 ORDER BY a.attnum",
                None,
                &[pg_sys::Oid::from_u32(ftable_oid).into()],
            )?;
            for row in rows {
                let col_name = row.get_by_name::<String, _>("attname")?;
                let col_type_name = row.get_by_name::<String, _>("typname")?;
                let precision = row.get_by_name::<i32, _>("precision")?;
                let scale = row.get_by_name::<i32, _>("scale")?;
                let is_not_null = row.get_by_name::<bool, _>("attnotnull")?;
                if let (Some(col_name), Some(col_type_name), Some(is_not_null)) =
                    (col_name, col_type_name, is_not_null)
                {
                    schema_info.push((col_name, col_type_name, precision, scale, is_not_null));
                }
            }

            Ok::<Vec<_>, pgrx::spi::Error>(schema_info)
        })?;

        let mut fields: Vec<NestedFieldRef> = Vec::new();

        for (idx, (col_name, col_type_name, precision, scale, is_not_null)) in
            schema_info.iter().enumerate()
        {
            let col_id = idx as i32 + 1;
            let col_type = match col_type_name.as_str() {
                "boolean" | "bool" => Type::Primitive(PrimitiveType::Boolean),
                "integer" | "int4" => Type::Primitive(PrimitiveType::Int),
                "bigint" | "int8" => Type::Primitive(PrimitiveType::Long),
                "real" | "float4" => Type::Primitive(PrimitiveType::Float),
                "double precision" | "float8" => Type::Primitive(PrimitiveType::Double),
                "numeric" | "decimal" => {
                    let precision = precision.unwrap_or(38) as u32;
                    let scale = scale.unwrap_or(0) as u32;
                    Type::Primitive(PrimitiveType::Decimal { precision, scale })
                }
                "text" => Type::Primitive(PrimitiveType::String),
                "date" => Type::Primitive(PrimitiveType::Date),
                "time" => Type::Primitive(PrimitiveType::Time),
                "timestamp" => Type::Primitive(PrimitiveType::Timestamp),
                "timestamp with time zone" => Type::Primitive(PrimitiveType::Timestamptz),
                "uuid" => Type::Primitive(PrimitiveType::Uuid),
                "bytea" => Type::Primitive(PrimitiveType::Binary),
                _ => return Err(IcebergFdwError::UnsupportedType(col_type_name.clone())),
            };

            let field = NestedField::new(col_id, col_name, col_type, *is_not_null);
            fields.push(field.into());
        }

        let schema = Schema::builder().with_fields(fields).build()?;

        Ok(schema)
    }
}
