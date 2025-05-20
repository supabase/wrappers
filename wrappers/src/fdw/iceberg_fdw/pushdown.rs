use super::{IcebergFdwError, IcebergFdwResult};
use chrono::NaiveDate;
use iceberg::{
    expr::{Predicate, Reference},
    spec::{Datum, PrimitiveType, Type},
    table::Table,
};
use pgrx::varlena;
use rust_decimal::Decimal;
use supabase_wrappers::prelude::*;
use uuid::Uuid;

// extract pattern string for Iceberg 'starts with' and 'not starts with' predicate
// e.g. return Some("name") for "like 'name%'"
fn extract_starts_with_pattern(s: &str) -> Option<&str> {
    if s.ends_with("%") && s.matches('%').count() == 1 {
        s.split('%').next()
    } else {
        None
    }
}

// convert Wrappers Cell to Iceberg Datum with some type flexibility
fn cell_to_iceberg_datum(cell: &Cell, tgt_type: &Type) -> IcebergFdwResult<Option<Datum>> {
    Ok(match cell {
        Cell::Bool(v) => Some(Datum::bool(*v)),
        Cell::F32(v) => match tgt_type {
            Type::Primitive(PrimitiveType::Float) => Some(Datum::float(*v)),
            Type::Primitive(PrimitiveType::Double) => Some(Datum::double(*v)),
            _ => None,
        },
        Cell::I32(v) => match tgt_type {
            Type::Primitive(PrimitiveType::Int) => Some(Datum::int(*v)),
            Type::Primitive(PrimitiveType::Long) => Some(Datum::long(*v)),
            Type::Primitive(PrimitiveType::Double) => Some(Datum::double(*v)),
            _ => None,
        },
        Cell::F64(v) => match tgt_type {
            Type::Primitive(PrimitiveType::Double) => Some(Datum::double(*v)),
            _ => None,
        },
        Cell::I64(v) => match tgt_type {
            Type::Primitive(PrimitiveType::Long) => Some(Datum::long(*v)),
            _ => None,
        },
        Cell::Numeric(v) => {
            let s = v.normalize();
            match tgt_type {
                Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
                    let mut d = Decimal::from_str_exact(s)?;
                    d.rescale(*scale);
                    Some(Datum::decimal_with_precision(d, *precision)?)
                }
                Type::Primitive(PrimitiveType::Float) => Some(Datum::float(s.parse::<f32>()?)),
                Type::Primitive(PrimitiveType::Double) => Some(Datum::double(s.parse::<f64>()?)),
                _ => None,
            }
        }
        Cell::String(v) => Some(Datum::string(v)),
        Cell::Date(v) => Some(Datum::date(v.to_unix_epoch_days())),
        Cell::Time(v) => {
            let (h, m, s, micro) = v.to_hms_micro();
            Some(Datum::time_from_hms_micro(h as _, m as _, s as _, micro)?)
        }
        Cell::Timestamp(v) => {
            let (h, m, s, micro) = v.to_hms_micro();
            let ts = NaiveDate::from_ymd_opt(v.year(), v.month() as _, v.day() as _)
                .and_then(|dt| {
                    dt.and_hms_micro_opt(h as _, m as _, s as _, micro - (s as u32) * 1_000_000)
                })
                .ok_or_else(|| IcebergFdwError::DatumConversionError(v.to_string()))?;
            Some(Datum::timestamp_from_datetime(ts))
        }
        Cell::Timestamptz(v) => {
            let v = v.to_utc();
            let (h, m, s, micro) = v.to_hms_micro();
            let ts = NaiveDate::from_ymd_opt(v.year(), v.month() as _, v.day() as _)
                .and_then(|dt| {
                    dt.and_hms_micro_opt(h as _, m as _, s as _, micro - (s as u32) * 1_000_000)
                })
                .ok_or_else(|| IcebergFdwError::DatumConversionError(v.to_string()))?;
            let tstz = ts.and_utc();
            Some(Datum::timestamptz_from_datetime(tstz))
        }
        Cell::Bytea(v) => {
            let bytes = unsafe { varlena::varlena_to_byte_slice(*v) };
            Some(Datum::binary(bytes.iter().copied()))
        }
        Cell::Uuid(v) => Some(Datum::uuid(Uuid::from_bytes(*v.as_bytes()))),
        _ => None,
    })
}

// try to translate quals to predicates and push them down to Iceberg,
// return None if pushdown is not possible
pub(super) fn try_pushdown(table: &Table, quals: &[Qual]) -> IcebergFdwResult<Option<Predicate>> {
    let schema = table.metadata().current_schema();
    let mut preds: Vec<Predicate> = Vec::new();

    for qual in quals {
        if let Some(field) = schema.field_by_name(&qual.field) {
            let term = Reference::new(&qual.field);
            let tgt_type = field.field_type.as_ref();

            match &qual.value {
                Value::Array(cells) => {
                    let datums = cells
                        .iter()
                        .map(|cell| cell_to_iceberg_datum(cell, tgt_type))
                        .collect::<Result<Vec<_>, _>>()?;
                    // push down the whole predicate only when each datum can be pushed down
                    if datums.iter().any(|d| d.is_none()) {
                        continue;
                    }
                    let datums: Vec<Datum> = datums.into_iter().flatten().collect();

                    if qual.use_or && qual.operator == "=" {
                        preds.push(term.is_in(datums));
                    } else if !qual.use_or && qual.operator == "<>" {
                        preds.push(term.is_not_in(datums));
                    } else {
                        continue;
                    }
                }
                Value::Cell(cell) => {
                    if let Some(datum) = cell_to_iceberg_datum(cell, tgt_type)? {
                        let pred = match qual.operator.as_str() {
                            "=" => term.equal_to(datum),
                            "<" => term.less_than(datum),
                            "<=" => term.less_than_or_equal_to(datum),
                            ">" => term.greater_than(datum),
                            ">=" => term.greater_than_or_equal_to(datum),
                            "<>" => term.not_equal_to(datum),
                            "is" => match cell {
                                Cell::Bool(_) => term.equal_to(datum),
                                Cell::String(v) if v == "null" => term.is_null(),
                                _ => {
                                    continue;
                                }
                            },
                            "is not" => match cell {
                                Cell::Bool(_) => term.not_equal_to(datum),
                                Cell::String(s) if s == "null" => term.is_not_null(),
                                _ => {
                                    continue;
                                }
                            },
                            "~~" => match cell {
                                Cell::String(s) => {
                                    if let Some(p) = extract_starts_with_pattern(s) {
                                        term.starts_with(Datum::string(p))
                                    } else {
                                        continue;
                                    }
                                }
                                _ => {
                                    continue;
                                }
                            },
                            "!~~" => match cell {
                                Cell::String(s) => {
                                    if let Some(p) = extract_starts_with_pattern(s) {
                                        term.not_starts_with(Datum::string(p))
                                    } else {
                                        continue;
                                    }
                                }
                                _ => {
                                    continue;
                                }
                            },
                            _ => {
                                continue;
                            }
                        };
                        preds.push(pred);
                    }
                }
            }
        }
    }

    // use logical 'AND' for multiple predicates
    let ret = preds.into_iter().reduce(|ret, p| ret.and(p));

    if cfg!(debug_assertions) {
        log_debug1(&format!("pushdown predicate: {:?}", ret));
    }

    Ok(ret)
}
