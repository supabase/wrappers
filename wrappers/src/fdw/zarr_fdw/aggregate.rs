use std::cmp::Ordering;

use pgrx::{AnyNumeric, pg_sys};
use supabase_wrappers::prelude::{Aggregate, AggregateKind, Cell, ParamValue, Qual, Value};

use super::{ZarrFdwError, ZarrFdwResult};

const SUPPORTED_SCALAR_OIDS: &[pg_sys::Oid] = &[
    pg_sys::CHAROID,
    pg_sys::INT2OID,
    pg_sys::INT4OID,
    pg_sys::INT8OID,
    pg_sys::FLOAT4OID,
    pg_sys::FLOAT8OID,
    pg_sys::TIMESTAMPTZOID,
];

pub(crate) fn aggregate_signature_supported(aggregate: &Aggregate) -> bool {
    if aggregate.distinct {
        return false;
    }

    match aggregate.kind {
        AggregateKind::Count => aggregate.column.is_none() && aggregate.type_oid == pg_sys::INT8OID,
        AggregateKind::CountColumn => aggregate.column.as_ref().is_some_and(|column| {
            SUPPORTED_SCALAR_OIDS.contains(&column.type_oid)
                && aggregate.type_oid == pg_sys::INT8OID
        }),
        AggregateKind::Sum => aggregate.column.as_ref().is_some_and(|column| {
            matches!(
                (column.type_oid, aggregate.type_oid),
                (pg_sys::INT2OID | pg_sys::INT4OID, pg_sys::INT8OID)
                    | (pg_sys::INT8OID, pg_sys::NUMERICOID)
                    | (pg_sys::FLOAT4OID, pg_sys::FLOAT4OID)
                    | (pg_sys::FLOAT8OID, pg_sys::FLOAT8OID)
            )
        }),
        AggregateKind::Avg => aggregate.column.as_ref().is_some_and(|column| {
            matches!(
                (column.type_oid, aggregate.type_oid),
                (
                    pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID,
                    pg_sys::NUMERICOID
                ) | (pg_sys::FLOAT4OID | pg_sys::FLOAT8OID, pg_sys::FLOAT8OID)
            )
        }),
        AggregateKind::Min | AggregateKind::Max => {
            aggregate.column.as_ref().is_some_and(|column| {
                SUPPORTED_SCALAR_OIDS.contains(&column.type_oid)
                    && aggregate.type_oid == column.type_oid
            })
        }
    }
}

fn qual_cell_supported(cell: &Cell) -> bool {
    matches!(
        cell,
        Cell::I8(_)
            | Cell::I16(_)
            | Cell::I32(_)
            | Cell::I64(_)
            | Cell::F32(_)
            | Cell::F64(_)
            | Cell::Timestamptz(_)
    )
}

pub(crate) fn qual_shape_supported(qual: &Qual) -> bool {
    if qual.use_or {
        return qual.operator == "="
            && matches!(&qual.value, Value::Array(values) if values.iter().all(qual_cell_supported));
    }

    match (&*qual.operator, &qual.value) {
        ("is" | "is not", Value::Cell(Cell::String(value))) => value == "null",
        ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=", Value::Cell(cell)) => {
            qual_cell_supported(cell)
        }
        _ => false,
    }
}

pub(crate) fn qual_matches(qual: &Qual, source: Option<&Cell>) -> ZarrFdwResult<bool> {
    if !qual_shape_supported(qual) {
        return Err(aggregate_error(format!(
            "qualifier on '{}' with operator '{}' is not supported for exact evaluation",
            qual.field, qual.operator
        )));
    }

    match qual.operator.as_str() {
        "is" => return Ok(source.is_none()),
        "is not" => return Ok(source.is_some()),
        _ => {}
    }

    let Some(source) = source else {
        // SQL comparisons with NULL evaluate to unknown, which WHERE rejects.
        return Ok(false);
    };

    let evaluated_value = qual.param.as_ref().map(|_| qual.evaluated_value());
    let value = match evaluated_value.as_ref() {
        None => &qual.value,
        Some(ParamValue::Value(value)) => value,
        Some(ParamValue::Null) => return Ok(false),
        Some(ParamValue::Unevaluated) => {
            return Err(aggregate_error(format!(
                "parameter for qualifier on '{}' was not evaluated before aggregate execution",
                qual.field
            )));
        }
    };

    if qual.use_or {
        let Value::Array(values) = value else {
            unreachable!("qual_shape_supported checked equality IN values")
        };
        return values.iter().try_fold(false, |matched, value| {
            Ok(matched || compare_cells(source, value)? == Ordering::Equal)
        });
    }

    let Value::Cell(value) = value else {
        unreachable!("qual_shape_supported checked scalar comparison value")
    };
    let ordering = compare_cells(source, value)?;
    Ok(match qual.operator.as_str() {
        "=" => ordering == Ordering::Equal,
        "<>" | "!=" => ordering != Ordering::Equal,
        "<" => ordering == Ordering::Less,
        "<=" => ordering != Ordering::Greater,
        ">" => ordering == Ordering::Greater,
        ">=" => ordering != Ordering::Less,
        _ => unreachable!("qual_shape_supported checked comparison operator"),
    })
}

fn compare_f32(left: f32, right: f32) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left
            .partial_cmp(&right)
            .expect("non-NaN floats always have an ordering"),
    }
}

fn compare_f64(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left
            .partial_cmp(&right)
            .expect("non-NaN floats always have an ordering"),
    }
}

fn compare_cells(left: &Cell, right: &Cell) -> ZarrFdwResult<Ordering> {
    if let (Some(left), Some(right)) = (integer_cell_as_i128(left), integer_cell_as_i128(right)) {
        return Ok(left.cmp(&right));
    }

    let ordering = match (left, right) {
        (Cell::I8(left), Cell::I8(right)) => left.cmp(right),
        (Cell::F32(left), Cell::F32(right)) => compare_f32(*left, *right),
        (Cell::F32(left), Cell::F64(right)) => compare_f64(f64::from(*left), *right),
        (Cell::F64(left), Cell::F32(right)) => compare_f64(*left, f64::from(*right)),
        (Cell::F64(left), Cell::F64(right)) => compare_f64(*left, *right),
        (Cell::Timestamptz(left), Cell::Timestamptz(right)) => {
            (*left).into_inner().cmp(&(*right).into_inner())
        }
        _ => {
            return Err(aggregate_error(format!(
                "cannot compare source cell {left:?} with qualifier or aggregate cell {right:?}"
            )));
        }
    };
    Ok(ordering)
}

fn integer_cell_as_i128(cell: &Cell) -> Option<i128> {
    match cell {
        Cell::I16(value) => Some(i128::from(*value)),
        Cell::I32(value) => Some(i128::from(*value)),
        Cell::I64(value) => Some(i128::from(*value)),
        _ => None,
    }
}

pub(crate) struct AggregateReducer {
    slots: Vec<AggregateSlot>,
}

struct AggregateSlot {
    alias: String,
    state: AggregateState,
}

enum AggregateState {
    Count {
        count: i64,
        nonnull_only: bool,
        input_oid: Option<pg_sys::Oid>,
    },
    Min {
        input_oid: pg_sys::Oid,
        value: Option<Cell>,
    },
    Max {
        input_oid: pg_sys::Oid,
        value: Option<Cell>,
    },
    SumI64 {
        input_oid: pg_sys::Oid,
        value: Option<i64>,
    },
    SumI128(Option<i128>),
    SumF32(Option<f32>),
    SumF64(Option<f64>),
    AvgI128 {
        input_oid: pg_sys::Oid,
        sum: i128,
        count: i64,
    },
    AvgF64 {
        input_oid: pg_sys::Oid,
        sum: Option<f64>,
        count: i64,
    },
}

impl AggregateReducer {
    pub(crate) fn new(aggregates: &[Aggregate]) -> ZarrFdwResult<Self> {
        let mut slots = Vec::new();
        slots.try_reserve_exact(aggregates.len()).map_err(|_| {
            aggregate_error(format!(
                "could not allocate state for {} aggregate expressions",
                aggregates.len()
            ))
        })?;

        for aggregate in aggregates {
            if !aggregate_signature_supported(aggregate) {
                return Err(aggregate_error(format!(
                    "unsupported aggregate signature: {} -> PostgreSQL type OID {}",
                    aggregate.deparse(),
                    aggregate.type_oid
                )));
            }

            let input_oid = aggregate.column.as_ref().map(|column| column.type_oid);
            let state = match aggregate.kind {
                AggregateKind::Count => AggregateState::Count {
                    count: 0,
                    nonnull_only: false,
                    input_oid: None,
                },
                AggregateKind::CountColumn => AggregateState::Count {
                    count: 0,
                    nonnull_only: true,
                    input_oid,
                },
                AggregateKind::Min => AggregateState::Min {
                    input_oid: input_oid.expect("supported MIN has an input column"),
                    value: None,
                },
                AggregateKind::Max => AggregateState::Max {
                    input_oid: input_oid.expect("supported MAX has an input column"),
                    value: None,
                },
                AggregateKind::Sum => match input_oid {
                    Some(pg_sys::INT2OID | pg_sys::INT4OID) => AggregateState::SumI64 {
                        input_oid: input_oid.expect("matched an integer input OID"),
                        value: None,
                    },
                    Some(pg_sys::INT8OID) => AggregateState::SumI128(None),
                    Some(pg_sys::FLOAT4OID) => AggregateState::SumF32(None),
                    Some(pg_sys::FLOAT8OID) => AggregateState::SumF64(None),
                    _ => unreachable!("aggregate signature was checked above"),
                },
                AggregateKind::Avg => match input_oid {
                    Some(oid @ (pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID)) => {
                        AggregateState::AvgI128 {
                            input_oid: oid,
                            sum: 0,
                            count: 0,
                        }
                    }
                    Some(oid @ (pg_sys::FLOAT4OID | pg_sys::FLOAT8OID)) => AggregateState::AvgF64 {
                        input_oid: oid,
                        sum: None,
                        count: 0,
                    },
                    _ => unreachable!("aggregate signature was checked above"),
                },
            };
            slots.push(AggregateSlot {
                alias: aggregate.alias.clone(),
                state,
            });
        }

        Ok(Self { slots })
    }

    pub(crate) fn observe(&mut self, values: &[Option<&Cell>]) -> ZarrFdwResult<()> {
        if values.len() != self.slots.len() {
            return Err(aggregate_error(format!(
                "received {} source values for {} aggregate expressions",
                values.len(),
                self.slots.len()
            )));
        }

        for (slot, value) in self.slots.iter_mut().zip(values) {
            slot.state.observe(*value)?;
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> ZarrFdwResult<Vec<(String, Option<Cell>)>> {
        self.slots
            .into_iter()
            .map(|slot| Ok((slot.alias, slot.state.finish()?)))
            .collect()
    }
}

impl AggregateState {
    fn observe(&mut self, value: Option<&Cell>) -> ZarrFdwResult<()> {
        match self {
            Self::Count {
                count,
                nonnull_only,
                input_oid,
            } => {
                if *nonnull_only && value.is_none() {
                    return Ok(());
                }
                if let (Some(oid), Some(cell)) = (*input_oid, value) {
                    require_cell_oid(cell, oid)?;
                }
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| aggregate_error("COUNT overflowed bigint"))?;
            }
            Self::Min {
                input_oid,
                value: min,
            } => {
                let Some(cell) = value else { return Ok(()) };
                require_cell_oid(cell, *input_oid)?;
                let replace = match min.as_ref() {
                    Some(current) => compare_cells(cell, current)? != Ordering::Greater,
                    None => true,
                };
                if replace {
                    *min = Some(cell.clone());
                }
            }
            Self::Max {
                input_oid,
                value: max,
            } => {
                let Some(cell) = value else { return Ok(()) };
                require_cell_oid(cell, *input_oid)?;
                let replace = match max.as_ref() {
                    Some(current) => compare_cells(cell, current)? != Ordering::Less,
                    None => true,
                };
                if replace {
                    *max = Some(cell.clone());
                }
            }
            Self::SumI64 {
                input_oid,
                value: sum,
            } => {
                let Some(cell) = value else { return Ok(()) };
                let next = integer_cell(cell, *input_oid)?;
                *sum = Some(match *sum {
                    Some(current) => current
                        .checked_add(next)
                        .ok_or_else(|| aggregate_error("SUM overflowed bigint"))?,
                    None => next,
                });
            }
            Self::SumI128(sum) => {
                let Some(cell) = value else { return Ok(()) };
                let Cell::I64(next) = cell else {
                    return Err(cell_type_error(cell, pg_sys::INT8OID));
                };
                *sum = Some(match *sum {
                    Some(current) => current
                        .checked_add(i128::from(*next))
                        .ok_or_else(|| aggregate_error("integer SUM accumulator overflowed"))?,
                    None => i128::from(*next),
                });
            }
            Self::SumF32(sum) => {
                let Some(cell) = value else { return Ok(()) };
                let Cell::F32(next) = cell else {
                    return Err(cell_type_error(cell, pg_sys::FLOAT4OID));
                };
                *sum = Some(match *sum {
                    Some(current) => checked_float_add_f32(current, *next)?,
                    None => *next,
                });
            }
            Self::SumF64(sum) => {
                let Some(cell) = value else { return Ok(()) };
                let Cell::F64(next) = cell else {
                    return Err(cell_type_error(cell, pg_sys::FLOAT8OID));
                };
                *sum = Some(match *sum {
                    Some(current) => checked_float_add_f64(current, *next)?,
                    None => *next,
                });
            }
            Self::AvgI128 {
                input_oid,
                sum,
                count,
            } => {
                let Some(cell) = value else { return Ok(()) };
                let next = i128::from(integer_cell(cell, *input_oid)?);
                *sum = sum
                    .checked_add(next)
                    .ok_or_else(|| aggregate_error("integer AVG accumulator overflowed"))?;
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| aggregate_error("AVG count overflowed bigint"))?;
            }
            Self::AvgF64 {
                input_oid,
                sum,
                count,
            } => {
                let Some(cell) = value else { return Ok(()) };
                let next = match (*input_oid, cell) {
                    (pg_sys::FLOAT4OID, Cell::F32(value)) => f64::from(*value),
                    (pg_sys::FLOAT8OID, Cell::F64(value)) => *value,
                    _ => return Err(cell_type_error(cell, *input_oid)),
                };
                *sum = Some(match *sum {
                    Some(current) => checked_float_add_f64(current, next)?,
                    None => next,
                });
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| aggregate_error("AVG count overflowed bigint"))?;
            }
        }
        Ok(())
    }

    fn finish(self) -> ZarrFdwResult<Option<Cell>> {
        match self {
            Self::Count { count, .. } => Ok(Some(Cell::I64(count))),
            Self::Min { value, .. } | Self::Max { value, .. } => Ok(value),
            Self::SumI64 { value, .. } => Ok(value.map(Cell::I64)),
            Self::SumI128(value) => value
                .map(numeric_from_i128)
                .transpose()
                .map(|value| value.map(Cell::Numeric)),
            Self::SumF32(value) => Ok(value.map(Cell::F32)),
            Self::SumF64(value) => Ok(value.map(Cell::F64)),
            Self::AvgI128 { sum, count, .. } => {
                if count == 0 {
                    return Ok(None);
                }
                let numerator = numeric_from_i128(sum)?;
                let denominator = AnyNumeric::from(count);
                Ok(Some(Cell::Numeric(numerator / denominator)))
            }
            Self::AvgF64 { sum, count, .. } => Ok(sum.map(|sum| Cell::F64(sum / count as f64))),
        }
    }
}

fn require_cell_oid(cell: &Cell, oid: pg_sys::Oid) -> ZarrFdwResult<()> {
    if cell_matches_oid(cell, oid) {
        Ok(())
    } else {
        Err(cell_type_error(cell, oid))
    }
}

fn cell_matches_oid(cell: &Cell, oid: pg_sys::Oid) -> bool {
    matches!(
        (oid, cell),
        (pg_sys::CHAROID, Cell::I8(_))
            | (pg_sys::INT2OID, Cell::I16(_))
            | (pg_sys::INT4OID, Cell::I32(_))
            | (pg_sys::INT8OID, Cell::I64(_))
            | (pg_sys::FLOAT4OID, Cell::F32(_))
            | (pg_sys::FLOAT8OID, Cell::F64(_))
            | (pg_sys::TIMESTAMPTZOID, Cell::Timestamptz(_))
    )
}

fn integer_cell(cell: &Cell, oid: pg_sys::Oid) -> ZarrFdwResult<i64> {
    match (oid, cell) {
        (pg_sys::INT2OID, Cell::I16(value)) => Ok(i64::from(*value)),
        (pg_sys::INT4OID, Cell::I32(value)) => Ok(i64::from(*value)),
        (pg_sys::INT8OID, Cell::I64(value)) => Ok(*value),
        _ => Err(cell_type_error(cell, oid)),
    }
}

fn checked_float_add_f32(left: f32, right: f32) -> ZarrFdwResult<f32> {
    let sum = left + right;
    if left.is_finite() && right.is_finite() && sum.is_infinite() {
        Err(aggregate_error("real SUM/AVG accumulator overflowed"))
    } else {
        Ok(sum)
    }
}

fn checked_float_add_f64(left: f64, right: f64) -> ZarrFdwResult<f64> {
    let sum = left + right;
    if left.is_finite() && right.is_finite() && sum.is_infinite() {
        Err(aggregate_error(
            "double precision SUM/AVG accumulator overflowed",
        ))
    } else {
        Ok(sum)
    }
}

fn numeric_from_i128(value: i128) -> ZarrFdwResult<AnyNumeric> {
    Ok(AnyNumeric::try_from(value.to_string().as_str())?)
}

fn aggregate_error(message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(format!("aggregate pushdown: {}", message.into()))
}

fn cell_type_error(cell: &Cell, oid: pg_sys::Oid) -> ZarrFdwError {
    aggregate_error(format!(
        "source cell {cell:?} does not match PostgreSQL input type OID {oid}"
    ))
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use supabase_wrappers::prelude::Column;

    fn aggregate(
        kind: AggregateKind,
        input_oid: Option<pg_sys::Oid>,
        type_oid: pg_sys::Oid,
        alias: &str,
    ) -> Aggregate {
        Aggregate {
            kind,
            column: input_oid.map(|type_oid| Column {
                name: "value".to_owned(),
                num: 1,
                type_oid,
            }),
            distinct: false,
            alias: alias.to_owned(),
            type_oid,
        }
    }

    fn qual(operator: &str, value: Value, use_or: bool) -> Qual {
        Qual {
            field: "value".to_owned(),
            operator: operator.to_owned(),
            value,
            use_or,
            param: None,
        }
    }

    #[test]
    fn signature_matrix_matches_postgres_result_types() {
        let supported = [
            aggregate(AggregateKind::Count, None, pg_sys::INT8OID, "c"),
            aggregate(
                AggregateKind::CountColumn,
                Some(pg_sys::FLOAT8OID),
                pg_sys::INT8OID,
                "c",
            ),
            aggregate(
                AggregateKind::Sum,
                Some(pg_sys::INT2OID),
                pg_sys::INT8OID,
                "s",
            ),
            aggregate(
                AggregateKind::Sum,
                Some(pg_sys::INT4OID),
                pg_sys::INT8OID,
                "s",
            ),
            aggregate(
                AggregateKind::Sum,
                Some(pg_sys::INT8OID),
                pg_sys::NUMERICOID,
                "s",
            ),
            aggregate(
                AggregateKind::Sum,
                Some(pg_sys::FLOAT4OID),
                pg_sys::FLOAT4OID,
                "s",
            ),
            aggregate(
                AggregateKind::Sum,
                Some(pg_sys::FLOAT8OID),
                pg_sys::FLOAT8OID,
                "s",
            ),
            aggregate(
                AggregateKind::Avg,
                Some(pg_sys::INT8OID),
                pg_sys::NUMERICOID,
                "a",
            ),
            aggregate(
                AggregateKind::Avg,
                Some(pg_sys::FLOAT4OID),
                pg_sys::FLOAT8OID,
                "a",
            ),
            aggregate(
                AggregateKind::Min,
                Some(pg_sys::CHAROID),
                pg_sys::CHAROID,
                "lo",
            ),
            aggregate(
                AggregateKind::Max,
                Some(pg_sys::TIMESTAMPTZOID),
                pg_sys::TIMESTAMPTZOID,
                "hi",
            ),
        ];
        assert!(supported.iter().all(aggregate_signature_supported));

        let mut distinct = supported[1].clone();
        distinct.distinct = true;
        assert!(!aggregate_signature_supported(&distinct));
        assert!(!aggregate_signature_supported(&aggregate(
            AggregateKind::Sum,
            Some(pg_sys::INT8OID),
            pg_sys::FLOAT8OID,
            "bad",
        )));
        assert!(!aggregate_signature_supported(&aggregate(
            AggregateKind::Avg,
            Some(pg_sys::CHAROID),
            pg_sys::NUMERICOID,
            "bad",
        )));
    }

    #[test]
    fn qualifier_matching_follows_sql_null_in_and_nan_semantics() {
        let is_null = qual("is", Value::Cell(Cell::String("null".to_owned())), false);
        let is_not_null = qual(
            "is not",
            Value::Cell(Cell::String("null".to_owned())),
            false,
        );
        assert!(qual_matches(&is_null, None).unwrap());
        assert!(!qual_matches(&is_null, Some(&Cell::F64(1.0))).unwrap());
        assert!(!qual_matches(&is_not_null, None).unwrap());

        let in_values = qual(
            "=",
            Value::Array(vec![Cell::F64(1.0), Cell::F64(f64::NAN)]),
            true,
        );
        assert!(qual_matches(&in_values, Some(&Cell::F64(f64::NAN))).unwrap());
        assert!(!qual_matches(&in_values, Some(&Cell::F64(2.0))).unwrap());
        assert!(!qual_matches(&in_values, None).unwrap());

        let greater = qual(">", Value::Cell(Cell::F64(10.0)), false);
        assert!(qual_matches(&greater, Some(&Cell::F64(f64::NAN))).unwrap());
        assert!(qual_matches(&greater, Some(&Cell::F64(f64::INFINITY))).unwrap());
        assert!(!qual_matches(&greater, Some(&Cell::F64(10.0))).unwrap());
        assert!(qual_matches(&greater, Some(&Cell::F64(11.0))).unwrap());

        let unsupported = qual("~~", Value::Cell(Cell::String("%".to_owned())), false);
        assert!(!qual_shape_supported(&unsupported));
        assert!(qual_matches(&unsupported, Some(&Cell::F64(1.0))).is_err());

        let float_cross_type = qual("=", Value::Cell(Cell::F64(-7.5)), false);
        assert!(qual_matches(&float_cross_type, Some(&Cell::F32(-7.5))).unwrap());

        let integer_cross_type = qual(">", Value::Cell(Cell::I64(32_000)), false);
        assert!(qual_matches(&integer_cross_type, Some(&Cell::I16(32_001))).unwrap());
    }

    #[test]
    fn reducer_preserves_order_aliases_nulls_and_float_ordering() {
        let aggregates = [
            aggregate(AggregateKind::Count, None, pg_sys::INT8OID, "all"),
            aggregate(
                AggregateKind::CountColumn,
                Some(pg_sys::FLOAT8OID),
                pg_sys::INT8OID,
                "present",
            ),
            aggregate(
                AggregateKind::Sum,
                Some(pg_sys::FLOAT8OID),
                pg_sys::FLOAT8OID,
                "sum",
            ),
            aggregate(
                AggregateKind::Min,
                Some(pg_sys::FLOAT8OID),
                pg_sys::FLOAT8OID,
                "min",
            ),
            aggregate(
                AggregateKind::Max,
                Some(pg_sys::FLOAT8OID),
                pg_sys::FLOAT8OID,
                "max",
            ),
        ];
        let mut reducer = AggregateReducer::new(&aggregates).unwrap();
        reducer.observe(&[None, None, None, None, None]).unwrap();
        let one = Cell::F64(-0.0);
        reducer
            .observe(&[None, Some(&one), Some(&one), Some(&one), Some(&one)])
            .unwrap();
        let nan = Cell::F64(f64::NAN);
        reducer
            .observe(&[None, Some(&nan), Some(&nan), Some(&nan), Some(&nan)])
            .unwrap();

        let values = reducer.finish().unwrap();
        assert_eq!(
            values
                .iter()
                .map(|(alias, _)| alias.as_str())
                .collect::<Vec<_>>(),
            ["all", "present", "sum", "min", "max"]
        );
        assert!(matches!(values[0].1, Some(Cell::I64(3))));
        assert!(matches!(values[1].1, Some(Cell::I64(2))));
        assert!(matches!(values[2].1, Some(Cell::F64(value)) if value.is_nan()));
        assert!(matches!(values[3].1, Some(Cell::F64(value)) if value == 0.0));
        assert!(matches!(values[4].1, Some(Cell::F64(value)) if value.is_nan()));
    }

    #[test]
    fn empty_and_all_null_inputs_return_postgres_results() {
        let aggregates = [
            aggregate(AggregateKind::Count, None, pg_sys::INT8OID, "all"),
            aggregate(
                AggregateKind::CountColumn,
                Some(pg_sys::INT4OID),
                pg_sys::INT8OID,
                "present",
            ),
            aggregate(
                AggregateKind::Sum,
                Some(pg_sys::INT4OID),
                pg_sys::INT8OID,
                "sum",
            ),
            aggregate(
                AggregateKind::Avg,
                Some(pg_sys::INT4OID),
                pg_sys::NUMERICOID,
                "avg",
            ),
            aggregate(
                AggregateKind::Min,
                Some(pg_sys::INT4OID),
                pg_sys::INT4OID,
                "min",
            ),
        ];
        let mut reducer = AggregateReducer::new(&aggregates).unwrap();
        reducer.observe(&[None, None, None, None, None]).unwrap();
        let values = reducer.finish().unwrap();
        assert!(matches!(values[0].1, Some(Cell::I64(1))));
        assert!(matches!(values[1].1, Some(Cell::I64(0))));
        assert!(values[2..].iter().all(|(_, value)| value.is_none()));

        let empty = AggregateReducer::new(&aggregates)
            .unwrap()
            .finish()
            .unwrap();
        assert!(matches!(empty[0].1, Some(Cell::I64(0))));
        assert!(matches!(empty[1].1, Some(Cell::I64(0))));
        assert!(empty[2..].iter().all(|(_, value)| value.is_none()));
    }

    #[test]
    fn checked_accumulators_report_overflow() {
        let mut count = AggregateState::Count {
            count: i64::MAX,
            nonnull_only: false,
            input_oid: None,
        };
        assert!(count.observe(None).is_err());

        let mut sum = AggregateState::SumI64 {
            input_oid: pg_sys::INT4OID,
            value: Some(i64::MAX),
        };
        assert!(sum.observe(Some(&Cell::I32(1))).is_err());

        let mut float_sum = AggregateState::SumF64(Some(f64::MAX));
        assert!(float_sum.observe(Some(&Cell::F64(f64::MAX))).is_err());
        let mut infinity = AggregateState::SumF64(Some(f64::INFINITY));
        assert!(infinity.observe(Some(&Cell::F64(1.0))).is_ok());
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use pgrx::pg_test;
    use supabase_wrappers::prelude::Column;

    fn float_min_max(oid: pg_sys::Oid, values: &[Cell]) -> (Cell, Cell) {
        let aggregates = [
            Aggregate {
                kind: AggregateKind::Min,
                column: Some(Column {
                    name: "value".to_owned(),
                    num: 1,
                    type_oid: oid,
                }),
                distinct: false,
                alias: "min".to_owned(),
                type_oid: oid,
            },
            Aggregate {
                kind: AggregateKind::Max,
                column: Some(Column {
                    name: "value".to_owned(),
                    num: 1,
                    type_oid: oid,
                }),
                distinct: false,
                alias: "max".to_owned(),
                type_oid: oid,
            },
        ];
        let mut reducer = AggregateReducer::new(&aggregates).unwrap();
        for value in values {
            reducer.observe(&[Some(value), Some(value)]).unwrap();
        }
        let values = reducer.finish().unwrap();
        (values[0].1.clone().unwrap(), values[1].1.clone().unwrap())
    }

    #[pg_test]
    fn integer_numeric_sum_and_average_are_exact() {
        let aggregates = [
            Aggregate {
                kind: AggregateKind::Sum,
                column: Some(Column {
                    name: "value".to_owned(),
                    num: 1,
                    type_oid: pg_sys::INT8OID,
                }),
                distinct: false,
                alias: "sum".to_owned(),
                type_oid: pg_sys::NUMERICOID,
            },
            Aggregate {
                kind: AggregateKind::Avg,
                column: Some(Column {
                    name: "value".to_owned(),
                    num: 1,
                    type_oid: pg_sys::INT8OID,
                }),
                distinct: false,
                alias: "avg".to_owned(),
                type_oid: pg_sys::NUMERICOID,
            },
        ];
        let mut reducer = AggregateReducer::new(&aggregates).unwrap();
        let first = Cell::I64(9_007_199_254_740_993);
        let second = Cell::I64(9_007_199_254_740_994);
        reducer.observe(&[Some(&first), Some(&first)]).unwrap();
        reducer.observe(&[Some(&second), Some(&second)]).unwrap();
        let values = reducer.finish().unwrap();

        let Some(Cell::Numeric(sum)) = &values[0].1 else {
            panic!("SUM(bigint) should return numeric")
        };
        let Some(Cell::Numeric(avg)) = &values[1].1 else {
            panic!("AVG(bigint) should return numeric")
        };
        assert_eq!(sum, &AnyNumeric::try_from("18014398509481987").unwrap());
        assert_eq!(avg, &AnyNumeric::try_from("9007199254740993.5").unwrap());
    }

    #[pg_test]
    fn float_min_max_signed_zero_matches_postgres() {
        for (sql, values) in [
            (
                "SELECT min(v), max(v) FROM unnest(ARRAY['0'::real, '-0'::real]) AS t(v)",
                vec![Cell::F32(0.0), Cell::F32(-0.0)],
            ),
            (
                "SELECT min(v), max(v) FROM unnest(ARRAY['-0'::real, '0'::real]) AS t(v)",
                vec![Cell::F32(-0.0), Cell::F32(0.0)],
            ),
            (
                "SELECT min(v), max(v) FROM unnest(ARRAY['NaN'::real, 'Infinity'::real, '-Infinity'::real]) AS t(v)",
                vec![
                    Cell::F32(f32::NAN),
                    Cell::F32(f32::INFINITY),
                    Cell::F32(f32::NEG_INFINITY),
                ],
            ),
        ] {
            let (pg_min, pg_max) = pgrx::Spi::connect(|client| {
                client
                    .select(sql, None, &[])
                    .unwrap()
                    .first()
                    .get_two::<f32, f32>()
                    .unwrap()
            });
            let (Cell::F32(fdw_min), Cell::F32(fdw_max)) =
                float_min_max(pg_sys::FLOAT4OID, &values)
            else {
                panic!("real MIN/MAX should return real cells")
            };
            assert_eq!(fdw_min.to_bits(), pg_min.unwrap().to_bits());
            assert_eq!(fdw_max.to_bits(), pg_max.unwrap().to_bits());
        }

        for (sql, values) in [
            (
                "SELECT min(v), max(v) FROM unnest(ARRAY['0'::double precision, '-0'::double precision]) AS t(v)",
                vec![Cell::F64(0.0), Cell::F64(-0.0)],
            ),
            (
                "SELECT min(v), max(v) FROM unnest(ARRAY['-0'::double precision, '0'::double precision]) AS t(v)",
                vec![Cell::F64(-0.0), Cell::F64(0.0)],
            ),
            (
                "SELECT min(v), max(v) FROM unnest(ARRAY['NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision]) AS t(v)",
                vec![
                    Cell::F64(f64::NAN),
                    Cell::F64(f64::INFINITY),
                    Cell::F64(f64::NEG_INFINITY),
                ],
            ),
        ] {
            let (pg_min, pg_max) = pgrx::Spi::connect(|client| {
                client
                    .select(sql, None, &[])
                    .unwrap()
                    .first()
                    .get_two::<f64, f64>()
                    .unwrap()
            });
            let (Cell::F64(fdw_min), Cell::F64(fdw_max)) =
                float_min_max(pg_sys::FLOAT8OID, &values)
            else {
                panic!("double precision MIN/MAX should return double precision cells")
            };
            assert_eq!(fdw_min.to_bits(), pg_min.unwrap().to_bits());
            assert_eq!(fdw_max.to_bits(), pg_max.unwrap().to_bits());
        }
    }
}
