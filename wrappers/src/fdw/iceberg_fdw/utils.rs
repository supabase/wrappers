use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use iceberg::spec::{Literal, PrimitiveLiteral, Struct, TableMetadata, Transform};
use std::collections::HashMap;

use super::{Cell, IcebergFdwResult, InputRow};
use crate::fdw::iceberg_fdw::IcebergFdwError;

// copy an option to another in an option HashMap, if the target option
// doesn't exist
pub(super) fn copy_option(map: &mut HashMap<String, String>, from_key: &str, to_key: &str) {
    if !map.contains_key(to_key) {
        let value = map.get(from_key).cloned().unwrap_or_default();
        map.insert(to_key.to_string(), value);
    }
}

/// Convert seconds since Unix epoch to years since Unix epoch
fn seconds_to_years(seconds: i64) -> i64 {
    // unix epoch is 1970-01-01
    // this is a simplified calculation that treats each year as 365.25 days
    // more precise implementations would account for leap years properly
    seconds / (365.25 * 24.0 * 60.0 * 60.0) as i64
}

/// Convert seconds since Unix epoch to months since Unix epoch
fn seconds_to_months(seconds: i64) -> i64 {
    // unix epoch is 1970-01-01
    // this is a simplified calculation that treats each month as 30.44 days (365.25/12)
    // more precise implementations would account for varying month lengths
    seconds / (30.44 * 24.0 * 60.0 * 60.0) as i64
}

/// Compute partition key for an input row
pub(super) fn compute_partition_key(
    metadata: &iceberg::spec::TableMetadata,
    schema: &iceberg::spec::Schema,
    row: &InputRow,
) -> IcebergFdwResult<String> {
    let partition_spec = metadata.default_partition_spec();

    // if no partition spec, return empty string
    if partition_spec.fields().is_empty() {
        return Ok("".to_string());
    }

    let mut key_parts = Vec::new();

    for partition_field in partition_spec.fields() {
        let source_field_id = partition_field.source_id;
        let field_name = &partition_field.name;
        let transform = &partition_field.transform;

        // find the column index for this field ID in the schema
        let mut source_column_index = None;
        for (idx, field) in schema.as_struct().fields().iter().enumerate() {
            if field.id == source_field_id {
                source_column_index = Some(idx);
                break;
            }
        }
        let column_index = source_column_index.ok_or_else(|| {
            IcebergFdwError::ColumnNotFound(format!(
                "cannot find source column with ID {source_field_id} for partition field",
            ))
        })?;

        // get the cell value for this column
        let partition_value_str = match row.cells.get(column_index) {
            Some(Some(cell)) => {
                // Apply transform based on the partition field's transform type
                match transform {
                    Transform::Identity => {
                        // For identity transform, use the raw value
                        cell.to_string()
                    }
                    Transform::Day => {
                        // Convert timestamp/date to days since epoch
                        match cell {
                            Cell::Date(date) => {
                                let days = date.to_unix_epoch_days() as i64;
                                days.to_string()
                            }
                            Cell::Timestamp(ts) => {
                                // Convert microsecond timestamp to days since epoch
                                let timestamp_us = i64::from(*ts);
                                let days = timestamp_us / (24 * 60 * 60 * 1_000_000);
                                days.to_string()
                            }
                            Cell::Timestamptz(tstz) => {
                                let timestamp_us = i64::from(*tstz);
                                let days = timestamp_us / (24 * 60 * 60 * 1_000_000);
                                days.to_string()
                            }
                            _ => {
                                return Err(IcebergFdwError::UnsupportedType(
                                    "expected date or timestamp type for day partition".to_string(),
                                ));
                            }
                        }
                    }
                    Transform::Month => {
                        // Convert timestamp/date to months since epoch
                        match cell {
                            Cell::Date(date) => {
                                let days = date.to_unix_epoch_days() as i64;
                                let seconds_since_epoch = days * 24 * 60 * 60;
                                seconds_to_months(seconds_since_epoch).to_string()
                            }
                            Cell::Timestamp(ts) => {
                                // Convert microsecond timestamp to months since epoch
                                let timestamp_us = i64::from(*ts);
                                let seconds_since_epoch = timestamp_us / 1_000_000;
                                seconds_to_months(seconds_since_epoch).to_string()
                            }
                            Cell::Timestamptz(tstz) => {
                                let timestamp_us = i64::from(*tstz);
                                let seconds_since_epoch = timestamp_us / 1_000_000;
                                seconds_to_months(seconds_since_epoch).to_string()
                            }
                            _ => {
                                return Err(IcebergFdwError::UnsupportedType(
                                    "expected date or timestamp type for month partition"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    Transform::Year => {
                        // Convert timestamp/date to years since epoch
                        match cell {
                            Cell::Date(date) => {
                                let days = date.to_unix_epoch_days() as i64;
                                let seconds_since_epoch = days * 24 * 60 * 60;
                                seconds_to_years(seconds_since_epoch).to_string()
                            }
                            Cell::Timestamp(ts) => {
                                // Convert microsecond timestamp to years since epoch
                                let timestamp_us = i64::from(*ts);
                                let seconds_since_epoch = timestamp_us / 1_000_000;
                                seconds_to_years(seconds_since_epoch).to_string()
                            }
                            Cell::Timestamptz(tstz) => {
                                let timestamp_us = i64::from(*tstz);
                                let seconds_since_epoch = timestamp_us / 1_000_000;
                                seconds_to_years(seconds_since_epoch).to_string()
                            }
                            _ => {
                                return Err(IcebergFdwError::UnsupportedType(
                                    "expected date or timestamp type for year partition"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    Transform::Hour => {
                        // Convert timestamp/date to hours since epoch
                        match cell {
                            Cell::Date(date) => {
                                let days = date.to_unix_epoch_days() as i64;
                                let hours = days * 24;
                                hours.to_string()
                            }
                            Cell::Timestamp(ts) => {
                                // Convert microsecond timestamp to hours since epoch
                                let timestamp_us = i64::from(*ts);
                                let hours = timestamp_us / (60 * 60 * 1_000_000);
                                hours.to_string()
                            }
                            Cell::Timestamptz(tstz) => {
                                let timestamp_us = i64::from(*tstz);
                                let hours = timestamp_us / (60 * 60 * 1_000_000);
                                hours.to_string()
                            }
                            _ => {
                                return Err(IcebergFdwError::UnsupportedType(
                                    "expected date or timestamp type for hour partition"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    _ => {
                        return Err(IcebergFdwError::UnsupportedType(format!(
                            "unsupported partition transform: {transform:?}",
                        )));
                    }
                }
            }
            _ => "null".to_string(), // Handle null values
        };

        key_parts.push(format!("{field_name}={partition_value_str}"));
    }

    Ok(key_parts.join("/"))
}

/// Compute partition information for a specific row, returning both the string key and struct value
pub(super) fn compute_partition_info(
    metadata: &TableMetadata,
    record_batch: &RecordBatch,
    row_idx: usize,
) -> IcebergFdwResult<(String, Option<Struct>)> {
    let partition_spec = metadata.default_partition_spec();

    // if no partition spec, return empty values
    if partition_spec.fields().is_empty() {
        return Ok(("".to_string(), None));
    }

    let schema = metadata.current_schema();
    let mut key_parts = Vec::new();
    let mut partition_values = Vec::new();

    for partition_field in partition_spec.fields() {
        let source_field_id = partition_field.source_id;
        let field_name = &partition_field.name;
        let transform = &partition_field.transform;

        // find the column index for this field ID in the schema
        let mut source_column_index = None;
        for (idx, field) in schema.as_ref().as_struct().fields().iter().enumerate() {
            if field.id == source_field_id {
                source_column_index = Some(idx);
                break;
            }
        }
        let column_index = source_column_index.ok_or_else(|| {
            IcebergFdwError::ColumnNotFound(format!(
                "cannot find source column with ID {source_field_id} for partition field",
            ))
        })?;

        // get the column data from record batch
        let column = record_batch.column(column_index);

        // extract partition value for this specific row
        match transform {
            Transform::Day => {
                let mut days_since_epoch = None;

                if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_us = timestamp_array.value(row_idx);
                        days_since_epoch = Some(timestamp_us / (24 * 60 * 60 * 1_000_000));
                    }
                } else if let Some(date_array) = column.as_any().downcast_ref::<Date32Array>() {
                    if !date_array.is_null(row_idx) {
                        let days = date_array.value(row_idx) as i64;
                        days_since_epoch = Some(days);
                    }
                } else if let Some(date_array) = column.as_any().downcast_ref::<Date64Array>() {
                    if !date_array.is_null(row_idx) {
                        let timestamp_ms = date_array.value(row_idx);
                        days_since_epoch = Some(timestamp_ms / (24 * 60 * 60 * 1_000));
                    }
                } else {
                    return Err(IcebergFdwError::UnsupportedType(
                        "expected timestamp or date array type for day partition".to_string(),
                    ));
                }

                if let Some(days) = days_since_epoch {
                    key_parts.push(format!("{field_name}={days}"));
                    partition_values
                        .push(Some(Literal::Primitive(PrimitiveLiteral::Int(days as i32))));
                } else {
                    key_parts.push(format!("{field_name}=null",));
                    partition_values.push(None);
                }
            }
            Transform::Year => {
                let mut years_since_epoch = None;

                if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_us = timestamp_array.value(row_idx);
                        let seconds_since_epoch = timestamp_us / 1_000_000;
                        years_since_epoch = Some(seconds_to_years(seconds_since_epoch));
                    }
                } else if let Some(date_array) = column.as_any().downcast_ref::<Date32Array>() {
                    if !date_array.is_null(row_idx) {
                        let days = date_array.value(row_idx) as i64;
                        let seconds_since_epoch = days * 24 * 60 * 60;
                        years_since_epoch = Some(seconds_to_years(seconds_since_epoch));
                    }
                } else if let Some(date_array) = column.as_any().downcast_ref::<Date64Array>() {
                    if !date_array.is_null(row_idx) {
                        let timestamp_ms = date_array.value(row_idx);
                        let seconds_since_epoch = timestamp_ms / 1_000;
                        years_since_epoch = Some(seconds_to_years(seconds_since_epoch));
                    }
                } else {
                    return Err(IcebergFdwError::UnsupportedType(
                        "expected timestamp or date array type for year partition".to_string(),
                    ));
                }

                if let Some(years) = years_since_epoch {
                    key_parts.push(format!("{field_name}={years}"));
                    partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        years as i32,
                    ))));
                } else {
                    key_parts.push(format!("{field_name}=null",));
                    partition_values.push(None);
                }
            }
            Transform::Month => {
                let mut months_since_epoch = None;

                if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_us = timestamp_array.value(row_idx);
                        let seconds_since_epoch = timestamp_us / 1_000_000;
                        months_since_epoch = Some(seconds_to_months(seconds_since_epoch));
                    }
                } else if let Some(date_array) = column.as_any().downcast_ref::<Date32Array>() {
                    if !date_array.is_null(row_idx) {
                        let days = date_array.value(row_idx) as i64;
                        let seconds_since_epoch = days * 24 * 60 * 60;
                        months_since_epoch = Some(seconds_to_months(seconds_since_epoch));
                    }
                } else if let Some(date_array) = column.as_any().downcast_ref::<Date64Array>() {
                    if !date_array.is_null(row_idx) {
                        let timestamp_ms = date_array.value(row_idx);
                        let seconds_since_epoch = timestamp_ms / 1_000;
                        months_since_epoch = Some(seconds_to_months(seconds_since_epoch));
                    }
                } else {
                    return Err(IcebergFdwError::UnsupportedType(
                        "expected timestamp or date array type for month partition".to_string(),
                    ));
                }

                if let Some(months) = months_since_epoch {
                    key_parts.push(format!("{field_name}={months}"));
                    partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        months as i32,
                    ))));
                } else {
                    key_parts.push(format!("{field_name}=null",));
                    partition_values.push(None);
                }
            }
            Transform::Hour => {
                let mut hours_since_epoch = None;

                if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_us = timestamp_array.value(row_idx);
                        hours_since_epoch = Some(timestamp_us / (60 * 60 * 1_000_000));
                    }
                } else if let Some(date_array) = column.as_any().downcast_ref::<Date32Array>() {
                    if !date_array.is_null(row_idx) {
                        let days = date_array.value(row_idx) as i64;
                        hours_since_epoch = Some(days * 24);
                    }
                } else if let Some(date_array) = column.as_any().downcast_ref::<Date64Array>() {
                    if !date_array.is_null(row_idx) {
                        let timestamp_ms = date_array.value(row_idx);
                        hours_since_epoch = Some(timestamp_ms / (60 * 60 * 1_000));
                    }
                } else {
                    return Err(IcebergFdwError::UnsupportedType(
                        "expected timestamp or date array type for hour partition".to_string(),
                    ));
                }

                if let Some(hours) = hours_since_epoch {
                    key_parts.push(format!("{field_name}={hours}"));
                    partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        hours as i32,
                    ))));
                } else {
                    key_parts.push(format!("{field_name}=null",));
                    partition_values.push(None);
                }
            }
            Transform::Identity => {
                // for identity transform, use the raw value from the column
                if let Some(boolean_array) = column.as_any().downcast_ref::<BooleanArray>() {
                    if !boolean_array.is_null(row_idx) {
                        let value = boolean_array.value(row_idx);
                        key_parts.push(format!("{field_name}={value}"));
                        partition_values
                            .push(Some(Literal::Primitive(PrimitiveLiteral::Boolean(value))));
                    } else {
                        key_parts.push(format!("{field_name}=null",));
                        partition_values.push(None);
                    }
                } else if let Some(int32_array) = column.as_any().downcast_ref::<Int32Array>() {
                    if !int32_array.is_null(row_idx) {
                        let value = int32_array.value(row_idx);
                        key_parts.push(format!("{field_name}={value}"));
                        partition_values
                            .push(Some(Literal::Primitive(PrimitiveLiteral::Int(value))));
                    } else {
                        key_parts.push(format!("{field_name}=null",));
                        partition_values.push(None);
                    }
                } else if let Some(int64_array) = column.as_any().downcast_ref::<Int64Array>() {
                    if !int64_array.is_null(row_idx) {
                        let value = int64_array.value(row_idx);
                        key_parts.push(format!("{field_name}={value}"));
                        partition_values
                            .push(Some(Literal::Primitive(PrimitiveLiteral::Long(value))));
                    } else {
                        key_parts.push(format!("{field_name}=null",));
                        partition_values.push(None);
                    }
                } else if let Some(float32_array) = column.as_any().downcast_ref::<Float32Array>() {
                    if !float32_array.is_null(row_idx) {
                        let value = float32_array.value(row_idx);
                        key_parts.push(format!("{field_name}={value}"));
                        partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Float(
                            value.into(),
                        ))));
                    } else {
                        key_parts.push(format!("{field_name}=null",));
                        partition_values.push(None);
                    }
                } else if let Some(float64_array) = column.as_any().downcast_ref::<Float64Array>() {
                    if !float64_array.is_null(row_idx) {
                        let value = float64_array.value(row_idx);
                        key_parts.push(format!("{field_name}={value}"));
                        partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Double(
                            value.into(),
                        ))));
                    } else {
                        key_parts.push(format!("{field_name}=null"));
                        partition_values.push(None);
                    }
                } else if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                    if !string_array.is_null(row_idx) {
                        let value = string_array.value(row_idx);
                        key_parts.push(format!("{field_name}={value}"));
                        partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::String(
                            value.to_string(),
                        ))));
                    } else {
                        key_parts.push(format!("{field_name}=null",));
                        partition_values.push(None);
                    }
                } else {
                    return Err(IcebergFdwError::UnsupportedType(
                        "expected primitive array type for identity partition".to_string(),
                    ));
                }
            }
            _ => {
                return Err(IcebergFdwError::UnsupportedType(format!(
                    "unsupported partition transform: {transform:?}",
                )));
            }
        }
    }

    let partition_key = key_parts.join("/");
    let partition_struct = if partition_values.is_empty() {
        None
    } else {
        Some(Struct::from_iter(partition_values))
    };

    Ok((partition_key, partition_struct))
}
