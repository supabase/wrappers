use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use iceberg::spec::{Literal, PrimitiveLiteral, Struct, TableMetadata, Transform};
use std::collections::HashMap;

use super::IcebergFdwResult;
use crate::fdw::iceberg_fdw::IcebergFdwError;

// copy an option to another in an option HashMap, if the target option
// doesn't exist
pub(super) fn copy_option(map: &mut HashMap<String, String>, from_key: &str, to_key: &str) {
    if !map.contains_key(to_key) {
        let value = map.get(from_key).cloned().unwrap_or_default();
        map.insert(to_key.to_string(), value);
    }
}

/// Split a record batch into multiple batches by partition values
/// Assumes the input record batch is already sorted by partition key
pub(super) fn split_record_batch_by_partition(
    metadata: &TableMetadata,
    record_batch: RecordBatch,
) -> IcebergFdwResult<Vec<RecordBatch>> {
    let partition_spec = metadata.default_partition_spec();

    // if no partition spec, return the original batch
    if partition_spec.fields().is_empty() {
        return Ok(vec![record_batch]);
    }

    // since data is pre-sorted by partition key, we can efficiently detect boundaries
    let mut result_batches = Vec::new();
    let mut partition_start = 0;
    let mut current_partition_key: Option<String> = None;

    for row_idx in 0..record_batch.num_rows() {
        let (partition_key, _) = compute_partition_info(metadata, &record_batch, row_idx)?;

        // check if we've hit a partition boundary
        if current_partition_key.as_ref() != Some(&partition_key) {
            // create batch for previous partition (if any)
            if row_idx > 0 {
                let batch_length = row_idx - partition_start;
                let partition_batch = record_batch.slice(partition_start, batch_length);
                result_batches.push(partition_batch);
            }

            // start new partition
            partition_start = row_idx;
            current_partition_key = Some(partition_key);
        }
    }

    // handle the last partition
    if partition_start < record_batch.num_rows() {
        let batch_length = record_batch.num_rows() - partition_start;
        let partition_batch = record_batch.slice(partition_start, batch_length);
        result_batches.push(partition_batch);
    }

    Ok(result_batches)
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
                "cannot find source column with ID {} for partition field",
                source_field_id
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
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampMillisecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_ms = timestamp_array.value(row_idx);
                        days_since_epoch = Some(timestamp_ms / (24 * 60 * 60 * 1_000));
                    }
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampNanosecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_ns = timestamp_array.value(row_idx);
                        days_since_epoch = Some(timestamp_ns / (24 * 60 * 60 * 1_000_000_000));
                    }
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampSecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_s = timestamp_array.value(row_idx);
                        days_since_epoch = Some(timestamp_s / (24 * 60 * 60));
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
                    key_parts.push(format!("{}={}", field_name, days));
                    partition_values
                        .push(Some(Literal::Primitive(PrimitiveLiteral::Int(days as i32))));
                } else {
                    key_parts.push(format!("{}=null", field_name));
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
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampMillisecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_ms = timestamp_array.value(row_idx);
                        let seconds_since_epoch = timestamp_ms / 1_000;
                        years_since_epoch = Some(seconds_to_years(seconds_since_epoch));
                    }
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampNanosecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_ns = timestamp_array.value(row_idx);
                        let seconds_since_epoch = timestamp_ns / 1_000_000_000;
                        years_since_epoch = Some(seconds_to_years(seconds_since_epoch));
                    }
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampSecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let seconds_since_epoch = timestamp_array.value(row_idx);
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
                    key_parts.push(format!("{}={}", field_name, years));
                    partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        years as i32,
                    ))));
                } else {
                    key_parts.push(format!("{}=null", field_name));
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
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampMillisecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_ms = timestamp_array.value(row_idx);
                        let seconds_since_epoch = timestamp_ms / 1_000;
                        months_since_epoch = Some(seconds_to_months(seconds_since_epoch));
                    }
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampNanosecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_ns = timestamp_array.value(row_idx);
                        let seconds_since_epoch = timestamp_ns / 1_000_000_000;
                        months_since_epoch = Some(seconds_to_months(seconds_since_epoch));
                    }
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampSecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let seconds_since_epoch = timestamp_array.value(row_idx);
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
                    key_parts.push(format!("{}={}", field_name, months));
                    partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        months as i32,
                    ))));
                } else {
                    key_parts.push(format!("{}=null", field_name));
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
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampMillisecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_ms = timestamp_array.value(row_idx);
                        hours_since_epoch = Some(timestamp_ms / (60 * 60 * 1_000));
                    }
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampNanosecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_ns = timestamp_array.value(row_idx);
                        hours_since_epoch = Some(timestamp_ns / (60 * 60 * 1_000_000_000));
                    }
                } else if let Some(timestamp_array) =
                    column.as_any().downcast_ref::<TimestampSecondArray>()
                {
                    if !timestamp_array.is_null(row_idx) {
                        let timestamp_s = timestamp_array.value(row_idx);
                        hours_since_epoch = Some(timestamp_s / (60 * 60));
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
                    key_parts.push(format!("{}={}", field_name, hours));
                    partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        hours as i32,
                    ))));
                } else {
                    key_parts.push(format!("{}=null", field_name));
                    partition_values.push(None);
                }
            }
            Transform::Identity => {
                // for identity transform, use the raw value from the column
                if let Some(boolean_array) = column.as_any().downcast_ref::<BooleanArray>() {
                    if !boolean_array.is_null(row_idx) {
                        let value = boolean_array.value(row_idx);
                        key_parts.push(format!("{}={}", field_name, value));
                        partition_values
                            .push(Some(Literal::Primitive(PrimitiveLiteral::Boolean(value))));
                    } else {
                        key_parts.push(format!("{}=null", field_name));
                        partition_values.push(None);
                    }
                } else if let Some(int32_array) = column.as_any().downcast_ref::<Int32Array>() {
                    if !int32_array.is_null(row_idx) {
                        let value = int32_array.value(row_idx);
                        key_parts.push(format!("{}={}", field_name, value));
                        partition_values
                            .push(Some(Literal::Primitive(PrimitiveLiteral::Int(value))));
                    } else {
                        key_parts.push(format!("{}=null", field_name));
                        partition_values.push(None);
                    }
                } else if let Some(int64_array) = column.as_any().downcast_ref::<Int64Array>() {
                    if !int64_array.is_null(row_idx) {
                        let value = int64_array.value(row_idx);
                        key_parts.push(format!("{}={}", field_name, value));
                        partition_values
                            .push(Some(Literal::Primitive(PrimitiveLiteral::Long(value))));
                    } else {
                        key_parts.push(format!("{}=null", field_name));
                        partition_values.push(None);
                    }
                } else if let Some(float32_array) = column.as_any().downcast_ref::<Float32Array>() {
                    if !float32_array.is_null(row_idx) {
                        let value = float32_array.value(row_idx);
                        key_parts.push(format!("{}={}", field_name, value));
                        partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Float(
                            value.into(),
                        ))));
                    } else {
                        key_parts.push(format!("{}=null", field_name));
                        partition_values.push(None);
                    }
                } else if let Some(float64_array) = column.as_any().downcast_ref::<Float64Array>() {
                    if !float64_array.is_null(row_idx) {
                        let value = float64_array.value(row_idx);
                        key_parts.push(format!("{}={}", field_name, value));
                        partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::Double(
                            value.into(),
                        ))));
                    } else {
                        key_parts.push(format!("{}=null", field_name));
                        partition_values.push(None);
                    }
                } else if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                    if !string_array.is_null(row_idx) {
                        let value = string_array.value(row_idx);
                        key_parts.push(format!("{}={}", field_name, value));
                        partition_values.push(Some(Literal::Primitive(PrimitiveLiteral::String(
                            value.to_string(),
                        ))));
                    } else {
                        key_parts.push(format!("{}=null", field_name));
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
                    "unsupported partition transform: {:?}",
                    transform
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
