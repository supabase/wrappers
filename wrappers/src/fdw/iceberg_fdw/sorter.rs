use super::{Cell, IcebergFdwResult, InputRow};
use iceberg::{
    spec::{SortDirection, SortOrder},
    table::Table,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(super) enum SortKeyValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl SortKeyValue {
    pub(super) fn compare_with_direction(
        &self,
        other: &Self,
        ascending: bool,
    ) -> std::cmp::Ordering {
        let comparison = match (self, other) {
            (SortKeyValue::Null, SortKeyValue::Null) => std::cmp::Ordering::Equal,
            (SortKeyValue::Null, _) => std::cmp::Ordering::Less,
            (_, SortKeyValue::Null) => std::cmp::Ordering::Greater,
            (SortKeyValue::Bool(a), SortKeyValue::Bool(b)) => a.cmp(b),
            (SortKeyValue::Int32(a), SortKeyValue::Int32(b)) => a.cmp(b),
            (SortKeyValue::Int64(a), SortKeyValue::Int64(b)) => a.cmp(b),
            (SortKeyValue::Float32(a), SortKeyValue::Float32(b)) => {
                if a.is_nan() && b.is_nan() {
                    std::cmp::Ordering::Equal
                } else if a.is_nan() {
                    std::cmp::Ordering::Greater
                } else if b.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                }
            }
            (SortKeyValue::Float64(a), SortKeyValue::Float64(b)) => {
                if a.is_nan() && b.is_nan() {
                    std::cmp::Ordering::Equal
                } else if a.is_nan() {
                    std::cmp::Ordering::Greater
                } else if b.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                }
            }
            (SortKeyValue::String(a), SortKeyValue::String(b)) => a.cmp(b),
            (SortKeyValue::Bytes(a), SortKeyValue::Bytes(b)) => a.cmp(b),
            _ => unreachable!("Cannot compare different SortKeyValue types"),
        };

        if ascending {
            comparison
        } else {
            comparison.reverse()
        }
    }
}

pub(super) struct Sorter;

impl Sorter {
    /// Sort rows in place according to table's sort specification
    pub(super) fn sort_partition_rows(
        &self,
        table: &Table,
        rows: &mut [InputRow],
    ) -> IcebergFdwResult<()> {
        let metadata = table.metadata();
        let sort_order = metadata.default_sort_order();

        if sort_order.is_unsorted() {
            return Ok(()); // Nothing to sort
        }

        // Get the schema to map sort fields to column indices
        let schema = metadata.current_schema();

        // Create a map from column ID to schema index for efficient lookup.
        let field_map: HashMap<i32, usize> = schema
            .as_struct()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| (field.id, idx))
            .collect();

        // Sort rows in-place without cloning the rows.
        // Precompute sort keys for all rows, propagating errors
        let mut keyed_rows: Vec<(Vec<SortKeyValue>, usize)> = Vec::with_capacity(rows.len());
        for (idx, row) in rows.iter().enumerate() {
            let keys = self.extract_sort_keys_for_row(row, sort_order, &field_map)?;
            keyed_rows.push((keys, idx));
        }

        // Sort the indices based on the sort keys
        keyed_rows.sort_unstable_by(|(a_keys, _), (b_keys, _)| {
            self.compare_sort_keys(a_keys, b_keys, sort_order)
        });

        // Reorder the rows in place according to the sorted order
        let mut new_rows = Vec::with_capacity(rows.len());
        for &(_, idx) in &keyed_rows {
            new_rows.push(rows[idx].clone());
        }
        rows.copy_from_slice(&new_rows);
        Ok(())
    }

    /// Helper function to extract sort keys for a row
    fn extract_sort_keys_for_row(
        &self,
        row: &InputRow,
        sort_order: &SortOrder,
        field_map: &HashMap<i32, usize>,
    ) -> IcebergFdwResult<Vec<SortKeyValue>> {
        let mut sort_keys = Vec::with_capacity(sort_order.fields.len());

        for sort_field in sort_order.fields.iter() {
            // Get the source field ID from the sort specification
            let source_id = sort_field.source_id;

            // Find the index of this field in the row using the pre-computed map
            let field_index = field_map.get(&source_id).copied();

            let sort_key = match field_index {
                Some(idx) => {
                    // Get the cell from the row and convert to sort key value
                    if let Some(cell) = row.cells.get(idx).and_then(|c| c.as_ref()) {
                        self.cell_to_sort_value(cell)
                    } else {
                        SortKeyValue::Null
                    }
                }
                None => SortKeyValue::Null, // Field not found in row
            };

            sort_keys.push(sort_key);
        }

        Ok(sort_keys)
    }

    /// Convert cell to sort value based on its type
    fn cell_to_sort_value(&self, cell: &Cell) -> SortKeyValue {
        match cell {
            Cell::Bool(value) => SortKeyValue::Bool(*value),
            Cell::I8(value) => SortKeyValue::Int32(*value as i32),
            Cell::I16(value) => SortKeyValue::Int32(*value as i32),
            Cell::I32(value) => SortKeyValue::Int32(*value),
            Cell::I64(value) => SortKeyValue::Int64(*value),
            Cell::F32(value) => SortKeyValue::Float32(*value),
            Cell::F64(value) => SortKeyValue::Float64(*value),
            Cell::String(value) => SortKeyValue::String(value.clone()),
            Cell::Json(json) => SortKeyValue::String(json.0.to_string()),
            Cell::Date(date) => SortKeyValue::Int64(i64::from(date.to_unix_epoch_days())),
            Cell::Time(time) => SortKeyValue::Int64(i64::from(*time)),
            Cell::Timestamp(ts) => SortKeyValue::Int64(i64::from(*ts)),
            Cell::Timestamptz(tstz) => SortKeyValue::Int64(i64::from(*tstz)),
            Cell::Numeric(numeric) => SortKeyValue::String(numeric.to_string()),
            Cell::Uuid(uuid) => SortKeyValue::String(uuid.to_string()),
            Cell::Bytea(bytea) => {
                // Convert byte array to a vector for direct byte-wise comparison
                let bytes = unsafe { pgrx::varlena::varlena_to_byte_slice(*bytea) };
                SortKeyValue::Bytes(bytes.to_vec())
            }
            Cell::Interval(iv) => SortKeyValue::String(iv.to_string()),
            // Handle any other cell types by converting to string representation
            _ => SortKeyValue::String(cell.to_string()),
        }
    }

    /// Compare sort keys based on the table's sort order specification
    fn compare_sort_keys(
        &self,
        a_keys: &[SortKeyValue],
        b_keys: &[SortKeyValue],
        sort_order: &SortOrder,
    ) -> std::cmp::Ordering {
        // Compare each sort field in order
        for (i, (a_key, b_key)) in a_keys.iter().zip(b_keys.iter()).enumerate() {
            if i >= sort_order.fields.len() {
                break; // We've processed all sort fields
            }

            let sort_field = &sort_order.fields[i];
            // Determine sort direction based on Iceberg crate API
            let is_ascending = matches!(sort_field.direction, SortDirection::Ascending);

            let comparison = a_key.compare_with_direction(b_key, is_ascending);

            if comparison != std::cmp::Ordering::Equal {
                return comparison;
            }
        }

        // All sort keys were equal
        std::cmp::Ordering::Equal
    }
}
