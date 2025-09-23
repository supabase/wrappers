use arrow_array::RecordBatch;
use chrono::{Duration, NaiveDate};
use iceberg::{
    spec::{DataFileFormat, Literal, PrimitiveLiteral, Struct, TableMetadata, Transform},
    writer::file_writer::location_generator,
};

use super::utils::compute_partition_info;
use super::IcebergFdwResult;
use crate::fdw::iceberg_fdw::IcebergFdwError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(super) struct FileNameGenerator {
    format: String,
    file_count: Arc<AtomicU64>,
}

impl FileNameGenerator {
    pub fn new(format: DataFileFormat) -> Self {
        Self {
            format: format.to_string(),
            file_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl location_generator::FileNameGenerator for FileNameGenerator {
    fn generate_file_name(&self) -> String {
        let file_id = self.file_count.fetch_add(1, Ordering::Relaxed);
        let uuid = Uuid::new_v4();
        format!(
            "{:05}-{}-{}{}",
            file_id,
            0, // task_id (always 0 for single task)
            uuid.as_hyphenated(),
            if self.format.is_empty() {
                "".to_string()
            } else {
                format!(".{}", self.format)
            }
        )
    }
}

#[derive(Clone, Debug)]
pub(super) struct LocationGenerator {
    dir_path: String,
    partition_value: Option<Struct>,
}

impl LocationGenerator {
    pub fn new(metadata: &TableMetadata, record_batch: &RecordBatch) -> IcebergFdwResult<Self> {
        let table_location = metadata.location();
        let prop = metadata.properties();
        let data_location = prop
            .get("write.data.path")
            .or(prop.get("write.folder-storage.path"));
        let base_path = if let Some(data_location) = data_location {
            data_location.clone()
        } else {
            format!("{}/data", table_location)
        };

        // compute partition value first
        // use the first row (row 0) to compute partition value for the entire batch
        let (_, partition_value) = compute_partition_info(metadata, record_batch, 0)?;

        // format partition path for directory structure
        let partition_path = Self::format_partition_path(metadata, &partition_value)?;
        let dir_path = if partition_path.is_empty() {
            base_path
        } else {
            format!("{}/{}", base_path, partition_path)
        };

        Ok(Self {
            dir_path,
            partition_value,
        })
    }

    /// Get the partition value for use with DataFileWriterBuilder
    pub fn partition_value(&self) -> Option<Struct> {
        self.partition_value.clone()
    }

    fn format_partition_path(
        metadata: &TableMetadata,
        partition_value: &Option<Struct>,
    ) -> IcebergFdwResult<String> {
        let Some(partition_struct) = partition_value else {
            return Ok(String::new());
        };

        let partition_spec = metadata.default_partition_spec();
        let mut path_parts = Vec::new();

        for (idx, partition_field) in partition_spec.fields().iter().enumerate() {
            let field_name = &partition_field.name;
            let transform = &partition_field.transform;

            if let Some(literal) = &partition_struct[idx] {
                match literal {
                    Literal::Primitive(PrimitiveLiteral::Int(value)) => {
                        match transform {
                            Transform::Day => {
                                // convert days since epoch to ISO date format
                                let epoch_date =
                                    NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| {
                                        IcebergFdwError::UnsupportedType(
                                            "invalid epoch date".to_string(),
                                        )
                                    })?;
                                let date = epoch_date + Duration::days(*value as i64);
                                path_parts.push(format!(
                                    "{}={}",
                                    field_name,
                                    date.format("%Y-%m-%d")
                                ));
                            }
                            Transform::Year => {
                                // year value is years since epoch (1970)
                                let year = 1970 + *value;
                                path_parts.push(format!("{}={}", field_name, year));
                            }
                            Transform::Month => {
                                // month value is months since epoch (1970-01)
                                let months_since_epoch = *value as i64;
                                let years = months_since_epoch / 12;
                                let months = months_since_epoch % 12;
                                let year = 1970 + years;
                                let month = 1 + months; // Months are 1-indexed
                                path_parts.push(format!("{}={:04}-{:02}", field_name, year, month));
                            }
                            Transform::Hour => {
                                // hour value is hours since epoch (1970-01-01 00:00)
                                let hours_since_epoch = *value as i64;
                                let days = hours_since_epoch / 24;
                                let hours = hours_since_epoch % 24;
                                let epoch_date =
                                    NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| {
                                        IcebergFdwError::UnsupportedType(
                                            "invalid epoch date".to_string(),
                                        )
                                    })?;
                                let date = epoch_date + Duration::days(days);
                                path_parts.push(format!(
                                    "{}={}-{:02}",
                                    field_name,
                                    date.format("%Y-%m-%d"),
                                    hours
                                ));
                            }
                            Transform::Identity => {
                                // for identity transform, use the value directly
                                path_parts.push(format!("{}={}", field_name, value));
                            }
                            _ => {
                                return Err(IcebergFdwError::UnsupportedType(format!(
                                    "unsupported partition transform: {:?}",
                                    transform
                                )));
                            }
                        }
                    }
                    Literal::Primitive(PrimitiveLiteral::Boolean(value)) => {
                        if matches!(transform, Transform::Identity) {
                            path_parts.push(format!("{}={}", field_name, value));
                        } else {
                            return Err(IcebergFdwError::UnsupportedType(format!(
                                "boolean values only supported with Identity transform, got: {:?}",
                                transform
                            )));
                        }
                    }
                    Literal::Primitive(PrimitiveLiteral::Long(value)) => {
                        if matches!(transform, Transform::Identity) {
                            path_parts.push(format!("{}={}", field_name, value));
                        } else {
                            return Err(IcebergFdwError::UnsupportedType(format!(
                                "long values only supported with Identity transform, got: {:?}",
                                transform
                            )));
                        }
                    }
                    Literal::Primitive(PrimitiveLiteral::Float(value)) => {
                        if matches!(transform, Transform::Identity) {
                            path_parts.push(format!("{}={}", field_name, value));
                        } else {
                            return Err(IcebergFdwError::UnsupportedType(format!(
                                "float values only supported with Identity transform, got: {:?}",
                                transform
                            )));
                        }
                    }
                    Literal::Primitive(PrimitiveLiteral::Double(value)) => {
                        if matches!(transform, Transform::Identity) {
                            path_parts.push(format!("{}={}", field_name, value));
                        } else {
                            return Err(IcebergFdwError::UnsupportedType(format!(
                                "double values only supported with Identity transform, got: {:?}",
                                transform
                            )));
                        }
                    }
                    Literal::Primitive(PrimitiveLiteral::String(value)) => {
                        if matches!(transform, Transform::Identity) {
                            path_parts.push(format!("{}={}", field_name, value));
                        } else {
                            return Err(IcebergFdwError::UnsupportedType(format!(
                                "string values only supported with Identity transform, got: {:?}",
                                transform
                            )));
                        }
                    }
                    _ => {
                        return Err(IcebergFdwError::UnsupportedType(
                            "unsupported partition literal type".to_string(),
                        ));
                    }
                }
            }
        }

        Ok(path_parts.join("/"))
    }
}

impl location_generator::LocationGenerator for LocationGenerator {
    fn generate_location(&self, file_name: &str) -> String {
        format!("{}/{}", self.dir_path, file_name)
    }
}
