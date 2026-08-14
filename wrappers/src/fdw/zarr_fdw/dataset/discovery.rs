use std::collections::HashSet;

use serde_json::{Map, Value};

use super::super::meta::ArrayMeta;
use super::super::{ZarrFdwError, ZarrFdwResult};
use super::model::{CoordinateRef, Dataset, Dimension, DimensionRole};

const ARRAY_DIMENSIONS: &str = "_ARRAY_DIMENSIONS";

/// Parse xarray dimension names without choosing strict or tolerant behavior.
///
/// `None` means the attribute is absent. Callers scanning an array turn that
/// into an error, while metadata inspection preserves it as an unknown hint.
pub(crate) fn parse_named_dimensions(
    attrs: &Map<String, Value>,
    rank: usize,
) -> Result<Option<Vec<String>>, String> {
    let Some(value) = attrs.get(ARRAY_DIMENSIONS) else {
        return Ok(None);
    };
    let values = value
        .as_array()
        .ok_or_else(|| format!("{ARRAY_DIMENSIONS} must be an array of strings"))?;
    let dimensions = values
        .iter()
        .map(Value::as_str)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| format!("{ARRAY_DIMENSIONS} must contain only strings"))?;
    if dimensions.len() != rank {
        return Err(format!(
            "{ARRAY_DIMENSIONS} has {} names but the array rank is {rank}",
            dimensions.len()
        ));
    }
    for name in &dimensions {
        validate_dimension_name(name)?;
    }
    let unique = dimensions.iter().copied().collect::<HashSet<_>>();
    if unique.len() != dimensions.len() {
        return Err(format!("{ARRAY_DIMENSIONS} names must be unique"));
    }
    Ok(Some(dimensions.into_iter().map(str::to_string).collect()))
}

/// Require valid xarray dimension metadata for a scan array.
pub(crate) fn named_dimensions(
    attrs: &Map<String, Value>,
    rank: usize,
    array_path: &str,
) -> ZarrFdwResult<Vec<String>> {
    match parse_named_dimensions(attrs, rank) {
        Ok(Some(dimensions)) => Ok(dimensions),
        Ok(None) => Err(ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' must define {ARRAY_DIMENSIONS}"
        ))),
        Err(message) => Err(ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' has invalid {ARRAY_DIMENSIONS}: {message}"
        ))),
    }
}

/// Build the scan's format-neutral dataset descriptor from named dimensions
/// and the aligned attributes of their same-group coordinate arrays.
pub(crate) fn named_array_dataset(
    array_path: &str,
    meta: &ArrayMeta,
    names: &[String],
    coordinate_attrs: &[Map<String, Value>],
) -> ZarrFdwResult<Dataset> {
    if names.len() != meta.shape.len() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' has {} discovered dimensions but rank {}",
            names.len(),
            meta.shape.len()
        )));
    }
    if coordinate_attrs.len() != names.len() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' has attributes for {} coordinate arrays but {} dimensions",
            coordinate_attrs.len(),
            names.len()
        )));
    }

    let coordinate_parent = array_parent_path(array_path);
    let dimensions = names
        .iter()
        .zip(meta.shape.iter())
        .zip(coordinate_attrs)
        .map(|((name, &length), attrs)| {
            validate_coordinate_dimensions(name, attrs)?;
            Ok(Dimension::new(
                name.clone(),
                length,
                CoordinateRef::new(coordinate_parent.to_string(), name.clone()),
                infer_dimension_role(name, attrs)?,
            ))
        })
        .collect::<ZarrFdwResult<Vec<_>>>()?;

    let time_dimensions = dimensions
        .iter()
        .filter(|dimension| dimension.semantic_role() == DimensionRole::Time)
        .count();
    if time_dimensions > 1 {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' has multiple dimensions with the Time semantic role"
        )));
    }

    Ok(Dataset::new(
        dimensions,
        array_path.to_string(),
        meta.dtype.clone(),
    ))
}

fn validate_dimension_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{ARRAY_DIMENSIONS} names must not be empty"));
    }
    if name.trim() != name || name.chars().any(char::is_whitespace) {
        return Err(format!(
            "{ARRAY_DIMENSIONS} names must not contain whitespace"
        ));
    }
    if name.chars().any(char::is_control) {
        return Err(format!(
            "{ARRAY_DIMENSIONS} names must not contain control characters"
        ));
    }
    if name.contains('/') || name.contains('\\') || matches!(name, "." | "..") {
        return Err(format!(
            "{ARRAY_DIMENSIONS} names must be same-group array names"
        ));
    }
    Ok(())
}

fn validate_coordinate_dimensions(name: &str, attrs: &Map<String, Value>) -> ZarrFdwResult<()> {
    match parse_named_dimensions(attrs, 1) {
        Ok(None) => Ok(()),
        Ok(Some(dimensions)) if dimensions.first().map(String::as_str) == Some(name) => Ok(()),
        Ok(Some(dimensions)) => Err(ZarrFdwError::InvalidMetadata(format!(
            "coordinate array '{name}' declares {ARRAY_DIMENSIONS} {dimensions:?}, expected [\"{name}\"]"
        ))),
        Err(message) => Err(ZarrFdwError::InvalidMetadata(format!(
            "coordinate array '{name}' has invalid {ARRAY_DIMENSIONS}: {message}"
        ))),
    }
}

fn infer_dimension_role(name: &str, attrs: &Map<String, Value>) -> ZarrFdwResult<DimensionRole> {
    let mut resolved: Option<(DimensionRole, &str)> = None;
    for (source, role) in [
        (
            "standard_name",
            string_attribute_role(attrs, "standard_name", standard_name_role)?,
        ),
        ("axis", string_attribute_role(attrs, "axis", axis_role)?),
        ("units", string_attribute_role(attrs, "units", units_role)?),
    ] {
        let Some(role) = role else {
            continue;
        };
        resolved = Some(match resolved {
            None => (role, source),
            Some((current, current_source)) => {
                let merged = merge_compatible_roles(current, role).ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(format!(
                        "coordinate array '{name}' has conflicting semantic-role signals: {current:?} from {current_source} and {role:?} from {source}"
                    ))
                })?;
                (merged, current_source)
            }
        });
    }
    Ok(resolved
        .map(|(role, _)| role)
        .or_else(|| name_role(name))
        .unwrap_or(DimensionRole::Unknown))
}

fn string_attribute_role(
    attrs: &Map<String, Value>,
    attribute: &str,
    classify: fn(&str) -> Option<DimensionRole>,
) -> ZarrFdwResult<Option<DimensionRole>> {
    let Some(value) = attrs.get(attribute) else {
        return Ok(None);
    };
    let value = value.as_str().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!(
            "coordinate attribute '{attribute}' must be a string"
        ))
    })?;
    Ok(classify(value))
}

fn standard_name_role(value: &str) -> Option<DimensionRole> {
    match value.trim().to_ascii_lowercase().as_str() {
        "projection_x_coordinate" => Some(DimensionRole::SpatialX),
        "projection_y_coordinate" => Some(DimensionRole::SpatialY),
        "latitude" => Some(DimensionRole::Latitude),
        "longitude" => Some(DimensionRole::Longitude),
        "time" => Some(DimensionRole::Time),
        "depth" | "height" | "altitude" | "air_pressure" | "model_level_number" => {
            Some(DimensionRole::Vertical)
        }
        _ => None,
    }
}

fn axis_role(value: &str) -> Option<DimensionRole> {
    match value.trim().to_ascii_uppercase().as_str() {
        "X" => Some(DimensionRole::SpatialX),
        "Y" => Some(DimensionRole::SpatialY),
        "Z" => Some(DimensionRole::Vertical),
        "T" => Some(DimensionRole::Time),
        _ => None,
    }
}

fn units_role(value: &str) -> Option<DimensionRole> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "degree_east" | "degrees_east" | "degree_e" | "degrees_e" => Some(DimensionRole::Longitude),
        "degree_north" | "degrees_north" | "degree_n" | "degrees_n" => {
            Some(DimensionRole::Latitude)
        }
        _ => normalized
            .split_once(" since ")
            .map(|(unit, _)| unit)
            .filter(|unit| {
                matches!(
                    *unit,
                    "seconds"
                        | "milliseconds"
                        | "microseconds"
                        | "nanoseconds"
                        | "minutes"
                        | "hours"
                        | "days"
                )
            })
            .map(|_| DimensionRole::Time),
    }
}

fn name_role(value: &str) -> Option<DimensionRole> {
    match value.to_ascii_lowercase().as_str() {
        "x" => Some(DimensionRole::SpatialX),
        "y" => Some(DimensionRole::SpatialY),
        "lat" | "latitude" => Some(DimensionRole::Latitude),
        "lon" | "longitude" => Some(DimensionRole::Longitude),
        "time" => Some(DimensionRole::Time),
        "depth" | "height" | "altitude" | "level" | "lev" | "z" => Some(DimensionRole::Vertical),
        "band" => Some(DimensionRole::Band),
        "channel" => Some(DimensionRole::Channel),
        _ => None,
    }
}

fn merge_compatible_roles(left: DimensionRole, right: DimensionRole) -> Option<DimensionRole> {
    match (left, right) {
        (left, right) if left == right => Some(left),
        (DimensionRole::Latitude, DimensionRole::SpatialY)
        | (DimensionRole::SpatialY, DimensionRole::Latitude) => Some(DimensionRole::Latitude),
        (DimensionRole::Longitude, DimensionRole::SpatialX)
        | (DimensionRole::SpatialX, DimensionRole::Longitude) => Some(DimensionRole::Longitude),
        _ => None,
    }
}

fn array_parent_path(array_path: &str) -> &str {
    array_path
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn meta(shape: Vec<u64>) -> ArrayMeta {
        ArrayMeta {
            zarr_format: 2,
            chunks: vec![1; shape.len()],
            shape,
            dtype: "<f4".to_string(),
            fill_value: json!(-7.5),
            compressor: None,
            dimension_separator: ".".to_string(),
            order: 'C',
            filters: None,
        }
    }

    fn attrs(value: Value) -> Map<String, Value> {
        value.as_object().cloned().unwrap()
    }

    #[test]
    fn parses_missing_valid_and_invalid_named_dimensions() {
        assert_eq!(parse_named_dimensions(&Map::new(), 2), Ok(None));
        assert!(matches!(
            named_dimensions(&Map::new(), 2, "nested/value"),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message.contains("array 'nested/value' must define _ARRAY_DIMENSIONS")
        ));
        assert_eq!(
            parse_named_dimensions(&attrs(json!({"_ARRAY_DIMENSIONS": ["row", "column"]})), 2,),
            Ok(Some(vec!["row".to_string(), "column".to_string()]))
        );

        for (value, message) in [
            (json!("x"), "must be an array of strings"),
            (json!(["x", 1]), "must contain only strings"),
            (json!(["x"]), "has 1 names but the array rank is 2"),
            (json!(["x", "x"]), "names must be unique"),
            (json!(["x", ""]), "names must not be empty"),
            (json!(["x", "bad name"]), "must not contain whitespace"),
            (json!(["x", "bad\u{7}name"]), "must not contain control"),
            (json!(["x", "../y"]), "must be same-group array names"),
        ] {
            let error =
                parse_named_dimensions(&attrs(json!({"_ARRAY_DIMENSIONS": value})), 2).unwrap_err();
            assert!(error.contains(message), "unexpected error: {error}");
        }
    }

    #[test]
    fn constructs_arbitrary_rank_nested_dataset() {
        let names = ["forecast_time", "level", "band", "channel"]
            .map(str::to_string)
            .to_vec();
        let coordinate_attrs = vec![
            attrs(json!({"standard_name": "time", "axis": "T"})),
            attrs(json!({"axis": "Z"})),
            Map::new(),
            Map::new(),
        ];
        let dataset = named_array_dataset(
            "nested/generic4d",
            &meta(vec![2, 5, 6, 1]),
            &names,
            &coordinate_attrs,
        )
        .unwrap();

        assert_eq!(dataset.variable().dimensions(), names);
        assert_eq!(dataset.dimensions()[0].semantic_role(), DimensionRole::Time);
        assert_eq!(
            dataset.dimensions()[1].semantic_role(),
            DimensionRole::Vertical
        );
        assert_eq!(dataset.dimensions()[2].semantic_role(), DimensionRole::Band);
        assert_eq!(
            dataset.dimensions()[3].semantic_role(),
            DimensionRole::Channel
        );
        assert_eq!(dataset.dimensions()[3].coordinate().parent(), "nested");
    }

    #[test]
    fn infers_specific_and_generic_roles_without_renaming_dimensions() {
        let cases = [
            ("x", json!({"axis": "X"}), DimensionRole::SpatialX),
            ("y", json!({"axis": "Y"}), DimensionRole::SpatialY),
            (
                "lon",
                json!({"standard_name": "longitude", "axis": "X"}),
                DimensionRole::Longitude,
            ),
            (
                "lat",
                json!({"standard_name": "latitude", "axis": "Y"}),
                DimensionRole::Latitude,
            ),
            ("forecast_time", json!({"axis": "T"}), DimensionRole::Time),
            (
                "valid_time",
                json!({"units": "hours since 2000-01-01"}),
                DimensionRole::Time,
            ),
            (
                "east_coordinate",
                json!({"units": "degrees_east"}),
                DimensionRole::Longitude,
            ),
            ("depth", json!({}), DimensionRole::Vertical),
            ("level", json!({}), DimensionRole::Vertical),
            ("band", json!({}), DimensionRole::Band),
            ("channel", json!({}), DimensionRole::Channel),
            ("sample", json!({}), DimensionRole::Unknown),
            ("time", json!({"axis": "Z"}), DimensionRole::Vertical),
            (
                "band",
                json!({"standard_name": "time"}),
                DimensionRole::Time,
            ),
        ];
        for (name, metadata, expected) in cases {
            assert_eq!(
                infer_dimension_role(name, &attrs(metadata)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_conflicting_roles_and_multiple_time_dimensions() {
        assert!(matches!(
            infer_dimension_role(
                "latitude",
                &attrs(json!({"standard_name": "latitude", "axis": "X"}))
            ),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message.contains("conflicting semantic-role signals")
        ));
        assert!(matches!(
            infer_dimension_role("east", &attrs(json!({"axis": "X", "units": "degrees_north"}))),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message.contains("conflicting semantic-role signals")
        ));

        let names = vec!["time".to_string(), "forecast_time".to_string()];
        let coordinate_attrs = vec![Map::new(), attrs(json!({"axis": "T"}))];
        assert!(matches!(
            named_array_dataset(
                "multiple_times",
                &meta(vec![2, 2]),
                &names,
                &coordinate_attrs,
            ),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message.contains("multiple dimensions with the Time semantic role")
        ));
    }

    #[test]
    fn validates_optional_coordinate_dimension_hint() {
        let names = vec!["level".to_string()];
        let invalid = vec![attrs(json!({"_ARRAY_DIMENSIONS": ["other"]}))];
        assert!(named_array_dataset("value", &meta(vec![5]), &names, &invalid).is_err());
        let missing = vec![Map::new()];
        assert!(named_array_dataset("value", &meta(vec![5]), &names, &missing).is_ok());
    }
}
