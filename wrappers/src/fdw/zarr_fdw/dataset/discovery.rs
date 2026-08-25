use std::collections::HashSet;

use serde_json::{Map, Value};

use super::super::meta::{ArrayNode, ZarrFormat};
use super::super::ome::ResolvedOmeLevel;
use super::super::{ZarrFdwError, ZarrFdwResult};
use super::model::{CoordinateRef, CoordinateSource, Dataset, Dimension, DimensionRole};

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
pub(crate) fn named_dimensions(node: &ArrayNode, array_path: &str) -> ZarrFdwResult<Vec<String>> {
    let rank = node.meta.shape.len();
    let legacy = parse_named_dimensions(&node.attributes, rank).map_err(|message| {
        ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' has invalid {ARRAY_DIMENSIONS}: {message}"
        ))
    })?;
    match node.format {
        ZarrFormat::V2 => legacy.ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "array '{array_path}' must define {ARRAY_DIMENSIONS}"
            ))
        }),
        ZarrFormat::V3 => {
            let native = strict_native_dimension_names(node, array_path)?;
            if legacy.as_ref().is_some_and(|legacy| legacy != &native) {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "array '{array_path}' has conflicting dimension_names and {ARRAY_DIMENSIONS}"
                )));
            }
            Ok(native)
        }
    }
}

/// Build the scan's format-neutral dataset descriptor from named dimensions
/// and the aligned attributes of their same-group coordinate arrays.
pub(crate) fn named_array_dataset(
    array_path: &str,
    node: &ArrayNode,
    names: &[String],
    coordinate_nodes: &[ArrayNode],
) -> ZarrFdwResult<Dataset> {
    let meta = &node.meta;
    if names.len() != meta.shape.len() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' has {} discovered dimensions but rank {}",
            names.len(),
            meta.shape.len()
        )));
    }
    if coordinate_nodes.len() != names.len() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' has attributes for {} coordinate arrays but {} dimensions",
            coordinate_nodes.len(),
            names.len()
        )));
    }

    let coordinate_parent = array_parent_path(array_path);
    let dimensions = names
        .iter()
        .zip(meta.shape.iter())
        .zip(coordinate_nodes)
        .map(|((name, &length), coordinate)| {
            if coordinate.format != node.format {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "array '{array_path}' and coordinate array '{name}' use different Zarr formats"
                )));
            }
            validate_coordinate_dimensions(name, coordinate)?;
            Ok(Dimension::new(
                name.clone(),
                length,
                CoordinateSource::Stored(CoordinateRef::new(
                    coordinate_parent.to_string(),
                    name.clone(),
                )),
                infer_dimension_role(name, &coordinate.attributes)?,
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

/// Build the initial OME-Zarr 0.5 execution descriptor.
///
/// This deliberately supports only a two-dimensional `[y, x]` image. The
/// OME axes are authoritative but must agree exactly with the selected v3
/// array's native `dimension_names`. Coordinate values are synthesized from
/// the already-composed effective transform in `level`.
pub(crate) fn ome_rank2_dataset(
    array_path: &str,
    node: &ArrayNode,
    level: &ResolvedOmeLevel,
) -> ZarrFdwResult<Dataset> {
    if node.format != ZarrFormat::V3 {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "OME-Zarr 0.5 array '{array_path}' must use Zarr v3"
        )));
    }
    node.meta.validate()?;
    if array_path != level.array_path {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "OME-Zarr selected array path '{}' does not match loaded array '{array_path}'",
            level.array_path
        )));
    }
    if node.meta.shape.len() != 2 || level.axes.len() != 2 {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "OME-Zarr rank-2 execution requires axes [y, x], found array rank {} and {} axes",
            node.meta.shape.len(),
            level.axes.len()
        )));
    }

    let expected_names = ["y", "x"];
    for (axis, expected) in level.axes.iter().zip(expected_names) {
        if axis.name != expected || axis.kind.as_deref() != Some("space") {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "OME-Zarr rank-2 execution requires axes [y, x] with type 'space', found {:?}",
                level
                    .axes
                    .iter()
                    .map(|axis| (&axis.name, axis.kind.as_deref()))
                    .collect::<Vec<_>>()
            )));
        }
    }

    let names = named_dimensions(node, array_path)?;
    let ome_names = level
        .axes
        .iter()
        .map(|axis| axis.name.clone())
        .collect::<Vec<_>>();
    if names != ome_names {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "OME-Zarr axes {ome_names:?} do not match array dimension_names {names:?} for '{array_path}'"
        )));
    }
    if level.transform.scale.len() != 2 || level.transform.translation.len() != 2 {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "OME-Zarr rank-2 transform for '{array_path}' must contain two scale and translation values"
        )));
    }

    let dimensions = level
        .axes
        .iter()
        .zip(node.meta.shape.iter().copied())
        .zip(
            level
                .transform
                .scale
                .iter()
                .copied()
                .zip(level.transform.translation.iter().copied()),
        )
        .enumerate()
        .map(|(axis_index, ((axis, length), (scale, translation)))| {
            let semantic_role = if axis_index == 0 {
                DimensionRole::SpatialY
            } else {
                DimensionRole::SpatialX
            };
            Dimension::new(
                axis.name.clone(),
                length,
                CoordinateSource::Affine { scale, translation },
                semantic_role,
            )
        })
        .collect();

    Ok(Dataset::new(
        dimensions,
        array_path.to_string(),
        node.meta.dtype.clone(),
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

fn strict_native_dimension_names(node: &ArrayNode, array_path: &str) -> ZarrFdwResult<Vec<String>> {
    let native = node.dimension_names.as_ref().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!(
            "Zarr v3 array '{array_path}' must define dimension_names"
        ))
    })?;
    if native.len() != node.meta.shape.len() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "Zarr v3 array '{array_path}' has {} dimension_names but rank {}",
            native.len(),
            node.meta.shape.len()
        )));
    }
    let names = native
        .iter()
        .map(|name| {
            name.clone().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "Zarr v3 array '{array_path}' has an unnamed dimension"
                ))
            })
        })
        .collect::<ZarrFdwResult<Vec<_>>>()?;
    for name in &names {
        validate_dimension_name(name).map_err(|message| {
            ZarrFdwError::InvalidMetadata(format!(
                "Zarr v3 array '{array_path}' has invalid dimension_names: {message}"
            ))
        })?;
        validate_v3_node_name(name).map_err(|message| {
            ZarrFdwError::InvalidMetadata(format!(
                "Zarr v3 array '{array_path}' has invalid dimension_names: {message}"
            ))
        })?;
    }
    if names.iter().collect::<HashSet<_>>().len() != names.len() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "Zarr v3 array '{array_path}' dimension_names must be unique"
        )));
    }
    Ok(names)
}

fn validate_v3_node_name(name: &str) -> Result<(), String> {
    if name == "zarr.json"
        || name.starts_with("__")
        || name.chars().all(|character| character == '.')
    {
        return Err("Zarr v3 dimension names must be valid node names".to_string());
    }
    Ok(())
}

fn validate_coordinate_dimensions(name: &str, node: &ArrayNode) -> ZarrFdwResult<()> {
    if node.format == ZarrFormat::V3 && node.dimension_names.is_some() {
        let dimensions = strict_native_dimension_names(node, name)?;
        if dimensions.len() != 1 || dimensions[0] != name {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "coordinate array '{name}' declares dimension_names {dimensions:?}, expected [\"{name}\"]"
            )));
        }
    }
    match parse_named_dimensions(&node.attributes, 1) {
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

    use super::super::super::ome::{AffineTransform, OmeAxis};
    use super::*;

    fn node(shape: Vec<u64>, attributes: Map<String, Value>) -> ArrayNode {
        ArrayNode {
            format: ZarrFormat::V2,
            meta: super::super::super::meta::ArrayMeta {
                zarr_format: 2,
                chunks: vec![1; shape.len()],
                shape,
                dtype: "<f4".to_string(),
                fill_value: json!(-7.5),
                compressor: None,
                codec_pipeline: super::super::super::codec::CodecPipeline::raw_v2(),
                storage_layout: super::super::super::sharding::StorageLayout::Direct,
                chunk_key_encoding: super::super::super::meta::ChunkKeyEncoding::V2 {
                    separator: '.',
                },
                order: 'C',
                filters: None,
            },
            attributes,
            dimension_names: None,
            native_dtype: "<f4".to_string(),
            native_codecs: json!({"filters": null, "compressor": null}),
        }
    }

    fn coordinate_nodes(values: Vec<Map<String, Value>>) -> Vec<ArrayNode> {
        values
            .into_iter()
            .map(|attributes| node(vec![2], attributes))
            .collect()
    }

    fn v3_node(
        shape: Vec<u64>,
        dimension_names: Option<Vec<Option<&str>>>,
        attributes: Map<String, Value>,
    ) -> ArrayNode {
        let mut node = node(shape, attributes);
        node.format = ZarrFormat::V3;
        node.meta.zarr_format = 3;
        node.meta.chunk_key_encoding =
            super::super::super::meta::ChunkKeyEncoding::Default { separator: '/' };
        node.native_dtype = "float32".to_string();
        node.native_codecs = json!([{"name":"bytes","configuration":{"endian":"little"}}]);
        node.dimension_names = dimension_names.map(|names| {
            names
                .into_iter()
                .map(|name| name.map(str::to_string))
                .collect()
        });
        node
    }

    fn attrs(value: Value) -> Map<String, Value> {
        value.as_object().cloned().unwrap()
    }

    #[test]
    fn parses_missing_valid_and_invalid_named_dimensions() {
        assert_eq!(parse_named_dimensions(&Map::new(), 2), Ok(None));
        assert_eq!(
            parse_named_dimensions(
                &attrs(json!({"_ARRAY_DIMENSIONS":["__private", "zarr.json", "..."]})),
                3,
            ),
            Ok(Some(vec![
                "__private".to_string(),
                "zarr.json".to_string(),
                "...".to_string(),
            ]))
        );
        assert!(matches!(
            named_dimensions(&node(vec![2, 2], Map::new()), "nested/value"),
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
    fn v3_native_dimensions_are_strict_and_must_match_legacy_hints() {
        let valid = v3_node(
            vec![2, 2],
            Some(vec![Some("row"), Some("column")]),
            Map::new(),
        );
        assert_eq!(
            named_dimensions(&valid, "nested/value").unwrap(),
            vec!["row", "column"]
        );

        for invalid in [
            v3_node(vec![2], None, Map::new()),
            v3_node(vec![2], Some(vec![None]), Map::new()),
            v3_node(vec![2, 2], Some(vec![Some("x")]), Map::new()),
            v3_node(vec![2, 2], Some(vec![Some("x"), Some("x")]), Map::new()),
            v3_node(vec![2], Some(vec![Some("../x")]), Map::new()),
            v3_node(
                vec![2],
                Some(vec![Some("x")]),
                attrs(json!({"_ARRAY_DIMENSIONS":["other"]})),
            ),
            v3_node(vec![2], Some(vec![Some("zarr.json")]), Map::new()),
            v3_node(vec![2], Some(vec![Some("__private")]), Map::new()),
            v3_node(vec![2], Some(vec![Some("...")]), Map::new()),
        ] {
            assert!(named_dimensions(&invalid, "nested/value").is_err());
        }
    }

    #[test]
    fn v3_coordinates_must_match_format_and_name_themselves() {
        let names = vec!["x".to_string()];
        let value = v3_node(vec![2], Some(vec![Some("x")]), Map::new());
        let good = vec![v3_node(vec![2], Some(vec![Some("x")]), Map::new())];
        assert!(named_array_dataset("value", &value, &names, &good).is_ok());

        let unnamed = vec![v3_node(vec![2], None, Map::new())];
        assert!(named_array_dataset("value", &value, &names, &unnamed).is_ok());

        let wrong_name = vec![v3_node(vec![2], Some(vec![Some("other")]), Map::new())];
        assert!(named_array_dataset("value", &value, &names, &wrong_name).is_err());

        let mixed_format = vec![node(vec![2], Map::new())];
        assert!(named_array_dataset("value", &value, &names, &mixed_format).is_err());
    }

    fn ome_level(axes: Vec<OmeAxis>) -> ResolvedOmeLevel {
        ResolvedOmeLevel {
            group_path: "nested/image".to_string(),
            multiscale_index: 0,
            multiscale_name: Some("image".to_string()),
            level_index: 0,
            array_path: "nested/image/0".to_string(),
            axes,
            transform: AffineTransform {
                scale: vec![2.0, 3.0],
                translation: vec![10.0, 100.0],
            },
            warnings: Vec::new(),
        }
    }

    fn ome_axes() -> Vec<OmeAxis> {
        ["y", "x"]
            .into_iter()
            .map(|name| OmeAxis {
                name: name.to_string(),
                kind: Some("space".to_string()),
                unit: Some("micrometer".to_string()),
            })
            .collect()
    }

    #[test]
    fn constructs_rank2_ome_dataset_with_affine_coordinates() {
        let value = v3_node(vec![2, 3], Some(vec![Some("y"), Some("x")]), Map::new());
        let dataset = ome_rank2_dataset("nested/image/0", &value, &ome_level(ome_axes())).unwrap();

        assert_eq!(dataset.axis_names(), vec!["y", "x"]);
        assert_eq!(
            dataset.dimensions()[0].semantic_role(),
            DimensionRole::SpatialY
        );
        assert_eq!(
            dataset.dimensions()[1].semantic_role(),
            DimensionRole::SpatialX
        );
        assert_eq!(dataset.dimensions()[0].stored_coordinate(), None);
        assert!(matches!(
            dataset.dimensions()[0].coordinate_source(),
            CoordinateSource::Affine {
                scale: 2.0,
                translation: 10.0
            }
        ));
        assert!(matches!(
            dataset.dimensions()[1].coordinate_source(),
            CoordinateSource::Affine {
                scale: 3.0,
                translation: 100.0
            }
        ));
    }

    #[test]
    fn ome_dataset_rejects_format_rank_axes_path_and_dimension_mismatches() {
        let valid = v3_node(vec![2, 3], Some(vec![Some("y"), Some("x")]), Map::new());
        let mut cases = Vec::new();
        cases.push((
            node(vec![2, 3], Map::new()),
            ome_level(ome_axes()),
            "must use Zarr v3",
        ));
        cases.push((
            v3_node(
                vec![1, 2, 3],
                Some(vec![Some("z"), Some("y"), Some("x")]),
                Map::new(),
            ),
            ome_level(ome_axes()),
            "rank-2 execution requires axes [y, x]",
        ));
        let mut wrong_axes = ome_axes();
        wrong_axes.swap(0, 1);
        cases.push((valid.clone(), ome_level(wrong_axes), "requires axes [y, x]"));
        cases.push((
            v3_node(
                vec![2, 3],
                Some(vec![Some("row"), Some("column")]),
                Map::new(),
            ),
            ome_level(ome_axes()),
            "do not match array dimension_names",
        ));
        let mut wrong_path = ome_level(ome_axes());
        wrong_path.array_path = "nested/image/other".to_string();
        cases.push((valid, wrong_path, "does not match loaded array"));

        for (node, level, phrase) in cases {
            let error = ome_rank2_dataset("nested/image/0", &node, &level).unwrap_err();
            assert!(
                error.to_string().contains(phrase),
                "unexpected error: {error}"
            );
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
        let value = node(vec![2, 5, 6, 1], attrs(json!({"_ARRAY_DIMENSIONS": names})));
        let coordinate_nodes = coordinate_nodes(coordinate_attrs);
        let dataset =
            named_array_dataset("nested/generic4d", &value, &names, &coordinate_nodes).unwrap();

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
        assert_eq!(
            dataset.dimensions()[3]
                .stored_coordinate()
                .unwrap()
                .parent(),
            "nested"
        );
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
        let value = node(vec![2, 2], attrs(json!({"_ARRAY_DIMENSIONS": names})));
        let coordinate_nodes = coordinate_nodes(vec![Map::new(), attrs(json!({"axis": "T"}))]);
        assert!(matches!(
            named_array_dataset(
                "multiple_times",
                &value,
                &names,
                &coordinate_nodes,
            ),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message.contains("multiple dimensions with the Time semantic role")
        ));
    }

    #[test]
    fn validates_optional_coordinate_dimension_hint() {
        let names = vec!["level".to_string()];
        let value = node(vec![5], attrs(json!({"_ARRAY_DIMENSIONS": names})));
        let invalid = vec![node(
            vec![5],
            attrs(json!({"_ARRAY_DIMENSIONS": ["other"]})),
        )];
        assert!(named_array_dataset("value", &value, &names, &invalid).is_err());
        let missing = vec![node(vec![5], Map::new())];
        assert!(named_array_dataset("value", &value, &names, &missing).is_ok());
    }
}
