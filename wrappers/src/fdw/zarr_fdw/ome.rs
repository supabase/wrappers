//! Strict OME-Zarr 0.5 multiscale metadata adapter.
//!
//! OME-Zarr 0.5 is a Zarr v3 convention. This module parses only the bounded
//! inline affine subset required by the initial rank-2 executor: every level
//! has one scale followed by an optional translation, and an optional group
//! transform with the same grammar is applied afterwards.

use std::collections::HashSet;

use serde::Serialize;
use serde_json::{Map, Value};

use super::{ZarrFdwError, ZarrFdwResult};

const OME_VERSION: &str = "0.5";
const MAX_MULTISCALES: usize = 1_024;
const MAX_LEVELS: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct OmeAxis {
    pub(crate) name: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unit: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AffineTransform {
    pub(crate) scale: Vec<f64>,
    pub(crate) translation: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OmeLevel {
    pub(crate) relative_path: String,
    /// Level transform after composing the optional group transform.
    pub(crate) effective_transform: AffineTransform,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OmeMultiscale {
    pub(crate) name: Option<String>,
    pub(crate) axes: Vec<OmeAxis>,
    pub(crate) levels: Vec<OmeLevel>,
    /// Non-fatal violations of OME `SHOULD` recommendations.
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedOmeLevel {
    /// Canonical path relative to the configured store; empty means root.
    pub(crate) group_path: String,
    pub(crate) multiscale_index: usize,
    pub(crate) multiscale_name: Option<String>,
    pub(crate) level_index: usize,
    pub(crate) array_path: String,
    pub(crate) axes: Vec<OmeAxis>,
    pub(crate) transform: AffineTransform,
    pub(crate) warnings: Vec<String>,
}

/// Parse every OME-Zarr 0.5 multiscale declared by one group.
pub(crate) fn parse_ome_05_multiscales(
    group_path: &str,
    attributes: &Map<String, Value>,
) -> ZarrFdwResult<Vec<OmeMultiscale>> {
    let display_group = display_path(group_path);
    validate_optional_ome_05_attributes(group_path, Some(attributes))?;
    let ome = attributes
        .get("ome")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            invalid(format!(
                "OME-Zarr group '{display_group}' must define attributes.ome as an object"
            ))
        })?;
    let raw_multiscales = required_array(ome, "multiscales", "attributes.ome")?;
    if raw_multiscales.is_empty() {
        return Err(invalid(format!(
            "OME-Zarr group '{display_group}' must define a non-empty ome.multiscales array"
        )));
    }
    if raw_multiscales.len() > MAX_MULTISCALES {
        return Err(invalid(format!(
            "OME-Zarr group '{display_group}' declares {} multiscales, exceeding the limit of {MAX_MULTISCALES}",
            raw_multiscales.len()
        )));
    }

    let mut multiscales = Vec::new();
    multiscales
        .try_reserve_exact(raw_multiscales.len())
        .map_err(|_| invalid("could not allocate OME-Zarr multiscale metadata"))?;
    let mut total_levels = 0usize;
    for (multiscale_index, value) in raw_multiscales.iter().enumerate() {
        multiscales.push(parse_multiscale(
            value,
            display_group,
            multiscale_index,
            &mut total_levels,
        )?);
    }
    Ok(multiscales)
}

/// Parse and resolve one explicit zero-based multiscale/level selection.
pub(crate) fn resolve_ome_05_level(
    group_path: &str,
    attributes: &Map<String, Value>,
    multiscale_index: usize,
    level_index: usize,
) -> ZarrFdwResult<ResolvedOmeLevel> {
    let canonical_group = canonical_ome_group_path(group_path)?;
    let multiscales = parse_ome_05_multiscales(&canonical_group, attributes)?;
    let multiscale = multiscales.get(multiscale_index).ok_or_else(|| {
        invalid(format!(
            "multiscale index {multiscale_index} is outside the {} multiscales declared by OME-Zarr group '{}'",
            multiscales.len(),
            display_path(&canonical_group)
        ))
    })?;
    let level = multiscale.levels.get(level_index).ok_or_else(|| {
        invalid(format!(
            "multiscale level {level_index} is outside the {} levels declared by multiscale {multiscale_index} in OME-Zarr group '{}'",
            multiscale.levels.len(),
            display_path(&canonical_group)
        ))
    })?;
    let array_path = join_relative_path(&canonical_group, &level.relative_path);

    Ok(ResolvedOmeLevel {
        group_path: canonical_group,
        multiscale_index,
        multiscale_name: multiscale.name.clone(),
        level_index,
        array_path,
        axes: multiscale.axes.clone(),
        transform: level.effective_transform.clone(),
        warnings: multiscale.warnings.clone(),
    })
}

/// Canonicalize a user-selected OME group path relative to the store root.
///
/// Empty and `/` both denote the root. Outer slashes are removed for nested
/// groups; every remaining component must be a valid, non-reserved Zarr v3
/// node component.
pub(crate) fn canonical_ome_group_path(group_path: &str) -> ZarrFdwResult<String> {
    if group_path.is_empty() || group_path == "/" {
        return Ok(String::new());
    }
    let canonical = group_path.trim_matches('/');
    validate_relative_path(canonical).map_err(|message| {
        invalid(format!(
            "OME-Zarr group path '{group_path}' is invalid: {message}"
        ))
    })?;
    Ok(canonical.to_string())
}

/// Validate OME-Zarr version consistency on an optional hierarchy node.
///
/// Ordinary Zarr v3 ancestor groups do not need OME metadata. When an `ome`
/// block is present, however, it must be an object with version `0.5` even if
/// the group is a plate, well, labels container, or another non-multiscale
/// hierarchy node.
pub(crate) fn validate_optional_ome_05_attributes(
    node_path: &str,
    attributes: Option<&Map<String, Value>>,
) -> ZarrFdwResult<()> {
    let Some(ome_value) = attributes.and_then(|attributes| attributes.get("ome")) else {
        return Ok(());
    };
    let ome = ome_value.as_object().ok_or_else(|| {
        invalid(format!(
            "OME-Zarr metadata at '{}' must define attributes.ome as an object",
            display_path(node_path)
        ))
    })?;
    let version = ome.get("version").and_then(Value::as_str).ok_or_else(|| {
        invalid(format!(
            "OME-Zarr metadata at '{}' must define attributes.ome.version as a string",
            display_path(node_path)
        ))
    })?;
    if version != OME_VERSION {
        return Err(invalid(format!(
            "unsupported OME-Zarr version '{version}' in group '{}'; expected '{OME_VERSION}'",
            display_path(node_path)
        )));
    }
    Ok(())
}

fn parse_multiscale(
    value: &Value,
    display_group: &str,
    multiscale_index: usize,
    total_levels: &mut usize,
) -> ZarrFdwResult<OmeMultiscale> {
    let context = format!("OME-Zarr multiscale {multiscale_index} in group '{display_group}'");
    let object = value
        .as_object()
        .ok_or_else(|| invalid(format!("{context} must be an object")))?;
    validate_fields(
        object,
        &[
            "name",
            "axes",
            "datasets",
            "coordinateTransformations",
            "type",
            "metadata",
        ],
        &context,
    )?;

    let mut warnings = Vec::new();
    let name = match object.get("name") {
        None => {
            warnings.push(format!("{context} should define a name"));
            None
        }
        Some(value) => {
            let name = value
                .as_str()
                .ok_or_else(|| invalid(format!("{context} name must be a string")))?;
            if name.is_empty() {
                return Err(invalid(format!("{context} name must not be empty")));
            }
            Some(name.to_string())
        }
    };
    if let Some(value) = object.get("type") {
        value
            .as_str()
            .ok_or_else(|| invalid(format!("{context} type must be a string")))?;
    }
    if let Some(value) = object.get("metadata") {
        value
            .as_object()
            .ok_or_else(|| invalid(format!("{context} metadata must be an object")))?;
    }

    let axes = parse_axes(
        required_array(object, "axes", &context)?,
        &context,
        &mut warnings,
    )?;
    let rank = axes.len();
    let group_transform = object
        .get("coordinateTransformations")
        .map(|value| parse_transformations(value, rank, "group", &context))
        .transpose()?
        .unwrap_or_else(|| AffineTransform::identity(rank));

    let datasets = required_array(object, "datasets", &context)?;
    if datasets.is_empty() {
        return Err(invalid(format!("{context} datasets must not be empty")));
    }
    *total_levels = total_levels
        .checked_add(datasets.len())
        .ok_or_else(|| invalid("OME-Zarr level count overflowed"))?;
    if *total_levels > MAX_LEVELS {
        return Err(invalid(format!(
            "OME-Zarr metadata declares {total_levels} levels, exceeding the limit of {MAX_LEVELS}"
        )));
    }

    let mut levels = Vec::new();
    levels
        .try_reserve_exact(datasets.len())
        .map_err(|_| invalid("could not allocate OME-Zarr level metadata"))?;
    let mut dataset_paths = HashSet::new();
    for (level_index, dataset) in datasets.iter().enumerate() {
        let level_context = format!("{context} level {level_index}");
        let dataset = dataset
            .as_object()
            .ok_or_else(|| invalid(format!("{level_context} must be an object")))?;
        validate_fields(
            dataset,
            &["path", "coordinateTransformations"],
            &level_context,
        )?;
        let relative_path = required_string(dataset, "path", &level_context)?.to_string();
        validate_relative_path(&relative_path).map_err(|message| {
            invalid(format!(
                "OME-Zarr dataset path '{relative_path}' in {level_context} is not a valid relative Zarr node path: {message}"
            ))
        })?;
        if !dataset_paths.insert(relative_path.clone()) {
            return Err(invalid(format!(
                "{context} dataset paths must be unique; '{relative_path}' is duplicated"
            )));
        }
        let level_transform = parse_transformations(
            dataset.get("coordinateTransformations").ok_or_else(|| {
                invalid(format!(
                    "{level_context} must define coordinateTransformations"
                ))
            })?,
            rank,
            "dataset",
            &level_context,
        )?;
        levels.push(OmeLevel {
            relative_path,
            effective_transform: compose_affine(
                &level_transform,
                &group_transform,
                &level_context,
            )?,
        });
    }

    Ok(OmeMultiscale {
        name,
        axes,
        levels,
        warnings,
    })
}

fn parse_axes(
    values: &[Value],
    context: &str,
    warnings: &mut Vec<String>,
) -> ZarrFdwResult<Vec<OmeAxis>> {
    if !(2..=5).contains(&values.len()) {
        return Err(invalid(format!(
            "{context} axes length must be between 2 and 5, got {}",
            values.len()
        )));
    }
    let mut axes = Vec::new();
    axes.try_reserve_exact(values.len())
        .map_err(|_| invalid("could not allocate OME-Zarr axis metadata"))?;
    let mut names = HashSet::new();
    for (index, value) in values.iter().enumerate() {
        let axis_context = format!("{context} axis {index}");
        let object = value
            .as_object()
            .ok_or_else(|| invalid(format!("{axis_context} must be an object")))?;
        validate_fields(object, &["name", "type", "unit"], &axis_context)?;
        let name = required_string(object, "name", &axis_context)?;
        validate_axis_name(name)
            .map_err(|message| invalid(format!("{axis_context} name is invalid: {message}")))?;
        if !names.insert(name) {
            return Err(invalid(format!(
                "{context} axis names must be unique; '{name}' is duplicated"
            )));
        }
        let kind = match object.get("type") {
            None | Some(Value::Null) => {
                warnings.push(format!("{axis_context} should define a type"));
                None
            }
            Some(value) => Some(
                value
                    .as_str()
                    .ok_or_else(|| {
                        invalid(format!("{axis_context} type must be a string or null"))
                    })?
                    .to_string(),
            ),
        };
        let unit = match object.get("unit") {
            None => {
                if matches!(kind.as_deref(), Some("space" | "time")) {
                    warnings.push(format!("{axis_context} should define a unit"));
                }
                None
            }
            Some(value) => {
                let unit = value
                    .as_str()
                    .ok_or_else(|| invalid(format!("{axis_context} unit must be a string")))?;
                if unit.is_empty() {
                    return Err(invalid(format!("{axis_context} unit must not be empty")));
                }
                Some(unit.to_string())
            }
        };
        axes.push(OmeAxis {
            name: name.to_string(),
            kind,
            unit,
        });
    }
    validate_axis_types(&axes, context)?;
    Ok(axes)
}

fn validate_axis_types(axes: &[OmeAxis], context: &str) -> ZarrFdwResult<()> {
    let spatial = axes
        .iter()
        .filter(|axis| axis.kind.as_deref() == Some("space"))
        .count();
    if !(2..=3).contains(&spatial) {
        return Err(invalid(format!(
            "{context} axes must contain 2 or 3 entries with type 'space', found {spatial}"
        )));
    }
    let time = axes
        .iter()
        .filter(|axis| axis.kind.as_deref() == Some("time"))
        .count();
    if time > 1 {
        return Err(invalid(format!(
            "{context} axes may contain at most one time axis"
        )));
    }
    let auxiliary = axes.len() - spatial - time;
    if auxiliary > 1 {
        return Err(invalid(format!(
            "{context} axes may contain at most one channel, custom, or null-type axis"
        )));
    }

    let mut previous_order = 0u8;
    for axis in axes {
        let order = match axis.kind.as_deref() {
            Some("time") => 0,
            Some("space") => 2,
            _ => 1,
        };
        if order < previous_order {
            return Err(invalid(format!(
                "{context} axes must be ordered time, then channel/custom, then space"
            )));
        }
        previous_order = order;
    }
    Ok(())
}

fn parse_transformations(
    value: &Value,
    rank: usize,
    location: &str,
    context: &str,
) -> ZarrFdwResult<AffineTransform> {
    let transforms = value.as_array().ok_or_else(|| {
        invalid(format!(
            "{context} {location} coordinateTransformations must be an array"
        ))
    })?;
    if !(1..=2).contains(&transforms.len()) {
        return Err(invalid(format!(
            "{context} {location} coordinateTransformations must contain scale followed by optional translation"
        )));
    }
    let scale = parse_transform(&transforms[0], "scale", rank, location, context)?;
    let translation = if transforms.len() == 2 {
        parse_transform(&transforms[1], "translation", rank, location, context)?
    } else {
        vec![0.0; rank]
    };
    Ok(AffineTransform { scale, translation })
}

fn parse_transform(
    value: &Value,
    expected_type: &str,
    rank: usize,
    location: &str,
    context: &str,
) -> ZarrFdwResult<Vec<f64>> {
    let object = value.as_object().ok_or_else(|| {
        invalid(format!(
            "{context} {location} {expected_type} transform must be an object"
        ))
    })?;
    if object.contains_key("path") {
        return Err(invalid(format!(
            "path-backed OME-Zarr transforms are not supported in {context}"
        )));
    }
    let actual_type = required_string(object, "type", context)?;
    if actual_type != expected_type {
        return Err(invalid(format!(
            "{context} {location} coordinateTransformations must contain scale followed by optional translation; expected '{expected_type}', found '{actual_type}'"
        )));
    }
    validate_fields(object, &["type", expected_type], context)?;
    let values = required_array(object, expected_type, context)?;
    if values.len() != rank {
        return Err(invalid(format!(
            "{context} {location} {expected_type} has {} values but the axes rank is {rank}",
            values.len()
        )));
    }
    values
        .iter()
        .enumerate()
        .map(|(axis, value)| {
            let value = value.as_f64().filter(|value| value.is_finite()).ok_or_else(|| {
                invalid(format!(
                    "{context} {location} {expected_type} value at axis {axis} must be a finite number"
                ))
            })?;
            if expected_type == "scale" && value <= 0.0 {
                return Err(invalid(format!(
                    "OME-Zarr scale values must be finite and greater than zero; {context} axis {axis} is {value}"
                )));
            }
            Ok(value)
        })
        .collect()
}

fn compose_affine(
    level: &AffineTransform,
    group: &AffineTransform,
    context: &str,
) -> ZarrFdwResult<AffineTransform> {
    if level.scale.len() != group.scale.len()
        || level.translation.len() != group.translation.len()
        || level.scale.len() != level.translation.len()
    {
        return Err(invalid(format!(
            "{context} affine transform ranks do not match"
        )));
    }
    let mut scale = Vec::new();
    let mut translation = Vec::new();
    scale
        .try_reserve_exact(level.scale.len())
        .map_err(|_| invalid("could not allocate composed OME-Zarr scale"))?;
    translation
        .try_reserve_exact(level.scale.len())
        .map_err(|_| invalid("could not allocate composed OME-Zarr translation"))?;
    for axis in 0..level.scale.len() {
        let effective_scale = group.scale[axis] * level.scale[axis];
        let effective_translation =
            group.scale[axis] * level.translation[axis] + group.translation[axis];
        if !effective_scale.is_finite() || effective_scale <= 0.0 {
            return Err(invalid(format!(
                "{context} composed scale at axis {axis} is not finite and positive"
            )));
        }
        if !effective_translation.is_finite() {
            return Err(invalid(format!(
                "{context} composed translation at axis {axis} is not finite"
            )));
        }
        scale.push(effective_scale);
        translation.push(effective_translation);
    }
    Ok(AffineTransform { scale, translation })
}

impl AffineTransform {
    fn identity(rank: usize) -> Self {
        Self {
            scale: vec![1.0; rank],
            translation: vec![0.0; rank],
        }
    }
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    context: &str,
) -> ZarrFdwResult<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{context} must define '{field}' as a string")))
}

fn required_array<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    context: &str,
) -> ZarrFdwResult<&'a [Value]> {
    object
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| invalid(format!("{context} must define '{field}' as an array")))
}

fn validate_fields(
    object: &Map<String, Value>,
    allowed: &[&str],
    context: &str,
) -> ZarrFdwResult<()> {
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(invalid(format!(
            "{context} contains unsupported field '{field}'"
        )));
    }
    Ok(())
}

fn validate_axis_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("must not be empty");
    }
    if name.trim() != name || name.chars().any(char::is_whitespace) {
        return Err("must not contain whitespace");
    }
    if name.chars().any(char::is_control) {
        return Err("must not contain control characters");
    }
    if name.contains('/') || name.contains('\\') || matches!(name, "." | "..") {
        return Err("must not contain path components");
    }
    if name == "zarr.json" || name.starts_with("__") || name.chars().all(|value| value == '.') {
        return Err("must be a valid Zarr v3 dimension name");
    }
    Ok(())
}

fn validate_relative_path(path: &str) -> Result<(), &'static str> {
    if path.is_empty() {
        return Err("must not be empty");
    }
    if path.starts_with('/') || path.ends_with('/') || path.contains('\\') {
        return Err("must be relative and use '/' separators");
    }
    for component in path.split('/') {
        if component.is_empty() || matches!(component, "." | ".." | "zarr.json") {
            return Err("contains an invalid node component");
        }
        if component.starts_with("__") || component.chars().all(|value| value == '.') {
            return Err("contains a reserved Zarr v3 node component");
        }
        if component.chars().any(char::is_control) {
            return Err("contains a control character");
        }
    }
    Ok(())
}

fn join_relative_path(group_path: &str, relative_path: &str) -> String {
    if group_path.is_empty() {
        relative_path.to_string()
    } else {
        format!("{group_path}/{relative_path}")
    }
}

fn display_path(path: &str) -> &str {
    if path.is_empty() { "/" } else { path }
}

fn invalid(message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(message.into())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn attributes(multiscales: Value) -> Map<String, Value> {
        json!({"ome": {"version": "0.5", "multiscales": multiscales}})
            .as_object()
            .cloned()
            .unwrap()
    }

    fn valid_multiscale() -> Value {
        json!({
            "name": "image",
            "axes": [
                {"name": "y", "type": "space", "unit": "micrometer"},
                {"name": "x", "type": "space", "unit": "micrometer"}
            ],
            "datasets": [
                {"path": "0", "coordinateTransformations": [
                    {"type": "scale", "scale": [2.0, 3.0]},
                    {"type": "translation", "translation": [10.0, 100.0]}
                ]},
                {"path": "pyramid/1", "coordinateTransformations": [
                    {"type": "scale", "scale": [4.0, 6.0]}
                ]}
            ],
            "coordinateTransformations": [
                {"type": "scale", "scale": [0.5, 2.0]},
                {"type": "translation", "translation": [-1.0, 5.0]}
            ],
            "type": "gaussian",
            "metadata": {"method": "fixture"}
        })
    }

    #[test]
    fn parses_and_composes_level_then_group_transforms() {
        let parsed =
            parse_ome_05_multiscales("nested/image", &attributes(json!([valid_multiscale()])))
                .unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].name.as_deref(), Some("image"));
        assert!(parsed[0].warnings.is_empty());
        assert_eq!(
            parsed[0].levels[0].effective_transform,
            AffineTransform {
                scale: vec![1.0, 6.0],
                translation: vec![4.0, 205.0],
            }
        );

        let resolved = resolve_ome_05_level(
            "/nested/image/",
            &attributes(json!([valid_multiscale()])),
            0,
            1,
        )
        .unwrap();
        assert_eq!(resolved.group_path, "nested/image");
        assert_eq!(resolved.array_path, "nested/image/pyramid/1");
        assert_eq!(resolved.multiscale_name.as_deref(), Some("image"));
        assert_eq!(resolved.transform.scale, vec![2.0, 12.0]);
        assert_eq!(resolved.transform.translation, vec![-1.0, 5.0]);
    }

    #[test]
    fn absent_group_transform_is_identity_and_should_gaps_warn() {
        let parsed = parse_ome_05_multiscales(
            "/",
            &attributes(json!([{
                "axes": [
                    {"name": "c"},
                    {"name": "y", "type": "space"},
                    {"name": "x", "type": "space"}
                ],
                "datasets": [{
                    "path": "0",
                    "coordinateTransformations": [{"type": "scale", "scale": [1, 2, 3]}]
                }]
            }])),
        )
        .unwrap();
        assert_eq!(
            parsed[0].levels[0].effective_transform.scale,
            vec![1.0, 2.0, 3.0]
        );
        assert_eq!(
            parsed[0].levels[0].effective_transform.translation,
            vec![0.0; 3]
        );
        assert_eq!(parsed[0].warnings.len(), 4);
    }

    #[test]
    fn rejects_missing_or_wrong_ome_envelope() {
        for (attributes, phrase) in [
            (Map::new(), "must define attributes.ome"),
            (
                json!({"ome": []}).as_object().cloned().unwrap(),
                "must define attributes.ome",
            ),
            (
                json!({"ome": {"version": "0.4", "multiscales": []}})
                    .as_object()
                    .cloned()
                    .unwrap(),
                "unsupported OME-Zarr version '0.4'",
            ),
            (attributes(json!([])), "non-empty ome.multiscales"),
        ] {
            let error = parse_ome_05_multiscales("bad", &attributes).unwrap_err();
            assert!(
                error.to_string().contains(phrase),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn rejects_invalid_axes_and_ordering() {
        let invalid_axes = [
            (json!([{"name":"y","type":"space"}]), "between 2 and 5"),
            (
                json!([
                    {"name":"y","type":"space"},
                    {"name":"y","type":"space"}
                ]),
                "axis names must be unique",
            ),
            (
                json!([
                    {"name":"y","type":"space"},
                    {"name":"t","type":"time"},
                    {"name":"x","type":"space"}
                ]),
                "must be ordered",
            ),
            (
                json!([
                    {"name":"t","type":"time"},
                    {"name":"c","type":"channel"},
                    {"name":"other"},
                    {"name":"y","type":"space"},
                    {"name":"x","type":"space"}
                ]),
                "at most one channel, custom, or null-type axis",
            ),
            (
                json!([
                    {"name":"row","type":"custom"},
                    {"name":"column","type":"custom"}
                ]),
                "2 or 3 entries with type 'space'",
            ),
        ];
        for (axes, phrase) in invalid_axes {
            let error = parse_ome_05_multiscales(
                "bad",
                &attributes(json!([{
                    "name": "bad",
                    "axes": axes,
                    "datasets": [{
                        "path":"0",
                        "coordinateTransformations":[{"type":"scale","scale":[1,1]}]
                    }]
                }])),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains(phrase),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn rejects_malformed_or_unsupported_transforms() {
        let cases = [
            (json!([]), "scale followed by optional translation"),
            (
                json!([{"type":"translation","translation":[0,0]}]),
                "expected 'scale'",
            ),
            (
                json!([
                    {"type":"scale","scale":[1,1]},
                    {"type":"scale","scale":[1,1]}
                ]),
                "expected 'translation'",
            ),
            (
                json!([{"type":"scale","scale":[1]}]),
                "has 1 values but the axes rank is 2",
            ),
            (json!([{"type":"scale","scale":[1,0]}]), "greater than zero"),
            (
                json!([{"type":"scale","path":"transform/scale"}]),
                "path-backed OME-Zarr transforms",
            ),
        ];
        for (transforms, phrase) in cases {
            let error = parse_ome_05_multiscales(
                "bad",
                &attributes(json!([{
                    "name":"bad",
                    "axes":[
                        {"name":"y","type":"space"},
                        {"name":"x","type":"space"}
                    ],
                    "datasets":[{"path":"0","coordinateTransformations":transforms}]
                }])),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains(phrase),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn rejects_unsafe_paths_and_out_of_range_selection() {
        for path in ["", "/0", "../0", "a//b", "a\\b", "__private"] {
            let mut multiscale = valid_multiscale();
            multiscale["datasets"][0]["path"] = json!(path);
            let error =
                parse_ome_05_multiscales("bad", &attributes(json!([multiscale]))).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("not a valid relative Zarr node path"),
                "unexpected error for {path:?}: {error}"
            );
        }

        let attrs = attributes(json!([valid_multiscale()]));
        assert!(
            resolve_ome_05_level("root", &attrs, 1, 0)
                .unwrap_err()
                .to_string()
                .contains("multiscale index 1 is outside")
        );
        assert!(
            resolve_ome_05_level("root", &attrs, 0, 2)
                .unwrap_err()
                .to_string()
                .contains("multiscale level 2 is outside")
        );
    }

    #[test]
    fn rejects_duplicate_dataset_paths() {
        let mut multiscale = valid_multiscale();
        multiscale["datasets"][1]["path"] = json!("0");
        let error = parse_ome_05_multiscales("bad", &attributes(json!([multiscale]))).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("dataset paths must be unique; '0' is duplicated"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn canonicalizes_safe_group_paths_and_rejects_reserved_components() {
        assert_eq!(canonical_ome_group_path("").unwrap(), "");
        assert_eq!(canonical_ome_group_path("/").unwrap(), "");
        assert_eq!(
            canonical_ome_group_path("/plates/A/1/").unwrap(),
            "plates/A/1"
        );

        for path in [
            "//",
            "a//b",
            "a/./b",
            "a/../b",
            "a/zarr.json",
            "a/__private",
            "a/...",
            "a\\b",
            "a/line\nbreak",
        ] {
            let error = canonical_ome_group_path(path).unwrap_err();
            assert!(
                error.to_string().contains("OME-Zarr group path"),
                "unexpected error for {path:?}: {error}"
            );
        }
    }

    #[test]
    fn validates_optional_hierarchy_ome_versions_without_requiring_multiscales() {
        assert!(validate_optional_ome_05_attributes("/", None).is_ok());
        assert!(validate_optional_ome_05_attributes("plain", Some(&Map::new())).is_ok());

        let hierarchy_only = json!({"ome": {"version": "0.5", "plate": {}}})
            .as_object()
            .cloned()
            .unwrap();
        assert!(validate_optional_ome_05_attributes("plate", Some(&hierarchy_only)).is_ok());

        for (attributes, phrase) in [
            (
                json!({"ome": []}).as_object().cloned().unwrap(),
                "attributes.ome as an object",
            ),
            (
                json!({"ome": {}}).as_object().cloned().unwrap(),
                "attributes.ome.version as a string",
            ),
            (
                json!({"ome": {"version": 5}}).as_object().cloned().unwrap(),
                "attributes.ome.version as a string",
            ),
            (
                json!({"ome": {"version": "0.4"}})
                    .as_object()
                    .cloned()
                    .unwrap(),
                "unsupported OME-Zarr version '0.4'",
            ),
        ] {
            let error = validate_optional_ome_05_attributes("bad", Some(&attributes)).unwrap_err();
            assert!(
                error.to_string().contains(phrase),
                "unexpected error: {error}"
            );
        }
    }
}
