//! Strict CRS metadata resolution for spatial execution.
//!
//! `zarr_inspect` intentionally exposes best-effort raw CRS metadata. Spatial
//! execution instead requires one unambiguous EPSG identifier and a valid
//! same-group `grid_mapping` reference when one is declared.

use serde_json::{Map, Value};

use super::super::{ZarrFdwError, ZarrFdwResult};

const GRID_MAPPING: &str = "grid_mapping";
const GEO_TRANSFORM: &str = "GeoTransform";

/// Metadata loaded from the sibling array named by `grid_mapping`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GridMappingMetadata<'a> {
    pub(crate) path: &'a str,
    pub(crate) attributes: &'a Map<String, Value>,
}

/// CRS information accepted by the initial rectilinear spatial engine.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedCrs {
    pub(crate) epsg: i32,
    pub(crate) wkt: Option<String>,
    pub(crate) geotransform: Option<[f64; 6]>,
}

#[derive(Debug)]
struct EpsgCandidate {
    code: i32,
    source: String,
}

#[derive(Default)]
struct CollectedCrs {
    epsg: Vec<EpsgCandidate>,
    wkt: Vec<String>,
    geotransforms: Vec<([f64; 6], String)>,
}

/// Return the same-group sibling path named by an array's `grid_mapping`.
///
/// This separate step lets an executor validate the reference before reading
/// the one sibling metadata object needed to resolve it.
pub(crate) fn grid_mapping_sibling_path(
    array_path: &str,
    array_attributes: &Map<String, Value>,
) -> ZarrFdwResult<Option<String>> {
    let Some(value) = array_attributes.get(GRID_MAPPING) else {
        return Ok(None);
    };
    let reference = value.as_str().ok_or_else(|| {
        invalid_crs(
            array_path,
            "grid_mapping reference must be a string naming a same-group array",
        )
    })?;
    validate_same_group_name(array_path, reference)?;
    Ok(Some(same_group_sibling_path(array_path, reference)))
}

/// Resolve CRS data from CF grid-mapping, direct array, and group metadata.
///
/// A declared grid mapping must be supplied and is the authoritative source.
/// Direct array and group EPSG candidates are still checked for conflicts.
pub(crate) fn resolve_crs(
    array_path: &str,
    array_attributes: &Map<String, Value>,
    group_attributes: Option<&Map<String, Value>>,
    grid_mapping: Option<GridMappingMetadata<'_>>,
) -> ZarrFdwResult<ResolvedCrs> {
    let expected_mapping_path = grid_mapping_sibling_path(array_path, array_attributes)?;
    match (expected_mapping_path.as_deref(), grid_mapping.as_ref()) {
        (Some(expected), Some(actual)) if actual.path != expected => {
            return Err(invalid_crs(
                array_path,
                format!(
                    "grid_mapping resolves to sibling array '{expected}', but metadata for '{}' was supplied",
                    actual.path
                ),
            ));
        }
        (Some(expected), None) => {
            return Err(invalid_crs(
                array_path,
                format!("grid_mapping sibling array '{expected}' was not found"),
            ));
        }
        (None, Some(actual)) => {
            return Err(invalid_crs(
                array_path,
                format!(
                    "CRS metadata for sibling array '{}' was supplied without a grid_mapping reference",
                    actual.path
                ),
            ));
        }
        _ => {}
    }

    let mut collected = CollectedCrs::default();
    if let Some(mapping) = grid_mapping {
        collect_attributes(
            array_path,
            &format!("grid_mapping array '{}'", mapping.path),
            mapping.attributes,
            &mut collected,
        )?;
    }
    collect_attributes(
        array_path,
        "selected array",
        array_attributes,
        &mut collected,
    )?;
    if let Some(attributes) = group_attributes {
        collect_attributes(
            array_path,
            "same-group metadata",
            attributes,
            &mut collected,
        )?;
    }

    let Some(first) = collected.epsg.first() else {
        let message = if collected.wkt.is_empty() {
            "no supported EPSG identifier was found in grid-mapping, array, or group metadata"
        } else {
            "CRS metadata contains WKT but no supported EPSG identifier; WKT-only CRS resolution is not supported yet"
        };
        return Err(invalid_crs(array_path, message));
    };
    if let Some(conflict) = collected
        .epsg
        .iter()
        .skip(1)
        .find(|candidate| candidate.code != first.code)
    {
        return Err(invalid_crs(
            array_path,
            format!(
                "conflicting EPSG identifiers: EPSG:{} from {} and EPSG:{} from {}",
                first.code, first.source, conflict.code, conflict.source
            ),
        ));
    }

    let geotransform = collected
        .geotransforms
        .first()
        .map(|(transform, _)| *transform);
    if let Some((first_transform, first_source)) = collected.geotransforms.first()
        && let Some((_, conflict_source)) = collected
            .geotransforms
            .iter()
            .skip(1)
            .find(|(transform, _)| transform != first_transform)
    {
        return Err(invalid_crs(
            array_path,
            format!("conflicting GeoTransform values in {first_source} and {conflict_source}"),
        ));
    }

    Ok(ResolvedCrs {
        epsg: first.code,
        wkt: collected.wkt.into_iter().next(),
        geotransform,
    })
}

fn collect_attributes(
    array_path: &str,
    source: &str,
    attributes: &Map<String, Value>,
    collected: &mut CollectedCrs,
) -> ZarrFdwResult<()> {
    if let Some(value) = attributes.get("epsg_code") {
        let code = parse_epsg_value(value).ok_or_else(|| {
            invalid_crs(
                array_path,
                format!("epsg_code in {source} must be a positive integer or 'EPSG:<integer>'"),
            )
        })?;
        collected.epsg.push(EpsgCandidate {
            code,
            source: format!("{source}.epsg_code"),
        });
    }

    if let Some(value) = attributes.get("crs") {
        collect_crs_value(array_path, source, value, collected)?;
    }
    for attribute in ["spatial_ref", "crs_wkt"] {
        let Some(value) = attributes.get(attribute) else {
            continue;
        };
        let text = nonempty_string(array_path, source, attribute, value)?;
        if let Some(code) = parse_epsg_label(text) {
            collected.epsg.push(EpsgCandidate {
                code,
                source: format!("{source}.{attribute}"),
            });
        } else {
            collected.wkt.push(text.to_string());
        }
    }

    if let Some(value) = attributes.get(GEO_TRANSFORM) {
        let transform = parse_geotransform(array_path, source, value)?;
        if transform[2] != 0.0 || transform[4] != 0.0 {
            return Err(invalid_crs(
                array_path,
                format!(
                    "{GEO_TRANSFORM} in {source} describes a rotated grid; only zero-rotation rectilinear grids are supported"
                ),
            ));
        }
        collected
            .geotransforms
            .push((transform, source.to_string()));
    }
    Ok(())
}

fn collect_crs_value(
    array_path: &str,
    source: &str,
    value: &Value,
    collected: &mut CollectedCrs,
) -> ZarrFdwResult<()> {
    let text = match value {
        Value::String(value) if !value.trim().is_empty() => value.as_str(),
        Value::Object(object) if object.get("type").and_then(Value::as_str) == Some("name") => {
            object
                .get("properties")
                .and_then(Value::as_object)
                .and_then(|properties| properties.get("name"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    invalid_crs(
                        array_path,
                        format!(
                            "crs in {source} with type 'name' must contain a non-empty properties.name string"
                        ),
                    )
                })?
        }
        _ => {
            return Err(invalid_crs(
                array_path,
                format!("crs in {source} must be a non-empty string or a named CRS object"),
            ));
        }
    };

    if let Some(code) = parse_epsg_label(text) {
        collected.epsg.push(EpsgCandidate {
            code,
            source: format!("{source}.crs"),
        });
    } else {
        collected.wkt.push(text.to_string());
    }
    Ok(())
}

fn parse_epsg_value(value: &Value) -> Option<i32> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .filter(|value| *value > 0),
        Value::String(value) => parse_epsg_label(value),
        _ => None,
    }
}

fn parse_epsg_label(value: &str) -> Option<i32> {
    let (authority, code) = value.split_once(':')?;
    if !authority.eq_ignore_ascii_case("EPSG")
        || code.is_empty()
        || !code.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    code.parse::<i32>().ok().filter(|code| *code > 0)
}

fn nonempty_string<'a>(
    array_path: &str,
    source: &str,
    attribute: &str,
    value: &'a Value,
) -> ZarrFdwResult<&'a str> {
    value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            invalid_crs(
                array_path,
                format!("{attribute} in {source} must be a non-empty string"),
            )
        })
}

fn parse_geotransform(array_path: &str, source: &str, value: &Value) -> ZarrFdwResult<[f64; 6]> {
    let values = match value {
        Value::String(value) => value
            .split_whitespace()
            .map(str::parse::<f64>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| {
                invalid_crs(
                    array_path,
                    format!("{GEO_TRANSFORM} in {source} must contain six numbers"),
                )
            })?,
        Value::Array(values) => values
            .iter()
            .map(Value::as_f64)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                invalid_crs(
                    array_path,
                    format!("{GEO_TRANSFORM} in {source} must contain six numbers"),
                )
            })?,
        _ => {
            return Err(invalid_crs(
                array_path,
                format!(
                    "{GEO_TRANSFORM} in {source} must be a whitespace-separated string or numeric array"
                ),
            ));
        }
    };
    let transform: [f64; 6] = values.try_into().map_err(|values: Vec<f64>| {
        invalid_crs(
            array_path,
            format!(
                "{GEO_TRANSFORM} in {source} has {} values; expected exactly six",
                values.len()
            ),
        )
    })?;
    if transform.iter().any(|value| !value.is_finite()) {
        return Err(invalid_crs(
            array_path,
            format!("{GEO_TRANSFORM} in {source} must contain only finite numbers"),
        ));
    }
    Ok(transform)
}

fn validate_same_group_name(array_path: &str, reference: &str) -> ZarrFdwResult<()> {
    if reference.is_empty()
        || reference.trim() != reference
        || reference.chars().any(char::is_whitespace)
        || reference.chars().any(char::is_control)
        || reference.contains('/')
        || reference.contains('\\')
        || matches!(reference, "." | "..")
    {
        return Err(invalid_crs(
            array_path,
            format!(
                "grid_mapping reference '{reference}' must be a non-empty same-group array name"
            ),
        ));
    }
    Ok(())
}

fn same_group_sibling_path(array_path: &str, reference: &str) -> String {
    array_path
        .rsplit_once('/')
        .map(|(parent, _)| format!("{parent}/{reference}"))
        .unwrap_or_else(|| reference.to_string())
}

fn invalid_crs(array: &str, message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidCrs {
        array: array.to_string(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn attributes(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn resolves_consistent_grid_mapping_array_and_group_candidates() {
        let array = attributes(json!({
            "grid_mapping": "spatial_ref",
            "crs": {"type": "name", "properties": {"name": "epsg:3857"}}
        }));
        let mapping = attributes(json!({
            "epsg_code": "EPSG:3857",
            "spatial_ref": "PROJCRS[\"WGS 84 / Pseudo-Mercator\"]",
            "GeoTransform": "100 10 0 50 0 -10"
        }));
        let group = attributes(json!({"crs": "EPSG:3857"}));

        let resolved = resolve_crs(
            "nested/raw",
            &array,
            Some(&group),
            Some(GridMappingMetadata {
                path: "nested/spatial_ref",
                attributes: &mapping,
            }),
        )
        .unwrap();

        assert_eq!(resolved.epsg, 3857);
        assert_eq!(
            resolved.wkt.as_deref(),
            Some("PROJCRS[\"WGS 84 / Pseudo-Mercator\"]")
        );
        assert_eq!(
            resolved.geotransform,
            Some([100.0, 10.0, 0.0, 50.0, 0.0, -10.0])
        );
    }

    #[test]
    fn root_array_grid_mapping_resolves_to_root_sibling() {
        let array = attributes(json!({"grid_mapping": "crs"}));
        assert_eq!(
            grid_mapping_sibling_path("temperature", &array).unwrap(),
            Some("crs".to_string())
        );
    }

    #[test]
    fn rejects_invalid_or_mismatched_grid_mapping_references() {
        for reference in [json!(7), json!(""), json!("../crs"), json!("spatial ref")] {
            let array = attributes(json!({"grid_mapping": reference}));
            assert!(grid_mapping_sibling_path("nested/raw", &array).is_err());
        }

        let array = attributes(json!({"grid_mapping": "spatial_ref"}));
        let mapping = attributes(json!({"epsg_code": 3857}));
        assert!(
            resolve_crs(
                "nested/raw",
                &array,
                None,
                Some(GridMappingMetadata {
                    path: "other/spatial_ref",
                    attributes: &mapping,
                }),
            )
            .is_err()
        );
        assert!(resolve_crs("nested/raw", &array, None, None).is_err());
    }

    #[test]
    fn rejects_conflicting_epsg_candidates() {
        let array = attributes(json!({"crs": "EPSG:4326"}));
        let group = attributes(json!({"epsg_code": 3857}));
        let error = resolve_crs("nested/raw", &array, Some(&group), None).unwrap_err();
        assert!(error.to_string().contains("conflicting EPSG identifiers"));
    }

    #[test]
    fn rejects_wkt_only_or_invalid_epsg_metadata() {
        let wkt = attributes(json!({"crs_wkt": "GEOGCRS[\"WGS 84\"]"}));
        assert!(resolve_crs("nested/raw", &wkt, None, None).is_err());

        for value in [json!(0), json!(-1), json!("EPSG:0"), json!("EPSG:abc")] {
            let invalid = attributes(json!({"epsg_code": value}));
            assert!(resolve_crs("nested/raw", &invalid, None, None).is_err());
        }
    }

    #[test]
    fn validates_geotransform_shape_finiteness_rotation_and_conflicts() {
        for value in [
            json!("0 1 0 2 0"),
            json!("0 1 NaN 2 0 -1"),
            json!([0, 1, 0.5, 2, 0, -1]),
        ] {
            let invalid = attributes(json!({
                "epsg_code": 3857,
                "GeoTransform": value
            }));
            assert!(resolve_crs("nested/raw", &invalid, None, None).is_err());
        }

        let array = attributes(json!({
            "epsg_code": 3857,
            "GeoTransform": [0, 1, 0, 2, 0, -1]
        }));
        let group = attributes(json!({
            "epsg_code": 3857,
            "GeoTransform": [10, 1, 0, 2, 0, -1]
        }));
        let error = resolve_crs("nested/raw", &array, Some(&group), None).unwrap_err();
        assert!(error.to_string().contains("conflicting GeoTransform"));
    }
}
