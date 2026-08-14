//! Read-only Zarr hierarchy and scientific-metadata inspection.
//!
//! This module intentionally does not call the FDW scan executor. It lists
//! group prefixes and reads only `.zgroup`, `.zarray`, and `.zattrs` objects.

use std::collections::{HashMap, HashSet, VecDeque};

use pgrx::pg_sys::panic::{ErrorReport, ErrorReportable};
use pgrx::{JsonB, pg_sys, prelude::*};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use supabase_wrappers::prelude::ForeignServer;

use super::meta::ArrayMeta;
use super::store::{MAX_METADATA_OBJECT_BYTES, ZarrStore, join_key};
use super::{ZarrFdwError, ZarrFdwResult};

const MAX_INSPECTION_DEPTH: usize = 32;
const MAX_INSPECTION_NODES: usize = 10_000;
const MAX_INSPECTION_LIST_PAGES: usize = 1_000;
const MAX_INSPECTION_METADATA_BYTES: usize = 64 * 1024 * 1024;
const MAX_INSPECTION_DERIVED_CRS_BYTES: usize = MAX_INSPECTION_METADATA_BYTES;

#[derive(Debug, Deserialize)]
struct GroupMeta {
    zarr_format: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct InspectionRow {
    path: String,
    kind: String,
    group_path: Option<String>,
    variable: Option<String>,
    zarr_format: Option<i64>,
    shape: Option<Value>,
    dimensions: Option<Vec<String>>,
    dtype: Option<String>,
    chunks: Option<Value>,
    codecs: Option<Value>,
    units: Option<String>,
    fill_value: Option<Value>,
    scale_factor: Option<f64>,
    add_offset: Option<f64>,
    crs: Option<Value>,
    calendar: Option<String>,
    attributes: Value,
    warnings: Vec<String>,
}

impl InspectionRow {
    // pgrx represents a named set-returning SQL record as an explicit tuple.
    #[allow(clippy::type_complexity)]
    fn sql_row(
        self,
    ) -> (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<JsonB>,
        Option<Vec<String>>,
        Option<String>,
        Option<JsonB>,
        Option<JsonB>,
        Option<String>,
        Option<JsonB>,
        Option<f64>,
        Option<f64>,
        Option<JsonB>,
        Option<String>,
        JsonB,
        Vec<String>,
    ) {
        (
            self.path,
            self.kind,
            self.group_path,
            self.variable,
            self.zarr_format,
            self.shape.map(JsonB),
            self.dimensions,
            self.dtype,
            self.chunks.map(JsonB),
            self.codecs.map(JsonB),
            self.units,
            self.fill_value.map(JsonB),
            self.scale_factor,
            self.add_offset,
            self.crs.map(JsonB),
            self.calendar,
            JsonB(self.attributes),
            self.warnings,
        )
    }
}

// pgrx requires the complete named SQL record in the exported signature.
#[allow(clippy::type_complexity)]
#[pg_extern(create_or_replace)]
fn zarr_inspect(
    server_name: &str,
) -> TableIterator<
    'static,
    (
        name!(path, String),
        name!(kind, String),
        name!(group_path, Option<String>),
        name!(variable, Option<String>),
        name!(zarr_format, Option<i64>),
        name!(shape, Option<JsonB>),
        name!(dimensions, Option<Vec<String>>),
        name!(dtype, Option<String>),
        name!(chunks, Option<JsonB>),
        name!(codecs, Option<JsonB>),
        name!(units, Option<String>),
        name!(fill_value, Option<JsonB>),
        name!(scale_factor, Option<f64>),
        name!(add_offset, Option<f64>),
        name!(crs, Option<JsonB>),
        name!(calendar, Option<String>),
        name!(attributes, JsonB),
        name!(warnings, Vec<String>),
    ),
> {
    let rows = inspect_server(server_name)
        .map_err(ErrorReport::from)
        .unwrap_or_report();
    TableIterator::new(rows.into_iter().map(InspectionRow::sql_row))
}

fn inspect_server(server_name: &str) -> ZarrFdwResult<Vec<InspectionRow>> {
    let server = load_foreign_server(server_name)?;
    let store = ZarrStore::new(&server)?;
    inspect_store(&store)
}

fn load_foreign_server(server_name: &str) -> ZarrFdwResult<ForeignServer> {
    let (server_oid, server_type, server_version, options) = Spi::connect(|client| {
        let rows = client.select(
            "SELECT s.oid::bigint AS server_oid,
                    s.srvtype::text AS server_type,
                    s.srvversion::text AS server_version,
                    option.option_name::text AS option_name,
                    option.option_value::text AS option_value
               FROM pg_catalog.pg_foreign_server AS s
               LEFT JOIN LATERAL pg_catalog.pg_options_to_table(s.srvoptions) AS option
                 ON true
              WHERE s.srvname = $1
                AND pg_catalog.has_server_privilege(s.oid, 'USAGE')",
            None,
            &[server_name.into()],
        )?;

        let mut server_oid = None;
        let mut server_type = None;
        let mut server_version = None;
        let mut options = HashMap::new();
        for row in rows {
            server_oid = row.get_by_name::<i64, _>("server_oid")?;
            server_type = row.get_by_name::<String, _>("server_type")?;
            server_version = row.get_by_name::<String, _>("server_version")?;
            if let (Some(name), Some(value)) = (
                row.get_by_name::<String, _>("option_name")?,
                row.get_by_name::<String, _>("option_value")?,
            ) {
                options.insert(name, value);
            }
        }

        Ok::<_, pgrx::spi::Error>((server_oid, server_type, server_version, options))
    })?;

    let server_oid = server_oid.ok_or_else(|| ZarrFdwError::ServerUnavailable {
        server: server_name.to_string(),
    })?;
    let server_oid = u32::try_from(server_oid).map_err(|_| {
        ZarrFdwError::InvalidMetadata("foreign server OID is out of range".to_string())
    })?;

    Ok(ForeignServer {
        server_oid: pg_sys::Oid::from_u32(server_oid),
        server_name: server_name.to_string(),
        server_type,
        server_version,
        options,
    })
}

fn inspect_store(store: &ZarrStore) -> ZarrFdwResult<Vec<InspectionRow>> {
    let mut pending = VecDeque::from([(String::new(), 0usize)]);
    let mut discovered = HashSet::from([String::new()]);
    let mut rows = Vec::new();
    let mut metadata_bytes = 0usize;
    let mut list_pages = 0usize;

    while let Some((path, depth)) = pending.pop_front() {
        let array_key = metadata_key(&path, ".zarray");
        if let Some(bytes) = read_optional_metadata(store, &array_key, &mut metadata_bytes)? {
            let meta = ArrayMeta::parse(&bytes).map_err(|error| {
                ZarrFdwError::InvalidMetadata(format!(
                    "could not parse '{}': {error}",
                    display_path(&array_key)
                ))
            })?;
            let attrs = read_attributes(store, &path, &mut metadata_bytes)?;
            rows.push(array_row(&path, meta, attrs));
            continue;
        }

        let group_key = metadata_key(&path, ".zgroup");
        let group_meta = read_optional_metadata(store, &group_key, &mut metadata_bytes)?
            .map(|bytes| {
                serde_json::from_slice::<GroupMeta>(&bytes).map_err(|error| {
                    ZarrFdwError::InvalidMetadata(format!(
                        "could not parse '{}': {error}",
                        display_path(&group_key)
                    ))
                })
            })
            .transpose()?;
        let attrs = read_attributes(store, &path, &mut metadata_bytes)?;

        let mut child_prefixes = Vec::new();
        let mut continuation_token = None;
        loop {
            list_pages = list_pages.checked_add(1).ok_or_else(|| {
                ZarrFdwError::InvalidMetadata("inspection list-page count overflowed".to_string())
            })?;
            if list_pages > MAX_INSPECTION_LIST_PAGES {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "inspection exceeds the limit of {MAX_INSPECTION_LIST_PAGES} S3 list pages"
                )));
            }
            let page = store.list_directory_page_sync(&path, continuation_token)?;
            for child in page.child_prefixes {
                if discovered.insert(child.clone()) {
                    if discovered.len() > MAX_INSPECTION_NODES {
                        return Err(ZarrFdwError::InvalidMetadata(format!(
                            "inspection exceeds the limit of {MAX_INSPECTION_NODES} Zarr nodes"
                        )));
                    }
                    child_prefixes.push(child);
                }
            }
            continuation_token = page.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        let is_group = group_meta.is_some()
            || !attrs.is_empty()
            || !child_prefixes.is_empty()
            || path.is_empty();
        if is_group {
            let mut warnings = Vec::new();
            if group_meta.is_none() {
                warnings.push("group has no .zgroup metadata".to_string());
            }
            rows.push(group_row(
                &path,
                group_meta.map(|meta| i64::from(meta.zarr_format)),
                Value::Object(attrs),
                warnings,
            ));
        }

        if !child_prefixes.is_empty() && depth >= MAX_INSPECTION_DEPTH {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "inspection exceeds the maximum group depth of {MAX_INSPECTION_DEPTH} at '{}'",
                display_path(&path)
            )));
        }
        child_prefixes.sort();
        for child in child_prefixes {
            pending.push_back((child, depth + 1));
        }
    }

    rows.sort_by(|left, right| left.path.cmp(&right.path));
    resolve_crs_references(&mut rows);
    Ok(rows)
}

fn read_optional_metadata(
    store: &ZarrStore,
    key: &str,
    total_bytes: &mut usize,
) -> ZarrFdwResult<Option<Vec<u8>>> {
    let bytes = store.get_object_optional_sync(key, MAX_METADATA_OBJECT_BYTES)?;
    if let Some(bytes) = &bytes {
        *total_bytes = total_bytes.checked_add(bytes.len()).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("inspection metadata byte count overflowed".to_string())
        })?;
        if *total_bytes > MAX_INSPECTION_METADATA_BYTES {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "inspection exceeds the metadata read limit of {MAX_INSPECTION_METADATA_BYTES} bytes"
            )));
        }
    }
    Ok(bytes)
}

fn read_attributes(
    store: &ZarrStore,
    path: &str,
    total_bytes: &mut usize,
) -> ZarrFdwResult<Map<String, Value>> {
    let key = metadata_key(path, ".zattrs");
    let Some(bytes) = read_optional_metadata(store, &key, total_bytes)? else {
        return Ok(Map::new());
    };
    let value = serde_json::from_slice::<Value>(&bytes).map_err(|error| {
        ZarrFdwError::InvalidMetadata(format!("could not parse '{}': {error}", display_path(&key)))
    })?;
    value.as_object().cloned().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!(
            "'{}' must contain a JSON object",
            display_path(&key)
        ))
    })
}

fn array_row(path: &str, meta: ArrayMeta, attrs: Map<String, Value>) -> InspectionRow {
    let mut warnings = Vec::new();
    let dimensions = named_dimensions(&attrs, meta.shape.len(), &mut warnings);
    let units = string_attribute(&attrs, "units", &mut warnings);
    let calendar = string_attribute(&attrs, "calendar", &mut warnings);
    let scale_factor = numeric_attribute(&attrs, "scale_factor", &mut warnings);
    let add_offset = numeric_attribute(&attrs, "add_offset", &mut warnings);
    let crs = crs_attribute(&attrs);
    let codecs = json!({
        "filters": meta.filters,
        "compressor": meta.compressor,
    });

    InspectionRow {
        path: display_path(path),
        kind: "array".to_string(),
        group_path: parent_path(path),
        variable: Some(node_name(path)),
        zarr_format: Some(i64::from(meta.zarr_format)),
        shape: Some(json!(meta.shape)),
        dimensions,
        dtype: Some(meta.dtype),
        chunks: Some(json!(meta.chunks)),
        codecs: Some(codecs),
        units,
        fill_value: Some(meta.fill_value),
        scale_factor,
        add_offset,
        crs,
        calendar,
        attributes: Value::Object(attrs),
        warnings,
    }
}

fn group_row(
    path: &str,
    zarr_format: Option<i64>,
    attributes: Value,
    warnings: Vec<String>,
) -> InspectionRow {
    let crs = attributes.as_object().and_then(crs_attribute);
    InspectionRow {
        path: display_path(path),
        kind: "group".to_string(),
        group_path: parent_path(path),
        variable: None,
        zarr_format,
        shape: None,
        dimensions: None,
        dtype: None,
        chunks: None,
        codecs: None,
        units: None,
        fill_value: None,
        scale_factor: None,
        add_offset: None,
        crs,
        calendar: None,
        attributes,
        warnings,
    }
}

fn crs_attribute(attrs: &Map<String, Value>) -> Option<Value> {
    direct_crs_attribute(attrs).or_else(|| attrs.get("grid_mapping").cloned())
}

fn direct_crs_attribute(attrs: &Map<String, Value>) -> Option<Value> {
    ["crs", "spatial_ref", "crs_wkt"]
        .iter()
        .find_map(|key| attrs.get(*key).cloned())
}

fn resolve_crs_references(rows: &mut [InspectionRow]) {
    resolve_crs_references_with_limit(rows, MAX_INSPECTION_DERIVED_CRS_BYTES);
}

fn resolve_crs_references_with_limit(rows: &mut [InspectionRow], max_derived_bytes: usize) {
    let mut kinds_by_path = HashMap::new();
    let mut direct_crs_by_array_path = HashMap::new();
    for row in rows.iter() {
        kinds_by_path.insert(row.path.clone(), row.kind.clone());
        if row.kind == "array" {
            if let Some(crs) = row.attributes.as_object().and_then(direct_crs_attribute) {
                direct_crs_by_array_path.insert(row.path.clone(), crs);
            }
        }
    }

    let mut derived_crs_bytes = 0usize;
    for row in rows.iter_mut() {
        if row.kind != "array" {
            continue;
        }
        let Some(grid_mapping) = row.attributes.as_object().and_then(|attrs| {
            if direct_crs_attribute(attrs).is_some() {
                return None;
            }
            attrs.get("grid_mapping").cloned()
        }) else {
            continue;
        };
        let Some(reference) = grid_mapping_reference(&grid_mapping, &mut row.warnings) else {
            continue;
        };
        let sibling_path = same_group_sibling_path(row.group_path.as_deref(), reference);
        match kinds_by_path.get(&sibling_path).map(String::as_str) {
            Some("array") => {
                let Some(crs) = direct_crs_by_array_path.get(&sibling_path) else {
                    row.warnings.push(format!(
                        "grid_mapping reference '{reference}' resolves to a sibling array without direct CRS metadata"
                    ));
                    continue;
                };
                let Ok(bytes) = serde_json::to_vec(crs) else {
                    row.warnings.push(format!(
                        "grid_mapping reference '{reference}' CRS metadata could not be measured"
                    ));
                    continue;
                };
                let Some(next_bytes) = derived_crs_bytes.checked_add(bytes.len()) else {
                    row.warnings.push(format!(
                        "grid_mapping reference '{reference}' exceeds the derived CRS byte limit of {max_derived_bytes} bytes"
                    ));
                    continue;
                };
                if next_bytes > max_derived_bytes {
                    row.warnings.push(format!(
                        "grid_mapping reference '{reference}' exceeds the derived CRS byte limit of {max_derived_bytes} bytes"
                    ));
                    continue;
                }
                derived_crs_bytes = next_bytes;
                row.crs = Some(crs.clone());
            }
            _ => row.warnings.push(format!(
                "grid_mapping reference '{reference}' does not resolve to a sibling array"
            )),
        }
    }
}

fn grid_mapping_reference<'a>(value: &'a Value, warnings: &mut Vec<String>) -> Option<&'a str> {
    let Some(reference) = value.as_str() else {
        warnings.push("grid_mapping reference must be a string".to_string());
        return None;
    };
    if reference.trim().is_empty() {
        warnings.push("grid_mapping reference must not be empty".to_string());
        return None;
    }
    if reference.trim() != reference
        || reference.chars().any(char::is_whitespace)
        || reference.contains('/')
        || reference.contains('\\')
        || reference == "."
        || reference == ".."
    {
        warnings.push(format!(
            "grid_mapping reference '{reference}' must be a same-group array name"
        ));
        return None;
    }
    Some(reference)
}

fn same_group_sibling_path(group_path: Option<&str>, reference: &str) -> String {
    match group_path {
        Some("/") | None => reference.to_string(),
        Some(group_path) => format!("{group_path}/{reference}"),
    }
}

fn named_dimensions(
    attrs: &Map<String, Value>,
    rank: usize,
    warnings: &mut Vec<String>,
) -> Option<Vec<String>> {
    let value = attrs.get("_ARRAY_DIMENSIONS")?;
    let Some(values) = value.as_array() else {
        warnings.push("_ARRAY_DIMENSIONS must be an array of strings".to_string());
        return None;
    };
    let dimensions = values.iter().map(Value::as_str).collect::<Option<Vec<_>>>();
    let Some(dimensions) = dimensions else {
        warnings.push("_ARRAY_DIMENSIONS must contain only strings".to_string());
        return None;
    };
    if dimensions.len() != rank {
        warnings.push(format!(
            "_ARRAY_DIMENSIONS has {} names but the array rank is {rank}",
            dimensions.len()
        ));
        return None;
    }
    if dimensions.iter().any(|name| name.trim().is_empty()) {
        warnings.push("_ARRAY_DIMENSIONS names must not be empty".to_string());
        return None;
    }
    let unique = dimensions.iter().copied().collect::<HashSet<_>>();
    if unique.len() != dimensions.len() {
        warnings.push("_ARRAY_DIMENSIONS names must be unique".to_string());
        return None;
    }
    Some(dimensions.into_iter().map(str::to_string).collect())
}

fn string_attribute(
    attrs: &Map<String, Value>,
    name: &str,
    warnings: &mut Vec<String>,
) -> Option<String> {
    let value = attrs.get(name)?;
    match value.as_str() {
        Some(value) => Some(value.to_string()),
        None => {
            warnings.push(format!("attribute '{name}' must be a string"));
            None
        }
    }
}

fn numeric_attribute(
    attrs: &Map<String, Value>,
    name: &str,
    warnings: &mut Vec<String>,
) -> Option<f64> {
    let value = attrs.get(name)?;
    match value.as_f64() {
        Some(value) if value.is_finite() => Some(value),
        _ => {
            warnings.push(format!("attribute '{name}' must be a finite number"));
            None
        }
    }
}

fn metadata_key(path: &str, name: &str) -> String {
    join_key(path, name)
}

fn display_path(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        path.to_string()
    }
}

fn parent_path(path: &str) -> Option<String> {
    if path.is_empty() {
        return None;
    }
    Some(
        path.rsplit_once('/')
            .map(|(parent, _)| display_path(parent))
            .unwrap_or_else(|| "/".to_string()),
    )
}

fn node_name(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        path.rsplit('/').next().unwrap_or(path).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn array_meta() -> ArrayMeta {
        ArrayMeta {
            zarr_format: 2,
            shape: vec![2, 5, 6],
            chunks: vec![1, 3, 4],
            dtype: "<f4".to_string(),
            fill_value: json!(-7.5),
            compressor: Some(json!({"id": "blosc", "cname": "lz4"})),
            dimension_separator: ".".to_string(),
            order: 'C',
            filters: Some(vec![]),
        }
    }

    #[test]
    fn array_row_exposes_scientific_metadata_without_decoding_it() {
        let attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "_ARRAY_DIMENSIONS": ["time", "lat", "lon"],
            "units": "K",
            "calendar": "proleptic_gregorian",
            "scale_factor": 0.01,
            "add_offset": 273.15,
            "grid_mapping": "spatial_ref",
            "long_name": "air temperature"
        }))
        .unwrap();

        let row = array_row("climate/temperature", array_meta(), attrs);

        assert_eq!(row.path, "climate/temperature");
        assert_eq!(row.group_path.as_deref(), Some("climate"));
        assert_eq!(row.variable.as_deref(), Some("temperature"));
        assert_eq!(row.dimensions.unwrap(), vec!["time", "lat", "lon"]);
        assert_eq!(row.units.as_deref(), Some("K"));
        assert_eq!(row.calendar.as_deref(), Some("proleptic_gregorian"));
        assert_eq!(row.scale_factor, Some(0.01));
        assert_eq!(row.add_offset, Some(273.15));
        assert_eq!(row.crs, Some(json!("spatial_ref")));
        assert_eq!(row.fill_value, Some(json!(-7.5)));
        assert_eq!(row.codecs.unwrap()["compressor"]["id"], "blosc");
        assert!(row.warnings.is_empty());
    }

    #[test]
    fn malformed_named_dimensions_are_preserved_as_attributes_and_warned() {
        let attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "_ARRAY_DIMENSIONS": ["lat", "lat", "lon"]
        }))
        .unwrap();

        let row = array_row("temperature", array_meta(), attrs);

        assert!(row.dimensions.is_none());
        assert_eq!(
            row.attributes["_ARRAY_DIMENSIONS"],
            json!(["lat", "lat", "lon"])
        );
        assert_eq!(row.warnings, vec!["_ARRAY_DIMENSIONS names must be unique"]);
    }

    #[test]
    fn crs_resolution_preserves_direct_precedence() {
        let value_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "crs": "EPSG:4326",
            "grid_mapping": "spatial_ref"
        }))
        .unwrap();
        let ref_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "spatial_ref": "EPSG:3857"
        }))
        .unwrap();
        let mut rows = vec![
            array_row("nested/raw", array_meta(), value_attrs),
            array_row("nested/spatial_ref", array_meta(), ref_attrs),
        ];

        resolve_crs_references(&mut rows);

        let raw = rows.iter().find(|row| row.path == "nested/raw").unwrap();
        assert_eq!(raw.crs, Some(json!("EPSG:4326")));
        assert!(raw.warnings.is_empty());
    }

    #[test]
    fn crs_resolution_uses_same_group_sibling_array_direct_crs() {
        let value_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": "spatial_ref"
        }))
        .unwrap();
        let ref_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "spatial_ref": {"type": "ProjectedCRS", "name": "EPSG:3857"}
        }))
        .unwrap();
        let mut rows = vec![
            array_row("nested/raw", array_meta(), value_attrs),
            array_row("nested/spatial_ref", array_meta(), ref_attrs),
        ];

        resolve_crs_references(&mut rows);

        let raw = rows.iter().find(|row| row.path == "nested/raw").unwrap();
        assert_eq!(
            raw.crs,
            Some(json!({"type": "ProjectedCRS", "name": "EPSG:3857"}))
        );
        assert_eq!(raw.attributes["grid_mapping"], json!("spatial_ref"));
        assert!(raw.warnings.is_empty());
    }

    #[test]
    fn crs_resolution_warns_for_missing_non_array_and_crs_less_references() {
        let missing_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": "missing"
        }))
        .unwrap();
        let non_array_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": "group_ref"
        }))
        .unwrap();
        let crs_less_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": "no_crs"
        }))
        .unwrap();
        let group_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": "no_crs"
        }))
        .unwrap();
        let mut rows = vec![
            array_row("nested/missing_raw", array_meta(), missing_attrs),
            array_row("nested/non_array_raw", array_meta(), non_array_attrs),
            array_row("nested/crs_less_raw", array_meta(), crs_less_attrs),
            group_row(
                "nested/group_ref",
                Some(2),
                Value::Object(Map::new()),
                vec![],
            ),
            group_row(
                "nested/source_group",
                Some(2),
                Value::Object(group_attrs),
                vec![],
            ),
            array_row("nested/no_crs", array_meta(), Map::new()),
        ];

        resolve_crs_references(&mut rows);

        let missing = rows
            .iter()
            .find(|row| row.path == "nested/missing_raw")
            .unwrap();
        assert_eq!(missing.crs, Some(json!("missing")));
        assert_eq!(
            missing.warnings,
            vec!["grid_mapping reference 'missing' does not resolve to a sibling array"]
        );
        let non_array = rows
            .iter()
            .find(|row| row.path == "nested/non_array_raw")
            .unwrap();
        assert_eq!(non_array.crs, Some(json!("group_ref")));
        assert_eq!(
            non_array.warnings,
            vec!["grid_mapping reference 'group_ref' does not resolve to a sibling array"]
        );
        let crs_less = rows
            .iter()
            .find(|row| row.path == "nested/crs_less_raw")
            .unwrap();
        assert_eq!(crs_less.crs, Some(json!("no_crs")));
        assert_eq!(
            crs_less.warnings,
            vec![
                "grid_mapping reference 'no_crs' resolves to a sibling array without direct CRS metadata"
            ]
        );
        let source_group = rows
            .iter()
            .find(|row| row.path == "nested/source_group")
            .unwrap();
        assert_eq!(source_group.crs, Some(json!("no_crs")));
        assert!(source_group.warnings.is_empty());
    }

    #[test]
    fn crs_resolution_warns_for_invalid_references() {
        let non_string_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": 7
        }))
        .unwrap();
        let empty_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": ""
        }))
        .unwrap();
        let path_like_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": "../spatial_ref"
        }))
        .unwrap();
        let multi_token_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": "spatial ref"
        }))
        .unwrap();
        let mut rows = vec![
            array_row("nested/non_string", array_meta(), non_string_attrs),
            array_row("nested/empty", array_meta(), empty_attrs),
            array_row("nested/path_like", array_meta(), path_like_attrs),
            array_row("nested/multi_token", array_meta(), multi_token_attrs),
        ];

        resolve_crs_references(&mut rows);

        let non_string = rows
            .iter()
            .find(|row| row.path == "nested/non_string")
            .unwrap();
        assert_eq!(non_string.crs, Some(json!(7)));
        assert_eq!(
            non_string.warnings,
            vec!["grid_mapping reference must be a string"]
        );
        let empty = rows.iter().find(|row| row.path == "nested/empty").unwrap();
        assert_eq!(empty.crs, Some(json!("")));
        assert_eq!(
            empty.warnings,
            vec!["grid_mapping reference must not be empty"]
        );
        let path_like = rows
            .iter()
            .find(|row| row.path == "nested/path_like")
            .unwrap();
        assert_eq!(path_like.crs, Some(json!("../spatial_ref")));
        assert_eq!(
            path_like.warnings,
            vec!["grid_mapping reference '../spatial_ref' must be a same-group array name"]
        );
        let multi_token = rows
            .iter()
            .find(|row| row.path == "nested/multi_token")
            .unwrap();
        assert_eq!(multi_token.crs, Some(json!("spatial ref")));
        assert_eq!(
            multi_token.warnings,
            vec!["grid_mapping reference 'spatial ref' must be a same-group array name"]
        );
    }

    #[test]
    fn crs_resolution_enforces_derived_output_byte_cap() {
        let value_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "grid_mapping": "spatial_ref"
        }))
        .unwrap();
        let ref_attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "spatial_ref": "EPSG:3857"
        }))
        .unwrap();
        let mut rows = vec![
            array_row("nested/raw", array_meta(), value_attrs),
            array_row("nested/spatial_ref", array_meta(), ref_attrs),
        ];

        resolve_crs_references_with_limit(&mut rows, 4);

        let raw = rows.iter().find(|row| row.path == "nested/raw").unwrap();
        assert_eq!(raw.crs, Some(json!("spatial_ref")));
        assert_eq!(
            raw.warnings,
            vec![
                "grid_mapping reference 'spatial_ref' exceeds the derived CRS byte limit of 4 bytes"
            ]
        );
    }

    #[test]
    fn paths_are_stable_for_root_and_nested_nodes() {
        assert_eq!(display_path(""), "/");
        assert_eq!(parent_path(""), None);
        assert_eq!(parent_path("temperature"), Some("/".to_string()));
        assert_eq!(
            parent_path("nested/temperature"),
            Some("nested".to_string())
        );
        assert_eq!(node_name(""), "/");
        assert_eq!(node_name("nested/temperature"), "temperature");
    }
}
