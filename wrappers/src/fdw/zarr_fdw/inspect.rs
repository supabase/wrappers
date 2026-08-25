//! Read-only Zarr hierarchy and scientific-metadata inspection.
//!
//! This module intentionally does not call the FDW scan executor. It lists
//! group prefixes and reads only v2 `.zgroup`, `.zarray`, and `.zattrs`
//! objects or v3 `zarr.json` objects.

use std::collections::{HashMap, HashSet, VecDeque};

use pgrx::pg_sys::panic::{ErrorReport, ErrorReportable};
use pgrx::{JsonB, pg_sys, prelude::*};
use serde_json::{Map, Value, json};
use supabase_wrappers::prelude::ForeignServer;

use super::dataset::parse_named_dimensions;
use super::meta::{ArrayNode, NodeMeta, ZarrFormat, parse_v2_array, parse_v2_group, parse_v3_node};
use super::store::{MAX_METADATA_OBJECT_BYTES, ZarrStore, join_key};
use super::{ZarrFdwError, ZarrFdwResult};

const MAX_INSPECTION_DEPTH: usize = 32;
const MAX_INSPECTION_NODES: usize = 10_000;
const MAX_INSPECTION_LIST_PAGES: usize = 1_000;
const MAX_INSPECTION_METADATA_BYTES: usize = 64 * 1024 * 1024;
const MAX_INSPECTION_DERIVED_CRS_BYTES: usize = MAX_INSPECTION_METADATA_BYTES;

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
    let mut pending = VecDeque::from([(String::new(), 0usize, None)]);
    let mut discovered = HashSet::from([String::new()]);
    let mut rows = Vec::new();
    let mut metadata_bytes = 0usize;
    let mut list_pages = 0usize;

    while let Some((path, depth, parent_format)) = pending.pop_front() {
        let v3_key = metadata_key(&path, "zarr.json");
        let v2_array_key = metadata_key(&path, ".zarray");
        let v3 = read_optional_metadata(store, &v3_key, &mut metadata_bytes)?;
        let v2_array = read_optional_metadata(store, &v2_array_key, &mut metadata_bytes)?;

        let (group_format, group_attributes) = if let Some(bytes) = v3 {
            let v2_group_key = metadata_key(&path, ".zgroup");
            let v2_group = read_optional_metadata(store, &v2_group_key, &mut metadata_bytes)?;
            reject_dual_metadata(&path, true, v2_array.is_some(), v2_group.is_some())?;
            validate_hierarchy_format(&path, parent_format, ZarrFormat::V3)?;

            match parse_v3_node(&bytes).map_err(|error| {
                ZarrFdwError::InvalidMetadata(format!(
                    "could not parse '{}': {error}",
                    display_path(&v3_key)
                ))
            })? {
                NodeMeta::Array(node) => {
                    rows.push(array_row(&path, *node));
                    continue;
                }
                NodeMeta::Group(node) => (Some(node.format), node.attributes),
            }
        } else if let Some(bytes) = v2_array {
            validate_hierarchy_format(&path, parent_format, ZarrFormat::V2)?;
            let attributes = read_attributes(store, &path, &mut metadata_bytes)?;
            let node = parse_v2_array(&bytes, attributes).map_err(|error| {
                ZarrFdwError::InvalidMetadata(format!(
                    "could not parse '{}': {error}",
                    display_path(&v2_array_key)
                ))
            })?;
            rows.push(array_row(&path, node));
            continue;
        } else {
            if parent_format == Some(ZarrFormat::V3) {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "Zarr v3 node '{}' must contain explicit zarr.json metadata",
                    display_path(&path)
                )));
            }
            let v2_group_key = metadata_key(&path, ".zgroup");
            let v2_group = read_optional_metadata(store, &v2_group_key, &mut metadata_bytes)?;
            let attributes = read_attributes(store, &path, &mut metadata_bytes)?;
            let group = v2_group
                .map(|bytes| {
                    parse_v2_group(&bytes, attributes.clone()).map_err(|error| {
                        ZarrFdwError::InvalidMetadata(format!(
                            "could not parse '{}': {error}",
                            display_path(&v2_group_key)
                        ))
                    })
                })
                .transpose()?;
            if let Some(group) = &group {
                validate_hierarchy_format(&path, parent_format, group.format)?;
            }
            (
                group.as_ref().map(|group| group.format),
                group.map(|group| group.attributes).unwrap_or(attributes),
            )
        };

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

        let is_group = group_format.is_some()
            || !group_attributes.is_empty()
            || !child_prefixes.is_empty()
            || path.is_empty();
        if is_group {
            let mut warnings = Vec::new();
            if group_format.is_none() {
                warnings.push("group has no .zgroup metadata".to_string());
            }
            rows.push(group_row(
                &path,
                group_format.map(zarr_format_number),
                Value::Object(group_attributes),
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
            pending.push_back((child, depth + 1, group_format.or(parent_format)));
        }
    }

    rows.sort_by(|left, right| left.path.cmp(&right.path));
    resolve_crs_references(&mut rows);
    Ok(rows)
}

fn reject_dual_metadata(
    path: &str,
    has_v3: bool,
    has_v2_array: bool,
    has_v2_group: bool,
) -> ZarrFdwResult<()> {
    if has_v3 && (has_v2_array || has_v2_group) {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "node '{}' contains both zarr.json and Zarr v2 metadata",
            display_path(path)
        )));
    }
    Ok(())
}

fn validate_hierarchy_format(
    path: &str,
    parent_format: Option<ZarrFormat>,
    node_format: ZarrFormat,
) -> ZarrFdwResult<()> {
    if node_format == ZarrFormat::V3 && !path.is_empty() && parent_format.is_none() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "Zarr v3 node '{}' requires explicit Zarr v3 metadata on every ancestor group",
            display_path(path)
        )));
    }
    if let Some(parent_format) = parent_format {
        if parent_format != node_format {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "node '{}' uses Zarr v{}, but its parent group uses Zarr v{}",
                display_path(path),
                zarr_format_number(node_format),
                zarr_format_number(parent_format)
            )));
        }
    }
    Ok(())
}

fn zarr_format_number(format: ZarrFormat) -> i64 {
    match format {
        ZarrFormat::V2 => 2,
        ZarrFormat::V3 => 3,
    }
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

fn array_row(path: &str, node: ArrayNode) -> InspectionRow {
    let ArrayNode {
        format,
        meta,
        attributes: attrs,
        dimension_names,
        native_dtype,
        native_codecs,
    } = node;
    let mut warnings = Vec::new();
    let dimensions = inspected_dimensions(
        format,
        dimension_names.as_deref(),
        &attrs,
        meta.shape.len(),
        &mut warnings,
    );
    let units = string_attribute(&attrs, "units", &mut warnings);
    let calendar = string_attribute(&attrs, "calendar", &mut warnings);
    let scale_factor = numeric_attribute(&attrs, "scale_factor", &mut warnings);
    let add_offset = numeric_attribute(&attrs, "add_offset", &mut warnings);
    let crs = crs_attribute(&attrs);

    InspectionRow {
        path: display_path(path),
        kind: "array".to_string(),
        group_path: parent_path(path),
        variable: Some(node_name(path)),
        zarr_format: Some(zarr_format_number(format)),
        shape: Some(json!(meta.shape)),
        dimensions,
        dtype: Some(native_dtype),
        chunks: Some(json!(meta.native_chunk_shape())),
        codecs: Some(native_codecs),
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
    match parse_named_dimensions(attrs, rank) {
        Ok(dimensions) => dimensions,
        Err(message) => {
            warnings.push(message);
            None
        }
    }
}

fn inspected_dimensions(
    format: ZarrFormat,
    native: Option<&[Option<String>]>,
    attrs: &Map<String, Value>,
    rank: usize,
    warnings: &mut Vec<String>,
) -> Option<Vec<String>> {
    if format == ZarrFormat::V2 {
        return named_dimensions(attrs, rank, warnings);
    }

    let native = native?;
    if native.len() != rank {
        warnings.push(format!(
            "dimension_names has {} names but the array rank is {rank}",
            native.len()
        ));
        return None;
    }
    let Some(names) = native.iter().cloned().collect::<Option<Vec<_>>>() else {
        warnings.push("dimension_names contains an unnamed dimension".to_string());
        return None;
    };
    let probe = Map::from_iter([("_ARRAY_DIMENSIONS".to_string(), json!(names))]);
    match parse_named_dimensions(&probe, rank) {
        Ok(dimensions) => dimensions,
        Err(message) => {
            warnings.push(
                message
                    .replace("_ARRAY_DIMENSIONS names", "dimension_names")
                    .replace("_ARRAY_DIMENSIONS", "dimension_names"),
            );
            None
        }
    }
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
    use super::super::meta::{ArrayMeta, ChunkKeyEncoding};
    use super::*;

    fn array_node(attributes: Map<String, Value>) -> ArrayNode {
        ArrayNode {
            format: ZarrFormat::V2,
            meta: ArrayMeta {
                zarr_format: 2,
                shape: vec![2, 5, 6],
                chunks: vec![1, 3, 4],
                dtype: "<f4".to_string(),
                fill_value: json!(-7.5),
                compressor: Some(json!({"id": "blosc", "cname": "lz4"})),
                codec_pipeline: super::super::codec::CodecPipeline::raw_v2(),
                storage_layout: super::super::sharding::StorageLayout::Direct,
                chunk_key_encoding: ChunkKeyEncoding::V2 { separator: '.' },
                order: 'C',
                filters: Some(vec![]),
            },
            attributes,
            dimension_names: None,
            native_dtype: "<f4".to_string(),
            native_codecs: json!({
                "filters": [],
                "compressor": {"id": "blosc", "cname": "lz4"}
            }),
        }
    }

    fn v3_array_node(attributes: Map<String, Value>) -> ArrayNode {
        let mut node = array_node(attributes);
        node.format = ZarrFormat::V3;
        node.meta.zarr_format = 3;
        node.meta.chunk_key_encoding = ChunkKeyEncoding::Default { separator: '/' };
        node.dimension_names = Some(vec![
            Some("time".to_string()),
            Some("y".to_string()),
            Some("x".to_string()),
        ]);
        node.native_dtype = "float32".to_string();
        node.native_codecs = json!([{
            "name": "bytes",
            "configuration": {"endian": "little"}
        }]);
        node
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

        let row = array_row("climate/temperature", array_node(attrs));

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

        let row = array_row("temperature", array_node(attrs));

        assert!(row.dimensions.is_none());
        assert_eq!(
            row.attributes["_ARRAY_DIMENSIONS"],
            json!(["lat", "lat", "lon"])
        );
        assert_eq!(row.warnings, vec!["_ARRAY_DIMENSIONS names must be unique"]);
    }

    #[test]
    fn v3_array_row_exposes_native_metadata_and_embedded_attributes() {
        let attrs = serde_json::from_value::<Map<String, Value>>(json!({
            "units": "K",
            "scale_factor": 0.01,
            "add_offset": 273.15
        }))
        .unwrap();

        let row = array_row("nested/raw", v3_array_node(attrs));

        assert_eq!(row.zarr_format, Some(3));
        assert_eq!(row.dimensions.unwrap(), vec!["time", "y", "x"]);
        assert_eq!(row.dtype.as_deref(), Some("float32"));
        assert_eq!(
            row.codecs.unwrap(),
            json!([{
                "name": "bytes",
                "configuration": {"endian": "little"}
            }])
        );
        assert_eq!(row.attributes["units"], json!("K"));
        assert!(row.warnings.is_empty());
    }

    #[test]
    fn rejects_dual_or_implicitly_nested_v3_metadata() {
        assert!(matches!(
            reject_dual_metadata("nested/raw", true, true, false),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message == "node 'nested/raw' contains both zarr.json and Zarr v2 metadata"
        ));
        assert!(matches!(
            validate_hierarchy_format("nested/raw", None, ZarrFormat::V3),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message == "Zarr v3 node 'nested/raw' requires explicit Zarr v3 metadata on every ancestor group"
        ));
        validate_hierarchy_format("", None, ZarrFormat::V3).unwrap();
        validate_hierarchy_format("nested/raw", Some(ZarrFormat::V3), ZarrFormat::V3).unwrap();
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
            array_row("nested/raw", array_node(value_attrs)),
            array_row("nested/spatial_ref", array_node(ref_attrs)),
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
            array_row("nested/raw", array_node(value_attrs)),
            array_row("nested/spatial_ref", array_node(ref_attrs)),
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
            array_row("nested/missing_raw", array_node(missing_attrs)),
            array_row("nested/non_array_raw", array_node(non_array_attrs)),
            array_row("nested/crs_less_raw", array_node(crs_less_attrs)),
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
            array_row("nested/no_crs", array_node(Map::new())),
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
            array_row("nested/non_string", array_node(non_string_attrs)),
            array_row("nested/empty", array_node(empty_attrs)),
            array_row("nested/path_like", array_node(path_like_attrs)),
            array_row("nested/multi_token", array_node(multi_token_attrs)),
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
            array_row("nested/raw", array_node(value_attrs)),
            array_row("nested/spatial_ref", array_node(ref_attrs)),
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
