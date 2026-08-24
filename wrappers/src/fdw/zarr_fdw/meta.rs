//! Version-neutral Zarr array metadata used by scan execution.
//!
//! Format-specific metadata is normalized here. The executor therefore sees
//! the same shape, chunk, dtype, fill, and chunk-key contract for Zarr v2 and
//! for the bounded direct Zarr v3 subset supported by this module.

use super::decode::{DType, fill_value_bytes};
use super::{ZarrFdwError, ZarrFdwResult};
use serde::Deserialize;
use serde_json::{Map, Value};

const MAX_SCAN_RANK: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZarrFormat {
    V2,
    V3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkKeyEncoding {
    /// Zarr v3 default encoding: `c/<indices>` or `c.<indices>`.
    Default { separator: char },
    /// Zarr v2 encoding, including when selected by a v3 array.
    V2 { separator: char },
}

#[derive(Debug, Clone)]
pub struct ArrayMeta {
    pub zarr_format: u32,
    pub shape: Vec<u64>,
    pub chunks: Vec<u64>,
    /// Executor-normalized NumPy dtype, for example `<f4`.
    pub dtype: String,
    pub fill_value: Value,
    /// V2 compressor metadata. Direct v3 arrays normalize to raw here.
    pub compressor: Option<Value>,
    pub chunk_key_encoding: ChunkKeyEncoding,
    pub order: char,
    pub filters: Option<Vec<Value>>,
}

#[derive(Debug, Clone)]
pub struct ArrayNode {
    pub format: ZarrFormat,
    pub meta: ArrayMeta,
    pub attributes: Map<String, Value>,
    /// Native v3 names. `None` distinguishes an absent field from a present
    /// list containing null entries. V2 names remain in `_ARRAY_DIMENSIONS`.
    pub dimension_names: Option<Vec<Option<String>>>,
    /// Native spelling retained for truthful inspection.
    pub native_dtype: String,
    /// Native ordered codec metadata retained for truthful inspection.
    pub native_codecs: Value,
}

#[derive(Debug, Clone)]
pub struct GroupNode {
    pub format: ZarrFormat,
    pub attributes: Map<String, Value>,
}

#[derive(Debug, Clone)]
pub enum NodeMeta {
    Array(Box<ArrayNode>),
    Group(GroupNode),
}

#[derive(Debug, Deserialize)]
struct V2ArrayMeta {
    zarr_format: u32,
    shape: Vec<u64>,
    chunks: Vec<u64>,
    dtype: String,
    fill_value: Value,
    compressor: Option<Value>,
    #[serde(default = "default_v2_separator")]
    dimension_separator: String,
    #[serde(default = "default_order")]
    order: char,
    filters: Option<Vec<Value>>,
}

fn default_v2_separator() -> String {
    ".".to_string()
}

fn default_order() -> char {
    'C'
}

impl ArrayMeta {
    pub(crate) fn validate(&self) -> ZarrFdwResult<()> {
        if !matches!(self.zarr_format, 2 | 3) {
            return Err(ZarrFdwError::UnsupportedZarrFormat {
                version: self.zarr_format,
            });
        }
        if !(1..=MAX_SCAN_RANK).contains(&self.shape.len()) {
            return Err(ZarrFdwError::UnsupportedRank {
                rank: self.shape.len(),
            });
        }
        self.validate_common()
    }

    fn validate_common(&self) -> ZarrFdwResult<()> {
        if self.shape.len() != self.chunks.len() {
            return Err(ZarrFdwError::InvalidMetadata(
                "shape and chunks lengths differ".to_string(),
            ));
        }
        if let Some(axis) = self.shape.iter().position(|&extent| extent == 0) {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "shape dimension {axis} must be greater than zero"
            )));
        }
        if let Some(axis) = self.chunks.iter().position(|&extent| extent == 0) {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "chunk dimension {axis} must be greater than zero"
            )));
        }
        for axis in 0..self.shape.len() {
            self.shape_extent(axis)?;
            self.chunk_extent(axis)?;
        }
        self.chunk_cell_count()?;
        if self
            .filters
            .as_ref()
            .is_some_and(|filters| !filters.is_empty())
        {
            return Err(ZarrFdwError::InvalidMetadata(
                "zarr filters are not supported yet".to_string(),
            ));
        }
        if self.order != 'C' {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "only row-major (C order) arrays are supported, got '{}'",
                self.order
            )));
        }
        Ok(())
    }

    pub fn validate_coordinate(&self) -> ZarrFdwResult<()> {
        if self.shape.len() != 1 {
            return Err(ZarrFdwError::CoordinateReadError {
                axis: String::new(),
                error: format!("coordinate array must be 1D, got rank {}", self.shape.len()),
            });
        }
        self.validate_common()
    }

    pub fn chunks_per_axis(&self) -> Vec<u64> {
        self.shape
            .iter()
            .zip(self.chunks.iter())
            .map(|(shape, chunk)| shape.div_ceil(*chunk))
            .collect()
    }

    pub fn shape_extent(&self, axis: usize) -> ZarrFdwResult<usize> {
        usize::try_from(self.shape[axis]).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "shape dimension {axis} exceeds this platform's index capacity"
            ))
        })
    }

    pub fn chunk_extent(&self, axis: usize) -> ZarrFdwResult<usize> {
        usize::try_from(self.chunks[axis]).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "chunk dimension {axis} exceeds this platform's index capacity"
            ))
        })
    }

    pub fn chunk_cell_count(&self) -> ZarrFdwResult<usize> {
        self.chunks
            .iter()
            .enumerate()
            .try_fold(1usize, |cells, (axis, &extent)| {
                let extent = usize::try_from(extent).map_err(|_| {
                    ZarrFdwError::InvalidMetadata(format!(
                        "chunk dimension {axis} exceeds this platform's index capacity"
                    ))
                })?;
                cells.checked_mul(extent).ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(
                        "declared chunk cell count exceeds this platform's index capacity"
                            .to_string(),
                    )
                })
            })
    }
}

pub fn parse_v2_array(bytes: &[u8], attributes: Map<String, Value>) -> ZarrFdwResult<ArrayNode> {
    let raw = serde_json::from_slice::<V2ArrayMeta>(bytes)?;
    let native_dtype = raw.dtype.clone();
    let native_codecs = serde_json::json!({
        "filters": raw.filters.clone(),
        "compressor": raw.compressor.clone(),
    });
    Ok(ArrayNode {
        format: ZarrFormat::V2,
        meta: v2_meta(raw)?,
        attributes,
        dimension_names: None,
        native_dtype,
        native_codecs,
    })
}

pub fn parse_v2_group(bytes: &[u8], attributes: Map<String, Value>) -> ZarrFdwResult<GroupNode> {
    let value = serde_json::from_slice::<Value>(bytes)?;
    let version = required_u64(value.as_object(), "zarr_format")?;
    if version != 2 {
        return Err(ZarrFdwError::UnsupportedZarrFormat {
            version: u32::try_from(version).unwrap_or(u32::MAX),
        });
    }
    Ok(GroupNode {
        format: ZarrFormat::V2,
        attributes,
    })
}

pub fn parse_v3_node(bytes: &[u8]) -> ZarrFdwResult<NodeMeta> {
    let value = serde_json::from_slice::<Value>(bytes)?;
    let object = value.as_object().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata("zarr.json must contain a JSON object".to_string())
    })?;
    let version = required_u64(Some(object), "zarr_format")?;
    if version != 3 {
        return Err(ZarrFdwError::UnsupportedZarrFormat {
            version: u32::try_from(version).unwrap_or(u32::MAX),
        });
    }
    let node_type = required_string(object, "node_type")?;
    if object.contains_key("consolidated_metadata") {
        return Err(ZarrFdwError::InvalidMetadata(
            "Zarr v3 consolidated metadata is not supported yet".to_string(),
        ));
    }
    match node_type {
        "group" => {
            validate_additional_fields(
                object,
                &["zarr_format", "node_type", "attributes"],
                "Zarr v3 group",
            )?;
            Ok(NodeMeta::Group(GroupNode {
                format: ZarrFormat::V3,
                attributes: optional_object(object, "attributes")?,
            }))
        }
        "array" => {
            validate_additional_fields(
                object,
                &[
                    "zarr_format",
                    "node_type",
                    "shape",
                    "data_type",
                    "chunk_grid",
                    "chunk_key_encoding",
                    "fill_value",
                    "codecs",
                    "attributes",
                    "storage_transformers",
                    "dimension_names",
                ],
                "Zarr v3 array",
            )?;
            parse_v3_array(object, optional_object(object, "attributes")?)
                .map(|node| NodeMeta::Array(Box::new(node)))
        }
        other => Err(ZarrFdwError::InvalidMetadata(format!(
            "zarr.json node_type must be 'array' or 'group', got '{other}'"
        ))),
    }
}

fn v2_meta(raw: V2ArrayMeta) -> ZarrFdwResult<ArrayMeta> {
    if raw.zarr_format != 2 {
        return Err(ZarrFdwError::UnsupportedZarrFormat {
            version: raw.zarr_format,
        });
    }
    let separator = parse_separator(&raw.dimension_separator, "dimension_separator")?;
    Ok(ArrayMeta {
        zarr_format: 2,
        shape: raw.shape,
        chunks: raw.chunks,
        dtype: raw.dtype,
        fill_value: raw.fill_value,
        compressor: raw.compressor,
        chunk_key_encoding: ChunkKeyEncoding::V2 { separator },
        order: raw.order,
        filters: raw.filters,
    })
}

fn parse_v3_array(
    object: &Map<String, Value>,
    attributes: Map<String, Value>,
) -> ZarrFdwResult<ArrayNode> {
    let shape = required_u64_array(object, "shape")?;
    let native_dtype = required_string(object, "data_type")?.to_string();
    let fill_value = object.get("fill_value").cloned().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata("zarr v3 array must define fill_value".to_string())
    })?;
    if fill_value.is_null() {
        return Err(ZarrFdwError::InvalidMetadata(
            "zarr v3 numeric fill_value must not be null".to_string(),
        ));
    }

    let chunk_grid = required_object(object, "chunk_grid")?;
    validate_extension_object(chunk_grid, "chunk_grid")?;
    reject_ignorable_extension(chunk_grid, "chunk_grid")?;
    if required_string(chunk_grid, "name")? != "regular" {
        return Err(ZarrFdwError::InvalidMetadata(
            "only the Zarr v3 regular chunk grid is supported".to_string(),
        ));
    }
    let chunk_grid_configuration = required_object(chunk_grid, "configuration")?;
    validate_exact_fields(
        chunk_grid_configuration,
        &["chunk_shape"],
        "regular chunk grid configuration",
    )?;
    let chunks = required_u64_array(chunk_grid_configuration, "chunk_shape")?;

    let key = required_object(object, "chunk_key_encoding")?;
    validate_extension_object(key, "chunk_key_encoding")?;
    reject_ignorable_extension(key, "chunk_key_encoding")?;
    let key_name = required_string(key, "name")?;
    let key_configuration = optional_object_ref(key, "configuration")?;
    if let Some(configuration) = key_configuration {
        validate_exact_fields(
            configuration,
            &["separator"],
            "chunk-key encoding configuration",
        )?;
    }
    let default_separator = if key_name == "default" { "/" } else { "." };
    let separator = key_configuration
        .and_then(|configuration| configuration.get("separator"))
        .map(|value| {
            value.as_str().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata("chunk-key separator must be a string".to_string())
            })
        })
        .transpose()?
        .unwrap_or(default_separator);
    let separator = parse_separator(separator, "chunk-key separator")?;
    let chunk_key_encoding = match key_name {
        "default" => ChunkKeyEncoding::Default { separator },
        "v2" => ChunkKeyEncoding::V2 { separator },
        other => {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "Zarr v3 chunk-key encoding '{other}' is not supported"
            )));
        }
    };

    if object
        .get("storage_transformers")
        .is_some_and(|value| value.as_array().is_none_or(|items| !items.is_empty()))
    {
        return Err(ZarrFdwError::InvalidMetadata(
            "Zarr v3 storage transformers are not supported yet".to_string(),
        ));
    }

    let native_codecs = object.get("codecs").cloned().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata("Zarr v3 array must define codecs".to_string())
    })?;
    let codecs = native_codecs.as_array().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata("Zarr v3 codecs must be an array".to_string())
    })?;
    if codecs.len() != 1 {
        return Err(ZarrFdwError::InvalidMetadata(
            "this Zarr v3 slice requires exactly one bytes codec".to_string(),
        ));
    }
    let codec = codecs[0].as_object().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata("Zarr v3 codec entries must be objects".to_string())
    })?;
    validate_extension_object(codec, "codec")?;
    if required_string(codec, "name")? != "bytes" {
        return Err(ZarrFdwError::InvalidMetadata(
            "this Zarr v3 slice supports only the bytes codec".to_string(),
        ));
    }
    let codec_configuration = optional_object_ref(codec, "configuration")?;
    if let Some(configuration) = codec_configuration {
        validate_exact_fields(configuration, &["endian"], "bytes codec configuration")?;
    }
    let (dtype, multi_byte) = normalize_v3_dtype(&native_dtype)?;
    let endian = codec_configuration
        .and_then(|configuration| configuration.get("endian"))
        .map(|value| {
            value.as_str().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "the Zarr v3 bytes codec endian must be a string".to_string(),
                )
            })
        })
        .transpose()?;
    if endian.is_some_and(|endian| !matches!(endian, "little" | "big")) {
        return Err(ZarrFdwError::InvalidMetadata(
            "the Zarr v3 bytes codec endian must be 'little' or 'big'".to_string(),
        ));
    }
    if multi_byte {
        let endian = endian.ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "the Zarr v3 bytes codec must declare endian for multi-byte numeric data"
                    .to_string(),
            )
        })?;
        if endian != "little" {
            return Err(ZarrFdwError::UnsupportedDataType(format!(
                "{native_dtype} with {endian}-endian bytes"
            )));
        }
    }
    let parsed_dtype = DType::parse(&dtype)?;
    fill_value_bytes(parsed_dtype, &fill_value).map_err(|error| {
        ZarrFdwError::InvalidMetadata(format!(
            "invalid Zarr v3 fill_value for {native_dtype}: {error}"
        ))
    })?;

    let dimension_names = object
        .get("dimension_names")
        .map(|value| {
            value
                .as_array()
                .ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(
                        "Zarr v3 dimension_names must be an array".to_string(),
                    )
                })?
                .iter()
                .map(|name| {
                    if name.is_null() {
                        Ok(None)
                    } else {
                        name.as_str()
                            .map(|name| Some(name.to_string()))
                            .ok_or_else(|| {
                                ZarrFdwError::InvalidMetadata(
                                    "Zarr v3 dimension_names entries must be strings or null"
                                        .to_string(),
                                )
                            })
                    }
                })
                .collect::<ZarrFdwResult<Vec<_>>>()
        })
        .transpose()?;

    let meta = ArrayMeta {
        zarr_format: 3,
        shape,
        chunks,
        dtype,
        fill_value,
        compressor: None,
        chunk_key_encoding,
        order: 'C',
        filters: None,
    };
    meta.validate()?;
    Ok(ArrayNode {
        format: ZarrFormat::V3,
        meta,
        attributes,
        dimension_names,
        native_dtype,
        native_codecs,
    })
}

fn normalize_v3_dtype(data_type: &str) -> ZarrFdwResult<(String, bool)> {
    let normalized = match data_type {
        "float32" => ("<f4", true),
        "float64" => ("<f8", true),
        "int8" => ("|i1", false),
        "int16" => ("<i2", true),
        "int32" => ("<i4", true),
        "int64" => ("<i8", true),
        other => return Err(ZarrFdwError::UnsupportedDataType(other.to_string())),
    };
    Ok((normalized.0.to_string(), normalized.1))
}

fn parse_separator(value: &str, field: &str) -> ZarrFdwResult<char> {
    match value {
        "." => Ok('.'),
        "/" => Ok('/'),
        _ => Err(ZarrFdwError::InvalidMetadata(format!(
            "{field} must be '.' or '/', got '{value}'"
        ))),
    }
}

fn validate_additional_fields(
    object: &Map<String, Value>,
    known: &[&str],
    context: &str,
) -> ZarrFdwResult<()> {
    for (field, value) in object {
        if known.contains(&field.as_str()) {
            continue;
        }
        let ignorable = value
            .as_object()
            .and_then(|extension| extension.get("must_understand"))
            == Some(&Value::Bool(false));
        if !ignorable {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "{context} contains unrecognized field '{field}' that is not marked must_understand=false"
            )));
        }
    }
    Ok(())
}

fn validate_extension_object(object: &Map<String, Value>, context: &str) -> ZarrFdwResult<()> {
    validate_exact_fields(
        object,
        &["name", "configuration", "must_understand"],
        context,
    )?;
    if object
        .get("must_understand")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "{context} must_understand must be a boolean"
        )));
    }
    Ok(())
}

fn reject_ignorable_extension(object: &Map<String, Value>, context: &str) -> ZarrFdwResult<()> {
    if object.get("must_understand") == Some(&Value::Bool(false)) {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "{context} cannot set must_understand=false"
        )));
    }
    Ok(())
}

fn validate_exact_fields(
    object: &Map<String, Value>,
    allowed: &[&str],
    context: &str,
) -> ZarrFdwResult<()> {
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "{context} contains unsupported field '{field}'"
        )));
    }
    Ok(())
}

fn required_object<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> ZarrFdwResult<&'a Map<String, Value>> {
    object.get(field).and_then(Value::as_object).ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!("Zarr metadata field '{field}' must be an object"))
    })
}

fn optional_object_ref<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> ZarrFdwResult<Option<&'a Map<String, Value>>> {
    object
        .get(field)
        .map(|value| {
            value.as_object().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "Zarr metadata field '{field}' must be an object"
                ))
            })
        })
        .transpose()
}

fn optional_object(object: &Map<String, Value>, field: &str) -> ZarrFdwResult<Map<String, Value>> {
    Ok(optional_object_ref(object, field)?
        .cloned()
        .unwrap_or_default())
}

fn required_string<'a>(object: &'a Map<String, Value>, field: &str) -> ZarrFdwResult<&'a str> {
    object.get(field).and_then(Value::as_str).ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!("Zarr metadata field '{field}' must be a string"))
    })
}

fn required_u64(object: Option<&Map<String, Value>>, field: &str) -> ZarrFdwResult<u64> {
    object
        .and_then(|object| object.get(field))
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "Zarr metadata field '{field}' must be a non-negative integer"
            ))
        })
}

fn required_u64_array(object: &Map<String, Value>, field: &str) -> ZarrFdwResult<Vec<u64>> {
    object
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!("Zarr metadata field '{field}' must be an array"))
        })?
        .iter()
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "Zarr metadata field '{field}' must contain non-negative integers"
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(shape: Vec<u64>, chunks: Vec<u64>) -> ArrayMeta {
        ArrayMeta {
            zarr_format: 2,
            shape,
            chunks,
            dtype: "<f4".to_string(),
            fill_value: Value::Null,
            compressor: None,
            chunk_key_encoding: ChunkKeyEncoding::V2 { separator: '.' },
            order: 'C',
            filters: None,
        }
    }

    #[test]
    fn parses_v2_metadata_and_separator() {
        let node = parse_v2_array(
            br#"{"zarr_format":2,"shape":[10,100],"chunks":[2,32],"dtype":"<f4","compressor":null,"fill_value":null,"dimension_separator":"/","order":"C"}"#,
            Map::new(),
        )
        .unwrap();
        assert_eq!(node.format, ZarrFormat::V2);
        assert_eq!(node.meta.chunks_per_axis(), vec![5, 4]);
        assert_eq!(
            node.meta.chunk_key_encoding,
            ChunkKeyEncoding::V2 { separator: '/' }
        );
    }

    #[test]
    fn parses_direct_v3_array_and_group() {
        let node = parse_v3_node(
            br#"{
          "zarr_format":3,"node_type":"array","shape":[2,5,6],
          "data_type":"float32",
          "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3,4]}},
          "chunk_key_encoding":{"name":"default"},"fill_value":-7.5,
          "codecs":[{"name":"bytes","configuration":{"endian":"little"}}],
          "dimension_names":["time","y","x"],"attributes":{"units":"K"}
        }"#,
        )
        .unwrap();
        let NodeMeta::Array(node) = node else {
            panic!("expected array")
        };
        assert_eq!(node.meta.dtype, "<f4");
        assert_eq!(node.native_dtype, "float32");
        assert_eq!(node.attributes["units"], "K");
        assert_eq!(
            node.meta.chunk_key_encoding,
            ChunkKeyEncoding::Default { separator: '/' }
        );
        assert_eq!(node.dimension_names.unwrap()[0].as_deref(), Some("time"));

        let NodeMeta::Group(group) = parse_v3_node(
            br#"{"zarr_format":3,"node_type":"group","attributes":{"title":"root"}}"#,
        )
        .unwrap() else {
            panic!("expected group")
        };
        assert_eq!(group.attributes["title"], "root");
    }

    #[test]
    fn normalizes_every_supported_v3_numeric_type() {
        for (data_type, configuration, expected) in [
            ("float32", serde_json::json!({"endian":"little"}), "<f4"),
            ("float64", serde_json::json!({"endian":"little"}), "<f8"),
            ("int8", serde_json::json!({}), "|i1"),
            ("int16", serde_json::json!({"endian":"little"}), "<i2"),
            ("int32", serde_json::json!({"endian":"little"}), "<i4"),
            ("int64", serde_json::json!({"endian":"little"}), "<i8"),
        ] {
            let value = serde_json::json!({
                "zarr_format": 3, "node_type": "array", "shape": [2],
                "data_type": data_type,
                "chunk_grid": {"name":"regular","configuration":{"chunk_shape":[1]}},
                "chunk_key_encoding": {"name":"v2","configuration":{"separator":"/"}},
                "fill_value": 0,
                "codecs": [{"name":"bytes","configuration":configuration}],
                "dimension_names": ["x"], "attributes": {}
            });
            let NodeMeta::Array(node) =
                parse_v3_node(&serde_json::to_vec(&value).unwrap()).unwrap()
            else {
                panic!("expected array")
            };
            assert_eq!(node.meta.dtype, expected);
            assert_eq!(node.native_dtype, data_type);
            assert_eq!(
                node.meta.chunk_key_encoding,
                ChunkKeyEncoding::V2 { separator: '/' }
            );
        }
    }

    #[test]
    fn rejects_unsupported_v3_features() {
        let base = serde_json::json!({
            "zarr_format": 3, "node_type": "array", "shape": [2],
            "data_type": "float32",
            "chunk_grid": {"name":"regular","configuration":{"chunk_shape":[1]}},
            "chunk_key_encoding": {"name":"default"}, "fill_value": 0,
            "codecs": [{"name":"bytes","configuration":{"endian":"little"}}],
            "dimension_names": ["x"], "attributes": {}
        });
        for mutate in ["grid", "pipeline", "transformer", "endian", "dtype", "fill"] {
            let mut value = base.clone();
            match mutate {
                "grid" => value["chunk_grid"]["name"] = Value::String("rectilinear".into()),
                "pipeline" => {
                    value["codecs"] = serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}, {"name":"gzip"}])
                }
                "transformer" => value["storage_transformers"] = serde_json::json!([{"name":"x"}]),
                "endian" => {
                    value["codecs"][0]["configuration"]["endian"] = Value::String("big".into())
                }
                "dtype" => value["data_type"] = Value::String("uint16".into()),
                "fill" => value["fill_value"] = Value::Null,
                _ => unreachable!(),
            }
            assert!(
                parse_v3_node(serde_json::to_vec(&value).unwrap().as_slice()).is_err(),
                "{mutate}"
            );
        }
    }

    #[test]
    fn v3_metadata_is_fail_closed_and_fill_values_are_typed() {
        let base = serde_json::json!({
            "zarr_format": 3, "node_type": "array", "shape": [2],
            "data_type": "float32",
            "chunk_grid": {"name":"regular","configuration":{"chunk_shape":[1]}},
            "chunk_key_encoding": {"name":"default"}, "fill_value": 0,
            "codecs": [{"name":"bytes","configuration":{"endian":"little"}}],
            "dimension_names": ["x"], "attributes": {}
        });
        for (label, mutate) in [
            ("unknown", serde_json::json!({"unexpected": true})),
            (
                "grid config",
                serde_json::json!({"chunk_grid":{"configuration":{"extra":1}}}),
            ),
            (
                "key config",
                serde_json::json!({"chunk_key_encoding":{"configuration":{"extra":1}}}),
            ),
            (
                "codec config",
                serde_json::json!({"codecs":[{"configuration":{"extra":1}}]}),
            ),
            ("bad float fill", serde_json::json!({"fill_value":"bogus"})),
        ] {
            let mut value = base.clone();
            merge_json(&mut value, mutate);
            assert!(
                parse_v3_node(&serde_json::to_vec(&value).unwrap()).is_err(),
                "{label}"
            );
        }

        let mut integer = base.clone();
        integer["data_type"] = serde_json::json!("int8");
        integer["codecs"] = serde_json::json!([{"name":"bytes"}]);
        integer["fill_value"] = serde_json::json!(128);
        assert!(parse_v3_node(&serde_json::to_vec(&integer).unwrap()).is_err());

        integer["fill_value"] = serde_json::json!(0);
        integer["codecs"] =
            serde_json::json!([{"name":"bytes","configuration":{"endian":"banana"}}]);
        assert!(parse_v3_node(&serde_json::to_vec(&integer).unwrap()).is_err());

        for field in ["chunk_grid", "chunk_key_encoding"] {
            let mut value = base.clone();
            value[field]["must_understand"] = serde_json::json!(false);
            assert!(parse_v3_node(&serde_json::to_vec(&value).unwrap()).is_err());
        }

        let mut ignorable = base.clone();
        ignorable["example_extension"] =
            serde_json::json!({"name":"example","must_understand":false});
        assert!(parse_v3_node(&serde_json::to_vec(&ignorable).unwrap()).is_ok());

        let consolidated = serde_json::json!({
            "zarr_format":3,
            "node_type":"group",
            "attributes":{},
            "consolidated_metadata":{"must_understand":false,"kind":"inline","metadata":{}}
        });
        assert!(parse_v3_node(&serde_json::to_vec(&consolidated).unwrap()).is_err());

        let mut array_consolidated = base.clone();
        array_consolidated["consolidated_metadata"] = consolidated["consolidated_metadata"].clone();
        assert!(parse_v3_node(&serde_json::to_vec(&array_consolidated).unwrap()).is_err());
    }

    fn merge_json(target: &mut Value, patch: Value) {
        for (key, value) in patch.as_object().unwrap() {
            if let (Some(target_object), Some(patch_object)) = (
                target.get_mut(key).and_then(Value::as_object_mut),
                value.as_object(),
            ) {
                for (nested_key, nested_value) in patch_object {
                    if let (Some(target_nested), Some(patch_nested)) = (
                        target_object
                            .get_mut(nested_key)
                            .and_then(Value::as_object_mut),
                        nested_value.as_object(),
                    ) {
                        for (leaf, leaf_value) in patch_nested {
                            target_nested.insert(leaf.clone(), leaf_value.clone());
                        }
                    } else {
                        target_object.insert(nested_key.clone(), nested_value.clone());
                    }
                }
            } else {
                target[key] = value.clone();
            }
        }
    }

    #[test]
    fn validates_rank_shape_and_chunk_arithmetic() {
        for rank in [1, 4, MAX_SCAN_RANK] {
            meta(vec![1; rank], vec![1; rank]).validate().unwrap();
        }
        for rank in [0, MAX_SCAN_RANK + 1] {
            assert!(matches!(
                meta(vec![1; rank], vec![1; rank]).validate(),
                Err(ZarrFdwError::UnsupportedRank { rank: actual }) if actual == rank
            ));
        }
        assert!(meta(vec![1, 0], vec![1, 1]).validate().is_err());
        assert!(meta(vec![1, 1], vec![1, 0]).validate().is_err());
        assert!(meta(vec![1, 1], vec![1]).validate().is_err());
        assert!(
            meta(vec![u64::MAX; 2], vec![u64::MAX; 2])
                .validate()
                .is_err()
        );
    }

    #[test]
    fn accepts_empty_v2_filters_and_rejects_non_empty() {
        let mut value = meta(vec![2, 2], vec![1, 1]);
        value.filters = Some(vec![]);
        value.validate().unwrap();
        value.filters = Some(vec![serde_json::json!({"id":"delta"})]);
        assert!(value.validate().is_err());
    }
}
