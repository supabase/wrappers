//! Format-neutral, ordered Zarr chunk codec pipelines.
//!
//! Zarr v2 compressors and the bounded direct Zarr v3 codec subset are
//! normalized into one execution contract. Pipelines always decode to the
//! executor's logical C-order primitive bytes.

use std::io::Cursor;

use serde_json::{Map, Value};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

use super::{ZarrFdwError, ZarrFdwResult};

const MAX_DECODED_CHUNK_BYTES: usize = 256 * 1024 * 1024;
const COMPRESSED_OVERHEAD_ALLOWANCE: usize = 1024 * 1024;
const BLOSC_HEADER_BYTES: usize = 16;
const CRC32C_BYTES: usize = 4;
const STREAM_POLL_BYTES: usize = 64 * 1024;
const CRC_POLL_BYTES: usize = 1024 * 1024;
const TRANSPOSE_POLL_CELLS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endian {
    Little,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CodecStage {
    Transpose { order: Vec<usize> },
    Bytes { endian: Option<Endian> },
    Gzip,
    Crc32c,
    // Zlib and Blosc remain v2-only in this ticket.
    Zlib,
    Blosc,
}

impl CodecStage {
    fn label(&self) -> &'static str {
        match self {
            Self::Transpose { .. } => "transpose",
            Self::Bytes { .. } => "bytes",
            Self::Gzip => "gzip",
            Self::Crc32c => "crc32c",
            Self::Zlib => "zlib",
            Self::Blosc => "blosc",
        }
    }
}

/// A validated codec sequence in metadata/encoding order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodecPipeline {
    stages: Vec<CodecStage>,
}

/// Interruptible codec execution result. PostgreSQL's error-raising interrupt
/// handler must only be called by the executor after `Runtime::block_on` has
/// returned and its future has been dropped.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CodecDecode {
    Decoded(Vec<u8>),
    Interrupted,
}

impl CodecPipeline {
    /// A raw v2 pipeline, useful for format-neutral metadata construction.
    pub(crate) fn raw_v2() -> Self {
        Self { stages: Vec::new() }
    }

    /// Normalize the existing Zarr v2 compressor representation without
    /// tightening its accepted compressor-specific configuration.
    pub(crate) fn from_v2(compressor: &Option<Value>) -> ZarrFdwResult<Self> {
        let Some(compressor) = compressor else {
            return Ok(Self::raw_v2());
        };
        let id = compressor
            .as_object()
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "compressor must be null or a JSON object with a string 'id'".to_string(),
                )
            })?
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "compressor object must contain a non-empty string 'id'".to_string(),
                )
            })?;
        let stage = match id {
            "gzip" => CodecStage::Gzip,
            "zlib" => CodecStage::Zlib,
            "blosc" => CodecStage::Blosc,
            other => return Err(ZarrFdwError::UnsupportedCompressor(other.to_string())),
        };
        Ok(Self {
            stages: vec![stage],
        })
    }

    /// Parse the council-locked direct Zarr v3 pipeline and return its
    /// executor-normalized NumPy dtype.
    ///
    /// Supported metadata order is exactly:
    /// `[transpose]? -> bytes -> [gzip]? -> [crc32c]?`.
    pub(crate) fn from_v3(
        native_dtype: &str,
        rank: usize,
        codecs: &Value,
    ) -> ZarrFdwResult<(Self, String)> {
        let (dtype, multi_byte) = normalize_v3_dtype(native_dtype)?;
        let codecs = codecs.as_array().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("Zarr v3 codecs must be an array".to_string())
        })?;
        if codecs.is_empty() {
            return Err(ZarrFdwError::InvalidMetadata(
                "Zarr v3 codecs must contain exactly one bytes codec".to_string(),
            ));
        }

        let mut stages = Vec::with_capacity(codecs.len());
        let mut cursor = 0usize;
        if codec_name(codecs, cursor)? == "transpose" {
            stages.push(parse_transpose(
                codec_object(codecs, cursor)?,
                rank,
                cursor,
            )?);
            cursor += 1;
        }

        if cursor >= codecs.len() || codec_name(codecs, cursor)? != "bytes" {
            return Err(pipeline_metadata_error(
                cursor.min(codecs.len().saturating_sub(1)),
                "expected exactly one bytes codec after the optional transpose codec",
            ));
        }
        stages.push(parse_bytes(
            codec_object(codecs, cursor)?,
            multi_byte,
            cursor,
        )?);
        cursor += 1;

        if cursor < codecs.len() && codec_name(codecs, cursor)? == "gzip" {
            stages.push(parse_gzip(codec_object(codecs, cursor)?, cursor)?);
            cursor += 1;
        }
        if cursor < codecs.len() && codec_name(codecs, cursor)? == "crc32c" {
            stages.push(parse_crc32c(codec_object(codecs, cursor)?, cursor)?);
            cursor += 1;
        }
        if cursor != codecs.len() {
            let name = codec_name(codecs, cursor)?;
            let reason = match name {
                "transpose" | "bytes" | "gzip" | "crc32c" => {
                    format!("codec '{name}' is duplicated or appears out of supported order")
                }
                "blosc" => "Zarr v3 Blosc is not supported yet".to_string(),
                "zstd" => "Zarr v3 Zstd is not supported yet".to_string(),
                "sharding_indexed" => "sharded Zarr v3 chunks are not supported".to_string(),
                other => format!("Zarr v3 codec '{other}' is not supported"),
            };
            return Err(pipeline_metadata_error(cursor, reason));
        }

        Ok((Self { stages }, dtype))
    }

    /// Ordered codec names for `EXPLAIN ANALYZE` and diagnostics.
    pub(crate) fn ordered_label(&self) -> String {
        if self.stages.is_empty() {
            "raw".to_string()
        } else {
            self.stages
                .iter()
                .map(CodecStage::label)
                .collect::<Vec<_>>()
                .join(" -> ")
        }
    }

    /// Maximum complete encoded object accepted for a declared decoded size.
    /// This retains the v2 bounds and composes the exact CRC suffix allowance.
    pub(crate) fn encoded_read_limit(&self, decoded_bytes: usize) -> ZarrFdwResult<usize> {
        if decoded_bytes > MAX_DECODED_CHUNK_BYTES {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "declared chunk decodes to {decoded_bytes} bytes, exceeding the safety limit of {MAX_DECODED_CHUNK_BYTES}"
            )));
        }
        self.stages.iter().try_fold(decoded_bytes, |limit, stage| {
            let allowance = match stage {
                CodecStage::Gzip | CodecStage::Zlib => COMPRESSED_OVERHEAD_ALLOWANCE,
                CodecStage::Blosc => BLOSC_HEADER_BYTES,
                CodecStage::Crc32c => CRC32C_BYTES,
                CodecStage::Transpose { .. } | CodecStage::Bytes { .. } => 0,
            };
            limit.checked_add(allowance).ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "encoded chunk read limit exceeds this platform's index capacity".to_string(),
                )
            })
        })
    }

    /// Decode a complete direct chunk in reverse metadata order. Every stage
    /// is bounded by the checked logical layout, and long loops poll the
    /// supplied non-raising cancellation callback.
    pub(crate) async fn decode_interruptible<F>(
        &self,
        encoded: Vec<u8>,
        logical_shape: &[usize],
        itemsize: usize,
        mut interrupt_pending: F,
    ) -> ZarrFdwResult<CodecDecode>
    where
        F: FnMut() -> bool,
    {
        let expected = checked_logical_bytes(logical_shape, itemsize)?;
        let encoded_limit = self.encoded_read_limit(expected)?;
        if encoded.len() > encoded_limit {
            return Err(codec_read_error(
                self.stages.len(),
                "input",
                format!(
                    "encoded chunk has {} bytes, exceeding its read limit of {encoded_limit}",
                    encoded.len()
                ),
            ));
        }
        if interrupt_pending() {
            return Ok(CodecDecode::Interrupted);
        }
        let mut data = encoded;
        for (index, stage) in self.stages.iter().enumerate().rev() {
            if interrupt_pending() {
                return Ok(CodecDecode::Interrupted);
            }
            data = match stage {
                CodecStage::Crc32c => {
                    let Some(decoded) = decode_crc32c(data, &mut interrupt_pending, index)? else {
                        return Ok(CodecDecode::Interrupted);
                    };
                    decoded
                }
                CodecStage::Gzip => {
                    let Some(decoded) = decode_stream(
                        StreamCodec::Gzip,
                        data,
                        expected,
                        &mut interrupt_pending,
                        index,
                    )
                    .await?
                    else {
                        return Ok(CodecDecode::Interrupted);
                    };
                    decoded
                }
                CodecStage::Zlib => {
                    let Some(decoded) = decode_stream(
                        StreamCodec::Zlib,
                        data,
                        expected,
                        &mut interrupt_pending,
                        index,
                    )
                    .await?
                    else {
                        return Ok(CodecDecode::Interrupted);
                    };
                    decoded
                }
                CodecStage::Blosc => {
                    let decoded = decode_blosc(data, expected, index)?;
                    if interrupt_pending() {
                        return Ok(CodecDecode::Interrupted);
                    }
                    decoded
                }
                CodecStage::Bytes { endian } => {
                    debug_assert!(endian.is_none() || *endian == Some(Endian::Little));
                    require_exact_length(data, expected, index, stage.label())?
                }
                CodecStage::Transpose { order } => {
                    let Some(decoded) = inverse_transpose(
                        data,
                        logical_shape,
                        itemsize,
                        order,
                        &mut interrupt_pending,
                        index,
                    )?
                    else {
                        return Ok(CodecDecode::Interrupted);
                    };
                    decoded
                }
            };
            if interrupt_pending() {
                return Ok(CodecDecode::Interrupted);
            }
        }
        require_exact_length(data, expected, self.stages.len(), "pipeline")
            .map(CodecDecode::Decoded)
    }
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

fn codec_object(codecs: &[Value], index: usize) -> ZarrFdwResult<&Map<String, Value>> {
    codecs
        .get(index)
        .and_then(Value::as_object)
        .ok_or_else(|| pipeline_metadata_error(index, "codec entry must be an object"))
}

fn codec_name(codecs: &[Value], index: usize) -> ZarrFdwResult<&str> {
    codec_object(codecs, index)?
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| pipeline_metadata_error(index, "codec name must be a non-empty string"))
}

fn validate_codec_object(codec: &Map<String, Value>, index: usize) -> ZarrFdwResult<()> {
    validate_fields(codec, &["name", "configuration", "must_understand"], index)?;
    if codec
        .get("must_understand")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(pipeline_metadata_error(
            index,
            "must_understand must be a boolean",
        ));
    }
    Ok(())
}

fn codec_configuration(
    codec: &Map<String, Value>,
    index: usize,
    required: bool,
) -> ZarrFdwResult<Option<&Map<String, Value>>> {
    let configuration = codec.get("configuration");
    if required && configuration.is_none() {
        return Err(pipeline_metadata_error(
            index,
            "codec configuration is required",
        ));
    }
    configuration
        .map(|value| {
            value.as_object().ok_or_else(|| {
                pipeline_metadata_error(index, "codec configuration must be an object")
            })
        })
        .transpose()
}

fn validate_fields(
    object: &Map<String, Value>,
    fields: &[&str],
    index: usize,
) -> ZarrFdwResult<()> {
    if let Some(field) = object
        .keys()
        .find(|field| !fields.contains(&field.as_str()))
    {
        return Err(pipeline_metadata_error(
            index,
            format!("codec configuration contains unsupported field '{field}'"),
        ));
    }
    Ok(())
}

fn parse_transpose(
    codec: &Map<String, Value>,
    rank: usize,
    index: usize,
) -> ZarrFdwResult<CodecStage> {
    validate_codec_object(codec, index)?;
    let configuration = codec_configuration(codec, index, true)?.expect("required above");
    validate_fields(configuration, &["order"], index)?;
    let order = configuration
        .get("order")
        .and_then(Value::as_array)
        .ok_or_else(|| pipeline_metadata_error(index, "transpose order must be an array"))?
        .iter()
        .map(|axis| {
            axis.as_u64()
                .and_then(|axis| usize::try_from(axis).ok())
                .ok_or_else(|| {
                    pipeline_metadata_error(
                        index,
                        "transpose order must contain non-negative platform-sized integers",
                    )
                })
        })
        .collect::<ZarrFdwResult<Vec<_>>>()?;
    validate_permutation(&order, rank, index)?;
    Ok(CodecStage::Transpose { order })
}

fn parse_bytes(
    codec: &Map<String, Value>,
    multi_byte: bool,
    index: usize,
) -> ZarrFdwResult<CodecStage> {
    validate_codec_object(codec, index)?;
    let configuration = codec_configuration(codec, index, multi_byte)?;
    if let Some(configuration) = configuration {
        validate_fields(configuration, &["endian"], index)?;
    }
    let endian = configuration
        .and_then(|configuration| configuration.get("endian"))
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| pipeline_metadata_error(index, "bytes endian must be a string"))
        })
        .transpose()?;
    if multi_byte {
        match endian {
            Some("little") => Ok(CodecStage::Bytes {
                endian: Some(Endian::Little),
            }),
            Some("big") => Err(pipeline_metadata_error(
                index,
                "big-endian Zarr v3 bytes are not supported yet",
            )),
            Some(other) => Err(pipeline_metadata_error(
                index,
                format!("bytes endian must be 'little', got '{other}'"),
            )),
            None => Err(pipeline_metadata_error(
                index,
                "bytes endian is required for multi-byte numeric data",
            )),
        }
    } else {
        match endian {
            None => Ok(CodecStage::Bytes { endian: None }),
            Some("little") => Ok(CodecStage::Bytes {
                endian: Some(Endian::Little),
            }),
            Some("big") => Err(pipeline_metadata_error(
                index,
                "big-endian Zarr v3 bytes are not supported yet",
            )),
            Some(other) => Err(pipeline_metadata_error(
                index,
                format!("bytes endian must be 'little', got '{other}'"),
            )),
        }
    }
}

fn parse_gzip(codec: &Map<String, Value>, index: usize) -> ZarrFdwResult<CodecStage> {
    validate_codec_object(codec, index)?;
    let configuration = codec_configuration(codec, index, true)?.expect("required above");
    validate_fields(configuration, &["level"], index)?;
    let level = configuration
        .get("level")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            pipeline_metadata_error(index, "gzip level must be an integer from 0 to 9")
        })?;
    if level > 9 {
        return Err(pipeline_metadata_error(
            index,
            "gzip level must be an integer from 0 to 9",
        ));
    }
    Ok(CodecStage::Gzip)
}

fn parse_crc32c(codec: &Map<String, Value>, index: usize) -> ZarrFdwResult<CodecStage> {
    validate_codec_object(codec, index)?;
    if let Some(configuration) = codec_configuration(codec, index, false)? {
        validate_fields(configuration, &[], index)?;
    }
    Ok(CodecStage::Crc32c)
}

fn validate_permutation(order: &[usize], rank: usize, index: usize) -> ZarrFdwResult<()> {
    if order.len() != rank {
        return Err(pipeline_metadata_error(
            index,
            format!(
                "transpose order has rank {}, expected array rank {rank}",
                order.len()
            ),
        ));
    }
    let mut seen = vec![false; rank];
    for &axis in order {
        if axis >= rank || seen[axis] {
            return Err(pipeline_metadata_error(
                index,
                format!("transpose order must be a permutation of 0..{rank}"),
            ));
        }
        seen[axis] = true;
    }
    Ok(())
}

fn pipeline_metadata_error(index: usize, reason: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(format!("Zarr v3 codec index {index}: {}", reason.into()))
}

fn codec_read_error(index: usize, name: &str, reason: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::ReadError(std::io::Error::other(format!(
        "codec index {index} ('{name}'): {}",
        reason.into()
    )))
}

fn checked_logical_bytes(shape: &[usize], itemsize: usize) -> ZarrFdwResult<usize> {
    if shape.is_empty() || shape.contains(&0) || itemsize == 0 {
        return Err(ZarrFdwError::InvalidMetadata(
            "codec chunk shape and item size must be positive".to_string(),
        ));
    }
    let cells = shape.iter().try_fold(1usize, |cells, &extent| {
        cells.checked_mul(extent).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "declared chunk cell count exceeds this platform's index capacity".to_string(),
            )
        })
    })?;
    let bytes = cells.checked_mul(itemsize).ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(
            "declared chunk byte length exceeds this platform's index capacity".to_string(),
        )
    })?;
    if bytes > MAX_DECODED_CHUNK_BYTES {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "declared chunk decodes to {bytes} bytes, exceeding the safety limit of {MAX_DECODED_CHUNK_BYTES}"
        )));
    }
    Ok(bytes)
}

fn require_exact_length(
    data: Vec<u8>,
    expected: usize,
    index: usize,
    name: &str,
) -> ZarrFdwResult<Vec<u8>> {
    if data.len() != expected {
        return Err(codec_read_error(
            index,
            name,
            format!(
                "decoded chunk has {} bytes, expected exactly {expected}",
                data.len()
            ),
        ));
    }
    Ok(data)
}

#[derive(Debug, Clone, Copy)]
enum StreamCodec {
    Gzip,
    Zlib,
}

impl StreamCodec {
    fn label(&self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Zlib => "zlib",
        }
    }
}

async fn decode_stream<F>(
    codec: StreamCodec,
    data: Vec<u8>,
    expected: usize,
    interrupt_pending: &mut F,
    index: usize,
) -> ZarrFdwResult<Option<Vec<u8>>>
where
    F: FnMut() -> bool,
{
    use async_compression::tokio::bufread::{GzipDecoder, ZlibDecoder};

    match codec {
        StreamCodec::Gzip => {
            let mut decoder = BufReader::new(GzipDecoder::new(BufReader::new(Cursor::new(data))));
            read_exact_bounded(
                &mut decoder,
                expected,
                interrupt_pending,
                index,
                codec.label(),
            )
            .await
        }
        StreamCodec::Zlib => {
            let mut decoder = BufReader::new(ZlibDecoder::new(BufReader::new(Cursor::new(data))));
            read_exact_bounded(
                &mut decoder,
                expected,
                interrupt_pending,
                index,
                codec.label(),
            )
            .await
        }
    }
}

async fn read_exact_bounded<R, F>(
    reader: &mut R,
    expected: usize,
    interrupt_pending: &mut F,
    index: usize,
    label: &str,
) -> ZarrFdwResult<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
    F: FnMut() -> bool,
{
    let mut decoded = Vec::new();
    decoded.try_reserve_exact(expected).map_err(|_| {
        ZarrFdwError::InvalidMetadata(format!(
            "could not allocate a decoded chunk of {expected} bytes"
        ))
    })?;
    let mut buffer = vec![0u8; STREAM_POLL_BYTES.min(expected.max(1))];
    while decoded.len() < expected {
        if interrupt_pending() {
            return Ok(None);
        }
        let remaining = expected - decoded.len();
        let read_len = remaining.min(buffer.len());
        let count = reader
            .read(&mut buffer[..read_len])
            .await
            .map_err(|error| {
                codec_read_error(index, label, format!("failed to decode stream: {error}"))
            })?;
        if count == 0 {
            return Err(codec_read_error(
                index,
                label,
                format!(
                    "decoded chunk has {} bytes, expected exactly {expected}",
                    decoded.len()
                ),
            ));
        }
        decoded.extend_from_slice(&buffer[..count]);
    }
    if interrupt_pending() {
        return Ok(None);
    }
    let mut extra = [0u8; 1];
    let extra_count = reader.read(&mut extra).await.map_err(|error| {
        codec_read_error(index, label, format!("failed to finish stream: {error}"))
    })?;
    if extra_count != 0 {
        return Err(codec_read_error(
            index,
            label,
            format!("decoded chunk has more than {expected} bytes, expected exactly {expected}"),
        ));
    }
    Ok(Some(decoded))
}

fn decode_blosc(data: Vec<u8>, expected: usize, index: usize) -> ZarrFdwResult<Vec<u8>> {
    let decoder = blosc_rs::Decoder::new(data).map_err(|error| {
        codec_read_error(index, "blosc", format!("invalid Blosc chunk: {error}"))
    })?;
    if decoder.nbytes() != expected {
        return Err(codec_read_error(
            index,
            "blosc",
            format!(
                "decoded chunk has {} bytes, expected exactly {expected}",
                decoder.nbytes()
            ),
        ));
    }
    let decoded = decoder.decompress(1).map_err(|error| {
        codec_read_error(
            index,
            "blosc",
            format!("failed to decompress Blosc chunk: {error}"),
        )
    })?;
    require_exact_length(decoded, expected, index, "blosc")
}

fn decode_crc32c<F>(
    mut data: Vec<u8>,
    interrupt_pending: &mut F,
    index: usize,
) -> ZarrFdwResult<Option<Vec<u8>>>
where
    F: FnMut() -> bool,
{
    if data.len() < CRC32C_BYTES {
        return Err(codec_read_error(
            index,
            "crc32c",
            "encoded chunk is truncated before the four-byte checksum",
        ));
    }
    let payload_len = data.len() - CRC32C_BYTES;
    let expected = u32::from_le_bytes(data[payload_len..].try_into().expect("four bytes"));
    let mut actual = 0u32;
    for chunk in data[..payload_len].chunks(CRC_POLL_BYTES) {
        if interrupt_pending() {
            return Ok(None);
        }
        actual = crc32c::crc32c_append(actual, chunk);
    }
    if actual != expected {
        return Err(codec_read_error(
            index,
            "crc32c",
            format!("checksum mismatch: expected {expected:#010x}, computed {actual:#010x}"),
        ));
    }
    data.truncate(payload_len);
    Ok(Some(data))
}

fn checked_strides(shape: &[usize]) -> ZarrFdwResult<Vec<usize>> {
    let mut strides = vec![0usize; shape.len()];
    let mut stride = 1usize;
    for axis in (0..shape.len()).rev() {
        strides[axis] = stride;
        stride = stride.checked_mul(shape[axis]).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "transpose stride exceeds this platform's index capacity".to_string(),
            )
        })?;
    }
    Ok(strides)
}

fn inverse_transpose<F>(
    data: Vec<u8>,
    logical_shape: &[usize],
    itemsize: usize,
    order: &[usize],
    interrupt_pending: &mut F,
    index: usize,
) -> ZarrFdwResult<Option<Vec<u8>>>
where
    F: FnMut() -> bool,
{
    validate_permutation(order, logical_shape.len(), index)?;
    let expected = checked_logical_bytes(logical_shape, itemsize)?;
    let data = require_exact_length(data, expected, index, "transpose")?;
    if order.iter().copied().eq(0..order.len()) {
        return Ok(Some(data));
    }

    let logical_strides = checked_strides(logical_shape)?;
    let encoded_shape = order
        .iter()
        .map(|&axis| logical_shape[axis])
        .collect::<Vec<_>>();
    let encoded_strides = checked_strides(&encoded_shape)?;
    let cells = expected / itemsize;
    let mut logical = Vec::new();
    logical.try_reserve_exact(expected).map_err(|_| {
        codec_read_error(
            index,
            "transpose",
            format!("could not allocate a transposed chunk of {expected} bytes"),
        )
    })?;
    logical.resize(expected, 0);
    for logical_flat in 0..cells {
        if logical_flat % TRANSPOSE_POLL_CELLS == 0 && interrupt_pending() {
            return Ok(None);
        }
        let encoded_flat =
            order
                .iter()
                .enumerate()
                .try_fold(0usize, |flat, (encoded_axis, &logical_axis)| {
                    let coordinate = (logical_flat / logical_strides[logical_axis])
                        % logical_shape[logical_axis];
                    coordinate
                        .checked_mul(encoded_strides[encoded_axis])
                        .and_then(|offset| flat.checked_add(offset))
                        .ok_or_else(|| {
                            codec_read_error(
                                index,
                                "transpose",
                                "transpose element offset exceeds this platform's index capacity",
                            )
                        })
                })?;
        let logical_byte = logical_flat
            .checked_mul(itemsize)
            .ok_or_else(|| codec_read_error(index, "transpose", "logical byte offset overflow"))?;
        let encoded_byte = encoded_flat
            .checked_mul(itemsize)
            .ok_or_else(|| codec_read_error(index, "transpose", "encoded byte offset overflow"))?;
        let encoded_end = encoded_byte
            .checked_add(itemsize)
            .ok_or_else(|| codec_read_error(index, "transpose", "encoded byte range overflow"))?;
        let logical_end = logical_byte
            .checked_add(itemsize)
            .ok_or_else(|| codec_read_error(index, "transpose", "logical byte range overflow"))?;
        logical[logical_byte..logical_end].copy_from_slice(&data[encoded_byte..encoded_end]);
    }
    Ok(Some(logical))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v3(codecs: Value) -> ZarrFdwResult<(CodecPipeline, String)> {
        CodecPipeline::from_v3("float32", 3, &codecs)
    }

    #[test]
    fn parses_locked_v3_pipeline_and_label() {
        let codecs = serde_json::json!([
            {"name":"transpose","configuration":{"order":[2,1,0]}},
            {"name":"bytes","configuration":{"endian":"little"}},
            {"name":"gzip","configuration":{"level":1}},
            {"name":"crc32c"}
        ]);
        let (pipeline, dtype) = v3(codecs).unwrap();
        assert_eq!(dtype, "<f4");
        assert_eq!(
            pipeline.ordered_label(),
            "transpose -> bytes -> gzip -> crc32c"
        );
        assert_eq!(
            pipeline.encoded_read_limit(1024).unwrap(),
            1024 + COMPRESSED_OVERHEAD_ALLOWANCE + CRC32C_BYTES
        );
    }

    #[test]
    fn normalizes_v2_without_changing_legacy_coverage() {
        for (compressor, label, allowance) in [
            (None, "raw", 0),
            (
                Some(serde_json::json!({"id":"gzip","level":1})),
                "gzip",
                COMPRESSED_OVERHEAD_ALLOWANCE,
            ),
            (
                Some(serde_json::json!({"id":"zlib","level":1})),
                "zlib",
                COMPRESSED_OVERHEAD_ALLOWANCE,
            ),
            (
                Some(serde_json::json!({"id":"blosc","cname":"lz4"})),
                "blosc",
                BLOSC_HEADER_BYTES,
            ),
        ] {
            let pipeline = CodecPipeline::from_v2(&compressor).unwrap();
            assert_eq!(pipeline.ordered_label(), label);
            assert_eq!(pipeline.encoded_read_limit(32).unwrap(), 32 + allowance);
        }
    }

    #[test]
    fn rejects_every_unsupported_v3_order_and_codec() {
        let cases = [
            serde_json::json!([{"name":"gzip","configuration":{"level":1}}, {"name":"bytes","configuration":{"endian":"little"}}]),
            serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}, {"name":"transpose","configuration":{"order":[0,1,2]}}]),
            serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}, {"name":"crc32c"}, {"name":"gzip","configuration":{"level":1}}]),
            serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}, {"name":"bytes","configuration":{"endian":"little"}}]),
            serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}, {"name":"blosc","configuration":{}}]),
            serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}, {"name":"zstd","configuration":{"level":1}}]),
            serde_json::json!([{"name":"sharding_indexed","configuration":{}}]),
        ];
        for codecs in cases {
            assert!(v3(codecs).is_err());
        }
    }

    #[test]
    fn validates_codec_configurations_and_endian() {
        for codecs in [
            serde_json::json!([{"name":"transpose","configuration":{"order":[0,0,2]}}, {"name":"bytes","configuration":{"endian":"little"}}]),
            serde_json::json!([{"name":"transpose","configuration":{"order":[0,1]}}, {"name":"bytes","configuration":{"endian":"little"}}]),
            serde_json::json!([{"name":"bytes"}]),
            serde_json::json!([{"name":"bytes","configuration":{"endian":"big"}}]),
            serde_json::json!([{"name":"bytes","configuration":{"endian":"little","extra":1}}]),
            serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}, {"name":"gzip","configuration":{"level":10}}]),
            serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}, {"name":"crc32c","configuration":{"seed":0}}]),
        ] {
            assert!(v3(codecs).is_err());
        }

        let (_, dtype) = CodecPipeline::from_v3(
            "int8",
            1,
            &serde_json::json!([{"name":"bytes","configuration":{}}]),
        )
        .unwrap();
        assert_eq!(dtype, "|i1");
        let (_, dtype) = CodecPipeline::from_v3(
            "int8",
            1,
            &serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}]),
        )
        .unwrap();
        assert_eq!(dtype, "|i1");

        let (pipeline, _) = CodecPipeline::from_v3(
            "float32",
            3,
            &serde_json::json!([
                {"name":"transpose","configuration":{"order":[0,1,2]},"must_understand":false},
                {"name":"bytes","configuration":{"endian":"little"},"must_understand":false},
                {"name":"gzip","configuration":{"level":1},"must_understand":false},
                {"name":"crc32c","must_understand":false}
            ]),
        )
        .unwrap();
        assert_eq!(
            pipeline.ordered_label(),
            "transpose -> bytes -> gzip -> crc32c"
        );
    }

    #[test]
    fn checksum_rejects_truncation_and_corruption() {
        let payload = b"payload".to_vec();
        let mut encoded = payload.clone();
        encoded.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        assert_eq!(
            decode_crc32c(encoded, &mut || false, 2).unwrap(),
            Some(payload)
        );
        assert!(decode_crc32c(vec![1, 2, 3], &mut || false, 2).is_err());

        let mut corrupt = b"payload".to_vec();
        corrupt.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_crc32c(corrupt, &mut || false, 2).is_err());
    }

    #[test]
    fn inverse_transpose_restores_logical_c_order() {
        // Logical A shape [2, 2, 3], values 0..12. Encoding with order
        // [2, 1, 0] produces B[x, y, z] in C order.
        let logical = (0u8..12).collect::<Vec<_>>();
        let encoded = vec![0, 6, 3, 9, 1, 7, 4, 10, 2, 8, 5, 11];
        assert_eq!(
            inverse_transpose(encoded, &[2, 2, 3], 1, &[2, 1, 0], &mut || false, 0,).unwrap(),
            Some(logical)
        );
    }

    #[test]
    fn pipeline_decodes_crc_then_gzip_then_transpose() {
        use async_compression::tokio::write::GzipEncoder;
        use tokio::io::AsyncWriteExt;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pipeline = v3(serde_json::json!([
                {"name":"transpose","configuration":{"order":[2,1,0]}},
                {"name":"bytes","configuration":{"endian":"little"}},
                {"name":"gzip","configuration":{"level":1}},
                {"name":"crc32c"}
            ]))
            .unwrap()
            .0;
            let encoded_order = vec![0, 6, 3, 9, 1, 7, 4, 10, 2, 8, 5, 11];
            let mut encoder = GzipEncoder::new(Vec::new());
            encoder.write_all(&encoded_order).await.unwrap();
            encoder.shutdown().await.unwrap();
            let mut encoded = encoder.into_inner();
            let checksum = crc32c::crc32c(&encoded);
            encoded.extend_from_slice(&checksum.to_le_bytes());
            assert_eq!(
                pipeline
                    .decode_interruptible(encoded, &[2, 2, 3], 1, || false)
                    .await
                    .unwrap(),
                CodecDecode::Decoded((0u8..12).collect())
            );
        });
    }

    #[test]
    fn v2_raw_gzip_zlib_and_blosc_decode_through_one_interface() {
        use async_compression::tokio::write::{GzipEncoder, ZlibEncoder};
        use blosc_rs::{CompressAlgo, Encoder};
        use tokio::io::AsyncWriteExt;

        let raw = (0..64u64).flat_map(u64::to_le_bytes).collect::<Vec<_>>();
        let blosc = Encoder::default()
            .compressor(CompressAlgo::Lz4)
            .typesize(8.try_into().unwrap())
            .compress(&raw)
            .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut gzip = GzipEncoder::new(Vec::new());
            gzip.write_all(&raw).await.unwrap();
            gzip.shutdown().await.unwrap();
            let mut zlib = ZlibEncoder::new(Vec::new());
            zlib.write_all(&raw).await.unwrap();
            zlib.shutdown().await.unwrap();
            for (compressor, encoded) in [
                (None, raw.clone()),
                (
                    Some(serde_json::json!({"id":"gzip","level":1})),
                    gzip.into_inner(),
                ),
                (
                    Some(serde_json::json!({"id":"zlib","level":1})),
                    zlib.into_inner(),
                ),
                (Some(serde_json::json!({"id":"blosc","cname":"lz4"})), blosc),
            ] {
                let pipeline = CodecPipeline::from_v2(&compressor).unwrap();
                assert_eq!(
                    pipeline
                        .decode_interruptible(encoded, &[raw.len()], 1, || false)
                        .await
                        .unwrap(),
                    CodecDecode::Decoded(raw.clone())
                );
            }
        });
    }

    #[test]
    fn decoding_is_exact_bounded_and_interruptible() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let raw = CodecPipeline::raw_v2();
            assert!(
                raw.decode_interruptible(vec![1, 2, 3], &[2], 1, || false)
                    .await
                    .is_err()
            );
            assert_eq!(
                raw.decode_interruptible(vec![1, 2], &[2], 1, || true)
                    .await
                    .unwrap(),
                CodecDecode::Interrupted
            );

            let transposed = v3(serde_json::json!([
                {"name":"transpose","configuration":{"order":[2,1,0]}},
                {"name":"bytes","configuration":{"endian":"little"}}
            ]))
            .unwrap()
            .0;
            let mut polls = 0;
            assert_eq!(
                transposed
                    .decode_interruptible(vec![0; 12], &[2, 2, 3], 1, || {
                        polls += 1;
                        polls > 2
                    })
                    .await
                    .unwrap(),
                CodecDecode::Interrupted
            );
        });
        assert!(
            CodecPipeline::raw_v2()
                .encoded_read_limit(MAX_DECODED_CHUNK_BYTES + 1)
                .is_err()
        );
    }
}
