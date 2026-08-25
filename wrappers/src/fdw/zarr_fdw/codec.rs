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
const ZSTD_WINDOW_LOG_MAX: u32 = 23;
const ZSTD_WINDOW_BYTES: u64 = 1 << ZSTD_WINDOW_LOG_MAX;
const ZSTD_FRAME_MAGIC: u32 = 0xfd2f_b528;
const ZSTD_SKIPPABLE_MAGIC_START: u32 = 0x184d_2a50;
const ZSTD_SKIPPABLE_MAGIC_MASK: u32 = 0xffff_fff0;

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
    // Zlib remains v2-only. A present Blosc configuration identifies the
    // strictly validated v3 codec; v2 deliberately retains its permissive
    // compressor-metadata behavior through `None`.
    Zlib,
    Blosc { config: Option<BloscConfig> },
    Zstd { config: ZstdConfig },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BloscCname {
    Blosclz,
    Lz4,
    Lz4hc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BloscShuffle {
    None,
    Byte,
    Bit,
}

/// The complete, validated Zarr v3 Blosc encoding configuration. These
/// parameters are retained even though c-blosc's self-describing header owns
/// decompression; retaining them keeps metadata normalization lossless and
/// permits safe header consistency checks where the binary format represents
/// the corresponding setting.
#[allow(dead_code)] // Encoding parameters are retained intentionally; decoding is self-describing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BloscConfig {
    cname: BloscCname,
    clevel: u8,
    shuffle: BloscShuffle,
    typesize: Option<usize>,
    blocksize: usize,
}

/// The complete Zarr v3 Zstd encoding configuration. Compression level is
/// retained for lossless metadata normalization even though it does not
/// affect decoding.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ZstdConfig {
    level: i32,
    checksum: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ZstdFrameHeader {
    checksum: bool,
    content_size: Option<u64>,
}

impl CodecStage {
    fn label(&self) -> &'static str {
        match self {
            Self::Transpose { .. } => "transpose",
            Self::Bytes { .. } => "bytes",
            Self::Gzip => "gzip",
            Self::Crc32c => "crc32c",
            Self::Zlib => "zlib",
            Self::Blosc { .. } => "blosc",
            Self::Zstd { .. } => "zstd",
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
            "blosc" => CodecStage::Blosc { config: None },
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
    /// `[transpose]? -> bytes -> [gzip | blosc | zstd]? -> [crc32c]?`.
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

        if cursor < codecs.len() {
            let stage = match codec_name(codecs, cursor)? {
                "gzip" => Some(parse_gzip(codec_object(codecs, cursor)?, cursor)?),
                "blosc" => Some(parse_blosc(codec_object(codecs, cursor)?, cursor)?),
                "zstd" => Some(parse_zstd(codec_object(codecs, cursor)?, cursor)?),
                _ => None,
            };
            if let Some(stage) = stage {
                stages.push(stage);
                cursor += 1;
            }
        }
        if cursor < codecs.len() && codec_name(codecs, cursor)? == "crc32c" {
            stages.push(parse_crc32c(codec_object(codecs, cursor)?, cursor)?);
            cursor += 1;
        }
        if cursor != codecs.len() {
            let codec = codec_object(codecs, cursor)?;
            validate_codec_object(codec, cursor)?;
            let name = codec_name(codecs, cursor)?;
            match name {
                "transpose" | "bytes" | "gzip" | "blosc" | "zstd" | "crc32c" => {
                    return Err(pipeline_metadata_error(
                        cursor,
                        format!("codec '{name}' is duplicated or appears out of supported order"),
                    ));
                }
                "sharding_indexed" => {
                    return Err(unsupported_pipeline_feature(
                        cursor,
                        "sharded Zarr v3 chunks are not supported",
                    ));
                }
                other => {
                    return Err(unsupported_pipeline_feature(
                        cursor,
                        format!("Zarr v3 codec '{other}' is not supported"),
                    ));
                }
            }
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
                CodecStage::Gzip | CodecStage::Zlib | CodecStage::Zstd { .. } => {
                    COMPRESSED_OVERHEAD_ALLOWANCE
                }
                CodecStage::Blosc { .. } => BLOSC_HEADER_BYTES,
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
                CodecStage::Blosc { config } => {
                    let decoded = decode_blosc(data, expected, index, config.as_ref())?;
                    if interrupt_pending() {
                        return Ok(CodecDecode::Interrupted);
                    }
                    decoded
                }
                CodecStage::Zstd { config } => {
                    let Some(decoded) =
                        decode_zstd(data, expected, config, &mut interrupt_pending, index)?
                    else {
                        return Ok(CodecDecode::Interrupted);
                    };
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
            Some("big") => Err(unsupported_pipeline_feature(
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
            Some("big") => Err(unsupported_pipeline_feature(
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

fn parse_zstd(codec: &Map<String, Value>, index: usize) -> ZarrFdwResult<CodecStage> {
    validate_codec_object(codec, index)?;
    let configuration = codec_configuration(codec, index, true)?.expect("required above");
    validate_fields(configuration, &["level", "checksum"], index)?;

    let level = configuration
        .get("level")
        .and_then(Value::as_i64)
        .filter(|&level| (-131_072..=22).contains(&level))
        .and_then(|level| i32::try_from(level).ok())
        .ok_or_else(|| {
            pipeline_metadata_error(index, "Zstd level must be an integer from -131072 to 22")
        })?;
    let checksum = configuration
        .get("checksum")
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| pipeline_metadata_error(index, "Zstd checksum must be a boolean"))
        })
        .transpose()?
        .unwrap_or(false);

    Ok(CodecStage::Zstd {
        config: ZstdConfig { level, checksum },
    })
}

fn parse_blosc(codec: &Map<String, Value>, index: usize) -> ZarrFdwResult<CodecStage> {
    validate_codec_object(codec, index)?;
    let configuration = codec_configuration(codec, index, true)?.expect("required above");
    validate_fields(
        configuration,
        &["cname", "clevel", "shuffle", "typesize", "blocksize"],
        index,
    )?;

    let cname = configuration
        .get("cname")
        .and_then(Value::as_str)
        .ok_or_else(|| pipeline_metadata_error(index, "Blosc cname must be a string"))?;
    let (cname, unavailable_cname) = match cname {
        "blosclz" => (Some(BloscCname::Blosclz), None),
        "lz4" => (Some(BloscCname::Lz4), None),
        "lz4hc" => (Some(BloscCname::Lz4hc), None),
        "zstd" | "snappy" | "zlib" => (None, Some(cname)),
        other => {
            return Err(pipeline_metadata_error(
                index,
                format!("Blosc cname '{other}' is not defined by the Zarr v3 Blosc codec"),
            ));
        }
    };

    let clevel = configuration
        .get("clevel")
        .and_then(Value::as_u64)
        .filter(|&level| level <= 9)
        .ok_or_else(|| {
            pipeline_metadata_error(index, "Blosc clevel must be an integer from 0 to 9")
        })? as u8;

    let shuffle = configuration
        .get("shuffle")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            pipeline_metadata_error(
                index,
                "Blosc shuffle must be 'noshuffle', 'shuffle', or 'bitshuffle'",
            )
        })?;
    let shuffle = match shuffle {
        "noshuffle" => BloscShuffle::None,
        "shuffle" => BloscShuffle::Byte,
        "bitshuffle" => BloscShuffle::Bit,
        _ => {
            return Err(pipeline_metadata_error(
                index,
                "Blosc shuffle must be 'noshuffle', 'shuffle', or 'bitshuffle'",
            ));
        }
    };

    let typesize = configuration
        .get("typesize")
        .map(|value| {
            value
                .as_u64()
                .filter(|&value| value > 0)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| {
                    pipeline_metadata_error(
                        index,
                        "Blosc typesize must be a positive platform-sized integer",
                    )
                })
        })
        .transpose()?;
    if shuffle != BloscShuffle::None && typesize.is_none() {
        return Err(pipeline_metadata_error(
            index,
            "Blosc typesize is required when shuffle is 'shuffle' or 'bitshuffle'",
        ));
    }

    let blocksize = configuration
        .get("blocksize")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            pipeline_metadata_error(
                index,
                "Blosc blocksize must be a non-negative platform-sized integer",
            )
        })?;

    if let Some(cname) = unavailable_cname {
        return Err(unsupported_pipeline_feature(
            index,
            format!("Blosc cname '{cname}' is not enabled in this build"),
        ));
    }
    let cname = cname.expect("supported Blosc cname resolved above");

    Ok(CodecStage::Blosc {
        config: Some(BloscConfig {
            cname,
            clevel,
            shuffle,
            typesize,
            blocksize,
        }),
    })
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

fn unsupported_pipeline_feature(index: usize, reason: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::UnsupportedExecutionFeature(format!(
        "Zarr v3 codec index {index}: {}",
        reason.into()
    ))
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

fn parse_zstd_frame_header(data: &[u8], index: usize) -> ZarrFdwResult<ZstdFrameHeader> {
    let magic_bytes: [u8; 4] = data
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| zstd_header_error(index, "object is shorter than the 4-byte magic"))?;
    let magic = u32::from_le_bytes(magic_bytes);
    if magic & ZSTD_SKIPPABLE_MAGIC_MASK == ZSTD_SKIPPABLE_MAGIC_START {
        return Err(codec_read_error(
            index,
            "zstd",
            "skippable Zstandard frames are not supported",
        ));
    }
    if magic != ZSTD_FRAME_MAGIC {
        return Err(zstd_header_error(
            index,
            format!("unexpected magic 0x{magic:08x}"),
        ));
    }

    let descriptor = *data
        .get(4)
        .ok_or_else(|| zstd_header_error(index, "frame descriptor is missing"))?;
    if descriptor & 0b0000_1000 != 0 {
        return Err(zstd_header_error(index, "reserved descriptor bit is set"));
    }
    if descriptor & 0b0000_0011 != 0 {
        return Err(codec_read_error(
            index,
            "zstd",
            "Zstandard dictionaries are not supported",
        ));
    }

    let single_segment = descriptor & 0b0010_0000 != 0;
    let checksum = descriptor & 0b0000_0100 != 0;
    let mut cursor = 5usize;
    let advertised_window = if single_segment {
        None
    } else {
        let window_descriptor = *data
            .get(cursor)
            .ok_or_else(|| zstd_header_error(index, "window descriptor is missing"))?;
        cursor += 1;
        let exponent = u32::from(window_descriptor >> 3);
        let mantissa = u64::from(window_descriptor & 0b0000_0111);
        let window_log = 10u32
            .checked_add(exponent)
            .ok_or_else(|| zstd_header_error(index, "window descriptor exponent overflows"))?;
        let window_base = 1u64.checked_shl(window_log).ok_or_else(|| {
            zstd_header_error(
                index,
                "window descriptor exceeds the supported integer range",
            )
        })?;
        let window_add = (window_base / 8)
            .checked_mul(mantissa)
            .ok_or_else(|| zstd_header_error(index, "window descriptor mantissa overflows"))?;
        Some(
            window_base
                .checked_add(window_add)
                .ok_or_else(|| zstd_header_error(index, "advertised window size overflows"))?,
        )
    };

    let content_size_flag = descriptor >> 6;
    let content_size_bytes = match content_size_flag {
        0 if single_segment => 1usize,
        0 => 0usize,
        1 => 2usize,
        2 => 4usize,
        3 => 8usize,
        _ => unreachable!("two-bit field"),
    };
    let content_size = if content_size_bytes == 0 {
        None
    } else {
        let end = cursor
            .checked_add(content_size_bytes)
            .ok_or_else(|| zstd_header_error(index, "frame content-size field offset overflows"))?;
        let bytes = data
            .get(cursor..end)
            .ok_or_else(|| zstd_header_error(index, "frame content-size field is truncated"))?;
        let mut value_bytes = [0u8; 8];
        value_bytes[..content_size_bytes].copy_from_slice(bytes);
        let value = u64::from_le_bytes(value_bytes);
        Some(if content_size_bytes == 2 {
            value
                .checked_add(256)
                .ok_or_else(|| zstd_header_error(index, "frame content size overflows"))?
        } else {
            value
        })
    };

    let window_size = if single_segment {
        content_size
            .ok_or_else(|| zstd_header_error(index, "single-segment frame has no content size"))?
    } else {
        advertised_window.expect("non-single-segment frame parsed a window descriptor")
    };
    if window_size > ZSTD_WINDOW_BYTES {
        return Err(codec_read_error(
            index,
            "zstd",
            format!(
                "Zstandard frame window {window_size} exceeds the {ZSTD_WINDOW_BYTES}-byte limit"
            ),
        ));
    }

    Ok(ZstdFrameHeader {
        checksum,
        content_size,
    })
}

fn zstd_header_error(index: usize, reason: impl Into<String>) -> ZarrFdwError {
    codec_read_error(
        index,
        "zstd",
        format!("invalid Zstandard frame header: {}", reason.into()),
    )
}

fn zstd_native_error(index: usize, operation: &str, code: usize) -> ZarrFdwError {
    codec_read_error(
        index,
        "zstd",
        format!("{operation}: {}", zstd::zstd_safe::get_error_name(code)),
    )
}

fn decode_zstd<F>(
    data: Vec<u8>,
    expected: usize,
    config: &ZstdConfig,
    interrupt_pending: &mut F,
    index: usize,
) -> ZarrFdwResult<Option<Vec<u8>>>
where
    F: FnMut() -> bool,
{
    if interrupt_pending() {
        return Ok(None);
    }
    let header = parse_zstd_frame_header(&data, index)?;
    if header.checksum != config.checksum {
        return Err(codec_read_error(
            index,
            "zstd",
            format!(
                "Zstandard checksum metadata does not match the frame checksum flag (metadata={}, frame={})",
                config.checksum, header.checksum
            ),
        ));
    }
    if let Some(content_size) = header.content_size {
        let expected = u64::try_from(expected).map_err(|_| {
            codec_read_error(
                index,
                "zstd",
                "logical decoded size exceeds the supported integer range",
            )
        })?;
        if content_size != expected {
            return Err(codec_read_error(
                index,
                "zstd",
                format!(
                    "Zstandard frame content size {content_size} does not match expected {expected}"
                ),
            ));
        }
    }

    if interrupt_pending() {
        return Ok(None);
    }
    let frame_size = zstd::zstd_safe::find_frame_compressed_size(&data)
        .map_err(|code| zstd_native_error(index, "invalid Zstandard frame", code))?;
    if frame_size != data.len() {
        return Err(codec_read_error(
            index,
            "zstd",
            format!(
                "concatenated Zstandard frames and trailing bytes are not supported (first frame has {frame_size} bytes, object has {})",
                data.len()
            ),
        ));
    }

    if interrupt_pending() {
        return Ok(None);
    }
    let mut context = zstd::zstd_safe::DCtx::try_create().ok_or_else(|| {
        codec_read_error(
            index,
            "zstd",
            "could not allocate a Zstandard decoder context",
        )
    })?;
    context
        .init()
        .map_err(|code| zstd_native_error(index, "failed to initialize Zstandard decoder", code))?;
    context
        .set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(
            ZSTD_WINDOW_LOG_MAX,
        ))
        .map_err(|code| {
            zstd_native_error(index, "failed to set the Zstandard window limit", code)
        })?;
    let mut decoder =
        zstd::stream::read::Decoder::with_context(Cursor::new(data), &mut context).single_frame();
    read_exact_bounded_sync(&mut decoder, expected, interrupt_pending, index)
}

fn read_exact_bounded_sync<R, F>(
    reader: &mut R,
    expected: usize,
    interrupt_pending: &mut F,
    index: usize,
) -> ZarrFdwResult<Option<Vec<u8>>>
where
    R: std::io::Read,
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
        let count = std::io::Read::read(reader, &mut buffer[..read_len]).map_err(|error| {
            codec_read_error(
                index,
                "zstd",
                format!("failed to decode Zstandard frame: {error}"),
            )
        })?;
        if count == 0 {
            return Err(codec_read_error(
                index,
                "zstd",
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
    let extra_count = std::io::Read::read(reader, &mut extra).map_err(|error| {
        codec_read_error(
            index,
            "zstd",
            format!("failed to decode Zstandard frame: {error}"),
        )
    })?;
    if extra_count != 0 {
        return Err(codec_read_error(
            index,
            "zstd",
            format!("decoded chunk has more than {expected} bytes, expected exactly {expected}"),
        ));
    }
    Ok(Some(decoded))
}

fn decode_blosc(
    data: Vec<u8>,
    expected: usize,
    index: usize,
    v3_config: Option<&BloscConfig>,
) -> ZarrFdwResult<Vec<u8>> {
    if v3_config.is_some() {
        validate_v3_blosc_header(&data, expected, index)?;
    }
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

fn validate_v3_blosc_header(data: &[u8], expected: usize, index: usize) -> ZarrFdwResult<()> {
    if data.len() < BLOSC_HEADER_BYTES {
        return Err(codec_read_error(
            index,
            "blosc",
            "encoded chunk is shorter than the 16-byte Blosc header",
        ));
    }
    let declared_nbytes = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .expect("validated 16-byte Blosc header"),
    ) as usize;
    if declared_nbytes != expected {
        return Err(codec_read_error(
            index,
            "blosc",
            format!(
                "Blosc header declares {declared_nbytes} uncompressed bytes, expected exactly {expected}"
            ),
        ));
    }
    let declared_cbytes = u32::from_le_bytes(
        data[12..16]
            .try_into()
            .expect("validated 16-byte Blosc header"),
    ) as usize;
    if declared_cbytes != data.len() {
        return Err(codec_read_error(
            index,
            "blosc",
            format!(
                "Blosc header declares {declared_cbytes} compressed bytes, actual object has {}",
                data.len()
            ),
        ));
    }
    Ok(())
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
    use std::io::Write;

    fn v3(codecs: Value) -> ZarrFdwResult<(CodecPipeline, String)> {
        CodecPipeline::from_v3("float32", 3, &codecs)
    }

    fn v3_blosc(cname: &str, shuffle: &str, typesize: Option<Value>) -> Value {
        let mut configuration = serde_json::json!({
            "cname": cname,
            "clevel": 5,
            "shuffle": shuffle,
            "blocksize": 0
        });
        if let Some(typesize) = typesize {
            configuration["typesize"] = typesize;
        }
        serde_json::json!({"name":"blosc", "configuration":configuration})
    }

    fn v3_zstd(level: Value, checksum: Option<Value>) -> Value {
        let mut configuration = serde_json::json!({"level":level});
        if let Some(checksum) = checksum {
            configuration["checksum"] = checksum;
        }
        serde_json::json!({"name":"zstd", "configuration":configuration})
    }

    fn zstd_pipeline(checksum: bool) -> CodecPipeline {
        CodecPipeline::from_v3(
            "int8",
            1,
            &serde_json::json!([
                {"name":"bytes","configuration":{}},
                v3_zstd(serde_json::json!(1), Some(serde_json::json!(checksum)))
            ]),
        )
        .unwrap()
        .0
    }

    fn encode_zstd(raw: &[u8], checksum: bool, include_content_size: bool) -> Vec<u8> {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.include_checksum(checksum).unwrap();
        encoder.include_contentsize(include_content_size).unwrap();
        let pledged_size = u64::try_from(raw.len()).unwrap();
        encoder
            .set_pledged_src_size(include_content_size.then_some(pledged_size))
            .unwrap();
        encoder.write_all(raw).unwrap();
        encoder.finish().unwrap()
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
    fn parses_supported_v3_zstd_configurations_and_label() {
        for level in [-131_072, 0, 22] {
            for checksum in [None, Some(false), Some(true)] {
                let codecs = serde_json::json!([
                    {"name":"bytes","configuration":{"endian":"little"}},
                    v3_zstd(
                        serde_json::json!(level),
                        checksum.map(|value| serde_json::json!(value))
                    ),
                    {"name":"crc32c"}
                ]);
                let (pipeline, dtype) = v3(codecs).unwrap();
                assert_eq!(dtype, "<f4");
                assert_eq!(pipeline.ordered_label(), "bytes -> zstd -> crc32c");
                assert_eq!(
                    pipeline.encoded_read_limit(1024).unwrap(),
                    1024 + COMPRESSED_OVERHEAD_ALLOWANCE + CRC32C_BYTES
                );
                let CodecStage::Zstd { config } = &pipeline.stages[1] else {
                    panic!("expected configured v3 Zstd stage");
                };
                assert_eq!(config.level, level);
                assert_eq!(config.checksum, checksum.unwrap_or(false));
            }
        }
    }

    #[test]
    fn rejects_malformed_v3_zstd_configurations() {
        let invalid = [
            serde_json::json!({"name":"zstd"}),
            serde_json::json!({"name":"zstd","configuration":{}}),
            serde_json::json!({"name":"zstd","configuration":{"checksum":true}}),
            v3_zstd(serde_json::json!(-131_073), None),
            v3_zstd(serde_json::json!(23), None),
            v3_zstd(serde_json::json!(1.5), None),
            v3_zstd(serde_json::json!("1"), None),
            v3_zstd(serde_json::json!(1), Some(serde_json::json!(1))),
            serde_json::json!({"name":"zstd","configuration":{"level":1,"extra":true}}),
            serde_json::json!({"name":"zstd","configuration":{"level":1},"must_understand":"yes"}),
        ];
        for codec in invalid {
            let codecs = serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                codec
            ]);
            let error = v3(codecs).unwrap_err();
            assert!(matches!(error, ZarrFdwError::InvalidMetadata(_)));
        }
    }

    #[test]
    fn rejects_duplicate_mixed_and_misordered_v3_zstd() {
        let zstd = v3_zstd(serde_json::json!(1), None);
        for codecs in [
            serde_json::json!([
                zstd.clone(),
                {"name":"bytes","configuration":{"endian":"little"}}
            ]),
            serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                zstd.clone(),
                zstd.clone()
            ]),
            serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                {"name":"gzip","configuration":{"level":1}},
                zstd.clone()
            ]),
            serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                zstd.clone(),
                v3_blosc("lz4", "noshuffle", None)
            ]),
            serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                {"name":"crc32c"},
                zstd
            ]),
        ] {
            assert!(matches!(v3(codecs), Err(ZarrFdwError::InvalidMetadata(_))));
        }
    }

    #[test]
    fn parses_supported_v3_blosc_configurations_and_label() {
        for cname in ["blosclz", "lz4", "lz4hc"] {
            for (shuffle, typesize) in [
                ("noshuffle", None),
                ("noshuffle", Some(serde_json::json!(4))),
                ("shuffle", Some(serde_json::json!(4))),
                ("bitshuffle", Some(serde_json::json!(4))),
            ] {
                let codecs = serde_json::json!([
                    {"name":"bytes","configuration":{"endian":"little"}},
                    v3_blosc(cname, shuffle, typesize),
                    {"name":"crc32c"}
                ]);
                let (pipeline, dtype) = v3(codecs).unwrap();
                assert_eq!(dtype, "<f4");
                assert_eq!(pipeline.ordered_label(), "bytes -> blosc -> crc32c");
                assert_eq!(
                    pipeline.encoded_read_limit(1024).unwrap(),
                    1024 + BLOSC_HEADER_BYTES + CRC32C_BYTES
                );
                let CodecStage::Blosc {
                    config: Some(config),
                } = &pipeline.stages[1]
                else {
                    panic!("expected configured v3 Blosc stage");
                };
                assert!((0..=9).contains(&config.clevel));
            }
        }
    }

    #[test]
    fn rejects_malformed_and_unavailable_v3_blosc_configurations() {
        let invalid = [
            serde_json::json!({"name":"blosc"}),
            serde_json::json!({"name":"blosc","configuration":{}}),
            serde_json::json!({"name":"blosc","configuration":{"cname":1,"clevel":5,"shuffle":"noshuffle","blocksize":0}}),
            v3_blosc("future", "noshuffle", None),
            serde_json::json!({"name":"blosc","configuration":{"cname":"lz4","clevel":-1,"shuffle":"noshuffle","blocksize":0}}),
            serde_json::json!({"name":"blosc","configuration":{"cname":"lz4","clevel":10,"shuffle":"noshuffle","blocksize":0}}),
            serde_json::json!({"name":"blosc","configuration":{"cname":"lz4","clevel":1.5,"shuffle":"noshuffle","blocksize":0}}),
            v3_blosc("lz4", "auto", Some(serde_json::json!(4))),
            v3_blosc("lz4", "shuffle", None),
            v3_blosc("lz4", "shuffle", Some(serde_json::json!(0))),
            v3_blosc("lz4", "shuffle", Some(serde_json::json!(-1))),
            serde_json::json!({"name":"blosc","configuration":{"cname":"lz4","clevel":5,"shuffle":"noshuffle"}}),
            serde_json::json!({"name":"blosc","configuration":{"cname":"lz4","clevel":5,"shuffle":"noshuffle","blocksize":-1}}),
            serde_json::json!({"name":"blosc","configuration":{"cname":"lz4","clevel":5,"shuffle":"noshuffle","blocksize":0,"extra":true}}),
            serde_json::json!({"name":"blosc","configuration":{"cname":"lz4","clevel":5,"shuffle":"noshuffle","blocksize":0},"must_understand":"yes"}),
        ];
        for codec in invalid {
            let codecs = serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                codec
            ]);
            assert!(matches!(v3(codecs), Err(ZarrFdwError::InvalidMetadata(_))));
        }

        for cname in ["zstd", "snappy", "zlib"] {
            let codecs = serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                v3_blosc(cname, "noshuffle", None)
            ]);
            let error = v3(codecs).unwrap_err();
            assert!(matches!(
                &error,
                ZarrFdwError::UnsupportedExecutionFeature(_)
            ));
            assert!(format!("{error}").contains(&format!("Blosc cname '{cname}'")));
        }
    }

    #[test]
    fn rejects_duplicate_or_misordered_v3_blosc() {
        let blosc = v3_blosc("lz4", "shuffle", Some(serde_json::json!(4)));
        for codecs in [
            serde_json::json!([
                blosc.clone(),
                {"name":"bytes","configuration":{"endian":"little"}}
            ]),
            serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                blosc.clone(),
                blosc.clone()
            ]),
            serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                {"name":"gzip","configuration":{"level":1}},
                blosc.clone()
            ]),
            serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                blosc,
                {"name":"gzip","configuration":{"level":1}}
            ]),
        ] {
            assert!(matches!(v3(codecs), Err(ZarrFdwError::InvalidMetadata(_))));
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
            serde_json::json!([{"name":"bytes","configuration":{"endian":"little"}}, {"name":"zstd","configuration":{"level":23}}]),
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
    fn parses_zstd_frame_header_variants_and_enforces_window_policy() {
        let mut unknown_size = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        unknown_size.extend_from_slice(&[0b0001_0000, 13 << 3]);
        assert_eq!(
            parse_zstd_frame_header(&unknown_size, 1).unwrap(),
            ZstdFrameHeader {
                checksum: false,
                content_size: None,
            }
        );

        let mut single_segment = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        single_segment.extend_from_slice(&[0b0010_0100, 4]);
        assert_eq!(
            parse_zstd_frame_header(&single_segment, 1).unwrap(),
            ZstdFrameHeader {
                checksum: true,
                content_size: Some(4),
            }
        );

        for (descriptor, encoded, expected) in [
            (0b0100_0000, 0u64, 256u64),
            (0b1000_0000, 65_792u64, 65_792u64),
            (
                0b1100_0000,
                u64::from(u32::MAX) + 1,
                u64::from(u32::MAX) + 1,
            ),
        ] {
            let width = match descriptor >> 6 {
                1 => 2,
                2 => 4,
                3 => 8,
                _ => unreachable!(),
            };
            let mut frame = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
            frame.extend_from_slice(&[descriptor, 0]);
            frame.extend_from_slice(&encoded.to_le_bytes()[..width]);
            assert_eq!(
                parse_zstd_frame_header(&frame, 1).unwrap().content_size,
                Some(expected)
            );
        }

        let mut excessive_window = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        excessive_window.extend_from_slice(&[0, (13 << 3) | 1]);
        let error = parse_zstd_frame_header(&excessive_window, 1).unwrap_err();
        assert!(format!("{error}").contains("exceeds the 8388608-byte limit"));

        let mut excessive_single_segment = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        excessive_single_segment.push(0b1110_0000);
        excessive_single_segment.extend_from_slice(&(ZSTD_WINDOW_BYTES + 1).to_le_bytes());
        let error = parse_zstd_frame_header(&excessive_single_segment, 1).unwrap_err();
        assert!(format!("{error}").contains("exceeds the 8388608-byte limit"));

        let mut reserved = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        reserved.extend_from_slice(&[0b0000_1000, 0]);
        assert!(
            format!("{}", parse_zstd_frame_header(&reserved, 1).unwrap_err())
                .contains("reserved descriptor bit")
        );

        let mut dictionary = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        dictionary.push(1);
        assert!(
            format!("{}", parse_zstd_frame_header(&dictionary, 1).unwrap_err())
                .contains("Zstandard dictionaries are not supported")
        );

        let skippable = ZSTD_SKIPPABLE_MAGIC_START.to_le_bytes();
        assert!(
            format!("{}", parse_zstd_frame_header(&skippable, 1).unwrap_err())
                .contains("skippable Zstandard frames are not supported")
        );
        assert!(
            format!("{}", parse_zstd_frame_header(&[1, 2, 3], 1).unwrap_err())
                .contains("invalid Zstandard frame header")
        );
        assert!(
            format!(
                "{}",
                parse_zstd_frame_header(&[0, 0, 0, 0, 0, 0], 1).unwrap_err()
            )
            .contains("unexpected magic")
        );
        assert!(
            format!(
                "{}",
                parse_zstd_frame_header(&ZSTD_FRAME_MAGIC.to_le_bytes(), 1).unwrap_err()
            )
            .contains("frame descriptor is missing")
        );
        let mut truncated_window = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        truncated_window.push(0);
        assert!(
            format!(
                "{}",
                parse_zstd_frame_header(&truncated_window, 1).unwrap_err()
            )
            .contains("window descriptor is missing")
        );
        let mut truncated_content_size = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        truncated_content_size.push(0b0010_0000);
        assert!(
            format!(
                "{}",
                parse_zstd_frame_header(&truncated_content_size, 1).unwrap_err()
            )
            .contains("content-size field is truncated")
        );
    }

    #[test]
    fn zstd_decodes_known_and_unknown_content_sizes_and_polls_cancellation() {
        let raw = (0..(STREAM_POLL_BYTES * 2 + 17))
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>();
        let known = encode_zstd(&raw, true, true);
        let unknown = encode_zstd(&raw, false, false);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert_eq!(
                zstd_pipeline(true)
                    .decode_interruptible(known.clone(), &[raw.len()], 1, || false)
                    .await
                    .unwrap(),
                CodecDecode::Decoded(raw.clone())
            );
            assert_eq!(
                zstd_pipeline(false)
                    .decode_interruptible(unknown.clone(), &[raw.len()], 1, || false)
                    .await
                    .unwrap(),
                CodecDecode::Decoded(raw.clone())
            );
        });

        let config = ZstdConfig {
            level: 1,
            checksum: false,
        };
        assert_eq!(
            decode_zstd(unknown.clone(), raw.len(), &config, &mut || true, 1).unwrap(),
            None
        );
        let mut polls = 0usize;
        assert_eq!(
            decode_zstd(
                unknown,
                raw.len(),
                &config,
                &mut || {
                    polls += 1;
                    polls >= 5
                },
                1,
            )
            .unwrap(),
            None
        );
        assert!(polls >= 5);
    }

    #[test]
    fn zstd_rejects_checksum_mismatch_corruption_and_non_exact_output() {
        let raw = (0u8..64).collect::<Vec<_>>();
        let checksummed = encode_zstd(&raw, true, true);
        let unchecked = encode_zstd(&raw, false, true);

        for (encoded, metadata_checksum) in
            [(checksummed.clone(), false), (unchecked.clone(), true)]
        {
            let config = ZstdConfig {
                level: 1,
                checksum: metadata_checksum,
            };
            let error = decode_zstd(encoded, raw.len(), &config, &mut || false, 1).unwrap_err();
            assert!(
                format!("{error}")
                    .contains("Zstandard checksum metadata does not match the frame checksum flag")
            );
        }

        let mut corrupt = checksummed;
        *corrupt.last_mut().unwrap() ^= 1;
        let error = decode_zstd(
            corrupt,
            raw.len(),
            &ZstdConfig {
                level: 1,
                checksum: true,
            },
            &mut || false,
            1,
        )
        .unwrap_err();
        let error = format!("{error}");
        assert!(error.contains("codec index 1 ('zstd')"));
        assert!(error.contains("failed to decode Zstandard frame"));

        let unknown = encode_zstd(&raw, false, false);
        let config = ZstdConfig {
            level: 1,
            checksum: false,
        };
        let short =
            decode_zstd(unknown.clone(), raw.len() + 1, &config, &mut || false, 1).unwrap_err();
        assert!(format!("{short}").contains("expected exactly 65"));
        let long = decode_zstd(unknown, raw.len() - 1, &config, &mut || false, 1).unwrap_err();
        assert!(format!("{long}").contains("more than 63 bytes"));
    }

    #[test]
    fn zstd_rejects_frame_policy_violations_before_decoding() {
        let raw = [1u8, 2, 3, 4];
        let frame = encode_zstd(&raw, true, true);
        let config = ZstdConfig {
            level: 1,
            checksum: true,
        };

        let mismatch = decode_zstd(frame.clone(), 5, &config, &mut || false, 1).unwrap_err();
        assert!(format!("{mismatch}").contains("content size 4 does not match expected 5"));

        let mut trailing = frame.clone();
        trailing.push(0);
        let error = decode_zstd(trailing, raw.len(), &config, &mut || false, 1).unwrap_err();
        assert!(
            format!("{error}")
                .contains("concatenated Zstandard frames and trailing bytes are not supported")
        );

        let mut concatenated = frame.clone();
        concatenated.extend_from_slice(&frame);
        let error = decode_zstd(concatenated, raw.len(), &config, &mut || false, 1).unwrap_err();
        assert!(
            format!("{error}")
                .contains("concatenated Zstandard frames and trailing bytes are not supported")
        );

        let mut excessive_window = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        excessive_window.extend_from_slice(&[0b0000_0100, (13 << 3) | 1]);
        assert!(
            format!(
                "{}",
                decode_zstd(excessive_window, raw.len(), &config, &mut || false, 1).unwrap_err()
            )
            .contains("exceeds the 8388608-byte limit")
        );

        let mut dictionary = ZSTD_FRAME_MAGIC.to_le_bytes().to_vec();
        dictionary.push(0b0000_0101);
        assert!(
            format!(
                "{}",
                decode_zstd(dictionary, raw.len(), &config, &mut || false, 1).unwrap_err()
            )
            .contains("Zstandard dictionaries are not supported")
        );
    }

    #[test]
    fn pipeline_decodes_crc_then_zstd_then_transpose() {
        let logical = (0u8..12).collect::<Vec<_>>();
        let encoded_order = vec![0, 6, 3, 9, 1, 7, 4, 10, 2, 8, 5, 11];
        let compressed = encode_zstd(&encoded_order, true, true);
        let mut encoded = compressed.clone();
        encoded.extend_from_slice(&crc32c::crc32c(&compressed).to_le_bytes());
        let pipeline = v3(serde_json::json!([
            {"name":"transpose","configuration":{"order":[2,1,0]}},
            {"name":"bytes","configuration":{"endian":"little"}},
            v3_zstd(serde_json::json!(1), Some(serde_json::json!(true))),
            {"name":"crc32c"}
        ]))
        .unwrap()
        .0;
        assert_eq!(
            pipeline.ordered_label(),
            "transpose -> bytes -> zstd -> crc32c"
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        assert_eq!(
            rt.block_on(pipeline.decode_interruptible(encoded, &[2, 2, 3], 1, || false))
                .unwrap(),
            CodecDecode::Decoded(logical)
        );
    }

    #[test]
    fn v3_blosc_header_validation_rejects_truncation_mismatch_and_trailing_bytes() {
        use blosc_rs::{CompressAlgo, Encoder};

        let raw = (0..64u32).flat_map(u32::to_le_bytes).collect::<Vec<_>>();
        let encoded = Encoder::default()
            .compressor(CompressAlgo::Lz4)
            .typesize(4.try_into().unwrap())
            .compress(&raw)
            .unwrap();
        validate_v3_blosc_header(&encoded, raw.len(), 1).unwrap();

        let error = validate_v3_blosc_header(&encoded[..15], raw.len(), 1).unwrap_err();
        assert!(format!("{error}").contains("shorter than the 16-byte Blosc header"));

        let mut wrong_nbytes = encoded.clone();
        wrong_nbytes[4..8].copy_from_slice(&1u32.to_le_bytes());
        let error = validate_v3_blosc_header(&wrong_nbytes, raw.len(), 1).unwrap_err();
        assert!(format!("{error}").contains("declares 1 uncompressed bytes"));

        let mut wrong_cbytes = encoded.clone();
        wrong_cbytes[12..16].copy_from_slice(&16u32.to_le_bytes());
        let error = validate_v3_blosc_header(&wrong_cbytes, raw.len(), 1).unwrap_err();
        assert!(format!("{error}").contains("declares 16 compressed bytes"));

        let mut trailing = encoded;
        trailing.push(0);
        let error = validate_v3_blosc_header(&trailing, raw.len(), 1).unwrap_err();
        assert!(format!("{error}").contains("actual object has"));
    }

    #[test]
    fn v3_blosc_decodes_every_advertised_backend_and_shuffle() {
        use blosc_rs::{CompressAlgo, Encoder, Shuffle};

        // A repetitive, nontrivial payload is large enough that every enabled
        // backend actually compresses it instead of emitting Blosc's memcpy
        // fallback. That makes the test prove native unshuffle behavior too.
        let raw = (0..4096)
            .map(|index| u8::try_from(index % 16).unwrap())
            .collect::<Vec<_>>();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            for (cname, algorithm) in [
                ("blosclz", CompressAlgo::Blosclz),
                ("lz4", CompressAlgo::Lz4),
                ("lz4hc", CompressAlgo::Lz4hc),
            ] {
                for (shuffle_name, shuffle, expected_flag) in [
                    ("noshuffle", Shuffle::None, 0u8),
                    ("shuffle", Shuffle::Byte, 1u8),
                    ("bitshuffle", Shuffle::Bit, 4u8),
                ] {
                    let mut encoder = Encoder::default();
                    encoder
                        .compressor(algorithm)
                        .shuffle(shuffle)
                        .typesize(4.try_into().unwrap());
                    let encoded = encoder.compress(&raw).unwrap();
                    assert!(encoded.len() < raw.len() + BLOSC_HEADER_BYTES);
                    assert_eq!(encoded[2] & 0b111, expected_flag);
                    let pipeline = v3(serde_json::json!([
                        {"name":"bytes","configuration":{"endian":"little"}},
                        v3_blosc(cname, shuffle_name, Some(serde_json::json!(4)))
                    ]))
                    .unwrap()
                    .0;
                    assert_eq!(
                        pipeline
                            .decode_interruptible(encoded, &[1024], 4, || false)
                            .await
                            .unwrap(),
                        CodecDecode::Decoded(raw.clone()),
                        "backend={cname}, shuffle={shuffle_name}"
                    );
                }
            }
        });
    }

    #[test]
    fn v3_blosc_decodes_in_reverse_order_and_polls_after_native_call() {
        use blosc_rs::{CompressAlgo, Encoder};

        let raw = (0..64u32).flat_map(u32::to_le_bytes).collect::<Vec<_>>();
        let compressed = Encoder::default()
            .compressor(CompressAlgo::Lz4)
            .typesize(4.try_into().unwrap())
            .compress(&raw)
            .unwrap();
        let pipeline = v3(serde_json::json!([
            {"name":"bytes","configuration":{"endian":"little"}},
            v3_blosc("lz4", "shuffle", Some(serde_json::json!(4))),
            {"name":"crc32c"}
        ]))
        .unwrap()
        .0;
        let mut encoded = compressed.clone();
        encoded.extend_from_slice(&crc32c::crc32c(&compressed).to_le_bytes());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert_eq!(
                pipeline
                    .decode_interruptible(encoded.clone(), &[64], 4, || false)
                    .await
                    .unwrap(),
                CodecDecode::Decoded(raw.clone())
            );

            let no_crc = v3(serde_json::json!([
                {"name":"bytes","configuration":{"endian":"little"}},
                v3_blosc("lz4", "shuffle", Some(serde_json::json!(4)))
            ]))
            .unwrap()
            .0;
            let mut polls = 0usize;
            assert_eq!(
                no_crc
                    .decode_interruptible(compressed.clone(), &[64], 4, || {
                        polls += 1;
                        polls >= 3
                    })
                    .await
                    .unwrap(),
                CodecDecode::Interrupted
            );

            let mut truncated = compressed;
            truncated.pop();
            let declared = u32::try_from(truncated.len()).unwrap();
            truncated[12..16].copy_from_slice(&declared.to_le_bytes());
            let error = no_crc
                .decode_interruptible(truncated, &[64], 4, || false)
                .await
                .unwrap_err();
            let error = format!("{error}");
            assert!(error.contains("codec index 1 ('blosc')"));
            assert!(
                error.contains("invalid Blosc chunk") || error.contains("failed to decompress")
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
