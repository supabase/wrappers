//! Chunk decoding: dtype parsing, decompression, and raw chunk bytes -> cells.
//!
//! MVP codec coverage:
//! - compression: `null`/raw, `gzip`, `zlib`, Blosc/LZ4
//! - dtypes: `f4`, `f8`, `i1`, `i2`, `i4`, `i8` (signed little/big endian)
//! - byte order: `<` little-endian, `>` big-endian
//!
//! Zstd and more exotic dtypes are left as explicit errors so the gap
//! is visible rather than silently wrong.

use super::{ZarrFdwError, ZarrFdwResult};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, BufReader};

/// Decompressed-byte representation of a chunk.
pub enum Codec {
    /// No compression (compressor null).
    Raw,
    Gzip,
    Zlib,
    Blosc,
    Unsupported(String),
}

impl Codec {
    pub fn from_compressor_json(c: &Option<serde_json::Value>) -> Self {
        match c {
            None => Codec::Raw,
            Some(json) => {
                let id = json
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                match id.as_str() {
                    "" => Codec::Raw,
                    "gzip" => Codec::Gzip,
                    "zlib" => Codec::Zlib,
                    "blosc" => Codec::Blosc,
                    other => Codec::Unsupported(other.to_string()),
                }
            }
        }
    }

    pub async fn decompress(&self, data: &[u8]) -> ZarrFdwResult<Vec<u8>> {
        use async_compression::tokio::bufread::{GzipDecoder, ZlibDecoder};
        match self {
            Codec::Raw => Ok(data.to_vec()),
            Codec::Gzip => {
                let mut decoded = Vec::new();
                let mut dec = BufReader::new(GzipDecoder::new(BufReader::new(Cursor::new(data))));
                dec.read_to_end(&mut decoded).await?;
                Ok(decoded)
            }
            Codec::Zlib => {
                let mut decoded = Vec::new();
                let mut dec = BufReader::new(ZlibDecoder::new(BufReader::new(Cursor::new(data))));
                dec.read_to_end(&mut decoded).await?;
                Ok(decoded)
            }
            Codec::Blosc => {
                let decoder = blosc_rs::Decoder::new(data).map_err(|e| {
                    ZarrFdwError::ReadError(std::io::Error::other(format!(
                        "invalid Blosc chunk: {e}"
                    )))
                })?;
                decoder.decompress(1).map_err(|e| {
                    ZarrFdwError::ReadError(std::io::Error::other(format!(
                        "failed to decompress Blosc chunk: {e}"
                    )))
                })
            }
            Codec::Unsupported(id) => Err(ZarrFdwError::UnsupportedCompressor(id.clone())),
        }
    }
}

/// Parsed numpy-style dtype string, e.g. `<f4`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DType {
    F32,
    F64,
    I8,
    I16,
    I32,
    I64,
}

impl DType {
    pub fn parse(dtype: &str) -> ZarrFdwResult<Self> {
        if dtype.len() < 2 {
            return Err(ZarrFdwError::UnsupportedDataType(dtype.to_string()));
        }
        // numpy dtype: [byteorder]<type><size>, e.g. "<f4", "|u1", ">i2"
        let (byte_order, rest) = dtype.split_at(1);
        let ty = &rest[0..1];
        let size: usize = rest[1..]
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .map_err(|_| ZarrFdwError::UnsupportedDataType(dtype.to_string()))?;
        if byte_order == "=" || byte_order == "|" || byte_order == "<" || byte_order == ">" {
            // MVP only supports native-ish and explicit-endian numeric types
        } else {
            return Err(ZarrFdwError::UnsupportedDataType(dtype.to_string()));
        }
        // For 1-byte types byte order is irrelevant.
        let big_endian = byte_order == ">";
        let dt = match (ty, size) {
            ("f", 4) => DType::F32,
            ("f", 8) => DType::F64,
            ("i", 1) => DType::I8,
            ("i", 2) => DType::I16,
            ("i", 4) => DType::I32,
            ("i", 8) => DType::I64,
            _ => return Err(ZarrFdwError::UnsupportedDataType(dtype.to_string())),
        };
        if big_endian && dt != DType::I8 {
            // reject until we implement endian-swapped reads
            return Err(ZarrFdwError::UnsupportedDataType(format!(
                "{dtype} (big-endian numeric types not supported yet)"
            )));
        }
        Ok(dt)
    }

    pub fn itemsize(self) -> usize {
        match self {
            DType::F32 | DType::I32 => 4,
            DType::F64 | DType::I64 => 8,
            DType::I8 => 1,
            DType::I16 => 2,
        }
    }
}

/// Interpret raw `data` bytes as coordinate values (`f64`) using the numpy
/// style `dtype` string of a coordinate array (e.g. `<f8`, `>i4`, `|u2`).
///
/// Lenient by design: coordinates can be stored as floats or (un)signed ints,
/// in either byte order — anything numeric is read as `f64` for pushdown and
/// output. Errors are surfaced as [`ZarrFdwError::UnsupportedDataType`] when
/// the dtype is not a recognized numeric one.
pub fn coord_bytes_to_f64(dtype: &str, data: &[u8]) -> ZarrFdwResult<Vec<f64>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let mut chars = dtype.chars();
    let byte_order = chars.next().unwrap_or('<');
    let kind = chars.next().unwrap_or('f');
    let size: usize = chars
        .collect::<String>()
        .parse()
        .map_err(|_| ZarrFdwError::UnsupportedDataType(dtype.to_string()))?;

    let read = |b: &[u8]| -> f64 {
        match (kind, size) {
            ('f', 4) => f32_bytes(b, byte_order) as f64,
            ('f', 8) => f64_bytes(b, byte_order),
            ('i', 1) => b[0] as i8 as f64,
            ('i', 2) => i16_bytes(b, byte_order) as f64,
            ('i', 4) => i32_bytes(b, byte_order) as f64,
            ('i', 8) => i64_bytes(b, byte_order) as f64,
            ('u', 1) => b[0] as f64,
            ('u', 2) => u16_bytes(b, byte_order) as f64,
            ('u', 4) => u32_bytes(b, byte_order) as f64,
            ('u', 8) => u64_bytes(b, byte_order) as f64,
            _ => f64::NAN,
        }
    };

    let item = size.max(1);
    if data.len() % item != 0 {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "coordinate data length {} is not a multiple of dtype item size {item}",
            data.len()
        )));
    }
    let mut out = Vec::with_capacity(data.len() / item);
    for b in data.chunks(item) {
        let v = read(b);
        if v.is_nan() && !(kind == 'f' && (size == 4 || size == 8)) {
            return Err(ZarrFdwError::UnsupportedDataType(dtype.to_string()));
        }
        out.push(v);
    }
    Ok(out)
}

fn f32_bytes(b: &[u8], byte_order: char) -> f32 {
    let a = b.try_into().unwrap();
    if byte_order == '>' {
        f32::from_be_bytes(a)
    } else {
        f32::from_le_bytes(a)
    }
}
fn f64_bytes(b: &[u8], byte_order: char) -> f64 {
    let a = b.try_into().unwrap();
    if byte_order == '>' {
        f64::from_be_bytes(a)
    } else {
        f64::from_le_bytes(a)
    }
}
fn i16_bytes(b: &[u8], byte_order: char) -> i16 {
    let a = b.try_into().unwrap();
    if byte_order == '>' {
        i16::from_be_bytes(a)
    } else {
        i16::from_le_bytes(a)
    }
}
fn i32_bytes(b: &[u8], byte_order: char) -> i32 {
    let a = b.try_into().unwrap();
    if byte_order == '>' {
        i32::from_be_bytes(a)
    } else {
        i32::from_le_bytes(a)
    }
}
fn i64_bytes(b: &[u8], byte_order: char) -> i64 {
    let a = b.try_into().unwrap();
    if byte_order == '>' {
        i64::from_be_bytes(a)
    } else {
        i64::from_le_bytes(a)
    }
}
fn u16_bytes(b: &[u8], byte_order: char) -> u16 {
    let a = b.try_into().unwrap();
    if byte_order == '>' {
        u16::from_be_bytes(a)
    } else {
        u16::from_le_bytes(a)
    }
}
fn u32_bytes(b: &[u8], byte_order: char) -> u32 {
    let a = b.try_into().unwrap();
    if byte_order == '>' {
        u32::from_be_bytes(a)
    } else {
        u32::from_le_bytes(a)
    }
}
fn u64_bytes(b: &[u8], byte_order: char) -> u64 {
    let a = b.try_into().unwrap();
    if byte_order == '>' {
        u64::from_be_bytes(a)
    } else {
        u64::from_le_bytes(a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dtypes() {
        assert_eq!(DType::parse("<f4").unwrap(), DType::F32);
        assert_eq!(DType::parse("|i1").unwrap(), DType::I8);
        assert_eq!(DType::parse("<i2").unwrap(), DType::I16);
        assert!(DType::parse("<u8").is_err());
        assert!(DType::parse(">i4").is_err());
        assert!(DType::parse("<S10").is_err());
    }

    #[test]
    fn itemsizes() {
        assert_eq!(DType::F64.itemsize(), 8);
        assert_eq!(DType::I8.itemsize(), 1);
    }

    #[test]
    fn coord_f64_floats() {
        let data = 1.5f32.to_le_bytes();
        assert_eq!(coord_bytes_to_f64("<f4", &data).unwrap(), vec![1.5]);
        let data = (-2.25f64).to_le_bytes();
        assert_eq!(coord_bytes_to_f64("<f8", &data).unwrap(), vec![-2.25]);
    }

    #[test]
    fn coord_f64_big_endian() {
        let data = 255i16.to_be_bytes();
        assert_eq!(coord_bytes_to_f64(">i2", &data).unwrap(), vec![255.0]);
    }

    #[test]
    fn coord_f64_unsigned() {
        let data = 300u16.to_le_bytes();
        assert_eq!(coord_bytes_to_f64("<u2", &data).unwrap(), vec![300.0]);
    }

    #[test]
    fn coord_f64_ints() {
        let data = [0u8, 255];
        assert_eq!(coord_bytes_to_f64("|i1", &data).unwrap(), vec![0.0, -1.0]);
        let data = (-400000i32).to_le_bytes();
        assert_eq!(coord_bytes_to_f64("<i4", &data).unwrap(), vec![-400000.0]);
    }

    #[test]
    fn coord_f64_rejects_unknown_kind() {
        assert!(coord_bytes_to_f64("|S10", b"hello").is_err());
    }

    #[test]
    fn decompresses_blosc_lz4() {
        use blosc_rs::{CompressAlgo, Encoder};

        let raw = (0..64u64).flat_map(u64::to_le_bytes).collect::<Vec<_>>();
        let compressed = Encoder::default()
            .compressor(CompressAlgo::Lz4)
            .typesize(8.try_into().unwrap())
            .compress(&raw)
            .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let decoded = rt.block_on(Codec::Blosc.decompress(&compressed)).unwrap();
        assert_eq!(decoded, raw);
    }
}
