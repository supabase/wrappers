//! Primitive dtype, fill, and raw chunk byte decoding.
//!
//! - dtypes: `f4`, `f8`, `i1`, `i2`, `i4`, `i8` (signed little/big endian)
//! - byte order: `<` little-endian, `>` big-endian

use super::{ZarrFdwError, ZarrFdwResult};

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
        // numpy dtype: [byteorder]<type><size>, e.g. "<f4", "|u1", ">i2"
        let (byte_order, ty, size) = numeric_dtype_parts(dtype)?;
        // For 1-byte types byte order is irrelevant.
        let big_endian = byte_order == '>';
        let dt = match (ty, size) {
            ('f', 4) => DType::F32,
            ('f', 8) => DType::F64,
            ('i', 1) => DType::I8,
            ('i', 2) => DType::I16,
            ('i', 4) => DType::I32,
            ('i', 8) => DType::I64,
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

    fn name(self) -> &'static str {
        match self {
            DType::F32 => "f4",
            DType::F64 => "f8",
            DType::I8 => "i1",
            DType::I16 => "i2",
            DType::I32 => "i4",
            DType::I64 => "i8",
        }
    }
}

/// Decode one supported primitive value into the `f64` domain used by CF
/// scale/offset processing. Raw scans keep their exact PostgreSQL primitive
/// type; this conversion is only used when scientific decoding is enabled.
pub fn value_bytes_to_f64(dtype: DType, bytes: &[u8]) -> ZarrFdwResult<f64> {
    let too_short = |needed: usize| {
        ZarrFdwError::ReadError(std::io::Error::other(format!(
            "chunk cell data has {} bytes, expected exactly {needed}",
            bytes.len()
        )))
    };
    Ok(match dtype {
        DType::F32 => f32::from_le_bytes(bytes.try_into().map_err(|_| too_short(4))?) as f64,
        DType::F64 => f64::from_le_bytes(bytes.try_into().map_err(|_| too_short(8))?),
        DType::I8 => bytes.first().copied().ok_or_else(|| too_short(1))? as i8 as f64,
        DType::I16 => i16::from_le_bytes(bytes.try_into().map_err(|_| too_short(2))?) as f64,
        DType::I32 => i32::from_le_bytes(bytes.try_into().map_err(|_| too_short(4))?) as f64,
        DType::I64 => i64::from_le_bytes(bytes.try_into().map_err(|_| too_short(8))?) as f64,
    })
}

fn invalid_fill(dtype: &str, reason: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(format!(
        "fill_value is invalid for dtype '{dtype}': {}",
        reason.into()
    ))
}

fn float_fill(dtype: &str, value: &serde_json::Value) -> ZarrFdwResult<f64> {
    match value {
        serde_json::Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| invalid_fill(dtype, "expected a numeric value")),
        serde_json::Value::String(value) => match value.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(invalid_fill(
                dtype,
                "expected a number, 'NaN', 'Infinity', or '-Infinity'",
            )),
        },
        _ => Err(invalid_fill(
            dtype,
            "expected a number, 'NaN', 'Infinity', or '-Infinity'",
        )),
    }
}

fn signed_integer_fill(
    dtype: &str,
    value: &serde_json::Value,
    min: i64,
    max: i64,
) -> ZarrFdwResult<i64> {
    let serde_json::Value::Number(number) = value else {
        return Err(invalid_fill(dtype, "expected an integer"));
    };
    let parsed = if let Some(value) = number.as_i64() {
        value
    } else if let Some(value) = number.as_u64() {
        i64::try_from(value).map_err(|_| invalid_fill(dtype, "integer is out of range"))?
    } else {
        let value = number
            .as_f64()
            .ok_or_else(|| invalid_fill(dtype, "expected an integer"))?;
        // The exclusive upper bound avoids accepting 2^63 after an imprecise
        // f64 conversion. Plain JSON integers at i64::MAX take the as_i64 path.
        const I64_EXCLUSIVE_UPPER: f64 = 9_223_372_036_854_775_808.0;
        if !value.is_finite()
            || value.fract() != 0.0
            || value < i64::MIN as f64
            || value >= I64_EXCLUSIVE_UPPER
        {
            return Err(invalid_fill(dtype, "expected an in-range integer"));
        }
        value as i64
    };
    if parsed < min || parsed > max {
        return Err(invalid_fill(dtype, "integer is out of range"));
    }
    Ok(parsed)
}

fn unsigned_integer_fill(dtype: &str, value: &serde_json::Value, max: u64) -> ZarrFdwResult<u64> {
    let serde_json::Value::Number(number) = value else {
        return Err(invalid_fill(dtype, "expected a non-negative integer"));
    };
    let parsed = if let Some(value) = number.as_u64() {
        value
    } else if let Some(value) = number.as_i64() {
        u64::try_from(value).map_err(|_| invalid_fill(dtype, "expected a non-negative integer"))?
    } else {
        let value = number
            .as_f64()
            .ok_or_else(|| invalid_fill(dtype, "expected a non-negative integer"))?;
        // As above, plain u64::MAX JSON integers use the exact as_u64 path.
        const U64_EXCLUSIVE_UPPER: f64 = 18_446_744_073_709_551_616.0;
        if !value.is_finite()
            || value.fract() != 0.0
            || !(0.0..U64_EXCLUSIVE_UPPER).contains(&value)
        {
            return Err(invalid_fill(
                dtype,
                "expected an in-range non-negative integer",
            ));
        }
        value as u64
    };
    if parsed > max {
        return Err(invalid_fill(dtype, "integer is out of range"));
    }
    Ok(parsed)
}

/// Parse a cube array's scalar fill into its decoded little-endian bytes.
/// `None` preserves Zarr's explicit-null/undefined missing-chunk semantics.
pub fn fill_value_bytes(dtype: DType, value: &serde_json::Value) -> ZarrFdwResult<Option<Vec<u8>>> {
    if value.is_null() {
        return Ok(None);
    }
    let bytes = match dtype {
        DType::F32 => {
            let value = float_fill(dtype.name(), value)?;
            let narrowed = value as f32;
            if value.is_finite() && !narrowed.is_finite() {
                return Err(invalid_fill(dtype.name(), "number is out of range"));
            }
            narrowed.to_le_bytes().to_vec()
        }
        DType::F64 => float_fill(dtype.name(), value)?.to_le_bytes().to_vec(),
        DType::I8 => (signed_integer_fill(dtype.name(), value, i8::MIN as i64, i8::MAX as i64)?
            as i8)
            .to_le_bytes()
            .to_vec(),
        DType::I16 => (signed_integer_fill(dtype.name(), value, i16::MIN as i64, i16::MAX as i64)?
            as i16)
            .to_le_bytes()
            .to_vec(),
        DType::I32 => (signed_integer_fill(dtype.name(), value, i32::MIN as i64, i32::MAX as i64)?
            as i32)
            .to_le_bytes()
            .to_vec(),
        DType::I64 => signed_integer_fill(dtype.name(), value, i64::MIN, i64::MAX)?
            .to_le_bytes()
            .to_vec(),
    };
    Ok(Some(bytes))
}

fn numeric_dtype_parts(dtype: &str) -> ZarrFdwResult<(char, char, usize)> {
    let mut chars = dtype.chars();
    let byte_order = chars
        .next()
        .ok_or_else(|| ZarrFdwError::UnsupportedDataType(dtype.to_string()))?;
    if !matches!(byte_order, '=' | '|' | '<' | '>') {
        return Err(ZarrFdwError::UnsupportedDataType(dtype.to_string()));
    }
    let kind = chars
        .next()
        .ok_or_else(|| ZarrFdwError::UnsupportedDataType(dtype.to_string()))?;
    let size_text = chars.collect::<String>();
    if size_text.is_empty() || !size_text.chars().all(|c| c.is_ascii_digit()) {
        return Err(ZarrFdwError::UnsupportedDataType(dtype.to_string()));
    }
    let size = size_text
        .parse()
        .map_err(|_| ZarrFdwError::UnsupportedDataType(dtype.to_string()))?;
    Ok((byte_order, kind, size))
}

fn exact_i64_coordinate(dtype: &str, value: i64) -> ZarrFdwResult<f64> {
    let converted = value as f64;
    if converted as i128 != value as i128 {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "coordinate integer {value} for dtype '{dtype}' cannot be represented exactly as double precision"
        )));
    }
    Ok(converted)
}

fn exact_u64_coordinate(dtype: &str, value: u64) -> ZarrFdwResult<f64> {
    let converted = value as f64;
    if converted as u128 != value as u128 {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "coordinate integer {value} for dtype '{dtype}' cannot be represented exactly as double precision"
        )));
    }
    Ok(converted)
}

/// Decoded byte width of a supported coordinate scalar.
pub fn coordinate_itemsize(dtype: &str) -> ZarrFdwResult<usize> {
    let (_, kind, size) = numeric_dtype_parts(dtype)?;
    match (kind, size) {
        ('f', 4 | 8) | ('i' | 'u', 1 | 2 | 4 | 8) => Ok(size),
        _ => Err(ZarrFdwError::UnsupportedDataType(dtype.to_string())),
    }
}

/// Parse a coordinate array's scalar fill into the f64 representation used by
/// the scan. Coordinate dtypes retain the broader signed/unsigned and endian
/// coverage of [`coord_bytes_to_f64`].
pub fn coord_fill_value_to_f64(
    dtype: &str,
    value: &serde_json::Value,
) -> ZarrFdwResult<Option<f64>> {
    if value.is_null() {
        return Ok(None);
    }
    let (_, kind, size) = numeric_dtype_parts(dtype)?;
    let parsed = match (kind, size) {
        ('f', 4) => {
            let value = float_fill(dtype, value)?;
            let narrowed = value as f32;
            if value.is_finite() && !narrowed.is_finite() {
                return Err(invalid_fill(dtype, "number is out of range"));
            }
            narrowed as f64
        }
        ('f', 8) => float_fill(dtype, value)?,
        ('i', 1) => exact_i64_coordinate(
            dtype,
            signed_integer_fill(dtype, value, i8::MIN as i64, i8::MAX as i64)?,
        )?,
        ('i', 2) => exact_i64_coordinate(
            dtype,
            signed_integer_fill(dtype, value, i16::MIN as i64, i16::MAX as i64)?,
        )?,
        ('i', 4) => exact_i64_coordinate(
            dtype,
            signed_integer_fill(dtype, value, i32::MIN as i64, i32::MAX as i64)?,
        )?,
        ('i', 8) => exact_i64_coordinate(
            dtype,
            signed_integer_fill(dtype, value, i64::MIN, i64::MAX)?,
        )?,
        ('u', 1) => {
            exact_u64_coordinate(dtype, unsigned_integer_fill(dtype, value, u8::MAX as u64)?)?
        }
        ('u', 2) => {
            exact_u64_coordinate(dtype, unsigned_integer_fill(dtype, value, u16::MAX as u64)?)?
        }
        ('u', 4) => {
            exact_u64_coordinate(dtype, unsigned_integer_fill(dtype, value, u32::MAX as u64)?)?
        }
        ('u', 8) => exact_u64_coordinate(dtype, unsigned_integer_fill(dtype, value, u64::MAX)?)?,
        _ => return Err(ZarrFdwError::UnsupportedDataType(dtype.to_string())),
    };
    Ok(Some(parsed))
}

/// Interpret raw `data` bytes as coordinate values (`f64`) using the numpy
/// style `dtype` string of a coordinate array (e.g. `<f8`, `>i4`, `|u2`).
///
/// Coordinates can be stored as floats or (un)signed ints in either byte
/// order. Integer values must be exactly representable as `f64`, because the
/// resulting value is used for both identity and predicate pruning.
pub fn coord_bytes_to_f64(dtype: &str, data: &[u8]) -> ZarrFdwResult<Vec<f64>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let (byte_order, kind, size) = numeric_dtype_parts(dtype)?;
    let item = coordinate_itemsize(dtype)?;

    let read = |b: &[u8]| -> ZarrFdwResult<f64> {
        match (kind, size) {
            ('f', 4) => Ok(f32_bytes(b, byte_order) as f64),
            ('f', 8) => Ok(f64_bytes(b, byte_order)),
            ('i', 1) => exact_i64_coordinate(dtype, b[0] as i8 as i64),
            ('i', 2) => exact_i64_coordinate(dtype, i16_bytes(b, byte_order) as i64),
            ('i', 4) => exact_i64_coordinate(dtype, i32_bytes(b, byte_order) as i64),
            ('i', 8) => exact_i64_coordinate(dtype, i64_bytes(b, byte_order)),
            ('u', 1) => exact_u64_coordinate(dtype, b[0] as u64),
            ('u', 2) => exact_u64_coordinate(dtype, u16_bytes(b, byte_order) as u64),
            ('u', 4) => exact_u64_coordinate(dtype, u32_bytes(b, byte_order) as u64),
            ('u', 8) => exact_u64_coordinate(dtype, u64_bytes(b, byte_order)),
            _ => Err(ZarrFdwError::UnsupportedDataType(dtype.to_string())),
        }
    };

    if !data.len().is_multiple_of(item) {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "coordinate data length {} is not a multiple of dtype item size {item}",
            data.len()
        )));
    }
    let mut out = Vec::with_capacity(data.len() / item);
    for b in data.chunks(item) {
        out.push(read(b)?);
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
    fn parses_floating_fill_values() {
        let finite = fill_value_bytes(DType::F32, &serde_json::json!(-7.5))
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_le_bytes(finite.try_into().unwrap()), -7.5);

        for (text, expected) in [
            ("NaN", f64::NAN),
            ("Infinity", f64::INFINITY),
            ("-Infinity", f64::NEG_INFINITY),
        ] {
            let bytes = fill_value_bytes(DType::F64, &serde_json::json!(text))
                .unwrap()
                .unwrap();
            let actual = f64::from_le_bytes(bytes.try_into().unwrap());
            if expected.is_nan() {
                assert!(actual.is_nan());
            } else {
                assert_eq!(actual, expected);
            }
        }
        assert_eq!(
            fill_value_bytes(DType::F64, &serde_json::Value::Null).unwrap(),
            None
        );
    }

    #[test]
    fn parses_exact_range_signed_integer_fills() {
        let cases = [
            (
                DType::I8,
                serde_json::json!(i8::MIN),
                i8::MIN.to_le_bytes().to_vec(),
            ),
            (
                DType::I16,
                serde_json::json!(i16::MAX),
                i16::MAX.to_le_bytes().to_vec(),
            ),
            (
                DType::I32,
                serde_json::json!(i32::MIN),
                i32::MIN.to_le_bytes().to_vec(),
            ),
            (
                DType::I64,
                serde_json::json!(i64::MAX),
                i64::MAX.to_le_bytes().to_vec(),
            ),
        ];
        for (dtype, fill, expected) in cases {
            assert_eq!(fill_value_bytes(dtype, &fill).unwrap(), Some(expected));
        }
    }

    #[test]
    fn rejects_invalid_or_out_of_range_fills() {
        assert!(fill_value_bytes(DType::I8, &serde_json::json!(128)).is_err());
        assert!(fill_value_bytes(DType::I16, &serde_json::json!(1.5)).is_err());
        assert!(fill_value_bytes(DType::I32, &serde_json::json!("1")).is_err());
        assert!(fill_value_bytes(DType::F32, &serde_json::json!(1e100)).is_err());
        assert!(fill_value_bytes(DType::F64, &serde_json::json!("nan")).is_err());
    }

    #[test]
    fn parses_coordinate_fills_for_supported_numeric_dtypes() {
        assert_eq!(
            coord_fill_value_to_f64(">i2", &serde_json::json!(-12)).unwrap(),
            Some(-12.0)
        );
        assert_eq!(
            coord_fill_value_to_f64("<u2", &serde_json::json!(65535)).unwrap(),
            Some(65535.0)
        );
        assert!(
            coord_fill_value_to_f64("<f8", &serde_json::json!("NaN"))
                .unwrap()
                .unwrap()
                .is_nan()
        );
        assert!(coord_fill_value_to_f64("<u1", &serde_json::json!(-1)).is_err());
    }

    #[test]
    fn coordinate_integer_fills_must_be_exact_in_double_precision() {
        let exact = 1u64 << 63;
        assert_eq!(
            coord_fill_value_to_f64("<u8", &serde_json::json!(exact)).unwrap(),
            Some(exact as f64)
        );
        assert!(coord_fill_value_to_f64("<i8", &serde_json::json!((1i64 << 53) + 1)).is_err());
        assert!(coord_fill_value_to_f64("<u8", &serde_json::json!(u64::MAX)).is_err());
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
    fn coordinate_integer_values_must_be_exact_in_double_precision() {
        let exact = (1i64 << 53).to_le_bytes();
        assert_eq!(
            coord_bytes_to_f64("<i8", &exact).unwrap(),
            vec![(1i64 << 53) as f64]
        );

        let lossy_signed = ((1i64 << 53) + 1).to_le_bytes();
        assert!(coord_bytes_to_f64("<i8", &lossy_signed).is_err());
        let lossy_unsigned = u64::MAX.to_le_bytes();
        assert!(coord_bytes_to_f64("<u8", &lossy_unsigned).is_err());
    }

    #[test]
    fn coord_f64_rejects_unknown_kind() {
        assert!(coord_bytes_to_f64("|S10", b"hello").is_err());
    }
}
