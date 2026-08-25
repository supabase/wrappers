//! Narrow CF-style scientific value semantics layered above primitive decode.
//!
//! This module deliberately does not know how Zarr metadata or chunks are
//! stored. A future v3 metadata adapter can supply the same attribute map and
//! reuse this decoder unchanged.

pub(crate) mod time;

use serde_json::{Map, Value};

use super::decode::{DType, fill_value_bytes, value_bytes_to_f64};
use super::{ZarrFdwError, ZarrFdwResult};

const ATTR_FILL_VALUE: &str = "_FillValue";
const ATTR_MISSING_VALUE: &str = "missing_value";
const ATTR_VALID_RANGE: &str = "valid_range";
const ATTR_VALID_MIN: &str = "valid_min";
const ATTR_VALID_MAX: &str = "valid_max";
const ATTR_SCALE_FACTOR: &str = "scale_factor";
const ATTR_ADD_OFFSET: &str = "add_offset";

/// Parsed CF-style masking and packing rules for one value variable.
#[derive(Debug, Clone)]
pub(crate) struct ScientificValueDecoder {
    dtype: DType,
    missing_values: Vec<Vec<u8>>,
    mask_nan: bool,
    valid_min: Option<f64>,
    valid_max: Option<f64>,
    scale_factor: f64,
    add_offset: f64,
}

impl ScientificValueDecoder {
    pub(crate) fn from_attributes(
        dtype: DType,
        attributes: &Map<String, Value>,
    ) -> ZarrFdwResult<Self> {
        let mut missing_values = Vec::new();
        let mut mask_nan = false;
        if let Some(value) = attributes.get(ATTR_FILL_VALUE) {
            let (bytes, is_nan) = missing_attribute_bytes(dtype, ATTR_FILL_VALUE, value)?;
            missing_values.push(bytes);
            mask_nan |= is_nan;
        }
        if let Some(value) = attributes.get(ATTR_MISSING_VALUE) {
            match value {
                Value::Array(values) => {
                    if values.is_empty() {
                        return Err(attribute_error(
                            ATTR_MISSING_VALUE,
                            "must be a numeric scalar or non-empty numeric array",
                        ));
                    }
                    for value in values {
                        let (bytes, is_nan) =
                            missing_attribute_bytes(dtype, ATTR_MISSING_VALUE, value)?;
                        missing_values.push(bytes);
                        mask_nan |= is_nan;
                    }
                }
                _ => {
                    let (bytes, is_nan) =
                        missing_attribute_bytes(dtype, ATTR_MISSING_VALUE, value)?;
                    missing_values.push(bytes);
                    mask_nan |= is_nan;
                }
            }
        }
        missing_values.sort_unstable();
        missing_values.dedup();

        if attributes.contains_key(ATTR_VALID_RANGE)
            && (attributes.contains_key(ATTR_VALID_MIN) || attributes.contains_key(ATTR_VALID_MAX))
        {
            return Err(attribute_error(
                ATTR_VALID_RANGE,
                "cannot be combined with 'valid_min' or 'valid_max'",
            ));
        }

        let (valid_min, valid_max) = match attributes.get(ATTR_VALID_RANGE) {
            Some(Value::Array(values)) if values.len() == 2 => (
                Some(raw_attribute_f64(dtype, ATTR_VALID_RANGE, &values[0])?),
                Some(raw_attribute_f64(dtype, ATTR_VALID_RANGE, &values[1])?),
            ),
            Some(_) => {
                return Err(attribute_error(
                    ATTR_VALID_RANGE,
                    "must be a two-element numeric array",
                ));
            }
            None => (
                attributes
                    .get(ATTR_VALID_MIN)
                    .map(|value| raw_attribute_f64(dtype, ATTR_VALID_MIN, value))
                    .transpose()?,
                attributes
                    .get(ATTR_VALID_MAX)
                    .map(|value| raw_attribute_f64(dtype, ATTR_VALID_MAX, value))
                    .transpose()?,
            ),
        };
        if valid_min
            .zip(valid_max)
            .is_some_and(|(minimum, maximum)| minimum > maximum)
        {
            return Err(attribute_error(
                ATTR_VALID_RANGE,
                "minimum must not exceed maximum",
            ));
        }

        let scale_factor = finite_attribute(attributes, ATTR_SCALE_FACTOR)?.unwrap_or(1.0);
        let add_offset = finite_attribute(attributes, ATTR_ADD_OFFSET)?.unwrap_or(0.0);

        Ok(Self {
            dtype,
            missing_values,
            mask_nan,
            valid_min,
            valid_max,
            scale_factor,
            add_offset,
        })
    }

    /// Return a decoded physical value, or `None` for semantic missing data.
    /// Masking and valid-range checks intentionally happen in the raw packed
    /// domain before scale/offset are applied.
    pub(crate) fn decode(&self, raw_bytes: &[u8]) -> ZarrFdwResult<Option<f64>> {
        if self
            .missing_values
            .iter()
            .any(|missing| missing.as_slice() == raw_bytes)
        {
            return Ok(None);
        }
        let raw = value_bytes_to_f64(self.dtype, raw_bytes)?;
        if self.mask_nan && raw.is_nan() {
            return Ok(None);
        }
        if self.valid_min.is_some_and(|minimum| raw < minimum)
            || self.valid_max.is_some_and(|maximum| raw > maximum)
        {
            return Ok(None);
        }
        Ok(Some(raw.mul_add(self.scale_factor, self.add_offset)))
    }
}

fn missing_attribute_bytes(
    dtype: DType,
    name: &str,
    value: &Value,
) -> ZarrFdwResult<(Vec<u8>, bool)> {
    let mask_nan = matches!(value, Value::String(value) if value == "NaN");
    let valid_special_float = matches!(
        value,
        Value::String(value) if matches!(value.as_str(), "NaN" | "Infinity" | "-Infinity")
    ) && matches!(dtype, DType::F32 | DType::F64);
    if !value.is_number() && !valid_special_float {
        return Err(attribute_error(
            name,
            "must be numeric, or a supported non-finite string for a floating-point array",
        ));
    }
    let bytes = fill_value_bytes(dtype, value)
        .map_err(|error| attribute_error(name, error.to_string()))?
        .ok_or_else(|| attribute_error(name, "must not be null"))?;
    Ok((bytes, mask_nan))
}

fn raw_attribute_bytes(dtype: DType, name: &str, value: &Value) -> ZarrFdwResult<Vec<u8>> {
    if !value.is_number() {
        return Err(attribute_error(name, "must be a numeric value"));
    }
    fill_value_bytes(dtype, value)
        .map_err(|error| attribute_error(name, error.to_string()))?
        .ok_or_else(|| attribute_error(name, "must not be null"))
}

fn raw_attribute_f64(dtype: DType, name: &str, value: &Value) -> ZarrFdwResult<f64> {
    let bytes = raw_attribute_bytes(dtype, name, value)?;
    value_bytes_to_f64(dtype, &bytes)
}

fn finite_attribute(attributes: &Map<String, Value>, name: &str) -> ZarrFdwResult<Option<f64>> {
    let Some(value) = attributes.get(name) else {
        return Ok(None);
    };
    let value = value
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| attribute_error(name, "must be a finite numeric value"))?;
    Ok(Some(value))
}

fn attribute_error(name: &str, message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(format!(
        "CF attribute '{name}' is invalid: {}",
        message.into()
    ))
}

#[cfg(test)]
mod tests;
