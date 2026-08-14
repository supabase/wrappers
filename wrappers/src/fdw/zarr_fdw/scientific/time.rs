use chrono::{DateTime, NaiveDate, NaiveDateTime};
use serde_json::{Map, Value};

use super::super::{ZarrFdwError, ZarrFdwResult};

const ATTR_UNITS: &str = "units";
const ATTR_CALENDAR: &str = "calendar";
const SUPPORTED_CALENDAR: &str = "proleptic_gregorian";

// PostgreSQL epoch (2000-01-01 00:00:00 UTC) in microseconds since Unix epoch.
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
const PG_EPOCH_SECONDS: i64 = 946_684_800;
const I64_MIN_AS_F64: f64 = -9_223_372_036_854_775_808.0;
const I64_MAX_EXCLUSIVE_AS_F64: f64 = 9_223_372_036_854_775_808.0;

/// Unit of raw CF time coordinate values.
#[derive(Debug, Clone, Copy, PartialEq)]
enum TimeUnit {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
    Minutes,
    Hours,
    Days,
}

impl TimeUnit {
    fn parse_option(value: &str) -> ZarrFdwResult<Self> {
        match value {
            "seconds" => Ok(Self::Seconds),
            "milliseconds" => Ok(Self::Milliseconds),
            "microseconds" => Ok(Self::Microseconds),
            "nanoseconds" => Ok(Self::Nanoseconds),
            "minutes" => Ok(Self::Minutes),
            "hours" => Ok(Self::Hours),
            "days" => Ok(Self::Days),
            _ => Err(ZarrFdwError::InvalidOptionValue {
                option: value.to_string(),
                message: supported_units_message(),
            }),
        }
    }

    fn parse_cf(value: &str) -> ZarrFdwResult<Self> {
        Self::parse_option(value).map_err(|_| {
            time_metadata_error(format!(
                "unit '{value}' is unsupported; {}",
                supported_units_message()
            ))
        })
    }

    fn microseconds_factor(self) -> f64 {
        match self {
            Self::Seconds => 1_000_000.0,
            Self::Milliseconds => 1_000.0,
            Self::Microseconds => 1.0,
            Self::Nanoseconds => 1e-3,
            Self::Minutes => 60_000_000.0,
            Self::Hours => 3_600_000_000.0,
            Self::Days => 86_400_000_000.0,
        }
    }
}

/// Describes how raw `time` coordinate values map to PostgreSQL instants.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TimeSpec {
    unit: TimeUnit,
    origin_unix_micros: i64,
}

impl TimeSpec {
    pub(crate) fn default() -> Self {
        Self {
            unit: TimeUnit::Seconds,
            origin_unix_micros: 0,
        }
    }

    pub(crate) fn from_legacy_options(
        unit: Option<&str>,
        origin: Option<&str>,
    ) -> ZarrFdwResult<Self> {
        let unit = match unit {
            Some(value) => TimeUnit::parse_option(value)?,
            None => TimeUnit::Seconds,
        };
        let origin_unix_micros = match origin {
            Some("unix") => 0,
            Some("postgres") => PG_EPOCH_SECONDS
                .checked_mul(1_000_000)
                .ok_or_else(|| time_metadata_error("PostgreSQL epoch overflowed"))?,
            Some(other) => {
                return Err(ZarrFdwError::InvalidOptionValue {
                    option: other.to_string(),
                    message: "must be 'unix' or 'postgres'".to_string(),
                });
            }
            None => 0,
        };
        Ok(Self {
            unit,
            origin_unix_micros,
        })
    }

    pub(crate) fn from_cf_attributes(attributes: &Map<String, Value>) -> ZarrFdwResult<Self> {
        let units = required_string_attribute(attributes, ATTR_UNITS)?;
        let calendar = required_string_attribute(attributes, ATTR_CALENDAR)?;
        if calendar != SUPPORTED_CALENDAR {
            return Err(time_metadata_error(format!(
                "calendar must be '{SUPPORTED_CALENDAR}', got '{calendar}'"
            )));
        }
        let (unit, origin) = units.split_once(" since ").ok_or_else(|| {
            time_metadata_error("units must have the form '<unit> since <origin>'")
        })?;
        if unit.trim() != unit || origin.trim() != origin || unit.is_empty() || origin.is_empty() {
            return Err(time_metadata_error(
                "units must have the form '<unit> since <origin>' without empty fields",
            ));
        }
        Ok(Self {
            unit: TimeUnit::parse_cf(unit)?,
            origin_unix_micros: parse_origin_micros(origin)?,
        })
    }

    /// Convert a raw coordinate value into PostgreSQL-epoch microseconds.
    pub(crate) fn raw_to_pg_micros(&self, raw: f64) -> ZarrFdwResult<i64> {
        if !raw.is_finite() {
            return Err(time_metadata_error(format!(
                "raw time coordinate value must be finite, got {raw}"
            )));
        }
        let unix_micros = raw
            .mul_add(
                self.unit.microseconds_factor(),
                self.origin_unix_micros as f64,
            )
            .round();
        let unix_micros = checked_f64_to_i64_micros(unix_micros)?;
        unix_micros
            .checked_sub(PG_EPOCH_MICROS)
            .ok_or_else(|| ZarrFdwError::TimeOutOfRange(raw))
    }

    /// Return the conservative raw-coordinate interval that can round to one
    /// PostgreSQL microsecond. Both endpoints are included intentionally:
    /// PostgreSQL rechecks the original qual, while inclusive bounds ensure
    /// pruning never drops values at `f64::round` tie boundaries.
    pub(crate) fn pg_micros_to_raw_bounds(&self, pg_micros: i64) -> Option<(f64, f64)> {
        let unix_micros = pg_micros.checked_add(PG_EPOCH_MICROS)?;
        let delta = unix_micros.checked_sub(self.origin_unix_micros)? as f64;
        let factor = self.unit.microseconds_factor();
        Some(((delta - 0.5) / factor, (delta + 0.5) / factor))
    }
}

fn required_string_attribute<'a>(
    attributes: &'a Map<String, Value>,
    name: &str,
) -> ZarrFdwResult<&'a str> {
    attributes
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| time_metadata_error(format!("attribute '{name}' must be a string")))
}

fn parse_origin_micros(origin: &str) -> ZarrFdwResult<i64> {
    if let Ok(value) = DateTime::parse_from_rfc3339(origin) {
        return Ok(value.timestamp_micros());
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(value) = NaiveDateTime::parse_from_str(origin, format) {
            return Ok(value.and_utc().timestamp_micros());
        }
    }
    if let Ok(value) = NaiveDate::parse_from_str(origin, "%Y-%m-%d") {
        let Some(value) = value.and_hms_opt(0, 0, 0) else {
            return Err(time_metadata_error(format!(
                "origin '{origin}' is outside the supported timestamp range"
            )));
        };
        return Ok(value.and_utc().timestamp_micros());
    }
    Err(time_metadata_error(format!(
        "origin '{origin}' must be a Gregorian date, date-time, or RFC 3339 date-time"
    )))
}

fn checked_f64_to_i64_micros(value: f64) -> ZarrFdwResult<i64> {
    if !value.is_finite()
        || !(I64_MIN_AS_F64..I64_MAX_EXCLUSIVE_AS_F64).contains(&value)
        || value.fract() != 0.0
    {
        return Err(time_metadata_error(
            "time conversion produced a microsecond value outside the supported range",
        ));
    }
    Ok(value as i64)
}

fn supported_units_message() -> String {
    "must be one of: seconds, milliseconds, microseconds, nanoseconds, minutes, hours, days"
        .to_string()
}

fn time_metadata_error(message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(format!("CF time metadata is invalid: {}", message.into()))
}

#[cfg(test)]
mod tests;
