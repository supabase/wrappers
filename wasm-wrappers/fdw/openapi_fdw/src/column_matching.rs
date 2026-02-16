//! Column name matching and JSON-to-Cell conversion
//!
//! Handles the mapping between SQL column names and JSON keys,
//! including camelCase, snake_case, and normalized matching strategies.

use std::borrow::Cow;

use serde_json::Value as JsonValue;

use crate::bindings::supabase::wrappers::{
    time,
    types::{Cell, FdwError, TypeOid},
};
use crate::{OpenApiFdw, extract_effective_row};

/// How a SQL column name was resolved to a JSON key.
///
/// Avoids cloning strings that already exist in [`CachedColumn`] — only the
/// case-insensitive fallback (rare) needs its own allocation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyMatch {
    /// JSON key matches `CachedColumn::name` exactly
    Exact,
    /// JSON key matches `CachedColumn::camel_name`
    CamelCase,
    /// JSON key matched case-insensitively (stores the original API key)
    CaseInsensitive(String),
}

/// Pre-computed column metadata to avoid repeated WASM boundary crossings.
///
/// During `iter_scan`, each call to `ctx.get_columns()`, `col.name()`, and
/// `col.type_oid()` crosses the WASM boundary. By caching these once in
/// `begin_scan`, we eliminate ~2000 boundary crossings per 100-row scan.
#[derive(Debug)]
pub(crate) struct CachedColumn {
    pub name: String,
    pub type_oid: TypeOid,
    pub camel_name: String,
    pub lower_name: String,
    /// Alphanumeric-only lowercase name for normalized matching.
    /// Strips `@`, `.`, `-`, `$`, etc. so `@id` → `_id` → `id` can match.
    pub alnum_name: String,
}

/// Convert `snake_case` to `camelCase`
pub(crate) fn to_camel_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;

    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_uppercase().next().unwrap_or(c));
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

/// Strip non-alphanumeric chars and lowercase for normalized matching.
///
/// Used to match JSON keys with special characters (`@id`, `user.name`, `$oid`)
/// to sanitized SQL column names (`_id`, `user_name`, `_oid`).
pub(crate) fn normalize_to_alnum(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .collect::<String>()
        .to_lowercase()
}

impl OpenApiFdw {
    /// Normalize a date/datetime string for RFC3339 parsing.
    ///
    /// Handles two non-RFC3339 formats:
    /// - Date-only `"2024-01-15"` → `"2024-01-15T00:00:00Z"`
    /// - ISO 8601 tz without colon `"2024-01-15T12:00:00+0000"` → `"2024-01-15T12:00:00+00:00"`
    ///
    /// Returns `Cow<str>` to avoid allocating when the string is already valid.
    pub(crate) fn normalize_datetime(s: &str) -> Cow<'_, str> {
        // Date-only: exactly 10 chars matching YYYY-MM-DD pattern
        if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(7) == Some(&b'-')
        {
            return Cow::Owned(format!("{s}T00:00:00Z"));
        }

        // Fix timezone offset without colon: +0000 → +00:00, -0500 → -05:00
        // ISO 8601 allows ±HHMM but RFC 3339 requires ±HH:MM
        let bytes = s.as_bytes();
        let len = bytes.len();
        if len >= 5 {
            let sign_pos = len - 4;
            if (bytes[sign_pos - 1] == b'+' || bytes[sign_pos - 1] == b'-')
                && bytes[sign_pos..].iter().all(|b| b.is_ascii_digit())
            {
                let mut fixed = String::with_capacity(len + 1);
                fixed.push_str(&s[..sign_pos + 2]);
                fixed.push(':');
                fixed.push_str(&s[sign_pos + 2..]);
                return Cow::Owned(fixed);
            }
        }

        Cow::Borrowed(s)
    }

    /// Build a map from column index to resolved JSON key, using the first row's keys.
    ///
    /// This runs the 3-step matching (exact → camelCase → case-insensitive) once per
    /// column instead of once per column per row. Called after each `make_request`.
    pub(crate) fn build_column_key_map(&mut self) {
        if self.cached_columns.is_empty() || self.src_rows.is_empty() {
            self.column_key_map = vec![None; self.cached_columns.len()];
            return;
        }

        let first_row = &self.src_rows[0];
        let effective_row = extract_effective_row(first_row, self.object_path.as_deref());

        self.column_key_map = if let Some(obj) = effective_row.as_object() {
            self.cached_columns
                .iter()
                .map(|cc| {
                    // attrs is special-cased (returns entire row), no key lookup needed
                    if cc.name == "attrs" {
                        return None;
                    }
                    if obj.contains_key(&cc.name) {
                        Some(KeyMatch::Exact)
                    } else if obj.contains_key(&cc.camel_name) {
                        Some(KeyMatch::CamelCase)
                    } else if let Some(key) = obj.keys().find(|k| k.to_lowercase() == cc.lower_name)
                    {
                        Some(KeyMatch::CaseInsensitive(key.clone()))
                    } else {
                        // Normalized match: strip non-alphanumeric chars and compare.
                        // Handles JSON-LD @-prefixed keys (@id↔_id), dotted names
                        // (user.name↔user_name), and other special-char properties.
                        obj.keys()
                            .find(|k| normalize_to_alnum(k) == cc.alnum_name)
                            .cloned()
                            .map(KeyMatch::CaseInsensitive)
                    }
                })
                .collect()
        } else {
            vec![None; self.cached_columns.len()]
        };
    }

    /// Convert a JSON value to a Cell based on the target PostgreSQL type.
    ///
    /// Handles type coercion, date/time parsing, and numeric conversions.
    pub(crate) fn convert_json_to_cell(
        src: &JsonValue,
        type_oid: &TypeOid,
    ) -> Result<Option<Cell>, FdwError> {
        let cell = match type_oid {
            TypeOid::Bool => src.as_bool().map(Cell::Bool),
            TypeOid::I8 => src
                .as_i64()
                .and_then(|v| i8::try_from(v).ok())
                .map(Cell::I8),
            TypeOid::I16 => src
                .as_i64()
                .and_then(|v| i16::try_from(v).ok())
                .map(Cell::I16),
            TypeOid::I32 => src
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(Cell::I32),
            TypeOid::I64 => src.as_i64().map(Cell::I64),
            #[allow(clippy::cast_possible_truncation)]
            TypeOid::F32 => src.as_f64().map(|v| Cell::F32(v as f32)),
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            TypeOid::Numeric => src.as_f64().map(Cell::Numeric),
            TypeOid::String => Some(Cell::String(
                src.as_str()
                    .map_or_else(|| src.to_string(), ToOwned::to_owned),
            )),
            TypeOid::Date => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(&Self::normalize_datetime(s))?;
                    Some(Cell::Date(ts / 1_000_000))
                } else {
                    // Unix timestamp (seconds since epoch)
                    src.as_i64().map(Cell::Date)
                }
            }
            TypeOid::Timestamp | TypeOid::Timestamptz => {
                let wrap: fn(i64) -> Cell = if matches!(type_oid, TypeOid::Timestamp) {
                    Cell::Timestamp
                } else {
                    Cell::Timestamptz
                };
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(&Self::normalize_datetime(s))?;
                    Some(wrap(ts))
                } else {
                    // Unix timestamp (seconds since epoch) → microseconds
                    src.as_i64()
                        .and_then(|epoch| epoch.checked_mul(1_000_000))
                        .map(wrap)
                }
            }
            TypeOid::Uuid => src.as_str().map(|v| Cell::Uuid(v.to_owned())),
            // Json and unknown types: serialize to JSON string
            TypeOid::Json | TypeOid::Other(_) => Some(Cell::Json(src.to_string())),
        };

        Ok(cell)
    }

    /// Convert a string value from path/query params to a Cell based on target type.
    ///
    /// Used for injecting WHERE clause values that were used as URL parameters.
    pub(crate) fn convert_string_to_cell(value: &str, type_oid: &TypeOid) -> Option<Cell> {
        match type_oid {
            TypeOid::Bool => value.parse::<bool>().ok().map(Cell::Bool),
            TypeOid::I8 => value.parse::<i8>().ok().map(Cell::I8),
            TypeOid::I16 => value.parse::<i16>().ok().map(Cell::I16),
            TypeOid::I32 => value.parse::<i32>().ok().map(Cell::I32),
            TypeOid::I64 => value.parse::<i64>().ok().map(Cell::I64),
            #[allow(clippy::cast_possible_truncation)]
            TypeOid::F32 => value.parse::<f64>().ok().map(|v| Cell::F32(v as f32)),
            TypeOid::F64 => value.parse::<f64>().ok().map(Cell::F64),
            TypeOid::Numeric => value.parse::<f64>().ok().map(Cell::Numeric),
            TypeOid::Date => time::parse_from_rfc3339(&Self::normalize_datetime(value))
                .ok()
                .map(|ts| Cell::Date(ts / 1_000_000)),
            TypeOid::Timestamp | TypeOid::Timestamptz => {
                let wrap: fn(i64) -> Cell = if matches!(type_oid, TypeOid::Timestamp) {
                    Cell::Timestamp
                } else {
                    Cell::Timestamptz
                };
                time::parse_from_rfc3339(&Self::normalize_datetime(value))
                    .ok()
                    .map(wrap)
            }
            TypeOid::Json => Some(Cell::Json(value.to_string())),
            _ => Some(Cell::String(value.to_string())),
        }
    }

    /// Convert a JSON value to a Cell using cached column metadata and pre-resolved key map.
    ///
    /// Uses `CachedColumn` fields instead of WASM resource methods, and the pre-built
    /// `column_key_map` for O(1) JSON key lookup instead of per-row 3-step matching.
    pub(crate) fn json_to_cell_cached(
        &self,
        src_row: &JsonValue,
        col_idx: usize,
    ) -> Result<Option<Cell>, FdwError> {
        let cc = &self.cached_columns[col_idx];

        // Special handling for 'attrs' column - returns entire row as JSON
        if cc.name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        // If this column was used as a query/path parameter, inject the WHERE clause
        // value directly. Coerce to target column type to avoid type mismatches.
        if let Some(value) = self.injected_params.get(&cc.lower_name) {
            let cell = Self::convert_string_to_cell(value, &cc.type_oid);
            return Ok(cell.or_else(|| Some(Cell::String(value.clone()))));
        }

        // Use pre-resolved key from column_key_map for O(1) lookup
        let src = src_row.as_object().and_then(|obj| {
            match self.column_key_map.get(col_idx) {
                Some(Some(KeyMatch::Exact)) => obj.get(&cc.name),
                Some(Some(KeyMatch::CamelCase)) => obj.get(&cc.camel_name),
                Some(Some(KeyMatch::CaseInsensitive(key))) => obj.get(key),
                _ => {
                    // Fallback: 4-step matching for heterogeneous row shapes
                    obj.get(&cc.name)
                        .or_else(|| obj.get(&cc.camel_name))
                        .or_else(|| {
                            obj.iter()
                                .find(|(k, _)| k.to_lowercase() == cc.lower_name)
                                .map(|(_, v)| v)
                        })
                        .or_else(|| {
                            // Normalized: strip non-alnum, compare (handles @-keys, dots, etc.)
                            obj.iter()
                                .find(|(k, _)| normalize_to_alnum(k) == cc.alnum_name)
                                .map(|(_, v)| v)
                        })
                }
            }
        });

        let src = match src {
            Some(v) if !v.is_null() => v,
            _ => return Ok(None),
        };

        Self::convert_json_to_cell(src, &cc.type_oid)
    }
}

#[cfg(test)]
#[path = "column_matching_tests.rs"]
mod tests;
