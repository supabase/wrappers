//! Response parsing: data extraction and pagination handling

use serde_json::Value as JsonValue;

use crate::OpenApiFdw;
use crate::bindings::supabase::wrappers::types::FdwError;
use crate::pagination::PaginationToken;

/// Common wrapper keys for auto-detecting the data array in API responses.
pub(crate) const WRAPPER_KEYS: &[&str] = &[
    "data", "results", "items", "records", "entries", "features", "@graph",
];

/// JSON pointer paths to check for a next-page URL.
const NEXT_URL_PATHS: &[&str] = &[
    "/meta/pagination/next",
    "/meta/pagination/next_url",
    "/pagination/next",
    "/pagination/next_url",
    "/links/next",
    "/links/next_url",
    "/next",
    "/next_url",
    "/_links/next/href",
];

/// JSON pointer paths to check for a boolean "has more pages" flag.
const HAS_MORE_PATHS: &[&str] = &[
    "/meta/pagination/has_more",
    "/has_more",
    "/pagination/has_more",
];

/// JSON pointer paths to check for a next-page cursor value.
const CURSOR_PATHS: &[&str] = &[
    "/meta/pagination/next_cursor",
    "/pagination/next_cursor",
    "/next_cursor",
    "/cursor",
];

impl OpenApiFdw {
    /// Extract the data array from the response, taking ownership to avoid cloning
    pub(crate) fn extract_data(&self, resp: &mut JsonValue) -> Result<Vec<JsonValue>, FdwError> {
        // If response_path is specified, use it
        if let Some(ref path) = self.response_path {
            if let Some(data) = resp.pointer_mut(path).map(JsonValue::take) {
                return Self::json_to_rows(data);
            }
            // response_path not found — fall through to auto-detection
            // (common when rowid lookup returns single object instead of collection)
        }

        // Direct array response
        if resp.is_array() {
            return Self::json_to_rows(resp.take());
        }

        // Try common wrapper patterns
        if resp.is_object() {
            for key in WRAPPER_KEYS {
                if resp
                    .get(*key)
                    .is_some_and(|d| d.is_array() || d.is_object())
                {
                    return Self::json_to_rows(resp[*key].take());
                }
            }

            // Single object response
            return Ok(vec![resp.take()]);
        }

        Err(format!(
            "Unable to extract data from response (type: {}). \
             Expected an array, object with a known wrapper key \
             ({}), \
             or set response_path in table options.",
            match resp {
                JsonValue::Null => "null",
                JsonValue::Bool(_) => "boolean",
                JsonValue::Number(_) => "number",
                JsonValue::String(_) => "string",
                _ => "unknown",
            },
            WRAPPER_KEYS.join(", "),
        ))
    }

    /// Convert a JSON value to a vector of row objects (takes ownership, no cloning)
    pub(crate) fn json_to_rows(data: JsonValue) -> Result<Vec<JsonValue>, FdwError> {
        match data {
            JsonValue::Array(arr) => Ok(arr),
            data if data.is_object() => Ok(vec![data]),
            _ => Err(format!(
                "Response data is not an array or object (got {})",
                match &data {
                    JsonValue::Null => "null",
                    JsonValue::Bool(_) => "boolean",
                    JsonValue::Number(_) => "number",
                    JsonValue::String(_) => "string",
                    _ => "unknown",
                }
            )),
        }
    }

    /// Handle pagination from the response
    pub(crate) fn handle_pagination(&mut self, resp: &JsonValue) {
        self.pagination.clear_next();

        // Try configured cursor path first
        if !self.cursor_path.is_empty() {
            if let Some(value) = Self::extract_non_empty_string(resp, &self.cursor_path) {
                if value.starts_with("http://") || value.starts_with("https://") {
                    self.pagination.next = Some(PaginationToken::Url(value));
                } else {
                    self.pagination.next = Some(PaginationToken::Cursor(value));
                }
                return;
            }
        }

        // Only try auto-detection for object responses
        if resp.as_object().is_none() {
            return;
        }

        // Check for next URL in common locations
        for path in NEXT_URL_PATHS {
            if let Some(url) = Self::extract_non_empty_string(resp, path) {
                self.pagination.next = Some(PaginationToken::Url(url));
                return;
            }
        }

        // Check for has_more flag with cursor
        let has_more = HAS_MORE_PATHS
            .iter()
            .find_map(|p| resp.pointer(p))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);

        if !has_more {
            return;
        }

        // Find next cursor
        for path in CURSOR_PATHS {
            if let Some(cursor) = Self::extract_non_empty_string(resp, path) {
                self.pagination.next = Some(PaginationToken::Cursor(cursor));
                return;
            }
        }
    }

    /// Extract a non-empty string from a JSON pointer path
    pub(crate) fn extract_non_empty_string(json: &JsonValue, path: &str) -> Option<String> {
        json.pointer(path)
            .and_then(JsonValue::as_str)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
    }
}

#[cfg(test)]
#[path = "response_tests.rs"]
mod tests;
