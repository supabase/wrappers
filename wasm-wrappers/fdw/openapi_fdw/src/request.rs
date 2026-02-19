//! HTTP request building, URL construction, and API communication

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::bindings::supabase::wrappers::{
    http, stats, time,
    types::{Cell, Context, FdwError, FdwResult, Qual, Value},
    utils,
};
use crate::spec::OpenApiSpec;
use crate::{FDW_NAME, OpenApiFdw};

const RETRY_AFTER_HEADER: &str = "retry-after";
pub(crate) const MAX_RETRY_DELAY_MS: u64 = 30_000;

/// Compute retry delay from a Retry-After header value (in seconds), capped to max_delay_ms.
pub(crate) fn retry_delay_from_header(secs: u64, max_delay_ms: u64) -> u64 {
    secs.saturating_mul(1000).min(max_delay_ms)
}

/// Compute exponential backoff delay for a retry attempt, capped to max_delay_ms.
pub(crate) fn exponential_backoff_delay(retry_count: u32, max_delay_ms: u64) -> u64 {
    1000u64.saturating_mul(1 << retry_count).min(max_delay_ms)
}

/// Extract the origin (scheme://authority) from a URL for same-origin comparison.
/// Returns everything up to (but not including) the first / after ://.
fn extract_origin(url: &str) -> &str {
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        if let Some(slash) = rest.find('/') {
            &url[..scheme_end + 3 + slash]
        } else {
            url
        }
    } else {
        url
    }
}

/// Redact a query parameter value from a URL for safe logging.
/// Replaces the value of the named parameter with [REDACTED].
fn redact_query_param(url: &str, param_name: &str) -> String {
    let encoded_prefix = format!("{}=", urlencoding::encode(param_name));
    if let Some(start) = url.find(&encoded_prefix) {
        let value_start = start + encoded_prefix.len();
        let value_end = url[value_start..]
            .find('&')
            .map_or(url.len(), |i| value_start + i);
        format!("{}[REDACTED]{}", &url[..value_start], &url[value_end..])
    } else {
        url.to_string()
    }
}

impl OpenApiFdw {
    /// Fetch and parse the OpenAPI spec
    pub(crate) fn fetch_spec(&mut self) -> Result<(), FdwError> {
        if let Some(ref url) = self.config.spec_url {
            let req = http::Request {
                method: http::Method::Get,
                url: url.clone(),
                headers: self.config.headers.clone(),
                body: String::default(),
            };
            let resp = http::get(&req)?;
            http::error_for_status(&resp).map_err(|_| {
                // Discard opaque error body — may contain URL with credentials
                format!("Failed to fetch OpenAPI spec (HTTP {})", resp.status_code)
            })?;

            if resp.body.len() > self.config.max_response_bytes {
                return Err(format!(
                    "OpenAPI spec too large: {} bytes (limit: {} bytes). \
                     Increase max_response_bytes server option if needed.",
                    resp.body.len(),
                    self.config.max_response_bytes
                ));
            }

            // Try JSON first, fall back to YAML (many OpenAPI specs are published as YAML)
            let spec_json: JsonValue = match serde_json::from_str(&resp.body) {
                Ok(v) => v,
                Err(json_err) => {
                    serde_yaml_ng::from_str::<JsonValue>(&resp.body).map_err(|yaml_err| {
                        format!(
                            "Failed to parse OpenAPI spec as JSON ({json_err}) \
                             or YAML ({yaml_err})"
                        )
                    })?
                }
            };
            let spec = OpenApiSpec::from_json(spec_json)?;

            // Use base_url from spec if not explicitly set
            if self.config.base_url.is_empty() {
                if let Some(url) = spec.base_url() {
                    self.config.base_url = url.trim_end_matches('/').to_string();
                    crate::validate_url(&self.config.base_url, "base_url (from spec servers)")?;
                }
            }

            self.spec = Some(spec);
            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);
        } else if let Some(ref raw_json) = self.config.spec_json {
            if raw_json.len() > self.config.max_response_bytes {
                return Err(format!(
                    "OpenAPI spec_json too large: {} bytes (limit: {} bytes). \
                     Increase max_response_bytes server option if needed.",
                    raw_json.len(),
                    self.config.max_response_bytes
                ));
            }

            let spec_json: JsonValue =
                serde_json::from_str(raw_json).map_err(|e| format!("Invalid spec_json: {e}"))?;
            let spec = OpenApiSpec::from_json(spec_json)?;

            if self.config.base_url.is_empty() {
                if let Some(url) = spec.base_url() {
                    self.config.base_url = url.trim_end_matches('/').to_string();
                    crate::validate_url(&self.config.base_url, "base_url (from spec servers)")?;
                }
            }

            self.spec = Some(spec);
        }
        Ok(())
    }

    /// Extract a qual value as a string
    pub(crate) fn qual_value_to_string(qual: &Qual) -> Option<String> {
        if qual.operator() != "=" {
            return None;
        }
        if let Value::Cell(cell) = qual.value() {
            match cell {
                Cell::String(s) => Some(s),
                Cell::I8(n) => Some(n.to_string()),
                Cell::I16(n) => Some(n.to_string()),
                Cell::I32(n) => Some(n.to_string()),
                Cell::I64(n) => Some(n.to_string()),
                Cell::F32(n) => Some(n.to_string()),
                Cell::F64(n) => Some(n.to_string()),
                Cell::Bool(b) => Some(b.to_string()),
                Cell::Uuid(u) => Some(u),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Resolve a relative or absolute pagination URL against the base URL and endpoint.
    ///
    /// Handles four forms of next_url:
    /// - Absolute URLs (http://..., https://...) -- validated against base_url origin
    /// - Query-only (?page=2) -- resolves against base_url + endpoint
    /// - Absolute paths (/items?page=2) -- resolves against base_url
    /// - Bare relative paths (page/2) -- resolves against base_url/
    ///
    /// Returns an error if an absolute pagination URL points to a different origin
    /// than base_url, which would leak authentication credentials to a third party.
    pub(crate) fn resolve_pagination_url(&self, next_url: &str) -> Result<String, String> {
        if next_url.starts_with("http://") || next_url.starts_with("https://") {
            let next_origin = extract_origin(next_url);
            let base_origin = extract_origin(&self.config.base_url);
            if !next_origin.eq_ignore_ascii_case(base_origin) {
                return Err(format!(
                    "Pagination URL origin mismatch: API returned '{next_origin}' \
                     but base_url is '{base_origin}'. Cross-origin pagination URLs are \
                     rejected to prevent credential leakage. If this API legitimately \
                     uses a different host for pagination, set base_url to match \
                     the pagination host."
                ));
            }
            Ok(next_url.to_string())
        } else if next_url.starts_with('?') {
            // Use resolved_endpoint (post path-param substitution) if available,
            // falling back to the template for endpoints without path params.
            let ep = if self.resolved_endpoint.is_empty() {
                &self.endpoint
            } else {
                &self.resolved_endpoint
            };
            let endpoint_base = ep.split('?').next().unwrap_or(ep);
            Ok(format!("{}{endpoint_base}{next_url}", self.config.base_url))
        } else if next_url.starts_with('/') {
            // Use only the origin (scheme://host) to avoid duplicating any
            // path prefix that base_url may contain (e.g. /v1).
            Ok(format!(
                "{}{next_url}",
                extract_origin(&self.config.base_url)
            ))
        } else {
            Ok(format!("{}/{next_url}", self.config.base_url))
        }
    }

    /// Substitute path parameters in endpoint template from quals.
    ///
    /// Writes substituted values into injected so they can be re-injected
    /// into result rows (ensuring PostgreSQL's post-filter passes).
    ///
    /// Returns (resolved_endpoint, path_params_used) where path_params_used
    /// contains lowercase names of parameters that were substituted.
    ///
    /// # Errors
    /// Returns an error if required path parameters are missing from quals.
    pub(crate) fn substitute_path_params(
        endpoint: &str,
        quals: &[Qual],
        injected: &mut HashMap<String, String>,
    ) -> Result<(String, Vec<String>), String> {
        if !endpoint.contains('{') {
            return Ok((endpoint.to_string(), Vec::new()));
        }

        // Build a map of qual field -> value for path parameter substitution
        // Pre-allocate for 2 entries per qual (original + lowercase key)
        let mut qual_map: HashMap<String, String> = HashMap::with_capacity(quals.len() * 2);
        for qual in quals {
            if let Some(value) = Self::qual_value_to_string(qual) {
                // Store both original and lowercase versions for flexible matching
                qual_map.insert(qual.field().to_lowercase(), value.clone());
                qual_map.insert(qual.field(), value);
            }
        }

        let mut endpoint = endpoint.to_string();
        let mut path_params_used: Vec<String> = Vec::new();
        let mut missing_params: Vec<String> = Vec::new();

        // Find all {param} patterns and substitute
        while let Some(start) = endpoint.find('{') {
            if let Some(end) = endpoint[start..].find('}') {
                let param_name = &endpoint[start + 1..start + end];
                let param_lower = param_name.to_lowercase();

                // Try to find matching qual (case-insensitive)
                let value = qual_map
                    .get(&param_lower)
                    .or_else(|| qual_map.get(param_name));

                if let Some(val) = value {
                    path_params_used.push(param_lower.clone());
                    // Store the path param for injection into rows (unencoded for PostgreSQL filter)
                    injected.insert(param_lower, val.clone());
                    endpoint = format!(
                        "{}{}{}",
                        &endpoint[..start],
                        urlencoding::encode(val),
                        &endpoint[start + end + 1..]
                    );
                } else {
                    // Track missing parameter and remove the {param} placeholder to continue
                    // parsing. This is safe because OpenAPI path params are always separated
                    // by '/' (e.g., /{a}/{b}), so removing one doesn't mangle the next.
                    missing_params.push(param_name.to_string());
                    endpoint = format!("{}{}", &endpoint[..start], &endpoint[start + end + 1..]);
                }
            } else {
                return Err(format!("Unclosed '{{' in endpoint template: {endpoint}"));
            }
        }

        // Return error if any required path parameters are missing
        if !missing_params.is_empty() {
            return Err(format!(
                "Missing required path parameter(s) in WHERE clause: {}. \
                 Add WHERE {} to your query.",
                missing_params.join(", "),
                missing_params
                    .iter()
                    .map(|p| format!("{p} = '<value>'"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ));
        }

        Ok((endpoint, path_params_used))
    }

    /// Build query parameters from pagination state, quals, and API key.
    ///
    /// Returns (url_params, injected_entries) where injected_entries are
    /// qual values to merge into self.injected_params for row injection.
    /// Excludes path parameters and rowid column.
    pub(crate) fn build_query_params(
        &self,
        quals: &[Qual],
        path_params_used: &[String],
    ) -> (Vec<String>, Vec<(String, String)>) {
        // Pre-allocate for cursor + page_size + quals + api_key
        let mut params = Vec::with_capacity(quals.len() + 3);
        let mut injected_entries = Vec::new();

        // Add pagination cursor if we have one
        if let Some(cursor) = self.pagination.next.as_ref().and_then(|t| t.as_cursor()) {
            params.push(format!(
                "{}={}",
                urlencoding::encode(&self.config.cursor_param),
                urlencoding::encode(cursor)
            ));
        }

        // Add page size if configured, reduced by LIMIT when available
        if self.config.page_size > 0 && !self.config.page_size_param.is_empty() {
            let effective_size = match self.src_limit {
                Some(limit) if limit > 0 => self.config.page_size.min(limit as usize),
                _ => self.config.page_size,
            };
            params.push(format!(
                "{}={}",
                urlencoding::encode(&self.config.page_size_param),
                effective_size
            ));
        }

        // Add remaining quals as query params (exclude path params and rowid)
        for qual in quals {
            let field_lower = qual.field().to_lowercase();

            // Skip if used as path param
            if path_params_used.contains(&field_lower) {
                continue;
            }

            // Skip the rowid column
            if field_lower == self.rowid_col {
                continue;
            }

            if let Some(value) = Self::qual_value_to_string(qual) {
                // Track for injection back into rows
                // (so PostgreSQL's WHERE filter passes even if the API doesn't echo it back)
                injected_entries.push((field_lower, value.clone()));
                params.push(format!(
                    "{}={}",
                    urlencoding::encode(&qual.field()),
                    urlencoding::encode(&value)
                ));
            }
        }

        // Add API key as query parameter if configured
        if let Some((ref param_name, ref param_value)) = self.config.api_key_query {
            params.push(format!(
                "{}={}",
                urlencoding::encode(param_name),
                urlencoding::encode(param_value)
            ));
        }

        (params, injected_entries)
    }

    /// Build the URL for a request, handling path parameters and pagination.
    ///
    /// Updates self.injected_params in place (avoids cloning on pagination).
    ///
    /// Supports endpoint templates like:
    /// - /users/{user_id}/posts
    /// - /projects/{org}/{repo}/issues
    /// - /resources/{type}/{id}
    ///
    /// Path parameters are substituted from WHERE clause quals.
    ///
    /// # Errors
    /// Returns an error if required path parameters are missing from the WHERE clause.
    pub(crate) fn build_url(&mut self, ctx: &Context) -> Result<String, String> {
        // Use next_url for pagination if available (injected_params unchanged)
        if let Some(next_url) = self.pagination.next.as_ref().and_then(|t| t.as_url()) {
            let mut url = self.resolve_pagination_url(next_url)?;
            if let Some((ref param_name, ref param_value)) = self.config.api_key_query {
                let separator = if url.contains('?') { '&' } else { '?' };
                url.push(separator);
                url.push_str(&format!(
                    "{}={}",
                    urlencoding::encode(param_name),
                    urlencoding::encode(param_value)
                ));
            }
            return Ok(url);
        }

        let quals = ctx.get_quals();

        // Substitute path parameters (no self borrow — takes &mut injected_params directly)
        let (endpoint, path_params_used) =
            Self::substitute_path_params(&self.endpoint, &quals, &mut self.injected_params)?;

        // Store resolved endpoint for pagination (query-only URLs need the
        // substituted path, not the raw template with {param} placeholders).
        self.resolved_endpoint = endpoint.clone();

        // Check for rowid pushdown for single-resource access
        // Only if endpoint doesn't already have path params and rowid qual exists
        if path_params_used.is_empty() {
            if let Some(id_qual) = quals
                .iter()
                .find(|q| q.field().to_lowercase() == self.rowid_col && q.operator() == "=")
            {
                if let Some(id) = Self::qual_value_to_string(id_qual) {
                    self.injected_params
                        .insert(self.rowid_col.clone(), id.clone());
                    return Ok(format!(
                        "{}{}/{}",
                        self.config.base_url,
                        endpoint,
                        urlencoding::encode(&id)
                    ));
                }
            }
        }

        // Build query parameters
        let (params, injected_entries) = self.build_query_params(&quals, &path_params_used);
        self.injected_params.extend(injected_entries);

        // Assemble final URL
        let mut url = format!("{}{}", self.config.base_url, endpoint);
        if !params.is_empty() {
            let separator = if url.contains('?') { '&' } else { '?' };
            url.push(separator);
            url.push_str(&params.join("&"));
        }

        Ok(url)
    }

    /// Make a request to the API with automatic rate limit handling
    pub(crate) fn make_request(&mut self, ctx: &Context) -> FdwResult {
        let url = self.build_url(ctx)?;

        let req = http::Request {
            method: self.method,
            url,
            headers: self.config.headers.clone(),
            body: self.request_body.clone(),
        };

        // Retry loop for transient errors (HTTP 429 rate limit, 502/503 server errors)
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 3;

        let resp = loop {
            let resp = match req.method {
                http::Method::Post => http::post(&req)?,
                _ => http::get(&req)?,
            };

            let is_retryable = matches!(resp.status_code, 429 | 502 | 503);
            if is_retryable {
                if retry_count >= MAX_RETRIES {
                    let hint = if resp.status_code == 429 {
                        " Consider adding a page_size option to reduce request frequency."
                    } else {
                        ""
                    };
                    return Err(format!(
                        "API request failed with HTTP {} after {MAX_RETRIES} retries.{hint}",
                        resp.status_code
                    ));
                }

                // Try to get retry delay from Retry-After header (case-insensitive),
                // capped to prevent absurdly long waits from malicious/buggy servers
                let delay_ms = resp
                    .headers
                    .iter()
                    .find(|h| h.0.to_lowercase() == RETRY_AFTER_HEADER)
                    .and_then(|h| h.1.parse::<u64>().ok())
                    .map(|secs| retry_delay_from_header(secs, MAX_RETRY_DELAY_MS))
                    .unwrap_or_else(|| exponential_backoff_delay(retry_count, MAX_RETRY_DELAY_MS));

                time::sleep(delay_ms);
                retry_count += 1;
                continue;
            }

            break resp;
        };

        if self.config.debug {
            let log_url = match self.config.api_key_query {
                Some((ref param_name, _)) => redact_query_param(&req.url, param_name),
                None => req.url.clone(),
            };
            utils::report_info(&format!(
                "[openapi_fdw] HTTP {} {} -> {} ({} bytes)",
                if matches!(req.method, http::Method::Post) {
                    "POST"
                } else {
                    "GET"
                },
                log_url,
                resp.status_code,
                resp.body.len()
            ));
        }

        // Handle 404 as empty result (no matching resource)
        if resp.status_code == 404 {
            self.src_rows = Vec::new();
            self.src_idx = 0;
            self.pagination.clear_next();
            return Ok(());
        }

        http::error_for_status(&resp).map_err(|_| {
            // Discard the opaque error body from error_for_status — it may
            // contain the full request URL, which leaks API key query params
            // when api_key_location = 'query'.
            format!(
                "HTTP {} error from API endpoint ({})",
                resp.status_code,
                self.endpoint.split('?').next().unwrap_or(&self.endpoint)
            )
        })?;

        if resp.body.len() > self.config.max_response_bytes {
            return Err(format!(
                "Response body too large: {} bytes (limit: {} bytes). \
                 Increase max_response_bytes server option if needed.",
                resp.body.len(),
                self.config.max_response_bytes
            ));
        }

        let mut resp_json: JsonValue =
            serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

        // Handle pagination before extracting data (borrows resp_json)
        self.handle_pagination(&resp_json);

        // Extract data by taking ownership (avoids cloning the array)
        self.src_rows = self.extract_data(&mut resp_json)?;
        self.src_idx = 0;

        // Build column key map for O(1) lookups during iter_scan
        self.build_column_key_map();

        // Debug: warn once if object_path doesn't match response structure
        if self.config.debug {
            if let Some(ref path) = self.object_path {
                if let Some(first_row) = self.src_rows.first() {
                    if first_row.pointer(path).is_none() {
                        utils::report_info(&format!(
                            "[openapi_fdw] object_path '{path}' not found in response. \
                             Falling back to full row object."
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
