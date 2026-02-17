//! Server configuration: headers and authentication setup

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::bindings::supabase::wrappers::{
    types::{FdwResult, Options},
    utils,
};

pub(crate) const DEFAULT_MAX_PAGES: usize = 1000;
pub(crate) const DEFAULT_MAX_RESPONSE_BYTES: usize = 50 * 1024 * 1024; // 50 MiB

/// Server-level configuration.
///
/// Fields are set once in init() from server options. A few fields
/// (page_size, page_size_param, cursor_param) can be overridden
/// per-table in begin_scan; call save_pagination_defaults() after
/// init and restore_pagination_defaults() at the start of each scan.
pub(crate) struct ServerConfig {
    pub(crate) base_url: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) spec_url: Option<String>,
    pub(crate) spec_json: Option<String>,
    pub(crate) api_key_query: Option<(String, String)>,
    pub(crate) include_attrs: bool,
    pub(crate) page_size: usize,
    pub(crate) page_size_param: String,
    pub(crate) cursor_param: String,
    pub(crate) max_pages: usize,
    pub(crate) max_response_bytes: usize,
    pub(crate) debug: bool,

    // Server-level defaults (saved after init, restored in begin_scan)
    pub(crate) default_page_size: usize,
    pub(crate) default_page_size_param: String,
    pub(crate) default_cursor_param: String,
}

/// Manual Debug impl to redact authentication secrets (headers may contain
/// API keys or bearer tokens, and api_key_query contains the raw key value).
impl std::fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerConfig")
            .field("base_url", &self.base_url)
            .field("headers", &format!("[{} header(s)]", self.headers.len()))
            .field("spec_url", &self.spec_url)
            .field(
                "spec_json",
                &self
                    .spec_json
                    .as_ref()
                    .map(|s| format!("[{} bytes]", s.len())),
            )
            .field(
                "api_key_query",
                &self
                    .api_key_query
                    .as_ref()
                    .map(|(k, _)| format!("{k}=[REDACTED]")),
            )
            .field("include_attrs", &self.include_attrs)
            .field("page_size", &self.page_size)
            .field("page_size_param", &self.page_size_param)
            .field("cursor_param", &self.cursor_param)
            .field("max_pages", &self.max_pages)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("debug", &self.debug)
            .finish()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            headers: Vec::new(),
            spec_url: None,
            spec_json: None,
            api_key_query: None,
            include_attrs: true,
            page_size: 0,
            page_size_param: String::new(),
            cursor_param: String::new(),
            max_pages: DEFAULT_MAX_PAGES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            debug: false,
            default_page_size: 0,
            default_page_size_param: String::new(),
            default_cursor_param: String::new(),
        }
    }
}

impl ServerConfig {
    /// Snapshot the current pagination fields as server-level defaults.
    ///
    /// Call once at the end of init(), after server options are parsed.
    pub(crate) fn save_pagination_defaults(&mut self) {
        self.default_page_size = self.page_size;
        self.default_page_size_param
            .clone_from(&self.page_size_param);
        self.default_cursor_param.clone_from(&self.cursor_param);
    }

    /// Restore pagination fields to server-level defaults.
    ///
    /// Call at the start of each begin_scan(), before applying table-level overrides.
    pub(crate) fn restore_pagination_defaults(&mut self) {
        self.page_size = self.default_page_size;
        self.page_size_param
            .clone_from(&self.default_page_size_param);
        self.cursor_param.clone_from(&self.default_cursor_param);
    }

    /// Configure request headers from server options
    pub(crate) fn configure_headers(&mut self, opts: &Options) -> FdwResult {
        self.apply_headers(
            opts.get("user_agent"),
            opts.get("accept"),
            opts.get("headers"),
        )
    }

    /// Apply header configuration from extracted option values.
    ///
    /// Separated from configure_headers for testability (Options is a WASM resource).
    pub(crate) fn apply_headers(
        &mut self,
        user_agent: Option<String>,
        accept: Option<String>,
        headers_json: Option<String>,
    ) -> FdwResult {
        self.headers
            .push(("content-type".to_owned(), "application/json".to_string()));

        if let Some(user_agent) = user_agent {
            self.headers.push(("user-agent".to_owned(), user_agent));
        }

        if let Some(accept) = accept {
            self.headers.push(("accept".to_owned(), accept));
        }

        if let Some(headers_json) = headers_json {
            let headers: JsonMap<String, JsonValue> = serde_json::from_str(&headers_json)
                .map_err(|e| format!("Invalid JSON for 'headers' option: {e}"))?;
            for (key, value) in headers {
                if let Some(v) = value.as_str() {
                    let key_lower = key.to_lowercase();
                    // Replace existing header with same name to avoid duplicates
                    // (e.g., custom content-type overrides the default)
                    if let Some(existing) = self.headers.iter_mut().find(|h| h.0 == key_lower) {
                        existing.1 = v.to_string();
                    } else {
                        self.headers.push((key_lower, v.to_string()));
                    }
                } else {
                    return Err(format!(
                        "Invalid non-string value for header '{key}' in 'headers' option"
                    ));
                }
            }
        }

        Ok(())
    }

    /// Configure authentication from server options
    pub(crate) fn configure_auth(&mut self, opts: &Options) -> FdwResult {
        let api_key = opts.get("api_key").or_else(|| {
            opts.get("api_key_id")
                .and_then(|key_id| utils::get_vault_secret(&key_id))
        });

        let bearer_token = opts.get("bearer_token").or_else(|| {
            opts.get("bearer_token_id")
                .and_then(|token_id| utils::get_vault_secret(&token_id))
        });

        // Warn on empty credentials (likely vault misconfiguration)
        if let Some(ref key) = api_key {
            if key.trim().is_empty() {
                utils::report_warning(
                    "[openapi_fdw] api_key is empty. Requests may fail authentication.",
                );
            }
        }
        if let Some(ref token) = bearer_token {
            if token.trim().is_empty() {
                utils::report_warning(
                    "[openapi_fdw] bearer_token is empty. Requests may fail authentication.",
                );
            }
        }

        let location = opts.require_or("api_key_location", "header");
        let header = opts.require_or("api_key_header", "Authorization");
        let prefix = opts.get("api_key_prefix");

        self.apply_auth(api_key, bearer_token, &location, &header, prefix)?;

        // Warn if query auth uses the default header name (likely misconfiguration)
        if location == "query" && header == "Authorization" && self.api_key_query.is_some() {
            utils::report_warning(
                "[openapi_fdw] api_key_location is 'query' but api_key_header \
                 is the default 'Authorization'. This will send ?Authorization=<key> \
                 as a query parameter, which is likely incorrect. Set api_key_header \
                 to the actual query parameter name (e.g., 'api_key' or 'key').",
            );
        }

        Ok(())
    }

    /// Apply authentication configuration from extracted option values.
    ///
    /// Separated from configure_auth for testability (Options is a WASM resource).
    pub(crate) fn apply_auth(
        &mut self,
        api_key: Option<String>,
        bearer_token: Option<String>,
        api_key_location: &str,
        api_key_header: &str,
        api_key_prefix: Option<String>,
    ) -> FdwResult {
        // Filter out empty/whitespace-only credentials (likely vault misconfiguration).
        // The warning is emitted upstream by configure_auth; here we just skip them
        // to avoid sending meaningless auth headers (e.g., "Bearer ").
        let api_key = api_key.filter(|k| !k.trim().is_empty());
        let bearer_token = bearer_token.filter(|t| !t.trim().is_empty());

        // Enforce mutual exclusivity — both would emit duplicate auth headers
        if api_key.is_some() && bearer_token.is_some() {
            return Err(
                "Cannot use both api_key/api_key_id and bearer_token/bearer_token_id. \
                 Choose one authentication method."
                    .to_string(),
            );
        }

        if let Some(key) = api_key {
            match api_key_location {
                "query" => {
                    // API key sent as query parameter (e.g., ?api_key=xxx)
                    self.api_key_query = Some((api_key_header.to_string(), key));
                }
                "cookie" => {
                    // API key sent as cookie (e.g., Cookie: session=xxx)
                    self.headers
                        .push(("cookie".to_owned(), format!("{api_key_header}={key}")));
                }
                "header" => {
                    // API key sent as header (default)
                    let header_value = match (api_key_header, &api_key_prefix) {
                        ("Authorization", None) => format!("Bearer {key}"),
                        (_, Some(p)) => format!("{p} {key}"),
                        (_, None) => key,
                    };

                    self.headers
                        .push((api_key_header.to_lowercase(), header_value));
                }
                other => {
                    return Err(format!(
                        "Invalid api_key_location '{other}'. \
                         Must be 'header', 'query', or 'cookie'."
                    ));
                }
            }
        }

        if let Some(token) = bearer_token {
            self.headers
                .push(("authorization".to_owned(), format!("Bearer {token}")));
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
