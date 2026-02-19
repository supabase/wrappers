use super::{DEFAULT_MAX_PAGES, DEFAULT_MAX_RESPONSE_BYTES, ServerConfig};

// --- Default values ---

#[test]
fn test_default_max_pages() {
    let config = ServerConfig::default();
    assert_eq!(config.max_pages, DEFAULT_MAX_PAGES);
}

#[test]
fn test_default_max_response_bytes() {
    let config = ServerConfig::default();
    assert_eq!(config.max_response_bytes, DEFAULT_MAX_RESPONSE_BYTES);
}

#[test]
fn test_default_page_size_zero() {
    let config = ServerConfig::default();
    assert_eq!(config.page_size, 0);
}

#[test]
fn test_default_no_api_key_query() {
    let config = ServerConfig::default();
    assert!(config.api_key_query.is_none());
}

#[test]
fn test_default_empty_headers() {
    let config = ServerConfig::default();
    assert!(config.headers.is_empty());
}

#[test]
fn test_default_debug_off() {
    let config = ServerConfig::default();
    assert!(!config.debug);
}

#[test]
fn test_default_include_attrs_on() {
    let config = ServerConfig::default();
    assert!(config.include_attrs);
}

// --- save_pagination_defaults / restore_pagination_defaults ---

#[test]
fn test_save_and_restore_pagination_defaults() {
    let mut config = ServerConfig {
        page_size: 50,
        page_size_param: "per_page".to_string(),
        cursor_param: "cursor".to_string(),
        ..Default::default()
    };
    config.save_pagination_defaults();

    // Override with table-level values
    config.page_size = 100;
    config.page_size_param = "limit".to_string();
    config.cursor_param = "next".to_string();

    // Restore should bring back server defaults
    config.restore_pagination_defaults();
    assert_eq!(config.page_size, 50);
    assert_eq!(config.page_size_param, "per_page");
    assert_eq!(config.cursor_param, "cursor");
}

#[test]
fn test_restore_before_save_uses_default_zeros() {
    let mut config = ServerConfig {
        page_size: 100,
        page_size_param: "limit".to_string(),
        ..Default::default()
    };

    // Restore without save → restores to Default::default() values
    config.restore_pagination_defaults();
    assert_eq!(config.page_size, 0);
    assert_eq!(config.page_size_param, "");
    assert_eq!(config.cursor_param, "");
}

#[test]
fn test_save_pagination_defaults_idempotent() {
    let mut config = ServerConfig {
        page_size: 25,
        page_size_param: "limit".to_string(),
        cursor_param: "after".to_string(),
        ..Default::default()
    };

    config.save_pagination_defaults();
    config.save_pagination_defaults(); // Second save should be same

    config.page_size = 999;
    config.restore_pagination_defaults();
    assert_eq!(config.page_size, 25);
}

#[test]
fn test_multiple_save_restore_cycles() {
    let mut config = ServerConfig {
        page_size: 10,
        page_size_param: "limit".to_string(),
        cursor_param: "after".to_string(),
        ..Default::default()
    };
    config.save_pagination_defaults();

    // Cycle 1: override and restore
    config.page_size = 50;
    config.page_size_param = "per_page".to_string();
    config.restore_pagination_defaults();
    assert_eq!(config.page_size, 10);
    assert_eq!(config.page_size_param, "limit");

    // Cycle 2: different override and restore
    config.page_size = 200;
    config.cursor_param = "next_token".to_string();
    config.restore_pagination_defaults();
    assert_eq!(config.page_size, 10);
    assert_eq!(config.cursor_param, "after");
}

#[test]
fn test_restore_does_not_affect_non_pagination_fields() {
    let mut config = ServerConfig {
        base_url: "https://api.example.com".to_string(),
        max_pages: 500,
        debug: true,
        page_size: 25,
        ..Default::default()
    };
    config.save_pagination_defaults();

    config.base_url = "https://other.example.com".to_string();
    config.max_pages = 100;
    config.debug = false;

    config.restore_pagination_defaults();
    // Non-pagination fields should be unchanged
    assert_eq!(config.base_url, "https://other.example.com");
    assert_eq!(config.max_pages, 100);
    assert!(!config.debug);
    // Pagination fields should be restored
    assert_eq!(config.page_size, 25);
}

// --- apply_headers ---

#[test]
fn test_apply_headers_content_type_always_added() {
    let mut config = ServerConfig::default();
    config.apply_headers(None, None, None).unwrap();
    assert_eq!(config.headers.len(), 1);
    assert_eq!(config.headers[0].0, "content-type");
    assert_eq!(config.headers[0].1, "application/json");
}

#[test]
fn test_apply_headers_with_user_agent() {
    let mut config = ServerConfig::default();
    config
        .apply_headers(Some("MyApp/1.0".to_string()), None, None)
        .unwrap();
    assert_eq!(config.headers.len(), 2);
    assert_eq!(config.headers[1].0, "user-agent");
    assert_eq!(config.headers[1].1, "MyApp/1.0");
}

#[test]
fn test_apply_headers_with_accept() {
    let mut config = ServerConfig::default();
    config
        .apply_headers(None, Some("application/geo+json".to_string()), None)
        .unwrap();
    assert_eq!(config.headers.len(), 2);
    assert_eq!(config.headers[1].0, "accept");
    assert_eq!(config.headers[1].1, "application/geo+json");
}

#[test]
fn test_apply_headers_with_user_agent_and_accept() {
    let mut config = ServerConfig::default();
    config
        .apply_headers(
            Some("Bot/2.0".to_string()),
            Some("text/xml".to_string()),
            None,
        )
        .unwrap();
    assert_eq!(config.headers.len(), 3);
    assert_eq!(config.headers[1].0, "user-agent");
    assert_eq!(config.headers[1].1, "Bot/2.0");
    assert_eq!(config.headers[2].0, "accept");
    assert_eq!(config.headers[2].1, "text/xml");
}

#[test]
fn test_apply_headers_custom_json() {
    let mut config = ServerConfig::default();
    config
        .apply_headers(
            None,
            None,
            Some(r#"{"X-Custom": "value1", "Feature-Flag": "beta"}"#.to_string()),
        )
        .unwrap();
    // content-type + 2 custom headers
    assert_eq!(config.headers.len(), 3);
    // Custom headers should be lowercased
    let custom_headers: Vec<_> = config.headers[1..].to_vec();
    assert!(
        custom_headers
            .iter()
            .any(|(k, v)| k == "x-custom" && v == "value1")
    );
    assert!(
        custom_headers
            .iter()
            .any(|(k, v)| k == "feature-flag" && v == "beta")
    );
}

#[test]
fn test_apply_headers_custom_json_lowercases_keys() {
    let mut config = ServerConfig::default();
    config
        .apply_headers(
            None,
            None,
            Some(r#"{"X-API-KEY": "secret123"}"#.to_string()),
        )
        .unwrap();
    assert_eq!(config.headers[1].0, "x-api-key");
    assert_eq!(config.headers[1].1, "secret123");
}

#[test]
fn test_apply_headers_invalid_json() {
    let mut config = ServerConfig::default();
    let result = config.apply_headers(None, None, Some("not valid json".to_string()));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Invalid JSON"));
}

#[test]
fn test_apply_headers_non_string_value_error() {
    let mut config = ServerConfig::default();
    let result = config.apply_headers(None, None, Some(r#"{"X-Count": 42}"#.to_string()));
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("non-string value"));
    assert!(err.contains("X-Count"));
}

#[test]
fn test_apply_headers_empty_json_object() {
    let mut config = ServerConfig::default();
    config
        .apply_headers(None, None, Some("{}".to_string()))
        .unwrap();
    // Only content-type, no custom headers
    assert_eq!(config.headers.len(), 1);
}

#[test]
fn test_apply_headers_boolean_value_error() {
    let mut config = ServerConfig::default();
    let result = config.apply_headers(None, None, Some(r#"{"X-Debug": true}"#.to_string()));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("non-string value"));
}

#[test]
fn test_apply_headers_array_value_error() {
    let mut config = ServerConfig::default();
    let result = config.apply_headers(None, None, Some(r#"{"X-Tags": ["a", "b"]}"#.to_string()));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("non-string value"));
}

#[test]
fn test_apply_headers_all_options_combined() {
    let mut config = ServerConfig::default();
    config
        .apply_headers(
            Some("MyApp/1.0".to_string()),
            Some("application/json".to_string()),
            Some(r#"{"X-Request-Id": "abc123"}"#.to_string()),
        )
        .unwrap();
    assert_eq!(config.headers.len(), 4);
    assert_eq!(
        config.headers[0],
        ("content-type".to_string(), "application/json".to_string())
    );
    assert_eq!(
        config.headers[1],
        ("user-agent".to_string(), "MyApp/1.0".to_string())
    );
    assert_eq!(
        config.headers[2],
        ("accept".to_string(), "application/json".to_string())
    );
    assert_eq!(
        config.headers[3],
        ("x-request-id".to_string(), "abc123".to_string())
    );
}

// --- apply_headers: Deduplication ---

#[test]
fn test_apply_headers_custom_content_type_replaces_default() {
    let mut config = ServerConfig::default();
    config
        .apply_headers(
            None,
            None,
            Some(r#"{"Content-Type": "text/xml"}"#.to_string()),
        )
        .unwrap();
    // Custom content-type should replace the default, not add a duplicate
    assert_eq!(config.headers.len(), 1);
    assert_eq!(config.headers[0].0, "content-type");
    assert_eq!(config.headers[0].1, "text/xml");
}

#[test]
fn test_apply_headers_custom_header_does_not_duplicate() {
    let mut config = ServerConfig::default();
    config
        .apply_headers(
            Some("MyApp/1.0".to_string()),
            None,
            Some(r#"{"User-Agent": "CustomBot/2.0"}"#.to_string()),
        )
        .unwrap();
    // Custom user-agent should replace the one set via option, not duplicate
    assert_eq!(config.headers.len(), 2); // content-type + user-agent
    let ua = config.headers.iter().find(|h| h.0 == "user-agent").unwrap();
    assert_eq!(ua.1, "CustomBot/2.0");
}

// --- apply_auth: API key as header (default) ---

#[test]
fn test_auth_api_key_default_header_authorization() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            Some("my-api-key".to_string()),
            None,
            "header",
            "Authorization",
            None,
        )
        .unwrap();
    assert_eq!(config.headers.len(), 1);
    assert_eq!(config.headers[0].0, "authorization");
    assert_eq!(config.headers[0].1, "Bearer my-api-key");
}

#[test]
fn test_auth_api_key_custom_header() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            Some("key123".to_string()),
            None,
            "header",
            "X-API-Key",
            None,
        )
        .unwrap();
    assert_eq!(config.headers.len(), 1);
    assert_eq!(config.headers[0].0, "x-api-key");
    assert_eq!(config.headers[0].1, "key123"); // No "Bearer" prefix for non-Authorization headers
}

#[test]
fn test_auth_api_key_custom_header_with_prefix() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            Some("key123".to_string()),
            None,
            "header",
            "X-API-Key",
            Some("Token".to_string()),
        )
        .unwrap();
    assert_eq!(config.headers[0].0, "x-api-key");
    assert_eq!(config.headers[0].1, "Token key123");
}

#[test]
fn test_auth_api_key_authorization_with_custom_prefix() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            Some("key123".to_string()),
            None,
            "header",
            "Authorization",
            Some("Basic".to_string()),
        )
        .unwrap();
    assert_eq!(config.headers[0].0, "authorization");
    assert_eq!(config.headers[0].1, "Basic key123");
}

// --- apply_auth: API key as query parameter ---

#[test]
fn test_auth_api_key_query_param() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(Some("secret".to_string()), None, "query", "api_key", None)
        .unwrap();
    assert!(config.headers.is_empty()); // No headers added
    assert_eq!(
        config.api_key_query,
        Some(("api_key".to_string(), "secret".to_string()))
    );
}

#[test]
fn test_auth_api_key_query_custom_param_name() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(Some("key123".to_string()), None, "query", "appid", None)
        .unwrap();
    assert_eq!(
        config.api_key_query,
        Some(("appid".to_string(), "key123".to_string()))
    );
}

// --- apply_auth: API key as cookie ---

#[test]
fn test_auth_api_key_cookie() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            Some("session-token-xyz".to_string()),
            None,
            "cookie",
            "session",
            None,
        )
        .unwrap();
    assert_eq!(config.headers.len(), 1);
    assert_eq!(config.headers[0].0, "cookie");
    assert_eq!(config.headers[0].1, "session=session-token-xyz");
}

#[test]
fn test_auth_api_key_cookie_default_name() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(Some("token".to_string()), None, "cookie", "api_key", None)
        .unwrap();
    assert_eq!(config.headers[0].1, "api_key=token");
}

// --- apply_auth: Bearer token ---

#[test]
fn test_auth_bearer_token() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            None,
            Some("eyJhbGciOiJIUzI1NiJ9.test".to_string()),
            "header",
            "Authorization",
            None,
        )
        .unwrap();
    assert_eq!(config.headers.len(), 1);
    assert_eq!(config.headers[0].0, "authorization");
    assert_eq!(config.headers[0].1, "Bearer eyJhbGciOiJIUzI1NiJ9.test");
}

// --- apply_auth: Mutual exclusivity ---

#[test]
fn test_auth_mutual_exclusivity_error() {
    let mut config = ServerConfig::default();
    let result = config.apply_auth(
        Some("api-key".to_string()),
        Some("bearer-token".to_string()),
        "header",
        "Authorization",
        None,
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Cannot use both"));
    assert!(err.contains("api_key"));
    assert!(err.contains("bearer_token"));
}

#[test]
fn test_auth_mutual_exclusivity_no_side_effects() {
    let mut config = ServerConfig::default();
    let _ = config.apply_auth(
        Some("api-key".to_string()),
        Some("bearer-token".to_string()),
        "header",
        "Authorization",
        None,
    );
    // Error should not have added any headers
    assert!(config.headers.is_empty());
    assert!(config.api_key_query.is_none());
}

// --- apply_auth: No auth ---

#[test]
fn test_auth_no_credentials() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(None, None, "header", "Authorization", None)
        .unwrap();
    assert!(config.headers.is_empty());
    assert!(config.api_key_query.is_none());
}

// --- apply_auth: Edge cases ---

#[test]
fn test_auth_api_key_empty_string_skipped() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(Some(String::new()), None, "header", "Authorization", None)
        .unwrap();
    // Empty api_key is filtered out — no auth header should be added
    assert!(config.headers.is_empty());
}

#[test]
fn test_auth_bearer_token_empty_string_skipped() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(None, Some(String::new()), "header", "Authorization", None)
        .unwrap();
    // Empty bearer_token is filtered out — no auth header should be added
    assert!(config.headers.is_empty());
}

#[test]
fn test_auth_api_key_whitespace_only_skipped() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            Some("   ".to_string()),
            None,
            "header",
            "Authorization",
            None,
        )
        .unwrap();
    assert!(config.headers.is_empty());
}

#[test]
fn test_auth_both_empty_no_mutual_exclusivity_error() {
    // When both credentials are empty, they're filtered out, so no mutual exclusivity error
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            Some(String::new()),
            Some(String::new()),
            "header",
            "Authorization",
            None,
        )
        .unwrap();
    assert!(config.headers.is_empty());
}

#[test]
fn test_auth_api_key_preserves_existing_headers() {
    let mut config = ServerConfig::default();
    config
        .headers
        .push(("content-type".to_string(), "application/json".to_string()));

    config
        .apply_auth(
            Some("key123".to_string()),
            None,
            "header",
            "X-API-Key",
            None,
        )
        .unwrap();
    assert_eq!(config.headers.len(), 2);
    assert_eq!(config.headers[0].0, "content-type");
    assert_eq!(config.headers[1].0, "x-api-key");
}

#[test]
fn test_auth_explicit_header_location() {
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            Some("key".to_string()),
            None,
            "header",
            "Authorization",
            None,
        )
        .unwrap();
    assert_eq!(config.headers[0].0, "authorization");
    assert_eq!(config.headers[0].1, "Bearer key");
}

#[test]
fn test_auth_unknown_location_returns_error() {
    let mut config = ServerConfig::default();
    let result = config.apply_auth(
        Some("key".to_string()),
        None,
        "queery", // typo
        "api_key",
        None,
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Invalid api_key_location"));
    assert!(err.contains("queery"));
}

#[test]
fn test_auth_unknown_location_no_side_effects() {
    let mut config = ServerConfig::default();
    let _ = config.apply_auth(
        Some("key".to_string()),
        None,
        "invalid",
        "Authorization",
        None,
    );
    // Error should not have added any headers or query params
    assert!(config.headers.is_empty());
    assert!(config.api_key_query.is_none());
}

// --- Debug impl redaction ---

#[test]
fn test_debug_redacts_headers() {
    let mut config = ServerConfig::default();
    config.headers.push((
        "authorization".to_string(),
        "Bearer secret-token".to_string(),
    ));
    config
        .headers
        .push(("x-api-key".to_string(), "my-secret-key".to_string()));

    let debug_output = format!("{config:?}");
    assert!(!debug_output.contains("secret-token"));
    assert!(!debug_output.contains("my-secret-key"));
    assert!(debug_output.contains("[2 header(s)]"));
}

#[test]
fn test_debug_redacts_api_key_query() {
    let config = ServerConfig {
        api_key_query: Some(("api_key".to_string(), "super-secret".to_string())),
        ..Default::default()
    };

    let debug_output = format!("{config:?}");
    assert!(!debug_output.contains("super-secret"));
    assert!(debug_output.contains("api_key=[REDACTED]"));
}

#[test]
fn test_debug_shows_non_sensitive_fields() {
    let config = ServerConfig {
        base_url: "https://api.example.com".to_string(),
        max_pages: 500,
        debug: true,
        ..Default::default()
    };

    let debug_output = format!("{config:?}");
    assert!(debug_output.contains("https://api.example.com"));
    assert!(debug_output.contains("500"));
    assert!(debug_output.contains("true"));
}

#[test]
fn test_debug_no_api_key_query_shows_none() {
    let config = ServerConfig::default();
    let debug_output = format!("{config:?}");
    assert!(debug_output.contains("api_key_query: None"));
}

// --- apply_auth: query + default header name ---

#[test]
fn test_auth_query_with_default_header_still_works() {
    // Even though this is likely misconfigured (Authorization as query param name),
    // the function should still set api_key_query correctly.
    // The warning is emitted by configure_auth (WASM layer), not apply_auth.
    let mut config = ServerConfig::default();
    config
        .apply_auth(
            Some("key123".to_string()),
            None,
            "query",
            "Authorization",
            None,
        )
        .unwrap();
    assert_eq!(
        config.api_key_query,
        Some(("Authorization".to_string(), "key123".to_string()))
    );
}
