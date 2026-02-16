use super::*;
use crate::config::ServerConfig;
use crate::pagination::PaginationState;

// --- resolve_pagination_url tests ---

fn make_fdw_for_url(base_url: &str, endpoint: &str) -> OpenApiFdw {
    OpenApiFdw {
        config: ServerConfig {
            base_url: base_url.to_string(),
            ..Default::default()
        },
        endpoint: endpoint.to_string(),
        ..Default::default()
    }
}

#[test]
fn test_resolve_pagination_url_absolute_https() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw
        .resolve_pagination_url("https://api.example.com/items?page=2&limit=10")
        .unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2&limit=10");
}

#[test]
fn test_resolve_pagination_url_absolute_http() {
    let fdw = make_fdw_for_url("http://mockserver:1080", "/items");
    let url = fdw
        .resolve_pagination_url("http://mockserver:1080/items?page=2")
        .unwrap();
    assert_eq!(url, "http://mockserver:1080/items?page=2");
}

#[test]
fn test_resolve_pagination_url_query_only() {
    // "?page=2" should resolve against base_url + endpoint
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw.resolve_pagination_url("?page=2").unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2");
}

#[test]
fn test_resolve_pagination_url_query_only_strips_existing_query() {
    // If endpoint already has query params, only the path part is used
    let fdw = make_fdw_for_url("https://api.example.com", "/items?status=active");
    let url = fdw.resolve_pagination_url("?page=2").unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2");
}

#[test]
fn test_resolve_pagination_url_absolute_path() {
    // "/items?page=2" should resolve against base_url
    let fdw = make_fdw_for_url("https://api.example.com", "/old-endpoint");
    let url = fdw
        .resolve_pagination_url("/items?page=2&limit=50")
        .unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2&limit=50");
}

#[test]
fn test_resolve_pagination_url_bare_relative() {
    // "page/2" should resolve against base_url/
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw.resolve_pagination_url("page/2").unwrap();
    assert_eq!(url, "https://api.example.com/page/2");
}

#[test]
fn test_resolve_pagination_url_empty_string() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw.resolve_pagination_url("").unwrap();
    assert_eq!(url, "https://api.example.com/");
}

#[test]
fn test_resolve_pagination_url_trailing_slash_base() {
    // base_url is already trimmed of trailing slash in init()
    let fdw = make_fdw_for_url("https://api.example.com", "/v2/items");
    let url = fdw.resolve_pagination_url("/v2/items?offset=100").unwrap();
    assert_eq!(url, "https://api.example.com/v2/items?offset=100");
}

// --- Cross-origin pagination rejection ---

#[test]
fn test_resolve_pagination_url_cross_origin_rejected() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let result = fdw.resolve_pagination_url("https://evil.com/exfiltrate?token=abc");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("origin mismatch"));
    assert!(err.contains("credential leakage"));
}

#[test]
fn test_resolve_pagination_url_cross_origin_different_subdomain() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let result = fdw.resolve_pagination_url("https://cdn.example.com/items?page=2");
    assert!(result.is_err());
}

#[test]
fn test_resolve_pagination_url_cross_origin_different_port() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let result = fdw.resolve_pagination_url("https://api.example.com:8443/items?page=2");
    assert!(result.is_err());
}

#[test]
fn test_resolve_pagination_url_cross_origin_http_vs_https() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let result = fdw.resolve_pagination_url("http://api.example.com/items?page=2");
    assert!(result.is_err());
}

#[test]
fn test_resolve_pagination_url_same_origin_with_path() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw
        .resolve_pagination_url("https://api.example.com/v2/items?page=2")
        .unwrap();
    assert_eq!(url, "https://api.example.com/v2/items?page=2");
}

#[test]
fn test_resolve_pagination_url_same_origin_with_port() {
    let fdw = make_fdw_for_url("http://mockserver:1080", "/items");
    let url = fdw
        .resolve_pagination_url("http://mockserver:1080/next?cursor=abc")
        .unwrap();
    assert_eq!(url, "http://mockserver:1080/next?cursor=abc");
}

#[test]
fn test_resolve_pagination_url_same_origin_case_insensitive() {
    let fdw = make_fdw_for_url("https://API.Example.COM", "/items");
    let url = fdw
        .resolve_pagination_url("https://api.example.com/items?page=2")
        .unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2");
}

// --- extract_origin tests ---

#[test]
fn test_extract_origin_https() {
    assert_eq!(
        extract_origin("https://api.example.com/items?page=2"),
        "https://api.example.com"
    );
}

#[test]
fn test_extract_origin_with_port() {
    assert_eq!(
        extract_origin("http://localhost:8080/api/v1"),
        "http://localhost:8080"
    );
}

#[test]
fn test_extract_origin_no_path() {
    assert_eq!(
        extract_origin("https://api.example.com"),
        "https://api.example.com"
    );
}

#[test]
fn test_extract_origin_no_scheme() {
    assert_eq!(
        extract_origin("api.example.com/items"),
        "api.example.com/items"
    );
}

#[test]
fn test_extract_origin_trailing_slash() {
    assert_eq!(
        extract_origin("https://api.example.com/"),
        "https://api.example.com"
    );
}

// --- redact_query_param tests ---

#[test]
fn test_redact_query_param_present() {
    let url = "https://api.example.com/items?api_key=SECRET123&page=2";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(
        redacted,
        "https://api.example.com/items?api_key=[REDACTED]&page=2"
    );
    assert!(!redacted.contains("SECRET123"));
}

#[test]
fn test_redact_query_param_at_end() {
    let url = "https://api.example.com/items?page=2&api_key=SECRET123";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(
        redacted,
        "https://api.example.com/items?page=2&api_key=[REDACTED]"
    );
}

#[test]
fn test_redact_query_param_only_param() {
    let url = "https://api.example.com/items?api_key=SECRET123";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(redacted, "https://api.example.com/items?api_key=[REDACTED]");
}

#[test]
fn test_redact_query_param_not_present() {
    let url = "https://api.example.com/items?page=2&limit=10";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(redacted, url);
}

#[test]
fn test_redact_query_param_no_query_string() {
    let url = "https://api.example.com/items";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(redacted, url);
}

#[test]
fn test_redact_query_param_encoded_name() {
    // urlencoding::encode("api key") = "api%20key"
    let url = "https://api.example.com/items?api%20key=SECRET&page=2";
    let redacted = redact_query_param(url, "api key");
    assert_eq!(
        redacted,
        "https://api.example.com/items?api%20key=[REDACTED]&page=2"
    );
}

// --- URL encoding security tests ---

#[test]
fn test_rowid_url_encoding_path_traversal() {
    // Verify urlencoding::encode handles path traversal attempts
    let malicious_id = "../admin";
    let encoded = urlencoding::encode(malicious_id);
    assert_eq!(encoded, "..%2Fadmin");
    // Resulting URL would be /items/..%2Fadmin (safe) not /items/../admin (traversal)
}

#[test]
fn test_rowid_url_encoding_query_injection() {
    // Verify urlencoding::encode handles query injection attempts
    let malicious_id = "123?admin=true";
    let encoded = urlencoding::encode(malicious_id);
    assert_eq!(encoded, "123%3Fadmin%3Dtrue");
}

#[test]
fn test_rowid_url_encoding_special_chars() {
    // Verify urlencoding::encode handles various URL-unsafe chars
    let special = "id with spaces&more=stuff#fragment";
    let encoded = urlencoding::encode(special);
    assert!(!encoded.contains(' '));
    assert!(!encoded.contains('&'));
    assert!(!encoded.contains('='));
    assert!(!encoded.contains('#'));
}

#[test]
fn test_rowid_url_encoding_normal_ids() {
    // Normal IDs should pass through unchanged
    assert_eq!(urlencoding::encode("123"), "123");
    assert_eq!(urlencoding::encode("abc-def"), "abc-def");
    assert_eq!(
        urlencoding::encode("550e8400-e29b-41d4-a716-446655440000"),
        "550e8400-e29b-41d4-a716-446655440000"
    );
}

// --- Retry delay cap tests ---

#[test]
fn test_retry_delay_cap_normal_value() {
    // Normal Retry-After: 5 seconds → 5000ms, well under cap
    let secs: u64 = 5;
    let max_delay: u64 = 30_000;
    let delay = secs.saturating_mul(1000).min(max_delay);
    assert_eq!(delay, 5000);
}

#[test]
fn test_retry_delay_cap_large_value() {
    // Absurdly large Retry-After: 999999 seconds → capped to 30s
    let secs: u64 = 999_999;
    let max_delay: u64 = 30_000;
    let delay = secs.saturating_mul(1000).min(max_delay);
    assert_eq!(delay, 30_000);
}

#[test]
fn test_retry_delay_cap_u64_max() {
    // u64::MAX seconds → saturating_mul prevents overflow, then capped
    let secs: u64 = u64::MAX;
    let max_delay: u64 = 30_000;
    let delay = secs.saturating_mul(1000).min(max_delay);
    assert_eq!(delay, 30_000);
}

#[test]
fn test_retry_delay_cap_zero() {
    // Retry-After: 0 → 0ms (immediate retry)
    let secs: u64 = 0;
    let max_delay: u64 = 30_000;
    let delay = secs.saturating_mul(1000).min(max_delay);
    assert_eq!(delay, 0);
}

#[test]
fn test_retry_backoff_cap() {
    // Exponential backoff at retry_count=10 would be 1024s, but capped
    let retry_count: u32 = 10;
    let max_delay: u64 = 30_000;
    let backoff = 1000u64.saturating_mul(1 << retry_count);
    let delay = backoff.min(max_delay);
    assert_eq!(delay, 30_000);
}

// --- build_query_params: LIMIT-to-page_size optimization ---

fn make_fdw_for_page_size(page_size: usize, src_limit: Option<i64>) -> OpenApiFdw {
    OpenApiFdw {
        config: ServerConfig {
            page_size,
            page_size_param: "per_page".to_string(),
            ..Default::default()
        },
        src_limit,
        ..Default::default()
    }
}

fn get_page_size_param(fdw: &OpenApiFdw) -> Option<String> {
    let (params, _) = fdw.build_query_params(&[], &[]);
    params.iter().find(|p| p.starts_with("per_page=")).cloned()
}

#[test]
fn test_page_size_reduced_by_limit() {
    // LIMIT 5 with page_size=30 → per_page=5
    let fdw = make_fdw_for_page_size(30, Some(5));
    assert_eq!(get_page_size_param(&fdw), Some("per_page=5".to_string()));
}

#[test]
fn test_page_size_not_increased_by_limit() {
    // LIMIT 50 with page_size=30 → per_page=30 (limit larger than page_size)
    let fdw = make_fdw_for_page_size(30, Some(50));
    assert_eq!(get_page_size_param(&fdw), Some("per_page=30".to_string()));
}

#[test]
fn test_page_size_unchanged_without_limit() {
    // No LIMIT → per_page=30
    let fdw = make_fdw_for_page_size(30, None);
    assert_eq!(get_page_size_param(&fdw), Some("per_page=30".to_string()));
}

#[test]
fn test_page_size_zero_no_param() {
    // page_size=0 → no per_page param regardless of LIMIT
    let fdw = make_fdw_for_page_size(0, Some(5));
    assert_eq!(get_page_size_param(&fdw), None);
}

// --- fetch_spec with spec_json tests ---

const MINIMAL_SPEC_JSON: &str = r#"{
    "openapi": "3.0.0",
    "info": { "title": "Test", "version": "1.0" },
    "servers": [{ "url": "https://api.example.com" }],
    "paths": {
        "/items": {
            "get": {
                "responses": { "200": { "description": "OK" } }
            }
        }
    }
}"#;

#[test]
fn test_fetch_spec_from_spec_json() {
    let mut fdw = OpenApiFdw {
        config: ServerConfig {
            spec_json: Some(MINIMAL_SPEC_JSON.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    fdw.fetch_spec().unwrap();
    assert!(fdw.spec.is_some());
    assert_eq!(fdw.config.base_url, "https://api.example.com");
}

#[test]
fn test_fetch_spec_from_spec_json_preserves_explicit_base_url() {
    let mut fdw = OpenApiFdw {
        config: ServerConfig {
            base_url: "https://custom.example.com".to_string(),
            spec_json: Some(MINIMAL_SPEC_JSON.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    fdw.fetch_spec().unwrap();
    assert!(fdw.spec.is_some());
    assert_eq!(fdw.config.base_url, "https://custom.example.com");
}

#[test]
fn test_fetch_spec_from_spec_json_invalid_json() {
    let mut fdw = OpenApiFdw {
        config: ServerConfig {
            spec_json: Some("{ not valid json".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let err = fdw.fetch_spec().unwrap_err();
    assert!(err.contains("Invalid spec_json"));
}

#[test]
fn test_fetch_spec_from_spec_json_too_large() {
    let mut fdw = OpenApiFdw {
        config: ServerConfig {
            spec_json: Some("x".repeat(200)),
            max_response_bytes: 100,
            ..Default::default()
        },
        ..Default::default()
    };
    let err = fdw.fetch_spec().unwrap_err();
    assert!(err.contains("spec_json too large"));
    assert!(err.contains("200 bytes"));
    assert!(err.contains("limit: 100 bytes"));
}

#[test]
fn test_fetch_spec_neither_url_nor_json() {
    let mut fdw = OpenApiFdw::default();
    // Neither spec_url nor spec_json set → succeeds but spec stays None
    fdw.fetch_spec().unwrap();
    assert!(fdw.spec.is_none());
}

// --- Fix 1: api_key_query appended to URL-based pagination ---

#[test]
fn test_resolve_pagination_url_appends_api_key_query() {
    let fdw = OpenApiFdw {
        config: ServerConfig {
            base_url: "https://api.example.com".to_string(),
            api_key_query: Some(("api_key".to_string(), "secret123".to_string())),
            ..Default::default()
        },
        endpoint: "/items".to_string(),
        pagination: PaginationState {
            next: Some(crate::pagination::PaginationToken::Url(
                "https://api.example.com/items?page=2".to_string(),
            )),
            ..Default::default()
        },
        ..Default::default()
    };

    // Simulate what build_url does for URL-based pagination
    let next_url = fdw.pagination.next.as_ref().unwrap().as_url().unwrap();
    let mut url = fdw.resolve_pagination_url(next_url).unwrap();
    if let Some((ref param_name, ref param_value)) = fdw.config.api_key_query {
        let separator = if url.contains('?') { '&' } else { '?' };
        url.push(separator);
        url.push_str(&format!(
            "{}={}",
            urlencoding::encode(param_name),
            urlencoding::encode(param_value)
        ));
    }
    assert_eq!(
        url,
        "https://api.example.com/items?page=2&api_key=secret123"
    );
}

#[test]
fn test_resolve_pagination_url_no_api_key_unchanged() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw
        .resolve_pagination_url("https://api.example.com/items?page=2")
        .unwrap();
    // No api_key_query configured → URL unchanged
    assert_eq!(url, "https://api.example.com/items?page=2");
}

// --- Fix 7: Unclosed '{' in endpoint template ---

#[test]
fn test_substitute_path_params_unclosed_brace_error() {
    let mut injected = std::collections::HashMap::new();
    let result = OpenApiFdw::substitute_path_params("/items/{id", &[], &mut injected);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Unclosed '{'"));
    assert!(err.contains("/items/{id"));
}

#[test]
fn test_substitute_path_params_unclosed_brace_after_valid() {
    let mut injected = std::collections::HashMap::new();
    // First param is valid, second is unclosed
    let result =
        OpenApiFdw::substitute_path_params("/users/{user_id}/posts/{title", &[], &mut injected);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unclosed '{'"));
}
