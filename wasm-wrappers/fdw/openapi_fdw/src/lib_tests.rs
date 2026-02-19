use super::*;
use config::{DEFAULT_MAX_PAGES, DEFAULT_MAX_RESPONSE_BYTES};

// --- Cross-cutting default tests ---

#[test]
fn test_max_response_bytes_default() {
    let fdw = OpenApiFdw::default();
    assert_eq!(fdw.config.max_response_bytes, DEFAULT_MAX_RESPONSE_BYTES);
}

#[test]
fn test_pagination_safety_defaults() {
    let fdw = OpenApiFdw::default();
    assert_eq!(fdw.config.max_pages, DEFAULT_MAX_PAGES);
    assert_eq!(fdw.pagination.pages_fetched, 0);
    assert!(fdw.pagination.previous.is_none());
    assert!(fdw.pagination.next.is_none());
}

// --- validate_url ---

#[test]
fn test_validate_url_https() {
    assert!(validate_url("https://api.example.com", "base_url").is_ok());
}

#[test]
fn test_validate_url_http() {
    assert!(validate_url("http://localhost:8080", "base_url").is_ok());
}

#[test]
fn test_validate_url_no_scheme() {
    let err = validate_url("api.example.com", "base_url").unwrap_err();
    assert!(err.contains("Invalid base_url"));
    assert!(err.contains("api.example.com"));
    assert!(err.contains("http://"));
}

#[test]
fn test_validate_url_ftp_scheme() {
    let err = validate_url("ftp://files.example.com", "spec_url").unwrap_err();
    assert!(err.contains("Invalid spec_url"));
}

#[test]
fn test_validate_url_empty_string() {
    let err = validate_url("", "base_url").unwrap_err();
    assert!(err.contains("Invalid base_url"));
}

// --- parse_usize_option ---

#[test]
fn test_parse_usize_option_valid() {
    assert_eq!(parse_usize_option("100", "page_size").unwrap(), 100);
}

#[test]
fn test_parse_usize_option_zero() {
    assert_eq!(parse_usize_option("0", "page_size").unwrap(), 0);
}

#[test]
fn test_parse_usize_option_large() {
    assert_eq!(
        parse_usize_option("52428800", "max_response_bytes").unwrap(),
        52_428_800
    );
}

#[test]
fn test_parse_usize_option_negative() {
    let err = parse_usize_option("-1", "max_pages").unwrap_err();
    assert!(err.contains("Invalid value for 'max_pages'"));
    assert!(err.contains("-1"));
}

#[test]
fn test_parse_usize_option_not_a_number() {
    let err = parse_usize_option("abc", "page_size").unwrap_err();
    assert!(err.contains("Invalid value for 'page_size'"));
    assert!(err.contains("abc"));
}

#[test]
fn test_parse_usize_option_float() {
    let err = parse_usize_option("3.14", "page_size").unwrap_err();
    assert!(err.contains("Invalid value for 'page_size'"));
}

// --- parse_bool_flag ---

#[test]
fn test_parse_bool_flag_true() {
    assert!(parse_bool_flag(Some("true")));
}

#[test]
fn test_parse_bool_flag_one() {
    assert!(parse_bool_flag(Some("1")));
}

#[test]
fn test_parse_bool_flag_false() {
    assert!(!parse_bool_flag(Some("false")));
}

#[test]
fn test_parse_bool_flag_zero() {
    assert!(!parse_bool_flag(Some("0")));
}

#[test]
fn test_parse_bool_flag_none() {
    assert!(!parse_bool_flag(None));
}

#[test]
fn test_parse_bool_flag_random_string() {
    assert!(!parse_bool_flag(Some("yes")));
}

// --- should_stop_scanning ---

#[test]
fn test_should_stop_scanning_no_limit() {
    assert!(!should_stop_scanning(100, None));
}

#[test]
fn test_should_stop_scanning_below_limit() {
    assert!(!should_stop_scanning(5, Some(10)));
}

#[test]
fn test_should_stop_scanning_at_limit() {
    assert!(should_stop_scanning(10, Some(10)));
}

#[test]
fn test_should_stop_scanning_above_limit() {
    assert!(should_stop_scanning(15, Some(10)));
}

#[test]
fn test_should_stop_scanning_zero_consumed() {
    assert!(!should_stop_scanning(0, Some(10)));
}

// --- extract_effective_row ---

#[test]
fn test_extract_effective_row_no_path() {
    let row = serde_json::json!({"name": "Alice"});
    let result = extract_effective_row(&row, None);
    assert_eq!(result, &row);
}

#[test]
fn test_extract_effective_row_with_path() {
    let row = serde_json::json!({"properties": {"name": "Alice"}, "type": "Feature"});
    let result = extract_effective_row(&row, Some("/properties"));
    assert_eq!(result, &serde_json::json!({"name": "Alice"}));
}

#[test]
fn test_extract_effective_row_missing_path() {
    let row = serde_json::json!({"name": "Alice"});
    let result = extract_effective_row(&row, Some("/nonexistent"));
    // Falls back to the original row when path doesn't exist
    assert_eq!(result, &row);
}

#[test]
fn test_extract_effective_row_nested_path() {
    let row = serde_json::json!({"a": {"b": {"c": 42}}});
    let result = extract_effective_row(&row, Some("/a/b"));
    assert_eq!(result, &serde_json::json!({"c": 42}));
}
