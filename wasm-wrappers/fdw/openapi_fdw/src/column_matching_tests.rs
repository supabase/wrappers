use super::*;

// --- to_camel_case tests ---

#[test]
fn test_to_camel_case() {
    assert_eq!(to_camel_case("snake_case"), "snakeCase");
    assert_eq!(to_camel_case("already"), "already");
    assert_eq!(to_camel_case("multi_word_name"), "multiWordName");
    assert_eq!(to_camel_case(""), "");
}

#[test]
fn test_to_camel_case_trailing_underscore() {
    assert_eq!(to_camel_case("name_"), "name");
}

#[test]
fn test_to_camel_case_double_underscore() {
    assert_eq!(to_camel_case("a__b"), "aB");
}

#[test]
fn test_to_camel_case_single_char() {
    assert_eq!(to_camel_case("x"), "x");
}

#[test]
fn test_to_camel_case_with_numbers() {
    assert_eq!(to_camel_case("field_2_name"), "field2Name");
}

#[test]
fn test_to_camel_case_all_uppercase() {
    // Already uppercase segments
    assert_eq!(to_camel_case("a_b_c"), "aBC");
}

// --- normalize_to_alnum tests ---

#[test]
fn test_normalize_to_alnum_basic() {
    assert_eq!(normalize_to_alnum("hello"), "hello");
    assert_eq!(normalize_to_alnum("Hello"), "hello");
    assert_eq!(normalize_to_alnum(""), "");
}

#[test]
fn test_normalize_to_alnum_special_chars() {
    assert_eq!(normalize_to_alnum("@id"), "id");
    assert_eq!(normalize_to_alnum("$oid"), "oid");
    assert_eq!(normalize_to_alnum("user.name"), "username");
    assert_eq!(normalize_to_alnum("user-name"), "username");
    assert_eq!(normalize_to_alnum("_id"), "id");
}

#[test]
fn test_normalize_to_alnum_mixed() {
    assert_eq!(normalize_to_alnum("user_Name"), "username");
    assert_eq!(normalize_to_alnum("@Type"), "type");
    assert_eq!(normalize_to_alnum("123-abc"), "123abc");
}

// --- build_column_key_map tests ---

/// Helper: create an FDW with cached columns and src_rows, then build key map
fn build_key_map(
    col_names: &[&str],
    rows: Vec<JsonValue>,
    object_path: Option<&str>,
) -> Vec<Option<KeyMatch>> {
    let mut fdw = OpenApiFdw {
        src_rows: rows,
        object_path: object_path.map(String::from),
        ..Default::default()
    };
    fdw.cached_columns = col_names
        .iter()
        .map(|name| CachedColumn {
            name: name.to_string(),
            type_oid: TypeOid::String,
            camel_name: to_camel_case(name),
            lower_name: name.to_lowercase(),
            alnum_name: normalize_to_alnum(name),
        })
        .collect();
    fdw.build_column_key_map();
    fdw.column_key_map
}

#[test]
fn test_build_column_key_map_exact() {
    let rows = vec![serde_json::json!({"id": 1, "name": "alice"})];
    let map = build_key_map(&["id", "name"], rows, None);
    assert_eq!(map, vec![Some(KeyMatch::Exact), Some(KeyMatch::Exact)]);
}

#[test]
fn test_build_column_key_map_camel() {
    // API returns camelCase, SQL columns are snake_case
    let rows = vec![serde_json::json!({"firstName": "Alice", "lastName": "Smith"})];
    let map = build_key_map(&["first_name", "last_name"], rows, None);
    assert_eq!(
        map,
        vec![Some(KeyMatch::CamelCase), Some(KeyMatch::CamelCase)]
    );
}

#[test]
fn test_build_column_key_map_case_insensitive() {
    // API returns PascalCase, SQL columns are lowercase
    let rows = vec![serde_json::json!({"Id": 1, "UserName": "alice"})];
    let map = build_key_map(&["id", "username"], rows, None);
    assert_eq!(
        map,
        vec![
            Some(KeyMatch::CaseInsensitive("Id".to_string())),
            Some(KeyMatch::CaseInsensitive("UserName".to_string()))
        ]
    );
}

#[test]
fn test_build_column_key_map_empty_rows() {
    let map = build_key_map(&["id", "name"], vec![], None);
    assert_eq!(map, vec![None, None]);
}

#[test]
fn test_build_column_key_map_missing_column() {
    let rows = vec![serde_json::json!({"id": 1, "name": "alice"})];
    let map = build_key_map(&["id", "email"], rows, None);
    assert_eq!(map, vec![Some(KeyMatch::Exact), None]);
}

#[test]
fn test_build_column_key_map_attrs_skipped() {
    let rows = vec![serde_json::json!({"id": 1, "name": "alice"})];
    let map = build_key_map(&["id", "attrs"], rows, None);
    // attrs should be None (special-cased, not looked up)
    assert_eq!(map, vec![Some(KeyMatch::Exact), None]);
}

#[test]
fn test_build_column_key_map_with_object_path() {
    // GeoJSON-style: keys live under /properties
    let rows = vec![serde_json::json!({
        "type": "Feature",
        "properties": {"name": "Park", "area": 500}
    })];
    let map = build_key_map(&["name", "area"], rows, Some("/properties"));
    assert_eq!(map, vec![Some(KeyMatch::Exact), Some(KeyMatch::Exact)]);
}

#[test]
fn test_build_column_key_map_at_prefixed_keys() {
    // JSON-LD @-prefixed keys: @id sanitizes to _id.
    // The normalized matching step strips non-alnum chars:
    //   column "_id" → alnum "id", key "@id" → alnum "id" → match!
    let rows = vec![serde_json::json!({"@id": "urn:test", "@type": "Feature"})];
    let map = build_key_map(&["_id", "_type"], rows, None);
    assert_eq!(
        map,
        vec![
            Some(KeyMatch::CaseInsensitive("@id".to_string())),
            Some(KeyMatch::CaseInsensitive("@type".to_string()))
        ]
    );
}

#[test]
fn test_build_column_key_map_dotted_keys() {
    // Dotted property names: "user.name" sanitizes to "user_name"
    // Normalized: "username" == "username" → match
    let rows = vec![serde_json::json!({"user.name": "Alice", "user.email": "a@b.com"})];
    let map = build_key_map(&["user_name", "user_email"], rows, None);
    assert_eq!(
        map,
        vec![
            Some(KeyMatch::CaseInsensitive("user.name".to_string())),
            Some(KeyMatch::CaseInsensitive("user.email".to_string()))
        ]
    );
}

#[test]
fn test_build_column_key_map_dollar_prefixed_keys() {
    // MongoDB-style $-prefixed keys: "$oid" sanitizes to "_oid"
    let rows = vec![serde_json::json!({"$oid": "abc123"})];
    let map = build_key_map(&["_oid"], rows, None);
    assert_eq!(
        map,
        vec![Some(KeyMatch::CaseInsensitive("$oid".to_string()))]
    );
}

#[test]
fn test_build_column_key_map_mixed_conventions() {
    // Mixed API response: some exact, some camel, some case-insensitive
    // Note: case-insensitive compares k.to_lowercase() == col.lower_name,
    // so it only works for pure case differences (e.g., "Status" vs "status"),
    // not camelCase→snake_case transformations.
    let rows = vec![serde_json::json!({
        "id": 1,
        "firstName": "Alice",
        "Status": "active"
    })];
    let map = build_key_map(&["id", "first_name", "status"], rows, None);
    assert_eq!(
        map,
        vec![
            Some(KeyMatch::Exact),
            Some(KeyMatch::CamelCase),
            Some(KeyMatch::CaseInsensitive("Status".to_string()))
        ]
    );
}

#[test]
fn test_build_column_key_map_k8s_nested_metadata() {
    // Kubernetes response: access nested fields via object_path
    let rows = vec![serde_json::json!({
        "metadata": {"name": "my-pod", "namespace": "default", "uid": "abc-123"},
        "status": {"phase": "Running"}
    })];
    let map = build_key_map(&["name", "namespace", "uid"], rows, Some("/metadata"));
    assert_eq!(
        map,
        vec![
            Some(KeyMatch::Exact),
            Some(KeyMatch::Exact),
            Some(KeyMatch::Exact)
        ]
    );
}

#[test]
fn test_build_column_key_map_github_mixed_casing() {
    // GitHub returns mixed casing: some camelCase, some snake_case
    let rows = vec![serde_json::json!({
        "id": 1,
        "node_id": "MDQ6VXNlcjE=",
        "login": "octocat",
        "gravatar_id": "",
        "followers_url": "https://api.github.com/users/octocat/followers"
    })];
    let map = build_key_map(
        &["id", "node_id", "login", "gravatar_id", "followers_url"],
        rows,
        None,
    );
    // All exact match — GitHub uses snake_case for these fields
    assert!(map.iter().all(|m| matches!(m, Some(KeyMatch::Exact))));
}

#[test]
fn test_build_column_key_map_numeric_keys() {
    // APIs that return numeric-like keys
    let rows = vec![serde_json::json!({
        "200": {"description": "OK"},
        "404": {"description": "Not Found"}
    })];
    // Sanitized: 200 → _200, 404 → _404
    // to_camel_case("_200") = "200", which matches the JSON key "200" → CamelCase match
    let map = build_key_map(&["_200", "_404"], rows, None);
    assert_eq!(
        map,
        vec![Some(KeyMatch::CamelCase), Some(KeyMatch::CamelCase)]
    );
}

#[test]
fn test_hyphen_case_key_matching() {
    // REST APIs with hyphen-case keys: "user-id" → normalized match to "user_id"
    let rows = vec![serde_json::json!({"user-id": "abc", "user-name": "Alice"})];
    let map = build_key_map(&["user_id", "user_name"], rows, None);
    // Normalized matching: "userid" matches "userid" (after stripping non-alnum)
    assert_eq!(
        map,
        vec![
            Some(KeyMatch::CaseInsensitive("user-id".to_string())),
            Some(KeyMatch::CaseInsensitive("user-name".to_string()))
        ]
    );
}

#[test]
fn test_screaming_snake_case_matching() {
    // Legacy APIs with SCREAMING_SNAKE_CASE: "USER_NAME" → case-insensitive match
    let rows = vec![serde_json::json!({"USER_NAME": "alice", "USER_ID": 42})];
    let map = build_key_map(&["user_name", "user_id"], rows, None);
    assert_eq!(
        map,
        vec![
            Some(KeyMatch::CaseInsensitive("USER_NAME".to_string())),
            Some(KeyMatch::CaseInsensitive("USER_ID".to_string()))
        ]
    );
}

// --- normalize_datetime tests ---

#[test]
fn test_normalize_datetime_date_only() {
    assert_eq!(
        OpenApiFdw::normalize_datetime("2024-01-15"),
        "2024-01-15T00:00:00Z"
    );
}

#[test]
fn test_normalize_datetime_full_datetime() {
    // Full datetime should pass through unchanged
    let dt = "2024-06-15T10:30:00Z";
    assert_eq!(OpenApiFdw::normalize_datetime(dt), dt);
}

#[test]
fn test_normalize_datetime_with_offset() {
    let dt = "2024-06-15T10:30:00+05:00";
    assert_eq!(OpenApiFdw::normalize_datetime(dt), dt);
}

#[test]
fn test_normalize_datetime_not_date_format() {
    // Strings that are 10 chars but not date format should pass through
    assert_eq!(OpenApiFdw::normalize_datetime("abcdefghij"), "abcdefghij");
}

#[test]
fn test_normalize_datetime_with_milliseconds() {
    // ISO 8601 datetime with milliseconds — should pass through
    let dt = "2024-06-15T10:30:00.123Z";
    assert_eq!(OpenApiFdw::normalize_datetime(dt), dt);
}

#[test]
fn test_normalize_datetime_short_string() {
    // String shorter than 10 chars — should not be treated as date
    assert_eq!(OpenApiFdw::normalize_datetime("2024-01"), "2024-01");
}

#[test]
fn test_normalize_datetime_long_non_date() {
    // Exactly 10 chars but not a date pattern (no dashes at right positions)
    assert_eq!(OpenApiFdw::normalize_datetime("1234567890"), "1234567890");
}

#[test]
fn test_normalize_datetime_empty_string() {
    assert_eq!(OpenApiFdw::normalize_datetime(""), "");
}

#[test]
fn test_normalize_datetime_tz_without_colon() {
    // Threads API format: +0000 → +00:00
    assert_eq!(
        OpenApiFdw::normalize_datetime("2026-02-12T04:46:47+0000"),
        "2026-02-12T04:46:47+00:00"
    );
}

#[test]
fn test_normalize_datetime_negative_tz_without_colon() {
    assert_eq!(
        OpenApiFdw::normalize_datetime("2024-09-12T23:17:39-0500"),
        "2024-09-12T23:17:39-05:00"
    );
}

#[test]
fn test_normalize_datetime_tz_with_colon_unchanged() {
    // Already has colon — should pass through
    let dt = "2024-06-15T10:30:00+05:30";
    assert_eq!(OpenApiFdw::normalize_datetime(dt), dt);
}

// --- json_to_cell_cached tests ---

/// Helper: build an FDW with cached columns and column_key_map, then call json_to_cell_cached
fn cell_from_json(
    col_name: &str,
    type_oid: TypeOid,
    json_obj: &JsonValue,
) -> Result<Option<Cell>, String> {
    let fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: col_name.to_string(),
            type_oid: type_oid.clone(),
            camel_name: to_camel_case(col_name),
            lower_name: col_name.to_lowercase(),
            alnum_name: normalize_to_alnum(col_name),
        }],
        column_key_map: vec![Some(KeyMatch::Exact)],
        ..Default::default()
    };
    fdw.json_to_cell_cached(json_obj, 0)
}

/// Helper: extract string from Cell
fn cell_to_string(cell: &Cell) -> Option<String> {
    match cell {
        Cell::String(s) => Some(s.clone()),
        _ => None,
    }
}

#[test]
fn test_json_to_cell_bool() {
    let obj = serde_json::json!({"active": true});
    let cell = cell_from_json("active", TypeOid::Bool, &obj).unwrap();
    assert!(matches!(cell, Some(Cell::Bool(true))));
}

#[test]
fn test_json_to_cell_i8() {
    let obj = serde_json::json!({"val": 42});
    let cell = cell_from_json("val", TypeOid::I8, &obj).unwrap();
    assert!(matches!(cell, Some(Cell::I8(42))));
}

#[test]
fn test_json_to_cell_i8_overflow() {
    // 200 exceeds i8 range (-128..127)
    let obj = serde_json::json!({"val": 200});
    let cell = cell_from_json("val", TypeOid::I8, &obj).unwrap();
    assert!(cell.is_none());
}

#[test]
fn test_json_to_cell_i16() {
    let obj = serde_json::json!({"val": 1000});
    let cell = cell_from_json("val", TypeOid::I16, &obj).unwrap();
    assert!(matches!(cell, Some(Cell::I16(1000))));
}

#[test]
fn test_json_to_cell_i16_overflow() {
    // i16 max is 32767 — 40000 should overflow
    let obj = serde_json::json!({"val": 40000});
    let cell = cell_from_json("val", TypeOid::I16, &obj).unwrap();
    assert!(cell.is_none());
}

#[test]
fn test_json_to_cell_i32() {
    let obj = serde_json::json!({"val": 100_000});
    let cell = cell_from_json("val", TypeOid::I32, &obj).unwrap();
    assert!(matches!(cell, Some(Cell::I32(100_000))));
}

#[test]
fn test_json_to_cell_i32_overflow() {
    // i32 max is 2147483647 — 3 billion should overflow
    let obj = serde_json::json!({"val": 3_000_000_000_i64});
    let cell = cell_from_json("val", TypeOid::I32, &obj).unwrap();
    assert!(cell.is_none());
}

#[test]
fn test_json_to_cell_i32_from_float_truncates() {
    // JSON number 42.9 → i64 returns 42 (as_i64 truncates), then try_from
    // serde_json::as_i64() returns None for floats, so this should be None
    let obj = serde_json::json!({"val": 42.9});
    let cell = cell_from_json("val", TypeOid::I32, &obj).unwrap();
    assert!(cell.is_none(), "Float value should not coerce to I32");
}

#[test]
fn test_json_to_cell_i64() {
    let obj = serde_json::json!({"val": 9_000_000_000_i64});
    let cell = cell_from_json("val", TypeOid::I64, &obj).unwrap();
    assert!(matches!(cell, Some(Cell::I64(9_000_000_000))));
}

#[test]
fn test_json_to_cell_i64_max() {
    // Maximum i64 value
    let obj = serde_json::json!({"val": i64::MAX});
    let cell = cell_from_json("val", TypeOid::I64, &obj).unwrap();
    assert!(matches!(cell, Some(Cell::I64(v)) if v == i64::MAX));
}

#[test]
fn test_json_to_cell_f32() {
    let obj = serde_json::json!({"val": 2.78});
    let cell = cell_from_json("val", TypeOid::F32, &obj).unwrap();
    if let Some(Cell::F32(v)) = cell {
        assert!((v - 2.78_f32).abs() < 0.001);
    } else {
        panic!("Expected F32");
    }
}

#[test]
fn test_json_to_cell_f64() {
    let obj = serde_json::json!({"val": 1.234_567_890_123});
    let cell = cell_from_json("val", TypeOid::F64, &obj).unwrap();
    if let Some(Cell::F64(v)) = cell {
        assert!((v - 1.234_567_890_123).abs() < f64::EPSILON);
    } else {
        panic!("Expected F64");
    }
}

#[test]
fn test_json_to_cell_f64_nan_like() {
    // JSON doesn't have NaN — should always be a valid number
    let obj = serde_json::json!({"val": 1e308});
    let cell = cell_from_json("val", TypeOid::F64, &obj).unwrap();
    assert!(matches!(cell, Some(Cell::F64(_))));
}

#[test]
fn test_json_to_cell_numeric() {
    let obj = serde_json::json!({"val": 99.99});
    let cell = cell_from_json("val", TypeOid::Numeric, &obj).unwrap();
    if let Some(Cell::Numeric(v)) = cell {
        assert!((v - 99.99).abs() < f64::EPSILON);
    } else {
        panic!("Expected Numeric");
    }
}

#[test]
fn test_json_to_cell_numeric_from_string() {
    // Numeric column with string value → None (as_f64 returns None for strings)
    let obj = serde_json::json!({"price": "29.99"});
    let cell = cell_from_json("price", TypeOid::Numeric, &obj).unwrap();
    assert!(cell.is_none(), "String number should not coerce to Numeric");
}

#[test]
fn test_json_to_cell_string_from_string() {
    let obj = serde_json::json!({"name": "alice"});
    let cell = cell_from_json("name", TypeOid::String, &obj).unwrap();
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("alice".to_string())
    );
}

#[test]
fn test_json_to_cell_string_from_number() {
    // Non-string values should be serialized to string
    let obj = serde_json::json!({"name": 42});
    let cell = cell_from_json("name", TypeOid::String, &obj).unwrap();
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("42".to_string())
    );
}

#[test]
fn test_json_to_cell_string_from_bool() {
    let obj = serde_json::json!({"name": true});
    let cell = cell_from_json("name", TypeOid::String, &obj).unwrap();
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("true".to_string())
    );
}

#[test]
fn test_json_to_cell_string_from_object() {
    // When a column expects text but JSON value is an object, serialize it
    let obj = serde_json::json!({"info": {"nested": true, "value": 42}});
    let cell = cell_from_json("info", TypeOid::String, &obj).unwrap();
    let s = cell_to_string(cell.as_ref().unwrap()).unwrap();
    assert!(s.contains("\"nested\":true"));
    assert!(s.contains("\"value\":42"));
}

#[test]
fn test_json_to_cell_string_from_array() {
    // When a column expects text but JSON value is an array, serialize it
    let obj = serde_json::json!({"tags": ["rust", "wasm", "sql"]});
    let cell = cell_from_json("tags", TypeOid::String, &obj).unwrap();
    let s = cell_to_string(cell.as_ref().unwrap()).unwrap();
    assert_eq!(s, r#"["rust","wasm","sql"]"#);
}

#[test]
fn test_json_to_cell_null_returns_none() {
    let obj = serde_json::json!({"val": null});
    let cell = cell_from_json("val", TypeOid::String, &obj).unwrap();
    assert!(cell.is_none());
}

#[test]
fn test_json_to_cell_missing_key_returns_none() {
    let obj = serde_json::json!({"other": "value"});
    let fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "missing".to_string(),
            type_oid: TypeOid::String,
            camel_name: "missing".to_string(),
            lower_name: "missing".to_string(),
            alnum_name: "missing".to_string(),
        }],
        column_key_map: vec![None], // no match found
        ..Default::default()
    };
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert!(cell.is_none());
}

#[test]
fn test_json_to_cell_bool_from_non_bool() {
    // Non-boolean value for boolean column → None
    let obj = serde_json::json!({"active": "yes"});
    let cell = cell_from_json("active", TypeOid::Bool, &obj).unwrap();
    assert!(cell.is_none(), "String 'yes' should not coerce to Bool");
}

// Note: Date/Timestamp/Timestamptz from string tests are skipped because
// they call `time::parse_from_rfc3339` which is a WASM host import that
// panics outside the WASM runtime. These paths are covered by integration tests.

#[test]
fn test_json_to_cell_date_from_unix() {
    // Unix timestamp as integer → Date (doesn't call parse_from_rfc3339)
    let obj = serde_json::json!({"dt": 1718409600});
    let cell = cell_from_json("dt", TypeOid::Date, &obj).unwrap();
    assert!(matches!(cell, Some(Cell::Date(1718409600))));
}

#[test]
fn test_json_to_cell_timestamp_from_unix() {
    // Unix epoch → microseconds (doesn't call parse_from_rfc3339)
    let obj = serde_json::json!({"ts": 1718409600});
    let cell = cell_from_json("ts", TypeOid::Timestamp, &obj).unwrap();
    if let Some(Cell::Timestamp(v)) = cell {
        assert_eq!(v, 1_718_409_600_000_000);
    } else {
        panic!("Expected Timestamp");
    }
}

#[test]
fn test_json_to_cell_timestamptz_from_unix() {
    // Unix epoch → microseconds (doesn't call parse_from_rfc3339)
    let obj = serde_json::json!({"ts": 1718409600});
    let cell = cell_from_json("ts", TypeOid::Timestamptz, &obj).unwrap();
    if let Some(Cell::Timestamptz(v)) = cell {
        assert_eq!(v, 1_718_409_600_000_000);
    } else {
        panic!("Expected Timestamptz");
    }
}

#[test]
fn test_json_to_cell_uuid() {
    let obj = serde_json::json!({"uid": "550e8400-e29b-41d4-a716-446655440000"});
    let cell = cell_from_json("uid", TypeOid::Uuid, &obj).unwrap();
    assert!(matches!(
        cell,
        Some(Cell::Uuid(ref s)) if s == "550e8400-e29b-41d4-a716-446655440000"
    ));
}

#[test]
fn test_json_to_cell_uuid_from_non_string() {
    // UUID column with numeric value → None (as_str returns None)
    let obj = serde_json::json!({"uid": 12345});
    let cell = cell_from_json("uid", TypeOid::Uuid, &obj).unwrap();
    assert!(cell.is_none());
}

#[test]
fn test_json_to_cell_json_object() {
    let obj = serde_json::json!({"meta": {"key": "val"}});
    let cell = cell_from_json("meta", TypeOid::Json, &obj).unwrap();
    if let Some(Cell::Json(s)) = cell {
        assert_eq!(s, r#"{"key":"val"}"#);
    } else {
        panic!("Expected Json");
    }
}

#[test]
fn test_json_to_cell_json_array() {
    let obj = serde_json::json!({"tags": ["a", "b", "c"]});
    let cell = cell_from_json("tags", TypeOid::Json, &obj).unwrap();
    if let Some(Cell::Json(s)) = cell {
        assert_eq!(s, r#"["a","b","c"]"#);
    } else {
        panic!("Expected Json");
    }
}

#[test]
fn test_json_to_cell_json_from_null() {
    // Null value for JSON column → None (null is filtered before type matching)
    let obj = serde_json::json!({"meta": null});
    let cell = cell_from_json("meta", TypeOid::Json, &obj).unwrap();
    assert!(cell.is_none());
}

#[test]
fn test_json_to_cell_json_from_primitive() {
    // Primitive value serialized as JSON
    let obj = serde_json::json!({"val": 42});
    let cell = cell_from_json("val", TypeOid::Json, &obj).unwrap();
    if let Some(Cell::Json(s)) = cell {
        assert_eq!(s, "42");
    } else {
        panic!("Expected Json cell");
    }
}

#[test]
fn test_json_to_cell_json_string_value() {
    // JSON column with plain string value → serialize as JSON string
    let obj = serde_json::json!({"data": "hello"});
    let cell = cell_from_json("data", TypeOid::Json, &obj).unwrap();
    if let Some(Cell::Json(s)) = cell {
        assert_eq!(s, r#""hello""#);
    } else {
        panic!("Expected Json cell");
    }
}

#[test]
fn test_json_to_cell_json_bool_value() {
    // JSON column with bool value
    let obj = serde_json::json!({"flag": true});
    let cell = cell_from_json("flag", TypeOid::Json, &obj).unwrap();
    if let Some(Cell::Json(s)) = cell {
        assert_eq!(s, "true");
    } else {
        panic!("Expected Json cell");
    }
}

#[test]
fn test_json_to_cell_typeoid_other() {
    // TypeOid::Other(n) → Json cell (same as TypeOid::Json)
    let obj = serde_json::json!({"payload": {"nested": true, "count": 42}});
    let cell = cell_from_json("payload", TypeOid::Other("custom_type".to_string()), &obj).unwrap();
    if let Some(Cell::Json(s)) = cell {
        assert!(s.contains("\"nested\":true"));
        assert!(s.contains("\"count\":42"));
    } else {
        panic!("Expected Json cell for TypeOid::Other");
    }
}

#[test]
fn test_json_to_cell_path_param_injection() {
    // When a column is a path param, its value is injected from injected_params
    let mut fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "user_id".to_string(),
            type_oid: TypeOid::String,
            camel_name: "userId".to_string(),
            lower_name: "user_id".to_string(),
            alnum_name: "userid".to_string(),
        }],
        column_key_map: vec![None],
        ..Default::default()
    };
    fdw.injected_params
        .insert("user_id".to_string(), "42".to_string());

    let obj = serde_json::json!({"title": "Post Title"});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("42".to_string())
    );
}

#[test]
fn test_json_to_cell_path_param_type_coercion() {
    // Path param for an integer column should be coerced
    let mut fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "id".to_string(),
            type_oid: TypeOid::I64,
            camel_name: "id".to_string(),
            lower_name: "id".to_string(),
            alnum_name: "id".to_string(),
        }],
        column_key_map: vec![None],
        ..Default::default()
    };
    fdw.injected_params
        .insert("id".to_string(), "123".to_string());

    let obj = serde_json::json!({});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert!(matches!(cell, Some(Cell::I64(123))));
}

#[test]
fn test_json_to_cell_path_param_bool_coercion() {
    // Path param for boolean column
    let mut fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "active".to_string(),
            type_oid: TypeOid::Bool,
            camel_name: "active".to_string(),
            lower_name: "active".to_string(),
            alnum_name: "active".to_string(),
        }],
        column_key_map: vec![None],
        ..Default::default()
    };
    fdw.injected_params
        .insert("active".to_string(), "true".to_string());

    let obj = serde_json::json!({});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert!(matches!(cell, Some(Cell::Bool(true))));
}

#[test]
fn test_json_to_cell_path_param_f64_coercion() {
    // Path param for float column
    let mut fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "lat".to_string(),
            type_oid: TypeOid::F64,
            camel_name: "lat".to_string(),
            lower_name: "lat".to_string(),
            alnum_name: "lat".to_string(),
        }],
        column_key_map: vec![None],
        ..Default::default()
    };
    fdw.injected_params
        .insert("lat".to_string(), "37.7749".to_string());

    let obj = serde_json::json!({});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    if let Some(Cell::F64(v)) = cell {
        assert!((v - 37.7749).abs() < f64::EPSILON);
    } else {
        panic!("Expected F64");
    }
}

#[test]
fn test_json_to_cell_path_param_invalid_number_fallback() {
    // Path param that can't parse as target type → falls back to String
    let mut fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "id".to_string(),
            type_oid: TypeOid::I64,
            camel_name: "id".to_string(),
            lower_name: "id".to_string(),
            alnum_name: "id".to_string(),
        }],
        column_key_map: vec![None],
        ..Default::default()
    };
    fdw.injected_params
        .insert("id".to_string(), "not-a-number".to_string());

    let obj = serde_json::json!({});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    // i64 parse fails → falls back to Cell::String
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("not-a-number".to_string())
    );
}

#[test]
fn test_json_to_cell_path_param_json_type() {
    // Path param for JSON column
    let mut fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "filter".to_string(),
            type_oid: TypeOid::Json,
            camel_name: "filter".to_string(),
            lower_name: "filter".to_string(),
            alnum_name: "filter".to_string(),
        }],
        column_key_map: vec![None],
        ..Default::default()
    };
    fdw.injected_params
        .insert("filter".to_string(), r#"{"status":"active"}"#.to_string());

    let obj = serde_json::json!({});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    if let Some(Cell::Json(s)) = cell {
        assert_eq!(s, r#"{"status":"active"}"#);
    } else {
        panic!("Expected Json cell");
    }
}

#[test]
fn test_json_to_cell_attrs_column() {
    // The "attrs" column gets the full row as JSON
    let fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "attrs".to_string(),
            type_oid: TypeOid::Json,
            camel_name: "attrs".to_string(),
            lower_name: "attrs".to_string(),
            alnum_name: "attrs".to_string(),
        }],
        column_key_map: vec![None], // attrs is special-cased, no key match
        ..Default::default()
    };

    let obj = serde_json::json!({"id": 1, "name": "test"});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert!(matches!(cell, Some(Cell::Json(_))));
}

#[test]
fn test_json_to_cell_fallback_camel_match() {
    // When column_key_map has None (heterogeneous rows), fallback to camelCase match
    let fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "first_name".to_string(),
            type_oid: TypeOid::String,
            camel_name: "firstName".to_string(),
            lower_name: "first_name".to_string(),
            alnum_name: "firstname".to_string(),
        }],
        column_key_map: vec![None], // force fallback path
        ..Default::default()
    };

    let obj = serde_json::json!({"firstName": "Alice"});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("Alice".to_string())
    );
}

#[test]
fn test_json_to_cell_fallback_normalized_match() {
    // Fallback to normalized (alnum-only) matching for @-prefixed keys
    let fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "_id".to_string(),
            type_oid: TypeOid::String,
            camel_name: "Id".to_string(),
            lower_name: "_id".to_string(),
            alnum_name: "id".to_string(),
        }],
        column_key_map: vec![None], // force fallback path
        ..Default::default()
    };

    let obj = serde_json::json!({"@id": "urn:test:123"});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("urn:test:123".to_string())
    );
}

#[test]
fn test_json_to_cell_fallback_case_insensitive() {
    // column_key_map=None → fallback 4-step matching with case-insensitive step
    let fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "status".to_string(),
            type_oid: TypeOid::String,
            camel_name: "status".to_string(),
            lower_name: "status".to_string(),
            alnum_name: "status".to_string(),
        }],
        column_key_map: vec![None], // force fallback path
        ..Default::default()
    };

    let obj = serde_json::json!({"Status": "active"});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("active".to_string())
    );
}

#[test]
fn test_json_to_cell_camel_case_key_match() {
    // CamelCase key match path
    let fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "created_at".to_string(),
            type_oid: TypeOid::String,
            camel_name: "createdAt".to_string(),
            lower_name: "created_at".to_string(),
            alnum_name: "createdat".to_string(),
        }],
        column_key_map: vec![Some(KeyMatch::CamelCase)],
        ..Default::default()
    };

    let obj = serde_json::json!({"createdAt": "2024-01-15T10:00:00Z"});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("2024-01-15T10:00:00Z".to_string())
    );
}

#[test]
fn test_json_to_cell_case_insensitive_key_match() {
    // CaseInsensitive key match path
    let fdw = OpenApiFdw {
        cached_columns: vec![CachedColumn {
            name: "user_name".to_string(),
            type_oid: TypeOid::String,
            camel_name: "userName".to_string(),
            lower_name: "user_name".to_string(),
            alnum_name: "username".to_string(),
        }],
        column_key_map: vec![Some(KeyMatch::CaseInsensitive("UserName".to_string()))],
        ..Default::default()
    };

    let obj = serde_json::json!({"UserName": "alice"});
    let cell = fdw.json_to_cell_cached(&obj, 0).unwrap();
    assert_eq!(
        cell_to_string(cell.as_ref().unwrap()),
        Some("alice".to_string())
    );
}
