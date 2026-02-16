use super::*;

#[test]
fn test_sanitize_column_name() {
    assert_eq!(sanitize_column_name("userName"), "user_name");
    assert_eq!(sanitize_column_name("user-name"), "user_name");
    assert_eq!(sanitize_column_name("123abc"), "_123abc");
    assert_eq!(sanitize_column_name("already_snake"), "already_snake");
}

#[test]
fn test_sanitize_column_name_acronyms() {
    // Consecutive uppercase letters (acronyms) should be grouped
    assert_eq!(sanitize_column_name("clusterIP"), "cluster_ip");
    assert_eq!(sanitize_column_name("HTMLParser"), "html_parser");
    assert_eq!(sanitize_column_name("getHTTPSUrl"), "get_https_url");
    assert_eq!(sanitize_column_name("IOError"), "io_error");
    assert_eq!(sanitize_column_name("apiURL"), "api_url");
    // Single uppercase still works
    assert_eq!(sanitize_column_name("firstName"), "first_name");
}

#[test]
fn test_sanitize_column_name_special_chars() {
    // @ prefix (JSON-LD)
    assert_eq!(sanitize_column_name("@id"), "_id");
    assert_eq!(sanitize_column_name("@type"), "_type");
    // Dots (nested keys)
    assert_eq!(sanitize_column_name("user.name"), "user_name");
    // Plus/minus (GitHub reactions)
    assert_eq!(sanitize_column_name("+1"), "_1");
    assert_eq!(sanitize_column_name("-1"), "_1");
}

#[test]
fn test_openapi_to_pg_type() {
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {}
    }"#,
    )
    .unwrap();

    let string_schema = Schema {
        schema_type: Some("string".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&string_schema, &spec), "text");

    let date_schema = Schema {
        schema_type: Some("string".to_string()),
        format: Some("date-time".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&date_schema, &spec), "timestamptz");

    let int_schema = Schema {
        schema_type: Some("integer".to_string()),
        format: Some("int32".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&int_schema, &spec), "integer");
}

#[test]
fn test_openapi_to_pg_type_unix_time() {
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {}
    }"#,
    )
    .unwrap();

    // Stripe's format: "unix-time" should map to timestamptz
    let unix_time_schema = Schema {
        schema_type: Some("integer".to_string()),
        format: Some("unix-time".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&unix_time_schema, &spec), "timestamptz");

    // Regular integer without format should still be bigint
    let int_schema = Schema {
        schema_type: Some("integer".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&int_schema, &spec), "bigint");

    // string format: "date" should be date
    let date_schema = Schema {
        schema_type: Some("string".to_string()),
        format: Some("date".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&date_schema, &spec), "date");

    // boolean
    let bool_schema = Schema {
        schema_type: Some("boolean".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&bool_schema, &spec), "boolean");

    // number format: "float" → real
    let float_schema = Schema {
        schema_type: Some("number".to_string()),
        format: Some("float".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&float_schema, &spec), "real");

    // number without format → double precision
    let num_schema = Schema {
        schema_type: Some("number".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&num_schema, &spec), "double precision");

    // array → jsonb
    let arr_schema = Schema {
        schema_type: Some("array".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&arr_schema, &spec), "jsonb");

    // object → jsonb
    let obj_schema = Schema {
        schema_type: Some("object".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&obj_schema, &spec), "jsonb");

    // None type → jsonb (OpenAPI 3.1 type arrays that resolve to None)
    let none_schema = Schema {
        schema_type: None,
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&none_schema, &spec), "jsonb");
}

#[test]
fn test_openapi_to_pg_type_time_format() {
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {}
    }"#,
    )
    .unwrap();

    let time_schema = Schema {
        schema_type: Some("string".to_string()),
        format: Some("time".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&time_schema, &spec), "time");
}

#[test]
fn test_openapi_to_pg_type_byte_binary_format() {
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {}
    }"#,
    )
    .unwrap();

    let byte_schema = Schema {
        schema_type: Some("string".to_string()),
        format: Some("byte".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&byte_schema, &spec), "bytea");

    let binary_schema = Schema {
        schema_type: Some("string".to_string()),
        format: Some("binary".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&binary_schema, &spec), "bytea");
}

#[test]
fn test_openapi_to_pg_type_uuid_format() {
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {}
    }"#,
    )
    .unwrap();

    let uuid_schema = Schema {
        schema_type: Some("string".to_string()),
        format: Some("uuid".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&uuid_schema, &spec), "uuid");
}

#[test]
fn test_column_name_collision_dedup() {
    // Properties that collide after sanitization should get suffixed
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {}
    }"#,
    )
    .unwrap();

    let mut properties = HashMap::new();
    properties.insert(
        "user-name".to_string(),
        Schema {
            schema_type: Some("string".to_string()),
            ..Default::default()
        },
    );
    properties.insert(
        "userName".to_string(),
        Schema {
            schema_type: Some("string".to_string()),
            ..Default::default()
        },
    );

    let schema = Schema {
        schema_type: Some("object".to_string()),
        properties,
        ..Default::default()
    };

    let columns = extract_columns(&schema, &spec, false);
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    // Both should exist, one with a suffix
    assert!(
        names.contains(&"user_name"),
        "Expected user_name in {names:?}",
    );
    assert!(
        names.contains(&"user_name_1"),
        "Expected user_name_1 for collision in {names:?}",
    );
}

// --- generate_all_tables filter tests ---

fn make_test_spec() -> OpenApiSpec {
    OpenApiSpec::from_str(
        r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/users": {
                "get": {
                    "responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"type": "array", "items": {"type": "object", "properties": {"id": {"type": "string"}}}}}}}}
                }
            },
            "/posts": {
                "get": {
                    "responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"type": "array", "items": {"type": "object", "properties": {"id": {"type": "string"}}}}}}}}
                }
            },
            "/comments": {
                "get": {
                    "responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"type": "array", "items": {"type": "object", "properties": {"id": {"type": "string"}}}}}}}}
                }
            }
        }
    }"#,
    )
    .unwrap()
}

#[test]
fn test_generate_all_tables_no_filter() {
    let spec = make_test_spec();
    let tables = generate_all_tables(&spec, "test_server", None, false, false);
    assert_eq!(tables.len(), 3);
}

#[test]
fn test_generate_all_tables_limit_to() {
    let spec = make_test_spec();
    let filter = vec!["users".to_string(), "posts".to_string()];
    let tables = generate_all_tables(&spec, "test_server", Some(&filter), false, false);
    assert_eq!(tables.len(), 2);
    assert!(tables.iter().any(|t| t.contains("\"users\"")));
    assert!(tables.iter().any(|t| t.contains("\"posts\"")));
    assert!(!tables.iter().any(|t| t.contains("\"comments\"")));
}

#[test]
fn test_generate_all_tables_except() {
    let spec = make_test_spec();
    let filter = vec!["comments".to_string()];
    let tables = generate_all_tables(&spec, "test_server", Some(&filter), true, false);
    assert_eq!(tables.len(), 2);
    assert!(tables.iter().any(|t| t.contains("\"users\"")));
    assert!(tables.iter().any(|t| t.contains("\"posts\"")));
    assert!(!tables.iter().any(|t| t.contains("\"comments\"")));
}

#[test]
fn test_generate_all_tables_limit_to_nonexistent() {
    let spec = make_test_spec();
    let filter = vec!["nonexistent".to_string()];
    let tables = generate_all_tables(&spec, "test_server", Some(&filter), false, false);
    assert_eq!(tables.len(), 0);
}

#[test]
fn test_generate_all_tables_include_attrs() {
    let spec = make_test_spec();
    let tables = generate_all_tables(&spec, "test_server", None, false, true);
    // All tables should have an 'attrs' column
    for table in &tables {
        assert!(
            table.contains("\"attrs\" jsonb"),
            "Missing attrs in: {table}"
        );
    }
}

#[test]
fn test_generate_all_tables_exclude_attrs() {
    let spec = make_test_spec();
    let tables = generate_all_tables(&spec, "test_server", None, false, false);
    // No table should have an 'attrs' column
    for table in &tables {
        assert!(!table.contains("\"attrs\""), "Unexpected attrs in: {table}");
    }
}

#[test]
fn test_generate_foreign_table_no_schema() {
    // Endpoint with no response schema → default id + attrs columns
    let spec = make_test_spec();
    let endpoint = crate::spec::EndpointInfo {
        path: "/health".to_string(),
        method: "GET",
        response_schema: None,
    };
    let table = generate_foreign_table(&endpoint, &spec, "test_server", true);
    assert!(table.contains("\"id\" text NOT NULL"));
    assert!(table.contains("\"attrs\" jsonb"));
    assert!(table.contains("rowid_column 'id'"));
}

#[test]
fn test_write_only_properties_filtered() {
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {}
    }"#,
    )
    .unwrap();

    let mut properties = HashMap::new();
    properties.insert(
        "username".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            ..Default::default()
        },
    );
    properties.insert(
        "password".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            write_only: true,
            ..Default::default()
        },
    );
    properties.insert(
        "email".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            ..Default::default()
        },
    );

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        properties,
        ..Default::default()
    };

    let columns = extract_columns(&schema, &spec, false);
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    assert!(names.contains(&"username"), "username should be included");
    assert!(names.contains(&"email"), "email should be included");
    assert!(
        !names.contains(&"password"),
        "password (writeOnly) should be excluded"
    );
    assert_eq!(columns.len(), 2);
}

#[test]
fn test_generate_foreign_table_post_method() {
    let spec = make_test_spec();
    let endpoint = crate::spec::EndpointInfo {
        path: "/search".to_string(),
        method: "POST",
        response_schema: None,
    };
    let table = generate_foreign_table(&endpoint, &spec, "test_server", true);
    assert!(
        table.contains("method 'POST'"),
        "POST DDL should include method option: {table}"
    );
    assert!(table.contains("endpoint '/search'"));
}

#[test]
fn test_generate_foreign_table_get_no_method() {
    let spec = make_test_spec();
    let endpoint = crate::spec::EndpointInfo {
        path: "/items".to_string(),
        method: "GET",
        response_schema: None,
    };
    let table = generate_foreign_table(&endpoint, &spec, "test_server", true);
    assert!(
        !table.contains("method "),
        "GET DDL should NOT include method option: {table}"
    );
    assert!(table.contains("endpoint '/items'"));
}

// --- OpenAPI 3.1 type mapping and DDL generation tests ---

#[test]
fn test_int64_format_explicit() {
    // integer + format: "int64" → bigint
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let schema = crate::spec::Schema {
        schema_type: Some("integer".to_string()),
        format: Some("int64".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&schema, &spec), "bigint");
}

#[test]
fn test_double_format_explicit() {
    // number + format: "double" → double precision
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let schema = crate::spec::Schema {
        schema_type: Some("number".to_string()),
        format: Some("double".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&schema, &spec), "double precision");
}

#[test]
fn test_unknown_string_format_fallback() {
    // Unknown string formats (email, uri, hostname, ipv4, ipv6, password) → text
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    for fmt in &["email", "uri", "hostname", "ipv4", "ipv6", "password"] {
        let schema = crate::spec::Schema {
            schema_type: Some("string".to_string()),
            format: Some(fmt.to_string()),
            ..Default::default()
        };
        assert_eq!(
            openapi_to_pg_type(&schema, &spec),
            "text",
            "format '{fmt}' should map to text",
        );
    }
}

#[test]
fn test_extract_columns_github_type_arrays() {
    // Nullable via type arrays in column extraction (OpenAPI 3.1)
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.1.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let mut properties = HashMap::new();
    properties.insert(
        "name".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            nullable: true, // from type: ["string", "null"]
            ..Default::default()
        },
    );
    properties.insert(
        "count".to_string(),
        crate::spec::Schema {
            schema_type: Some("integer".to_string()),
            ..Default::default()
        },
    );

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        properties,
        required: vec!["name".to_string(), "count".to_string()],
        ..Default::default()
    };

    let columns = extract_columns(&schema, &spec, false);

    let name_col = columns.iter().find(|c| c.name == "name").unwrap();
    // name is required but nullable (from type array) → nullable=true
    assert!(name_col.nullable);
    assert_eq!(name_col.pg_type, "text");

    let count_col = columns.iter().find(|c| c.name == "count").unwrap();
    // count is required and not nullable → nullable=false
    assert!(!count_col.nullable);
    assert_eq!(count_col.pg_type, "bigint");
}

#[test]
fn test_rowid_selection_no_id_column() {
    // No 'id' column → picks first non-attrs non-jsonb column as rowid
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let mut properties = HashMap::new();
    properties.insert(
        "name".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            ..Default::default()
        },
    );
    properties.insert(
        "metadata".to_string(),
        crate::spec::Schema {
            schema_type: Some("object".to_string()),
            ..Default::default()
        },
    );

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        properties,
        ..Default::default()
    };

    let endpoint = crate::spec::EndpointInfo {
        path: "/things".to_string(),
        method: "GET",
        response_schema: Some(schema),
    };

    let table = generate_foreign_table(&endpoint, &spec, "test_server", false);
    // 'metadata' is jsonb, so 'name' (text) should be the rowid
    assert!(
        table.contains("rowid_column 'name'"),
        "Expected name as rowid: {table}"
    );
}

#[test]
fn test_rowid_selection_all_jsonb() {
    // All columns are jsonb → omits rowid_column
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let mut properties = HashMap::new();
    properties.insert(
        "data".to_string(),
        crate::spec::Schema {
            schema_type: Some("object".to_string()),
            ..Default::default()
        },
    );
    properties.insert(
        "meta".to_string(),
        crate::spec::Schema {
            schema_type: Some("array".to_string()),
            ..Default::default()
        },
    );

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        properties,
        ..Default::default()
    };

    let endpoint = crate::spec::EndpointInfo {
        path: "/blobs".to_string(),
        method: "GET",
        response_schema: Some(schema),
    };

    let table = generate_foreign_table(&endpoint, &spec, "test_server", false);
    // All columns are jsonb → no suitable rowid column
    assert!(
        !table.contains("rowid_column"),
        "All-jsonb schema should omit rowid_column: {table}"
    );
}

#[test]
fn test_no_properties_schema_defaults() {
    // Empty properties (e.g., additionalProperties-only schema) → only attrs column if enabled
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        // No properties
        ..Default::default()
    };

    let columns_with_attrs = extract_columns(&schema, &spec, true);
    assert_eq!(columns_with_attrs.len(), 1);
    assert_eq!(columns_with_attrs[0].name, "attrs");

    let columns_without_attrs = extract_columns(&schema, &spec, false);
    assert_eq!(columns_without_attrs.len(), 0);
}

#[test]
fn test_column_ordering_id_first() {
    // id sorts first, rest alphabetical
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let mut properties = HashMap::new();
    for name in &["zebra", "id", "alpha", "middle"] {
        properties.insert(
            name.to_string(),
            crate::spec::Schema {
                schema_type: Some("string".to_string()),
                ..Default::default()
            },
        );
    }

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        properties,
        ..Default::default()
    };

    let columns = extract_columns(&schema, &spec, false);
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "alpha", "middle", "zebra"]);
}

#[test]
fn test_sanitize_consecutive_special_chars() {
    // @@@id → ___id (each special char becomes _)
    assert_eq!(sanitize_column_name("@@@id"), "___id");
}

#[test]
fn test_sanitize_leading_underscore_preserved() {
    // _id stays _id (leading underscore preserved)
    assert_eq!(sanitize_column_name("_id"), "_id");
}

// --- OpenAPI 3.1 DDL generation and type mapping coverage ---

#[test]
fn test_extract_columns_from_ref_array_schema() {
    // Schema is a $ref to an array of objects — should extract items properties
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {},
        "components": {
            "schemas": {
                "User": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"}
                    },
                    "required": ["id"]
                },
                "UserList": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/User"}
                }
            }
        }
    }"##,
    )
    .unwrap();

    let schema = spec.resolve_ref("#/components/schemas/UserList").unwrap();
    let columns = extract_columns(schema, &spec, false);

    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"id"), "Missing id in {names:?}");
    assert!(names.contains(&"name"), "Missing name in {names:?}");

    let id_col = columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(id_col.pg_type, "bigint");
    assert!(!id_col.nullable);

    let name_col = columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name_col.pg_type, "text");
    assert!(name_col.nullable); // not in required list
}

#[test]
fn test_extract_columns_from_allof_resolved() {
    // Schema with allOf — extract_columns should resolve and merge
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {},
        "components": {
            "schemas": {
                "Base": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"}
                    },
                    "required": ["id"]
                },
                "Extended": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Base"},
                        {
                            "type": "object",
                            "properties": {
                                "email": {"type": "string", "format": "email"},
                                "created_at": {"type": "string", "format": "date-time"}
                            },
                            "required": ["email"]
                        }
                    ]
                }
            }
        }
    }"##,
    )
    .unwrap();

    let schema = spec.resolve_ref("#/components/schemas/Extended").unwrap();
    let columns = extract_columns(schema, &spec, false);

    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"email"));
    assert!(names.contains(&"created_at"));

    // id is required + not nullable → NOT NULL
    let id_col = columns.iter().find(|c| c.name == "id").unwrap();
    assert!(!id_col.nullable);

    // email is required + not nullable → NOT NULL
    let email_col = columns.iter().find(|c| c.name == "email").unwrap();
    assert!(!email_col.nullable);
    assert_eq!(email_col.pg_type, "text"); // format: "email" → text

    // created_at is not required → nullable
    let created_col = columns.iter().find(|c| c.name == "created_at").unwrap();
    assert!(created_col.nullable);
    assert_eq!(created_col.pg_type, "timestamptz");
}

#[test]
fn test_generate_foreign_table_single_quote_in_endpoint() {
    // Endpoint with single quotes — should be escaped in SQL
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let endpoint = crate::spec::EndpointInfo {
        path: "/o'reilly/books".to_string(),
        method: "GET",
        response_schema: None,
    };
    let table = generate_foreign_table(&endpoint, &spec, "test_server", false);
    // Single quote should be doubled in SQL
    assert!(
        table.contains("endpoint '/o''reilly/books'"),
        "Should escape single quotes: {table}"
    );
}

#[test]
fn test_quote_identifier_with_double_quote() {
    // Table name with double quote — sanitized to underscore by table_name()
    let endpoint = crate::spec::EndpointInfo {
        path: "/he\"llo".to_string(),
        method: "GET",
        response_schema: None,
    };
    let table_name = endpoint.table_name();
    assert_eq!(table_name, "he_llo");

    // The DDL uses the sanitized name
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();
    let ddl = generate_foreign_table(&endpoint, &spec, "test_server", false);
    assert!(
        ddl.contains(r#""he_llo""#),
        "Sanitized table name should appear in DDL: {ddl}"
    );
}

#[test]
fn test_full_ddl_from_openapi_31_spec() {
    // Full end-to-end: OpenAPI 3.1 spec → DDL with correct type arrays
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test 3.1 API", "version": "1.0"},
        "paths": {
            "/users": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "integer"},
                                                "name": {"type": "string"},
                                                "bio": {"type": ["string", "null"]},
                                                "age": {"type": ["integer", "null"], "format": "int32"},
                                                "score": {"type": ["number", "null"], "format": "float"},
                                                "active": {"type": ["boolean", "null"]},
                                                "tags": {"type": "array", "items": {"type": "string"}},
                                                "metadata": {"type": "object"}
                                            },
                                            "required": ["id", "name"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#,
    )
    .unwrap();

    let tables = generate_all_tables(&spec, "api_server", None, false, true);
    assert_eq!(tables.len(), 1);

    let ddl = &tables[0];

    // Check column types
    assert!(ddl.contains("\"id\" bigint NOT NULL"), "id: {ddl}");
    assert!(ddl.contains("\"name\" text NOT NULL"), "name: {ddl}");
    assert!(
        ddl.contains("\"bio\" text"),
        "bio should be nullable text: {ddl}"
    );
    assert!(
        ddl.contains("\"age\" integer"),
        "age should be int32: {ddl}"
    );
    assert!(
        ddl.contains("\"score\" real"),
        "score should be float: {ddl}"
    );
    assert!(ddl.contains("\"active\" boolean"), "active: {ddl}");
    assert!(ddl.contains("\"tags\" jsonb"), "tags array → jsonb: {ddl}");
    assert!(
        ddl.contains("\"metadata\" jsonb"),
        "metadata object → jsonb: {ddl}"
    );
    assert!(ddl.contains("\"attrs\" jsonb"), "attrs column: {ddl}");

    // id column should be rowid
    assert!(ddl.contains("rowid_column 'id'"), "rowid: {ddl}");

    // bio and age should NOT have NOT NULL (they're nullable via type arrays)
    assert!(
        !ddl.contains("\"bio\" text NOT NULL"),
        "bio should be nullable: {ddl}"
    );
    assert!(
        !ddl.contains("\"age\" integer NOT NULL"),
        "age should be nullable: {ddl}"
    );
}

#[test]
fn test_openapi_to_pg_type_ref_resolved() {
    // Type mapping with $ref — should resolve the ref before mapping
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {},
        "components": {
            "schemas": {
                "UserId": {
                    "type": "string",
                    "format": "uuid"
                }
            }
        }
    }"##,
    )
    .unwrap();

    let ref_schema = crate::spec::Schema {
        reference: Some("#/components/schemas/UserId".to_string()),
        ..Default::default()
    };
    assert_eq!(openapi_to_pg_type(&ref_schema, &spec), "uuid");
}

#[test]
fn test_extract_columns_nullable_required_interaction() {
    // Column is both required AND nullable (OpenAPI 3.1 type arrays)
    // → should be nullable (nullable overrides required)
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.1.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let mut properties = HashMap::new();
    properties.insert(
        "email".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            nullable: true, // from type: ["string", "null"]
            ..Default::default()
        },
    );

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        properties,
        required: vec!["email".to_string()],
        ..Default::default()
    };

    let columns = extract_columns(&schema, &spec, false);
    let email = columns.iter().find(|c| c.name == "email").unwrap();
    // required=true but nullable=true → column should be nullable
    assert!(email.nullable, "nullable should override required");
}

#[test]
fn test_extract_columns_write_only_in_allof() {
    // writeOnly property inside allOf should still be filtered
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test"},
        "paths": {},
        "components": {
            "schemas": {
                "User": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer"},
                                "password": {"type": "string", "writeOnly": true}
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"}
                            }
                        }
                    ]
                }
            }
        }
    }"##,
    )
    .unwrap();

    let user = spec.resolve_ref("#/components/schemas/User").unwrap();
    let columns = extract_columns(user, &spec, false);
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    assert!(names.contains(&"id"));
    assert!(names.contains(&"name"));
    assert!(
        !names.contains(&"password"),
        "writeOnly in allOf should be excluded"
    );
}

#[test]
fn test_generate_foreign_table_with_attrs_existing() {
    // Schema already has an "attrs" property — should not duplicate
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.0.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let mut properties = HashMap::new();
    properties.insert(
        "id".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            ..Default::default()
        },
    );
    properties.insert(
        "attrs".to_string(),
        crate::spec::Schema {
            schema_type: Some("object".to_string()),
            ..Default::default()
        },
    );

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        properties,
        ..Default::default()
    };

    let columns = extract_columns(&schema, &spec, true);
    let attrs_count = columns.iter().filter(|c| c.name == "attrs").count();
    assert_eq!(attrs_count, 1, "Should not duplicate existing attrs column");
}

#[test]
fn test_sanitize_column_name_unicode() {
    // Unicode letters are alphanumeric, so they pass through (lowercased)
    assert_eq!(sanitize_column_name("café"), "café");
    assert_eq!(sanitize_column_name("naïve"), "naïve");
    // Non-letter unicode (e.g., emoji) → underscore
    assert_eq!(sanitize_column_name("key→val"), "key_val");
}

#[test]
fn test_sanitize_column_name_all_special() {
    // All special characters
    assert_eq!(sanitize_column_name("@#$"), "___");
}

#[test]
fn test_sanitize_column_name_mixed_digits_and_uppercase() {
    // Mix of digits and uppercase in camelCase
    assert_eq!(sanitize_column_name("ipV4Address"), "ip_v4_address");
    assert_eq!(sanitize_column_name("x509Certificate"), "x509_certificate");
}

#[test]
fn test_generate_all_tables_post_method_in_ddl() {
    // POST endpoint should include method option in DDL
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/search": {
                "post": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "string"},
                                                "score": {"type": "number"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#,
    )
    .unwrap();

    let tables = generate_all_tables(&spec, "api_server", None, false, false);
    assert_eq!(tables.len(), 1);
    let ddl = &tables[0];
    assert!(ddl.contains("\"search_post\""), "Table name: {ddl}");
    assert!(ddl.contains("method 'POST'"), "Method option: {ddl}");
    assert!(ddl.contains("endpoint '/search'"), "Endpoint option: {ddl}");
}

// =============================================================================
// OpenAPI 3.1 DDL generation coverage — end-to-end spec → DDL pipeline
// =============================================================================

#[test]
fn test_extract_columns_from_oneof_nullable_properties() {
    // oneOf merges properties as nullable — verify column extraction reflects this
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test"},
        "paths": {},
        "components": {
            "schemas": {
                "Cat": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "indoor": {"type": "boolean"}
                    },
                    "required": ["name"]
                },
                "Dog": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "breed": {"type": "string"}
                    },
                    "required": ["name"]
                },
                "Pet": {
                    "oneOf": [
                        {"$ref": "#/components/schemas/Cat"},
                        {"$ref": "#/components/schemas/Dog"}
                    ]
                }
            }
        }
    }"##,
    )
    .unwrap();

    let pet = spec.resolve_ref("#/components/schemas/Pet").unwrap();
    let columns = extract_columns(pet, &spec, false);
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    assert!(names.contains(&"name"), "Missing name in {names:?}");
    assert!(names.contains(&"indoor"), "Missing indoor in {names:?}");
    assert!(names.contains(&"breed"), "Missing breed in {names:?}");

    // All oneOf properties should be nullable (don't know which variant)
    for col in &columns {
        assert!(col.nullable, "{} should be nullable in oneOf", col.name);
    }
}

#[test]
fn test_extract_columns_31_anyof_ref_and_null() {
    // GitHub pattern: anyOf: [$ref, {type: "null"}] — should produce columns from the ref
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test"},
        "paths": {},
        "components": {
            "schemas": {
                "SimpleUser": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "login": {"type": "string"},
                        "avatar_url": {"type": ["string", "null"], "format": "uri"}
                    },
                    "required": ["id", "login"]
                }
            }
        }
    }"##,
    )
    .unwrap();

    // Simulate what get_response_schema returns for anyOf: [$ref, null]
    let schema = crate::spec::Schema {
        any_of: vec![
            crate::spec::Schema {
                reference: Some("#/components/schemas/SimpleUser".to_string()),
                ..Default::default()
            },
            crate::spec::Schema {
                schema_type: Some("null".to_string()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let columns = extract_columns(&schema, &spec, false);
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    assert!(names.contains(&"id"), "Missing id in {names:?}");
    assert!(names.contains(&"login"), "Missing login in {names:?}");
    assert!(
        names.contains(&"avatar_url"),
        "Missing avatar_url in {names:?}"
    );

    // anyOf makes everything nullable
    for col in &columns {
        assert!(col.nullable, "{} should be nullable in anyOf", col.name);
    }

    // avatar_url gets format: "uri" → text (not a special PG type for uri)
    let avatar = columns.iter().find(|c| c.name == "avatar_url").unwrap();
    assert_eq!(avatar.pg_type, "text");
}

#[test]
fn test_full_ddl_31_anyof_nullable_ref_pattern() {
    // End-to-end: 3.1 spec with GitHub-style anyOf nullable ref → correct DDL
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.1.0",
        "info": {"title": "GitHub-style API", "version": "1.0"},
        "paths": {
            "/repos": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "integer"},
                                                "name": {"type": "string"},
                                                "owner": {
                                                    "anyOf": [
                                                        {"$ref": "#/components/schemas/SimpleUser"},
                                                        {"type": "null"}
                                                    ]
                                                },
                                                "description": {"type": ["string", "null"]},
                                                "created_at": {"type": "string", "format": "date-time"}
                                            },
                                            "required": ["id", "name"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "SimpleUser": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "login": {"type": "string"}
                    },
                    "required": ["id", "login"]
                }
            }
        }
    }"##,
    )
    .unwrap();

    let tables = generate_all_tables(&spec, "github_server", None, false, true);
    assert_eq!(tables.len(), 1);
    let ddl = &tables[0];

    // id and name are required, not nullable
    assert!(ddl.contains("\"id\" bigint NOT NULL"), "id: {ddl}");
    assert!(ddl.contains("\"name\" text NOT NULL"), "name: {ddl}");
    // created_at is not required → nullable
    assert!(
        ddl.contains("\"created_at\" timestamptz"),
        "created_at: {ddl}"
    );
    assert!(
        !ddl.contains("\"created_at\" timestamptz NOT NULL"),
        "created_at should be nullable: {ddl}"
    );
    // description is nullable via type array
    assert!(ddl.contains("\"description\" text"), "description: {ddl}");
    assert!(
        !ddl.contains("\"description\" text NOT NULL"),
        "description should be nullable: {ddl}"
    );
    // owner is anyOf → jsonb (merged from oneOf/anyOf without type)
    assert!(
        ddl.contains("\"owner\" jsonb"),
        "owner should be jsonb: {ddl}"
    );
    // attrs column
    assert!(ddl.contains("\"attrs\" jsonb"), "attrs: {ddl}");
    // rowid should be id
    assert!(ddl.contains("rowid_column 'id'"), "rowid: {ddl}");
}

#[test]
fn test_full_ddl_31_allof_inheritance_chain() {
    // End-to-end: allOf inheritance with 3.1 type arrays → correct DDL
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.1.0",
        "info": {"title": "Inheritance API", "version": "1.0"},
        "paths": {
            "/tickets": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {"$ref": "#/components/schemas/Ticket"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "BaseEntity": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "format": "uuid"},
                        "created_at": {"type": "string", "format": "date-time"},
                        "updated_at": {"type": ["string", "null"], "format": "date-time"}
                    },
                    "required": ["id", "created_at"]
                },
                "Ticket": {
                    "allOf": [
                        {"$ref": "#/components/schemas/BaseEntity"},
                        {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string"},
                                "priority": {"type": ["integer", "null"], "format": "int32"},
                                "assignee": {"type": ["string", "null"]}
                            },
                            "required": ["title"]
                        }
                    ]
                }
            }
        }
    }"##,
    )
    .unwrap();

    let tables = generate_all_tables(&spec, "api_server", None, false, false);
    assert_eq!(tables.len(), 1);
    let ddl = &tables[0];

    // From BaseEntity: id (uuid, required), created_at (timestamptz, required), updated_at (nullable)
    assert!(ddl.contains("\"id\" uuid NOT NULL"), "id: {ddl}");
    assert!(
        ddl.contains("\"created_at\" timestamptz NOT NULL"),
        "created_at: {ddl}"
    );
    assert!(
        ddl.contains("\"updated_at\" timestamptz"),
        "updated_at: {ddl}"
    );
    assert!(
        !ddl.contains("\"updated_at\" timestamptz NOT NULL"),
        "updated_at should be nullable: {ddl}"
    );

    // From Ticket extension: title (required), priority (nullable int32), assignee (nullable)
    assert!(ddl.contains("\"title\" text NOT NULL"), "title: {ddl}");
    assert!(ddl.contains("\"priority\" integer"), "priority: {ddl}");
    assert!(
        !ddl.contains("\"priority\" integer NOT NULL"),
        "priority should be nullable: {ddl}"
    );
    assert!(ddl.contains("\"assignee\" text"), "assignee: {ddl}");
    assert!(
        !ddl.contains("\"assignee\" text NOT NULL"),
        "assignee should be nullable: {ddl}"
    );

    // rowid should be id
    assert!(ddl.contains("rowid_column 'id'"), "rowid: {ddl}");
}

#[test]
fn test_generate_all_tables_31_spec_with_type_arrays() {
    // generate_all_tables with a full 3.1 spec — multiple endpoints
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.1.0",
        "info": {"title": "3.1 API", "version": "1.0"},
        "paths": {
            "/users": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "integer"},
                                                "email": {"type": ["string", "null"]}
                                            },
                                            "required": ["id"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/events": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "string", "format": "uuid"},
                                                "occurred_at": {"type": "string", "format": "date-time"},
                                                "payload": {"type": ["object", "null"]}
                                            },
                                            "required": ["id", "occurred_at"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#,
    )
    .unwrap();

    let tables = generate_all_tables(&spec, "api31", None, false, true);
    assert_eq!(tables.len(), 2);

    // Find the users table
    let users_ddl = tables.iter().find(|t| t.contains("\"users\"")).unwrap();
    assert!(
        users_ddl.contains("\"id\" bigint NOT NULL"),
        "users.id: {users_ddl}"
    );
    assert!(
        users_ddl.contains("\"email\" text"),
        "users.email: {users_ddl}"
    );
    assert!(
        !users_ddl.contains("\"email\" text NOT NULL"),
        "users.email nullable: {users_ddl}"
    );

    // Find the events table
    let events_ddl = tables.iter().find(|t| t.contains("\"events\"")).unwrap();
    assert!(
        events_ddl.contains("\"id\" uuid NOT NULL"),
        "events.id: {events_ddl}"
    );
    assert!(
        events_ddl.contains("\"occurred_at\" timestamptz NOT NULL"),
        "events.occurred_at: {events_ddl}"
    );
    assert!(
        events_ddl.contains("\"payload\" jsonb"),
        "events.payload: {events_ddl}"
    );
    assert!(
        !events_ddl.contains("\"payload\" jsonb NOT NULL"),
        "events.payload nullable: {events_ddl}"
    );
}

#[test]
fn test_extract_columns_31_all_nullable_format_types() {
    // Every format type mapped through 3.1 nullable type arrays
    let spec = OpenApiSpec::from_str(
        r#"{
        "openapi": "3.1.0",
        "info": {"title": "T"},
        "paths": {}
    }"#,
    )
    .unwrap();

    let mut properties = HashMap::new();
    // string formats
    properties.insert(
        "date_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            format: Some("date".to_string()),
            nullable: true, // from type: ["string", "null"]
            ..Default::default()
        },
    );
    properties.insert(
        "datetime_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            format: Some("date-time".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    properties.insert(
        "time_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            format: Some("time".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    properties.insert(
        "uuid_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            format: Some("uuid".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    properties.insert(
        "byte_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            format: Some("byte".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    properties.insert(
        "binary_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            format: Some("binary".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    // integer formats
    properties.insert(
        "int32_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("integer".to_string()),
            format: Some("int32".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    properties.insert(
        "int64_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("integer".to_string()),
            format: Some("int64".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    properties.insert(
        "unix_time_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("integer".to_string()),
            format: Some("unix-time".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    // number formats
    properties.insert(
        "float_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("number".to_string()),
            format: Some("float".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    properties.insert(
        "double_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("number".to_string()),
            format: Some("double".to_string()),
            nullable: true,
            ..Default::default()
        },
    );
    // boolean
    properties.insert(
        "bool_field".to_string(),
        crate::spec::Schema {
            schema_type: Some("boolean".to_string()),
            nullable: true,
            ..Default::default()
        },
    );

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        properties,
        // None required — all optional
        ..Default::default()
    };

    let columns = extract_columns(&schema, &spec, false);

    let expected = vec![
        ("binary_field", "bytea"),
        ("bool_field", "boolean"),
        ("byte_field", "bytea"),
        ("date_field", "date"),
        ("datetime_field", "timestamptz"),
        ("double_field", "double precision"),
        ("float_field", "real"),
        ("int32_field", "integer"),
        ("int64_field", "bigint"),
        ("time_field", "time"),
        ("unix_time_field", "timestamptz"),
        ("uuid_field", "uuid"),
    ];

    for (name, pg_type) in &expected {
        let col = columns
            .iter()
            .find(|c| c.name == *name)
            .unwrap_or_else(|| panic!("Missing column {name}"));
        assert_eq!(col.pg_type, *pg_type, "{name} should be {pg_type}");
        assert!(col.nullable, "{name} should be nullable");
    }
}

#[test]
fn test_full_ddl_31_stripe_expandable_field() {
    // Stripe pattern: anyOf [string, $ref] for expandable fields → produces jsonb in DDL
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.1.0",
        "info": {"title": "Stripe-style API", "version": "1.0"},
        "paths": {
            "/charges": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "data": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id": {"type": "string"},
                                                        "amount": {"type": "integer"},
                                                        "customer": {
                                                            "anyOf": [
                                                                {"type": "string"},
                                                                {"$ref": "#/components/schemas/Customer"}
                                                            ]
                                                        },
                                                        "currency": {"type": "string"},
                                                        "created": {"type": "integer", "format": "unix-time"}
                                                    },
                                                    "required": ["id", "amount", "currency", "created"]
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "Customer": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "email": {"type": ["string", "null"]}
                    }
                }
            }
        }
    }"##,
    )
    .unwrap();

    let tables = generate_all_tables(&spec, "stripe_server", None, false, false);
    assert_eq!(tables.len(), 1);
    let ddl = &tables[0];

    // The response is an object with a "data" array — extract_columns sees the top-level object,
    // which has a "data" property of type array → jsonb column
    // This verifies that wrapper objects don't get their inner items extracted at DDL time
    // (that's a runtime concern handled by extract_data auto-detection)
    assert!(ddl.contains("\"data\" jsonb"), "data wrapper: {ddl}");
}

#[test]
fn test_full_ddl_31_response_level_ref() {
    // Response-level $ref with 3.1 type arrays in the referenced response schema
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {
                            "$ref": "#/components/responses/ItemList"
                        }
                    }
                }
            }
        },
        "components": {
            "responses": {
                "ItemList": {
                    "description": "List of items",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "string", "format": "uuid"},
                                        "label": {"type": ["string", "null"]},
                                        "weight": {"type": ["number", "null"], "format": "float"},
                                        "active": {"type": ["boolean", "null"]}
                                    },
                                    "required": ["id"]
                                }
                            }
                        }
                    }
                }
            }
        }
    }"##,
    )
    .unwrap();

    let tables = generate_all_tables(&spec, "test_server", None, false, false);
    assert_eq!(tables.len(), 1);
    let ddl = &tables[0];

    assert!(ddl.contains("\"id\" uuid NOT NULL"), "id: {ddl}");
    assert!(ddl.contains("\"label\" text"), "label: {ddl}");
    assert!(
        !ddl.contains("\"label\" text NOT NULL"),
        "label nullable: {ddl}"
    );
    assert!(ddl.contains("\"weight\" real"), "weight: {ddl}");
    assert!(
        !ddl.contains("\"weight\" real NOT NULL"),
        "weight nullable: {ddl}"
    );
    assert!(ddl.contains("\"active\" boolean"), "active: {ddl}");
    assert!(
        !ddl.contains("\"active\" boolean NOT NULL"),
        "active nullable: {ddl}"
    );
}

#[test]
fn test_extract_columns_31_writeonly_excluded_with_type_arrays() {
    // writeOnly with 3.1 type arrays — should still be filtered
    let spec =
        OpenApiSpec::from_str(r#"{"openapi": "3.1.0", "info": {"title": "T"}, "paths": {}}"#)
            .unwrap();

    let mut properties = HashMap::new();
    properties.insert(
        "username".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            ..Default::default()
        },
    );
    properties.insert(
        "password".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            nullable: true, // type: ["string", "null"]
            write_only: true,
            ..Default::default()
        },
    );
    properties.insert(
        "api_key".to_string(),
        crate::spec::Schema {
            schema_type: Some("string".to_string()),
            write_only: true,
            ..Default::default()
        },
    );

    let schema = crate::spec::Schema {
        schema_type: Some("object".to_string()),
        properties,
        ..Default::default()
    };

    let columns = extract_columns(&schema, &spec, false);
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["username"]);
}

#[test]
fn test_full_ddl_31_post_for_read_with_composition() {
    // POST-for-read endpoint with allOf response in 3.1
    let spec = OpenApiSpec::from_str(
        r##"{
        "openapi": "3.1.0",
        "info": {"title": "Search API", "version": "1.0"},
        "paths": {
            "/search": {
                "post": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "allOf": [
                                                {"$ref": "#/components/schemas/BaseResult"},
                                                {
                                                    "type": "object",
                                                    "properties": {
                                                        "relevance": {"type": ["number", "null"], "format": "float"},
                                                        "snippet": {"type": ["string", "null"]}
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "BaseResult": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "created_at": {"type": "string", "format": "date-time"}
                    },
                    "required": ["id", "title"]
                }
            }
        }
    }"##,
    )
    .unwrap();

    let tables = generate_all_tables(&spec, "search_server", None, false, false);
    assert_eq!(tables.len(), 1);
    let ddl = &tables[0];

    // POST table name
    assert!(ddl.contains("\"search_post\""), "table name: {ddl}");
    assert!(ddl.contains("method 'POST'"), "method: {ddl}");

    // Base fields from allOf
    assert!(ddl.contains("\"id\" text NOT NULL"), "id: {ddl}");
    assert!(ddl.contains("\"title\" text NOT NULL"), "title: {ddl}");
    assert!(
        ddl.contains("\"created_at\" timestamptz"),
        "created_at: {ddl}"
    );

    // Extension fields — nullable via 3.1 type arrays
    assert!(ddl.contains("\"relevance\" real"), "relevance: {ddl}");
    assert!(
        !ddl.contains("\"relevance\" real NOT NULL"),
        "relevance nullable: {ddl}"
    );
    assert!(ddl.contains("\"snippet\" text"), "snippet: {ddl}");
    assert!(
        !ddl.contains("\"snippet\" text NOT NULL"),
        "snippet nullable: {ddl}"
    );
}
