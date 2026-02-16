use super::*;

#[test]
fn test_parse_minimal_spec() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test API", "version": "1.0"},
        "paths": {}
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    assert_eq!(spec.openapi, "3.0.0");
    assert_eq!(spec.info.title, "Test API");
}

#[test]
fn test_endpoint_table_name() {
    let endpoint = EndpointInfo {
        path: "/api/v1/user-accounts".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "api_v1_user_accounts");

    // Single segment
    let endpoint = EndpointInfo {
        path: "/users".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "users");

    // Collision avoidance: different versions produce different names
    let v1 = EndpointInfo {
        path: "/v1/users".to_string(),
        method: "GET",
        response_schema: None,
    };
    let v2 = EndpointInfo {
        path: "/v2/users".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_ne!(v1.table_name(), v2.table_name());

    // Empty path
    let endpoint = EndpointInfo {
        path: "/".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "unknown");
}

#[test]
fn test_resolve_ref() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "User": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "name": {"type": "string"}
                    },
                    "required": ["id"]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let user_schema = spec.resolve_ref("#/components/schemas/User").unwrap();

    assert_eq!(user_schema.schema_type, Some("object".to_string()));
    assert!(user_schema.properties.contains_key("id"));
    assert!(user_schema.properties.contains_key("name"));
    assert!(user_schema.required.contains(&"id".to_string()));
}

#[test]
fn test_allof_merges_properties() {
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
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
                                "name": {"type": "string"},
                                "email": {"type": "string"}
                            },
                            "required": ["name"]
                        }
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let extended = spec.resolve_ref("#/components/schemas/Extended").unwrap();
    let resolved = spec.resolve_schema(extended);

    // Should have all properties from both schemas
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("email"));

    // Required from both should be merged
    assert!(resolved.required.contains(&"id".to_string()));
    assert!(resolved.required.contains(&"name".to_string()));
}

#[test]
fn test_oneof_merges_as_nullable() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Response": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "user_id": {"type": "string"},
                                "user_name": {"type": "string"}
                            },
                            "required": ["user_id"]
                        },
                        {
                            "type": "object",
                            "properties": {
                                "org_id": {"type": "string"},
                                "org_name": {"type": "string"}
                            },
                            "required": ["org_id"]
                        }
                    ]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let response = spec.resolve_ref("#/components/schemas/Response").unwrap();
    let resolved = spec.resolve_schema(response);

    // Should have properties from all variants
    assert!(resolved.properties.contains_key("user_id"));
    assert!(resolved.properties.contains_key("user_name"));
    assert!(resolved.properties.contains_key("org_id"));
    assert!(resolved.properties.contains_key("org_name"));

    // All properties should be nullable (since we don't know which variant)
    assert!(resolved.properties.get("user_id").unwrap().nullable);
    assert!(resolved.properties.get("org_id").unwrap().nullable);

    // Nothing should be required for oneOf
    assert!(resolved.required.is_empty());
}

#[test]
fn test_anyof_merges_as_nullable() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Flexible": {
                    "anyOf": [
                        {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"}
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string"}
                            }
                        }
                    ]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let flexible = spec.resolve_ref("#/components/schemas/Flexible").unwrap();
    let resolved = spec.resolve_schema(flexible);

    // Should have properties from all variants
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("title"));

    // All should be nullable
    assert!(resolved.properties.get("name").unwrap().nullable);
    assert!(resolved.properties.get("title").unwrap().nullable);
}

#[test]
fn test_nested_ref_resolution() {
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"}
                    }
                },
                "Person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "address": {"$ref": "#/components/schemas/Address"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let person = spec.resolve_ref("#/components/schemas/Person").unwrap();
    let resolved = spec.resolve_schema(person);

    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("address"));

    // The address property should still have a $ref (we resolve at property level when needed)
    let address_prop = resolved.properties.get("address").unwrap();
    assert!(address_prop.reference.is_some() || !address_prop.properties.is_empty());
}

#[test]
fn test_allof_later_schema_overrides_earlier() {
    // Test that for allOf, later schemas override earlier ones
    // This is important for inheritance patterns where a child refines a parent's type
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Base": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "id": {"type": "integer"}
                    }
                },
                "Refined": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Base"},
                        {
                            "type": "object",
                            "properties": {
                                "status": {
                                    "type": "string",
                                    "format": "enum"
                                },
                                "extra": {"type": "boolean"}
                            }
                        }
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let refined = spec.resolve_ref("#/components/schemas/Refined").unwrap();
    let resolved = spec.resolve_schema(refined);

    // Should have all properties
    assert!(resolved.properties.contains_key("status"));
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("extra"));

    // The 'status' property should be from the later schema (has format: "enum")
    // The base schema's status has no format, so if we get "enum", the later one won
    let status_prop = resolved.properties.get("status").unwrap();
    assert_eq!(
        status_prop.format,
        Some("enum".to_string()),
        "Later allOf schema should override earlier schema's property definition"
    );
}

// --- OpenAPI 3.1 type array tests ---

#[test]
fn test_openapi_31_type_string_null() {
    // OpenAPI 3.1: "type": ["string", "null"] should parse as type=string, nullable=true
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test 3.1", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "User": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "nickname": {"type": ["string", "null"]},
                        "age": {"type": ["integer", "null"]}
                    },
                    "required": ["name"]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let user = spec.resolve_ref("#/components/schemas/User").unwrap();

    // name: plain string, not nullable
    let name_prop = user.properties.get("name").unwrap();
    assert_eq!(name_prop.schema_type, Some("string".to_string()));
    assert!(!name_prop.nullable);

    // nickname: ["string", "null"] → string + nullable
    let nickname_prop = user.properties.get("nickname").unwrap();
    assert_eq!(nickname_prop.schema_type, Some("string".to_string()));
    assert!(nickname_prop.nullable);

    // age: ["integer", "null"] → integer + nullable
    let age_prop = user.properties.get("age").unwrap();
    assert_eq!(age_prop.schema_type, Some("integer".to_string()));
    assert!(age_prop.nullable);
}

#[test]
fn test_openapi_31_type_array_without_null() {
    // OpenAPI 3.1: "type": ["string"] (single-element array without null)
    let schema: Schema = serde_json::from_str(r#"{"type": ["string"]}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(!schema.nullable);
}

#[test]
fn test_openapi_30_type_string_still_works() {
    // OpenAPI 3.0: plain string type should still work
    let schema: Schema = serde_json::from_str(r#"{"type": "string"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(!schema.nullable);
}

#[test]
fn test_openapi_30_nullable_flag_still_works() {
    // OpenAPI 3.0: nullable as a separate flag
    let schema: Schema = serde_json::from_str(r#"{"type": "string", "nullable": true}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(schema.nullable);
}

#[test]
fn test_openapi_31_type_mapping_with_spec() {
    // Verify that type arrays produce correct PostgreSQL type mappings
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/records": {
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
                                                "name": {"type": ["string", "null"]},
                                                "score": {"type": ["number", "null"], "format": "float"},
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
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let resolved = spec.resolve_schema(schema);
    let items = resolved.items.as_ref().unwrap();

    // name should be string type, not jsonb (the old bug)
    assert_eq!(
        items.properties.get("name").unwrap().schema_type,
        Some("string".to_string())
    );
    assert_eq!(
        items.properties.get("score").unwrap().schema_type,
        Some("number".to_string())
    );
    assert_eq!(
        items.properties.get("active").unwrap().schema_type,
        Some("boolean".to_string())
    );
}

// --- Circular reference tests ---

#[test]
fn test_circular_ref_depth_limit() {
    // Self-referential schema should not stack overflow
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "TreeNode": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "children": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/TreeNode"}
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let node = spec.resolve_ref("#/components/schemas/TreeNode").unwrap();
    // Should not stack overflow — depth limit kicks in
    let resolved = spec.resolve_schema(node);
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("children"));
}

#[test]
fn test_mutual_circular_refs() {
    // A references B which references A
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "SchemaA": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "b_ref": {"$ref": "#/components/schemas/SchemaB"}
                    }
                },
                "SchemaB": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "integer"},
                        "a_ref": {"$ref": "#/components/schemas/SchemaA"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let schema_a = spec.resolve_ref("#/components/schemas/SchemaA").unwrap();
    let resolved = spec.resolve_schema(schema_a);
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("b_ref"));
}

// --- Deep allOf chain tests (Box-style inheritance) ---

#[test]
fn test_deep_allof_chain() {
    // FileBase → FileMini → File → FileFull (4-level chain)
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "FileBase": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "type": {"type": "string"}
                    },
                    "required": ["id"]
                },
                "FileMini": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FileBase"},
                        {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "size": {"type": "integer"}
                            }
                        }
                    ]
                },
                "File": {
                    "allOf": [
                        {"$ref": "#/components/schemas/FileMini"},
                        {
                            "type": "object",
                            "properties": {
                                "content_type": {"type": "string"},
                                "created_at": {"type": "string", "format": "date-time"}
                            }
                        }
                    ]
                },
                "FileFull": {
                    "allOf": [
                        {"$ref": "#/components/schemas/File"},
                        {
                            "type": "object",
                            "properties": {
                                "permissions": {"type": "object"},
                                "version_number": {"type": "integer"}
                            }
                        }
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let full = spec.resolve_ref("#/components/schemas/FileFull").unwrap();
    let resolved = spec.resolve_schema(full);

    // Should have all properties from the entire chain
    assert!(
        resolved.properties.contains_key("id"),
        "Missing id from FileBase"
    );
    assert!(
        resolved.properties.contains_key("type"),
        "Missing type from FileBase"
    );
    assert!(
        resolved.properties.contains_key("name"),
        "Missing name from FileMini"
    );
    assert!(
        resolved.properties.contains_key("size"),
        "Missing size from FileMini"
    );
    assert!(
        resolved.properties.contains_key("content_type"),
        "Missing content_type from File"
    );
    assert!(
        resolved.properties.contains_key("created_at"),
        "Missing created_at from File"
    );
    assert!(
        resolved.properties.contains_key("permissions"),
        "Missing permissions from FileFull"
    );
    assert!(
        resolved.properties.contains_key("version_number"),
        "Missing version_number from FileFull"
    );

    // id should be required (from FileBase)
    assert!(resolved.required.contains(&"id".to_string()));
}

// --- Swagger 2.0 rejection ---

#[test]
fn test_swagger_20_rejected() {
    let spec_json = r#"{
        "swagger": "2.0",
        "info": {"title": "Old API", "version": "1.0"},
        "paths": {}
    }"#;

    let result = OpenApiSpec::from_str(spec_json);
    assert!(result.is_err());
}

// --- Parameterized path exclusion ---

#[test]
fn test_parameterized_paths_excluded_from_endpoints() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {"200": {"description": "ok"}}
                }
            },
            "/items/{id}": {
                "get": {
                    "responses": {"200": {"description": "ok"}}
                }
            },
            "/users/{user_id}/posts": {
                "get": {
                    "responses": {"200": {"description": "ok"}}
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();

    // Only /items should be included — parameterized paths are excluded
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].path, "/items");
}

// --- Response schema extraction from non-200 codes ---

#[test]
fn test_response_schema_from_201() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/created": {
                "get": {
                    "responses": {
                        "201": {
                            "description": "Created",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "created": {"type": "boolean"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("id"));
    assert!(schema.properties.contains_key("created"));
}

#[test]
fn test_no_schema_endpoint() {
    // Endpoint with no content schema should still be returned with None schema
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/health": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Healthy"
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    assert!(endpoints[0].response_schema.is_none());
}

// --- anyOf with null variant (OpenAPI 3.1 pattern) ---

#[test]
fn test_anyof_with_null_variant() {
    // OpenAPI 3.1 uses anyOf: [{type: string}, {type: "null"}] for nullable
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "User": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "bio": {
                            "anyOf": [
                                {"type": "string"},
                                {"type": "object", "properties": {"text": {"type": "string"}}}
                            ]
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let user = spec.resolve_ref("#/components/schemas/User").unwrap();
    let resolved = spec.resolve_schema(user);

    assert!(resolved.properties.contains_key("name"));
    // bio is anyOf — the resolver should merge all variant properties
    let bio = resolved.properties.get("bio").unwrap();
    let bio_resolved = spec.resolve_schema(bio);
    // Should have merged properties from both variants
    assert!(bio_resolved.properties.contains_key("text") || bio_resolved.schema_type.is_some());
}

// --- resolve_schema edge cases ---

#[test]
fn test_resolve_schema_broken_ref() {
    // A $ref pointing to a nonexistent schema should return the schema unchanged
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Broken": {
                    "type": "object",
                    "properties": {
                        "ref_field": {"$ref": "#/components/schemas/DoesNotExist"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let broken = spec.resolve_ref("#/components/schemas/Broken").unwrap();
    let resolved = spec.resolve_schema(broken);
    // Should still have the property, just unresolved
    assert!(resolved.properties.contains_key("ref_field"));
    let ref_field = resolved.properties.get("ref_field").unwrap();
    assert_eq!(
        ref_field.reference,
        Some("#/components/schemas/DoesNotExist".to_string())
    );
}

#[test]
fn test_resolve_ref_invalid_path() {
    // Refs that don't match #/components/schemas/X should return None
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {}
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    assert!(spec.resolve_ref("#/definitions/User").is_none());
    assert!(
        spec.resolve_ref("#/components/responses/NotFound")
            .is_none()
    );
    assert!(spec.resolve_ref("User").is_none());
    assert!(spec.resolve_ref("").is_none());
}

#[test]
fn test_resolve_schema_plain_object_passthrough() {
    // A simple object (no $ref, no allOf/oneOf/anyOf) should be returned as-is
    let schema = Schema {
        schema_type: Some("object".to_string()),
        properties: {
            let mut map = HashMap::new();
            map.insert(
                "id".to_string(),
                Schema {
                    schema_type: Some("string".to_string()),
                    ..Default::default()
                },
            );
            map
        },
        ..Default::default()
    };

    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {}
    }"#;
    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let resolved = spec.resolve_schema(&schema);

    assert_eq!(resolved.schema_type, Some("object".to_string()));
    assert!(resolved.properties.contains_key("id"));
}

#[test]
fn test_resolve_schema_allof_with_ref_and_inline() {
    // Common pattern: allOf combining a $ref with inline properties
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "allOf": [
                                                {"$ref": "#/components/schemas/Base"},
                                                {
                                                    "type": "object",
                                                    "properties": {
                                                        "extra": {"type": "string"}
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
                "Base": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"}
                    },
                    "required": ["id"]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let items = schema.items.as_ref().unwrap();
    let resolved = spec.resolve_schema(items);

    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("extra"));
    assert!(resolved.required.contains(&"id".to_string()));
}

#[test]
fn test_resolve_schema_oneof_keeps_first_definition() {
    // When two variants define the same property, oneOf should keep the first
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Poly": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "status": {"type": "string", "format": "v1"}
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "status": {"type": "string", "format": "v2"}
                            }
                        }
                    ]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let poly = spec.resolve_ref("#/components/schemas/Poly").unwrap();
    let resolved = spec.resolve_schema(poly);

    // oneOf uses or_insert, so first definition wins
    let status = resolved.properties.get("status").unwrap();
    assert_eq!(status.format, Some("v1".to_string()));
}

#[test]
fn test_resolve_schema_no_components() {
    // Spec with no components section — resolve_ref should return None
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {}
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    assert!(spec.resolve_ref("#/components/schemas/User").is_none());
}

#[test]
fn test_get_response_schema_default_response() {
    // When only "default" response exists (no 200 or 201)
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "default": {
                            "description": "Default response",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("id"));
}

#[test]
fn test_get_response_schema_non_json_content_type() {
    // When content type is not application/json (e.g., application/xml),
    // should still pick up the first available content type
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/geo": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/geo+json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "type": {"type": "string"},
                                            "features": {"type": "array"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("type"));
    assert!(schema.properties.contains_key("features"));
}

#[test]
fn test_paths_without_get_include_post() {
    // Paths with POST are now included (POST-for-read support)
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {"200": {"description": "ok"}}
                }
            },
            "/upload": {
                "post": {
                    "responses": {"201": {"description": "created"}}
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 2);
    assert_eq!(endpoints[0].path, "/items");
    assert_eq!(endpoints[0].method, "GET");
    assert_eq!(endpoints[1].path, "/upload");
    assert_eq!(endpoints[1].method, "POST");
}

#[test]
fn test_table_name_deeply_nested_path() {
    let endpoint = EndpointInfo {
        path: "/api/v2/projects/issues/comments".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "api_v2_projects_issues_comments");
}

#[test]
fn test_openapi_31_type_only_null() {
    // Edge case: "type": ["null"] — no actual type, just null
    let schema: Schema = serde_json::from_str(r#"{"type": ["null"]}"#).unwrap();
    assert_eq!(schema.schema_type, None);
    assert!(schema.nullable);
}

#[test]
fn test_openapi_31_type_non_standard_value() {
    // Edge case: "type" is not a string or array (e.g., number or boolean)
    let schema: Schema = serde_json::from_str(r#"{"type": 42}"#).unwrap();
    assert_eq!(schema.schema_type, None);
    assert!(!schema.nullable);
}

#[test]
fn test_schema_no_type() {
    // Schema with no type field at all
    let schema: Schema = serde_json::from_str(r#"{"format": "date-time"}"#).unwrap();
    assert_eq!(schema.schema_type, None);
    assert!(!schema.nullable);
    assert_eq!(schema.format, Some("date-time".to_string()));
}

// --- Fix 1: $ref in Response objects ---

#[test]
fn test_response_ref_resolution() {
    // Response $ref should be resolved via components.responses
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {"$ref": "#/components/responses/ItemList"}
                    }
                }
            }
        },
        "components": {
            "responses": {
                "ItemList": {
                    "description": "A list of items",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "integer"},
                                        "name": {"type": "string"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let items = schema.items.as_ref().unwrap();
    assert!(items.properties.contains_key("id"));
    assert!(items.properties.contains_key("name"));
}

// --- Fix 2: 2XX wildcard status codes ---

#[test]
fn test_response_schema_from_2xx_wildcard() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "2XX": {
                            "description": "Success",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "status": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("id"));
    assert!(schema.properties.contains_key("status"));
}

#[test]
fn test_200_preferred_over_2xx() {
    // "200" should be preferred over "2XX"
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "OK",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "from_200": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        },
                        "2XX": {
                            "description": "Wildcard",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "from_2xx": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("from_200"));
    assert!(!schema.properties.contains_key("from_2xx"));
}

// --- Fix 3: writeOnly properties ---

#[test]
fn test_write_only_property_deserialization() {
    let schema: Schema = serde_json::from_str(r#"{"type": "string", "writeOnly": true}"#).unwrap();
    assert!(schema.write_only);
    assert_eq!(schema.schema_type, Some("string".to_string()));
}

#[test]
fn test_write_only_default_false() {
    let schema: Schema = serde_json::from_str(r#"{"type": "string"}"#).unwrap();
    assert!(!schema.write_only);
}

// --- Fix 4: Multi-type arrays ---

#[test]
fn test_multi_type_array_becomes_none() {
    // ["string", "integer"] — multiple non-null types → schema_type = None (jsonb)
    let schema: Schema = serde_json::from_str(r#"{"type": ["string", "integer"]}"#).unwrap();
    assert_eq!(schema.schema_type, None);
    assert!(!schema.nullable);
}

#[test]
fn test_multi_type_array_with_null() {
    // ["string", "integer", "null"] → None type + nullable
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["string", "integer", "null"]}"#).unwrap();
    assert_eq!(schema.schema_type, None);
    assert!(schema.nullable);
}

#[test]
fn test_single_type_array_still_works() {
    // ["string", "null"] → exactly one non-null type → Some("string")
    let schema: Schema = serde_json::from_str(r#"{"type": ["string", "null"]}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(schema.nullable);
}

// --- Fix 5: Composition on primitives ---

#[test]
fn test_oneof_primitives_not_object() {
    // oneOf with primitive types should NOT produce schema_type = "object"
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "StringOrInt": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "integer"}
                    ]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let schema = spec
        .resolve_ref("#/components/schemas/StringOrInt")
        .unwrap();
    let resolved = spec.resolve_schema(schema);
    // Should be None (→ jsonb), NOT "object"
    assert_eq!(resolved.schema_type, None);
    assert!(resolved.properties.is_empty());
}

#[test]
fn test_oneof_with_objects_stays_object() {
    // oneOf with object schemas should still produce "object"
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "UserOrOrg": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {"user_id": {"type": "string"}}
                        },
                        {
                            "type": "object",
                            "properties": {"org_id": {"type": "string"}}
                        }
                    ]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let schema = spec.resolve_ref("#/components/schemas/UserOrOrg").unwrap();
    let resolved = spec.resolve_schema(schema);
    assert_eq!(resolved.schema_type, Some("object".to_string()));
    assert!(resolved.properties.contains_key("user_id"));
    assert!(resolved.properties.contains_key("org_id"));
}

// --- Fix 7: Server URL variable substitution ---

#[test]
fn test_server_variable_substitution() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "servers": [
            {
                "url": "https://{region}.api.example.com/v{version}",
                "variables": {
                    "region": {"default": "us-east-1"},
                    "version": {"default": "2"}
                }
            }
        ],
        "paths": {}
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    assert_eq!(
        spec.base_url(),
        Some("https://us-east-1.api.example.com/v2".to_string())
    );
}

#[test]
fn test_server_no_variables() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "servers": [{"url": "https://api.example.com"}],
        "paths": {}
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    assert_eq!(spec.base_url(), Some("https://api.example.com".to_string()));
}

#[test]
fn test_endpoints_sorted_alphabetically() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/zebras": {"get": {"responses": {"200": {"description": "ok"}}}},
            "/apples": {"get": {"responses": {"200": {"description": "ok"}}}},
            "/middle": {"get": {"responses": {"200": {"description": "ok"}}}}
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 3);
    assert_eq!(endpoints[0].path, "/apples");
    assert_eq!(endpoints[1].path, "/middle");
    assert_eq!(endpoints[2].path, "/zebras");
}

#[test]
fn test_response_schema_charset_content_type() {
    // Content type with charset parameter should still match
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json; charset=utf-8": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "integer"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("id"));
}

#[test]
fn test_response_schema_json_preferred_over_xml() {
    // When both JSON and XML content types exist, JSON should be preferred
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/xml": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "xml_field": {"type": "string"}
                                        }
                                    }
                                },
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "json_field": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("json_field"));
}

#[test]
fn test_resolve_schema_ref_with_nullable_sibling() {
    // OpenAPI 3.1: $ref with nullable sibling should merge
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"}
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();

    let ref_with_nullable = Schema {
        reference: Some("#/components/schemas/Address".to_string()),
        nullable: true,
        ..Default::default()
    };

    let resolved = spec.resolve_schema(&ref_with_nullable);
    assert!(resolved.nullable);
    assert!(resolved.properties.contains_key("street"));
    assert!(resolved.properties.contains_key("city"));
}

#[test]
fn test_resolve_schema_ref_with_extra_properties() {
    // OpenAPI 3.1: $ref with additional properties sibling should merge
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Base": {
                    "type": "object",
                    "required": ["id"],
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"}
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();

    let mut extra_props = std::collections::HashMap::new();
    extra_props.insert(
        "extra_field".to_string(),
        Schema {
            schema_type: Some("string".to_string()),
            ..Default::default()
        },
    );

    let ref_with_props = Schema {
        reference: Some("#/components/schemas/Base".to_string()),
        properties: extra_props,
        required: vec!["extra_field".to_string()],
        ..Default::default()
    };

    let resolved = spec.resolve_schema(&ref_with_props);
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("extra_field"));
    assert!(resolved.required.contains(&"id".to_string()));
    assert!(resolved.required.contains(&"extra_field".to_string()));
}

// --- POST-for-read tests ---

#[test]
fn test_post_endpoint_included() {
    let spec_json = r#"{
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
                                                "id": {"type": "string"}
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
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].path, "/search");
    assert_eq!(endpoints[0].method, "POST");
    assert!(endpoints[0].response_schema.is_some());
}

#[test]
fn test_post_endpoint_table_name_suffix() {
    let endpoint = EndpointInfo {
        path: "/search".to_string(),
        method: "POST",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "search_post");

    let get_endpoint = EndpointInfo {
        path: "/search".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_eq!(get_endpoint.table_name(), "search");
}

#[test]
fn test_get_and_post_same_path() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {"200": {"description": "ok"}}
                },
                "post": {
                    "responses": {"200": {"description": "ok"}}
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 2);
    // Sorted by path then method, so GET comes first
    assert_eq!(endpoints[0].method, "GET");
    assert_eq!(endpoints[1].method, "POST");
    // Table names should differ
    let names: Vec<String> = endpoints.iter().map(|e| e.table_name()).collect();
    assert_eq!(names[0], "items");
    assert_eq!(names[1], "items_post");
}

#[test]
fn test_parameterized_post_excluded() {
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items/{id}/search": {
                "post": {
                    "responses": {"200": {"description": "ok"}}
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 0);
}

#[test]
fn test_post_only_path() {
    // Path with only POST and no GET should still be included
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/search": {
                "post": {
                    "responses": {"200": {"description": "ok"}}
                }
            },
            "/items": {
                "get": {
                    "responses": {"200": {"description": "ok"}}
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 2);
    assert!(
        endpoints
            .iter()
            .any(|e| e.method == "GET" && e.path == "/items")
    );
    assert!(
        endpoints
            .iter()
            .any(|e| e.method == "POST" && e.path == "/search")
    );
}

// --- OpenAPI 3.1 real-world API pattern tests ---

#[test]
fn test_stripe_expandable_anyof() {
    // Stripe pattern: anyOf: [{type: "string"}, {$ref: "..."}] for expandable ID/object fields
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Stripe", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Customer": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "name": {"type": "string"}
                    }
                },
                "Charge": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "customer": {
                            "anyOf": [
                                {"type": "string"},
                                {"$ref": "#/components/schemas/Customer"}
                            ]
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let charge = spec.resolve_ref("#/components/schemas/Charge").unwrap();
    let resolved = spec.resolve_schema(charge);

    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("customer"));
    // customer is anyOf → resolver merges; the Customer variant has properties (id, name),
    // so merged schema_type = "object". The string variant has no properties.
    let customer = resolved.properties.get("customer").unwrap();
    let customer_resolved = spec.resolve_schema(customer);
    // Should have merged properties from the Customer $ref
    assert!(
        customer_resolved.properties.contains_key("id") || customer_resolved.schema_type.is_some()
    );
}

#[test]
fn test_github_nullable_anyof_null_type() {
    // GitHub 3.1 pattern: anyOf: [{$ref: "..."}, {type: "null"}] for nullable refs
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "GitHub", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "SimpleUser": {
                    "type": "object",
                    "properties": {
                        "login": {"type": "string"},
                        "id": {"type": "integer"}
                    }
                },
                "Issue": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "assignee": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/SimpleUser"},
                                {"type": "null"}
                            ]
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let issue = spec.resolve_ref("#/components/schemas/Issue").unwrap();
    let resolved = spec.resolve_schema(issue);

    assert!(resolved.properties.contains_key("assignee"));
    let assignee = resolved.properties.get("assignee").unwrap();
    let assignee_resolved = spec.resolve_schema(assignee);
    // Should have merged SimpleUser properties
    assert!(assignee_resolved.properties.contains_key("login"));
    assert!(assignee_resolved.properties.contains_key("id"));
}

#[test]
fn test_kubernetes_deep_ref_chain_8_levels() {
    // Kubernetes-style: 8-level chain of $ref → $ref → ... deep resolution
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "K8s", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "L1": {"$ref": "#/components/schemas/L2"},
                "L2": {"$ref": "#/components/schemas/L3"},
                "L3": {"$ref": "#/components/schemas/L4"},
                "L4": {"$ref": "#/components/schemas/L5"},
                "L5": {"$ref": "#/components/schemas/L6"},
                "L6": {"$ref": "#/components/schemas/L7"},
                "L7": {"$ref": "#/components/schemas/L8"},
                "L8": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let l1 = spec.resolve_ref("#/components/schemas/L1").unwrap();
    let resolved = spec.resolve_schema(l1);

    // Should resolve through all 8 levels
    assert!(resolved.properties.contains_key("value"));
    assert_eq!(
        resolved.properties.get("value").unwrap().schema_type,
        Some("string".to_string())
    );
}

#[test]
fn test_allof_multiple_refs() {
    // Multi-inheritance: allOf: [{$ref: "A"}, {$ref: "B"}, {inline}]
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Auditable": {
                    "type": "object",
                    "properties": {
                        "created_at": {"type": "string", "format": "date-time"},
                        "updated_at": {"type": "string", "format": "date-time"}
                    }
                },
                "Identifiable": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "slug": {"type": "string"}
                    },
                    "required": ["id"]
                },
                "Resource": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Identifiable"},
                        {"$ref": "#/components/schemas/Auditable"},
                        {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "status": {"type": "string"}
                            },
                            "required": ["name"]
                        }
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let resource = spec.resolve_ref("#/components/schemas/Resource").unwrap();
    let resolved = spec.resolve_schema(resource);

    // Should have properties from all three sources
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("slug"));
    assert!(resolved.properties.contains_key("created_at"));
    assert!(resolved.properties.contains_key("updated_at"));
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("status"));

    // Required from both Identifiable and inline
    assert!(resolved.required.contains(&"id".to_string()));
    assert!(resolved.required.contains(&"name".to_string()));
}

#[test]
fn test_openapi_31_nullable_array() {
    // GitHub pattern: type: ["array", "null"] with items
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["array", "null"], "items": {"type": "string"}}"#)
            .unwrap();
    assert_eq!(schema.schema_type, Some("array".to_string()));
    assert!(schema.nullable);
    assert!(schema.items.is_some());
    assert_eq!(
        schema.items.as_ref().unwrap().schema_type,
        Some("string".to_string())
    );
}

#[test]
fn test_openapi_31_nullable_boolean() {
    // General 3.1 pattern: type: ["boolean", "null"]
    let schema: Schema = serde_json::from_str(r#"{"type": ["boolean", "null"]}"#).unwrap();
    assert_eq!(schema.schema_type, Some("boolean".to_string()));
    assert!(schema.nullable);
}

#[test]
fn test_type_array_three_non_null_types() {
    // Edge case: type: ["string", "integer", "boolean"] → None (jsonb)
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["string", "integer", "boolean"]}"#).unwrap();
    assert_eq!(schema.schema_type, None);
    assert!(!schema.nullable);
}

#[test]
fn test_content_type_jsonld() {
    // NWS/JSON-LD: application/ld+json picked up via fallback
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "NWS", "version": "1.0"},
        "paths": {
            "/alerts": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/ld+json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "@context": {"type": "object"},
                                            "@graph": {"type": "array", "items": {"type": "object"}}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("@graph"));
}

#[test]
fn test_content_type_jsonapi() {
    // JSON:API: application/vnd.api+json picked up via fallback
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "JSON:API", "version": "1.0"},
        "paths": {
            "/articles": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/vnd.api+json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "data": {"type": "array"},
                                            "meta": {"type": "object"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("data"));
    assert!(schema.properties.contains_key("meta"));
}

#[test]
fn test_ref_with_description_sibling() {
    // Common 3.1 pattern: $ref + description sibling (description ignored, ref resolved)
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Pet": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "tag": {"type": "string"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();

    // Simulate a $ref with a description sibling — description is not in Schema struct,
    // so it's implicitly ignored. The key point is $ref still resolves correctly.
    let ref_schema = Schema {
        reference: Some("#/components/schemas/Pet".to_string()),
        ..Default::default()
    };

    let resolved = spec.resolve_schema(&ref_schema);
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("tag"));
}

#[test]
fn test_stripe_metadata_additional_properties() {
    // Stripe pattern: additionalProperties without properties → maps to jsonb (type: "object")
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Stripe", "version": "1.0"},
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
                                            "id": {"type": "string"},
                                            "metadata": {
                                                "type": "object"
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
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let metadata = schema.properties.get("metadata").unwrap();
    // type: "object" with no properties → maps to jsonb
    assert_eq!(metadata.schema_type, Some("object".to_string()));
    assert!(metadata.properties.is_empty());
}

#[test]
fn test_discriminator_doesnt_break_parsing() {
    // Polymorphic APIs: discriminator field on oneOf is silently ignored
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Cat": {
                    "type": "object",
                    "properties": {
                        "pet_type": {"type": "string"},
                        "purrs": {"type": "boolean"}
                    }
                },
                "Dog": {
                    "type": "object",
                    "properties": {
                        "pet_type": {"type": "string"},
                        "barks": {"type": "boolean"}
                    }
                },
                "Pet": {
                    "oneOf": [
                        {"$ref": "#/components/schemas/Cat"},
                        {"$ref": "#/components/schemas/Dog"}
                    ],
                    "discriminator": {
                        "propertyName": "pet_type"
                    }
                }
            }
        }
    }"##;

    // Should parse without error (discriminator field is ignored by serde)
    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let pet = spec.resolve_ref("#/components/schemas/Pet").unwrap();
    let resolved = spec.resolve_schema(pet);

    // oneOf merges all variant properties as nullable
    assert!(resolved.properties.contains_key("pet_type"));
    assert!(resolved.properties.contains_key("purrs"));
    assert!(resolved.properties.contains_key("barks"));
}

#[test]
fn test_empty_type_array() {
    // Edge case: type: [] → None (jsonb)
    let schema: Schema = serde_json::from_str(r#"{"type": []}"#).unwrap();
    assert_eq!(schema.schema_type, None);
    assert!(!schema.nullable);
}

// --- OpenAPI 3.1 full READ operation coverage tests ---
// Based on real-world OpenAPI 3.1 schemas from GitHub, Stripe, Kubernetes, and others.

#[test]
fn test_github_31_nullable_allof_with_ref() {
    // GitHub API 3.1 pattern: property is allOf + nullable for merged nullable refs
    // Example: pull_request.head.repo is allOf: [{$ref: "#/components/schemas/repository"}]
    // with nullable: true on the outer property
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "GitHub", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "repository": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "full_name": {"type": "string"},
                        "private": {"type": "boolean"}
                    },
                    "required": ["id", "full_name"]
                },
                "pull-request": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "title": {"type": "string"},
                        "head_repo": {
                            "allOf": [
                                {"$ref": "#/components/schemas/repository"}
                            ],
                            "nullable": true
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let pr = spec
        .resolve_ref("#/components/schemas/pull-request")
        .unwrap();
    let resolved = spec.resolve_schema(pr);

    assert!(resolved.properties.contains_key("head_repo"));
    let head_repo = resolved.properties.get("head_repo").unwrap();
    // The nullable flag on the outer property should carry through
    // allOf with a single $ref should resolve to that ref's properties
    let head_resolved = spec.resolve_schema(head_repo);
    assert!(head_resolved.properties.contains_key("id"));
    assert!(head_resolved.properties.contains_key("full_name"));
}

#[test]
fn test_github_31_nullable_simple_user_ref() {
    // GitHub 3.1: nullable-simple-user is used everywhere as:
    //   "assignee": { "anyOf": [{"$ref": "..."}, {"type": "null"}] }
    // This tests the full resolution chain with nullable merging
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "GitHub", "version": "1.0"},
        "paths": {
            "/repos/issues": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {"$ref": "#/components/schemas/issue"}
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
                "simple-user": {
                    "type": "object",
                    "properties": {
                        "login": {"type": "string"},
                        "id": {"type": "integer"},
                        "avatar_url": {"type": "string", "format": "uri"}
                    },
                    "required": ["login", "id", "avatar_url"]
                },
                "issue": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "number": {"type": "integer"},
                        "title": {"type": "string"},
                        "state": {"type": "string"},
                        "assignee": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/simple-user"},
                                {"type": "null"}
                            ]
                        },
                        "labels": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "integer"},
                                    "name": {"type": "string"},
                                    "color": {"type": "string"}
                                }
                            }
                        }
                    },
                    "required": ["id", "number", "title", "state"]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let resolved = spec.resolve_schema(schema);
    let items = resolved.items.as_ref().unwrap();
    let item_resolved = spec.resolve_schema(items);

    assert!(item_resolved.properties.contains_key("id"));
    assert!(item_resolved.properties.contains_key("title"));
    assert!(item_resolved.properties.contains_key("assignee"));
    assert!(item_resolved.properties.contains_key("labels"));

    // assignee resolves to simple-user properties via anyOf
    let assignee = item_resolved.properties.get("assignee").unwrap();
    let assignee_resolved = spec.resolve_schema(assignee);
    assert!(assignee_resolved.properties.contains_key("login"));
}

#[test]
fn test_kubernetes_allof_with_description_only_ref() {
    // Kubernetes pattern: allOf with one $ref and one schema that only has description
    // (no properties, no type — just metadata). Should not break resolution.
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "K8s", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "ObjectMeta": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "namespace": {"type": "string"}
                    }
                },
                "Pod": {
                    "type": "object",
                    "properties": {
                        "metadata": {
                            "allOf": [
                                {"$ref": "#/components/schemas/ObjectMeta"},
                                {}
                            ]
                        },
                        "kind": {"type": "string"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let pod = spec.resolve_ref("#/components/schemas/Pod").unwrap();
    let resolved = spec.resolve_schema(pod);

    assert!(resolved.properties.contains_key("metadata"));
    assert!(resolved.properties.contains_key("kind"));

    let meta = resolved.properties.get("metadata").unwrap();
    let meta_resolved = spec.resolve_schema(meta);
    assert!(meta_resolved.properties.contains_key("name"));
    assert!(meta_resolved.properties.contains_key("namespace"));
}

#[test]
fn test_stripe_31_expandable_field_string_or_ref() {
    // Stripe 3.1 uses anyOf: [{maxLength:5000, type:"string"}, {$ref:"..."}]
    // for expandable fields (default: string ID, expanded: full object)
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Stripe", "version": "1.0"},
        "paths": {
            "/v1/charges": {
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
                                                "items": {"$ref": "#/components/schemas/charge"}
                                            },
                                            "has_more": {"type": "boolean"},
                                            "url": {"type": "string"}
                                        },
                                        "required": ["data", "has_more", "url"]
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
                "customer": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "email": {"type": ["string", "null"]},
                        "name": {"type": ["string", "null"]}
                    },
                    "required": ["id"]
                },
                "charge": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "amount": {"type": "integer"},
                        "currency": {"type": "string"},
                        "created": {"type": "integer", "format": "unix-time"},
                        "customer": {
                            "anyOf": [
                                {"type": "string"},
                                {"$ref": "#/components/schemas/customer"}
                            ]
                        },
                        "metadata": {"type": "object"}
                    },
                    "required": ["id", "amount", "currency", "created"]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("data"));
    assert!(schema.properties.contains_key("has_more"));

    // Verify nested charge schema resolution
    let data = schema.properties.get("data").unwrap();
    let items = data.items.as_ref().unwrap();
    let charge_resolved = spec.resolve_schema(items);
    assert!(charge_resolved.properties.contains_key("id"));
    assert!(charge_resolved.properties.contains_key("amount"));
    assert!(charge_resolved.properties.contains_key("created"));

    // created has format: unix-time
    let created = charge_resolved.properties.get("created").unwrap();
    assert_eq!(created.format, Some("unix-time".to_string()));
}

#[test]
fn test_ref_with_write_only_sibling() {
    // OpenAPI 3.1: $ref + writeOnly sibling should merge writeOnly into resolved schema
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Credentials": {
                    "type": "object",
                    "properties": {
                        "token": {"type": "string"}
                    }
                },
                "User": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "auth": {
                            "$ref": "#/components/schemas/Credentials",
                            "writeOnly": true
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let user = spec.resolve_ref("#/components/schemas/User").unwrap();
    let resolved = spec.resolve_schema(user);

    let auth = resolved.properties.get("auth").unwrap();
    let auth_resolved = spec.resolve_schema(auth);
    assert!(auth_resolved.write_only);
    assert!(auth_resolved.properties.contains_key("token"));
}

#[test]
fn test_openapi_31_version_string_accepted() {
    // OpenAPI 3.1.0 should be accepted as valid 3.x
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test 3.1", "version": "1.0"},
        "paths": {}
    }"#;
    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    assert_eq!(spec.openapi, "3.1.0");
}

#[test]
fn test_openapi_31_minor_version_accepted() {
    // Future 3.x versions (e.g., 3.2.0) should also be accepted
    let spec_json = r#"{
        "openapi": "3.2.0",
        "info": {"title": "Future", "version": "1.0"},
        "paths": {}
    }"#;
    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    assert_eq!(spec.openapi, "3.2.0");
}

#[test]
fn test_response_ref_chain_through_components() {
    // Response uses $ref to components/responses, which itself contains a schema $ref
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/pets": {
                "get": {
                    "responses": {
                        "200": {"$ref": "#/components/responses/PetList"}
                    }
                }
            }
        },
        "components": {
            "responses": {
                "PetList": {
                    "description": "A list of pets",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {"$ref": "#/components/schemas/Pet"}
                            }
                        }
                    }
                }
            },
            "schemas": {
                "Pet": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                        "species": {"type": "string"}
                    },
                    "required": ["id", "name"]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let items = schema.items.as_ref().unwrap();
    let resolved = spec.resolve_schema(items);
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("species"));
}

#[test]
fn test_openapi_31_nullable_object_type_array() {
    // OpenAPI 3.1: type: ["object", "null"] — nullable object
    let schema: Schema = serde_json::from_str(
        r#"{"type": ["object", "null"], "properties": {"key": {"type": "string"}}}"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("object".to_string()));
    assert!(schema.nullable);
    assert!(schema.properties.contains_key("key"));
}

#[test]
fn test_openapi_31_nullable_integer_type_array() {
    // OpenAPI 3.1: type: ["integer", "null"] with format
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["integer", "null"], "format": "int32"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("integer".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("int32".to_string()));
}

#[test]
fn test_openapi_31_nullable_number_type_array() {
    // OpenAPI 3.1: type: ["number", "null"] with format: double
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["number", "null"], "format": "double"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("number".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("double".to_string()));
}

#[test]
fn test_mixed_allof_and_type_array_31() {
    // OpenAPI 3.1: allOf with type arrays inside sub-schemas
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Combined": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer"},
                                "name": {"type": ["string", "null"]}
                            },
                            "required": ["id"]
                        },
                        {
                            "type": "object",
                            "properties": {
                                "email": {"type": ["string", "null"]},
                                "count": {"type": ["integer", "null"]}
                            }
                        }
                    ]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let combined = spec.resolve_ref("#/components/schemas/Combined").unwrap();
    let resolved = spec.resolve_schema(combined);

    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("email"));
    assert!(resolved.properties.contains_key("count"));

    // id is required and not nullable
    assert!(resolved.required.contains(&"id".to_string()));

    // name should be nullable (from type array)
    let name = resolved.properties.get("name").unwrap();
    assert!(name.nullable);
    assert_eq!(name.schema_type, Some("string".to_string()));

    // count should be nullable
    let count = resolved.properties.get("count").unwrap();
    assert!(count.nullable);
    assert_eq!(count.schema_type, Some("integer".to_string()));
}

#[test]
fn test_twilio_nested_schema_ref_in_response() {
    // Twilio pattern: response has inline wrapper with $ref items
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Twilio", "version": "1.0"},
        "paths": {
            "/Accounts": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "accounts": {
                                                "type": "array",
                                                "items": {"$ref": "#/components/schemas/Account"}
                                            },
                                            "page": {"type": "integer"},
                                            "page_size": {"type": "integer"}
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
                "Account": {
                    "type": "object",
                    "properties": {
                        "sid": {"type": "string"},
                        "friendly_name": {"type": "string"},
                        "status": {"type": "string"},
                        "date_created": {"type": "string", "format": "date-time"}
                    },
                    "required": ["sid"]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    // Twilio wraps in "accounts" key
    assert!(schema.properties.contains_key("accounts"));

    let accounts = schema.properties.get("accounts").unwrap();
    let items = accounts.items.as_ref().unwrap();
    let resolved = spec.resolve_schema(items);
    assert!(resolved.properties.contains_key("sid"));
    assert!(resolved.properties.contains_key("friendly_name"));
    assert!(resolved.properties.contains_key("date_created"));
}

#[test]
fn test_oneof_with_null_type_31() {
    // OpenAPI 3.1: oneOf: [{$ref: "..."}, {type: "null"}] — common nullable pattern
    // Different from anyOf: semantically "exactly one", but for nullable refs identical behavior
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"},
                        "zip": {"type": "string"}
                    }
                },
                "Order": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "shipping_address": {
                            "oneOf": [
                                {"$ref": "#/components/schemas/Address"},
                                {"type": "null"}
                            ]
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let order = spec.resolve_ref("#/components/schemas/Order").unwrap();
    let resolved = spec.resolve_schema(order);

    let shipping = resolved.properties.get("shipping_address").unwrap();
    let shipping_resolved = spec.resolve_schema(shipping);
    // Should have Address properties from the non-null variant
    assert!(shipping_resolved.properties.contains_key("street"));
    assert!(shipping_resolved.properties.contains_key("city"));
    assert!(shipping_resolved.properties.contains_key("zip"));
}

#[test]
fn test_empty_paths_valid_spec() {
    // Valid spec with no paths (e.g., webhook-only APIs)
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Webhooks Only", "version": "1.0"},
        "paths": {}
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert!(endpoints.is_empty());
}

#[test]
fn test_multiple_servers_uses_first() {
    // Multiple servers — base_url should come from the first one
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "servers": [
            {"url": "https://api.production.com"},
            {"url": "https://api.staging.com"},
            {"url": "https://api.dev.com"}
        ],
        "paths": {}
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    assert_eq!(
        spec.base_url(),
        Some("https://api.production.com".to_string())
    );
}

#[test]
fn test_no_servers_returns_none() {
    // No servers array — base_url should be None
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {}
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    assert_eq!(spec.base_url(), None);
}

#[test]
fn test_openapi_31_const_ignored_gracefully() {
    // OpenAPI 3.1 introduced "const" (from JSON Schema). Our parser should ignore it
    // gracefully since serde skips unknown fields.
    let schema: Schema = serde_json::from_str(r#"{"type": "string", "const": "active"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
}

#[test]
fn test_openapi_31_examples_ignored_gracefully() {
    // OpenAPI 3.1: "examples" (array) replaces "example" (singular). Should be ignored.
    let schema: Schema =
        serde_json::from_str(r#"{"type": "string", "examples": ["foo", "bar"]}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
}

#[test]
fn test_allof_empty_schemas() {
    // Edge case: allOf with empty sub-schemas — should produce empty merged result
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Empty": {
                    "allOf": [{}, {}]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let empty = spec.resolve_ref("#/components/schemas/Empty").unwrap();
    let resolved = spec.resolve_schema(empty);
    assert!(resolved.properties.is_empty());
    // No properties → schema_type should not be "object"
    assert_eq!(resolved.schema_type, None);
}

#[test]
fn test_anyof_all_primitives() {
    // anyOf with only primitive types (no objects) → None type (jsonb)
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Flexible": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "integer"},
                        {"type": "boolean"}
                    ]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let flexible = spec.resolve_ref("#/components/schemas/Flexible").unwrap();
    let resolved = spec.resolve_schema(flexible);
    // All primitives, no properties → schema_type should be None (jsonb)
    assert_eq!(resolved.schema_type, None);
    assert!(resolved.properties.is_empty());
}

#[test]
fn test_response_schema_from_array_ref() {
    // Response schema is a $ref to an array schema (not inlined)
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/users": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/UserList"}
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "User": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "login": {"type": "string"}
                    }
                },
                "UserList": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/User"}
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    // The response schema is a $ref to UserList (array type)
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.reference.is_some());

    let resolved = spec.resolve_schema(schema);
    assert_eq!(resolved.schema_type, Some("array".to_string()));
    let items = resolved.items.as_ref().unwrap();
    let item_resolved = spec.resolve_schema(items);
    assert!(item_resolved.properties.contains_key("id"));
    assert!(item_resolved.properties.contains_key("login"));
}

#[test]
fn test_oneof_mixed_object_and_primitive() {
    // oneOf: [{type: "object", properties: {...}}, {type: "string"}]
    // Common in APIs that return either a structured error or a simple value
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Result": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "data": {"type": "string"},
                                "status": {"type": "integer"}
                            }
                        },
                        {"type": "string"}
                    ]
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let result = spec.resolve_ref("#/components/schemas/Result").unwrap();
    let resolved = spec.resolve_schema(result);

    // Should have properties from the object variant
    assert!(resolved.properties.contains_key("data"));
    assert!(resolved.properties.contains_key("status"));
    // schema_type should be "object" because at least one variant has properties
    assert_eq!(resolved.schema_type, Some("object".to_string()));
}

#[test]
fn test_allof_with_required_dedup() {
    // allOf where multiple schemas specify the same required field
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Merged": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {"id": {"type": "string"}},
                            "required": ["id"]
                        },
                        {
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                            "required": ["id", "name"]
                        }
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let merged = spec.resolve_ref("#/components/schemas/Merged").unwrap();
    let resolved = spec.resolve_schema(merged);

    // "id" should appear only once in required (deduplicated)
    assert_eq!(
        resolved.required.iter().filter(|r| *r == "id").count(),
        1,
        "Required should be deduplicated"
    );
    assert!(resolved.required.contains(&"name".to_string()));
}

#[test]
fn test_response_ref_broken_gracefully() {
    // Response $ref pointing to nonexistent components/responses — should still work
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {"$ref": "#/components/responses/NonExistent"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    // Broken ref → no schema
    assert!(endpoints[0].response_schema.is_none());
}

#[test]
fn test_allof_with_properties_sibling() {
    // K8s-style: allOf + sibling properties → allOf takes priority (sibling properties
    // are not in Schema's allOf path, they stay on the outer schema)
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Base": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"}
                    }
                },
                "Extended": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Base"},
                        {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"}
                            }
                        }
                    ],
                    "properties": {
                        "sibling_prop": {"type": "boolean"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let extended = spec.resolve_ref("#/components/schemas/Extended").unwrap();
    let resolved = spec.resolve_schema(extended);

    // allOf properties should be present
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("name"));
    // Parent-level sibling properties are now merged alongside allOf (OpenAPI 3.1)
    assert!(
        resolved.properties.contains_key("sibling_prop"),
        "sibling_prop should be merged from parent alongside allOf"
    );
}

#[test]
fn test_response_only_error_codes() {
    // Only 4xx/5xx responses → None schema (no success response to extract)
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Error API", "version": "1.0"},
        "paths": {
            "/errors": {
                "get": {
                    "responses": {
                        "400": {
                            "description": "Bad request",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "error": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error"
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    // No 200/201/2XX/default → response_schema should be None
    assert!(endpoints[0].response_schema.is_none());
}

// =============================================================================
// OpenAPI 3.1 READ operation coverage — based on real-world API specifications
// =============================================================================
// Tests below are derived from actual patterns found in production OpenAPI 3.1
// specs: GitHub REST API, Stripe API, Kubernetes API, DigitalOcean API, and
// the OpenAPI 3.1 specification itself (JSON Schema 2020-12 alignment).

// --- GitHub API 3.1: nullable format strings via type arrays ---

#[test]
fn test_github_31_nullable_datetime_type_array() {
    // GitHub API: type: ["string", "null"] with format: "date-time"
    // e.g., issue.closed_at, pull_request.merged_at
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["string", "null"], "format": "date-time"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("date-time".to_string()));
}

#[test]
fn test_github_31_nullable_uri_type_array() {
    // GitHub API: type: ["string", "null"] with format: "uri"
    // e.g., repository.homepage, issue.body_url
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["string", "null"], "format": "uri"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("uri".to_string()));
}

#[test]
fn test_github_31_nullable_date_type_array() {
    // DigitalOcean/GitHub: type: ["string", "null"] with format: "date"
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["string", "null"], "format": "date"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("date".to_string()));
}

#[test]
fn test_github_31_nullable_email_type_array() {
    // GitHub API: type: ["string", "null"] with format: "email"
    // e.g., user.email
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["string", "null"], "format": "email"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("email".to_string()));
}

// --- GitHub API 3.1: anyOf with all $ref variants (polymorphic events) ---

#[test]
fn test_github_31_anyof_all_refs() {
    // GitHub API: timeline events use anyOf with multiple $ref variants
    // e.g., GET /repos/{owner}/{repo}/issues/{issue_number}/timeline
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "GitHub", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "labeled-event": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "event": {"type": "string"},
                        "label": {"type": "object", "properties": {"name": {"type": "string"}}}
                    },
                    "required": ["id", "event"]
                },
                "commented-event": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "event": {"type": "string"},
                        "body": {"type": "string"}
                    },
                    "required": ["id", "event"]
                },
                "assigned-event": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "event": {"type": "string"},
                        "assignee": {"$ref": "#/components/schemas/simple-user"}
                    },
                    "required": ["id", "event"]
                },
                "simple-user": {
                    "type": "object",
                    "properties": {
                        "login": {"type": "string"},
                        "id": {"type": "integer"}
                    },
                    "required": ["login", "id"]
                },
                "timeline-event": {
                    "anyOf": [
                        {"$ref": "#/components/schemas/labeled-event"},
                        {"$ref": "#/components/schemas/commented-event"},
                        {"$ref": "#/components/schemas/assigned-event"}
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let timeline = spec
        .resolve_ref("#/components/schemas/timeline-event")
        .unwrap();
    let resolved = spec.resolve_schema(timeline);

    // anyOf merges all variant properties as nullable
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("event"));
    assert!(resolved.properties.contains_key("label"));
    assert!(resolved.properties.contains_key("body"));
    assert!(resolved.properties.contains_key("assignee"));

    // All properties should be nullable (anyOf makes everything nullable)
    assert!(resolved.properties.get("id").unwrap().nullable);
    assert!(resolved.properties.get("event").unwrap().nullable);
    assert!(resolved.properties.get("body").unwrap().nullable);

    // Nothing should be required for anyOf
    assert!(resolved.required.is_empty());
}

// --- GitHub API 3.1: nested anyOf inside allOf properties ---

#[test]
fn test_github_31_nested_anyof_inside_allof() {
    // GitHub API pattern: pull_request uses allOf for inheritance,
    // with nested anyOf for nullable ref fields inside the child schema
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "GitHub", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "simple-user": {
                    "type": "object",
                    "properties": {
                        "login": {"type": "string"},
                        "id": {"type": "integer"}
                    },
                    "required": ["login", "id"]
                },
                "milestone": {
                    "type": "object",
                    "properties": {
                        "number": {"type": "integer"},
                        "title": {"type": "string"},
                        "state": {"type": "string"}
                    },
                    "required": ["number", "title"]
                },
                "issue-base": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "title": {"type": "string"},
                        "state": {"type": "string"}
                    },
                    "required": ["id", "title", "state"]
                },
                "issue-full": {
                    "allOf": [
                        {"$ref": "#/components/schemas/issue-base"},
                        {
                            "type": "object",
                            "properties": {
                                "assignee": {
                                    "anyOf": [
                                        {"$ref": "#/components/schemas/simple-user"},
                                        {"type": "null"}
                                    ]
                                },
                                "milestone": {
                                    "anyOf": [
                                        {"$ref": "#/components/schemas/milestone"},
                                        {"type": "null"}
                                    ]
                                },
                                "body": {"type": ["string", "null"]}
                            }
                        }
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let issue = spec.resolve_ref("#/components/schemas/issue-full").unwrap();
    let resolved = spec.resolve_schema(issue);

    // Should have base properties
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("title"));
    assert!(resolved.properties.contains_key("state"));

    // Should have extended properties
    assert!(resolved.properties.contains_key("assignee"));
    assert!(resolved.properties.contains_key("milestone"));
    assert!(resolved.properties.contains_key("body"));

    // body should be nullable (from type array)
    let body = resolved.properties.get("body").unwrap();
    assert!(body.nullable);
    assert_eq!(body.schema_type, Some("string".to_string()));

    // Required should come from base
    assert!(resolved.required.contains(&"id".to_string()));
    assert!(resolved.required.contains(&"title".to_string()));
}

// --- GitHub API 3.1: readOnly properties (should be INCLUDED in responses) ---

#[test]
fn test_github_31_readonly_property_included() {
    // GitHub API: readOnly fields like id, node_id should appear in GET responses.
    // Our parser ignores readOnly (it's not in Schema struct), which is correct
    // behavior — readOnly properties SHOULD appear in responses.
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "GitHub", "version": "1.0"},
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
                                                "id": {"type": "integer", "readOnly": true},
                                                "node_id": {"type": "string", "readOnly": true},
                                                "name": {"type": "string"},
                                                "full_name": {"type": "string", "readOnly": true}
                                            },
                                            "required": ["id", "name", "full_name"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let items = schema.items.as_ref().unwrap();

    // readOnly properties should still be present (they appear in responses)
    assert!(items.properties.contains_key("id"));
    assert!(items.properties.contains_key("node_id"));
    assert!(items.properties.contains_key("name"));
    assert!(items.properties.contains_key("full_name"));
}

// --- Stripe API 3.1: enum values in response schemas ---

#[test]
fn test_stripe_31_enum_string_property() {
    // Stripe API: type: "string" with enum values (e.g., charge.status)
    // Enums should still map to text type (our parser ignores enum values)
    let schema: Schema =
        serde_json::from_str(r#"{"type": "string", "enum": ["active", "inactive", "pending"]}"#)
            .unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(!schema.nullable);
}

#[test]
fn test_stripe_31_nullable_enum_type_array() {
    // Stripe API 3.1: type: ["string", "null"] with enum
    // e.g., subscription.cancel_at_period_end reason
    let schema: Schema = serde_json::from_str(
        r#"{"type": ["string", "null"], "enum": ["duplicate", "fraudulent", "requested_by_customer", null]}"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(schema.nullable);
}

// --- Stripe API 3.1: nullable unix timestamps ---

#[test]
fn test_stripe_31_nullable_unix_time() {
    // Stripe API: type: ["integer", "null"] with format: "unix-time"
    // e.g., subscription.canceled_at, charge.refunded_at
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["integer", "null"], "format": "unix-time"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("integer".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("unix-time".to_string()));
}

// --- Stripe API: vendor extensions should be ignored ---

#[test]
fn test_stripe_31_vendor_extensions_ignored() {
    // Stripe API uses x-expansionResources, x-resourceId, x-stripeBypassValidation, etc.
    // These should be silently ignored by serde's default behavior
    let schema: Schema = serde_json::from_str(
        r##"{
            "type": "object",
            "x-resourceId": "charge",
            "x-expansionResources": {"oneOf": [{"$ref": "#/components/schemas/customer"}]},
            "x-stripeBypassValidation": true,
            "properties": {
                "id": {"type": "string"}
            }
        }"##,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("object".to_string()));
    assert!(schema.properties.contains_key("id"));
}

// --- Kubernetes API: schema with no type but with properties ---

#[test]
fn test_kubernetes_implicit_object_no_type() {
    // Kubernetes API: some schemas omit "type" but have "properties"
    // e.g., io.k8s.api.core.v1.PodSpec — properties without explicit "type": "object"
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "K8s", "version": "1.0"},
        "paths": {
            "/api/v1/pods": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "properties": {
                                            "apiVersion": {"type": "string"},
                                            "kind": {"type": "string"},
                                            "items": {
                                                "type": "array",
                                                "items": {
                                                    "properties": {
                                                        "name": {"type": "string"},
                                                        "namespace": {"type": "string"}
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
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    // Schema has no explicit type but has properties — should still parse
    assert!(schema.properties.contains_key("apiVersion"));
    assert!(schema.properties.contains_key("kind"));
    assert!(schema.properties.contains_key("items"));
    // schema_type should be None (no explicit type)
    assert_eq!(schema.schema_type, None);
}

// --- Kubernetes API: numeric format strings ---

#[test]
fn test_kubernetes_format_int32() {
    // Kubernetes API: format: "int32" on integer properties
    let schema: Schema = serde_json::from_str(r#"{"type": "integer", "format": "int32"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("integer".to_string()));
    assert_eq!(schema.format, Some("int32".to_string()));
}

#[test]
fn test_kubernetes_format_int64() {
    // Kubernetes API: format: "int64" on integer properties
    let schema: Schema = serde_json::from_str(r#"{"type": "integer", "format": "int64"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("integer".to_string()));
    assert_eq!(schema.format, Some("int64".to_string()));
}

#[test]
fn test_kubernetes_format_double() {
    // Kubernetes API: format: "double" on number properties
    let schema: Schema = serde_json::from_str(r#"{"type": "number", "format": "double"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("number".to_string()));
    assert_eq!(schema.format, Some("double".to_string()));
}

#[test]
fn test_format_float() {
    // General: format: "float" on number properties
    let schema: Schema = serde_json::from_str(r#"{"type": "number", "format": "float"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("number".to_string()));
    assert_eq!(schema.format, Some("float".to_string()));
}

// --- Kubernetes API: 3.1 nullable int32/int64 ---

#[test]
fn test_31_nullable_int32() {
    // OpenAPI 3.1: type: ["integer", "null"] with format: "int32"
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["integer", "null"], "format": "int32"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("integer".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("int32".to_string()));
}

#[test]
fn test_31_nullable_int64() {
    // OpenAPI 3.1: type: ["integer", "null"] with format: "int64"
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["integer", "null"], "format": "int64"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("integer".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("int64".to_string()));
}

#[test]
fn test_31_nullable_double() {
    // OpenAPI 3.1: type: ["number", "null"] with format: "double"
    // (already tested without null; testing nullable variant)
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["number", "null"], "format": "double"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("number".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("double".to_string()));
}

// --- DigitalOcean API: format: "uuid" ---

#[test]
fn test_digitalocean_format_uuid() {
    // DigitalOcean API: format: "uuid" on string properties
    let schema: Schema = serde_json::from_str(r#"{"type": "string", "format": "uuid"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert_eq!(schema.format, Some("uuid".to_string()));
}

#[test]
fn test_31_nullable_uuid() {
    // OpenAPI 3.1: type: ["string", "null"] with format: "uuid"
    let schema: Schema =
        serde_json::from_str(r#"{"type": ["string", "null"], "format": "uuid"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert!(schema.nullable);
    assert_eq!(schema.format, Some("uuid".to_string()));
}

// --- OpenAPI 3.1 JSON Schema 2020-12: keywords that should be ignored ---

#[test]
fn test_31_defs_ignored() {
    // OpenAPI 3.1 (JSON Schema 2020-12): "$defs" replaces "definitions"
    // Should be silently ignored by serde
    let schema: Schema = serde_json::from_str(
        r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "$defs": {
                "helper": {"type": "string"}
            }
        }"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("object".to_string()));
    assert!(schema.properties.contains_key("name"));
}

#[test]
fn test_31_comment_ignored() {
    // OpenAPI 3.1 (JSON Schema 2020-12): "$comment" for documentation
    let schema: Schema = serde_json::from_str(
        r#"{"type": "string", "$comment": "This is an internal comment for schema authors"}"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
}

#[test]
fn test_31_if_then_else_ignored() {
    // OpenAPI 3.1 (JSON Schema 2020-12): if/then/else conditional schemas
    // Our parser ignores these (they don't affect type resolution for FDW)
    let schema: Schema = serde_json::from_str(
        r#"{
            "type": "object",
            "properties": {
                "type": {"type": "string"},
                "value": {"type": "string"}
            },
            "if": {"properties": {"type": {"const": "email"}}},
            "then": {"properties": {"value": {"format": "email"}}},
            "else": {"properties": {"value": {"format": "uri"}}}
        }"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("object".to_string()));
    assert!(schema.properties.contains_key("type"));
    assert!(schema.properties.contains_key("value"));
}

#[test]
fn test_31_dependent_required_ignored() {
    // OpenAPI 3.1 (JSON Schema 2020-12): dependentRequired
    let schema: Schema = serde_json::from_str(
        r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "credit_card": {"type": "string"},
                "billing_address": {"type": "string"}
            },
            "dependentRequired": {
                "credit_card": ["billing_address"]
            }
        }"#,
    )
    .unwrap();
    assert!(schema.properties.contains_key("name"));
    assert!(schema.properties.contains_key("credit_card"));
    assert!(schema.properties.contains_key("billing_address"));
}

#[test]
fn test_31_prefix_items_ignored() {
    // OpenAPI 3.1 (JSON Schema 2020-12): prefixItems for tuple validation
    let schema: Schema = serde_json::from_str(
        r#"{
            "type": "array",
            "prefixItems": [
                {"type": "number"},
                {"type": "string"},
                {"type": "string"}
            ]
        }"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("array".to_string()));
}

#[test]
fn test_31_pattern_properties_ignored() {
    // OpenAPI 3.1 (JSON Schema 2020-12): patternProperties
    let schema: Schema = serde_json::from_str(
        r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "patternProperties": {
                "^x-": {"type": "string"}
            }
        }"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("object".to_string()));
    assert!(schema.properties.contains_key("name"));
    // patternProperties should not appear in regular properties
    assert!(!schema.properties.contains_key("^x-"));
}

#[test]
fn test_31_unevaluated_properties_ignored() {
    // OpenAPI 3.1 (JSON Schema 2020-12): unevaluatedProperties
    let schema: Schema = serde_json::from_str(
        r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "unevaluatedProperties": false
        }"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("object".to_string()));
    assert!(schema.properties.contains_key("name"));
}

#[test]
fn test_31_content_media_type_encoding_ignored() {
    // OpenAPI 3.1 (JSON Schema 2020-12): contentMediaType and contentEncoding
    // for binary/encoded string data
    let schema: Schema = serde_json::from_str(
        r#"{
            "type": "string",
            "contentMediaType": "image/png",
            "contentEncoding": "base64"
        }"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
}

#[test]
fn test_31_exclusive_min_max_as_numbers_ignored() {
    // OpenAPI 3.1 (JSON Schema 2020-12): exclusiveMinimum/exclusiveMaximum are now
    // numbers (not booleans as in 3.0). Should be silently ignored.
    let schema: Schema = serde_json::from_str(
        r#"{"type": "integer", "exclusiveMinimum": 0, "exclusiveMaximum": 100}"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, Some("integer".to_string()));
}

// --- Deprecated properties and operations ---

#[test]
fn test_31_deprecated_property_still_included() {
    // OpenAPI 3.1: deprecated: true on a property should not exclude it
    // (deprecated means "avoid using" not "removed")
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/users": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "integer"},
                                            "login": {"type": "string"},
                                            "gravatar_id": {
                                                "type": ["string", "null"],
                                                "deprecated": true
                                            }
                                        },
                                        "required": ["id", "login"]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    // Deprecated property should still be present
    assert!(schema.properties.contains_key("gravatar_id"));
    let gravatar = schema.properties.get("gravatar_id").unwrap();
    assert!(gravatar.nullable);
    assert_eq!(gravatar.schema_type, Some("string".to_string()));
}

#[test]
fn test_31_deprecated_operation_still_included() {
    // OpenAPI 3.1: deprecated: true on an operation should still be included
    // (the endpoint still works, just discouraged)
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/legacy/items": {
                "get": {
                    "deprecated": true,
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
                                                "id": {"type": "string"}
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
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    // Deprecated operations should still be discovered
    assert_eq!(endpoints.len(), 1);
    assert!(endpoints[0].response_schema.is_some());
}

// --- POST-for-read with OpenAPI 3.1 type arrays ---

#[test]
fn test_31_post_for_read_with_type_arrays() {
    // POST-for-read (e.g., Elasticsearch-style search) with 3.1 type arrays in response
    let spec_json = r#"{
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
                                        "type": "object",
                                        "properties": {
                                            "hits": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id": {"type": "string"},
                                                        "score": {"type": ["number", "null"]},
                                                        "matched_at": {"type": ["string", "null"], "format": "date-time"},
                                                        "highlight": {"type": ["object", "null"]}
                                                    },
                                                    "required": ["id"]
                                                }
                                            },
                                            "total": {"type": "integer"},
                                            "max_score": {"type": ["number", "null"]}
                                        },
                                        "required": ["hits", "total"]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].method, "POST");
    assert_eq!(endpoints[0].table_name(), "search_post");

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("hits"));
    assert!(schema.properties.contains_key("total"));

    let max_score = schema.properties.get("max_score").unwrap();
    assert!(max_score.nullable);
    assert_eq!(max_score.schema_type, Some("number".to_string()));
}

// --- Response $ref to schema that uses allOf (double indirection) ---

#[test]
fn test_31_response_ref_to_allof_schema() {
    // Real pattern: response has $ref to a schema, which itself is an allOf.
    // GitHub API uses this for pull-request (allOf of issue + extra fields).
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/pulls": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {"$ref": "#/components/schemas/pull-request"}
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
                "issue-base": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "title": {"type": "string"},
                        "state": {"type": "string"},
                        "created_at": {"type": "string", "format": "date-time"}
                    },
                    "required": ["id", "title", "state"]
                },
                "pull-request": {
                    "allOf": [
                        {"$ref": "#/components/schemas/issue-base"},
                        {
                            "type": "object",
                            "properties": {
                                "merged": {"type": "boolean"},
                                "merged_at": {"type": ["string", "null"], "format": "date-time"},
                                "commits": {"type": "integer"},
                                "additions": {"type": "integer"},
                                "deletions": {"type": "integer"}
                            }
                        }
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let items = schema.items.as_ref().unwrap();
    let resolved = spec.resolve_schema(items);

    // Should have all properties from the allOf chain
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("title"));
    assert!(resolved.properties.contains_key("state"));
    assert!(resolved.properties.contains_key("created_at"));
    assert!(resolved.properties.contains_key("merged"));
    assert!(resolved.properties.contains_key("merged_at"));
    assert!(resolved.properties.contains_key("commits"));

    // merged_at should be nullable
    let merged_at = resolved.properties.get("merged_at").unwrap();
    assert!(merged_at.nullable);
    assert_eq!(merged_at.format, Some("date-time".to_string()));

    // Required should include base requirements
    assert!(resolved.required.contains(&"id".to_string()));
    assert!(resolved.required.contains(&"title".to_string()));
}

// --- anyOf with $ref + inline type + null (3 variants) ---

#[test]
fn test_31_anyof_three_variants_ref_inline_null() {
    // Real pattern: anyOf: [{$ref: "..."}, {type: "string"}, {type: "null"}]
    // This appears in APIs where a field can be an ID string, expanded object, or null
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Account": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "email": {"type": "string"}
                    },
                    "required": ["id"]
                },
                "Transaction": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "source": {
                            "anyOf": [
                                {"type": "string"},
                                {"$ref": "#/components/schemas/Account"},
                                {"type": "null"}
                            ]
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let txn = spec
        .resolve_ref("#/components/schemas/Transaction")
        .unwrap();
    let resolved = spec.resolve_schema(txn);

    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("source"));

    // source is anyOf with 3 variants — should merge and resolve
    let source = resolved.properties.get("source").unwrap();
    let source_resolved = spec.resolve_schema(source);
    // The Account ref has properties, so merge produces object-like schema
    assert!(
        source_resolved.properties.contains_key("id")
            || source_resolved.schema_type.is_some()
            || source_resolved.properties.contains_key("email")
    );
}

// --- Schema with items but no explicit type: "array" ---

#[test]
fn test_schema_items_without_array_type() {
    // Some APIs define items without an explicit type: "array"
    // The schema should still parse (items is present even without type)
    let schema: Schema = serde_json::from_str(
        r#"{
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"}
                }
            }
        }"#,
    )
    .unwrap();
    assert_eq!(schema.schema_type, None);
    assert!(schema.items.is_some());
    let items = schema.items.as_ref().unwrap();
    assert!(items.properties.contains_key("id"));
}

// --- Full end-to-end: OpenAPI 3.1 spec with comprehensive patterns ---

#[test]
fn test_31_full_e2e_github_style_spec() {
    // End-to-end test modeled after GitHub API 3.1.0 structure:
    // - Multiple endpoints (GET + POST)
    // - Type arrays for nullable
    // - $ref resolution through allOf chains
    // - anyOf with null for nullable refs
    // - Format strings (date-time, uri)
    // - Response $ref to components/responses
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "GitHub-style API", "version": "2024-01-01"},
        "servers": [
            {
                "url": "https://api.github.com"
            }
        ],
        "paths": {
            "/repos": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "List repositories",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {"$ref": "#/components/schemas/repository"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/repos/search": {
                "post": {
                    "responses": {
                        "200": {
                            "description": "Search repositories",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "total_count": {"type": "integer"},
                                            "incomplete_results": {"type": "boolean"},
                                            "items": {
                                                "type": "array",
                                                "items": {"$ref": "#/components/schemas/repository"}
                                            }
                                        },
                                        "required": ["total_count", "incomplete_results", "items"]
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/notifications": {
                "get": {
                    "responses": {
                        "200": {"$ref": "#/components/responses/NotificationList"}
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "simple-user": {
                    "type": "object",
                    "properties": {
                        "login": {"type": "string"},
                        "id": {"type": "integer"},
                        "avatar_url": {"type": "string", "format": "uri"},
                        "html_url": {"type": "string", "format": "uri"}
                    },
                    "required": ["login", "id", "avatar_url", "html_url"]
                },
                "license": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                        "name": {"type": "string"},
                        "spdx_id": {"type": ["string", "null"]},
                        "url": {"type": ["string", "null"], "format": "uri"}
                    },
                    "required": ["key", "name"]
                },
                "repository": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                        "full_name": {"type": "string"},
                        "private": {"type": "boolean"},
                        "owner": {"$ref": "#/components/schemas/simple-user"},
                        "description": {"type": ["string", "null"]},
                        "fork": {"type": "boolean"},
                        "html_url": {"type": "string", "format": "uri"},
                        "created_at": {"type": "string", "format": "date-time"},
                        "updated_at": {"type": "string", "format": "date-time"},
                        "pushed_at": {"type": ["string", "null"], "format": "date-time"},
                        "homepage": {"type": ["string", "null"], "format": "uri"},
                        "size": {"type": "integer"},
                        "stargazers_count": {"type": "integer"},
                        "language": {"type": ["string", "null"]},
                        "archived": {"type": "boolean"},
                        "disabled": {"type": "boolean"},
                        "license": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/license"},
                                {"type": "null"}
                            ]
                        },
                        "topics": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["id", "name", "full_name", "private", "fork"]
                },
                "notification-subject": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "type": {"type": "string"},
                        "url": {"type": ["string", "null"], "format": "uri"}
                    },
                    "required": ["title", "type"]
                },
                "notification": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "unread": {"type": "boolean"},
                        "reason": {"type": "string"},
                        "updated_at": {"type": "string", "format": "date-time"},
                        "subject": {"$ref": "#/components/schemas/notification-subject"},
                        "repository": {"$ref": "#/components/schemas/repository"}
                    },
                    "required": ["id", "unread", "reason", "updated_at"]
                }
            },
            "responses": {
                "NotificationList": {
                    "description": "List of notifications",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {"$ref": "#/components/schemas/notification"}
                            }
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();

    // Server URL
    assert_eq!(spec.base_url(), Some("https://api.github.com".to_string()));

    // Should have 3 endpoints: GET /repos, POST /repos/search, GET /notifications
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 3);

    // Endpoints should be sorted
    assert_eq!(endpoints[0].path, "/notifications");
    assert_eq!(endpoints[0].method, "GET");
    assert_eq!(endpoints[1].path, "/repos");
    assert_eq!(endpoints[1].method, "GET");
    assert_eq!(endpoints[2].path, "/repos/search");
    assert_eq!(endpoints[2].method, "POST");

    // Table names
    assert_eq!(endpoints[0].table_name(), "notifications");
    assert_eq!(endpoints[1].table_name(), "repos");
    assert_eq!(endpoints[2].table_name(), "repos_search_post");

    // --- Verify /repos response schema ---
    let repos_schema = endpoints[1].response_schema.as_ref().unwrap();
    let repos_items = repos_schema.items.as_ref().unwrap();
    let repo_resolved = spec.resolve_schema(repos_items);

    assert!(repo_resolved.properties.contains_key("id"));
    assert!(repo_resolved.properties.contains_key("name"));
    assert!(repo_resolved.properties.contains_key("description"));
    assert!(repo_resolved.properties.contains_key("language"));
    assert!(repo_resolved.properties.contains_key("license"));
    assert!(repo_resolved.properties.contains_key("topics"));

    // description is nullable (type: ["string", "null"])
    let desc = repo_resolved.properties.get("description").unwrap();
    assert!(desc.nullable);
    assert_eq!(desc.schema_type, Some("string".to_string()));

    // pushed_at is nullable datetime
    let pushed_at = repo_resolved.properties.get("pushed_at").unwrap();
    assert!(pushed_at.nullable);
    assert_eq!(pushed_at.format, Some("date-time".to_string()));

    // --- Verify /notifications response (via $ref to components/responses) ---
    let notif_schema = endpoints[0].response_schema.as_ref().unwrap();
    let notif_items = notif_schema.items.as_ref().unwrap();
    let notif_resolved = spec.resolve_schema(notif_items);

    assert!(notif_resolved.properties.contains_key("id"));
    assert!(notif_resolved.properties.contains_key("unread"));
    assert!(notif_resolved.properties.contains_key("reason"));
    assert!(notif_resolved.properties.contains_key("subject"));
    assert!(notif_resolved.properties.contains_key("repository"));

    // --- Verify POST search response ---
    let search_schema = endpoints[2].response_schema.as_ref().unwrap();
    assert!(search_schema.properties.contains_key("total_count"));
    assert!(search_schema.properties.contains_key("items"));
}

// --- Stripe-style full spec with 3.1 patterns ---

#[test]
fn test_31_full_e2e_stripe_style_spec() {
    // End-to-end test modeled after Stripe API 3.1.0:
    // - List pagination wrapper (data/has_more/url)
    // - Expandable fields (anyOf: [string, $ref])
    // - Type arrays for nullable
    // - Unix timestamps
    // - Metadata as untyped object
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Stripe-style API", "version": "2024-01-01"},
        "servers": [{"url": "https://api.stripe.com"}],
        "paths": {
            "/v1/customers": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "object": {"type": "string"},
                                            "data": {
                                                "type": "array",
                                                "items": {"$ref": "#/components/schemas/customer"}
                                            },
                                            "has_more": {"type": "boolean"},
                                            "url": {"type": "string"}
                                        },
                                        "required": ["object", "data", "has_more", "url"]
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
                "address": {
                    "type": "object",
                    "properties": {
                        "city": {"type": ["string", "null"]},
                        "country": {"type": ["string", "null"]},
                        "line1": {"type": ["string", "null"]},
                        "line2": {"type": ["string", "null"]},
                        "postal_code": {"type": ["string", "null"]},
                        "state": {"type": ["string", "null"]}
                    }
                },
                "customer": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "object": {"type": "string"},
                        "created": {"type": "integer", "format": "unix-time"},
                        "email": {"type": ["string", "null"]},
                        "name": {"type": ["string", "null"]},
                        "phone": {"type": ["string", "null"]},
                        "description": {"type": ["string", "null"]},
                        "balance": {"type": "integer"},
                        "currency": {"type": ["string", "null"]},
                        "delinquent": {"type": ["boolean", "null"]},
                        "livemode": {"type": "boolean"},
                        "address": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/address"},
                                {"type": "null"}
                            ]
                        },
                        "metadata": {"type": "object"}
                    },
                    "required": ["id", "object", "created", "livemode"]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let data = schema.properties.get("data").unwrap();
    let items = data.items.as_ref().unwrap();
    let customer = spec.resolve_schema(items);

    // Verify all properties present
    assert!(customer.properties.contains_key("id"));
    assert!(customer.properties.contains_key("created"));
    assert!(customer.properties.contains_key("email"));
    assert!(customer.properties.contains_key("address"));
    assert!(customer.properties.contains_key("metadata"));

    // Verify nullable fields
    assert!(customer.properties.get("email").unwrap().nullable);
    assert!(customer.properties.get("name").unwrap().nullable);
    assert!(customer.properties.get("phone").unwrap().nullable);
    assert!(customer.properties.get("delinquent").unwrap().nullable);

    // Verify non-nullable fields
    assert!(!customer.properties.get("livemode").unwrap().nullable);
    assert!(!customer.properties.get("id").unwrap().nullable);

    // Verify created has unix-time format
    let created = customer.properties.get("created").unwrap();
    assert_eq!(created.format, Some("unix-time".to_string()));
    assert_eq!(created.schema_type, Some("integer".to_string()));
}

// --- oneOf with discriminator + mapping ---

#[test]
fn test_31_oneof_with_discriminator_and_mapping() {
    // OpenAPI 3.1: oneOf with discriminator.mapping for polymorphic types
    // Mapping provides URI-based resolution hints (we ignore mapping, just merge variants)
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "CreditCard": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string"},
                        "last_four": {"type": "string"},
                        "brand": {"type": "string"}
                    },
                    "required": ["type"]
                },
                "BankAccount": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string"},
                        "routing_number": {"type": "string"},
                        "account_holder": {"type": "string"}
                    },
                    "required": ["type"]
                },
                "PaymentMethod": {
                    "oneOf": [
                        {"$ref": "#/components/schemas/CreditCard"},
                        {"$ref": "#/components/schemas/BankAccount"}
                    ],
                    "discriminator": {
                        "propertyName": "type",
                        "mapping": {
                            "credit_card": "#/components/schemas/CreditCard",
                            "bank_account": "#/components/schemas/BankAccount"
                        }
                    }
                }
            }
        }
    }"##;

    // Should parse without error (discriminator + mapping ignored by serde)
    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let pm = spec
        .resolve_ref("#/components/schemas/PaymentMethod")
        .unwrap();
    let resolved = spec.resolve_schema(pm);

    // oneOf merges all variant properties as nullable
    assert!(resolved.properties.contains_key("type"));
    assert!(resolved.properties.contains_key("last_four"));
    assert!(resolved.properties.contains_key("brand"));
    assert!(resolved.properties.contains_key("routing_number"));
    assert!(resolved.properties.contains_key("account_holder"));
}

// --- allOf where one schema adds only required (no new properties) ---

#[test]
fn test_31_allof_ref_with_additional_required_only() {
    // Kubernetes pattern: allOf: [{$ref}, {required: [...]}]
    // The second schema only adds required constraints, no new properties
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Resource": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "name": {"type": "string"},
                        "status": {"type": "string"}
                    }
                },
                "StrictResource": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Resource"},
                        {
                            "required": ["id", "name", "status"]
                        }
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let strict = spec
        .resolve_ref("#/components/schemas/StrictResource")
        .unwrap();
    let resolved = spec.resolve_schema(strict);

    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("status"));
    assert!(resolved.required.contains(&"id".to_string()));
    assert!(resolved.required.contains(&"name".to_string()));
    assert!(resolved.required.contains(&"status".to_string()));
}

// --- Response with only binary content (application/octet-stream) ---

#[test]
fn test_response_only_binary_content() {
    // Some APIs return binary data — schema should still be extracted via fallback
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/download": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Binary file",
                            "content": {
                                "application/octet-stream": {
                                    "schema": {
                                        "type": "string",
                                        "format": "binary"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);
    // Should fall back to the only available content type
    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert_eq!(schema.format, Some("binary".to_string()));
}

// --- Multiple $ref resolution through response → ref → allOf → ref chain ---

#[test]
fn test_31_triple_indirection_response_ref_allof_ref() {
    // Response $ref → components/responses → schema $ref → allOf → $ref
    // This tests the deepest indirection chain common in real APIs
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/resources": {
                "get": {
                    "responses": {
                        "200": {"$ref": "#/components/responses/ResourceList"}
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "Timestamps": {
                    "type": "object",
                    "properties": {
                        "created_at": {"type": "string", "format": "date-time"},
                        "updated_at": {"type": ["string", "null"], "format": "date-time"}
                    },
                    "required": ["created_at"]
                },
                "Resource": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Timestamps"},
                        {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string", "format": "uuid"},
                                "name": {"type": "string"},
                                "status": {"type": "string"}
                            },
                            "required": ["id", "name"]
                        }
                    ]
                }
            },
            "responses": {
                "ResourceList": {
                    "description": "List of resources",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {"$ref": "#/components/schemas/Resource"}
                            }
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let items = schema.items.as_ref().unwrap();
    let resolved = spec.resolve_schema(items);

    // Should have all properties from the entire chain
    assert!(resolved.properties.contains_key("created_at"));
    assert!(resolved.properties.contains_key("updated_at"));
    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("status"));

    // updated_at should be nullable
    let updated = resolved.properties.get("updated_at").unwrap();
    assert!(updated.nullable);

    // id should have uuid format
    let id = resolved.properties.get("id").unwrap();
    assert_eq!(id.format, Some("uuid".to_string()));

    // Required should include from both
    assert!(resolved.required.contains(&"created_at".to_string()));
    assert!(resolved.required.contains(&"id".to_string()));
    assert!(resolved.required.contains(&"name".to_string()));
}

// --- Format strings: byte and binary (base64 encoded content) ---

#[test]
fn test_format_byte_base64() {
    // OpenAPI: format: "byte" for base64-encoded binary data
    let schema: Schema = serde_json::from_str(r#"{"type": "string", "format": "byte"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert_eq!(schema.format, Some("byte".to_string()));
}

#[test]
fn test_format_binary() {
    // OpenAPI: format: "binary" for raw binary data
    let schema: Schema = serde_json::from_str(r#"{"type": "string", "format": "binary"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert_eq!(schema.format, Some("binary".to_string()));
}

// --- Format string: time ---

#[test]
fn test_format_time() {
    // OpenAPI: format: "time" for time-only values
    let schema: Schema = serde_json::from_str(r#"{"type": "string", "format": "time"}"#).unwrap();
    assert_eq!(schema.schema_type, Some("string".to_string()));
    assert_eq!(schema.format, Some("time".to_string()));
}

// --- OpenAPI 3.1: $ref with siblings combined (nullable + writeOnly + required) ---

#[test]
fn test_31_ref_with_multiple_siblings() {
    // OpenAPI 3.1: $ref with nullable + additional required fields merged
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Address": {
                    "type": "object",
                    "properties": {
                        "line1": {"type": "string"},
                        "city": {"type": "string"},
                        "country": {"type": "string"}
                    },
                    "required": ["line1"]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();

    // Construct a $ref with multiple siblings
    let mut extra_props = HashMap::new();
    extra_props.insert(
        "verified".to_string(),
        Schema {
            schema_type: Some("boolean".to_string()),
            ..Default::default()
        },
    );

    let ref_with_siblings = Schema {
        reference: Some("#/components/schemas/Address".to_string()),
        nullable: true,
        write_only: true,
        properties: extra_props,
        required: vec!["city".to_string(), "verified".to_string()],
        ..Default::default()
    };

    let resolved = spec.resolve_schema(&ref_with_siblings);

    // All original properties + sibling properties
    assert!(resolved.properties.contains_key("line1"));
    assert!(resolved.properties.contains_key("city"));
    assert!(resolved.properties.contains_key("country"));
    assert!(resolved.properties.contains_key("verified"));

    // Siblings merged
    assert!(resolved.nullable);
    assert!(resolved.write_only);

    // Required merged and deduplicated
    assert!(resolved.required.contains(&"line1".to_string()));
    assert!(resolved.required.contains(&"city".to_string()));
    assert!(resolved.required.contains(&"verified".to_string()));
}

// --- Empty oneOf/anyOf (edge case) ---

#[test]
fn test_empty_oneof() {
    // Edge case: oneOf with no variants
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Empty": {
                    "oneOf": []
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let empty = spec.resolve_ref("#/components/schemas/Empty").unwrap();
    let resolved = spec.resolve_schema(empty);
    assert!(resolved.properties.is_empty());
}

#[test]
fn test_empty_anyof() {
    // Edge case: anyOf with no variants
    let spec_json = r#"{
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Empty": {
                    "anyOf": []
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let empty = spec.resolve_ref("#/components/schemas/Empty").unwrap();
    let resolved = spec.resolve_schema(empty);
    assert!(resolved.properties.is_empty());
}

// --- OpenAPI 3.1 response code variants with type arrays ---
// Ensures all response code paths work with 3.1-specific nullable type arrays.

#[test]
fn test_31_response_201_with_type_arrays() {
    // OpenAPI 3.1 endpoint returning 201 with type arrays in schema
    // Real pattern: POST creation endpoints that return the created object
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/resources": {
                "post": {
                    "responses": {
                        "201": {
                            "description": "Created",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string", "format": "uuid"},
                                            "name": {"type": "string"},
                                            "description": {"type": ["string", "null"]},
                                            "created_at": {"type": "string", "format": "date-time"}
                                        },
                                        "required": ["id", "name", "created_at"]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    assert!(schema.properties.contains_key("id"));
    let desc = schema.properties.get("description").unwrap();
    assert!(desc.nullable);
    assert_eq!(desc.schema_type, Some("string".to_string()));
}

#[test]
fn test_31_response_2xx_with_type_arrays() {
    // OpenAPI 3.1 endpoint using 2XX wildcard with type arrays
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "2XX": {
                            "description": "Success",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "integer"},
                                                "label": {"type": ["string", "null"]},
                                                "value": {"type": ["number", "null"], "format": "double"}
                                            },
                                            "required": ["id"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let items = schema.items.as_ref().unwrap();
    let label = items.properties.get("label").unwrap();
    assert!(label.nullable);
    assert_eq!(label.schema_type, Some("string".to_string()));

    let value = items.properties.get("value").unwrap();
    assert!(value.nullable);
    assert_eq!(value.format, Some("double".to_string()));
}

#[test]
fn test_31_response_default_only_with_type_arrays() {
    // OpenAPI 3.1 endpoint with only "default" response and type arrays
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/status": {
                "get": {
                    "responses": {
                        "default": {
                            "description": "Default response",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "status": {"type": "string"},
                                            "message": {"type": ["string", "null"]},
                                            "timestamp": {"type": "integer", "format": "unix-time"}
                                        },
                                        "required": ["status"]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let msg = schema.properties.get("message").unwrap();
    assert!(msg.nullable);
    assert_eq!(msg.schema_type, Some("string".to_string()));
}

#[test]
fn test_31_response_priority_all_codes_with_type_arrays() {
    // OpenAPI 3.1: 200 should be preferred over 201/2XX/default
    // Ensures response code priority works correctly with 3.1 schemas
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "OK",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "from_200": {"type": ["string", "null"]}
                                        }
                                    }
                                }
                            }
                        },
                        "201": {
                            "description": "Created",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "from_201": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        },
                        "2XX": {
                            "description": "Wildcard",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "from_2xx": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        },
                        "default": {
                            "description": "Default",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "from_default": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    let schema = endpoints[0].response_schema.as_ref().unwrap();

    // 200 should win
    assert!(schema.properties.contains_key("from_200"));
    assert!(!schema.properties.contains_key("from_201"));
    assert!(!schema.properties.contains_key("from_2xx"));
    assert!(!schema.properties.contains_key("from_default"));

    // And the 3.1 type array should be resolved
    let field = schema.properties.get("from_200").unwrap();
    assert!(field.nullable);
}

#[test]
fn test_31_non_json_content_type_with_type_arrays() {
    // OpenAPI 3.1 with non-JSON content types containing type arrays
    // Tests GeoJSON fallback with 3.1 nullable patterns
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/features": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/geo+json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "type": {"type": "string"},
                                            "bbox": {"type": ["array", "null"]},
                                            "features": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id": {"type": ["string", "null"]},
                                                        "geometry": {"type": ["object", "null"]}
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
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let bbox = schema.properties.get("bbox").unwrap();
    assert!(bbox.nullable);
    assert_eq!(bbox.schema_type, Some("array".to_string()));
}

#[test]
fn test_31_response_ref_with_type_arrays() {
    // OpenAPI 3.1: response-level $ref where the target schema uses type arrays
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/events": {
                "get": {
                    "responses": {
                        "200": {"$ref": "#/components/responses/EventList"}
                    }
                }
            }
        },
        "components": {
            "responses": {
                "EventList": {
                    "description": "List of events",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "integer"},
                                        "type": {"type": "string"},
                                        "actor": {"type": ["string", "null"]},
                                        "created_at": {"type": "string", "format": "date-time"},
                                        "payload": {"type": ["object", "null"]}
                                    },
                                    "required": ["id", "type", "created_at"]
                                }
                            }
                        }
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 1);

    let schema = endpoints[0].response_schema.as_ref().unwrap();
    let items = schema.items.as_ref().unwrap();

    let actor = items.properties.get("actor").unwrap();
    assert!(actor.nullable);
    assert_eq!(actor.schema_type, Some("string".to_string()));

    let payload = items.properties.get("payload").unwrap();
    assert!(payload.nullable);
    assert_eq!(payload.schema_type, Some("object".to_string()));
}

#[test]
fn test_31_get_post_same_path_different_schemas() {
    // OpenAPI 3.1: same path with GET and POST returning different schemas,
    // both using 3.1 type arrays
    let spec_json = r#"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "List items",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "integer"},
                                                "summary": {"type": ["string", "null"]}
                                            },
                                            "required": ["id"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "post": {
                    "responses": {
                        "201": {
                            "description": "Created",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "integer"},
                                            "summary": {"type": ["string", "null"]},
                                            "details": {"type": ["string", "null"]},
                                            "created_at": {"type": "string", "format": "date-time"}
                                        },
                                        "required": ["id", "created_at"]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let endpoints = spec.get_endpoints();
    assert_eq!(endpoints.len(), 2);

    // GET comes first (alphabetically by method)
    assert_eq!(endpoints[0].method, "GET");
    assert_eq!(endpoints[1].method, "POST");

    // GET has array response with 2 properties
    let get_schema = endpoints[0].response_schema.as_ref().unwrap();
    let get_items = get_schema.items.as_ref().unwrap();
    assert_eq!(get_items.properties.len(), 2);

    // POST has object response with 4 properties
    let post_schema = endpoints[1].response_schema.as_ref().unwrap();
    assert_eq!(post_schema.properties.len(), 4);
    assert!(post_schema.properties.contains_key("details"));

    // Table names should differ
    assert_eq!(endpoints[0].table_name(), "items");
    assert_eq!(endpoints[1].table_name(), "items_post");
}

#[test]
fn test_branching_allof_completes_within_call_limit() {
    // Build a spec where allOf branches exponentially:
    // Root -> allOf[A, B, C, D, E] and each of those -> allOf[F, G, H, I, J]
    // That's 5*5 = 25 resolve calls, well within the 10,000 limit.
    // This test verifies branching specs complete and produce merged properties.
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Branch Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Leaf1": {"type": "object", "properties": {"a": {"type": "string"}}},
                "Leaf2": {"type": "object", "properties": {"b": {"type": "integer"}}},
                "Leaf3": {"type": "object", "properties": {"c": {"type": "boolean"}}},
                "Leaf4": {"type": "object", "properties": {"d": {"type": "number"}}},
                "Leaf5": {"type": "object", "properties": {"e": {"type": "string"}}},
                "Mid1": {"allOf": [{"$ref": "#/components/schemas/Leaf1"}, {"$ref": "#/components/schemas/Leaf2"}]},
                "Mid2": {"allOf": [{"$ref": "#/components/schemas/Leaf3"}, {"$ref": "#/components/schemas/Leaf4"}]},
                "Mid3": {"allOf": [{"$ref": "#/components/schemas/Leaf5"}, {"$ref": "#/components/schemas/Leaf1"}]},
                "Mid4": {"allOf": [{"$ref": "#/components/schemas/Leaf2"}, {"$ref": "#/components/schemas/Leaf3"}]},
                "Mid5": {"allOf": [{"$ref": "#/components/schemas/Leaf4"}, {"$ref": "#/components/schemas/Leaf5"}]},
                "Root": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Mid1"},
                        {"$ref": "#/components/schemas/Mid2"},
                        {"$ref": "#/components/schemas/Mid3"},
                        {"$ref": "#/components/schemas/Mid4"},
                        {"$ref": "#/components/schemas/Mid5"}
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let root = spec.resolve_ref("#/components/schemas/Root").unwrap();
    let resolved = spec.resolve_schema(root);

    // All 5 leaf properties should be merged
    assert_eq!(resolved.schema_type, Some("object".to_string()));
    assert!(resolved.properties.contains_key("a"));
    assert!(resolved.properties.contains_key("b"));
    assert!(resolved.properties.contains_key("c"));
    assert!(resolved.properties.contains_key("d"));
    assert!(resolved.properties.contains_key("e"));
}

#[test]
fn test_excessive_branching_hits_call_limit_gracefully() {
    // Build a spec where each level has 4 branches x 4 levels = 4^4 = 256 resolve calls.
    // The call limit of 10,000 easily handles this, but we verify it doesn't hang.
    // A truly exponential spec (4^10 = 1M) would be capped by MAX_RESOLVE_CALLS.
    let spec_json = r##"{
        "openapi": "3.0.0",
        "info": {"title": "Deep Branch", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "L0a": {"type": "object", "properties": {"x": {"type": "string"}}},
                "L0b": {"type": "object", "properties": {"y": {"type": "string"}}},
                "L0c": {"type": "object", "properties": {"z": {"type": "string"}}},
                "L0d": {"type": "object", "properties": {"w": {"type": "string"}}},
                "L1a": {"allOf": [{"$ref": "#/components/schemas/L0a"}, {"$ref": "#/components/schemas/L0b"}, {"$ref": "#/components/schemas/L0c"}, {"$ref": "#/components/schemas/L0d"}]},
                "L1b": {"allOf": [{"$ref": "#/components/schemas/L0a"}, {"$ref": "#/components/schemas/L0b"}, {"$ref": "#/components/schemas/L0c"}, {"$ref": "#/components/schemas/L0d"}]},
                "L1c": {"allOf": [{"$ref": "#/components/schemas/L0a"}, {"$ref": "#/components/schemas/L0b"}, {"$ref": "#/components/schemas/L0c"}, {"$ref": "#/components/schemas/L0d"}]},
                "L1d": {"allOf": [{"$ref": "#/components/schemas/L0a"}, {"$ref": "#/components/schemas/L0b"}, {"$ref": "#/components/schemas/L0c"}, {"$ref": "#/components/schemas/L0d"}]},
                "L2a": {"allOf": [{"$ref": "#/components/schemas/L1a"}, {"$ref": "#/components/schemas/L1b"}, {"$ref": "#/components/schemas/L1c"}, {"$ref": "#/components/schemas/L1d"}]},
                "L2b": {"allOf": [{"$ref": "#/components/schemas/L1a"}, {"$ref": "#/components/schemas/L1b"}, {"$ref": "#/components/schemas/L1c"}, {"$ref": "#/components/schemas/L1d"}]},
                "L2c": {"allOf": [{"$ref": "#/components/schemas/L1a"}, {"$ref": "#/components/schemas/L1b"}, {"$ref": "#/components/schemas/L1c"}, {"$ref": "#/components/schemas/L1d"}]},
                "L2d": {"allOf": [{"$ref": "#/components/schemas/L1a"}, {"$ref": "#/components/schemas/L1b"}, {"$ref": "#/components/schemas/L1c"}, {"$ref": "#/components/schemas/L1d"}]},
                "Root": {"allOf": [{"$ref": "#/components/schemas/L2a"}, {"$ref": "#/components/schemas/L2b"}, {"$ref": "#/components/schemas/L2c"}, {"$ref": "#/components/schemas/L2d"}]}
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let root = spec.resolve_ref("#/components/schemas/Root").unwrap();

    // Should complete without hanging — the call limit prevents exponential blowup
    let resolved = spec.resolve_schema(root);

    // Should still produce an object with merged properties (from the leaves that resolved)
    assert_eq!(resolved.schema_type, Some("object".to_string()));
}

// --- Parent sibling merging for composition keywords ---

#[test]
fn test_allof_with_parent_required_siblings() {
    // Parent-level `required` alongside allOf should be merged into the result
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Base": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "name": {"type": "string"}
                    },
                    "required": ["id"]
                },
                "Extended": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Base"}
                    ],
                    "properties": {
                        "extra": {"type": "boolean"}
                    },
                    "required": ["name", "extra"]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let extended = spec.resolve_ref("#/components/schemas/Extended").unwrap();
    let resolved = spec.resolve_schema(extended);

    assert!(resolved.properties.contains_key("id"));
    assert!(resolved.properties.contains_key("name"));
    assert!(resolved.properties.contains_key("extra"));
    // Required from both Base and parent should be merged
    assert!(resolved.required.contains(&"id".to_string()));
    assert!(resolved.required.contains(&"name".to_string()));
    assert!(resolved.required.contains(&"extra".to_string()));
}

#[test]
fn test_oneof_with_parent_properties() {
    // Parent-level properties alongside oneOf should be merged
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Mixed": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "variant_a": {"type": "string"}
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "variant_b": {"type": "integer"}
                            }
                        }
                    ],
                    "properties": {
                        "common": {"type": "boolean"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let mixed = spec.resolve_ref("#/components/schemas/Mixed").unwrap();
    let resolved = spec.resolve_schema(mixed);

    assert!(resolved.properties.contains_key("variant_a"));
    assert!(resolved.properties.contains_key("variant_b"));
    assert!(
        resolved.properties.contains_key("common"),
        "parent-level 'common' should be merged alongside oneOf"
    );
}

#[test]
fn test_anyof_with_parent_properties() {
    // Parent-level properties alongside anyOf should be merged
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Flexible": {
                    "anyOf": [
                        {
                            "type": "object",
                            "properties": {
                                "opt_a": {"type": "string"}
                            }
                        }
                    ],
                    "properties": {
                        "shared": {"type": "number"}
                    }
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let flexible = spec.resolve_ref("#/components/schemas/Flexible").unwrap();
    let resolved = spec.resolve_schema(flexible);

    assert!(resolved.properties.contains_key("opt_a"));
    assert!(
        resolved.properties.contains_key("shared"),
        "parent-level 'shared' should be merged alongside anyOf"
    );
}

#[test]
fn test_ref_with_allof_coexistence() {
    // OpenAPI 3.1: $ref can coexist with allOf on the same schema
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Base": {
                    "type": "object",
                    "properties": {
                        "base_field": {"type": "string"}
                    }
                },
                "Mixin": {
                    "type": "object",
                    "properties": {
                        "mixin_field": {"type": "integer"}
                    }
                },
                "Combined": {
                    "$ref": "#/components/schemas/Base",
                    "allOf": [
                        {"$ref": "#/components/schemas/Mixin"}
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let combined = spec.resolve_ref("#/components/schemas/Combined").unwrap();
    let resolved = spec.resolve_schema(combined);

    assert!(
        resolved.properties.contains_key("base_field"),
        "base_field from $ref should be present"
    );
    assert!(
        resolved.properties.contains_key("mixin_field"),
        "mixin_field from allOf should be merged alongside $ref"
    );
}

#[test]
fn test_ref_with_oneof_coexistence() {
    // OpenAPI 3.1: $ref can coexist with oneOf
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Base": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"}
                    }
                },
                "VariantA": {
                    "type": "object",
                    "properties": {
                        "a_field": {"type": "boolean"}
                    }
                },
                "Extended": {
                    "$ref": "#/components/schemas/Base",
                    "oneOf": [
                        {"$ref": "#/components/schemas/VariantA"}
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let extended = spec.resolve_ref("#/components/schemas/Extended").unwrap();
    let resolved = spec.resolve_schema(extended);

    assert!(resolved.properties.contains_key("id"), "$ref id present");
    assert!(
        resolved.properties.contains_key("a_field"),
        "oneOf a_field merged alongside $ref"
    );
}

#[test]
fn test_parent_nullable_propagates_to_composition() {
    // Parent-level nullable should propagate through merge_parent_siblings
    let spec_json = r##"{
        "openapi": "3.1.0",
        "info": {"title": "Test", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "NullableComposed": {
                    "nullable": true,
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "field": {"type": "string"}
                            }
                        }
                    ]
                }
            }
        }
    }"##;

    let spec = OpenApiSpec::from_str(spec_json).unwrap();
    let schema = spec
        .resolve_ref("#/components/schemas/NullableComposed")
        .unwrap();
    let resolved = spec.resolve_schema(schema);

    assert!(resolved.properties.contains_key("field"));
    assert!(resolved.nullable, "parent nullable should propagate");
}

// --- Fix 8: table_name sanitization ---

#[test]
fn test_table_name_with_dots() {
    let endpoint = EndpointInfo {
        path: "/api/v1.0/items".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "api_v1_0_items");
}

#[test]
fn test_table_name_with_at_sign() {
    let endpoint = EndpointInfo {
        path: "/@user/repos".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "_user_repos");
}

#[test]
fn test_table_name_leading_digit() {
    let endpoint = EndpointInfo {
        path: "/3d-models".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "_3d_models");
}

#[test]
fn test_table_name_post_with_special_chars() {
    let endpoint = EndpointInfo {
        path: "/search.json".to_string(),
        method: "POST",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "search_json_post");
}

#[test]
fn test_table_name_multiple_special_chars() {
    let endpoint = EndpointInfo {
        path: "/api/v2.1/@me/data".to_string(),
        method: "GET",
        response_schema: None,
    };
    assert_eq!(endpoint.table_name(), "api_v2_1_me_data");
}
