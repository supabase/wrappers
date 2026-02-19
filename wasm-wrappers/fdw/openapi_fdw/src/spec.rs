//! OpenAPI 3.0+ specification parsing
//!
//! This module provides types and functions for parsing OpenAPI specifications
//! and extracting endpoint/schema information for FDW table generation.

use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::cell::Cell;
use std::collections::HashMap;

/// Raw schema for deserialization — handles OpenAPI 3.1 type arrays.
///
/// OpenAPI 3.1 changed type from a string to potentially an array:
/// - 3.0: "type": "string" with "nullable": true
/// - 3.1: "type": ["string", "null"]
///
/// This intermediate struct captures the raw type field, then From<RawSchema>
/// extracts the actual type and sets nullable accordingly.
#[derive(Debug, Deserialize)]
struct RawSchema {
    #[serde(rename = "type")]
    #[serde(default)]
    schema_type: Option<JsonValue>,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    properties: HashMap<String, Schema>,
    #[serde(default)]
    items: Option<Box<Schema>>,
    #[serde(rename = "$ref")]
    #[serde(default)]
    reference: Option<String>,
    #[serde(default)]
    required: Vec<String>,
    #[serde(default)]
    nullable: bool,
    #[serde(rename = "writeOnly")]
    #[serde(default)]
    write_only: bool,
    #[serde(rename = "allOf")]
    #[serde(default)]
    all_of: Vec<Schema>,
    #[serde(rename = "oneOf")]
    #[serde(default)]
    one_of: Vec<Schema>,
    #[serde(rename = "anyOf")]
    #[serde(default)]
    any_of: Vec<Schema>,
}

impl From<RawSchema> for Schema {
    fn from(raw: RawSchema) -> Self {
        let (schema_type, type_has_null) = match raw.schema_type {
            None => (None, false),
            Some(JsonValue::String(s)) => (Some(s), false),
            Some(JsonValue::Array(arr)) => {
                let has_null = arr.iter().any(|v| v.as_str() == Some("null"));
                let non_null_types: Vec<&str> = arr
                    .iter()
                    .filter_map(|v| v.as_str())
                    .filter(|s| *s != "null")
                    .collect();
                // Multiple non-null types (e.g., ["string", "integer"]) → None (maps to jsonb)
                let actual = if non_null_types.len() == 1 {
                    Some(non_null_types[0].to_string())
                } else {
                    None
                };
                (actual, has_null)
            }
            Some(_) => (None, false),
        };

        Schema {
            schema_type,
            format: raw.format,
            properties: raw.properties,
            items: raw.items,
            reference: raw.reference,
            required: raw.required,
            // nullable if explicitly set OR if type array contains "null"
            nullable: raw.nullable || type_has_null,
            write_only: raw.write_only,
            all_of: raw.all_of,
            one_of: raw.one_of,
            any_of: raw.any_of,
        }
    }
}

/// Represents an OpenAPI 3.0+ specification
#[derive(Debug, Deserialize)]
pub struct OpenApiSpec {
    /// OpenAPI version (must be 3.x)
    pub openapi: String,
    #[allow(dead_code)] // Required by OpenAPI spec format
    info: Info,
    #[serde(default)]
    pub servers: Vec<Server>,
    #[serde(default)]
    pub paths: HashMap<String, PathItem>,
    #[serde(default)]
    pub components: Option<Components>,
}

/// API metadata
#[derive(Debug, Deserialize)]
struct Info {
    #[allow(dead_code)]
    title: String,
}

/// Server definition
#[derive(Debug, Deserialize)]
pub(crate) struct Server {
    pub url: String,
    #[serde(default)]
    pub variables: HashMap<String, ServerVariable>,
}

/// Server variable with a default value for URL template substitution
#[derive(Debug, Deserialize)]
pub(crate) struct ServerVariable {
    pub default: String,
}

/// Path item (GET and POST operations are used for foreign tables)
#[derive(Debug, Deserialize)]
pub(crate) struct PathItem {
    #[serde(default)]
    pub get: Option<Operation>,
    #[serde(default)]
    pub post: Option<Operation>,
}

/// Operation definition
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Operation {
    #[serde(default)]
    pub responses: HashMap<String, Response>,
}

/// Response definition
#[derive(Debug, Deserialize)]
pub(crate) struct Response {
    #[serde(rename = "$ref")]
    #[serde(default)]
    pub reference: Option<String>,
    #[serde(default)]
    pub content: HashMap<String, MediaType>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MediaType {
    #[serde(default)]
    pub schema: Option<Schema>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(from = "RawSchema")]
#[allow(clippy::struct_field_names)]
pub struct Schema {
    pub schema_type: Option<String>,
    pub format: Option<String>,
    pub properties: HashMap<String, Self>,
    pub items: Option<Box<Self>>,
    pub reference: Option<String>,
    pub required: Vec<String>,
    pub nullable: bool,
    pub write_only: bool,
    pub all_of: Vec<Self>,
    pub one_of: Vec<Self>,
    pub any_of: Vec<Self>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Components {
    #[serde(default)]
    pub schemas: HashMap<String, Schema>,
    #[serde(default)]
    pub responses: HashMap<String, Response>,
}

impl OpenApiSpec {
    /// Parse an OpenAPI spec from a JSON value
    pub fn from_json(json: JsonValue) -> Result<Self, String> {
        let spec: Self = serde_json::from_value(json)
            .map_err(|e| format!("Failed to parse OpenAPI spec: {e}"))?;

        if !spec.openapi.starts_with("3.") {
            return Err(format!(
                "Unsupported OpenAPI version '{}'. Only OpenAPI 3.x specifications are supported (not Swagger 2.0).",
                spec.openapi
            ));
        }

        Ok(spec)
    }

    /// Parse an OpenAPI spec from a JSON string (used in tests)
    #[cfg(test)]
    pub fn from_str(s: &str) -> Result<Self, String> {
        let spec: Self =
            serde_json::from_str(s).map_err(|e| format!("Failed to parse OpenAPI spec: {e}"))?;

        if !spec.openapi.starts_with("3.") {
            return Err(format!(
                "Unsupported OpenAPI version '{}'. Only OpenAPI 3.x specifications are supported (not Swagger 2.0).",
                spec.openapi
            ));
        }

        Ok(spec)
    }

    /// Parse an OpenAPI spec from a YAML string (used in tests)
    #[cfg(test)]
    pub fn from_yaml_str(s: &str) -> Result<Self, String> {
        let json: JsonValue =
            serde_yaml_ng::from_str(s).map_err(|e| format!("Failed to parse YAML: {e}"))?;
        Self::from_json(json)
    }

    /// Get the base URL from the spec's first server entry, substituting any variables.
    ///
    /// Only the first server is used. Multi-server specs should set the base_url
    /// server option explicitly to select a different server.
    pub fn base_url(&self) -> Option<String> {
        self.servers.first().map(|s| {
            let mut url = s.url.clone();
            for (name, var) in &s.variables {
                url = url.replace(&format!("{{{name}}}"), &var.default);
            }
            url
        })
    }

    /// Get all endpoint paths that support GET or POST operations (for querying).
    ///
    /// Parameterized paths (e.g., /users/{id}, /users/{user_id}/posts) are
    /// excluded because they require path parameter values from WHERE clauses at
    /// query time. Users should create these tables manually with the appropriate
    /// endpoint option containing {param} placeholders. See the documentation
    /// for path parameter examples.
    pub fn get_endpoints(&self) -> Vec<EndpointInfo> {
        let mut endpoints = Vec::new();

        for (path, path_item) in &self.paths {
            // Skip parameterized paths — they require WHERE clause values at query time
            // and must be created manually. See docs for path parameter examples.
            if path.contains('{') {
                continue;
            }

            if let Some(ref op) = path_item.get {
                let response_schema = self.get_response_schema(op);
                endpoints.push(EndpointInfo {
                    path: path.clone(),
                    method: "GET",
                    response_schema,
                });
            }

            if let Some(ref op) = path_item.post {
                let response_schema = self.get_response_schema(op);
                endpoints.push(EndpointInfo {
                    path: path.clone(),
                    method: "POST",
                    response_schema,
                });
            }
        }

        endpoints.sort_by(|a, b| a.path.cmp(&b.path).then(a.method.cmp(b.method)));
        endpoints
    }

    /// Success response codes to check, in priority order
    const SUCCESS_RESPONSE_CODES: &[&str] = &["200", "201", "2XX", "default"];

    /// Get the response schema for a successful response (200, 201, 2XX, or default)
    fn get_response_schema(&self, op: &Operation) -> Option<Schema> {
        let response = Self::SUCCESS_RESPONSE_CODES
            .iter()
            .find_map(|code| op.responses.get(*code))?;

        // Resolve $ref at the response level (e.g., "$ref": "#/components/responses/Success")
        let resolved_response = response
            .reference
            .as_ref()
            .and_then(|r| self.resolve_response_ref(r))
            .unwrap_or(response);

        let media_type = resolved_response
            .content
            .iter()
            .find(|(k, _)| k.starts_with("application/json"))
            .map(|(_, v)| v)
            .or_else(|| resolved_response.content.values().next())?;

        media_type.schema.clone()
    }

    /// Parse a #/components/{section}/{name} reference, returning the name if it matches.
    fn parse_component_ref<'a>(reference: &'a str, section: &str) -> Option<&'a str> {
        let path = reference.strip_prefix("#/components/")?;
        let name = path.strip_prefix(section)?.strip_prefix('/')?;
        // Reject if name contains further slashes (e.g., "#/components/schemas/a/b")
        if name.contains('/') {
            return None;
        }
        Some(name)
    }

    /// Resolve a $ref to a response in components.responses
    fn resolve_response_ref(&self, reference: &str) -> Option<&Response> {
        let name = Self::parse_component_ref(reference, "responses")?;
        self.components.as_ref()?.responses.get(name)
    }

    /// Resolve a $ref to its schema
    pub fn resolve_ref(&self, reference: &str) -> Option<&Schema> {
        let name = Self::parse_component_ref(reference, "schemas")?;
        self.components.as_ref()?.schemas.get(name)
    }

    /// Recursively resolve a schema, following $ref pointers and handling composition.
    /// Uses depth limiting and a call counter to prevent infinite recursion and
    /// exponential blowup on branching schemas.
    pub fn resolve_schema(&self, schema: &Schema) -> Schema {
        let call_count = Cell::new(0usize);
        self.resolve_schema_internal(schema, 0, &call_count)
    }

    /// Maximum depth for schema resolution to prevent stack overflow on circular refs
    const MAX_RESOLVE_DEPTH: usize = 32;

    /// Maximum total resolve calls to prevent exponential blowup on branching schemas
    const MAX_RESOLVE_CALLS: usize = 10_000;

    /// Internal schema resolution with depth and call-count tracking
    fn resolve_schema_internal(
        &self,
        schema: &Schema,
        depth: usize,
        call_count: &Cell<usize>,
    ) -> Schema {
        let count = call_count.get() + 1;
        call_count.set(count);

        // Guard against circular references and exponential blowup
        if depth > Self::MAX_RESOLVE_DEPTH || count > Self::MAX_RESOLVE_CALLS {
            return schema.clone();
        }

        // First resolve any $ref
        if let Some(ref reference) = schema.reference {
            if let Some(resolved) = self.resolve_ref(reference) {
                let mut result = self.resolve_schema_internal(resolved, depth + 1, call_count);
                // Merge non-default siblings (OpenAPI 3.1 $ref with siblings)
                if schema.nullable {
                    result.nullable = true;
                }
                if schema.write_only {
                    result.write_only = true;
                }
                for (name, prop) in &schema.properties {
                    result.properties.insert(name.clone(), prop.clone());
                }
                if !schema.required.is_empty() {
                    result.required.extend(schema.required.iter().cloned());
                    result.required.sort();
                    result.required.dedup();
                }
                // OpenAPI 3.1: $ref can coexist with composition keywords
                if !schema.all_of.is_empty() {
                    let allof = self.merge_allof_schemas(&schema.all_of, depth + 1, call_count);
                    for (name, prop) in allof.properties {
                        result.properties.insert(name, prop);
                    }
                    result.required.extend(allof.required);
                    result.required.sort();
                    result.required.dedup();
                }
                if !schema.one_of.is_empty() {
                    let oneof = self.merge_union_schemas(&schema.one_of, depth + 1, call_count);
                    for (name, prop) in oneof.properties {
                        result.properties.entry(name).or_insert(prop);
                    }
                }
                if !schema.any_of.is_empty() {
                    let anyof = self.merge_union_schemas(&schema.any_of, depth + 1, call_count);
                    for (name, prop) in anyof.properties {
                        result.properties.entry(name).or_insert(prop);
                    }
                }
                return result;
            }
        }

        // Handle allOf by merging all properties (intersection - all schemas apply)
        if !schema.all_of.is_empty() {
            let mut merged = self.merge_allof_schemas(&schema.all_of, depth + 1, call_count);
            // Merge parent-level properties/required alongside allOf (OpenAPI 3.1)
            Self::merge_parent_siblings(schema, &mut merged);
            return merged;
        }

        // Handle oneOf by merging all possible properties as nullable (union - one of the schemas)
        if !schema.one_of.is_empty() {
            let mut merged = self.merge_union_schemas(&schema.one_of, depth + 1, call_count);
            Self::merge_parent_siblings(schema, &mut merged);
            return merged;
        }

        // Handle anyOf by merging all possible properties as nullable (union - any of the schemas)
        if !schema.any_of.is_empty() {
            let mut merged = self.merge_union_schemas(&schema.any_of, depth + 1, call_count);
            Self::merge_parent_siblings(schema, &mut merged);
            return merged;
        }

        schema.clone()
    }

    /// Merge allOf schemas: later schemas refine/override earlier ones, required fields preserved.
    fn merge_allof_schemas(
        &self,
        schemas: &[Schema],
        depth: usize,
        call_count: &Cell<usize>,
    ) -> Schema {
        let mut merged = Schema {
            properties: HashMap::new(),
            required: Vec::new(),
            ..Default::default()
        };

        let mut has_any_properties = false;

        for sub_schema in schemas {
            let resolved = self.resolve_schema_internal(sub_schema, depth, call_count);

            if !resolved.properties.is_empty() {
                has_any_properties = true;
            }

            // Later schemas refine/override earlier ones
            for (name, prop_schema) in resolved.properties {
                merged.properties.insert(name, prop_schema);
            }

            merged.required.extend(resolved.required);
        }

        if has_any_properties {
            merged.schema_type = Some("object".to_string());
        }

        merged.required.sort();
        merged.required.dedup();

        merged
    }

    /// Merge oneOf/anyOf schemas: all properties become nullable, first definition wins.
    fn merge_union_schemas(
        &self,
        schemas: &[Schema],
        depth: usize,
        call_count: &Cell<usize>,
    ) -> Schema {
        let mut merged = Schema {
            properties: HashMap::new(),
            required: Vec::new(),
            ..Default::default()
        };

        let mut has_any_properties = false;

        for sub_schema in schemas {
            let resolved = self.resolve_schema_internal(sub_schema, depth, call_count);

            if !resolved.properties.is_empty() {
                has_any_properties = true;
            }

            // Keep first definition (most permissive), mark all nullable
            for (name, mut prop_schema) in resolved.properties {
                prop_schema.nullable = true;
                merged.properties.entry(name).or_insert(prop_schema);
            }

            // Nothing is required — we don't know which variant applies
        }

        // Only set type to "object" if at least one sub-schema has properties.
        // Primitive composition (e.g., oneOf: [{type: "string"}, {type: "integer"}])
        // should produce None (→ jsonb), not "object".
        if has_any_properties {
            merged.schema_type = Some("object".to_string());
        }

        merged
    }

    /// Merge parent-level properties and required into a composition result.
    ///
    /// Per OpenAPI 3.1, properties defined alongside allOf/oneOf/anyOf
    /// should be merged into the composed schema (parent properties override).
    fn merge_parent_siblings(parent: &Schema, merged: &mut Schema) {
        for (name, prop) in &parent.properties {
            merged.properties.insert(name.clone(), prop.clone());
        }
        if !parent.required.is_empty() {
            merged.required.extend(parent.required.iter().cloned());
            merged.required.sort();
            merged.required.dedup();
        }
        if parent.nullable {
            merged.nullable = true;
        }
        if parent.write_only {
            merged.write_only = true;
        }
        // Promote to object if parent has properties
        if !parent.properties.is_empty() && merged.schema_type.is_none() {
            merged.schema_type = Some("object".to_string());
        }
    }
}

/// Extracted endpoint information for table generation
#[derive(Debug)]
pub struct EndpointInfo {
    pub path: String,
    pub method: &'static str,
    pub response_schema: Option<Schema>,
}

impl EndpointInfo {
    /// Generate a table name from the endpoint path.
    ///
    /// Uses the full path to avoid collisions (e.g., /v1/users and /v2/users
    /// become v1_users and v2_users instead of both becoming users).
    /// POST endpoints get a _post suffix to avoid collisions with GET tables.
    pub fn table_name(&self) -> String {
        let cleaned = self.path.trim_matches('/');

        let mut base = if cleaned.is_empty() {
            "unknown".to_string()
        } else {
            // Join path segments with '_' and convert kebab-case to snake_case
            let mut name = cleaned.replace(['/', '-'], "_");
            // Replace remaining non-alphanumeric/non-underscore chars with '_'
            name = name
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '_' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            // Collapse consecutive underscores
            while name.contains("__") {
                name = name.replace("__", "_");
            }
            // Trim trailing underscores
            name.trim_end_matches('_').to_string()
        };

        // Prepend '_' if starts with digit
        if base.starts_with(|c: char| c.is_ascii_digit()) {
            base.insert(0, '_');
        }

        if self.method == "POST" {
            format!("{base}_post")
        } else {
            base
        }
    }
}

#[cfg(test)]
#[path = "spec_tests.rs"]
mod tests;
