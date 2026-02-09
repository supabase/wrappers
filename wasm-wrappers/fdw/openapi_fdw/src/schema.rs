//! Schema generation and type mapping for `OpenAPI` FDW
//!
//! This module handles mapping `OpenAPI` types to `PostgreSQL` types
//! and generating CREATE FOREIGN TABLE statements.

use crate::spec::{EndpointInfo, OpenApiSpec, Schema};

/// Maps `OpenAPI` schema types to `PostgreSQL` type names
pub fn openapi_to_pg_type(schema: &Schema, spec: &OpenApiSpec) -> &'static str {
    // First resolve the schema if it's a reference
    let resolved = if schema.reference.is_some() {
        spec.resolve_schema(schema)
    } else {
        schema.clone()
    };

    match resolved.schema_type.as_deref() {
        Some("string") => match resolved.format.as_deref() {
            Some("date") => "date",
            Some("date-time") => "timestamptz",
            // All other string formats map to text
            _ => "text",
        },
        Some("integer") => match resolved.format.as_deref() {
            Some("int32") => "integer",
            // int64 and others default to bigint for safety
            _ => "bigint",
        },
        Some("number") => match resolved.format.as_deref() {
            Some("float") => "real",
            // double and others default to double precision
            _ => "double precision",
        },
        Some("boolean") => "boolean",
        // array, object, and unknown types default to jsonb
        _ => "jsonb",
    }
}

/// Column definition for a foreign table
#[derive(Debug)]
pub struct ColumnDef {
    pub name: String,
    pub pg_type: &'static str,
    pub nullable: bool,
}

/// Extract column definitions from an `OpenAPI` response schema
pub fn extract_columns(schema: &Schema, spec: &OpenApiSpec, include_attrs: bool) -> Vec<ColumnDef> {
    let mut columns = Vec::new();

    // Resolve the schema (handles $ref)
    let resolved = spec.resolve_schema(schema);

    // If the response is an array, look at the items schema
    let item_schema = if resolved.schema_type.as_deref() == Some("array") {
        resolved.items.as_ref().map(|s| spec.resolve_schema(s))
    } else {
        Some(resolved)
    };

    if let Some(schema) = item_schema {
        // Check if this is an object with properties
        if !schema.properties.is_empty() {
            for (name, prop_schema) in &schema.properties {
                let pg_type = openapi_to_pg_type(prop_schema, spec);
                let nullable = !schema.required.contains(name) || prop_schema.nullable;

                columns.push(ColumnDef {
                    name: sanitize_column_name(name),
                    pg_type,
                    nullable,
                });
            }
        }
    }

    // Sort columns alphabetically, but put 'id' first if present
    columns.sort_by(|a, b| match (a.name.as_str(), b.name.as_str()) {
        ("id", _) => std::cmp::Ordering::Less,
        (_, "id") => std::cmp::Ordering::Greater,
        _ => a.name.cmp(&b.name),
    });

    // Add an 'attrs' column for the full JSON response, unless disabled or already exists
    if include_attrs && !columns.iter().any(|c| c.name == "attrs") {
        columns.push(ColumnDef {
            name: "attrs".to_string(),
            pg_type: "jsonb",
            nullable: true,
        });
    }

    columns
}

/// Sanitize a column name for `PostgreSQL` (converts `camelCase` to `snake_case`)
fn sanitize_column_name(name: &str) -> String {
    let mut result = String::new();

    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('_');
            result.push(c.to_ascii_lowercase());
        } else if c.is_alphanumeric() || c == '_' {
            result.push(c.to_ascii_lowercase());
        } else {
            result.push('_');
        }
    }

    // PostgreSQL identifiers cannot start with a digit
    if result.starts_with(|c: char| c.is_ascii_digit()) {
        result.insert(0, '_');
    }

    result
}

/// Quote a PostgreSQL identifier (table name, column name, etc.)
/// Doubles any internal double quotes and wraps in double quotes.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Generate a CREATE FOREIGN TABLE statement for an endpoint
pub fn generate_foreign_table(
    endpoint: &EndpointInfo,
    spec: &OpenApiSpec,
    server_name: &str,
    include_attrs: bool,
) -> String {
    let table_name = endpoint.table_name();

    let columns = endpoint.response_schema.as_ref().map_or_else(
        || {
            // Default columns if no schema is available
            let mut cols = vec![ColumnDef {
                name: "id".to_string(),
                pg_type: "text",
                nullable: false,
            }];
            if include_attrs {
                cols.push(ColumnDef {
                    name: "attrs".to_string(),
                    pg_type: "jsonb",
                    nullable: true,
                });
            }
            cols
        },
        |schema| extract_columns(schema, spec, include_attrs),
    );

    let column_defs: Vec<String> = columns
        .iter()
        .map(|col| {
            let not_null = if col.nullable { "" } else { " NOT NULL" };
            format!(
                "    {} {}{}",
                quote_identifier(&col.name),
                col.pg_type,
                not_null
            )
        })
        .collect();

    // Determine rowid_column:
    //  - Prefer an explicit 'id' column if available
    //  - Otherwise, prefer the first non-'attrs' column with a non-jsonb type
    //  - If no suitable column exists, omit rowid_column entirely
    let rowid_col: Option<&str> = columns
        .iter()
        .find(|c| c.name == "id")
        .or_else(|| {
            columns
                .iter()
                .find(|c| c.name != "attrs" && c.pg_type != "jsonb")
        })
        .map(|c| c.name.as_str());

    // Escape single quotes in option values for SQL
    let escaped_endpoint = endpoint.path.replace('\'', "''");

    let options = if let Some(rowid) = rowid_col {
        let escaped_rowid = rowid.replace('\'', "''");
        format!("    endpoint '{escaped_endpoint}',\n    rowid_column '{escaped_rowid}'")
    } else {
        format!("    endpoint '{escaped_endpoint}'")
    };

    format!(
        r"CREATE FOREIGN TABLE IF NOT EXISTS {} (
{}
)
SERVER {} OPTIONS (
{}
)",
        quote_identifier(&table_name),
        column_defs.join(",\n"),
        quote_identifier(server_name),
        options
    )
}

/// Generate CREATE FOREIGN TABLE statements for all endpoints in a spec
pub fn generate_all_tables(
    spec: &OpenApiSpec,
    server_name: &str,
    filter: Option<&[String]>,
    exclude: bool,
    include_attrs: bool,
) -> Vec<String> {
    let endpoints = spec.get_endpoints();

    endpoints
        .iter()
        .filter(|e| {
            let table_name = e.table_name();
            match filter {
                None => true,
                Some(list) if exclude => !list.iter().any(|n| n == &table_name),
                Some(list) => list.iter().any(|n| n == &table_name),
            }
        })
        .map(|e| generate_foreign_table(e, spec, server_name, include_attrs))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_column_name() {
        assert_eq!(sanitize_column_name("userName"), "user_name");
        assert_eq!(sanitize_column_name("user-name"), "user_name");
        assert_eq!(sanitize_column_name("123abc"), "_123abc");
        assert_eq!(sanitize_column_name("already_snake"), "already_snake");
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
}
