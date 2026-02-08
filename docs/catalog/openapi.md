---
source:
documentation: https://spec.openapis.org/
author: supabase
tags:
  - wasm
  - api
---

# OpenAPI

[OpenAPI](https://www.openapis.org/) is a specification for describing HTTP APIs. The OpenAPI Wrapper is a generic WebAssembly (Wasm) foreign data wrapper that can connect to any REST API with an OpenAPI 3.0+ specification.

This wrapper allows you to query any REST API endpoint as a PostgreSQL foreign table, with support for path parameters, pagination, and automatic schema import.

## Available Versions

| Version | Wasm Package URL | Checksum | Required Wrappers Version |
| ------- | ---------------- | -------- | ------------------------- |
| 0.1.3   | `https://github.com/codybrom/openapi-fdw/releases/download/v0.1.3/openapi_fdw.wasm` | `711002e9d19136320f84ee7e166fddaf3b5abd91d36ec85dc64512027cce00c2` | >=0.5.0 |

## Preparation

Before you can query an API, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the OpenAPI Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your API key in Vault and retrieve the created `key_id`
select vault.create_secret(
  'your-api-key',
  'my_api',
  'API key for My API'
);
```

### Connecting to an API

We need to provide Postgres with the credentials to access the API and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server my_api_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_name 'supabase:openapi-fdw',
        fdw_package_url '{See: "Available Versions"}',
        fdw_package_checksum '{See: "Available Versions"}',
        fdw_package_version '{See: "Available Versions"}',
        base_url 'https://api.example.com/v1',
        api_key_id '<key_ID>'  -- The Key ID from Vault
      );
    ```

=== "Without Vault"

    ```sql
    create server my_api_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_name 'supabase:openapi-fdw',
        fdw_package_url '{See: "Available Versions"}',
        fdw_package_checksum '{See: "Available Versions"}',
        fdw_package_version '{See: "Available Versions"}',
        base_url 'https://api.example.com/v1',
        api_key 'your-api-key'
      );
    ```

### Server Options

| Option | Required | Description |
| ------ | :------: | ----------- |
| `fdw_package_*` | Yes | Standard Wasm FDW package metadata. See [Available Versions](#available-versions). |
| `base_url` | Yes* | Base URL for the API (e.g., `https://api.example.com/v1`). *Optional if `spec_url` provides servers. |
| `spec_url` | No | URL to the OpenAPI specification JSON. Required for `IMPORT FOREIGN SCHEMA`. |
| `api_key` | No | API key for authentication. |
| `api_key_id` | No | Vault secret key ID storing the API key. Use instead of `api_key`. |
| `api_key_header` | No | Header name for API key (default: `Authorization`). |
| `api_key_prefix` | No | Prefix for API key value (default: `Bearer` for Authorization header). |
| `bearer_token` | No | Bearer token for authentication (alternative to `api_key`). |
| `bearer_token_id` | No | Vault secret key ID storing the bearer token. |
| `user_agent` | No | Custom User-Agent header value. |
| `accept` | No | Custom Accept header for content negotiation (e.g., `application/geo+json`). |
| `headers` | No | Custom headers as JSON object (e.g., `'{"X-Custom": "value"}'`). |
| `page_size` | No | Default page size for pagination (0 = no automatic limit). |
| `page_size_param` | No | Query parameter name for page size (default: `limit`). |
| `cursor_param` | No | Query parameter name for pagination cursor (default: `after`). |

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists api;
```

## Creating Foreign Tables

### Manual Table Creation

Create foreign tables manually by specifying the endpoint and columns:

```sql
create foreign table api.users (
  id text,
  name text,
  email text,
  created_at timestamptz,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/users',
  rowid_column 'id'
);
```

### Table Options

| Option | Required | Description |
| ------ | :------: | ----------- |
| `endpoint` | Yes | API endpoint path (e.g., `/users`, `/users/{user_id}/posts`). |
| `rowid_column` | No | Column used as row identifier for single-resource access and modifications (default: `id`). |
| `response_path` | No | JSON pointer to extract data array from response (e.g., `/data`, `/results`). |
| `object_path` | No | JSON pointer to extract nested object from each row (e.g., `/properties` for GeoJSON). |
| `cursor_path` | No | JSON pointer to pagination cursor in response. |
| `cursor_param` | No | Override server-level cursor parameter name. |
| `page_size_param` | No | Override server-level page size parameter name. |
| `page_size` | No | Override server-level page size. |

### Automatic Schema Import

If you provide a `spec_url` in the server options, you can automatically import table definitions:

```sql
-- Import all endpoints
import foreign schema openapi
  from server my_api_server
  into api;

-- Import specific endpoints only
import foreign schema openapi
  limit to ("users", "orders")
  from server my_api_server
  into api;

-- Import all except specific endpoints
import foreign schema openapi
  except ("internal_endpoint")
  from server my_api_server
  into api;
```

## Path Parameters

The OpenAPI FDW supports path parameter substitution. Define parameters in the endpoint template using `{param_name}` syntax, and provide values via WHERE clauses:

```sql
-- Endpoint template with path parameter
create foreign table api.user_posts (
  user_id text,
  id text,
  title text,
  body text,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/users/{user_id}/posts',
  rowid_column 'id'
);

-- Query with path parameter - generates GET /users/123/posts
select * from api.user_posts where user_id = '123';
```

### Multiple Path Parameters

```sql
create foreign table api.project_issues (
  org text,
  repo text,
  id text,
  title text,
  status text,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/projects/{org}/{repo}/issues',
  rowid_column 'id'
);

-- Generates GET /projects/acme/widgets/issues
select * from api.project_issues where org = 'acme' and repo = 'widgets';
```

## Query Pushdown

### Single Resource Access

When filtering by the `rowid_column`, the FDW automatically requests a single resource:

```sql
-- Generates GET /users/user-123
select * from api.users where id = 'user-123';
```

### Query Parameters

Other WHERE clause filters are passed as query parameters:

```sql
-- Generates GET /users?status=active
select * from api.users where status = 'active';
```

Columns used as query or path parameters always return the value from the WHERE clause, even if the API response contains the same field with different casing. This ensures PostgreSQL's post-filter always passes.

## Pagination

The FDW automatically handles pagination. It supports:

1. **Cursor-based pagination** - Uses `cursor_param` and `cursor_path`
2. **URL-based pagination** - Follows `next` links in response

### Configuring Pagination

```sql
create server paginated_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_url '{See: "Available Versions"}',
    fdw_package_checksum '{See: "Available Versions"}',
    fdw_package_version '{See: "Available Versions"}',
    base_url 'https://api.example.com',
    page_size '100',
    page_size_param 'limit',
    cursor_param 'cursor'
  );
```

```sql
create foreign table api.items (
  id text,
  name text,
  attrs jsonb
)
server paginated_api
options (
  endpoint '/items',
  cursor_path '/meta/next_cursor'
);
```

## GeoJSON Support

For APIs that return GeoJSON, use `object_path` to extract properties:

```sql
create foreign table api.locations (
  id text,
  name text,
  category text,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/locations',
  response_path '/features',
  object_path '/properties'
);
```

## Supported Data Types

| Postgres Type | JSON Type |
| ------------- | --------- |
| text | string |
| boolean | boolean |
| smallint | number |
| integer | number |
| bigint | number |
| real | number |
| double precision | number |
| numeric | number |
| date | string (ISO 8601) |
| timestamp | string (ISO 8601) |
| timestamptz | string (ISO 8601) |
| jsonb | object/array |
| uuid | string |

### The `attrs` Column

Any foreign table can include an `attrs` column of type `jsonb` to capture the entire raw JSON response for each row:

```sql
create foreign table api.users (
  id text,
  name text,
  attrs jsonb  -- Contains full JSON object
)
server my_api_server
options (endpoint '/users');
```

## Limitations

- **Read-only**: This FDW only supports SELECT operations. INSERT, UPDATE, and DELETE are not supported at this time.
- **No transactions**: Each SQL statement results in immediate HTTP requests; there is no transactional grouping.
- **Authentication**: Currently supports API Key and Bearer Token authentication. OAuth flows are not supported.
- **OpenAPI version**: Only OpenAPI 3.0+ specifications are supported (not Swagger 2.0).

## Rate Limiting

The FDW automatically handles rate limiting:

- **HTTP 429 responses**: Automatically retries up to 3 times
- **Retry-After header**: Respects server-specified delay when provided
- **Exponential backoff**: Falls back to 1s, 2s, 4s delays when no Retry-After header is present

For APIs with very strict rate limits, consider using materialized views to cache results.

## Examples

### Basic Query

```sql
create foreign table api.users (
  id text,
  name text,
  email text,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/users',
  rowid_column 'id'
);

-- List all users
select id, name, email from api.users;

-- Get a specific user
select * from api.users where id = 'user-123';
```

### Nested Resources

```sql
create foreign table api.user_orders (
  user_id text,
  id text,
  total numeric,
  status text,
  created_at timestamptz,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/users/{user_id}/orders',
  rowid_column 'id'
);

-- Get orders for a specific user
select * from api.user_orders where user_id = 'user-123';

-- Get a specific order
select * from api.user_orders
where user_id = 'user-123' and id = 'order-456';
```

### Custom Headers

```sql
create server custom_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_url '{See: "Available Versions"}',
    fdw_package_checksum '{See: "Available Versions"}',
    fdw_package_version '{See: "Available Versions"}',
    base_url 'https://api.example.com',
    api_key 'your-key',
    user_agent 'MyApp/1.0',
    accept 'application/json',
    headers '{"X-Request-ID": "postgres-fdw", "X-Feature-Flag": "beta"}'
  );
```

### Response Path Extraction

For APIs that wrap data in a container object:

```sql
-- API returns: {"data": [...], "meta": {...}}
create foreign table api.items (
  id text,
  name text,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/items',
  response_path '/data'
);
```

### Combining with Materialized Views

For frequently accessed data, use materialized views to reduce API calls:

```sql
create materialized view api_users_cache as
select * from api.users;

-- Query the cache
select * from api_users_cache;

-- Refresh when needed
refresh materialized view api_users_cache;
```
