---
source:
documentation: https://spec.openapis.org/
author: Cody Bromley(https://github.com/codybrom)
tags:
  - wasm
  - api
  - community
---

# OpenAPI

[OpenAPI](https://www.openapis.org/) is a specification for describing HTTP APIs. The OpenAPI Wrapper is a generic WebAssembly (Wasm) foreign data wrapper that can connect to any REST API with an OpenAPI 3.0+ specification.

This wrapper allows you to query any REST API endpoint as a PostgreSQL foreign table, with support for path parameters, pagination, POST-for-read endpoints, and automatic schema import.

## Available Versions

| Version | Wasm Package URL | Checksum | Required Wrappers Version |
| ------- | ---------------- | -------- | ------------------------- |
| 0.2.0 | `https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm` | `f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa` | >=0.5.0 |
| 0.1.4 | `https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.1.4/openapi_fdw.wasm` | `dd434f8565b060b181d1e69e1e4d5c8b9c3ac5ca444056d3c2fb939038d308fe` | >=0.5.0 |

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
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
        fdw_package_name 'supabase:openapi-fdw',
        fdw_package_version '0.2.0',
        fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
        base_url 'https://api.example.com/v1',
        api_key_id '<key_ID>'  -- The Key ID from Vault
      );
    ```

=== "Without Vault"

    ```sql
    create server my_api_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
        fdw_package_name 'supabase:openapi-fdw',
        fdw_package_version '0.2.0',
        fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
        base_url 'https://api.example.com/v1',
        api_key 'your-api-key'
      );
    ```

### Server Options

| Option | Required | Description |
| ------ | :------: | ----------- |
| `fdw_package_*` | Yes | Standard Wasm FDW package metadata. See [Available Versions](#available-versions). |
| `base_url` | Yes* | Base URL for the API (e.g., `https://api.example.com/v1`). *Optional if `spec_url` or `spec_json` provides servers. |
| `spec_url` | No | URL to the OpenAPI specification (JSON or YAML). Required for `IMPORT FOREIGN SCHEMA`. Mutually exclusive with `spec_json`. |
| `spec_json` | No | Inline OpenAPI 3.0+ JSON spec for `IMPORT FOREIGN SCHEMA`. Mutually exclusive with `spec_url`. Useful when the API doesn't publish a spec URL. |
| `api_key` | No | API key for authentication. |
| `api_key_id` | No | Vault secret key ID storing the API key. Use instead of `api_key`. |
| `api_key_header` | No | Header name for API key (default: `Authorization`). |
| `api_key_prefix` | No | Prefix for API key value (default: `Bearer` for Authorization header). |
| `api_key_location` | No | Where to send the API key: `header` (default), `query`, or `cookie`. |
| `bearer_token` | No | Bearer token for authentication (alternative to `api_key`). |
| `bearer_token_id` | No | Vault secret key ID storing the bearer token. |
| `user_agent` | No | Custom User-Agent header value. |
| `accept` | No | Custom Accept header for content negotiation (e.g., `application/geo+json`). |
| `headers` | No | Custom headers as JSON object (e.g., `'{"X-Custom": "value"}'`). |
| `include_attrs` | No | Include `attrs` jsonb column in `IMPORT FOREIGN SCHEMA` output (default: `'true'`). Set to `'false'` to omit. |
| `page_size` | No | Default page size for pagination (0 = no automatic limit). |
| `page_size_param` | No | Query parameter name for page size (default: `limit`). |
| `cursor_param` | No | Query parameter name for pagination cursor (default: `after`). |
| `max_pages` | No | Maximum pages per scan to prevent infinite pagination loops (default: `1000`). |
| `max_response_bytes` | No | Maximum response body size in bytes (default: `52428800` / 50 MiB). |
| `debug` | No | Emit HTTP request details and scan stats via PostgreSQL INFO messages when set to `'true'` or `'1'`. |

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists openapi;
```

## Creating Foreign Tables

### Manual Table Creation

Create foreign tables manually by specifying the endpoint and columns:

```sql
create foreign table openapi.users (
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
| `method` | No | HTTP method for this endpoint. Use `POST` for read-via-POST endpoints (default: `GET`). |
| `request_body` | No | Request body string for POST endpoints. |

### Automatic Schema Import

If you provide a `spec_url` or `spec_json` in the server options, you can automatically import table definitions:

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

!!! note
    `IMPORT FOREIGN SCHEMA` only generates tables for non-parameterized GET endpoints (e.g., `/users`, `/orders`). Endpoints with path parameters like `/users/{user_id}/posts` are skipped because they require WHERE clause values at query time. Create these tables manually using the `endpoint` option with `{param}` placeholders — see [Path Parameters](#path-parameters) for examples.

## Path Parameters

The OpenAPI FDW supports path parameter substitution. Define parameters in the endpoint template using `{param_name}` syntax, and provide values via WHERE clauses:

```sql
-- Endpoint template with path parameter
create foreign table openapi.user_posts (
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
select * from openapi.user_posts where user_id = '123';
```

### Multiple Path Parameters

```sql
create foreign table openapi.project_issues (
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
select * from openapi.project_issues where org = 'acme' and repo = 'widgets';
```

## Query Pushdown

### Single Resource Access

When filtering by the `rowid_column`, the FDW automatically requests a single resource:

```sql
-- Generates GET /users/user-123
select * from openapi.users where id = 'user-123';
```

### Query Parameters

Other WHERE clause filters are passed as query parameters:

```sql
-- Generates GET /users?status=active
select * from openapi.users where status = 'active';
```

Columns used as query or path parameters always return the value from the WHERE clause, even if the API response contains the same field with different casing. This ensures PostgreSQL's post-filter always passes.

### LIMIT Pushdown

When your query includes a `LIMIT`, the FDW uses it as the `page_size` for the first API request, reducing unnecessary data transfer:

```sql
-- Sends GET /users?limit=5 (uses LIMIT as page_size)
select * from openapi.users limit 5;
```

## POST-for-Read Endpoints

Some APIs use POST requests for read operations (e.g., search or query endpoints). Use the `method` and `request_body` table options:

```sql
create foreign table openapi.search_results (
  id text,
  title text,
  score real,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/search',
  method 'POST',
  request_body '{"query": "openapi", "limit": 50}'
);

select id, title, score from openapi.search_results;
```

## Debug Mode

Enable debug mode to see HTTP request details and scan statistics in PostgreSQL INFO messages:

```sql
create server debug_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://api.example.com',
    debug 'true'
  );
```

Debug output includes:

- HTTP method and URL for each request
- Response status code and body size
- Total rows fetched and pages retrieved
- Pagination details

## Pagination

The FDW automatically handles pagination. It supports:

1. **Cursor-based pagination** - Uses `cursor_param` and `cursor_path`
2. **URL-based pagination** - Follows `next` links in response
3. **Offset-based pagination** - Auto-detected from common patterns

### Configuring Pagination

```sql
create server paginated_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://openapi.example.com',
    page_size '100',
    page_size_param 'limit',
    cursor_param 'cursor'
  );
```

```sql
create foreign table openapi.items (
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
create foreign table openapi.locations (
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
| smallint* | number |
| integer | number |
| bigint | number |
| real | number |
| double precision | number |
| numeric* | number |
| date | string (ISO 8601) |
| timestamp* | string (ISO 8601) |
| timestamptz | string (ISO 8601) |
| jsonb | object/array |
| uuid | string |

\* Types marked with an asterisk work when you define tables manually, but `IMPORT FOREIGN SCHEMA` won't generate columns with these types automatically.

### The `attrs` Column

Any foreign table can include an `attrs` column of type `jsonb` to capture the entire raw JSON response for each row:

```sql
create foreign table openapi.users (
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

## Automatic Retries

The FDW automatically retries transient HTTP errors up to 3 times:

- **HTTP 429** (Rate Limit), **502** (Bad Gateway), **503** (Service Unavailable)
- **Retry-After header**: Respects server-specified delay when provided
- **Exponential backoff**: Falls back to 1s, 2s, 4s delays when no Retry-After header is present

For APIs with very strict rate limits, consider using materialized views to cache results.

## Examples

For additional real-world examples with multiple tables, pagination, and advanced features, see the **[examples directory on GitHub](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/openapi_fdw/examples)**. There are step-by-step walkthroughs for querying the NWS Weather API, PokéAPI, CarAPI, GitHub, and Threads.

### Basic Query

```sql
-- Create a foreign server connecting to the Weather.gov API
create server openapi_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://api.weather.gov',
    spec_url 'https://api.weather.gov/openapi.json'
  );

-- Create a schema to hold the imported foreign tables
create schema if not exists openapi;

-- Auto-import all API endpoints as foreign tables based on the OpenAPI spec
import foreign schema openapi from server openapi_server into openapi;

-- Query the stations endpoint to get weather station data
select * from openapi.stations limit 5;
```

### Nested Resources

```sql
-- Create a foreign table for a parameterized endpoint with {zone_id} path parameter
create foreign table openapi.zone_stations (
  zone_id text,
  id text,
  type text,
  attrs jsonb
) server openapi_server
options (
  endpoint '/zones/forecast/{zone_id}/stations',
  rowid_column 'id'
);

-- Query stations for Alaska zone AKZ317 - generates GET /zones/forecast/AKZ317/stations
select id, type from openapi.zone_stations where zone_id = 'AKZ317';
```

### POST-for-Read

```sql
-- Query a search API that uses POST for read operations
create foreign table openapi.search_results (
  id text,
  title text,
  score real,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/search',
  method 'POST',
  request_body '{"query": "postgresql", "limit": 25}'
);

select id, title, score from openapi.search_results;
```

### Custom Headers

```sql
create server custom_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://openapi.example.com',
    api_key 'your-key',
    user_agent 'MyApp/1.0',
    accept 'application/json',
    headers '{"X-Request-ID": "postgres-fdw", "X-Feature-Flag": "beta"}'
  );
```

### API Key Location

By default, the API key is sent as a header. Use `api_key_location` to send it as a query parameter or cookie instead:

```sql
create server query_auth_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://api.example.com',
    api_key 'sk-your-api-key',
    api_key_location 'query'  -- sends as ?api_key=sk-... (uses api_key_header as param name)
  );
```

### Response Path Extraction

For APIs that wrap data in a container object:

```sql
-- API returns: {"data": [...], "meta": {...}}
create foreign table openapi.items (
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
select * from openapi.users;

-- Query the cache
select * from api_users_cache;

-- Refresh when needed
refresh materialized view api_users_cache;
```
