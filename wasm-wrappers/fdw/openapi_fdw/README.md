# OpenAPI WASM Foreign Data Wrapper

Created by [Cody Bromley](https://github.com/codybrom).

A WASM-based Foreign Data Wrapper (FDW) that lets you query any REST API with an [OpenAPI 3.0+](https://www.openapis.org/) spec as PostgreSQL foreign tables, built on [Supabase Wrappers](https://github.com/supabase/wrappers).

Point it at an OpenAPI spec and query the API with SQL. The FDW parses the spec for endpoints and response schemas, and allows you to `IMPORT FOREIGN SCHEMA` to generate tables automatically or manually create tables for parameterized endpoints.

## Features

- **Automatic schema import** — Reads OpenAPI 3.0/3.1 specs in JSON or YAML (via `spec_url` or inline `spec_json`) and generates foreign tables with `IMPORT FOREIGN SCHEMA`
- **Automatic pagination** — Detects and follows cursor-based, offset-based, and Link-header pagination across multiple pages
- **Path parameter support** — Substitutes path parameters from WHERE clauses (e.g., `WHERE user_id = '123'` fills `/users/{user_id}/posts`)
- **Query pushdown** — Forwards non-path WHERE clauses as query parameters to filter at the API level
- **LIMIT pushdown** — Passes `LIMIT` to the API's page-size parameter to avoid over-fetching
- **POST-for-read** — Supports APIs that use POST for search/query endpoints via the `method` table option
- **Rate-limit handling** — Retries automatically with exponential backoff on HTTP 429 responses
- **Type coercion** — Maps JSON types to PostgreSQL types (`text`, `integer`, `boolean`, `timestamptz`, `jsonb`, etc.)
- **camelCase matching** — Matches API field names like `stationIdentifier` to snake_case columns like `station_identifier`
- **Auth support** — API key (header, query param, or cookie) and Bearer token authentication, with Supabase Vault integration
- **Debug mode** — Set `debug 'true'` on the server to log HTTP request/response details as PostgreSQL INFO messages

## Limitations

- Read-only (no INSERT/UPDATE/DELETE)
- POST-for-read available via `method` table option, but only GET endpoints are auto-imported
- Auth: API key and Bearer token only (no OAuth2 flows — use pre-obtained tokens)
- OpenAPI 3.x only (Swagger 2.0 is rejected)

## Documentation

Full reference: [fdw.dev/catalog/openapi](https://fdw.dev/catalog/openapi/)

## Quick Start

```sql
-- Create a server pointing to any OpenAPI-compliant API
CREATE SERVER my_api_server
FOREIGN DATA WRAPPER wasm_wrapper
OPTIONS (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum '{see docs for latest checksum}',
    base_url 'https://api.example.com/v1',
    spec_url 'https://api.example.com/openapi.json',
    api_key_id '<vault_secret_id>'
);

-- Import all endpoints as tables
IMPORT FOREIGN SCHEMA openapi 
FROM SERVER my_api_server
INTO openapi;

-- Or create tables manually
CREATE FOREIGN TABLE api.users (
    id text, name text, email text, attrs jsonb
) SERVER my_api OPTIONS (endpoint '/users');

-- Query the API
SELECT * FROM openapi.users WHERE id = '123';
```

## Examples

The [`examples/`](examples/) directory has complete working setups for 5 real APIs:

| Example | API | Auth | Key Features |
| --- | --- | --- | --- |
| [nws](examples/nws/) | National Weather Service | None | GeoJSON, cursor pagination, path params, IMPORT FOREIGN SCHEMA |
| [pokeapi](examples/pokeapi/) | PokéAPI | None | Offset-based pagination, auto-detected `results` wrapper |
| [carapi](examples/carapi/) | CarAPI | None | Page-based pagination, query pushdown, `data` wrapper |
| [github](examples/github/) | GitHub REST API | Bearer token | Custom headers, search pushdown, `items` wrapper |
| [threads](examples/threads/) | Meta Threads API | OAuth (query param) | Cursor pagination, inline `spec_json` |

Each includes a `README.md` walkthrough and an `init.sql` you can run directly.

## Development

### Building

```bash
cargo component build --release --target wasm32-unknown-unknown
```

### Running Tests

```bash
# 518 unit tests
cargo test

# Integration tests (Docker: PostgreSQL + MockServer)
bash test/run.sh

# End-to-end benchmarks (Docker, vs pg_http)
bash test/benchmark.sh

# Example validation (Docker: runs all 5 examples)
bash test/run-examples.sh
```

### Code Quality

```bash
make check   # fmt, clippy, test, build
```

## Performance

Using any FDW comes with certain tradeoffs, and this one is no exception when it comes to performance. Wasm FDWs in Supabase Wrappers have a relatively fixed ~170-180ms overhead because the Supabase Wrappers WASM runtime needs to start up, compile the module, and initialize the component before any work begins. Once the runtime is up, processing each row is fast (~1-2us), but the overhead is the same whether you're fetching 1 row or 1,000.

The best alternative is something like `pg_http`, a native C extension that makes a raw HTTP call and hands you back JSON to parse yourself. Native is always going to be faster than WASM, but part of why it's faster is because it does less for you. Here's what it looks like to sync API data into a local table with each approach.

With the FDW:

```sql
INSERT INTO local_stations SELECT * FROM api.stations;
```

With `pg_http`:

```sql
INSERT INTO local_stations
SELECT props->>'station_id', props->>'name',
       props->>'state', (props->>'elevation')::bigint
FROM (SELECT content FROM http_get('.../features')) r,
     jsonb_array_elements((r.content::jsonb)->'features') AS f,
     LATERAL (SELECT f->'properties' AS props) AS sub;
```

And that's just one page of results from one endpoint. With `pg_http`, you'd also need to handle pagination loops, rate-limit retries, and response envelope detection yourself. These are all things the OpenAPI FDW handles automatically.

Here's the raw cost of that convenience. Both approaches benchmarked end-to-end (API call through to local table write) against a local mock server on PostgreSQL 15:

| Scenario | OpenAPI FDW | pg_http | Overhead |
| --- | --- | --- | --- |
| Simple Array (3 rows) | 188ms | 13ms | +175ms |
| Wrapped Response (2 rows) | 191ms | 8ms | +183ms |
| Type Coercion (1 row) | 188ms | 7ms | +181ms |
| GeoJSON Nested (3 rows) | 192ms | 7ms | +185ms |
| POST-for-Read (1 row) | 195ms | 13ms | +182ms |

These were measured with near-zero network latency, so they isolate the WASM overhead. With a real API responding in 100-400ms, the gap narrows. If your API call has 200ms of latency, the roundtrip takes ~210ms through `pg_http` and ~375ms through the FDW. The overhead doesn't disappear, but it becomes a lot more reasonable when you factor in the SQL you're not writing or maintaining.

For queries you run frequently, a [materialized view](https://supabase.com/blog/postgresql-views) can cache the results locally and skip the FDW on subsequent reads.

## Changelog

| Version | Date | Notes |
| --- | --- | --- |
| 0.2.0 | 2026-02-15 | Modular architecture, POST-for-read, `spec_json` inline specs, YAML spec support, LIMIT pushdown, OpenAPI 3.1 support, security hardening, 531 unit tests, 5 real-world examples |
| 0.1.4 | 2026-02-09 | Type coercion, auth validation, table naming, URL fixes |
| 0.1.3 | 2026-02-06 | Avoid cloning JSON response data |
| 0.1.2 | 2026-02-01 | Fix query param filtering |
| 0.1.1 | 2026-01-26 | URL encoding, identifier quoting, version validation |
| 0.1.0 | 2026-01-25 | Initial version |
