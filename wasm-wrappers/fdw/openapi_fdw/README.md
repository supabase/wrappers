# OpenAPI WASM Foreign Data Wrapper

This is a WASM-based Foreign Data Wrapper (FDW) for integrating any OpenAPI 3.0+ compliant REST API into PostgreSQL through Supabase Wrappers.

Point this at an OpenAPI spec and query the API with SQL. The FDW parses the spec, figures out the endpoints and response schemas, and lets you `IMPORT FOREIGN SCHEMA` to generate tables automatically.

Handles pagination, rate limiting (429 backoff), path parameter substitution from WHERE clauses, POST-for-read endpoints, and stops fetching early when you use LIMIT.

## Documentation

[https://fdw.dev/catalog/openapi/](https://fdw.dev/catalog/openapi/)

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

-- Query the API
SELECT * FROM openapi.users WHERE id = '123';
```

## Development

### Building

```bash
cd wasm-wrappers/fdw/openapi_fdw
cargo component build --release --target wasm32-unknown-unknown
```

### Running Tests

```bash
# Unit tests (run with native target)
cargo test

# Benchmarks
cargo bench --bench fdw_benchmarks

# Integration tests (Docker-based)
bash test/run.sh

# Integration tests (from wrappers directory)
cd wrappers
cargo pgrx test --features "wasm_fdw pg16"
```

### Code Quality

```bash
make check   # runs fmt, clippy, test, build
```

## Limitations

- Read-only (no INSERT/UPDATE/DELETE support)
- Only GET endpoints are supported (POST-for-read is available via the `method` table option)
- Authentication limited to API key and Bearer token (No OAuth2 flow support yet - use pre-obtained tokens)
- Only OpenAPI 3.x specs are supported (Swagger 2.0 is rejected)

## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.2.0   | 2026-02-15 | Modular architecture, POST-for-read, `spec_json` inline specs, LIMIT pushdown, OpenAPI 3.1 support, security hardening, 337 unit tests, 5 real-world examples |
| 0.1.4   | 2026-02-09 | Type coercion, auth validation, table naming, URL fixes |
| 0.1.3   | 2026-02-06 | Avoid cloning JSON response data                     |
| 0.1.2   | 2026-02-01 | Fix query param filtering                            |
| 0.1.1   | 2026-01-26 | URL encoding, identifier quoting, version validation |
| 0.1.0   | 2026-01-25 | Initial version                                      |
