-- OpenAPI FDW integration test setup
-- This runs automatically on container startup.
-- Note: fdw_package_url uses file:// for local Docker testing. In production, use the
-- GitHub release URL: https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm

-- Create supabase_admin role if it doesn't exist (required by wrappers extension)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'supabase_admin') THEN
    CREATE ROLE supabase_admin WITH SUPERUSER CREATEDB CREATEROLE LOGIN PASSWORD 'postgres';
  END IF;
END
$$;

create schema if not exists extensions;
create extension if not exists wrappers with schema extensions;

-- wasm_fdw functions live in the extensions schema
set search_path to public, extensions;

create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;

-- NWS API server (no auth required, just needs User-Agent)
create server nws_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://api.weather.gov',
    user_agent 'openapi-fdw-test/0.2.0',
    accept 'application/geo+json'
  );

-- NWS API server WITH debug enabled for comparison
create server nws_server_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://api.weather.gov',
    user_agent 'openapi-fdw-test/0.2.0',
    accept 'application/geo+json',
    debug 'true'
  );

-- Weather stations (GeoJSON FeatureCollection → /features, nested /properties)
create foreign table nws_stations (
  station_identifier text,
  name text,
  time_zone text,
  elevation jsonb,
  attrs jsonb
)
  server nws_server
  options (
    endpoint '/stations',
    response_path '/features',
    object_path '/properties',
    rowid_column 'station_identifier',
    cursor_path '/pagination/next',
    page_size '100',
    page_size_param 'limit'
  );

-- Same table with debug enabled
create foreign table nws_stations_debug (
  station_identifier text,
  name text,
  time_zone text,
  elevation jsonb,
  attrs jsonb
)
  server nws_server_debug
  options (
    endpoint '/stations',
    response_path '/features',
    object_path '/properties',
    rowid_column 'station_identifier',
    cursor_path '/pagination/next',
    page_size '100',
    page_size_param 'limit'
  );

-- Active weather alerts
create foreign table nws_alerts (
  id text,
  area_desc text,
  severity text,
  certainty text,
  event text,
  headline text,
  description text,
  onset text,
  expires text,
  attrs jsonb
)
  server nws_server
  options (
    endpoint '/alerts/active',
    response_path '/features',
    object_path '/properties',
    rowid_column 'id'
  );

-- Stations filtered by state (query parameter pushdown)
create foreign table nws_stations_by_state (
  station_identifier text,
  name text,
  state text,
  time_zone text,
  attrs jsonb
)
  server nws_server_debug
  options (
    endpoint '/stations',
    response_path '/features',
    object_path '/properties',
    rowid_column 'station_identifier',
    page_size '50',
    page_size_param 'limit'
  );

-- ============================================================
-- Mock API server (deterministic local testing via MockServer)
-- ============================================================

create server mock_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    page_size '10',
    page_size_param 'limit'
  );

create server mock_server_paginated
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    page_size '2',
    page_size_param 'limit',
    cursor_param 'after'
  );

-- Test 1: Basic JSON array response
create foreign table mock_items (
  id bigint,
  name text,
  price double precision,
  in_stock boolean,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/items',
    rowid_column 'id'
  );

-- Test 2: Wrapped response (auto-detect "data" key)
create foreign table mock_products (
  id text,
  name text,
  price double precision,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/products',
    rowid_column 'id'
  );

-- Test 3: GeoJSON-style nested objects
create foreign table mock_stations (
  station_id text,
  name text,
  state text,
  elevation bigint,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/features',
    response_path '/features',
    object_path '/properties',
    rowid_column 'station_id'
  );

-- Test 4: Cursor pagination (token-based)
create foreign table mock_paginated_items (
  id bigint,
  value text,
  attrs jsonb
)
  server mock_server_paginated
  options (
    endpoint '/paginated-items',
    rowid_column 'id',
    cursor_path '/cursor'
  );

-- Test 5: Cursor pagination (full URL — tests the bug fix)
create foreign table mock_url_paginated (
  id bigint,
  label text,
  attrs jsonb
)
  server mock_server_paginated
  options (
    endpoint '/url-paginated',
    rowid_column 'id',
    cursor_path '/pagination/next',
    response_path '/items'
  );

-- Test 6: Query parameter pushdown
create foreign table mock_search (
  id bigint,
  name text,
  category text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/search',
    rowid_column 'id'
  );

-- Test 7: Path parameter substitution
create foreign table mock_user_posts (
  id bigint,
  user_id text,
  title text,
  body text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/users/{user_id}/posts',
    rowid_column 'id'
  );

-- Test 8: camelCase column matching
create foreign table mock_camel (
  id bigint,
  first_name text,
  last_name text,
  email_address text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/camel-case',
    rowid_column 'id'
  );

-- Test 9: 404 returns empty (not error)
create foreign table mock_not_found (
  id text,
  name text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/not-found',
    rowid_column 'id'
  );

-- Test 10: Rate limiting with 429 retry
create foreign table mock_rate_limited (
  id bigint,
  status text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/rate-limited',
    rowid_column 'id'
  );

-- ============================================================
-- Auth servers (each auth mode needs its own server)
-- ============================================================

-- Auth: API key with default Authorization: Bearer header
create server mock_auth_bearer_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    api_key 'test-api-key-123'
  );

-- Auth: API key with custom header (X-API-Key)
create server mock_auth_custom_header_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    api_key 'custom-key-456',
    api_key_header 'X-API-Key'
  );

-- Auth: API key with custom prefix (Token)
create server mock_auth_prefix_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    api_key 'prefix-key-789',
    api_key_prefix 'Token'
  );

-- Auth: Bearer token (separate from api_key)
create server mock_auth_token_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    bearer_token 'bearer-token-abc'
  );

-- Auth: API key in query parameter (Fix 6)
create server mock_auth_query_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    api_key 'query-key-123',
    api_key_header 'api_key',
    api_key_location 'query'
  );

-- Auth: API key in cookie (Fix 13)
create server mock_auth_cookie_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    api_key 'cookie-key-abc',
    api_key_header 'session',
    api_key_location 'cookie'
  );

-- Auth: Custom headers via JSON
create server mock_auth_headers_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    headers '{"X-Custom-One":"value1","X-Custom-Two":"value2"}'
  );

-- Auth test tables (one per auth server)
create foreign table mock_auth_bearer_data (
  id bigint,
  status text,
  attrs jsonb
)
  server mock_auth_bearer_server
  options (
    endpoint '/auth/bearer',
    rowid_column 'id'
  );

create foreign table mock_auth_custom_data (
  id bigint,
  status text,
  attrs jsonb
)
  server mock_auth_custom_header_server
  options (
    endpoint '/auth/custom-header',
    rowid_column 'id'
  );

create foreign table mock_auth_prefix_data (
  id bigint,
  status text,
  attrs jsonb
)
  server mock_auth_prefix_server
  options (
    endpoint '/auth/custom-prefix',
    rowid_column 'id'
  );

create foreign table mock_auth_token_data (
  id bigint,
  status text,
  attrs jsonb
)
  server mock_auth_token_server
  options (
    endpoint '/auth/token',
    rowid_column 'id'
  );

create foreign table mock_auth_query_data (
  id bigint,
  status text,
  attrs jsonb
)
  server mock_auth_query_server
  options (
    endpoint '/auth/query-key',
    rowid_column 'id'
  );

create foreign table mock_auth_cookie_data (
  id bigint,
  status text,
  attrs jsonb
)
  server mock_auth_cookie_server
  options (
    endpoint '/auth/cookie',
    rowid_column 'id'
  );

create foreign table mock_auth_headers_data (
  id bigint,
  status text,
  attrs jsonb
)
  server mock_auth_headers_server
  options (
    endpoint '/auth/headers-json',
    rowid_column 'id'
  );

-- ============================================================
-- Type coercion and data format tests
-- ============================================================

-- Type coercion: all supported PostgreSQL types
create foreign table mock_typed_data (
  id bigint,
  name text,
  uuid_field text,
  score real,
  rating double precision,
  count integer,
  active boolean,
  created_date date,
  updated_at timestamptz,
  tags jsonb,
  metadata jsonb,
  nullable_field text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/typed-data',
    rowid_column 'id'
  );

-- Single object response (not wrapped in array)
create foreign table mock_singleton (
  id text,
  name text,
  version integer,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/singleton',
    rowid_column 'id'
  );

-- Empty array response
create foreign table mock_empty (
  id text,
  name text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/empty-results',
    rowid_column 'id'
  );

-- Records wrapper auto-detection
create foreign table mock_records (
  id bigint,
  value text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/records-wrapped',
    rowid_column 'id'
  );

-- Entries wrapper auto-detection
create foreign table mock_entries (
  id bigint,
  label text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/entries-wrapped',
    rowid_column 'id'
  );

-- Results wrapper auto-detection
create foreign table mock_results (
  id bigint,
  value text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/results-wrapped',
    rowid_column 'id'
  );

-- ============================================================
-- Column matching tests
-- ============================================================

-- PascalCase → case-insensitive matching
create foreign table mock_pascal (
  id bigint,
  name text,
  age integer,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/pascal-case',
    rowid_column 'id'
  );

-- ============================================================
-- Pagination edge case tests
-- ============================================================

-- has_more pattern (auto-detection, no explicit cursor_path)
create foreign table mock_has_more (
  id bigint,
  value text,
  attrs jsonb
)
  server mock_server_paginated
  options (
    endpoint '/has-more-items',
    rowid_column 'id'
  );

-- Relative URL pagination (query-only: "?page=2")
create foreign table mock_relative_paged (
  id bigint,
  label text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/relative-paged',
    rowid_column 'id'
  );

-- ============================================================
-- URL construction tests
-- ============================================================

-- Multiple path parameters
create foreign table mock_multi_path (
  id bigint,
  title text,
  state text,
  org text,
  repo text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/orgs/{org}/repos/{repo}/issues',
    rowid_column 'id'
  );

-- Rowid pushdown (WHERE id = 'x' → GET /resources/x)
create foreign table mock_resources (
  id text,
  name text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/resources',
    rowid_column 'id'
  );

-- ============================================================
-- Error handling tests
-- ============================================================

-- HTTP 500 error
create foreign table mock_server_error (
  id text,
  name text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/server-error',
    rowid_column 'id'
  );

-- Invalid JSON response
create foreign table mock_invalid_json (
  id text,
  name text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/invalid-json',
    rowid_column 'id'
  );

-- ============================================================
-- Edge case tests: unix timestamps, acronyms, mixed types
-- ============================================================

-- Unix timestamps (Stripe-style epoch seconds in timestamptz columns)
create foreign table mock_unix_timestamps (
  id bigint,
  name text,
  created_at timestamptz,
  updated_at timestamptz,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/unix-timestamps',
    rowid_column 'id'
  );

-- Acronym field names (clusterIP, apiURL, htmlParser)
create foreign table mock_acronym_fields (
  id bigint,
  cluster_ip text,
  api_url text,
  html_parser text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/acronym-fields',
    rowid_column 'id'
  );

-- Mixed types with nulls (testing null handling across types)
create foreign table mock_mixed_types (
  id bigint,
  count integer,
  enabled boolean,
  ratio double precision,
  name text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/mixed-types',
    rowid_column 'id'
  );

-- ============================================================
-- JSON-LD tests (@-prefixed keys, @graph wrapper)
-- ============================================================

-- JSON-LD @graph wrapper (NWS alerts with application/ld+json)
create foreign table mock_jsonld_alerts (
  _id text,
  _type text,
  headline text,
  severity text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/jsonld-graph',
    rowid_column '_id'
  );

-- JSON-LD GeoJSON with @-prefixed properties in nested objects
create foreign table mock_jsonld_stations (
  _id text,
  _type text,
  station_identifier text,
  name text,
  time_zone text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/jsonld-features',
    response_path '/features',
    object_path '/properties',
    rowid_column 'station_identifier'
  );

-- ============================================================
-- POST-for-read test
-- ============================================================

-- POST method for search (data retrieval via POST)
create foreign table mock_search_post (
  id bigint,
  label text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/search',
    method 'POST',
    rowid_column 'id'
  );

-- ============================================================
-- Bug regression tests: sticky config, re_scan
-- ============================================================

-- Bug 2: Table with custom page_size override (server default is 10)
-- Sends limit=5 — MockServer expectation requires exactly limit=5
create foreign table mock_config_custom_page (
  id bigint,
  value text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/config-test-custom',
    rowid_column 'id',
    page_size '5'
  );

-- Bug 2: Table WITHOUT page_size override (should use server default 10)
-- Sends limit=10 — MockServer expectation requires exactly limit=10
-- If sticky bug exists: would send limit=5 (leaked from previous scan) → no match → error
create foreign table mock_config_default_page (
  id bigint,
  value text,
  attrs jsonb
)
  server mock_server
  options (
    endpoint '/config-test-default',
    rowid_column 'id'
  );

-- ============================================================
-- OpenAPI spec-driven server (comprehensive custom spec)
-- Tables are created dynamically via IMPORT FOREIGN SCHEMA in run.sh
-- ============================================================

create server mock_openapi_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'http://mockserver:1080',
    spec_url 'http://mockserver:1080/openapi.json',
    page_size '10',
    page_size_param 'limit'
  );
