-- OpenAPI FDW example: Xquik X (Twitter) data API
-- Requires an Xquik API key. Replace 'placeholder' in the server definitions below
-- with your key, or see the README for how to get one.
-- See: https://docs.xquik.com
-- Note: fdw_package_url uses file:// for local Docker testing. In production, use the
-- GitHub release URL: https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.1/openapi_fdw.wasm

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

set search_path to public, extensions;

create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;

-- ============================================================
-- Server 1: xquik — Xquik API, key sent in the x-api-key header
-- ============================================================
create server xquik
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.1',
    base_url 'https://xquik.com/api/v1',
    api_key 'placeholder',
    api_key_header 'x-api-key',
    cursor_param 'cursor',
    max_pages '5'
  );

-- ============================================================
-- Server 2: xquik_debug — Same API with debug output
-- ============================================================
create server xquik_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.1',
    base_url 'https://xquik.com/api/v1',
    api_key 'placeholder',
    api_key_header 'x-api-key',
    cursor_param 'cursor',
    max_pages '5',
    debug 'true'
  );

-- ============================================================
-- Table 1: x_user — A profile by username (path parameter, single object)
-- ============================================================
create foreign table x_user (
  username text,
  id text,
  name text,
  description text,
  followers bigint,
  following bigint,
  verified boolean,
  location text,
  created_at text,
  attrs jsonb
)
  server xquik
  options (
    endpoint '/x/users/{username}',
    response_path ''
  );

-- ============================================================
-- Table 2: user_tweets — A profile's recent posts (path parameter, cursor pagination)
-- ============================================================
create foreign table user_tweets (
  account text,
  id text,
  text text,
  lang text,
  created_at text,
  like_count bigint,
  retweet_count bigint,
  reply_count bigint,
  view_count bigint,
  attrs jsonb
)
  server xquik
  options (
    endpoint '/x/users/{account}/tweets',
    response_path '/tweets',
    cursor_path '/next_cursor'
  );

-- ============================================================
-- Table 3: search_tweets — Post search (query pushdown, cursor pagination)
-- ============================================================
create foreign table search_tweets (
  q text,
  id text,
  text text,
  lang text,
  created_at text,
  like_count bigint,
  view_count bigint,
  author jsonb,
  attrs jsonb
)
  server xquik
  options (
    endpoint '/x/tweets/search',
    response_path '/tweets',
    cursor_path '/next_cursor'
  );

-- ============================================================
-- Table 4: user_followers — A profile's followers (path parameter, cursor pagination)
-- ============================================================
create foreign table user_followers (
  account text,
  id text,
  username text,
  name text,
  followers bigint,
  verified boolean,
  attrs jsonb
)
  server xquik
  options (
    endpoint '/x/users/{account}/followers',
    response_path '/users',
    cursor_path '/next_cursor'
  );

-- ============================================================
-- Table 5: trends — Trending topics (no parameters, response_path)
-- ============================================================
create foreign table trends (
  attrs jsonb
)
  server xquik
  options (
    endpoint '/trends',
    response_path '/trends'
  );

-- ============================================================
-- Table 6: search_tweets_debug — Post search with debug output
-- ============================================================
create foreign table search_tweets_debug (
  q text,
  id text,
  text text,
  attrs jsonb
)
  server xquik_debug
  options (
    endpoint '/x/tweets/search',
    response_path '/tweets',
    cursor_path '/next_cursor'
  );
