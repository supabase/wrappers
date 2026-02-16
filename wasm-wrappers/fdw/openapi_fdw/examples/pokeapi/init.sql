-- OpenAPI FDW example: PokéAPI
-- All queries hit the live PokéAPI (no auth required).
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

set search_path to public, extensions;

create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;

-- ============================================================
-- Server 1: pokeapi — Main PokéAPI server
-- ============================================================
create server pokeapi
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://pokeapi.co/api/v2',
    page_size '20',
    page_size_param 'limit'
  );

-- ============================================================
-- Server 2: pokeapi_debug — Same API with debug output
-- ============================================================
create server pokeapi_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://pokeapi.co/api/v2',
    page_size '20',
    page_size_param 'limit',
    debug 'true'
  );

-- ============================================================
-- Table 1: pokemon
-- Paginated list of all Pokémon (~1350 items)
-- Features: offset-based pagination, auto-detected `results`
--           wrapper key, LIMIT pushdown
-- ============================================================
create foreign table pokemon (
  name text,
  url text,
  attrs jsonb
)
  server pokeapi
  options (
    endpoint '/pokemon',
    rowid_column 'name'
  );

-- ============================================================
-- Table 2: pokemon_detail
-- Single Pokémon detail via path parameter
-- Features: path param substitution (WHERE name = 'pikachu'),
--           single object response, integer/boolean coercion,
--           jsonb for complex nested data (abilities, types, etc.)
-- ============================================================
create foreign table pokemon_detail (
  id integer,
  name text,
  height integer,
  weight integer,
  base_experience integer,
  is_default boolean,
  order_num integer,
  abilities jsonb,
  types jsonb,
  stats jsonb,
  moves jsonb,
  sprites jsonb,
  attrs jsonb
)
  server pokeapi
  options (
    endpoint '/pokemon/{name}',
    rowid_column 'id'
  );

-- ============================================================
-- Table 3: types
-- Paginated list of all Pokémon types (21 items)
-- Features: small paginated list, fits in a single page
-- ============================================================
create foreign table types (
  name text,
  url text,
  attrs jsonb
)
  server pokeapi
  options (
    endpoint '/type',
    rowid_column 'name'
  );

-- ============================================================
-- Table 4: type_detail
-- Single type detail via path parameter
-- Features: path param, damage relations as jsonb,
--           pokemon list per type
-- ============================================================
create foreign table type_detail (
  id integer,
  name text,
  damage_relations jsonb,
  pokemon jsonb,
  moves jsonb,
  attrs jsonb
)
  server pokeapi
  options (
    endpoint '/type/{name}',
    rowid_column 'id'
  );

-- ============================================================
-- Table 5: berries
-- Paginated list of all berries (64 items)
-- Features: offset-based pagination, auto-detected `results`
-- ============================================================
create foreign table berries (
  name text,
  url text,
  attrs jsonb
)
  server pokeapi
  options (
    endpoint '/berry',
    rowid_column 'name'
  );

-- ============================================================
-- Table 6: berry_detail
-- Single berry detail via path parameter
-- Features: path param, integer columns for growth/harvest data,
--           jsonb for firmness/flavors/natural_gift_type
-- ============================================================
create foreign table berry_detail (
  id integer,
  name text,
  growth_time integer,
  max_harvest integer,
  natural_gift_power integer,
  size integer,
  smoothness integer,
  soil_dryness integer,
  firmness jsonb,
  flavors jsonb,
  natural_gift_type jsonb,
  attrs jsonb
)
  server pokeapi
  options (
    endpoint '/berry/{name}',
    rowid_column 'id'
  );

-- ============================================================
-- Table 7: pokemon_debug
-- Same as pokemon but on the debug server
-- Features: debug output in INFO messages
-- ============================================================
create foreign table pokemon_debug (
  name text,
  url text,
  attrs jsonb
)
  server pokeapi_debug
  options (
    endpoint '/pokemon',
    rowid_column 'name'
  );
