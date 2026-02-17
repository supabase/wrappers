-- OpenAPI FDW example: CarAPI (Vehicle Data)
-- Free demo dataset (2015-2020 vehicles), no auth required.
-- See: https://carapi.app/api
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
-- Server 1: carapi — Main CarAPI server (no auth, free demo)
-- Response format: {"collection": {...pagination...}, "data": [...]}
-- The FDW auto-detects the "data" wrapper key.
-- ============================================================
create server carapi
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://carapi.app/api'
  );

-- ============================================================
-- Server 2: carapi_debug — Same API with debug output
-- ============================================================
create server carapi_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://carapi.app/api',
    debug 'true'
  );

-- ============================================================
-- Server 3: carapi_import — With spec_url for IMPORT FOREIGN SCHEMA
-- ============================================================
create server carapi_import
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://carapi.app/api',
    spec_url 'https://carapi.app/swagger.json'
  );

-- ============================================================
-- Table 1: makes
-- All car manufacturers (paginated)
-- Features: auto-detected "data" wrapper, page-based pagination
-- ============================================================
create foreign table makes (
  id integer,
  name text,
  attrs jsonb
)
  server carapi
  options (
    endpoint '/makes/v2',
    rowid_column 'id'
  );

-- ============================================================
-- Table 2: models
-- Car models filtered by make and year
-- Features: query param pushdown (make, year), pagination
-- ============================================================
create foreign table models (
  id integer,
  make_id integer,
  year integer,
  make text,
  name text,
  attrs jsonb
)
  server carapi
  options (
    endpoint '/models/v2',
    rowid_column 'id'
  );

-- ============================================================
-- Table 3: trims
-- Trim levels with MSRP and descriptions
-- Features: query pushdown (year, make, model), pricing data,
--           integer types, timestamptz coercion
-- ============================================================
create foreign table trims (
  id integer,
  make_id integer,
  model_id integer,
  year integer,
  make text,
  model text,
  submodel text,
  trim text,
  description text,
  msrp integer,
  invoice integer,
  created timestamptz,
  modified timestamptz,
  attrs jsonb
)
  server carapi
  options (
    endpoint '/trims/v2',
    rowid_column 'id'
  );

-- ============================================================
-- Table 4: bodies
-- Vehicle body dimensions and specs
-- Features: query pushdown, physical measurements (text for
--           decimal strings), integer for counts/weights
-- ============================================================
create foreign table bodies (
  id integer,
  year integer,
  make text,
  model text,
  submodel text,
  trim text,
  type text,
  doors integer,
  length text,
  width text,
  height text,
  wheel_base text,
  ground_clearance text,
  cargo_capacity text,
  curb_weight integer,
  seats integer,
  attrs jsonb
)
  server carapi
  options (
    endpoint '/bodies/v2',
    rowid_column 'id'
  );

-- ============================================================
-- Table 5: engines
-- Engine specifications and performance data
-- Features: query pushdown, horsepower/torque as integers,
--           engine type, fuel type, drive type, transmission
-- ============================================================
create foreign table engines (
  id integer,
  year integer,
  make text,
  model text,
  submodel text,
  trim text,
  engine_type text,
  fuel_type text,
  cylinders text,
  size text,
  horsepower_hp integer,
  horsepower_rpm integer,
  torque_ft_lbs integer,
  torque_rpm integer,
  valves integer,
  valve_timing text,
  cam_type text,
  drive_type text,
  transmission text,
  attrs jsonb
)
  server carapi
  options (
    endpoint '/engines/v2',
    rowid_column 'id'
  );

-- ============================================================
-- Table 6: mileages
-- Fuel economy and range data (MPG, tank capacity, EV stats)
-- Features: query pushdown, EPA mileage ratings, range data
-- ============================================================
create foreign table mileages (
  id integer,
  year integer,
  make text,
  model text,
  submodel text,
  trim text,
  fuel_tank_capacity text,
  combined_mpg integer,
  epa_city_mpg integer,
  epa_highway_mpg integer,
  range_city integer,
  range_highway integer,
  attrs jsonb
)
  server carapi
  options (
    endpoint '/mileages/v2',
    rowid_column 'id'
  );

-- ============================================================
-- Table 7: exterior_colors
-- Paint colors with RGB values
-- Features: query pushdown, color name + RGB string
-- ============================================================
create foreign table exterior_colors (
  id integer,
  year integer,
  make text,
  model text,
  submodel text,
  trim text,
  color text,
  rgb text,
  attrs jsonb
)
  server carapi
  options (
    endpoint '/exterior-colors/v2',
    rowid_column 'id'
  );

-- ============================================================
-- Table 8: obd_codes
-- OBD-II diagnostic trouble codes
-- Features: small dataset on free tier, code + description
-- ============================================================
create foreign table obd_codes (
  code text,
  description text,
  attrs jsonb
)
  server carapi
  options (
    endpoint '/obd-codes',
    rowid_column 'code'
  );

-- ============================================================
-- Table 9: makes_debug
-- Same as makes but on the debug server
-- Features: debug output in INFO messages
-- ============================================================
create foreign table makes_debug (
  id integer,
  name text,
  attrs jsonb
)
  server carapi_debug
  options (
    endpoint '/makes/v2',
    rowid_column 'id'
  );
