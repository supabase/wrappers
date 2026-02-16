-- OpenAPI FDW example: National Weather Service API
-- All queries hit the live NWS API (no auth required).
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
-- Server 1: nws — Main NWS API server
-- ============================================================
create server nws
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://api.weather.gov',
    user_agent 'openapi-fdw-example/0.2.0',
    accept 'application/geo+json'
  );

-- ============================================================
-- Server 2: nws_debug — Same API with debug output
-- ============================================================
create server nws_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://api.weather.gov',
    user_agent 'openapi-fdw-example/0.2.0',
    accept 'application/geo+json',
    debug 'true'
  );

-- ============================================================
-- Server 3: nws_import — With spec_url for IMPORT FOREIGN SCHEMA
-- ============================================================
create server nws_import
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://api.weather.gov',
    user_agent 'openapi-fdw-example/0.2.0',
    accept 'application/geo+json',
    spec_url 'https://api.weather.gov/openapi.json',
    page_size '50',
    page_size_param 'limit'
  );

-- ============================================================
-- Table 1: stations
-- GeoJSON FeatureCollection with cursor-based pagination
-- Features: response_path, object_path, cursor_path, page_size,
--           rowid_column, camelCase matching, attrs catch-all
-- ============================================================
create foreign table stations (
  station_identifier text,
  name text,
  time_zone text,
  elevation jsonb,
  attrs jsonb
)
  server nws
  options (
    endpoint '/stations',
    response_path '/features',
    object_path '/properties',
    rowid_column 'station_identifier',
    cursor_path '/pagination/next',
    page_size '50',
    page_size_param 'limit'
  );

-- ============================================================
-- Table 2: active_alerts
-- GeoJSON with different column shape, timestamptz coercion
-- Features: timestamp type coercion, severity/certainty columns
-- ============================================================
create foreign table active_alerts (
  id text,
  area_desc text,
  severity text,
  certainty text,
  event text,
  headline text,
  onset timestamptz,
  expires timestamptz,
  attrs jsonb
)
  server nws
  options (
    endpoint '/alerts/active',
    response_path '/features',
    object_path '/properties',
    rowid_column 'id'
  );

-- ============================================================
-- Table 3: station_observations
-- Path parameter substitution: /stations/{station_id}/observations
-- Features: path param, GeoJSON, observation data as jsonb
-- ============================================================
create foreign table station_observations (
  timestamp timestamptz,
  text_description text,
  temperature jsonb,
  wind_speed jsonb,
  wind_direction jsonb,
  station_id text,
  attrs jsonb
)
  server nws
  options (
    endpoint '/stations/{station_id}/observations',
    response_path '/features',
    object_path '/properties'
  );

-- ============================================================
-- Table 4: latest_observation
-- Single object response (GeoJSON Feature, not FeatureCollection)
-- Features: path param, single object, object_path extraction
-- ============================================================
create foreign table latest_observation (
  text_description text,
  temperature jsonb,
  wind_speed jsonb,
  wind_direction jsonb,
  barometric_pressure jsonb,
  relative_humidity jsonb,
  station_id text,
  attrs jsonb
)
  server nws
  options (
    endpoint '/stations/{station_id}/observations/latest',
    object_path '/properties'
  );

-- ============================================================
-- Table 5: point_metadata
-- Composite path parameter: lat,lon as a single value
-- Features: single object, grid coordinate lookup
-- ============================================================
create foreign table point_metadata (
  grid_id text,
  grid_x integer,
  grid_y integer,
  forecast text,
  forecast_hourly text,
  relative_location jsonb,
  point text,
  attrs jsonb
)
  server nws
  options (
    endpoint '/points/{point}',
    object_path '/properties'
  );

-- ============================================================
-- Table 6: forecast_periods
-- Multiple path params + nested response extraction
-- Features: 3 path params, response_path into nested array,
--           boolean coercion, integer temperature
-- ============================================================
create foreign table forecast_periods (
  number integer,
  name text,
  start_time timestamptz,
  end_time timestamptz,
  is_daytime boolean,
  temperature integer,
  temperature_unit text,
  wind_speed text,
  wind_direction text,
  short_forecast text,
  detailed_forecast text,
  wfo text,
  x text,
  y text,
  attrs jsonb
)
  server nws
  options (
    endpoint '/gridpoints/{wfo}/{x},{y}/forecast',
    response_path '/properties/periods'
  );

-- ============================================================
-- Table 7: stations_debug
-- Same as stations but on the debug server
-- Features: debug output in INFO messages
-- ============================================================
create foreign table stations_debug (
  station_identifier text,
  name text,
  time_zone text,
  elevation jsonb,
  attrs jsonb
)
  server nws_debug
  options (
    endpoint '/stations',
    response_path '/features',
    object_path '/properties',
    rowid_column 'station_identifier',
    cursor_path '/pagination/next',
    page_size '50',
    page_size_param 'limit'
  );
