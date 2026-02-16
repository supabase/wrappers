# NWS Weather API Example

Query the [National Weather Service API](https://www.weather.gov/documentation/services-web-api) using SQL. This example exercises all major features of the OpenAPI FDW against a real, free, no-auth API.

## Server Configuration

```sql
create server nws
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://api.weather.gov',
    user_agent 'openapi-fdw-example/0.2.0',
    accept 'application/geo+json'
  );
```

---

## 1. Weather Stations

Fetches the full list of US weather stations. Demonstrates **GeoJSON extraction** (`response_path` + `object_path`), **cursor-based pagination** (`cursor_path`), and **camelCase-to-snake_case** column matching (`stationIdentifier` → `station_identifier`).

```sql
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
```

```sql
SELECT station_identifier, name, time_zone
FROM stations
LIMIT 5;
```

| station_identifier | name | time_zone |
| --- | --- | --- |
| 0007W | Montford Middle | America/New_York |
| 000PG | Southside Road | America/Los_Angeles |
| 000SE | SCE South Hills Park | America/Los_Angeles |
| 001AS | Poloa_Wx | Pacific/Pago_Pago |
| 001BH | Tilford | America/Denver |

The `stations` table paginates automatically — the FDW follows `/pagination/next` cursors. Try fetching more:

```sql
SELECT count(*) FROM stations;
```

The `elevation` column is `jsonb` because the API returns a structured object with value and unit:

```sql
SELECT station_identifier, name, elevation
FROM stations
LIMIT 3;
```

| station_identifier | name | elevation |
| --- | --- | --- |
| 0007W | Montford Middle | `{"value": 49.0728, "unitCode": "wmoUnit:m"}` |
| 000PG | Southside Road | `{"value": 129.2352, "unitCode": "wmoUnit:m"}` |
| 000SE | SCE South Hills Park | `{"value": 242.9256, "unitCode": "wmoUnit:m"}` |

## 2. Active Alerts

Different GeoJSON shape with **timestamptz coercion** for `onset` and `expires` columns.

```sql
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
```

```sql
SELECT event, severity, headline, onset, expires
FROM active_alerts
LIMIT 5;
```

| event | severity | headline | onset | expires |
| --- | --- | --- | --- | --- |
| Flash Flood Warning | Severe | Flash Flood Warning issued February 13 at 10:07PM CST… | 2026-02-14 04:07:00+00 | 2026-02-14 05:30:00+00 |
| Small Craft Advisory | Minor | Small Craft Advisory issued February 13 at 11:03PM EST… | 2026-02-15 06:00:00+00 | 2026-02-14 18:15:00+00 |

Full alert details with area, certainty, and timing:

```sql
SELECT id, event, severity, certainty, area_desc,
       headline, onset, expires
FROM active_alerts
LIMIT 5;
```

Filter in SQL after fetching:

```sql
SELECT event, severity, headline
FROM active_alerts
WHERE severity IN ('Severe', 'Extreme')
LIMIT 10;
```

## 3. Query Param Pushdown (severity filter)

When a WHERE clause references a column that isn't a path parameter, the FDW sends it as a **query parameter** to the API. The NWS alerts endpoint supports a `severity` filter — and because it echoes `severity` back in every response object, the column is populated naturally:

```sql
-- Pushes down to: GET /alerts/active?severity=Severe
SELECT event, severity, headline
FROM active_alerts
WHERE severity = 'Severe'
LIMIT 3;
```

| event | severity | headline |
| --- | --- | --- |
| Flash Flood Warning | Severe | Flash Flood Warning issued February 13 at 10:07PM CST… |
| Severe Thunderstorm Warning | Severe | Severe Thunderstorm Warning issued February 13 at 10:02PM CST… |
| Winter Storm Watch | Severe | Winter Storm Watch issued February 13 at 7:52PM PST… |

Try other severity values: `Extreme`, `Moderate`, `Minor`, `Unknown`.

## 4. Station Observations

**Path parameter substitution**: the `{station_id}` placeholder in the endpoint is replaced with the value from your WHERE clause.

```sql
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
```

```sql
-- Pushes down to: GET /stations/KDEN/observations
SELECT timestamp, text_description, temperature
FROM station_observations
WHERE station_id = 'KDEN'
LIMIT 3;
```

| timestamp | text_description | temperature |
| --- | --- | --- |
| 2026-02-14 03:45:00+00 | Cloudy | `{"value": 7, "unitCode": "wmoUnit:degC", "qualityControl": "V"}` |
| 2026-02-14 03:40:00+00 | Cloudy | `{"value": 7, "unitCode": "wmoUnit:degC", "qualityControl": "V"}` |
| 2026-02-14 03:35:00+00 | Cloudy | `{"value": 8, "unitCode": "wmoUnit:degC", "qualityControl": "V"}` |

`KDEN` is Denver International Airport. Try other station IDs: `KJFK` (New York), `KLAX` (Los Angeles), `KORD` (Chicago).

Temperature and wind values are `jsonb` because the NWS returns them as objects with unit and value:

```sql
SELECT timestamp,
       temperature->>'value' AS temp_c,
       wind_speed->>'value' AS wind_mps,
       text_description
FROM station_observations
WHERE station_id = 'KDEN'
LIMIT 3;
```

| timestamp | temp_c | wind_mps | text_description |
| --- | --- | --- | --- |
| 2026-02-14 03:45:00+00 | 7 | 24.084 | Cloudy |
| 2026-02-14 03:40:00+00 | 7 | 25.92 | Cloudy |
| 2026-02-14 03:35:00+00 | 8 | 25.92 | Cloudy |

## 5. Current Conditions

**Single object response** — the `/observations/latest` endpoint returns one GeoJSON Feature (not a FeatureCollection). The FDW auto-detects this and returns a single row.

```sql
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
```

```sql
SELECT text_description,
       temperature->>'value' AS temp_c,
       wind_speed->>'value' AS wind_mps,
       wind_direction->>'value' AS wind_deg,
       barometric_pressure->>'value' AS pressure_pa,
       relative_humidity->>'value' AS humidity_pct
FROM latest_observation
WHERE station_id = 'KDEN';
```

| text_description | temp_c | wind_mps | wind_deg | pressure_pa | humidity_pct |
| --- | --- | --- | --- | --- | --- |
| Cloudy | 7 | 24.084 | 310 | | 65.63 |

## 6. Point Metadata & Forecast

This two-step flow demonstrates **composite path parameters** and **nested response extraction**.

```sql
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
```

**Step 1:** Look up grid coordinates for a location (Denver: 39.7456,-104.9887):

```sql
SELECT grid_id, grid_x, grid_y, forecast
FROM point_metadata
WHERE point = '39.7456,-104.9887';
```

| grid_id | grid_x | grid_y | forecast |
| --- | --- | --- | --- |
| BOU | 63 | 62 | <https://api.weather.gov/gridpoints/BOU/63,62/forecast> |

The point metadata includes more detail than just the grid coordinates:

```sql
SELECT grid_id, grid_x, grid_y,
       forecast, forecast_hourly,
       relative_location->>'city' AS city,
       relative_location->>'state' AS state
FROM point_metadata
WHERE point = '39.7456,-104.9887';
```

**Step 2:** Use those grid coordinates to fetch the forecast. This exercises **multiple path parameters** (`wfo`, `x`, `y`) and **nested `response_path`** (`/properties/periods` digs two levels into the response):

```sql
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
```

```sql
-- Replace wfo/x/y with values from Step 1
SELECT name, temperature, temperature_unit,
       is_daytime, wind_speed, short_forecast
FROM forecast_periods
WHERE wfo = 'BOU' AND x = '63' AND y = '62';
```

| name | temperature | temperature_unit | is_daytime | wind_speed | short_forecast |
| --- | --- | --- | --- | --- | --- |
| Tonight | 35 | F | false | 3 to 7 mph | Rain Showers Likely |
| Saturday | 57 | F | true | 6 mph | Sunny |
| Saturday Night | 31 | F | false | 5 mph | Mostly Clear |
| Sunday | 66 | F | true | 6 mph | Mostly Sunny |

Full forecast with timing, wind, and detailed text:

```sql
SELECT number, name, start_time, end_time,
       is_daytime, temperature, temperature_unit,
       wind_speed, wind_direction,
       short_forecast, detailed_forecast
FROM forecast_periods
WHERE wfo = 'BOU' AND x = '63' AND y = '62';
```

> Grid coordinates vary by location. Always use Step 1 to find the right values for your area.

## 7. IMPORT FOREIGN SCHEMA

Auto-generate table definitions from the NWS OpenAPI spec. The `nws_import` server has a `spec_url` configured.

```sql
CREATE SCHEMA IF NOT EXISTS nws_auto;

IMPORT FOREIGN SCHEMA "unused"
FROM SERVER nws_import
INTO nws_auto;
```

See what was generated:

```sql
SELECT foreign_table_name FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'nws_auto';
```

Pick a generated table and query it:

```sql
SELECT * FROM nws_auto.alerts LIMIT 3;
```

## 8. Debug Mode

The `stations_debug` table uses the `nws_debug` server which has `debug 'true'`. This emits HTTP request details (method, URL, status, response size) and scan statistics (row/column counts) as PostgreSQL INFO messages.

```sql
SELECT station_identifier, name
FROM stations_debug
LIMIT 5;
```

Look for INFO output like:

```log
INFO:  [openapi_fdw] HTTP GET https://api.weather.gov/stations?limit=50 -> 200 (51639 bytes)
INFO:  [openapi_fdw] Scan complete: 5 rows, 2 columns
```

## 9. The `attrs` Column

Every table includes an `attrs jsonb` column that captures **all fields not mapped to named columns**. This is useful for exploring what data the API returns without defining every column.

```sql
SELECT station_identifier, attrs->>'county' AS county
FROM stations
LIMIT 5;
```

| station_identifier | county |
| --- | --- |
| 0007W | <https://api.weather.gov/zones/county/FLC073> |
| 000PG | <https://api.weather.gov/zones/county/CAC069> |
| 000SE | <https://api.weather.gov/zones/county/CAC037> |
| 001AS | <https://api.weather.gov/zones/county/ASC050> |
| 001BH | <https://api.weather.gov/zones/county/SDC093> |

## Features Demonstrated

| Feature | Table(s) |
| --- | --- |
| GeoJSON extraction (`response_path` + `object_path`) | `stations`, `active_alerts`, `station_observations` |
| Cursor-based pagination (`cursor_path`) | `stations` |
| Path parameter substitution | `station_observations`, `latest_observation`, `point_metadata`, `forecast_periods` |
| Query parameter pushdown | `active_alerts` (with `WHERE severity = ...`) |
| camelCase → snake_case matching | All tables |
| Custom headers (`user_agent`, `accept`) | All servers |
| LIMIT pushdown | Any table with `LIMIT` |
| Debug mode (`debug`) | `stations_debug` |
| IMPORT FOREIGN SCHEMA | `nws_import` server |
| Single object response | `latest_observation`, `point_metadata` |
| Type coercion (timestamptz, jsonb, boolean, integer) | `active_alerts`, `forecast_periods` |
| `attrs` catch-all column | All tables |
| Multiple path parameters | `forecast_periods` |
| Nested response extraction (JSON pointer) | `forecast_periods` |
| `rowid_column` | `stations`, `active_alerts` |
