# CarAPI Example

Query the [CarAPI](https://carapi.app/) vehicle database using SQL. This example demonstrates the OpenAPI FDW against a free, no-auth API with **page-based pagination**, auto-detected `data` wrapper key, and **query parameter pushdown** for filtering by year, make, and model.

## Server Configuration

```sql
create server carapi
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://carapi.app/api'
  );
```

---

## 1. Quick Start with IMPORT FOREIGN SCHEMA

The `carapi_import` server has a `spec_url` pointing to the CarAPI OpenAPI spec, so tables can be auto-generated:

```sql
CREATE SCHEMA IF NOT EXISTS carapi_auto;

IMPORT FOREIGN SCHEMA "unused"
FROM SERVER carapi_import
INTO carapi_auto;
```

See what was generated:

```sql
SELECT foreign_table_name FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'carapi_auto';
```

Pick a generated table and query it:

```sql
SELECT * FROM carapi_auto.makes_v2 LIMIT 3;
```

The rest of this example uses manually defined tables to demonstrate specific features (query pushdown, type coercion, debug mode, etc.).

---

## 2. Makes

Fetches all car manufacturers. Demonstrates **page-based pagination** with auto-detected `data` wrapper key. The CarAPI wraps responses in `{"collection": {...}, "data": [...]}` and the FDW auto-detects the `data` key.

```sql
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
```

```sql
SELECT id, name
FROM makes
LIMIT 5;
```

| id | name |
| --- | --- |
| 1 | Acura |
| 24 | Alfa Romeo |
| 44 | Aston Martin |
| 2 | Audi |
| 25 | Bentley |

## 3. Models

Car models filtered by make and year. Demonstrates **query parameter pushdown** — the WHERE clause values are sent as query parameters to the API, so only matching data is returned.

```sql
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
```

```sql
SELECT id, name, make
FROM models
WHERE make = 'Toyota' AND year = '2020'
LIMIT 5;
```

| id | name | make |
| --- | --- | --- |
| 4841 | 4Runner | Toyota |
| 7245 | 86 | Toyota |
| 5689 | Avalon | Toyota |
| 7308 | C-HR | Toyota |
| 4779 | Camry | Toyota |

## 4. Trims

Trim levels with MSRP pricing. Combines query pushdown (year, make, model) with integer type coercion for pricing fields.

```sql
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
```

```sql
SELECT trim, msrp, description
FROM trims
WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry'
LIMIT 3;
```

| trim | msrp | description |
| --- | --- | --- |
| LE | 28430 | LE 4dr Sedan (2.5L 4cyl gas/electric hybrid CVT) |
| SE | 30130 | SE 4dr Sedan (2.5L 4cyl gas/electric hybrid CVT) |
| XLE | 32730 | XLE 4dr Sedan (2.5L 4cyl gas/electric hybrid CVT) |

Compare MSRP vs invoice price across trims:

```sql
SELECT trim, msrp, invoice, msrp - invoice AS dealer_margin
FROM trims
WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry';
```

See when trim data was last updated:

```sql
SELECT trim, msrp, created, modified
FROM trims
WHERE year = '2020' AND make = 'Honda' AND model = 'Civic'
LIMIT 3;
```

## 5. Bodies

Vehicle body dimensions. Demonstrates mixed types — integer for counts/weights, text for decimal measurements.

```sql
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
```

```sql
SELECT type, doors, length, curb_weight
FROM bodies
WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry'
LIMIT 3;
```

| type | doors | length | curb_weight |
| --- | --- | --- | --- |
| Sedan | 4 | 192.1 | 3472 |
| Sedan | 4 | 192.7 | 3549 |
| Sedan | 4 | 192.1 | 3572 |

Full dimension breakdown:

```sql
SELECT type, doors, seats, length, width, height,
       wheel_base, ground_clearance, cargo_capacity, curb_weight
FROM bodies
WHERE year = '2020' AND make = 'Toyota' AND model = 'RAV4'
LIMIT 3;
```

## 6. Engines

Engine specifications and performance data.

```sql
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
```

```sql
SELECT engine_type, horsepower_hp, cylinders, transmission
FROM engines
WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry'
LIMIT 3;
```

| engine_type | horsepower_hp | cylinders | transmission |
| --- | --- | --- | --- |
| hybrid | 208 | I4 | continuously variable-speed automatic |
| hybrid | 208 | I4 | continuously variable-speed automatic |
| hybrid | 208 | I4 | continuously variable-speed automatic |

Full engine specs with torque, valve config, and drive type:

```sql
SELECT engine_type, fuel_type, size, cylinders,
       horsepower_hp, horsepower_rpm,
       torque_ft_lbs, torque_rpm,
       valves, valve_timing, cam_type,
       drive_type, transmission
FROM engines
WHERE year = '2020' AND make = 'Ford' AND model = 'Mustang'
LIMIT 3;
```

## 7. Mileages

Fuel economy and range data (EPA ratings).

```sql
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
```

```sql
SELECT combined_mpg, epa_city_mpg, epa_highway_mpg, range_city
FROM mileages
WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry'
LIMIT 3;
```

| combined_mpg | epa_city_mpg | epa_highway_mpg | range_city |
| --- | --- | --- | --- |
| 52 | 51 | 53 | 673 |
| 46 | 44 | 47 | 581 |
| 46 | 44 | 47 | 581 |

Include fuel tank capacity and highway range:

```sql
SELECT trim, fuel_tank_capacity,
       combined_mpg, epa_city_mpg, epa_highway_mpg,
       range_city, range_highway
FROM mileages
WHERE year = '2020' AND make = 'Honda' AND model = 'Accord'
LIMIT 3;
```

## 8. Exterior Colors

Paint colors with RGB values.

```sql
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
```

```sql
SELECT color, rgb
FROM exterior_colors
WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry'
LIMIT 5;
```

| color | rgb |
| --- | --- |
| Blue Streak Metallic | 0,62,155 |
| Brownstone | 95,85,71 |
| Celestial Silver Metallic | 151,156,160 |
| Galactic Aqua Mica | 37,54,65 |
| Midnight Black Metallic | 23,23,23 |

## 9. OBD Codes

OBD-II diagnostic trouble codes. A small dataset available on the free tier.

```sql
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
```

```sql
SELECT code, description
FROM obd_codes
LIMIT 5;
```

| code | description |
| --- | --- |
| P0100 | Mass or Volume Air Flow Sensor A Circuit |
| U1000 | Manufacturer Controlled DTC |

## 10. Debug Mode

The `makes_debug` table uses the `carapi_debug` server which has `debug 'true'`. This emits HTTP request details and scan statistics as PostgreSQL INFO messages.

```sql
SELECT id FROM makes_debug LIMIT 1;
```

Look for INFO output like:

```log
INFO:  [openapi_fdw] HTTP GET https://carapi.app/api/makes/v2 -> 200 (1404 bytes)
INFO:  [openapi_fdw] Scan complete: 1 rows, 1 columns
```

## 11. The `attrs` Column

Every table includes an `attrs jsonb` column that captures **all fields not mapped to named columns**. This is useful for exploring what data the API returns without defining every column upfront.

```sql
SELECT name, attrs
FROM makes
LIMIT 1;
```

## Features Demonstrated

| Feature | Table(s) |
| --- | --- |
| IMPORT FOREIGN SCHEMA | `carapi_import` server |
| Page-based pagination (auto-followed) | `makes`, `models`, `trims`, `bodies`, `engines`, `mileages`, `exterior_colors` |
| Auto-detected `data` wrapper key | All tables |
| Query parameter pushdown | `models`, `trims`, `bodies`, `engines`, `mileages`, `exterior_colors` |
| Integer type coercion | `trims` (msrp), `bodies` (curb_weight), `engines` (horsepower), `mileages` (mpg) |
| `timestamptz` coercion | `trims` (created, modified) |
| LIMIT pushdown | Any table with `LIMIT` |
| Debug mode (`debug`) | `makes_debug` |
| `attrs` catch-all column | All tables |
| `rowid_column` | All tables |
| No authentication required | All servers |
