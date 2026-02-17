# PokéAPI Example

Query the [PokéAPI](https://pokeapi.co/) using SQL. This example demonstrates the OpenAPI FDW against a free, no-auth API with **offset-based pagination** and auto-detected `results` wrapper key.

## Server Configuration

```sql
create server pokeapi
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://pokeapi.co/api/v2',
    page_size '20',
    page_size_param 'limit'
  );
```

---

## 1. Quick Start with IMPORT FOREIGN SCHEMA

The `pokeapi_import` server has a `spec_url` pointing to the PokeAPI OpenAPI spec (YAML format, the FDW parses both JSON and YAML), so tables can be auto-generated:

```sql
create server pokeapi_import
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://pokeapi.co/api/v2',
    spec_url 'https://raw.githubusercontent.com/PokeAPI/pokeapi/master/openapi.yml',
    page_size '20',
    page_size_param 'limit'
  );
```

```sql
CREATE SCHEMA IF NOT EXISTS pokeapi_auto;

IMPORT FOREIGN SCHEMA "unused"
FROM SERVER pokeapi_import
INTO pokeapi_auto;
```

See what was generated:

```sql
SELECT foreign_table_name FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'pokeapi_auto';
```

Pick a generated table and query it:

```sql
SELECT * FROM pokeapi_auto.pokemon LIMIT 3;
```

The rest of this example uses manually defined tables to demonstrate specific features (path parameters, jsonb extraction, debug mode, etc.).

---

## 2. Pokemon List

Fetches the paginated list of all Pokemon (~1350 entries). Demonstrates **offset-based pagination** with auto-detected `results` wrapper key and `limit` page size parameter. The FDW automatically follows the `next` URL in each response to fetch subsequent pages.

```sql
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
```

```sql
SELECT name, url
FROM pokemon
LIMIT 5;
```

| name | url |
| --- | --- |
| bulbasaur | <https://pokeapi.co/api/v2/pokemon/1/> |
| ivysaur | <https://pokeapi.co/api/v2/pokemon/2/> |
| venusaur | <https://pokeapi.co/api/v2/pokemon/3/> |
| charmander | <https://pokeapi.co/api/v2/pokemon/4/> |
| charmeleon | <https://pokeapi.co/api/v2/pokemon/5/> |

List endpoints only return `name` and `url` pairs. Use the detail table to get full data for a specific Pokemon.

## 3. Pokemon Detail

**Path parameter substitution**: the `{name}` placeholder in the endpoint is replaced with the value from your WHERE clause. Returns a single object with full Pokemon data.

```sql
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
```

```sql
SELECT id, name, height, weight, base_experience, is_default
FROM pokemon_detail
WHERE name = 'pikachu';
```

| id | name | height | weight | base_experience | is_default |
| --- | --- | --- | --- | --- | --- |
| 25 | pikachu | 4 | 60 | 112 | t |

Complex nested data like abilities, types, and stats are returned as `jsonb`:

```sql
SELECT name, types, abilities
FROM pokemon_detail
WHERE name = 'charizard';
```

| name | types | abilities |
| --- | --- | --- |
| charizard | `[{"slot":1,"type":{"name":"fire","url":"..."}},{"slot":2,"type":{"name":"flying","url":"..."}}]` | `[{"slot":1,"ability":{"name":"blaze","url":"..."},"is_hidden":false},...]` |

Extract specific fields from the jsonb columns:

```sql
SELECT name,
       sprites->>'front_default' AS sprite_url
FROM pokemon_detail
WHERE name = 'eevee';
```

Extract base stats from the jsonb column:

```sql
SELECT name, height, weight,
       stats->0->>'base_stat' AS hp,
       stats->1->>'base_stat' AS attack,
       stats->2->>'base_stat' AS defense,
       stats->3->>'base_stat' AS sp_attack,
       stats->4->>'base_stat' AS sp_defense,
       stats->5->>'base_stat' AS speed
FROM pokemon_detail
WHERE name = 'pikachu';
```

| name | height | weight | hp | attack | defense | sp_attack | sp_defense | speed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pikachu | 4 | 60 | 35 | 55 | 40 | 50 | 50 | 90 |

Compare two Pokemon side by side:

```sql
SELECT name, height, weight, base_experience, is_default,
       types, abilities
FROM pokemon_detail
WHERE name IN ('charizard', 'blastoise');
```

Try other Pokemon: `bulbasaur`, `charizard`, `mewtwo`, `snorlax`, `gengar`.

## 4. Types List

Fetches all Pokemon types. With only 21 types, this fits within a single page (page size is 20, so it takes two small fetches).

```sql
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
```

```sql
SELECT name, url
FROM types;
```

| name | url |
| --- | --- |
| normal | <https://pokeapi.co/api/v2/type/1/> |
| fighting | <https://pokeapi.co/api/v2/type/2/> |
| flying | <https://pokeapi.co/api/v2/type/3/> |
| poison | <https://pokeapi.co/api/v2/type/4/> |
| ground | <https://pokeapi.co/api/v2/type/5/> |
| rock | <https://pokeapi.co/api/v2/type/6/> |
| bug | <https://pokeapi.co/api/v2/type/7/> |
| ghost | <https://pokeapi.co/api/v2/type/8/> |
| steel | <https://pokeapi.co/api/v2/type/9/> |
| fire | <https://pokeapi.co/api/v2/type/10/> |
| water | <https://pokeapi.co/api/v2/type/11/> |
| grass | <https://pokeapi.co/api/v2/type/12/> |
| electric | <https://pokeapi.co/api/v2/type/13/> |
| psychic | <https://pokeapi.co/api/v2/type/14/> |
| ice | <https://pokeapi.co/api/v2/type/15/> |
| dragon | <https://pokeapi.co/api/v2/type/16/> |
| dark | <https://pokeapi.co/api/v2/type/17/> |
| fairy | <https://pokeapi.co/api/v2/type/18/> |
| stellar | <https://pokeapi.co/api/v2/type/19/> |
| unknown | <https://pokeapi.co/api/v2/type/10001/> |
| shadow | <https://pokeapi.co/api/v2/type/10002/> |

## 5. Type Detail

Detailed information about a single type, including **damage relations** (strengths and weaknesses) and a list of all Pokemon of that type.

```sql
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
```

```sql
SELECT id, name, damage_relations
FROM type_detail
WHERE name = 'fire';
```

| id | name | damage_relations |
| --- | --- | --- |
| 10 | fire | `{"double_damage_to":[{"name":"grass","url":"..."},{"name":"ice","url":"..."},{"name":"bug","url":"..."},{"name":"steel","url":"..."}],"half_damage_from":[{"name":"fire","url":"..."},...],...}` |

The `damage_relations` jsonb column contains the full type effectiveness chart. Extract specific matchups:

```sql
SELECT name,
       damage_relations->'double_damage_to' AS super_effective_against
FROM type_detail
WHERE name = 'fire';
```

Get the list of all Pokemon for a given type:

```sql
SELECT name,
       damage_relations->'double_damage_to' AS super_effective,
       damage_relations->'half_damage_from' AS resists,
       pokemon,
       moves
FROM type_detail
WHERE name = 'dragon';
```

Try other types: `water`, `electric`, `dragon`, `fairy`, `ghost`.

## 6. Berries List

Fetches all berries (64 items). Demonstrates pagination across multiple pages.

```sql
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
```

```sql
SELECT name, url
FROM berries
LIMIT 5;
```

| name | url |
| --- | --- |
| cheri | <https://pokeapi.co/api/v2/berry/1/> |
| chesto | <https://pokeapi.co/api/v2/berry/2/> |
| pecha | <https://pokeapi.co/api/v2/berry/3/> |
| rawst | <https://pokeapi.co/api/v2/berry/4/> |
| aspear | <https://pokeapi.co/api/v2/berry/5/> |

## 7. Berry Detail

Detailed information about a single berry, including growth data, flavors, and natural gift properties.

```sql
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
```

```sql
SELECT id, name, growth_time, max_harvest, natural_gift_power,
       size, smoothness, soil_dryness
FROM berry_detail
WHERE name = 'cheri';
```

| id | name | growth_time | max_harvest | natural_gift_power | size | smoothness | soil_dryness |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | cheri | 3 | 5 | 60 | 20 | 25 | 15 |

Complex data like firmness, flavors, and natural gift type are returned as `jsonb`:

```sql
SELECT name,
       firmness->>'name' AS firmness,
       natural_gift_type->>'name' AS gift_type
FROM berry_detail
WHERE name = 'cheri';
```

| name | firmness | gift_type |
| --- | --- | --- |
| cheri | soft | fire |

Extract all flavor profiles from the jsonb column:

```sql
SELECT name, growth_time, max_harvest,
       firmness->>'name' AS firmness,
       natural_gift_type->>'name' AS gift_type,
       natural_gift_power,
       flavors
FROM berry_detail
WHERE name = 'sitrus';
```

Try other berries: `chesto`, `pecha`, `rawst`, `aspear`, `leppa`, `oran`, `sitrus`.

## 8. Debug Mode

The `pokemon_debug` table uses the `pokeapi_debug` server which has `debug 'true'`. This emits HTTP request details (method, URL, status, response size) and scan statistics as PostgreSQL INFO messages.

```sql
SELECT name, url
FROM pokemon_debug
LIMIT 3;
```

Look for INFO output like:

```log
INFO:  [openapi_fdw] HTTP GET https://pokeapi.co/api/v2/pokemon?limit=20 -> 200 (1416 bytes)
INFO:  [openapi_fdw] Scan complete: 3 rows, 1 columns
```

## 9. The `attrs` Column

Every table includes an `attrs jsonb` column that captures the full JSON response object for each row. This is useful for exploring what data the API returns without defining every column upfront.

For list endpoints, `attrs` will be mostly empty since the API only returns `name` and `url`. For detail endpoints, `attrs` captures the remaining fields:

```sql
SELECT name,
       attrs->>'location_area_encounters' AS encounters_url
FROM pokemon_detail
WHERE name = 'pikachu';
```

| name | encounters_url |
| --- | --- |
| pikachu | <https://pokeapi.co/api/v2/pokemon/25/encounters> |
