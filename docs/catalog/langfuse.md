---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Langfuse

[Langfuse](https://langfuse.com/) is an open source LLM observability platform which records traces, token usage, and cost for LLM applications.

The Langfuse Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read trace and observation data from Langfuse for use within your Postgres database.

It targets the Langfuse [Public API](https://langfuse.com/docs/api) rather than the ClickHouse store behind it, which upstream documents as [not a stable API contract](https://langfuse.com/self-hosting/infrastructure/clickhouse) and which Langfuse Cloud users cannot reach at all.

## Available Versions

| Version | Wasm Package URL                                                                                    | Checksum                                                           | Required Wrappers Version |
| ------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------- |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_langfuse_fdw_v0.1.0/langfuse_fdw.wasm` | `<to be filled in on release>`                                     | >=0.5.0                   |

## Preparation

Before you can query Langfuse, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Langfuse Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

Langfuse authenticates with a public/secret key pair, both found under project settings.

```sql
-- Save your Langfuse keys in Vault
select vault.create_secret(
  '<Langfuse public key>', -- pk-lf-...
  'langfuse_public_key',
  'Langfuse public key for Wrappers'
);

select vault.create_secret(
  '<Langfuse secret key>', -- sk-lf-...
  'langfuse_secret_key',
  'Langfuse secret key for Wrappers'
);
```

### Connecting to Langfuse

We need to provide Postgres with the credentials to access Langfuse and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server langfuse_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_langfuse_fdw_v0.1.0/langfuse_fdw.wasm',
        fdw_package_name 'supabase:langfuse-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '<checksum>',
        api_url 'https://cloud.langfuse.com',  -- optional
        public_key_name 'langfuse_public_key', -- the Vault secret name from above
        secret_key_name 'langfuse_secret_key'
      );
    ```

=== "Without Vault"

    ```sql
    create server langfuse_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_langfuse_fdw_v0.1.0/langfuse_fdw.wasm',
        fdw_package_name 'supabase:langfuse-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '<checksum>',
        api_url 'https://cloud.langfuse.com',  -- optional
        public_key '<Langfuse public key>',
        secret_key '<Langfuse secret key>'
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

!!! warning

    API keys are bound to the region the Langfuse project was created in. `api_url` must match it or every request returns 401. Use `https://cloud.langfuse.com` for EU, `https://us.cloud.langfuse.com` for US, `https://jp.cloud.langfuse.com` for Japan, or your own URL when self-hosting.

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists langfuse;
```

## Options

The full list of foreign table options are below:

- `object` - API path after `/api/public/`, required. For example `traces` or `observations`.
- `fields` - Field groups to request, optional. Applies to the `v2/` endpoints only.

The full list of server options are below:

- `api_url` - Langfuse API base URL, optional. Defaults to `https://cloud.langfuse.com`.
- `public_key_name` / `secret_key_name` - Vault secret names holding the keys.
- `public_key_id` / `secret_key_id` - Vault secret ids, as an alternative to the names.
- `public_key` / `secret_key` - Plaintext keys, for local development.
- `page_size` - Rows to request per upstream call, optional. Defaults to `100`, maximum `1000`.
- `verbose` - Set to `'true'` to log each request URL as an `INFO` message, optional.

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from Langfuse.

For example, using below SQL can automatically create foreign tables in the `langfuse` schema.

```sql
-- create all the foreign tables
import foreign schema langfuse from server langfuse_server into langfuse;
```

### Traces

A trace is one end-to-end request through the application. Traces carry `user_id` and an aggregate cost, which makes this the table to join local user tables against.

Ref: [Langfuse data model](https://langfuse.com/docs/observability/data-model)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| traces                |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table langfuse.traces (
  id text,
  name text,
  user_id text,
  session_id text,
  environment text,
  release text,
  version text,
  total_cost double precision,
  latency double precision,
  timestamp timestamp,
  created_at timestamp,
  updated_at timestamp,
  input text,
  output text,
  metadata jsonb,
  tags jsonb
)
  server langfuse_server
  options (
    object 'traces',
    rowid_column 'id'
  );
```

!!! note

    You can use `import foreign schema` statement to automatically create the foreign tables [see above](#entities)

### Observations

An observation is a single step inside a trace, most usefully a model call, with token counts and per-call cost.

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| observations          |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table langfuse.observations (
  id text,
  trace_id text,
  type text,
  name text,
  level text,
  model text,
  input_tokens bigint,
  output_tokens bigint,
  total_tokens bigint,
  input_cost double precision,
  output_cost double precision,
  total_cost double precision,
  latency double precision,
  time_to_first_token double precision,
  start_time timestamp,
  end_time timestamp,
  completion_start_time timestamp,
  prompt_name text,
  prompt_version bigint,
  input text,
  output text,
  metadata jsonb,
  model_parameters jsonb
)
  server langfuse_server
  options (
    object 'observations',
    rowid_column 'id'
  );
```

#### Notes

- Column names are snake_case and mapped to the API's camelCase automatically, so `session_id` reads `sessionId`. A column the API does not return is NULL rather than an error, so you can declare only the columns you need.

- Langfuse returns usage and cost as objects keyed by metric name. These are flattened into scalar columns so aggregates work without JSON extraction:

    | Column           | Source                                                    |
    | ---------------- | --------------------------------------------------------- |
    | `total_tokens`   | `usageDetails.total`                                      |
    | `input_tokens`   | `usageDetails.input`, else `promptTokens`                 |
    | `output_tokens`  | `usageDetails.output`, else `completionTokens`            |
    | `total_cost`     | `costDetails.total`, else `calculatedTotalCost`           |
    | `input_cost`     | `costDetails.input`, else `calculatedInputCost`           |
    | `output_cost`    | `costDetails.output`, else `calculatedOutputCost`         |

    The fallbacks matter in practice: the `observations` endpoint returns cost as `calculatedTotalCost` and has no `totalCost` key at all.

- `observations` does not return `user_id`, which lives on the parent trace, although it is still accepted as a filter. Join through `trace_id` to attribute a call to a user.

- `input`, `output`, and `metadata` hold arbitrary JSON. Declare them as `jsonb` to query into them, or as `text` for the raw value.

## Query Pushdown Support

This FDW supports:

- `limit` pushdown, so `limit 10` costs a single upstream request regardless of project size
- `where` equality pushdown on `trace_id`, `user_id`, `session_id`, `type`, `level`, and `name`
- `where` time bounds using `>=` and `<`

Pages are fetched lazily as the scan consumes them, rather than up front.

The time column and its parameters differ per endpoint: `observations` filters `start_time` via `fromStartTime`/`toStartTime`, everything else filters `timestamp` via `fromTimestamp`/`toTimestamp`.

Time bounds need literal timestamps. Postgres only forwards quals it can evaluate up front, so `now()` arrives as an empty qual list and the filter runs locally after every page has been fetched:

```sql
-- pushed down
select * from langfuse.traces
where timestamp >= '2026-08-02'::timestamp
  and timestamp <  '2026-08-09'::timestamp;

-- not pushed down; fetches every page, then filters
select * from langfuse.traces
where timestamp >= now() - interval '7 days';
```

Confirm with `explain (verbose)` and read the `Wrappers: quals` line — an empty list means nothing was pushed down.

## Supported Data Types

| Postgres Data Type | Langfuse Data Type |
| ------------------ | ------------------ |
| boolean            | Boolean            |
| bigint             | Number             |
| double precision   | Number             |
| text               | String             |
| timestamp          | Time               |
| jsonb              | Json               |

The Langfuse API uses JSON formatted data, please refer to [Langfuse API docs](https://langfuse.com/docs/api) for more details.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Read only; no `insert`, `update`, or `delete` support
- Only `>=` and `<` time bounds are pushed down. The API's bounds are inclusive-from and exclusive-to, so `>` and `<=` would need an epsilon shift to stay correct and are left to Postgres
- `now()` and other non-constant expressions in a time filter are not pushed down; use literal timestamps
- The `v2/observations` endpoint reads a different store than `observations`. On a freshly created cloud project, rows ingested through both `/api/public/ingestion` and the OTLP endpoint were readable via `observations` while `v2/observations` returned an empty result, although it still validated query parameters. Prefer `observations` until that settles
- `re_scan` is not supported, so these tables cannot sit on the inner side of a nested-loop join. Materialize with a CTE if needed
- Materialized views using these foreign tables may fail during logical backups

## Examples

Below are some examples on how to use Langfuse foreign tables.

### Basic example

```sql
import foreign schema langfuse from server langfuse_server into langfuse;

select id, name, user_id, total_cost
from langfuse.traces
limit 10;
```

### Cost per user

```sql
select user_id,
       count(*)          as traces,
       sum(total_cost)   as cost_usd,
       round(avg(latency)::numeric, 2) as avg_latency_s
from langfuse.traces
group by user_id
order by cost_usd desc;
```

### Token usage by model

```sql
select model,
       count(*)           as calls,
       sum(input_tokens)  as input_tokens,
       sum(output_tokens) as output_tokens,
       sum(total_cost)    as cost_usd
from langfuse.observations
group by model
order by cost_usd desc;
```

### Joining against a local users table

This is the query the wrapper exists for: attributing LLM spend to rows in your own database.

Aggregate the foreign table separately before joining. A direct join against several local tables multiplies the trace rows and inflates `sum()`.

```sql
with llm as (
  select user_id,
         count(*)        as llm_calls,
         sum(total_cost) as cost_usd
  from langfuse.traces
  where timestamp >= '2026-08-01'::timestamp
  group by user_id
)
select u.email,
       coalesce(l.llm_calls, 0) as llm_calls,
       l.cost_usd
from auth.users u
left join llm l on l.user_id = u.id::text
order by l.cost_usd desc nulls last;
```

This assumes the application passes the Supabase user id to Langfuse as its `userId`. If the two systems use different identifiers, join through a mapping table instead.
