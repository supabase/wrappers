---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Logflare

[Logflare](https://logflare.app) is a centralized web-based log management solution to easily access Cloudflare, Vercel & Elixir logs.

The Logflare Wrapper allows you to read data from Logflare endpoints within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Preparation

Before you can query Logflare, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Logflare Wrapper

Enable the `logflare_wrapper` FDW:

```sql
create foreign data wrapper logflare_wrapper
  handler logflare_fdw_handler
  validator logflare_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Logflare API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'logflare',
  'YOUR_SECRET'
)
returning key_id;
```

Once you have stored your credentials, you can create a server to connect to Logflare:

=== "With Vault"

    ```sql
    create server logflare_server
      foreign data wrapper logflare_wrapper
      options (
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server logflare_server
      foreign data wrapper logflare_wrapper
      options (
        api_key '<Logflare API Key>' -- Logflare API key, required
      );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema logflare;
```

## Entities

### Logflare

This is an object representing Logflare endpoint data.

Ref: [Logflare docs](https://logflare.app)

#### Operations

| Object   | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| Logflare |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table my_logflare_table (
  id bigint,
  name text,
  _result text
)
  server logflare_server
  options (
    endpoint '9dd9a6f6-8e9b-4fa4-b682-4f2f5cd99da3'
  );
```

#### Notes

- Meta Column `_result`:
  - Data type must be `text`
  - Stores the whole result record in JSON string format
  - Use JSON queries to extract fields: `_result::json->>'field_name'`

- Query Parameters:
  - Use parameter columns with prefix `_param_`
  - Example: `_param_org_id`, `_param_iso_timestamp_start`
  - Parameters are passed to the Logflare endpoint

## Foreign Table Options

- `endpoint` - Logflare endpoint UUID or name, required.

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

### Basic Example

Given a Logflare endpoint response:
```json
[
  {
    "id": 123,
    "name": "foo"
  }
]
```

You can create and query a foreign table:

```sql
create foreign table people (
  id bigint,
  name text,
  _result text
)
  server logflare_server
  options (
    endpoint '9dd9a6f6-8e9b-4fa4-b682-4f2f5cd99da3'
  );

select * from people;
```

### Query Parameters Example

For an endpoint accepting parameters:
- org_id
- iso_timestamp_start
- iso_timestamp_end

With response format:
```json
[
  {
    "db_size": "large",
    "org_id": "123",
    "runtime_hours": 21.95,
    "runtime_minutes": 1317
  }
]
```

Create and query the table with parameters:

```sql
create foreign table runtime_hours (
  db_size text,
  org_id text,
  runtime_hours numeric,
  runtime_minutes bigint,
  _param_org_id bigint,
  _param_iso_timestamp_start text,
  _param_iso_timestamp_end text,
  _result text
)
  server logflare_server
  options (
    endpoint 'my.custom.endpoint'
  );

select
  db_size,
  org_id,
  runtime_hours,
  runtime_minutes
from
  runtime_hours
where _param_org_id = 123
  and _param_iso_timestamp_start = '2023-07-01 02:03:04'
  and _param_iso_timestamp_end = '2023-07-02';
```
