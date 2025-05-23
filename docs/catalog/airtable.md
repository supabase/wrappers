---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Airtable

[Airtable](https://www.airtable.com) is an easy-to-use online platform for creating and sharing relational databases.

The Airtable Wrapper allows you to read data from your Airtable bases/tables within your Postgres database.

## Preparation

Before you can query Airtable, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Airtable Wrapper

Enable the `airtable_wrapper` FDW:

```sql
create foreign data wrapper airtable_wrapper
  handler airtable_fdw_handler
  validator airtable_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.


Get your token from [Airtable's developer portal](https://airtable.com/create/tokens).

```sql
-- Save your Airtable API key in Vault and retrieve the created `key_id`
select vault.create_secret(
  '<Airtable API Key or PAT>', -- Airtable API key or Personal Access Token (PAT)
  'airtable',
  'Airtable API key for Wrappers'
);
```

### Connecting to Airtable

We need to provide Postgres with the credentials to connect to Airtable, and any additional options. We can do this using the `create server` command:


=== "With Vault"

    ```sql
    create server airtable_server
      foreign data wrapper airtable_wrapper
      options (
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server airtable_server
      foreign data wrapper airtable_wrapper
      options (
        api_key '<your_api_key>'
      );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists airtable;
```

## Entities

The Airtable Wrapper supports data reads from the Airtable API.

### Records

The Airtable Wrapper supports data reads from Airtable's [Records](https://airtable.com/developers/web/api/list-records) endpoint (_read only_).

#### Operations

| Object  | Select | Insert | Update | Delete | Truncate |
| ------- | :----: | :----: | :----: | :----: | :------: |
| Records |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

Get your base ID and table ID from your table's URL.

![airtable_credentials](../assets/airtable_credentials.png)

!!! note

    Foreign tables must be lowercase, regardless of capitalization in Airtable.

```sql
create foreign table airtable.my_foreign_table (
  message text
  -- other fields
)
server airtable_server
options (
  base_id 'appXXXX',
  table_id 'tblXXXX'
);
```

#### Notes

- The table requires both `base_id` and `table_id` options
- Optional `view_id` can be specified to query a specific view

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Supported Data Types

| Postgres Data Type | Airtable Data Type |
| ------------------ | ------------------ |
| boolean            | Checkbox           |
| smallint           | Number             |
| integer            | Number             |
| bigint             | Autonumber         |
| bigint             | Number             |
| real               | Number             |
| double precision   | Number             |
| numeric            | Number             |
| numeric            | Currency           |
| numeric            | Percent            |
| text               | Single line text   |
| text               | Long text          |
| text               | Single select      |
| text               | Phone number       |
| text               | Email              |
| text               | URL                |
| date               | Date               |
| timestamp          | Created time       |
| timestamp          | Last modified time |
| jsonb              | Multiple select    |
| jsonb              | Created by         |
| jsonb              | Last modified by   |
| jsonb              | User               |

## Limitations

This section describes important limitations and considerations when using this FDW:

- No query pushdown support, all filtering must be done locally
- Large result sets may experience slower performance due to full data transfer requirement
- No support for Airtable formulas or computed fields
- Views must be pre-configured in Airtable
- No support for Airtable's block features
- Materialized views using these foreign tables may fail during logical backups

## Examples

### Query an Airtable table

This will create a "foreign table" inside your Postgres database called `airtable_table`:

```sql
create foreign table airtable.airtable_table (
  name text,
  notes text,
  content text,
  amount numeric,
  updated_at timestamp
)
server airtable_server
options (
  base_id 'appTc3yI68KN6ukZc',
  table_id 'tbltiLinE56l3YKfn'
);
```

You can now fetch your Airtable data from within your Postgres database:

```sql
select * from airtable.airtable_table;
```

### Query an Airtable view

We can also create a foreign table from an Airtable View called `airtable_view`:

```sql
create foreign table airtable.airtable_view (
  name text,
  notes text,
  content text,
  amount numeric,
  updated_at timestamp
)
server airtable_server
options (
  base_id 'appTc3yI68KN6ukZc',
  table_id 'tbltiLinE56l3YKfn',
  view_id 'viwY8si0zcEzw3ntZ'
);
```

You can now fetch your Airtable data from within your Postgres database:

```sql
select * from airtable.airtable_view;
```
