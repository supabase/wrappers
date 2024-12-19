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
-- Save your Airtable API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'airtable',
  '<Airtable API Key or PAT>' -- Airtable API key or Personal Access Token (PAT)
)
returning key_id;
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

The Airtable FDW supports the following PostgreSQL data types:

| PostgreSQL Type | Notes |
| -------------- | ----- |
| `boolean` | Maps to Airtable checkbox fields |
| `smallint`, `integer`, `bigint` | Maps to Airtable number fields |
| `real`, `double precision`, `numeric` | Maps to Airtable number fields |
| `text` | Maps to Airtable single line text, long text, URL, email, and phone fields |
| `date` | Maps to Airtable date fields |
| `timestamp` | Maps to Airtable date fields |
| `timestamp with time zone` | Maps to Airtable date fields |
| `jsonb` | Maps to Airtable array fields, linked records, and other complex types |

### Array and Complex Types

Arrays and linked records are supported through the `jsonb` type:

```sql
create foreign table airtable.complex_types (
  linked_records jsonb,  -- For Airtable linked records
  attachments jsonb,    -- For Airtable attachments
  multiselect jsonb     -- For Airtable multiple select fields
)
server airtable_server
options (
  base_id 'appXXXX',
  table_id 'tblXXXX'
);
```

### Column Naming Rules

1. Column names are case-insensitive
2. Column names containing spaces must be quoted:
```sql
create foreign table airtable.my_table (
  "Business Name" text,  -- Column with spaces must be quoted
  city text,            -- Simple column names don't need quotes
  "ZIP Code" text       -- Another column with spaces
)
server airtable_server
options (
  base_id 'appXXXX',
  table_id 'tblXXXX'
);
```

### Handling Complex Data Types

Here's an example of querying a table with linked records and arrays:

```sql
create foreign table airtable.contacts (
  name text,
  "Phone Numbers" jsonb,  -- Array of phone numbers
  "Related Companies" jsonb,  -- Linked records
  tags jsonb  -- Multiple select field
)
server airtable_server
options (
  base_id 'appXXXX',
  table_id 'tblXXXX'
);

-- Query the complex types
select
  name,
  "Phone Numbers"->0 as primary_phone,  -- Access first phone number
  jsonb_array_length("Related Companies") as company_count,
  tags
from airtable.contacts;
```

Note: Null values in Airtable fields are returned as SQL NULL values.

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
