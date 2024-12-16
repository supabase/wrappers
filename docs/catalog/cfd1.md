---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Cloudflare D1

[Cloudflare D1](https://developers.cloudflare.com/d1/) is Cloudflare's managed, serverless database with SQLite's SQL semantics, built-in disaster recovery, and Worker and HTTP API access.

The Cloudflare D1 Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read data from Cloudflare D1 database for use within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Limitations

This section describes important limitations and considerations when using this FDW:

- **Performance Limitations**:
  - Query pushdown support is limited to `where`, `order by`, and `limit` clauses
  - WebAssembly execution overhead may impact query performance
  - API request latency affects query performance as each operation requires D1 API calls
  - Large result sets may experience slower performance due to full data transfer requirement
  - No support for parallel query execution

- **Feature Limitations**:
  - Limited data type support (only bigint, double precision, text supported)
  - Truncate operations are not supported for any object type
  - Foreign tables with subquery options cannot support data modification
  - Column names must exactly match between D1 and foreign table
  - WebAssembly package version must match exactly with specified checksum
  - Database operations (create, update, delete) are not supported via the FDW

- **Resource Usage**:
  - WebAssembly module initialization requires additional memory overhead
  - Full result sets must be loaded into memory before processing
  - Each query requires a complete API request-response cycle
  - No connection pooling or caching support
  - Memory usage scales with result set size and JSON data volume

- **Known Issues**:
  - Materialized views using these foreign tables may fail during logical backups
  - WebAssembly binary must be downloaded and verified on each server restart
  - Checksum verification is currently marked as "tbd" in available versions
  - Complex nested JSON structures require manual parsing
  - Error handling between WebAssembly and PostgreSQL may not preserve all error details

## Supported Data Types

| Postgres Data Type | D1 Data Type |
| ------------------ | ------------ |
| bigint             | integer      |
| double precision   | real         |
| text               | text         |
| text               | blob         |

The D1 API uses JSON formatted data, please refer to [D1 API docs](https://developers.cloudflare.com/api/operations/cloudflare-d1-list-databases) for more details.

## Available Versions

| Version | Wasm Package URL                                                                                | Checksum                                                           |
| ------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_cfd1_fdw_v0.1.0/cfd1_fdw.wasm` | `tbd` |

## Preparation

Before you can query D1, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the D1 Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your D1 API token in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'cfd1',
  '<D1 API token>' -- Cloudflare D1 API token
)
returning key_id;
```

### Connecting to D1

We need to provide Postgres with the credentials to access D1 and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server cfd1_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_cfd1_fdw_v0.1.0/cfd1_fdw.wasm',
        fdw_package_name 'supabase:cfd1-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
        api_url 'https://api.cloudflare.com/client/v4/accounts/<account_id>/d1/database',  -- optional
        account_id '<Account ID>',
        database_id '<Database ID>',
        api_token_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server cfd1_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_cfd1_fdw_v0.1.0/cfd1_fdw.wasm',
        fdw_package_name 'supabase:cfd1-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
        api_url 'https://api.cloudflare.com/client/v4/accounts/<account_id>/d1/database',  -- optional
        account_id '<Account ID>',
        database_id '<Database ID>',
        api_token '<D1 API token>'
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists cfd1;
```

## Options

The full list of foreign table options are below:

- `table` - Source table name in D1, required.

    - This option can also be a subquery enclosed in parentheses, see below for examples.
    - A pseudo-table name `_meta_databases` can be used to query databases.

- `rowid_column` - Primary key column name, optional for data scan, required for data modify.

## Entities

The D1 Wrapper supports data reads and writes from the Cloudflare D1 API.

### D1 Databases

This is an object representing a D1 database.

Ref: [D1 databases docs](https://developers.cloudflare.com/api/operations/cloudflare-d1-list-databases)

#### Operations

| Object      | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| database    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table cfd1.databases (
  uuid text,
  name text,
  version text,
  num_tables bigint,
  file_size bigint,
  created_at text,
  _attrs jsonb
)
  server cfd1_server
  options (
    table '_meta_databases'
  );
```

#### Notes

- The `_attrs` meta column contains all database attributes in JSON format
- The table option must be `_meta_databases`
- Only column names listed above are allowed

### D1 Tables

This is an object representing a D1 table.

Ref: [D1 query docs](https://developers.cloudflare.com/api/operations/cloudflare-d1-query-database)

#### Operations

| Object      | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| table       |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table cfd1.mytable (
  id bigint,
  name text,
  amount double precision,
  metadata text,
  _attrs jsonb
)
  server cfd1_server
  options (
    table 'mytable',
    rowid_column 'id'
  );
```

#### Notes

- The `_attrs` meta column contains all attributes in JSON format
- Can use subquery in `table` option
- Requires `rowid_column` for data modification operations
- Supports query pushdown for `where`, `order by`, and `limit` clauses
- Column names, except `_attrs`, must match between D1 and foreign table
- Data types must be compatible according to type mapping table

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown.

## Examples

Below are some examples on how to use D1 foreign tables.

### Basic Example

This example will create a "foreign table" inside your Postgres database and query its data. 

```sql
create foreign table cfd1.databases (
  uuid text,
  name text,
  version text,
  num_tables bigint,
  file_size bigint,
  created_at text,
  _attrs jsonb
)
  server cfd1_server
  options (
    table '_meta_databases'
  );

-- query D1 databases
select * from cfd1.databases;
```

### Query A Table

Let's create a source table `test_table` in D1 web console and add some testing data.

| Column Name | Data Type |
| ----------- | --------- |
| id          | integer   |
| name        | text      |
| amount      | real      |
| metadata    | blob      |

This example will create a "foreign table" inside your Postgres database and query its data.

```sql
create foreign table cfd1.test_table (
  id bigint,
  name text,
  amount double precision,
  metadata text,
  _attrs jsonb
)
  server cfd1_server
  options (
    table 'test_table',
    rowid_column 'id'
  );

select * from cfd1.test_table;
```

### Table With Subquery

The `table` option can also be a subquery enclosed in parentheses.

```sql
create foreign table cfd1.test_table_subquery (
  id bigint,
  name text,
  amount double precision,
  metadata text,
  _attrs jsonb
)
  server cfd1_server
  options (
    table '(select * from test_table)'
  );

select * from cfd1.test_table_subquery;
```

!!! note

    The foreign table with subquery option cannot support data modification.

### Modify Data

This example will modify data in a "foreign table" inside your Postgres database, note that `rowid_column` table option is required for data modify.

```sql
-- insert new data
insert into cfd1.test_table(id, name, amount)
values (123, 'test name 123', 321.654);

-- update existing data
update cfd1.test_table
set name = 'new name', amount = null
where id = 123;

-- delete data
delete from cfd1.test_table where id = 123;
```

