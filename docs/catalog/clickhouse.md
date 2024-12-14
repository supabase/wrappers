---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# ClickHouse

[ClickHouse](https://clickhouse.com/) is a fast open-source column-oriented database management system that allows generating analytical data reports in real-time using SQL queries.

The ClickHouse Wrapper allows you to read and write data from ClickHouse within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Supported Data Types

| Postgres Type    | ClickHouse Type |
| ---------------- | --------------- |
| boolean          | UInt8           |
| smallint         | Int16           |
| integer          | UInt16          |
| integer          | Int32           |
| bigint           | UInt32          |
| bigint           | Int64           |
| bigint           | UInt64          |
| real             | Float32         |
| double precision | Float64         |
| text             | String          |
| date             | Date            |
| timestamp        | DateTime        |
| *                | Nullable&lt;T&gt; |

## Preparation

Before you can query ClickHouse, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the ClickHouse Wrapper

Enable the `clickhouse_wrapper` FDW:

```sql
create foreign data wrapper clickhouse_wrapper
  handler click_house_fdw_handler
  validator click_house_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your ClickHouse credential in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'clickhouse',
  'tcp://default:@localhost:9000/default'
)
returning key_id;
```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema clickhouse;
```

## Creating Foreign Tables

### Tables

The ClickHouse Wrapper supports data reads and writes from ClickHouse tables.

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Tables |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table my_clickhouse_table (
  id bigint,
  name text
)
  server clickhouse_server
  options (
    table 'people'
  );
```

#### Notes

- Supports `where`, `order by` and `limit` clause pushdown
- Supports parametrized views in subqueries
- When using `rowid_column`, it must be specified for data modification operations

### Foreign Table Options

The following options are available when creating ClickHouse foreign tables:

- `table` - Source table name in ClickHouse, required

  This can also be a subquery enclosed in parentheses, for example,

  ```sql
  table '(select * from my_table)'
  ```

  [Parametrized view](https://clickhouse.com/docs/en/sql-reference/statements/create/view#parameterized-view) is also supported in the subquery. In this case, you need to define a column for each parameter and use `where` to pass values to them. For example,

  ```sql
   create foreign table test_vw (
     id bigint,
     col1 text,
     col2 bigint,
     _param1 text,
     _param2 bigint
   )
     server clickhouse_server
     options (
       table '(select * from my_view(column1=${_param1}, column2=${_param2}))'
     );

   select * from test_vw where _param1='aaa' and _param2=32;
  ```

- `rowid_column` - Primary key column name, optional for data scan, required for data modify

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown, as well as parametrized view (see above).

## Examples

### Basic Query Example

This example demonstrates basic ClickHouse table operations.

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Tables |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table people (
  id bigint,
  name text
)
  server clickhouse_server
  options (
    table 'people'
  );
```

#### Notes

- Query the table using standard SQL: `select * from people;`
- Supports data modification operations with `rowid_column`
- Example operations:
  ```sql
  insert into people values (4, 'Yoda');
  update people set name = 'Princess Leia' where id = 2;
  delete from people where id = 3;
  ```
