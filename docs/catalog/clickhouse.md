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
-- Save your ClickHouse credential in Vault and retrieve the created `key_id`
select vault.create_secret(
  'tcp://default:@localhost:9000/default',
  'clickhouse',
  'ClickHouse credential for Wrappers'
);
```

### Connecting to ClickHouse

We need to provide Postgres with the credentials to connect to ClickHouse, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server clickhouse_server
      foreign data wrapper clickhouse_wrapper
      options (
        conn_string_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server clickhouse_server
      foreign data wrapper clickhouse_wrapper
      options (
        conn_string 'tcp://default:@localhost:9000/default'
      );
    ```

Some connection string examples:

- `tcp://user:password@host:9000/clicks?compression=lz4&ping_timeout=42ms`
- `tcp://default:PASSWORD@abc.eu-west-1.aws.clickhouse.cloud:9440/default?connection_timeout=30s&ping_before_query=false&secure=true`

Check out [more connection string parameters](https://github.com/suharev7/clickhouse-rs#dns).

!!! note

    This ClickHouse FDW only supports native protocol port `9000` and `9440`, HTTP(S) port like `8123` and `8443` are not supported yet.

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists clickhouse;
```

## Options

The following options are available when creating ClickHouse foreign tables:

- `table` - Source table name in ClickHouse, required

This can also be a subquery enclosed in parentheses, for example,

```sql
table '(select * from my_table)'
```

### Parametrized views

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

## Entities

### Tables

The ClickHouse Wrapper supports data reads and writes from ClickHouse tables.

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Tables |   ✅    |   ✅    |   ✅    |   ✅    |    ❌     |

#### Usage

```sql
create foreign table clickhouse.my_table (
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

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown, as well as parametrized view (see above).

## Supported Data Types

| Postgres Type      | ClickHouse Type   |
| ------------------ | ----------------- |
| boolean            | Bool              |
| "char"             | Int8              |
| smallint           | UInt8             |
| smallint           | Int16             |
| integer            | UInt16            |
| integer            | Int32             |
| bigint             | UInt32            |
| bigint             | Int64             |
| bigint             | UInt64            |
| real               | Float32           |
| double precision   | Float64           |
| numeric            | UInt128           |
| numeric            | Int128            |
| text               | UInt256           |
| text               | Int256            |
| numeric            | Decimal           |
| text               | String            |
| text               | FixedString(N)    |
| date               | Date              |
| timestamp          | DateTime          |
| uuid               | UUID              |
| boolean[]          | Array(Boolean)    |
| smallint[]         | Array(Int16)      |
| integer[]          | Array(Int32)      |
| bigint[]           | Array(Int64)      |
| real[]             | Array(Float32)    |
| double precision[] | Array(Float64)    |
| text[]             | Array(String)     |
| *                  | Nullable&lt;T&gt; |

## Limitations

This section describes important limitations and considerations when using this FDW:

- Full result sets must be transferred from ClickHouse to PostgreSQL
- Large result sets consume significant PostgreSQL memory
- Only basic query clauses (WHERE, ORDER BY, LIMIT) support pushdown
- Limited data type mappings (see [Supported Data Types](#supported-data-types) section)
- Materialized views using foreign tables may fail during logical backups

## Examples

### Basic Query Example

This example demonstrates basic ClickHouse table operations.

```sql
-- Run below SQLs on ClickHouse to create source table
create table people (
  id Int64,
  name String
)
engine=MergeTree()
order by id;

-- Add some test data
insert into people values (1, 'Luke Skywalker'), (2, 'Leia Organa'), (3, 'Han Solo');
```

Create foreign table on Postgres database:

```sql
create foreign table clickhouse.people (
  id bigint,
  name text
)
  server clickhouse_server
  options (
    table 'people'
  );

-- data scan
select * from clickhouse.people;

-- data modify
insert into clickhouse.people values (4, 'Yoda');
update clickhouse.people set name = 'Princess Leia' where id = 2;
delete from clickhouse.people where id = 3;
```
