---
source:
documentation:
author: 陈杨文(https://github.com/wenerme)
tags:
  - native
  - community
---

# MySQL

[MySQL](https://www.mysql.com/) is one of the world's most popular open-source relational database management systems.

The MySQL Wrapper allows you to read and write data from MySQL within your Postgres database.

## Preparation

Before you can query MySQL, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the MySQL Wrapper

Enable the `mysql_wrapper` FDW:

```sql
create foreign data wrapper mysql_wrapper
  handler mysql_fdw_handler
  validator mysql_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your MySQL connection string in Vault and retrieve the created `key_id`
select vault.create_secret(
  'mysql://user:password@host:3306/mydb',
  'mysql',
  'MySQL connection string for Wrappers'
);
```

### Connecting to MySQL

We need to provide Postgres with the credentials to connect to MySQL, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server mysql_server
      foreign data wrapper mysql_wrapper
      options (
        conn_string_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server mysql_server
      foreign data wrapper mysql_wrapper
      options (
        conn_string 'mysql://user:password@host:3306/mydb'
      );
    ```

The connection string follows the standard MySQL URL format:

```
mysql://[user[:password]@][host][:port]/database
```

Some connection string examples:

- `mysql://root:secret@localhost:3306/mydb`
- `mysql://app_user:password@db.example.com:3306/production`
- `mysql://user:password@127.0.0.1:3306/testdb?ssl-mode=required`

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists mysql;
```

## Options

The following options are available when creating MySQL foreign tables:

- `table` - Source table name in MySQL, required

    This can also be a subquery enclosed in parentheses, for example,

    ```sql
    table '(select id, name from my_table where active = 1)'
    ```

- `rowid_column` - Primary key column name, optional for data scan, required for data modify

## Entities

### Tables

The MySQL Wrapper supports data reads and writes from MySQL tables.

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Tables |   ✅    |   ✅    |   ✅    |   ✅    |    ❌     |

#### Usage

```sql
create foreign table mysql.my_table (
  id bigint,
  name text
)
  server mysql_server
  options (
    table 'people'
  );
```

#### Notes

- Supports `where`, `order by` and `limit` clause pushdown
- Data is streamed row-by-row from MySQL, making it suitable for large result sets
- When using `rowid_column`, it must be specified for data modification operations

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown.

## Import Foreign Schema

This FDW supports [`import foreign schema`](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to automatically create foreign table definitions by reading the MySQL table structure from `information_schema`.

The `remote_schema` maps to a MySQL database name. For example, to import all tables from the MySQL database `mydb`:

```sql
import foreign schema mydb
  from server mysql_server
  into mysql;
```

You can limit which tables are imported using `limit to` or `except`:

```sql
-- Import only specific tables
import foreign schema mydb
  limit to (users, orders)
  from server mysql_server
  into mysql;

-- Import all tables except specific ones
import foreign schema mydb
  except (tmp_data, archive)
  from server mysql_server
  into mysql;
```

An additional option is available for `import foreign schema`:

- `strict` - If set to `true`, the import will fail if any MySQL column type cannot be mapped to a Postgres type. Defaults to `false` (unsupported column types are silently skipped).

```sql
import foreign schema mydb
  from server mysql_server
  into mysql
  options (strict 'true');
```

Primary key columns are automatically detected and set as the `rowid_column` option on the generated foreign table, enabling INSERT, UPDATE, and DELETE operations without any manual configuration.

## Supported Data Types

| Postgres Type    | MySQL Type                                                     |
| ---------------- | -------------------------------------------------------------- |
| boolean          | boolean, bool, tinyint(1)                                      |
| smallint         | tinyint (non-boolean), smallint, year                          |
| integer          | mediumint, int, integer                                        |
| bigint           | bigint                                                         |
| real             | float                                                          |
| double precision | double, double precision                                       |
| numeric          | decimal, numeric                                               |
| text             | char, varchar, tinytext, text, mediumtext, longtext, enum, set |
| date             | date                                                           |
| timestamp        | datetime, timestamp                                            |
| time             | time                                                           |
| jsonb            | json                                                           |

!!! note

    `tinyint(1)` is mapped to `boolean` as it is the conventional MySQL representation for boolean values. All other `tinyint` variants are mapped to `smallint`.

    `decimal` and `numeric` columns with explicit precision and scale (e.g. `decimal(12,2)`) are imported as `numeric(p,s)` in Postgres.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Only a subset of MySQL data types are supported; columns with unmapped types are skipped during `import foreign schema` unless `strict` mode is enabled
- `timestamp` and `datetime` values are treated as naive timestamps without timezone conversion
- MySQL `json` columns are mapped to Postgres `jsonb`; invalid JSON will cause an error during reads
- Materialized views using foreign tables may fail during logical backups

## Examples

### Basic example

This example shows how to query a MySQL table from Postgres.

First, create the source table in MySQL:

```sql
-- Run on MySQL
create table people (
  id bigint primary key auto_increment,
  name varchar(100),
  email varchar(200)
);

insert into people (name, email) values
  ('Alice', 'alice@example.com'),
  ('Bob', 'bob@example.com'),
  ('Carol', 'carol@example.com');
```

Then create the foreign table in Postgres and query it:

```sql
create foreign table mysql.people (
  id bigint,
  name text,
  email text
)
  server mysql_server
  options (
    table 'people'
  );

select * from mysql.people;
```

### Data modification example

This example demonstrates INSERT, UPDATE, and DELETE on a foreign table. The `rowid_column` option is required for data modification:

```sql
create foreign table mysql.people (
  id bigint,
  name text,
  email text
)
  server mysql_server
  options (
    table 'people',
    rowid_column 'id'
  );

-- Insert a new row
insert into mysql.people (name, email)
values ('Dave', 'dave@example.com');

-- Update an existing row
update mysql.people
set email = 'alice_new@example.com'
where id = 1;

-- Delete a row
delete from mysql.people
where id = 2;
```

### Import foreign schema example

This example imports all tables from a MySQL database automatically:

```sql
-- Import all tables from the MySQL 'shop' database
import foreign schema shop
  from server mysql_server
  into mysql;

-- The foreign tables are now available
select * from mysql.products limit 10;
select * from mysql.orders where status = 'pending';
```