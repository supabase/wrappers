---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# SQL Server

[Microsoft SQL Server](https://www.microsoft.com/en-au/sql-server/) is a proprietary relational database management system developed by Microsoft.

The SQL Server Wrapper allows you to read data from Microsoft SQL Server within your Postgres database.

## Preparation

Before you can query SQL Server, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the SQL Server Wrapper

Enable the `mssql_wrapper` FDW:

```sql
create foreign data wrapper mssql_wrapper
  handler mssql_fdw_handler
  validator mssql_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your SQL Server connection string in Vault
select vault.create_secret(
  'Server=localhost,1433;User=sa;Password=my_password;Database=master;IntegratedSecurity=false;TrustServerCertificate=true;encrypt=DANGER_PLAINTEXT;ApplicationName=wrappers',
  'mssql',
  'MS SQL Server connection string for Wrappers'
);

-- Retrieve the `key_id`
select key_id from vault.decrypted_secrets where name = 'mssql';
```

The connection string is an [ADO.NET connection string](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings), which specifies connection parameters in semicolon-delimited string.

**Supported parameters**

All parameter keys are handled case-insensitive.

| Parameter              | Allowed Values                | Description                                                                                        |
| ---------------------- | ----------------------------- | -------------------------------------------------------------------------------------------------- |
| Server                 | `<string>`                    | The name or network address of the instance of SQL Server to which to connect. Format: `host,port` |
| User                   | `<string>`                    | The SQL Server login account.                                                                      |
| Password               | `<string>`                    | The password for the SQL Server account logging on.                                                |
| Database               | `<string>`                    | The name of the database.                                                                          |
| IntegratedSecurity     | false                         | Windows/Kerberos authentication and SQL authentication.                                            |
| TrustServerCertificate | true, false                   | Specifies whether the driver trusts the server certificate when connecting using TLS.              |
| Encrypt                | true, false, DANGER_PLAINTEXT | Specifies whether the driver uses TLS to encrypt communication.                                    |
| ApplicationName        | `<string>`                    | Sets the application name for the connection.                                                      |

### Connecting to SQL Server

We need to provide Postgres with the credentials to connect to SQL Server. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server mssql_server
      foreign data wrapper mssql_wrapper
      options (
        conn_string_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server mssql_server
      foreign data wrapper mssql_wrapper
      options (
        conn_string 'Server=localhost,1433;User=sa;Password=my_password;Database=master;IntegratedSecurity=false;TrustServerCertificate=true;encrypt=DANGER_PLAINTEXT;ApplicationName=wrappers'
      );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists mssql;
```

## Options

The full list of foreign table options are below:

- `table` - Source table or view name in SQL Server, required.

This can also be a subquery enclosed in parentheses, for example,

```sql
table '(select * from users where id = 42 or id = 43)'
```

## Entities

### SQL Server Tables

This is an object representing SQL Server tables and views.

Ref: [Microsoft SQL Server docs](https://www.microsoft.com/en-au/sql-server/)

#### Operations

| Object     | Select | Insert | Update | Delete | Truncate |
| ---------- | :----: | :----: | :----: | :----: | :------: |
| table/view |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table mssql.users (
  id bigint,
  name text,
  dt timestamp
)
  server mssql_server
  options (
    table 'users'
  );
```

#### Notes

- Supports both tables and views as data sources
- Can use subqueries in the `table` option
- Query pushdown supported for:
      - `where` clauses
      - `order by` clauses
      - `limit` clauses
- See Data Types section for type mappings between PostgreSQL and SQL Server

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown.

## Supported Data Types

| Postgres Type    | SQL Server Type                  |
| ---------------- | -------------------------------- |
| boolean          | bit                              |
| char             | tinyint                          |
| smallint         | smallint                         |
| real             | float(24)                        |
| integer          | int                              |
| double precision | float(53)                        |
| bigint           | bigint                           |
| numeric          | numeric/decimal                  |
| text             | varchar/char/text                |
| date             | date                             |
| timestamp        | datetime/datetime2/smalldatetime |
| timestamptz      | datetime/datetime2/smalldatetime |

## Limitations

This section describes important limitations and considerations when using this FDW:

- Large result sets may experience slower performance due to full data transfer requirement
- Only supports specific data type mappings between Postgres and SQL Server
- Only support read operations (no INSERT, UPDATE, DELETE, or TRUNCATE)
- Windows authentication (Integrated Security) not supported
- Materialized views using these foreign tables may fail during logical backups

## Examples

### Basic Example

First, create a source table in SQL Server:

```sql
-- Run below SQLs on SQL Server to create source table
create table users (
  id bigint,
  name varchar(30),
  dt datetime2
);

-- Add some test data
insert into users(id, name, dt) values (42, 'Foo', '2023-12-28');
insert into users(id, name, dt) values (43, 'Bar', '2023-12-27');
insert into users(id, name, dt) values (44, 'Baz', '2023-12-26');
```

Then create and query the foreign table in PostgreSQL:

```sql
create foreign table mssql.users (
  id bigint,
  name text,
  dt timestamp
)
  server mssql_server
  options (
    table 'users'
  );

select * from mssql.users;
```

### Remote Subquery Example

Create a foreign table using a subquery:

```sql
create foreign table mssql.users_subquery (
  id bigint,
  name text,
  dt timestamp
)
  server mssql_server
  options (
    table '(select * from users where id = 42 or id = 43)'
  );

select * from mssql.users_subquery;
```
