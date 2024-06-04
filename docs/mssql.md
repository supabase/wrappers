[Microsoft SQL Server](https://www.microsoft.com/en-au/sql-server/) is a proprietary relational database management system developed by Microsoft.

The SQL Server Wrapper allows you to read data from Microsoft SQL Server within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Supported Data Types

| Postgres Type      | SQL Server Type                  |
| ------------------ | -------------------------------- |
| boolean            | bit                              |
| char               | tinyint                          |
| smallint           | smallint                         |
| real               | float(24)                        |
| integer            | int                              |
| double precision   | float(53)                        |
| bigint             | bigint                           |
| numeric            | numeric/decimal                  |
| text               | varchar/char/text                |
| date               | date                             |
| timestamp          | datetime/datetime2/smalldatetime |
| timestamptz        | datetime/datetime2/smalldatetime |

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper mssql_wrapper
  handler mssql_fdw_handler
  validator mssql_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your SQL Server connection string in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'mssql',
  'Server=localhost,1433;User=sa;Password=my_password;Database=master;IntegratedSecurity=false;TrustServerCertificate=true;encrypt=DANGER_PLAINTEXT;ApplicationName=wrappers'
)
returning key_id;
```

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

The connection string is an [ADO.NET connection string](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings), which specifies connection parameters in semicolon-delimited string.

**Supported parameters**

All parameter keys are handled case-insensitive.

| Parameter              | Allowed Values        | Description                     |
| ---------------------- | --------------------- | ------------------------------- |
| Server                 | `<string>` | The name or network address of the instance of SQL Server to which to connect. Format: `host,port` |
| User                   | `<string>` | The SQL Server login account. |
| Password               | `<string>` | The password for the SQL Server account logging on. |
| Database               | `<string>` | The name of the database. |
| IntegratedSecurity     | false    | Windows/Kerberos authentication and SQL authentication.|
| TrustServerCertificate | true, false | Specifies whether the driver trusts the server certificate when connecting using TLS. |
| Encrypt                | true, false, DANGER_PLAINTEXT | Specifies whether the driver uses TLS to encrypt communication.|
| ApplicationName        | `<string>` | Sets the application name for the connection. |


## Creating Foreign Tables

The SQL Server Wrapper supports data reads from SQL Server.

| Integration | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| SQL Server  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
create foreign table mssql_users (
  id bigint,
  name text,
  dt timestamp
)
  server mssql_server
  options (
    table 'users'
  );
```

### Foreign table options

The full list of foreign table options are below:

- `table` - Source table or view name in SQL Server, required.

   This can also be a subquery enclosed in parentheses, for example,

   ```sql
   table '(select * from users where id = 42 or id = 43)'
   ```

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown.

## Examples

Some examples on how to use SQL Server foreign tables.

Let's prepare the source table in SQL Server first:

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

### Basic example

This example will create a foreign table inside your Postgres database and query its data:

```sql
create foreign table mssql_users (
  id bigint,
  name text,
  dt timestamp
)
  server mssql_server
  options (
    table 'users'
  );

select * from mssql_users;
```

### Remote subquery example

This example will create a foreign table based on a remote subquery and query its data:

```sql
create foreign table mssql_users_subquery (
  id bigint,
  name text,
  dt timestamp
)
  server mssql_server
  options (
    table '(select * from users where id = 42 or id = 43)'
  );

select * from mssql_users_subquery;
```

