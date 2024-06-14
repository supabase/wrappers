[Snowflake](https://www.snowflake.com) is a cloud-based data platform provided as a DaaS (Data-as-a-Service) solution with data storage and analytics service.

The Snowflake Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read and write data from Snowflake within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Supported Data Types

| Postgres Data Type | Snowflake Data Type |
| ------------------ | ------------------- |
| boolean            | BOOLEAN             |
| smallint           | SMALLINT            |
| integer            | INT                 |
| bigint             | BIGINT              |
| real               | FLOAT4              |
| double precision   | FLOAT8              |
| numeric            | NUMBER              |
| text               | VARCHAR             |
| date               | DATE                |
| timestamp          | TIMESTAMP_NTZ       |
| timestamptz        | TIMESTAMP_TZ        |

## Available Versions

| Version | Wasm Package URL |
| --------| ---------------- |
| 0.1.0   | https://github.com/supabase/wrappers/releases/download/v0.4.0/snowflake_fdw.wasm |

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

This FDW uses key-pair authentication to access Snowflake SQL Rest API, please refer to [Snowflake docs](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#label-sql-api-authenticating-key-pair) for more details about the key-pair authentication.

```sql
-- Save your Snowflake private key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'snowflake',
  E'-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----'
)
returning key_id;
```

### Connecting to Snowflake

We need to provide Postgres with the credentials to connect to Snowflake, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server snowflake_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/v0.4.0/snowflake_fdw.wasm',
        fdw_package_name 'supabase:snowflake-fdw',
        fdw_package_version '0.1.0',
        account_identifier 'MYORGANIZATION-MYACCOUNT',
        user 'MYUSER',
        public_key_fingerprint 'SizgPofeFX0jwC8IhbOfGFyOggFgo8oTOS1uPLZhzUQ=',
        private_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server snowflake_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/v0.4.0/snowflake_fdw.wasm',
        fdw_package_name 'supabase:snowflake-fdw',
        fdw_package_version '0.1.0',
        account_identifier 'MYORGANIZATION-MYACCOUNT',
        user 'MYUSER',
        public_key_fingerprint 'SizgPofeFX0jwC8IhbOfGFyOggFgo8oTOS1uPLZhzUQ=',
        private_key E'-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----'
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

## Creating Foreign Tables

The Snowflake Wrapper supports data reads and writes from Snowflake.

| Integration | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| Snowflake   |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

For example:

```sql
create foreign table snowflake_mytable (
  id bigint,
  name text,
  num numeric,
  dt date,
  ts timestamp
)
  server snowflake_server
  options (
    table 'mydatabase.public.mytable',
    rowid_column 'id'
  );
```

### Foreign table options

The full list of foreign table options are below:

- `table` - Source table or view name in Snowflake, required.

  This can also be a subquery enclosed in parentheses, for example,

  ```sql
  table '(select * from mydatabase.public.mytable where id = 42)'
  ```

- `rowid_column` - Primary key column name, optional for data scan, required for data modify

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown.

## Examples

Some examples on how to use Snowflake foreign tables.

Let's prepare the source table in Snowflake first:

```sql
-- Run below SQLs on Snowflake to create source table
create table mydatabase.public.mytable (
  id number(38,0),
  name varchar(16777216),
  num number(38,6),
  dt date,
  ts timestamp_ntz(9)
);

-- Add some test data
insert into mydatabase.public.mytable(id, name, num, dt, ts)
values (42, 'foo', 12.34, '2024-05-18', '2024-05-18 12:34:56');
insert into mydatabase.public.mytable(id, name, num, dt, ts)
values (43, 'bar', 56.78, '2024-05-19', '2024-05-19 12:34:56');
```

### Basic example

This example will create a "foreign table" inside your Postgres database and query its data. First, we can create a schema to hold all the Snowflake foreign tables.

```sql
create schema snowflake;
```

Then create the foreign table and query it, for example:

```sql
create foreign table snowflake.mytable (
  id bigint,
  name text,
  num numeric,
  dt date,
  ts timestamp
)
  server snowflake_server
  options (
    table 'mydatabase.public.mytable',
    rowid_column 'id'
  );

select * from snowflake.mytable;
```

### Data modify example

This example will modify data in a "foreign table" inside your Postgres database, note that `rowid_column` option is mandatory for data modify:

```sql
-- insert new data
insert into snowflake.mytable (id, name, num, dt, ts)
values ('44', 'hello', 456.123, '2024-05-20', '2024-05-20 12:34:56');

-- update existing data
update snowflake.mytable
set name = 'new name', num = null, dt = '2024-01-01', ts = '2024-01-02 21:43:56'
where id = 42;

-- delete data
delete from snowflake.mytable
where id = 42;
```

