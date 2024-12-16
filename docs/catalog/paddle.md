---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Paddle

[Paddle](https://www.paddle.com) is a merchant of record that acts to provide a payment infrastructure to thousands of software companies around the world.

The Paddle Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read and write data from Paddle within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Supported Data Types

| Postgres Data Type | Paddle Data Type |
| ------------------ | ---------------- |
| boolean            | Boolean          |
| smallint           | Money            |
| integer            | Money            |
| bigint             | Money            |
| real               | Money            |
| double precision   | Money            |
| numeric            | Money            |
| text               | Text             |
| date               | Dates and time   |
| timestamp          | Dates and time   |
| timestamptz        | Dates and time   |

The Paddle API uses JSON formatted data, please refer to [Paddle docs](https://developer.paddle.com/api-reference/about/data-types) for more details.

## Available Versions

| Version | Wasm Package URL                                                                                | Checksum                                                           |
| ------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.1.1   | `https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.1.1/paddle_fdw.wasm` | `c5ac70bb2eef33693787b7d4efce9a83cde8d4fa40889d2037403a51263ba657` |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.1.0/paddle_fdw.wasm` | `7d0b902440ac2ef1af85d09807145247f14d1d8fd4d700227e5a4d84c8145409` |

## Preparation

Before you can query Paddle, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Paddle Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Paddle API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'paddle',
  '<Paddle API key>' -- Paddle API key
)
returning key_id;
```

### Connecting to Paddle

We need to provide Postgres with the credentials to access Paddle, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server paddle_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.1.1/paddle_fdw.wasm',
        fdw_package_name 'supabase:paddle-fdw',
        fdw_package_version '0.1.1',
        fdw_package_checksum 'c5ac70bb2eef33693787b7d4efce9a83cde8d4fa40889d2037403a51263ba657',
        api_url 'https://sandbox-api.paddle.com', -- Use https://api.paddle.com for live account
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server paddle_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.1.1/paddle_fdw.wasm',
        fdw_package_name 'supabase:paddle-fdw',
        fdw_package_version '0.1.1',
        fdw_package_checksum 'c5ac70bb2eef33693787b7d4efce9a83cde8d4fa40889d2037403a51263ba657',
        api_url 'https://sandbox-api.paddle.com', -- Use https://api.paddle.com for live account
        api_key 'bb4e69088ea07a98a90565ac610c63654423f8f1e2d48b39b5'
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists paddle;
```

## Options

The full list of foreign table options are below:

- `object` - Object name in Paddle, required.

Supported objects are listed below:

| Object                |
| --------------------- |
| products              |
| prices                |
| discounts             |
| customers             |
| transactions          |
| reports               |
| notification-settings |
| notifications         |

- `rowid_column` - Primary key column name, optional for data scan, required for data modify

## Entities

### Products

This is an object representing Paddle Products.

Ref: [Paddle API docs](https://developer.paddle.com/api-reference/about/data-types)

#### Operations

| Object   | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| Products |   ✅    |   ✅    |   ✅    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table paddle.products (
  id text,
  name text,
  tax_category text,
  status text,
  description text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server paddle_server
  options (
    object 'products',
    rowid_column 'id'
  );
```

#### Notes

- Requires `rowid_column` option for data modification operations
- Query pushdown supported for `id` column
- Product type can be extracted using: `attrs->>'type'`

### Customers

This is an object representing Paddle Customers.

Ref: [Paddle API docs](https://developer.paddle.com/api-reference/about/data-types)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| Customers |   ✅    |   ✅    |   ✅    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table paddle.customers (
  id text,
  name text,
  email text,
  status text,
  custom_data jsonb,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server paddle_server
  options (
    object 'customers',
    rowid_column 'id'
  );
```

#### Notes

- Requires `rowid_column` option for data modification operations
- Query pushdown supported for `id` column
- Custom data stored in dedicated `custom_data` column

### Subscriptions

This is an object representing Paddle Subscriptions.

Ref: [Paddle API docs](https://developer.paddle.com/api-reference/about/data-types)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| Subscriptions |   ✅    |   ✅    |   ✅    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table paddle.subscriptions (
  id text,
  status text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server paddle_server
  options (
    object 'subscriptions',
    rowid_column 'id'
  );
```

#### Notes

- Requires `rowid_column` option for data modification operations
- Query pushdown supported for `id` column
- Subscription items status can be extracted using: `attrs#>'{items,status}'`

## Query Pushdown Support

This FDW supports `where` clause pushdown with `id` as the filter. For example,

```sql
select * from paddle.customers where id = 'ctm_01hymwgpkx639a6mkvg99563sp';
```

## Examples

### Basic Example

This example will create a "foreign table" inside your Postgres database and query its data.

```sql
create foreign table paddle.customers (
  id text,
  name text,
  email text,
  status text,
  custom_data jsonb,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server paddle_server
  options (
    object 'customers',
    rowid_column 'id'
  );

select * from paddle.customers;
```

`attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed or its associated sub objects from it. See more examples below.

### Query JSON Attributes

```sql
create foreign table paddle.products (
  id text,
  name text,
  tax_category text,
  status text,
  description text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server paddle_server
  options (
    object 'products',
    rowid_column 'id'
  );

-- extract product type for a product
select id, attrs->>'type' as type
from paddle.products where id = 'pro_01hymwj50rfavry9kqsf2vk6sy';

create foreign table paddle.subscriptions (
  id text,
  status text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server paddle_server
  options (
    object 'subscriptions',
    rowid_column 'id'
  );

-- extract subscription items for a subscription
select id, attrs#>'{items,status}' as item_status
from paddle.subscriptions where id = 'sub_01hv959anj4zrw503h2acawb3p';
```

### Data Modify Example

This example will modify data in a "foreign table" inside your Postgres database, note that `rowid_column` option is mandatory for data modify:

```sql
-- insert new data
insert into paddle.products(name, tax_category)
values ('my prod', 'standard');

-- update existing data
update paddle.products
set name = 'my prod'
where id = 'pro_01hzrr95qz1g0cys1f9sgj4t3h';
```
