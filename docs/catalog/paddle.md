---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Paddle

[Paddle](https://developer.paddle.com/) is a merchant of record platform built for modern SaaS, mobile app, AI, and digital product businesses. It manages your payments, taxes, and subscriptions in a single integration.

The Paddle Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read and write data from Paddle within your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                                    | Checksum                                                           | Required Wrappers Version |
| ------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------- |
| 0.3.0   | `https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.3.0/paddle_fdw.wasm`     | `<checksum added when the v0.3.0 artifact is released>`            | >=0.5.0                   |
| 0.2.0   | `https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.2.0/paddle_fdw.wasm`     | `e788b29ae46c158643e1e1f229d94b28a9af8edbd3233f59c5a79053c25da213` | >=0.5.0                   |
| 0.1.1   | `https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.1.1/paddle_fdw.wasm`     | `c5ac70bb2eef33693787b7d4efce9a83cde8d4fa40889d2037403a51263ba657` | >=0.4.0                   |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.1.0/paddle_fdw.wasm`     | `7d0b902440ac2ef1af85d09807145247f14d1d8fd4d700227e5a4d84c8145409` | >=0.4.0                   |

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
-- Save your Paddle API key in Vault and retrieve the created `key_id`
select vault.create_secret(
  '<Paddle API key>', -- Paddle API key
  'paddle',
  'Paddle API key for Wrappers'
);
```

### Connecting to Paddle

We need to provide Postgres with the credentials to access Paddle, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server paddle_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.3.0/paddle_fdw.wasm',
        fdw_package_name 'supabase:paddle-fdw',
        fdw_package_version '0.3.0',
        fdw_package_checksum '<checksum added when the v0.3.0 artifact is released>',
        api_url 'https://sandbox-api.paddle.com', -- Use https://api.paddle.com for live account
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server paddle_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_paddle_fdw_v0.3.0/paddle_fdw.wasm',
        fdw_package_name 'supabase:paddle-fdw',
        fdw_package_version '0.3.0',
        fdw_package_checksum '<checksum added when the v0.3.0 artifact is released>',
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
| discount-groups       |
| customers             |
| subscriptions         |
| transactions          |
| adjustments           |
| reports               |
| notification-settings |
| notifications         |

- `rowid_column` - Primary key column name, optional for data scan, required for data modify

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from Paddle.

`import foreign schema` creates typed foreign tables for the following objects:
`customers`, `products`, `prices`, `subscriptions`, `transactions`, `discounts`,
and `adjustments`. Each table exposes the object's commonly-used fields as
columns plus a catch-all `attrs jsonb` column for everything else. The
`limit to` / `except` clauses are honored.

```sql
-- create all the foreign tables
import foreign schema paddle from server paddle_server into paddle;

-- or, create selected tables only
import foreign schema paddle
   limit to ("products", "customers")
   from server paddle_server into paddle;

-- or, create all foreign tables except selected tables
import foreign schema paddle
   except ("customers")
   from server paddle_server into paddle;
```

You can also create foreign tables for other objects (`discount-groups`,
`reports`, `notification-settings`, `notifications`) by hand, setting the
`object` table option accordingly.

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
  description text,
  type text,
  tax_category text,
  status text,
  image_url text,
  custom_data jsonb,
  created_at timestamptz,
  updated_at timestamptz,
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
- Pushed-down filters: `id`, `status`, `tax_category`, `type`

### Prices

This is an object representing Paddle Prices.

Ref: [Paddle API docs](https://developer.paddle.com/api-reference/about/data-types)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Prices |   ✅    |   ✅    |   ✅    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table paddle.prices (
  id text,
  product_id text,
  description text,
  type text,
  name text,
  tax_mode text,
  status text,
  custom_data jsonb,
  created_at timestamptz,
  updated_at timestamptz,
  attrs jsonb
)
  server paddle_server
  options (
    object 'prices',
    rowid_column 'id'
  );
```

#### Notes

- Requires `rowid_column` option for data modification operations
- Pushed-down filters: `id`, `product_id`, `status`, `type`
- Unit price can be extracted using: `attrs->'unit_price'`

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
  marketing_consent boolean,
  locale text,
  custom_data jsonb,
  created_at timestamptz,
  updated_at timestamptz,
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
- Pushed-down filters: `id`, `status`, `email`
- Custom data stored in dedicated `custom_data` column

### Subscriptions

This is an object representing Paddle Subscriptions.

Ref: [Paddle API docs](https://developer.paddle.com/api-reference/about/data-types)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| Subscriptions |   ✅    |   ❌    |   ✅    |   ❌    |    ❌     |

Paddle has no create-subscription endpoint — subscriptions are created through
checkout or transactions — so `insert` is not supported.

#### Usage

```sql
create foreign table paddle.subscriptions (
  id text,
  status text,
  customer_id text,
  address_id text,
  business_id text,
  currency_code text,
  collection_mode text,
  scheduled_change_action text,
  started_at timestamptz,
  first_billed_at timestamptz,
  next_billed_at timestamptz,
  paused_at timestamptz,
  canceled_at timestamptz,
  created_at timestamptz,
  updated_at timestamptz,
  custom_data jsonb,
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
- Pushed-down filters: `id`, `customer_id`, `status`, `address_id`, `collection_mode`, `scheduled_change_action`
- `scheduled_change_action` reflects the nested `scheduled_change.action` and is `none` when the subscription has no pending change, so you can filter by subscriptions scheduled to `cancel`, `pause`, `resume`, or `none`
- Filtering by price is not supported on subscriptions (a subscription can have multiple items/prices); subscription items and their prices can be extracted from `attrs->'items'`

### Transactions

This is an object representing Paddle Transactions.

Ref: [Paddle API docs](https://developer.paddle.com/api-reference/about/data-types)

#### Operations

| Object       | Select | Insert | Update | Delete | Truncate |
| ------------ | :----: | :----: | :----: | :----: | :------: |
| Transactions |   ✅    |   ✅    |   ✅    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table paddle.transactions (
  id text,
  status text,
  customer_id text,
  address_id text,
  business_id text,
  subscription_id text,
  invoice_id text,
  invoice_number text,
  collection_mode text,
  discount_id text,
  origin text,
  currency_code text,
  custom_data jsonb,
  billed_at timestamptz,
  revised_at timestamptz,
  created_at timestamptz,
  updated_at timestamptz,
  attrs jsonb
)
  server paddle_server
  options (
    object 'transactions',
    rowid_column 'id'
  );
```

#### Notes

- Requires `rowid_column` option for data modification operations
- Pushed-down filters: `id`, `customer_id`, `subscription_id`, `status`, `collection_mode`, `origin`, `invoice_number`
- Date-range pushdown is supported for `created_at`, `updated_at`, and `billed_at` using `<`, `<=`, `>`, `>=` operators (see [Query Pushdown Support](#query-pushdown-support))
- Line items and totals can be extracted using: `attrs->'details'`

### Adjustments

This is an object representing Paddle Adjustments (refunds, credits, and chargebacks).

Ref: [Paddle API docs](https://developer.paddle.com/api-reference/about/data-types)

#### Operations

| Object      | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| Adjustments |   ✅    |   ✅    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table paddle.adjustments (
  id text,
  action text,
  type text,
  transaction_id text,
  subscription_id text,
  customer_id text,
  reason text,
  currency_code text,
  status text,
  credit_applied_to_balance boolean,
  created_at timestamptz,
  updated_at timestamptz,
  attrs jsonb
)
  server paddle_server
  options (
    object 'adjustments',
    rowid_column 'id'
  );
```

#### Notes

- Requires `rowid_column` option for data modification operations
- `id` is pushed down as a list filter (Paddle has no single-object adjustment endpoint)
- Pushed-down filters: `id`, `customer_id`, `subscription_id`, `transaction_id`, `status`, `action`
- Adjustment items and totals can be extracted using: `attrs->'totals'`

### Discounts

This is an object representing Paddle Discounts.

Ref: [Paddle API docs](https://developer.paddle.com/api-reference/about/data-types)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| Discounts |   ✅    |   ✅    |   ✅    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table paddle.discounts (
  id text,
  status text,
  description text,
  code text,
  type text,
  mode text,
  amount text,
  currency_code text,
  recur boolean,
  times_used integer,
  enabled_for_checkout boolean,
  discount_group_id text,
  expires_at timestamptz,
  created_at timestamptz,
  updated_at timestamptz,
  custom_data jsonb,
  attrs jsonb
)
  server paddle_server
  options (
    object 'discounts',
    rowid_column 'id'
  );
```

#### Notes

- Requires `rowid_column` option for data modification operations
- Pushed-down filters: `id`, `code`, `status`, `mode`, `discount_group_id`

## Query Pushdown Support

This FDW pushes `where` clause filters down to the Paddle API so that filtering
happens at the source instead of after a full table scan. Two kinds of pushdown
are supported:

**Single-object lookup.** For objects that expose a `GET /{object}/{id}`
endpoint, `where id = '...'` is turned into a direct single-object request:

```sql
select * from paddle.customers where id = 'ctm_01hymwgpkx639a6mkvg99563sp';
```

**List filters.** Equality filters on supported columns are pushed down as query
parameters (see each entity's Notes for the exact list). Multiple filters are
combined, and `in (...)` lists are pushed as comma-separated values:

```sql
-- server-side filtered instead of a full scan
select * from paddle.transactions
where customer_id = 'ctm_01hymwgpkx639a6mkvg99563sp'
  and status = 'completed';

select * from paddle.subscriptions where status = 'active';
```

**Date-range filters (transactions only).** Paddle supports range operators on
the transactions list endpoint, so `<`, `<=`, `>`, `>=` on `created_at`,
`updated_at`, and `billed_at` are pushed down as `[LT]`/`[LTE]`/`[GT]`/`[GTE]`
filters:

```sql
select * from paddle.transactions
where created_at >= '2024-01-01' and created_at < '2024-02-01';
```

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

## Limitations

This section describes important limitations and considerations when using this FDW:

- Query pushdown is supported for `id` and the per-object filters listed in each entity's Notes; filtering on other columns falls back to a full table scan
- Date-range pushdown (`<`, `<=`, `>`, `>=`) is only available for `transactions` (`created_at`, `updated_at`, `billed_at`)
- Paddle's list endpoints for `products`, `prices`, `customers`, and `discounts` default to returning only `active` (non-archived) entities. Unless you filter on `status` explicitly, archived rows are not returned — keep this in mind for reconciliation, and add `where status = 'archived'` to see archived entities
- Large result sets may experience slower performance due to full data transfer requirement
- This is a read-mostly wrapper that queries Paddle over HTTP on demand; it is not a replacement for webhook-driven fulfilment. Do not put a live FDW query on a latency-critical path (e.g. per-request access checks) — use it for reporting, reconciliation, and backfill instead
- Materialized views using these foreign tables may fail during logical backups

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
  created_at timestamptz,
  updated_at timestamptz,
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
  created_at timestamptz,
  updated_at timestamptz,
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
  customer_id text,
  created_at timestamptz,
  updated_at timestamptz,
  attrs jsonb
)
  server paddle_server
  options (
    object 'subscriptions',
    rowid_column 'id'
  );

-- extract the subscription items array (items is a JSON array, not an object)
select id, attrs->'items' as items
from paddle.subscriptions where id = 'sub_01hv959anj4zrw503h2acawb3p';
```

### Reconciliation and Reporting

Because filters are pushed down to Paddle, the wrapper works well for
reporting and for reconciling Paddle against your own tables — for example, a
periodic job that pulls only recently-changed transactions, or a per-customer
subscription lookup. Assumes the foreign tables were created via
`import foreign schema` (see [Entities](#entities)).

```sql
-- incremental pull: only transactions updated since the last sync
-- (updated_at range is pushed down, so this is a targeted request, not a full scan)
select id, status, customer_id, currency_code, billed_at, updated_at
from paddle.transactions
where updated_at >= '2024-06-01T00:00:00';

-- a customer's active subscriptions (customer_id + status pushed down)
select id, status, next_billed_at
from paddle.subscriptions
where customer_id = 'ctm_01hymwgpkx639a6mkvg99563sp'
  and status = 'active';

-- refunds and chargebacks for reporting (adjustments)
select id, action, transaction_id, customer_id, currency_code, created_at
from paddle.adjustments
where action = 'refund';
```

For real-time provisioning (granting/revoking access as subscriptions change),
use Paddle webhooks to keep your own tables up to date, and use this wrapper for
the reporting and reconciliation queries above. See Paddle's
[Provision access and handle subscription state](https://developer.paddle.com/build/subscriptions/provision-access-webhooks)
guide.

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
