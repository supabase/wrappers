---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Orb

[Orb](https://withorb.com/) is a metering and pricing platform built to support usage-based billing models.

The Orb Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read data from Orb for use within your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                                    | Checksum                                                           |
| ------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_orb_fdw_v0.1.0/orb_fdw.wasm` | `tbd` |

## Preparation

Before you can query Orb, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Orb Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Orb API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'orb',
  '<Orb API key>' -- Orb API key 
)
returning key_id;
```

### Connecting to Orb

We need to provide Postgres with the credentials to access Orb and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server orb_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_orb_fdw_v0.1.0/orb_fdw.wasm',
        fdw_package_name 'supabase:orb-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
        api_url 'https://api.withorb.com/v1',  -- optional
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server orb_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_orb_fdw_v0.1.0/orb_fdw.wasm',
        fdw_package_name 'supabase:orb-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
        api_url 'https://api.withorb.com/v1',  -- optional
        api_key '3e2e912...'  -- Orb API key
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists orb;
```

## Options

The full list of foreign table options are below:

- `object` - Object name in Orb, required.

Supported objects are listed below:

| Object name              |
| ------------------------ |
| alerts                   |
| coupons                  |
| credit_notes             |
| customers                |
| credits                  |
| credits_ledger           |
| dimensional_price_groups |
| events_backfills         |
| events_volume            |
| invoices                 |
| items                    |
| metrics                  |
| plans                    |
| prices                   |
| subscriptions            |

- `rowid_column` - Primary key column name, optional for data scan, required for data modify

## Entities

### Alert

This is a list of all alerts within Orb.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/alert/list-alerts)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| alerts                |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.alerts (
  id text,
  type text,
  enabled boolean,
  customer_id text,
  external_customer_id text,
  subscription_id text,
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'alerts'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- The query must specify one of `customer_id`, `external_customer_id`, or `subscription_id`

### Coupon

This is a list of all coupons for an account.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/coupon/list-coupons)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| coupons               |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.coupons (
  id text,
  redemption_code text,
  times_redeemed bigint,
  duration_in_months bigint,
  archived_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'coupons'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Credit Note

This is a list of all CreditNotes.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/credit-note/list-credit-notes)

#### Operations

| Object       | Select | Insert | Update | Delete | Truncate |
| ------------ | :----: | :----: | :----: | :----: | :------: |
| credit_notes |   ✅   |   ✅   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.credit_notes (
  id text,
  type text,
  total numeric(18,2),
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'credit_notes',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Customer

This is a list of all customers for an account.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/customer/list-customers)

#### Operations

| Object      | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| customers   |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table orb.customers (
  id text,
  name text,
  email text,
  created_at timestamp,
  auto_collection boolean,
  attrs jsonb
)
  server orb_server
  options (
    object 'customers',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Credit

This is a list of unexpired, non-zero credit blocks for a customer.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/credit/fetch-customer-credit-balance-by-external-customer-id)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| credits       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.credits (
  id text,
  customer_id text,
  external_customer_id text,
  balance numeric(18,2),
  status text,
  effective_date timestamp,
  expiry_date timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'credits'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- The query must specify one of `customer_id` or `external_customer_id`

### Credits ledger

This is a list of actions that have taken place to modify a customer’s credit balance.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/credit/fetch-customer-credits-ledger-by-external-id)

#### Operations

| Object             | Select | Insert | Update | Delete | Truncate |
| ------------------ | :----: | :----: | :----: | :----: | :------: |
| credits/ledger     |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.credits_ledger (
  id text,
  customer_id text,
  external_customer_id text,
  amount numeric(18,2),
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'credits/ledger'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- The query must specify one of `customer_id` or `external_customer_id`

### Dimensional Price Group

This is a list of dimensional price groups.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/dimensional-price-group/list-dimensional-price-groups)

#### Operations

| Object                   | Select | Insert | Update | Delete | Truncate |
| ------------------------ | :----: | :----: | :----: | :----: | :------: |
| dimensional_price_groups |   ✅   |   ✅   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.dimensional_price_groups (
  id text,
  name text,
  attrs jsonb
)
  server orb_server
  options (
    object 'dimensional_price_groups',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Event Backfill

This is a list of all event backfills.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/event/list-backfills)

#### Operations

| Object           | Select | Insert | Update | Delete | Truncate |
| ---------------- | :----: | :----: | :----: | :----: | :------: |
| events/backfills |   ✅   |   ✅   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.events_backfills (
  id text,
  status text,
  events_ingested bigint,
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'events/backfills',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Event Volume

This returns the event volume for an account.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/event/get-event-volume)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| events/volume |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.events_volume (
  count bigint,
  timeframe_start timestamp,
  timeframe_end timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'events/volume'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Invoice

This is a list of invoices for an account.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/invoice/list-invoices)

#### Operations

| Object   | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| invoices |   ✅   |   ✅   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.invoices (
  id text,
  invoice_number text,
  customer_id text,
  external_customer_id text,
  subscription_id text,
  status text,
  amount_due numeric(18,2),
  currency text,
  due_date timestamp,
  issued_at timestamp,
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'invoices',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Item

This is a list of all items.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/item/list-items)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| items  |   ✅   |   ✅   |   ✅   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.items (
  id text,
  name text,
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'items',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Metric

This is a list of metric details.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/metric/list-metrics)

#### Operations

| Object  | Select | Insert | Update | Delete | Truncate |
| ------- | :----: | :----: | :----: | :----: | :------: |
| metrics |   ✅   |   ✅   |   ✅   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.metrics (
  id text,
  name text,
  description text,
  status text,
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'metrics',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Plan

This is a list of all plans for an account.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/plan/list-plans)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| plans  |   ✅   |   ✅   |   ✅   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.plans (
  id text,
  name text,
  description text,
  status text,
  maximum_amount numeric(18,2),
  minimum_amount numeric(18,2),
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'plans',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Price

This is a list of all add-on prices.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/price/list-prices)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| prices |   ✅   |   ✅   |   ✅   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.prices (
  id text,
  name text,
  external_price_id text,
  price_type text,
  maximum_amount numeric(18,2),
  minimum_amount numeric(18,2),
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'prices',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Subscription

This is a list of all subscriptions for an account.

Ref: [Orb API docs](https://docs.withorb.com/api-reference/subscription/list-subscriptions)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| subscriptions |   ✅   |   ✅   |   ✅   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table orb.subscriptions (
  id text,
  customer_id text,
  external_customer_id text,
  billing_cycle_day bigint,
  status text,
  start_date timestamp,
  end_date timestamp,
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'subscriptions',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

## Query Pushdown Support

### `where` clause pushdown

This FDW supports `where id = 'xxx'` clause pushdown for below objects:

- Coupon
- Credit Note
- Customer
- Dimensional Price Group
- Event Backfill
- Invoice
- Item
- Metric
- Plan
- Price
- Subscription

Some other supported `where` clauses pushdown are listed below:

#### Alert

For example, `where customer_id = 'WmUkxWmvLvvXHaNV'`.

- customer_id, (operations: `=`)
- external_customer_id, (operations: `=`)
- subscription_id, (operations: `=`)

#### Customer

For example, `where created_at >= '2025-02-15T10:25:36'`.

- created_at, (operations: `<`, `<=`, `>`, `>=`)

#### Event Volume

For example, `where timeframe_start = '2025-02-15'`.

- timeframe_start, (operations: `=`)

#### Invoice

For example, `where status = 'paid'`.

- customer_id, (operations: `=`)
- external_customer_id, (operations: `=`)
- subscription_id, (operations: `=`)
- status, (operations: `=`)
- due_date, (operations: `=`, `<`, `>`)
- created_at, (operations: `<`, `<=`, `>`, `>=`)

#### Subscription

For example, `where status = 'active'`.

- customer_id, (operations: `=`)
- external_customer_id, (operations: `=`)
- status, (operations: `=`)
- created_at, (operations: `<`, `<=`, `>`, `>=`)

### `limit` clause pushdown

This FDW supports `limit` clause pushdown for all the objects. For example,

```sql
select * from orb.customers limit 200;
```

## Supported Data Types

| Postgres Data Type | Orb Data Type      |
| ------------------ | ------------------ |
| boolean            | Boolean            |
| bigint             | Number             |
| numeric            | Number             |
| text               | String             |
| timestamp          | Time               |
| jsonb              | Json               |

The Orb API uses JSON formatted data, please refer to [Orb API docs](https://docs.withorb.com/api-reference) for more details.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Large result sets may experience slower performance due to full data transfer requirement
- Materialized views using these foreign tables may fail during logical backups

## Examples

Below are some examples on how to use Orb foreign tables.

### Basic example

This example will create a "foreign table" inside your Postgres database and query its data.

```sql
create foreign table orb.customers (
  id text,
  name text,
  email text,
  created_at timestamp,
  auto_collection boolean,
  attrs jsonb
)
  server orb_server
  options (
    object 'customers',
    rowid_column 'id'
  );

-- query all customers
select * from orb.customers;

-- you can use `limit` clause to reduce query time if customer number is large
select * from orb.customers limit 200;
```

`attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Query JSON attributes

```sql
create foreign table orb.invoices (
  id text,
  invoice_number text,
  customer_id text,
  external_customer_id text,
  subscription_id text,
  status text,
  amount_due numeric(18,2),
  currency text,
  due_date timestamp,
  issued_at timestamp,
  created_at timestamp,
  attrs jsonb
)
  server orb_server
  options (
    object 'invoices',
    rowid_column 'id'
  );

-- extract all line items from an invoice
select
  i.id,
  li->>'name' as line_item_name,
  li->>'quantity' as line_item_quantity,
  li->>'subtotal' as line_item_subtotal
from orb.invoices i
  cross join json_array_elements((attrs->'line_items')::json) li
where
  i.id = 'PsEhbLd88auyhZ8F';
```

### Data Modify Example

This example will modify data in a "foreign table" inside your Postgres database, note that `rowid_column` foreign table option is mandatory for data modify. Data modify is done through the `attrs` jsonb column, which will be posted as request body to Orb API endpoint. Please refer to [Orb API reference docs](https://docs.withorb.com/api-reference) for the JSON request details.

```sql
-- create a new customer
insert into orb.customers(attrs)
values (
  '{
    "name": "John Doe",
    "email": "test@test.com"
  }'::jsonb
);

-- update the existing customer
update orb.customers
set attrs = '{
  "name": "Jane Smith",
  "billing_address": {
    "city": "New York",
    "country": "US"
  }
}'::jsonb
where id = 'n6DaYELQYubChJWf';

-- delete a customer
delete from orb.customers
where id = 'n6DaYELQYubChJWf';
```
