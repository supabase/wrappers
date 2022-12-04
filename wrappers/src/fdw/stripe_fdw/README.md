# Stripe Foreign Data Wrapper

This is a foreign data wrapper for [Stripe](https://stripe.com/) developed using [Wrappers](https://github.com/supabase/wrappers).

This FDW currently supports reading below objects from Stripe:

1. [Balance](https://stripe.com/docs/api/balance)
2. [Balance Transactions](https://stripe.com/docs/api/balance_transactions/list)
3. [Charges](https://stripe.com/docs/api/charges/list)
4. [Customers](https://stripe.com/docs/api/customers/list)
5. [Invoices](https://stripe.com/docs/api/invoices/list)
6. [PaymentIntents](https://stripe.com/docs/api/payment_intents/list)
7. [Products](https://stripe.com/docs/api/products/list)
8. [Subscriptions](https://stripe.com/docs/api/subscriptions/list)

## Installation

This FDW requires [pgx](https://github.com/tcdi/pgx), please refer to its installtion page to install it first.

After `pgx` installed, run below command to install this FDW.

```bash
cargo pgx install --pg-config [path_to_pg_config] --features stripe_fdw
```

If you are using [Supabase](https://www.supabase.com), this FDW is already included in `Wrappers` extension, go to https://app.supabase.com/project/_/database/extensions to enable it.

## Basic usage

These steps outline how to use the Stripe FDW locally:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgx with feature:

```bash
cd wrappers/wrappers
cargo pgx run --features stripe_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
drop extension if exists wrappers cascade;
create extension wrappers;

-- create foreign data wrapper and enable 'StripeFdw'
drop foreign data wrapper if exists stripe_wrapper cascade;
create foreign data wrapper stripe_wrapper
  handler stripe_fdw_handler;

-- Below we're using the API key stored in Vault, if you don't want
-- to use Vault, you can directly specify the API key in `api_key`
-- option but it is less secure. For example,
--
-- create server my_stripe_server
--   foreign data wrapper stripe_wrapper
--   options (
--     api_key 'sk_test_xxx'
--   );

-- save Stripe API key in Vault and get its key id
select pgsodium.create_key(name := 'stripe');
insert into vault.secrets (secret, key_id) values (
  'sk_test_xxx',
  (select id from pgsodium.valid_key where name = 'stripe')
) returning key_id;

-- create Stripe server and specify connection info.
do $$
declare
  key_id text;
begin
  select id into key_id from pgsodium.valid_key where name = 'stripe' limit 1;

  drop server if exists my_stripe_server cascade;

  execute format(
    E'create server my_stripe_server \n'
    '   foreign data wrapper stripe_wrapper \n'
    '   options ( \n'
    '     api_key_id ''%s'' \n'  -- the API Key ID saved in Vault, required
    ' );',
    key_id
  );
end $$;

-- create foreign tables
drop foreign table if exists balance;
create foreign table balance (
  amount bigint,
  currency text,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'balance'
  );

drop foreign table if exists balance_transactions;
create foreign table balance_transactions (
  id text,
  amount bigint,
  currency text,
  description text,
  fee bigint,
  net bigint,
  status text,
  type text,
  created timestamp,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'balance_transactions'
  );

drop foreign table if exists charges;
create foreign table charges (
  id text,
  amount bigint,
  currency text,
  customer text,
  description text,
  invoice text,
  payment_intent text,
  status text,
  created timestamp,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'charges'
  );

drop foreign table if exists customers;
create foreign table customers (
  id text,
  email text,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'customers'
  );
  
drop foreign table if exists invoices;
create foreign table invoices (
  id text,
  customer text,
  subscription text,
  status text,
  total bigint,
  currency text,
  period_start timestamp,
  period_end timestamp,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'invoices'
  );

drop foreign table if exists payment_intents;
create foreign table payment_intents (
  id text,
  customer text,
  amount bigint,
  currency text,
  payment_method text,
  created timestamp,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'payment_intents'
  );

drop foreign table if exists products;
create foreign table products (
  id text,
  name text,
  active bool,
  default_price text,
  description text,
  created timestamp,
  updated timestamp,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'products'
  );

drop foreign table if exists subscriptions;
create foreign table subscriptions (
  id text,
  customer text,
  currency text,
  current_period_start timestamp,
  current_period_end timestamp,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'subscriptions'
  );
```

4. Run some queries to check if it is working:

On Postgres:

```sql
-- use 'limit' to reduce data fetched from Stripe
select * from balance limit 10;
select * from customers limit 10;
select * from invoices limit 10;
select * from subscriptions limit 10;

-- 'attrs' is a common column which stores all the object attributes in JSON
-- format, you can extract any attributes needed or its associated sub objects.
-- For example,

-- extract account name for an invoice
select id, attrs->>'account_name' as account_name
from invoices where id = 'in_xxx';

-- extract invoice line items for an invoice
select id, attrs#>'{lines,data}' as line_items
from invoices where id = 'in_xxx';

-- extract subscription items for a subscription
select id, attrs#>'{items,data}' as items
from subscriptions where id = 'sub_xxx';
```

## Configurations

### Server options

Below are the options can be used in `CREATE SERVER`:

1. `api_key` - Stripe API key, required if `api_key_id` not specified
2. `api_key_id` - Stripe API key stored in Vault, required if `api_key` not specified
3. `api_url` - Stripe API url, optional, default is https://api.stripe.com/v1

### Foreign table options

Below are the options can be used in `CREATE FOREIGN TABLE`:

1. `object`, required

   Can be one of the following values:

   - balance
   - balance_transactions
   - charges
   - customers
   - invoices
   - payment_intents
   - products
   - subscriptions

## Limitations

- `WHERE` pushdown support is limited.

  You can use WHERE clause to reduce query result for below objects and fields.  For example,

  ```sql
  select * from customers where email = 'someone@foo.com';
  ```

  - balance_transactions: `payout`, `type`
  - charges: `customer`
  - customers: `email`
  - invoices: `customer`, `status`, `subscription`
  - payment_intents: `customer`
  - products: `active`
  - subscriptions: `customer`, `price`, `status`

- `LIMIT` pushdown support is limited, `limit` parameter used in Stripe API call is always 100.

- `ORDER BY` pushdown is not supported.

## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.2   | 2022-12-04 | Added 'products' objects support                     |
| 0.1.1   | 2022-12-03 | Added quals pushdown support                         |
| 0.1.0   | 2022-12-01 | Initial version                                      |
