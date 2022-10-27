# Stripe Foreign Data Wrapper

This is a foreign data wrapper for [Stripe](https://stripe.com/). It is developed using [Supabase Remote](https://github.com/supabase/remote) and only supports `balance` and `customers` at this moment.

## Basic usage

These steps outline how to use the Stripe FDW:

1. Clone this repo

```
git clone https://github.com/supabase/remote.git
```

2. Run it using pgx with feature:

```bash
cargo pgx run --features stripe_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
drop extension remote cascade;
create extension remote;

-- create foreign data wrapper and enable 'stripe_fdw'
drop foreign data wrapper if exists remote_stripe cascade;
create foreign data wrapper remote_stripe
  handler remote_handler
  validator remote_validator
  options (
    wrapper 'stripe_fdw'
  );

-- create a remote Stripe server and specify connection info
drop server if exists my_stripe_server cascade;
create server my_stripe_server
  foreign data wrapper remote_stripe
  options (
    api_key 'sk_test_key'
  );

-- create an example foreign table
drop foreign table if exists balance;
create foreign table balance (
  amount bigint,
  currency text
)
  server my_stripe_server
  options (
    object 'balance'    -- source object in stripe, required
  );

drop foreign table if exists customers;
create foreign table customers (
  id text,
  email text
)
  server my_stripe_server
  options (
    object 'customers'    -- source object in stripe, required
  );
```

4. Run some queries to check if it is working:

On Postgres:

```sql
select * from balance;
select * from customers;
```


