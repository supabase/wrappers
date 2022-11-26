# Stripe Foreign Data Wrapper

This is a foreign data wrapper for [Stripe](https://stripe.com/). It is developed using [Wrappers](https://github.com/supabase/wrappers) and only supports `balance` and `customers` at this moment.

## Basic usage

These steps outline how to use the Stripe FDW:

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
  handler wrappers_handler
  validator wrappers_validator
  options (
    wrapper 'StripeFdw'
  );

-- save Stripe API key in Vault and get its key id
select pgsodium.create_key(name := 'stripe');
insert into vault.secrets (secret, key_id) values (
  'sk_test_xxx',
  (select id from pgsodium.valid_key where name = 'stripe')
) returning key_id;

-- create a wrappers Stripe server and specify connection info
do $$
declare
  csid text;
begin
  select id into csid from pgsodium.valid_key where name = 'stripe' limit 1;

  drop server if exists my_stripe_server cascade;

  execute format(
    E'create server my_stripe_server \n'
    '   foreign data wrapper stripe_wrapper \n'
    '   options ( \n'
    '     api_url ''https://api.stripe.com/v1'', \n'  -- Stripe API base URL, optional
    '     api_key_id ''%s'' \n'  -- the API Key ID saved in Vault, required
    ' );',
    csid
  );
end $$;

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
  
drop foreign table if exists subscriptions;
create foreign table subscriptions (
  customer_id text,
  currency text,
  current_period_start bigint,
  current_period_end bigint
)
  server my_stripe_server
  options (
    object 'subscriptions'    -- source object in stripe, required
  );
```

4. Run some queries to check if it is working:

On Postgres:

```sql
select * from balance;
select * from customers;
select * from subscriptions;
```

