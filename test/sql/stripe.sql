-- The clickhouse healthcheck lies
select pg_sleep(3);
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

-- create a wrappers Stripe server and specify connection info
drop server if exists my_stripe_server cascade;
create server my_stripe_server
  foreign data wrapper stripe_wrapper
  options (
    api_url 'https://api.stripe.com/v1',  -- Stripe API base URL, optional
    api_key 'sk_test_key'  -- Stripe API Key, required
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

select * from balance;
select * from customers;
select * from subscriptions;
