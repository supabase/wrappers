[Stripe](https://stripe.com) is an API driven online payment processing utilty. `supabase/wrappers` exposes below endpoints.

1. [Balance](https://stripe.com/docs/api/balance) (*read only*)
2. [Balance Transactions](https://stripe.com/docs/api/balance_transactions/list) (*read only*)
3. [Charges](https://stripe.com/docs/api/charges/list) (*read only*)
4. [Customers](https://stripe.com/docs/api/customers/list) (*read and modify*)
5. [Invoices](https://stripe.com/docs/api/invoices/list) (*read only*)
6. [PaymentIntents](https://stripe.com/docs/api/payment_intents/list) (*read only*)
7. [Products](https://stripe.com/docs/api/products/list) (*read and modify*)
8. [Subscriptions](https://stripe.com/docs/api/subscriptions/list) (*read and modify*)

### Wrapper 
To get started with the Stripe wrapper, create a foreign data wrapper specifying `handler` and `validator` as below.

```sql
create extension if not exists wrappers;

create foreign data wrapper stripe_wrapper
  handler stripe_fdw_handler
  validator stripe_fdw_validator;
```

### Server 

Next, we need to create a server for the FDW to hold options and credentials.

#### Auth (Supabase)

If you are using the Supabase platform, this is the recommended approach for securing your [Stripe API key](https://stripe.com/docs/keys).

Create a secure key using pgsodium
```sql
select pgsodium.create_key(name := 'stripe');
```

Save your Stripe API key in Vault and retrieve the `key_id`
```sql
insert into vault.secrets (secret, key_id)
values (
  'sk_test_xxx',
  (select id from pgsodium.valid_key where name = 'stripe')
)
returning
	key_id;
```

Create the foreign server
```sql
create server stripe_server
  foreign data wrapper stripe_wrapper
  options (
    api_key_id '<your key_id from above>'
  );
```

#### Auth (Insecure)

If the platform you are using does not support `pgsodium` and `Vault` you can create a server by storing your [Stripe API key](https://stripe.com/docs/keys) directly.


!!! important

    Credentials stored using this method can be viewed as plain text by anyone with access to `pg_catalog.pg_foreign_server`

```sql
create server stripe_server
   foreign data wrapper stripe_wrapper
   options (
     api_key 'sk_test_xxx'
   );
```

### Tables

The Stripe tables mirror Stripe's API.


(Optional) Create a schema to hold the Stripe tables.
```sql
create schema stripe;
```

##### Balance
*read only*

Shows the balance currently on your Stripe account.

Ref: [Stripe docs](https://stripe.com/docs/api/balance)

```sql
create foreign table stripe.balance (
  balance_type text,
  amount bigint,
  currency text,
  attrs jsonb
)
  server stripe_server
  options (
    object 'balance'
  );
```


##### Balance Transactions
*read only*

Balance transactions represent funds moving through your Stripe account. They're created for every type of transaction that comes into or flows out of your Stripe account balance.

Ref: [Stripe docs](https://stripe.com/docs/api/balance_transactions/list)

```sql
create foreign table stripe.balance_transactions (
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
  server stripe_server
  options (
    object 'balance_transactions'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- type 

##### Charges
*read only*

To charge a credit or a debit card, you create a Charge object. You can retrieve and refund individual charges as well as list all charges. Charges are identified by a unique, random ID.

Ref: [Stripe docs](https://stripe.com/docs/api/charges/list)

```sql
create foreign table stripe.charges (
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
  server stripe_server
  options (
    object 'charges'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- customer

##### Customers
*read and modify*

Contains customers known to Stripe.

Ref: [Stripe docs](https://stripe.com/docs/api/customers/list) 

```sql
create foreign table stripe.customers (
  id text,
  email text,
  name text,
  description text,
  created timestamp,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'customers',
    rowid_column 'id'
  );
```

##### Invoices
*read only*

Invoices are statements of amounts owed by a customer, and are either generated one-off, or generated periodically from a subscription.

Ref: [Stripe docs](https://stripe.com/docs/api/invoices/list) 

```sql
create foreign table stripe.invoices (
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
  server stripe_server
  options (
    object 'invoices'
  );

```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- customer
- status
- subscription


##### Payment Intents
*read only*

A payment intent guides you through the process of collecting a payment from your customer.

Ref: [Stripe docs](https://stripe.com/docs/api/payment_intents/list) 

```sql
create foreign table stripe.payment_intents (
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
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- customer

##### Products
*read and modify*

All products available in Stripe.

Ref: [Stripe docs](https://stripe.com/docs/api/products/list) 

```sql
create foreign table stripe.products (
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
    object 'products',
    rowid_column 'id'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- active

##### Subscriptions 
*read and modify*

Customer recurring payment schedules.

Ref: [Stripe docs](https://stripe.com/docs/api/subscriptions/list) 


```sql
create foreign table stripe.subscriptions (
  id text,
  customer text,
  currency text,
  current_period_start timestamp,
  current_period_end timestamp,
  attrs jsonb
)
  server my_stripe_server
  options (
    object 'subscriptions',
    rowid_column 'id'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- customer
- price
- status
