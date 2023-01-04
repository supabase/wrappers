[Stripe](https://stripe.com) is an API driven online payment processing utilty. `supabase/wrappers` exposes below endpoints.

1.  [Balance](https://stripe.com/docs/api/balance) (*read only*)
2.  [Balance Transactions](https://stripe.com/docs/api/balance_transactions/list) (*read only*)
3.  [Charges](https://stripe.com/docs/api/charges/list) (*read only*)
4.  [Customers](https://stripe.com/docs/api/customers/list) (*read and modify*)
5.  [Disputes](https://stripe.com/docs/api/disputes/list) (*read only*)
6.  [Events](https://stripe.com/docs/api/events/list) (*read only*)
7.  [Files](https://stripe.com/docs/api/files/list) (*read only*)
8.  [File Links](https://stripe.com/docs/api/file_links/list) (*read only*)
9.  [Invoices](https://stripe.com/docs/api/invoices/list) (*read only*)
10. [Mandates](https://stripe.com/docs/api/mandates) (*read only*)
11. [PaymentIntents](https://stripe.com/docs/api/payment_intents/list) (*read only*)
12. [Payouts](https://stripe.com/docs/api/payouts/list) (*read only*)
13. [Products](https://stripe.com/docs/api/products/list) (*read and modify*)
14. [Refunds](https://stripe.com/docs/api/refunds/list) (*read only*)
15. [SetupAttempts](https://stripe.com/docs/api/setup_attempts/list) (*read only*)
16. [SetupIntents](https://stripe.com/docs/api/setup_intents/list) (*read only*)
17. [Subscriptions](https://stripe.com/docs/api/subscriptions/list) (*read and modify*)
18. [Tokens](https://stripe.com/docs/api/tokens) (*read only*)

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
  server stripe_server
  options (
    object 'customers',
    rowid_column 'id'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- email

##### Disputes
*read only*

A dispute occurs when a customer questions your charge with their card issuer.

Ref: [Stripe docs](https://stripe.com/docs/api/disputes/list) 

```sql
create foreign table stripe_disputes (
  id text,
  amount bigint,
  currency text,
  charge text,
  payment_intent text,
  reason text,
  status text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'disputes'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- charge
- payment_intent

##### Events
*read only*

Events are our way of letting you know when something interesting happens in your account.

Ref: [Stripe docs](https://stripe.com/docs/api/events/list) 

```sql
create foreign table stripe_events (
  id text,
  type text,
  api_version text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'events'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- type

##### Files
*read only*

This is an object representing a file hosted on Stripe's servers.

Ref: [Stripe docs](https://stripe.com/docs/api/files/list) 

```sql
create foreign table stripe_files (
  id text,
  filename text,
  purpose text,
  title text,
  size bigint,
  type text,
  url text,
  created timestamp,
  expires_at timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'files'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- purpose

##### File Links
*read only*

To share the contents of a `File` object with non-Stripe users, you can create a `FileLink`.

Ref: [Stripe docs](https://stripe.com/docs/api/file_links/list) 

```sql
create foreign table stripe_file_links (
  id text,
  file text,
  url text,
  created timestamp,
  expired bool,
  expires_at timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'file_links'
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

##### Mandates
*read only*

A Mandate is a record of the permission a customer has given you to debit their payment method.

Ref: [Stripe docs](https://stripe.com/docs/api/mandates) 

```sql
create foreign table stripe_mandates (
  id text,
  payment_method text,
  status text,
  type text,
  attrs jsonb
)
  server stripe_server
  options (
    object 'mandates'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id


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
  server stripe_server
  options (
    object 'payment_intents'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- customer

##### Payouts
*read only*

A `Payout` object is created when you receive funds from Stripe, or when you initiate a payout to either a bank account or debit card of a connected Stripe account.

Ref: [Stripe docs](https://stripe.com/docs/api/payouts/list) 

```sql
create foreign table stripe_payouts (
  id text,
  amount bigint,
  currency text,
  arrival_date timestamp,
  description text,
  statement_descriptor text,
  status text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'payouts'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- status

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
  server stripe_server
  options (
    object 'products',
    rowid_column 'id'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- active

##### Refunds
*read only*

`Refund` objects allow you to refund a charge that has previously been created but not yet refunded.

Ref: [Stripe docs](https://stripe.com/docs/api/refunds/list) 

```sql
create foreign table stripe_refunds (
  id text,
  amount bigint,
  currency text,
  charge text,
  payment_intent text,
  reason text,
  status text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'refunds'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- charge
- payment_intent

##### SetupAttempts
*read only*

A `SetupAttempt` describes one attempted confirmation of a SetupIntent, whether that confirmation was successful or unsuccessful.

Ref: [Stripe docs](https://stripe.com/docs/api/setup_attempts/list) 

```sql
create foreign table stripe_setup_attempts (
  id text,
  application text,
  customer text,
  on_behalf_of text,
  payment_method text,
  setup_intent text,
  status text,
  usage text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'setup_attempts'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- setup_intent

##### SetupIntents
*read only*

A `SetupIntent` guides you through the process of setting up and saving a customer's payment credentials for future payments.

Ref: [Stripe docs](https://stripe.com/docs/api/setup_intents/list) 

```sql
create foreign table stripe_setup_intents (
  id text,
  client_secret text,
  customer text,
  description text,
  payment_method text,
  status text,
  usage text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'setup_intents'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- customer
- payment_method

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
  server stripe_server
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

##### Tokens
*read only*

Tokenization is the process Stripe uses to collect sensitive card or bank account details, or personally identifiable information (PII), directly from your customers in a secure manner.

Ref: [Stripe docs](https://stripe.com/docs/api/tokens) 

```sql
create foreign table stripe_tokens (
  id text,
  customer text,
  currency text,
  current_period_start timestamp,
  current_period_end timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'tokens'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
