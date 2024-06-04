[Stripe](https://stripe.com) is an API driven online payment processing utility. `supabase/wrappers` exposes below endpoints.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper stripe_wrapper
  handler stripe_fdw_handler
  validator stripe_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Stripe API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'stripe',
  'YOUR_SECRET'
)
returning key_id;
```

### Connecting to Stripe

We need to provide Postgres with the credentials to connect to Stripe, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server stripe_server
      foreign data wrapper stripe_wrapper
      options (
        api_key_id '<key_ID>', -- The Key ID from above, required.
        api_url 'https://api.stripe.com/v1/'  -- Stripe API base URL, optional. Default is 'https://api.stripe.com/v1/'
      );
    ```

=== "Without Vault"

    ```sql
    create server stripe_server
      foreign data wrapper stripe_wrapper
      options (
        api_key '<Stripe API Key>',  -- Stripe API key, required
        api_url 'https://api.stripe.com/v1/'  -- Stripe API base URL, optional. Default is 'https://api.stripe.com/v1/'
      );
    ```

## Creating Foreign Tables

The Stripe Wrapper supports data read and modify from Stripe API.

| Object                                                                        | Select | Insert | Update | Delete | Truncate |
| ----------------------------------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Accounts](https://stripe.com/docs/api/accounts/list)                         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Balance](https://stripe.com/docs/api/balance)                                |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Balance Transactions](https://stripe.com/docs/api/balance_transactions/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Charges](https://stripe.com/docs/api/charges/list)                           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Checkout Sessions](https://stripe.com/docs/api/checkout/sessions/list)       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Customers](https://stripe.com/docs/api/customers/list)                       |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |
| [Disputes](https://stripe.com/docs/api/disputes/list)                         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Events](https://stripe.com/docs/api/events/list)                             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Files](https://stripe.com/docs/api/files/list)                               |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [File Links](https://stripe.com/docs/api/file_links/list)                     |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Invoices](https://stripe.com/docs/api/invoices/list)                         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Mandates](https://stripe.com/docs/api/mandates)                              |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [PaymentIntents](https://stripe.com/docs/api/payment_intents/list)            |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Payouts](https://stripe.com/docs/api/payouts/list)                           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Prices](https://stripe.com/docs/api/prices/list)                             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Products](https://stripe.com/docs/api/products/list)                         |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |
| [Refunds](https://stripe.com/docs/api/refunds/list)                           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [SetupAttempts](https://stripe.com/docs/api/setup_attempts/list)              |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [SetupIntents](https://stripe.com/docs/api/setup_intents/list)                |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Subscriptions](https://stripe.com/docs/api/subscriptions/list)               |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |
| [Tokens](https://stripe.com/docs/api/tokens)                                  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Topups](https://stripe.com/docs/api/topups/list)                             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Transfers](https://stripe.com/docs/api/transfers/list)                       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

The Stripe foreign tables mirror Stripe's API. We can create a schema to hold all the Stripe tables.

```sql
create schema stripe;
```

Then create the foreign table, for example:

```sql
create foreign table stripe.accounts (
  id text,
  business_type text,
  country text,
  email text,
  type text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'accounts'
  );
```

`attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed or its associated sub objects from it. See more examples below.

### Accounts

_read only_

This is an object representing a Stripe account.

Ref: [Stripe docs](https://stripe.com/docs/api/accounts/list)

```sql
create foreign table stripe.accounts (
  id text,
  business_type text,
  country text,
  email text,
  type text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'accounts'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id

### Balance

_read only_

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

### Balance Transactions

_read only_

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

### Charges

_read only_

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

### Checkout Sessions

_read only_

A Checkout Session represents your customer's session as they pay for one-time purchases or subscriptions through Checkout or Payment Links. We recommend creating a new Session each time your customer attempts to pay.

Ref: [Stripe docs](https://stripe.com/docs/api/checkout/sessions/list)

```sql
create foreign table stripe.checkout_sessions (
  id text,
  customer text,
  payment_intent text,
  subscription text,
  attrs jsonb
)
  server stripe_server
  options (
    object 'checkout/sessions',
    rowid_column 'id'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- customer
- payment_intent
- subscription

### Customers

_read and modify_

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

### Disputes

_read only_

A dispute occurs when a customer questions your charge with their card issuer.

Ref: [Stripe docs](https://stripe.com/docs/api/disputes/list)

```sql
create foreign table stripe.disputes (
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

### Events

_read only_

Events are our way of letting you know when something interesting happens in your account.

Ref: [Stripe docs](https://stripe.com/docs/api/events/list)

```sql
create foreign table stripe.events (
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

### Files

_read only_

This is an object representing a file hosted on Stripe's servers.

Ref: [Stripe docs](https://stripe.com/docs/api/files/list)

```sql
create foreign table stripe.files (
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

### File Links

_read only_

To share the contents of a `File` object with non-Stripe users, you can create a `FileLink`.

Ref: [Stripe docs](https://stripe.com/docs/api/file_links/list)

```sql
create foreign table stripe.file_links (
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

### Invoices

_read only_

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

### Mandates

_read only_

A Mandate is a record of the permission a customer has given you to debit their payment method.

Ref: [Stripe docs](https://stripe.com/docs/api/mandates)

```sql
create foreign table stripe.mandates (
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

### Payment Intents

_read only_

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

### Payouts

_read only_

A `Payout` object is created when you receive funds from Stripe, or when you initiate a payout to either a bank account or debit card of a connected Stripe account.

Ref: [Stripe docs](https://stripe.com/docs/api/payouts/list)

```sql
create foreign table stripe.payouts (
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

### Prices

_read only_

A `Price` object is needed for all of your products to facilitate multiple currencies and pricing options.

Ref: [Stripe docs](https://stripe.com/docs/api/prices/list)

```sql
create foreign table stripe.prices (
  id text,
  active bool,
  currency text,
  product text,
  unit_amount bigint,
  type text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'prices'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- active

### Products

_read and modify_

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

### Refunds

_read only_

`Refund` objects allow you to refund a charge that has previously been created but not yet refunded.

Ref: [Stripe docs](https://stripe.com/docs/api/refunds/list)

```sql
create foreign table stripe.refunds (
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

### SetupAttempts

_read only_

A `SetupAttempt` describes one attempted confirmation of a SetupIntent, whether that confirmation was successful or unsuccessful.

Ref: [Stripe docs](https://stripe.com/docs/api/setup_attempts/list)

```sql
create foreign table stripe.setup_attempts (
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

### SetupIntents

_read only_

A `SetupIntent` guides you through the process of setting up and saving a customer's payment credentials for future payments.

Ref: [Stripe docs](https://stripe.com/docs/api/setup_intents/list)

```sql
create foreign table stripe.setup_intents (
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

### Subscriptions

_read and modify_

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

### Tokens

_read only_

Tokenization is the process Stripe uses to collect sensitive card or bank account details, or personally identifiable information (PII), directly from your customers in a secure manner.

Ref: [Stripe docs](https://stripe.com/docs/api/tokens)

```sql
create foreign table stripe.tokens (
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

### Top-ups

_read only_

To top up your Stripe balance, you create a top-up object.

Ref: [Stripe docs](https://stripe.com/docs/api/topups/list)

```sql
create foreign table stripe.topups (
  id text,
  amount bigint,
  currency text,
  description text,
  status text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'topups'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- status

### Transfers

_read only_

A Transfer object is created when you move funds between Stripe accounts as part of Connect.

Ref: [Stripe docs](https://stripe.com/docs/api/transfers/list)

```sql
create foreign table stripe.transfers (
  id text,
  amount bigint,
  currency text,
  description text,
  destination text,
  created timestamp,
  attrs jsonb
)
  server stripe_server
  options (
    object 'transfers'
  );
```

While any column is allowed in a where clause, it is most efficient to filter by:

- id
- destination

## Query Pushdown Support

This FDW supports `where` clause pushdown. You can specify a filter in `where` clause and it will be passed to Stripe API call.

For example, this query

```sql
select * from stripe.customers where id = 'cus_xxx';
```

will be translated Stripe API call: `https://api.stripe.com/v1/customers/cus_xxx`.

For supported filter columns for each object, please check out foreign table documents above.

## Examples

Some examples on how to use Stripe foreign tables.

### Basic example

```sql
-- always limit records to reduce API calls to Stripe
select * from stripe.customers limit 10;
select * from stripe.invoices limit 10;
select * from stripe.subscriptions limit 10;
```

### Query JSON attributes

```sql
-- extract account name for an invoice
select id, attrs->>'account_name' as account_name
from stripe.invoices where id = 'in_xxx';

-- extract invoice line items for an invoice
select id, attrs#>'{lines,data}' as line_items
from stripe.invoices where id = 'in_xxx';

-- extract subscription items for a subscription
select id, attrs#>'{items,data}' as items
from stripe.subscriptions where id = 'sub_xxx';
```

### Data modify

```sql
insert into stripe.customers(email,name,description) values ('test@test.com', 'test name', null);
update stripe.customers set description='hello fdw' where id ='cus_xxx';
update stripe.customers set attrs='{"metadata[foo]": "bar"}' where id ='cus_xxx';
delete from stripe.customers where id ='cus_xxx';
```
