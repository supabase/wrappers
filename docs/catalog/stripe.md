---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Stripe

[Stripe](https://stripe.com) is an API driven online payment processing utility.

The Stripe Wrapper allows you to read data from Stripe within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Limitations

This section describes important limitations and considerations when using this FDW:

- **Performance Limitations**:
  - API request latency affects query performance as each operation requires Stripe API calls
  - No query pushdown support means all filtering happens locally after data retrieval
  - Large result sets may experience slower performance due to full data transfer requirement
  - API rate limits may affect performance during high-volume operations
  - Pagination is handled internally with fixed buffer sizes

- **Feature Limitations**:
  - Most objects are read-only, with only Customers, Products, and Subscriptions supporting write operations
  - Complex Stripe object relationships must be managed through separate queries
  - Filtering is most efficient only when using specific fields for each object type
  - Webhook events and real-time updates are not directly supported
  - Some Stripe object fields are only accessible through the `attrs` jsonb column

- **Resource Usage**:
  - Full result sets must be loaded into memory before processing
  - Each query requires a complete API request-response cycle
  - Large result sets may require significant PostgreSQL memory
  - Failed requests consume additional resources due to retry attempts
  - Connection pooling and caching are not supported

- **Known Issues**:
  - Materialized views using these foreign tables may fail during logical backups
  - Complex nested Stripe objects may require manual JSON parsing
  - API version mismatches can cause unexpected data format issues
  - Some Stripe events may not be immediately reflected in query results
  - Error handling for API failures may return empty results instead of errors

## Preparation

Before you can query Stripe, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Stripe Wrapper

Enable the `stripe_wrapper` FDW:

```sql
create foreign data wrapper stripe_wrapper
  handler stripe_fdw_handler
  validator stripe_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Stripe API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'stripe',
  '<Stripe API key>'
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
        api_key_id '<key_ID>', -- The Key ID from above, required if api_key_name is not specified.
        api_key_name '<key_Name>', -- The Key Name from above, required if api_key_id is not specified.
        api_url 'https://api.stripe.com/v1/',  -- Stripe API base URL, optional. Default is 'https://api.stripe.com/v1/'
        api_version '2024-06-20'  -- Stripe API version, optional. Default is your Stripe account’s default API version.
      );
    ```

=== "Without Vault"

    ```sql
    create server stripe_server
      foreign data wrapper stripe_wrapper
      options (
        api_key '<Stripe API key>',  -- Stripe API key, required
        api_url 'https://api.stripe.com/v1/',  -- Stripe API base URL, optional. Default is 'https://api.stripe.com/v1/'
        api_version '2024-06-20'  -- Stripe API version, optional. Default is your Stripe account’s default API version.
      );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists stripe;
```

## Entities

The Stripe Wrapper supports data read and modify from Stripe API.

| Object                                        | Select | Insert | Update | Delete | Truncate |
| --------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Accounts](#accounts)                         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Balance](#balance)                           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Balance Transactions](#balance-transactions) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Charges](#charges)                           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Checkout Sessions](#checkout-sessions)       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Customers](#customers)                       |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |
| [Disputes](#disputes)                         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Events](#events)                             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Files](#files)                               |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [File Links](#file-links)                     |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Invoices](#invoices)                         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Mandates](#mandates)                         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Meters](#meters)                             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [PaymentIntents](#payment-intents)            |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Payouts](#payouts)                           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Prices](#prices)                             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Products](#products)                         |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |
| [Refunds](#refunds)                           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [SetupAttempts](#setupattempts)               |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [SetupIntents](#setupintents)                 |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Subscriptions](#subscriptions)               |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |
| [Tokens](#tokens)                             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Topups](#top-ups)                            |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| [Transfers](#transfers)                       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

### Accounts

This is an object representing a Stripe account.

Ref: [Stripe docs](https://stripe.com/docs/api/accounts/list)

#### Operations

| Object                                                | Select | Insert | Update | Delete | Truncate |
| ----------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Accounts](https://stripe.com/docs/api/accounts/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- While any column is allowed in a where clause, it is most efficient to filter by `id`
- Use the `attrs` jsonb column to access additional account details

### Balance

This is an object representing your Stripe account's current balance.

Ref: [Stripe docs](https://stripe.com/docs/api/balance)

#### Operations

| Object                                            | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Balance](https://stripe.com/docs/api/balance)    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Balance is a read-only object that shows the current funds in your Stripe account
- The balance is broken down by source types (e.g., card, bank account) and currencies
- Use the `attrs` jsonb column to access additional balance details like pending amounts
- While any column is allowed in a where clause, filtering options are limited as this is a singleton object

### Balance Transactions

This is an object representing funds moving through your Stripe account. Balance transactions are created for every type of transaction that comes into or flows out of your Stripe account balance.

Ref: [Stripe docs](https://stripe.com/docs/api/balance_transactions/list)

#### Operations

| Object                                                                        | Select | Insert | Update | Delete | Truncate |
| ----------------------------------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Balance Transactions](https://stripe.com/docs/api/balance_transactions/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Balance transactions are read-only records of all funds movement in your Stripe account
- Each transaction includes amount, currency, fees, and net amount information
- Use the `attrs` jsonb column to access additional transaction details
- While any column is allowed in a where clause, it is most efficient to filter by:
    - id
    - type

### Charges

This is an object representing a charge on a credit or debit card. You can retrieve and refund individual charges as well as list all charges. Charges are identified by a unique, random ID.

Ref: [Stripe docs](https://stripe.com/docs/api/charges/list)

#### Operations

| Object                                            | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Charges](https://stripe.com/docs/api/charges/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Charges are read-only records of payment transactions in your Stripe account
- Each charge includes amount, currency, customer, and payment status information
- Use the `attrs` jsonb column to access additional charge details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - customer

### Checkout Sessions

This is an object representing your customer's session as they pay for one-time purchases or subscriptions through Checkout or Payment Links. We recommend creating a new Session each time your customer attempts to pay.

Ref: [Stripe docs](https://stripe.com/docs/api/checkout/sessions/list)

#### Operations

| Object                                                              | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Checkout Sessions](https://stripe.com/docs/api/checkout/sessions/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Checkout Sessions are read-only records of customer payment sessions in your Stripe account
- Each session includes customer, payment intent, and subscription information
- Use the `attrs` jsonb column to access additional session details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - customer
      - payment_intent
      - subscription

### Customers

This is an object representing your Stripe customers. You can create, retrieve, update, and delete customers.

Ref: [Stripe docs](https://stripe.com/docs/api/customers/list)

#### Operations

| Object                                              | Select | Insert | Update | Delete | Truncate |
| --------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Customers](https://stripe.com/docs/api/customers/list) |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

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

Example operations:

```sql
-- create a new customer
insert into stripe.customers(email, name, description)
values ('jane@example.com', 'Jane Smith', 'Premium customer');

-- update a customer
update stripe.customers
set name = 'Jane Doe'
where email = 'jane@example.com';

-- delete a customer
delete from stripe.customers
where id = 'cus_xxx';
```

#### Notes

- Customers can be created, retrieved, updated, and deleted through SQL operations
- Each customer can have an email, name, and description
- Use the `attrs` jsonb column to access additional customer details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - email

### Disputes

This is an object representing a dispute that occurs when a customer questions your charge with their card issuer.

Ref: [Stripe docs](https://stripe.com/docs/api/disputes/list)

#### Operations

| Object                                            | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Disputes](https://stripe.com/docs/api/disputes/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Disputes are read-only records of customer payment disputes in your Stripe account
- Each dispute includes amount, currency, charge, and payment intent information
- Use the `attrs` jsonb column to access additional dispute details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - charge
      - payment_intent

### Events

This is an object representing events that occur in your Stripe account, letting you know when something interesting happens.

Ref: [Stripe docs](https://stripe.com/docs/api/events/list)

#### Operations

| Object                                          | Select | Insert | Update | Delete | Truncate |
| ----------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Events](https://stripe.com/docs/api/events/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Events are read-only records of activities in your Stripe account
- Each event includes type, API version, and timestamp information
- Use the `attrs` jsonb column to access additional event details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - type

### Files

This is an object representing a file hosted on Stripe's servers.

Ref: [Stripe docs](https://stripe.com/docs/api/files/list)

#### Operations

| Object                                        | Select | Insert | Update | Delete | Truncate |
| --------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Files](https://stripe.com/docs/api/files/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Files are read-only records of files hosted on Stripe's servers
- Each file includes filename, purpose, size, type, and URL information
- Files may have an expiration date specified in expires_at
- Use the `attrs` jsonb column to access additional file details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - purpose

### File Links

This is an object representing a link that can be used to share the contents of a `File` object with non-Stripe users.

Ref: [Stripe docs](https://stripe.com/docs/api/file_links/list)

#### Operations

| Object                                                | Select | Insert | Update | Delete | Truncate |
| ----------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [File Links](https://stripe.com/docs/api/file_links/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- File Links are read-only records that provide shareable access to Stripe files
- Each link includes a reference to the file and a public URL
- Links can be configured to expire at a specific time
- Use the `expired` boolean to check if a link has expired
- Use the `attrs` jsonb column to access additional link details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - file

### Invoices

This is an object representing statements of amounts owed by a customer, which are either generated one-off or periodically from a subscription.

Ref: [Stripe docs](https://stripe.com/docs/api/invoices/list)

#### Operations

| Object                                            | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Invoices](https://stripe.com/docs/api/invoices/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Invoices are read-only records of amounts owed by customers
- Each invoice includes customer, subscription, status, and amount information
- Invoices track billing periods with period_start and period_end timestamps
- Use the `attrs` jsonb column to access additional invoice details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - customer
      - status
      - subscription

### Mandates

This is an object representing a record of the permission a customer has given you to debit their payment method.

Ref: [Stripe docs](https://stripe.com/docs/api/mandates)

#### Operations

| Object                                            | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Mandates](https://stripe.com/docs/api/mandates) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Mandates are read-only records of customer payment permissions
- Each mandate includes payment method, status, and type information
- Use the `attrs` jsonb column to access additional mandate details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id

### Meters

This is an object representing a billing meter that allows you to track usage of a particular event.

Ref: [Stripe docs](https://docs.stripe.com/api/billing/meter)

#### Operations

| Object                                                | Select | Insert | Update | Delete | Truncate |
| ----------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Meters](https://docs.stripe.com/api/billing/meter) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table stripe.meter (
  id text,
  display_name text,
  event_name text,
  event_time_window text,
  status text,
  attrs jsonb
)
  server stripe_server
  options (
    object 'billing/meters'
  );
```

#### Notes

- Meters are read-only records for tracking event usage in billing
- Each meter includes display name, event name, and time window information
- The status field indicates whether the meter is active
- Use the `attrs` jsonb column to access additional meter details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id

### Payment Intents

This is an object representing a guide through the process of collecting a payment from your customer.

Ref: [Stripe docs](https://stripe.com/docs/api/payment_intents/list)

#### Operations

| Object                                                        | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Payment Intents](https://stripe.com/docs/api/payment_intents/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Payment Intents are read-only records that guide the payment collection process
- Each intent includes customer, amount, currency, and payment method information
- The created timestamp tracks when the payment intent was initiated
- Use the `attrs` jsonb column to access additional payment intent details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - customer

### Payouts

This is an object representing funds received from Stripe or initiated payouts to a bank account or debit card of a connected Stripe account.

Ref: [Stripe docs](https://stripe.com/docs/api/payouts/list)

#### Operations

| Object                                            | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Payouts](https://stripe.com/docs/api/payouts/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Payouts are read-only records of fund transfers
- Each payout includes amount, currency, and status information
- The arrival_date indicates when funds will be available
- The statement_descriptor appears on your bank statement
- Use the `attrs` jsonb column to access additional payout details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - status

### Prices

This is an object representing pricing configurations for products to facilitate multiple currencies and pricing options.

Ref: [Stripe docs](https://stripe.com/docs/api/prices/list)

#### Operations

| Object                                            | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Prices](https://stripe.com/docs/api/prices/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Prices are read-only records that define product pricing configurations
- Each price includes currency, unit amount, and product reference
- The active boolean indicates if the price can be used
- The type field specifies the pricing model (e.g., one-time, recurring)
- Use the `attrs` jsonb column to access additional price details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - active

### Products

This is an object representing all products available in Stripe.

Ref: [Stripe docs](https://stripe.com/docs/api/products/list)

#### Operations

| Object                                            | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Products](https://stripe.com/docs/api/products/list) |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

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

#### Notes

- Products can be created, read, updated, and deleted
- Each product includes name, description, and active status
- The default_price links to the product's default Price object
- The updated timestamp tracks the last modification time
- Use the `attrs` jsonb column to access additional product details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - active

### Refunds

This is an object representing refunds for charges that have previously been created but not yet refunded.

Ref: [Stripe docs](https://stripe.com/docs/api/refunds/list)

#### Operations

| Object                                            | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Refunds](https://stripe.com/docs/api/refunds/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Refunds are read-only records of charge reversals
- Each refund includes amount, currency, and status information
- The charge and payment_intent fields link to the original transaction
- The reason field provides context for the refund
- Use the `attrs` jsonb column to access additional refund details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - charge
      - payment_intent

### SetupAttempts

This is an object representing attempted confirmations of SetupIntents, tracking both successful and unsuccessful attempts.

Ref: [Stripe docs](https://stripe.com/docs/api/setup_attempts/list)

#### Operations

| Object                                                      | Select | Insert | Update | Delete | Truncate |
| ----------------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [SetupAttempts](https://stripe.com/docs/api/setup_attempts/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- SetupAttempts are read-only records of payment setup confirmation attempts
- Each attempt includes customer, payment method, and status information
- The setup_intent field links to the associated SetupIntent
- The usage field indicates the intended payment method usage
- Use the `attrs` jsonb column to access additional attempt details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - setup_intent

### SetupIntents

This is an object representing a guide through the process of setting up and saving customer payment credentials for future payments.

Ref: [Stripe docs](https://stripe.com/docs/api/setup_intents/list)

#### Operations

| Object                                                      | Select | Insert | Update | Delete | Truncate |
| ----------------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [SetupIntents](https://stripe.com/docs/api/setup_intents/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- SetupIntents are read-only records for saving customer payment credentials
- Each intent includes customer, payment method, and status information
- The client_secret is used for client-side confirmation
- The usage field indicates how the payment method will be used
- Use the `attrs` jsonb column to access additional intent details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - customer
      - payment_method

### Subscriptions

This is an object representing customer recurring payment schedules.

Ref: [Stripe docs](https://stripe.com/docs/api/subscriptions/list)

#### Operations

| Object                                                      | Select | Insert | Update | Delete | Truncate |
| ----------------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Subscriptions](https://stripe.com/docs/api/subscriptions/list) |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

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

#### Notes

- Subscriptions can be created, read, updated, and deleted
- Each subscription includes customer and currency information
- The current_period_start and current_period_end track billing cycles
- The rowid_column option enables modification operations
- Use the `attrs` jsonb column to access additional subscription details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - customer
      - price
      - status

### Tokens

This is an object representing a secure way to collect sensitive card, bank account, or personally identifiable information (PII) from customers.

Ref: [Stripe docs](https://stripe.com/docs/api/tokens)

#### Operations

| Object                                              | Select | Insert | Update | Delete | Truncate |
| --------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Tokens](https://stripe.com/docs/api/tokens) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table stripe.tokens (
  id text,
  type text,
  client_ip text,
  created timestamp,
  livemode boolean,
  used boolean,
  attrs jsonb
)
  server stripe_server
  options (
    object 'tokens'
  );
```

#### Notes

- Tokens are read-only, single-use objects for secure data collection
- Each token includes type information (card, bank_account, pii, etc.)
- The client_ip field records where the token was created
- The used field indicates if the token has been used
- Use the `attrs` jsonb column to access token details like card or bank information
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - type
      - used

### Top-ups

This is an object representing a way to add funds to your Stripe balance.

Ref: [Stripe docs](https://stripe.com/docs/api/topups/list)

#### Operations

| Object                                              | Select | Insert | Update | Delete | Truncate |
| --------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Top-ups](https://stripe.com/docs/api/topups/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Top-ups are read-only records of balance additions
- Each top-up includes amount and currency information
- The status field tracks the top-up state (e.g., succeeded, failed)
- Use the `attrs` jsonb column to access additional top-up details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - status

### Transfers

This is an object representing fund movements between Stripe accounts as part of Connect.

Ref: [Stripe docs](https://stripe.com/docs/api/transfers/list)

#### Operations

| Object                                                  | Select | Insert | Update | Delete | Truncate |
| ------------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Transfers](https://stripe.com/docs/api/transfers/list) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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

#### Notes

- Transfers are read-only records of fund movements between accounts
- Each transfer includes amount, currency, and destination information
- The destination field identifies the receiving Stripe account
- Use the `attrs` jsonb column to access additional transfer details
- While any column is allowed in a where clause, it is most efficient to filter by:
      - id
      - destination

## Query Pushdown Support

This FDW supports `where` clause pushdown. You can specify a filter in `where` clause and it will be passed to Stripe API call.

For example, this query

```sql
select * from stripe.customers where id = 'cus_xxx';
```

will be translated to a Stripe API call: `https://api.stripe.com/v1/customers/cus_xxx`.

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
-- insert
insert into stripe.customers(email,name,description)
values ('test@test.com', 'test name', null);

-- update
update stripe.customers
set description='hello fdw'
where id = 'cus_xxx';

update stripe.customers
set attrs='{"metadata[foo]": "bar"}'
where id = 'cus_xxx';

-- delete
delete from stripe.customers
where id = 'cus_xxx';
```

To insert into an object with sub-fields, we need to create the foreign table with column name exactly same as the API required. For example, to insert a `subscription` object we can define the foreign table following [the Stripe API docs](https://docs.stripe.com/api/subscriptions/create):

```sql
-- create the subscription table for data insertion, the 'customer'
-- and 'items[0][price]' fields are required.
create foreign table stripe.subscriptions (
  id text,
  customer text,
  "items[0][price]" text  -- column name will be used in API Post request
)
  server stripe_server
  options (
    object 'subscriptions',
    rowid_column 'id'
  );
```

And then we can insert a subscription like below:

```sql
insert into stripe.subscriptions(customer, "items[0][price]")
values ('cus_Na6dX7aXxi11N4', 'price_1MowQULkdIwHu7ixraBm864M');
```

Note this foreign table is only for data insertion, it cannot be used in `select` statement.
