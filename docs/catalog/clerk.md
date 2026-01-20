---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Clerk

[Clerk](https://clerk.com/) is a complete suite of embeddable UIs, flexible APIs, and admin dashboards to authenticate and manage users.

The Clerk Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read data from Clerk for use within your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                                    | Checksum                                                           | Required Wrappers Version |
| ------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------- |
| 0.2.1   | `https://github.com/supabase/wrappers/releases/download/wasm_clerk_fdw_v0.2.1/clerk_fdw.wasm`       | `100f3f105e7e6dab92c433b2da6bec98fafeccd0304e6efaf3780d0a8cae30ec` | >=0.5.0                   |
| 0.2.0   | `https://github.com/supabase/wrappers/releases/download/wasm_clerk_fdw_v0.2.0/clerk_fdw.wasm`       | `89337bb11779d4d654cd3e54391aabd02509d213db6995f7dd58951774bf0e37` | >=0.5.0                   |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_clerk_fdw_v0.1.0/clerk_fdw.wasm`       | `613be26b59fa4c074e0b93f0db617fcd7b468d4d02edece0b1f85fdb683ebdc4` | >=0.4.0                   |

## Preparation

Before you can query Clerk, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Clerk Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Clerk API key in Vault and retrieve the created `key_id`
select vault.create_secret(
  '<Clerk API key>', -- Clerk API key
  'clerk',
  'Clerk API key for Wrappers'
);
```

### Connecting to Clerk

We need to provide Postgres with the credentials to access Clerk and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server clerk_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_clerk_fdw_v0.2.1/clerk_fdw.wasm',
        fdw_package_name 'supabase:clerk-fdw',
        fdw_package_version '0.2.1',
        fdw_package_checksum '100f3f105e7e6dab92c433b2da6bec98fafeccd0304e6efaf3780d0a8cae30ec',
        api_url 'https://api.clerk.com/v1',  -- optional
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server clerk_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_clerk_fdw_v0.2.1/clerk_fdw.wasm',
        fdw_package_name 'supabase:clerk-fdw',
        fdw_package_version '0.2.1',
        fdw_package_checksum '100f3f105e7e6dab92c433b2da6bec98fafeccd0304e6efaf3780d0a8cae30ec',
        api_url 'https://api.clerk.com/v1',  -- optional
        api_key 'sk_test_...'  -- Clerk API key
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists clerk;
```

## Options

The full list of foreign table options are below:

- `object` - Object name in Clerk, required.

Supported objects are listed below:

| Object name                        |
| ---------------------------------- |
| allowlist_identifiers              |
| billing_payment_attempts           |
| billing_plans                      |
| billing_statement                  |
| billing_statements                 |
| billing_subscription_items         |
| blocklist_identifiers              |
| domains                            |
| invitations                        |
| jwt_templates                      |
| oauth_applications                 |
| organizations                      |
| organization_billing_subscriptions |
| organization_invitations           |
| organization_memberships           |
| redirect_urls                      |
| saml_connections                   |
| user_billing_subscriptions         |
| users                              |

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from Clerk.

For example, using below SQL can automatically create foreign tables in the `clerk` schema.

```sql
-- create all the foreign tables
import foreign schema clerk from server clerk_server into clerk;

-- or, create selected tables only
import foreign schema clerk
   limit to ("users", "organizations")
   from server clerk_server into clerk;

-- or, create all foreign tables except selected tables
import foreign schema clerk
   except ("users")
   from server clerk_server into clerk;
```

### Allow-list

This is a list of all identifiers allowed to sign up to an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Allow-list-Block-list#operation/ListAllowlistIdentifiers)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| allowlist_identifiers |   ✅   |   ✅   |   ❌   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table clerk.allowlist_identifiers (
  id text,
  invitation_id text,
  identifier text,
  identifier_type text,
  instance_id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'allowlist_identifiers',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Block-list

This is a list of all identifiers which are not allowed to access an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Allow-list-Block-list#operation/ListBlocklistIdentifiers)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| blocklist_identifiers |   ✅   |   ✅   |   ❌   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table clerk.blocklist_identifiers (
  id text,
  identifier text,
  identifier_type text,
  instance_id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'blocklist_identifiers',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Domains

This is a list of all domains for an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Domains#operation/ListDomains)

#### Operations

| Object  | Select | Insert | Update | Delete | Truncate |
| ------- | :----: | :----: | :----: | :----: | :------: |
| domains |   ✅   |   ❌   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table clerk.domains (
  id text,
  name text,
  is_satellite boolean,
  frontend_api_url text,
  accounts_portal_url text,
  attrs jsonb
)
  server clerk_server
  options (
    object 'domains',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Invitations

This is a list of all non-revoked invitations for your application.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Invitations#operation/ListInvitations)

#### Operations

| Object      | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| invitations |   ✅   |   ✅   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.invitations (
  id text,
  email_address text,
  url text,
  revoked boolean,
  status text,
  expires_at timestamp,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'invitations',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### JWT Templates

This is a list of all JWT templates.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/JWT-Templates#operation/ListJWTTemplates)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| jwt_templates |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table clerk.jwt_templates (
  id text,
  name text,
  lifetime bigint,
  allowed_clock_skew bigint,
  custom_signing_key boolean,
  signing_algorithm text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'jwt_templates',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Single-item retrieval is supported via `WHERE id = 'xxx'` clause, which fetches from `GET /jwt_templates/{template_id}` endpoint

### OAuth Applications

This is a list of OAuth applications for an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/OAuth-Applications#operation/ListOAuthApplications)

#### Operations

| Object             | Select | Insert | Update | Delete | Truncate |
| ------------------ | :----: | :----: | :----: | :----: | :------: |
| oauth_applications |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table clerk.oauth_applications (
  id text,
  name text,
  instance_id text,
  client_id text,
  public boolean,
  scopes text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'oauth_applications',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Single-item retrieval is supported via `WHERE id = 'xxx'` clause, which fetches from `GET /oauth_applications/{oauth_application_id}` endpoint

### Organizations

This is a list of organizations for an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Organizations#operation/ListOrganizations)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| organizations |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table clerk.organizations (
  id text,
  name text,
  slug text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'organizations',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Single-item retrieval is supported via `WHERE id = 'xxx'` clause, which fetches from `GET /organizations/{organization_id}` endpoint

### Organization Invitations

This is a list of organization invitations for an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Organization-Invitations#operation/ListInstanceOrganizationInvitations)

#### Operations

| Object                   | Select | Insert | Update | Delete | Truncate |
| ------------------------ | :----: | :----: | :----: | :----: | :------: |
| organization_invitations |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.organization_invitations (
  id text,
  email_address text,
  role text,
  role_name text,
  organization_id text,
  status text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'organization_invitations'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Organization Memberships

This is a list of organization user memberships for an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Organization-Memberships#operation/InstanceGetOrganizationMemberships)

#### Operations

| Object                   | Select | Insert | Update | Delete | Truncate |
| ------------------------ | :----: | :----: | :----: | :----: | :------: |
| organization_memberships |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.organization_memberships (
  id text,
  role text,
  role_name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'organization_memberships'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Single-item retrieval is supported via `WHERE id = 'xxx'` clause, which fetches from `GET /oauth_applications/{oauth_application_id}` endpoint

### Redirect URLs

This is a list of all whitelisted redirect urls for the instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Redirect-URLs#operation/ListRedirectURLs)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| redirect_urls |   ✅   |   ✅   |   ❌   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table clerk.redirect_urls (
  id text,
  url text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'redirect_urls',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Single-item retrieval is supported via `WHERE id = 'xxx'` clause, which fetches from `GET /redirect_urls/{id}` endpoint

### SAML Connections

This is a list of SAML Connections for an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/SAML-Connections#operation/ListSAMLConnections)

#### Operations

| Object           | Select | Insert | Update | Delete | Truncate |
| ---------------- | :----: | :----: | :----: | :----: | :------: |
| saml_connections |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table clerk.saml_connections (
  id text,
  name text,
  domain text,
  active boolean,
  provider text,
  user_count bigint,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'saml_connections',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Single-item retrieval is supported via `WHERE id = 'xxx'` clause, which fetches from `GET /saml_connections/{saml_connection_id}` endpoint

### Users

This is a list of all users.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Users#operation/GetUserList)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| users  |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table clerk.users (
  id text,
  external_id text,
  username text,
  first_name text,
  last_name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'users',
    rowid_column 'id'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Single-item retrieval is supported via `WHERE id = 'xxx'` clause, which fetches from `GET /users/{user_id}` endpoint

### User Billing Subscriptions

This retrieves the billing subscription for a specific user.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/billing#operation/GetUserBillingSubscription)

#### Operations

| Object                      | Select | Insert | Update | Delete | Truncate |
| --------------------------- | :----: | :----: | :----: | :----: | :------: |
| users/billing/subscription  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.user_billing_subscriptions (
  user_id text,
  id text,
  status text,
  payer_id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'users/billing/subscription'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- The query must specify `user_id` in the WHERE clause

### Organization Billing Subscriptions

This retrieves the billing subscription for a specific organization.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/billing#operation/GetOrganizationBillingSubscription)

#### Operations

| Object                            | Select | Insert | Update | Delete | Truncate |
| --------------------------------- | :----: | :----: | :----: | :----: | :------: |
| organizations/billing/subscription |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.organization_billing_subscriptions (
  organization_id text,
  id text,
  status text,
  payer_id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'organizations/billing/subscription'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- The query must specify `organization_id` in the WHERE clause

### Billing Plans

This is a list of all billing plans.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/billing#operation/ListBillingPlans)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| billing/plans |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.billing_plans (
  id text,
  name text,
  description text,
  slug text,
  is_default boolean,
  is_recurring boolean,
  attrs jsonb
)
  server clerk_server
  options (
    object 'billing/plans'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Billing Subscription Items

This is a list of all billing subscription items.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/billing#operation/ListBillingSubscriptionItems)

#### Operations

| Object                     | Select | Insert | Update | Delete | Truncate |
| -------------------------- | :----: | :----: | :----: | :----: | :------: |
| billing/subscription_items |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.billing_subscription_items (
  id text,
  status text,
  plan_id text,
  plan_period text,
  payer_id text,
  is_free_trial boolean,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'billing/subscription_items'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Billing Statements

This is a list of all billing statements.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/billing#operation/ListBillingStatements)

#### Operations

| Object             | Select | Insert | Update | Delete | Truncate |
| ------------------ | :----: | :----: | :----: | :----: | :------: |
| billing/statements |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.billing_statements (
  id text,
  status text,
  timestamp timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'billing/statements'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Billing Statement

This retrieves a specific billing statement by ID.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/billing#operation/GetBillingStatement)

#### Operations

| Object            | Select | Insert | Update | Delete | Truncate |
| ----------------- | :----: | :----: | :----: | :----: | :------: |
| billing/statement |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.billing_statement (
  statement_id text,
  id text,
  status text,
  timestamp timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'billing/statement'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- The query must specify `statement_id` in the WHERE clause

### Billing Payment Attempts

This retrieves payment attempts for a specific billing statement.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/billing#operation/ListBillingStatementPaymentAttempts)

#### Operations

| Object                   | Select | Insert | Update | Delete | Truncate |
| ------------------------ | :----: | :----: | :----: | :----: | :------: |
| billing/payment_attempts |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table clerk.billing_payment_attempts (
  statement_id text,
  id text,
  status text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'billing/payment_attempts'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- The query must specify `statement_id` in the WHERE clause

## Query Pushdown Support

### `where` clause pushdown

This FDW supports `where id = 'xxx'` clause pushdown for the following objects:

- users
- organizations
- jwt_templates
- oauth_applications
- saml_connections
- redirect_urls

For example:

```sql
-- Fetches from GET /users/user_xxx (single API call)
select * from clerk.users where id = 'user_xxx';
```

### Parameterized endpoints

Some endpoints require specific qualifiers in the WHERE clause:

| Object | Required qualifier |
|--------|-------------------|
| user_billing_subscriptions | `user_id` |
| organization_billing_subscriptions | `organization_id` |
| billing_statement | `statement_id` |
| billing_payment_attempts | `statement_id` |

For example:

```sql
-- Fetches from GET /users/{user_id}/billing/subscription
select * from clerk.user_billing_subscriptions where user_id = 'user_xxx';
```

## Supported Data Types

| Postgres Data Type | Clerk Data Type    |
| ------------------ | ------------------ |
| boolean            | Boolean            |
| bigint             | Number             |
| double precision   | Number             |
| text               | String             |
| timestamp          | Time               |
| jsonb              | Json               |

The Clerk API uses JSON formatted data, please refer to [Clerk Backend API docs](https://clerk.com/docs/reference/backend-api) for more details.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Large result sets may experience slower performance due to full data transfer requirement
- Materialized views using these foreign tables may fail during logical backups

## Examples

Below are some examples on how to use Clerk foreign tables.

### Basic example

This example will create a "foreign table" inside your Postgres database and query its data.

```sql
create foreign table clerk.users (
  id text,
  external_id text,
  username text,
  first_name text,
  last_name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server clerk_server
  options (
    object 'users',
    rowid_column 'id'
  );

-- query all users (fetches from GET /users)
select * from clerk.users;

-- retrieve a specific user by ID (fetches from GET /users/{user_id})
select * from clerk.users where id = 'user_xxx';
```

`attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Query JSON attributes

```sql
-- extract all email addresses from user
select
  u.id,
  e->>'email_address' as email
from clerk.users u
  cross join json_array_elements((attrs->'email_addresses')::json) e;
```

### Data Modify Examples

Some tables support INSERT, UPDATE, and DELETE operations. Use the `attrs` JSONB column to provide the request body:

```sql
-- Create a new user
INSERT INTO clerk.users (attrs) VALUES ('{"email_address": ["user@example.com"], "password": "secure123"}');

-- Update a user (requires rowid_column 'id')
UPDATE clerk.users SET attrs = '{"first_name": "John", "last_name": "Doe"}' WHERE id = 'user_xxx';

-- Delete a user (requires rowid_column 'id')
DELETE FROM clerk.users WHERE id = 'user_xxx';

-- Create a new organization
INSERT INTO clerk.organizations (attrs) VALUES ('{"name": "My Organization", "slug": "my-org"}');

-- Update an organization
UPDATE clerk.organizations SET attrs = '{"name": "Updated Name"}' WHERE id = 'org_xxx';

-- Delete an organization
DELETE FROM clerk.organizations WHERE id = 'org_xxx';
```

### Billing examples

```sql
-- Query all billing plans
SELECT * FROM clerk.billing_plans;

-- Query all billing statements
SELECT * FROM clerk.billing_statements;

-- Query all billing subscription items
SELECT * FROM clerk.billing_subscription_items;

-- Query a specific statement (requires WHERE clause)
SELECT * FROM clerk.billing_statement WHERE statement_id = 'stmt_xxx';

-- Query payment attempts for a statement (requires WHERE clause)
SELECT * FROM clerk.billing_payment_attempts WHERE statement_id = 'stmt_xxx';

-- Query subscription for a specific user (requires WHERE clause)
SELECT * FROM clerk.user_billing_subscriptions WHERE user_id = 'user_xxx';

-- Query subscription for a specific organization (requires WHERE clause)
SELECT * FROM clerk.organization_billing_subscriptions WHERE organization_id = 'org_xxx';

-- Retrieve a single user by ID (fetches from GET /users/{user_id})
SELECT * FROM clerk.users WHERE id = 'user_xxx';

-- Retrieve a single organization by ID (fetches from GET /organizations/{organization_id})
SELECT * FROM clerk.organizations WHERE id = 'org_xxx';

-- Retrieve a single JWT template by ID (fetches from GET /jwt_templates/{template_id})
SELECT * FROM clerk.jwt_templates WHERE id = 'tmpl_xxx';

-- Retrieve a single OAuth application by ID (fetches from GET /oauth_applications/{oauth_application_id})
SELECT * FROM clerk.oauth_applications WHERE id = 'oauth_xxx';

-- Retrieve a single SAML connection by ID (fetches from GET /saml_connections/{saml_connection_id})
SELECT * FROM clerk.saml_connections WHERE id = 'samlconn_xxx';

-- Retrieve a single redirect URL by ID (fetches from GET /redirect_urls/{id})
SELECT * FROM clerk.redirect_urls WHERE id = 'redir_xxx';
```
