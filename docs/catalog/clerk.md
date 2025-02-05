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

| Version | Wasm Package URL                                                                                    | Checksum                                                           |
| ------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_clerk_fdw_v0.1.0/clerk_fdw.wasm` | `tbd` |

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
-- Save your Clerk API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'clerk',
  '<Clerk API key>' -- Clerk API key 
)
returning key_id;
```

### Connecting to Clerk

We need to provide Postgres with the credentials to access Clerk and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server clerk_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_clerk_fdw_v0.1.0/clerk_fdw.wasm',
        fdw_package_name 'supabase:clerk-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
        api_url 'https://api.clerk.com/v1',  -- optional
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server clerk_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_clerk_fdw_v0.1.0/clerk_fdw.wasm',
        fdw_package_name 'supabase:clerk-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
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

| Object name              |
| ------------------------ |
| allowlist_identifiers    |
| blocklist_identifiers    |
| domains                  |
| invitations              |
| jwt_templates            |
| oauth_applications       |
| organizations            |
| organization_invitations |
| organization_memberships |
| redirect_urls            |
| saml_connections         |
| users                    |

## Entities

### Allow-list

This is a list of all identifiers allowed to sign up to an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Allow-list-Block-list#operation/ListAllowlistIdentifiers)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| allowlist_identifiers |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'allowlist_identifiers'
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
| blocklist_identifiers |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'blocklist_identifiers'
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
| domains |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'domains'
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
| invitations |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'invitations'
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
| jwt_templates |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'jwt_templates'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### OAuth Applications

This is a list of OAuth applications for an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/OAuth-Applications#operation/ListOAuthApplications)

#### Operations

| Object             | Select | Insert | Update | Delete | Truncate |
| ------------------ | :----: | :----: | :----: | :----: | :------: |
| oauth_applications |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'oauth_applications'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Organizations

This is a list of organizations for an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Organizations#operation/ListOrganizations)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| organizations |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'organizations'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

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

### Redirect URLs

This is a list of all whitelisted redirect urls for the instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Redirect-URLs#operation/ListRedirectURLs)

#### Operations

| Object        | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| redirect_urls |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'redirect_urls'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### SAML Connections

This is a list of SAML Connections for an instance.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/SAML-Connections#operation/ListSAMLConnections)

#### Operations

| Object           | Select | Insert | Update | Delete | Truncate |
| ---------------- | :----: | :----: | :----: | :----: | :------: |
| saml_connections |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'saml_connections'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Users

This is a list of all users.

Ref: [Clerk API docs](https://clerk.com/docs/reference/backend-api/tag/Users#operation/GetUserList)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| users  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

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
    object 'users'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

## Query Pushdown Support

This FDW doesn't support query pushdown.

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
- Query pushdown is not supported
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
    object 'users'
  );

-- query all users
select * from clerk.users;
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
