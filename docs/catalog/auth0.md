---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Auth0

[Auth0](https://auth0.com/) is a flexible, drop-in solution to add authentication and authorization services to your applications

The Auth0 Wrapper allows you to read data from your Auth0 tenant for use within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Preparation

Before you can query Auth0, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Auth0 Wrapper

Enable the `auth0_wrapper` FDW:

```sql
create foreign data wrapper auth0_wrapper
  handler auth0_fdw_handler
  validator auth0_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Auth0 API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'auth0',
  '<Auth0 API Key or PAT>' -- Auth0 API key or Personal Access Token (PAT)
)
returning key_id;
```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema auth0;
```

## Entities

The Auth0 Wrapper supports data reads from Auth0's [Management API List users endpoint](https://auth0.com/docs/api/management/v2/users/get-users) endpoint (_read only_).

### Users

The Auth0 Wrapper supports data reads from Auth0's [Management API List users endpoint](https://auth0.com/docs/api/management/v2/users/get-users) endpoint (_read only_).

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Users  |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table my_foreign_table (
  name text
  -- other fields
)
server auth0_server
options (
  object 'users'
);
```

#### Notes

- Currently only supports the `users` object

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

### Basic Auth0 Users Query

This example demonstrates querying Auth0 users data.

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Users  |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table auth0_table (
  created_at text,
  email text,
  email_verified bool,
  identities jsonb
)
  server auth0_server
  options (
    object 'users'
  );
```

#### Notes

- The `object` option must be set to 'users'
- Query the table using standard SQL: `select * from auth0_table;`
- The table provides read-only access to Auth0 user data
