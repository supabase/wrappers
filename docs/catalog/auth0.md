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

## Limitations

This section describes important limitations and considerations when using this FDW:

- **Performance Limitations**:
  - No query pushdown support, all filtering must be done locally
  - API request latency affects query performance
  - Large result sets may experience slower performance due to full data transfer requirement
  - Performance depends on Auth0 API rate limits and response times
  - No support for parallel query execution
  - Pagination handling may impact performance for large user sets

- **Feature Limitations**:
  - Read-only access to Auth0 data (no insert, update, or delete operations)
  - Only supports the users object from Auth0 Management API
  - No support for other Auth0 API endpoints or objects
  - Authentication limited to API key or Personal Access Token (PAT)
  - No support for custom search queries beyond basic filtering
  - Cannot modify Auth0 user properties via FDW

- **Resource Usage**:
  - Full result sets must be loaded into memory before processing
  - Each query requires a complete API request-response cycle
  - No connection pooling or caching support
  - Memory usage scales with user count and attribute complexity
  - API rate limiting may affect resource availability
  - Large result sets may require significant local memory

- **Known Issues**:
  - Materialized views using these foreign tables may fail during logical backups
  - Complex JSON attributes require manual parsing and handling
  - Time zone differences may affect timestamp data interpretation
  - API version changes may impact data structure compatibility
  - Error messages from Auth0 API may not be fully preserved

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

### Connecting to Auth0

We need to provide Postgres with the credentials to connect to Airtable, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server auth0_server
      foreign data wrapper auth0_wrapper
      options (
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    -- create server and specify custom options
    create server auth0_server
    foreign data wrapper auth0_wrapper
    options (
        url 'https://dev-<tenant-id>.us.auth0.com/api/v2/users',
        api_key '<your_api_key>'
    );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists auth0;
```

## Entities

The Auth0 Wrapper supports data reads from Auth0 API.

### Users

The Auth0 Wrapper supports data reads from Auth0's [Management API List users endpoint](https://auth0.com/docs/api/management/v2/users/get-users) endpoint (_read only_).

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Users  |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table auth0.my_foreign_table (
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

```sql
create foreign table auth0.auth0_table (
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

You can now fetch your Auth0 data from within your Postgres database:

```sql
select * from auth0.auth0_table;
```
