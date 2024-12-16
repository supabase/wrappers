---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# AWS Cognito

[AWS Cognito](https://docs.aws.amazon.com/cognito/latest/developerguide/what-is-amazon-cognito.html) is an identity platform for web and mobile apps.

The Cognito wrapper allows you to read data from your Cognito Userpool within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Preparation

Before you can query AWS Cognito, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Cognito Wrapper

Enable the `cognito_wrapper` FDW:

```sql
create foreign data wrapper cognito_wrapper
  handler cognito_fdw_handler
  validator cognito_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers are designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
insert into vault.secrets (name, secret)
values (
  'cognito_secret_access_key',
  '<secret access key>'
)
returning key_id;
```

### Connecting to Cognito

We need to provide Postgres with the credentials to connect to Cognito, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server cognito_server
      foreign data wrapper cognito_wrapper
      options (
        aws_access_key_id '<your_access_key>',
        api_key_id '<your_secret_key_id_in_vault>',
        region '<your_aws_region>',
        user_pool_id '<your_user_pool_id>'
      );
    ```

=== "Without Vault"

    ```sql
    create server cognito_server
      foreign data wrapper cognito_wrapper
      options (
        aws_access_key_id '<your_access_key>',
        aws_secret_access_key '<your_secret_key>',
        region '<your_aws_region>',
        user_pool_id '<your_user_pool_id>'
      );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists cognito;
```

## Entities

### Users

This is an object representing Cognito User Records.

Ref: [AWS Cognito User Records](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-identity-pools.html)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Users  |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table cognito.users (
  username text,
  email text,
  status text,
  enabled boolean,
  created_at timestamp,
  updated_at timestamp,
  attributes jsonb
)
server cognito_server
options (
  object 'users'
);
```

#### Notes

- Only the columns listed above are accepted in the foreign table
- The `attributes` column contains additional user attributes in JSON format

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Limitations

This section describes important limitations and considerations when using this FDW:

- **Performance Limitations**:
  - No query pushdown support, all filtering must be done locally
  - API request latency affects query performance
  - Large result sets may experience slower performance due to full data transfer requirement
  - Performance depends on AWS Cognito API rate limits and response times
  - No support for parallel query execution
  - Pagination handling may impact performance for large user pools

- **Feature Limitations**:
  - Read-only access to Cognito data (no insert, update, or delete operations)
  - Only supports User Pool objects from Cognito API
  - Limited to specific predefined columns (username, email, status, etc.)
  - Authentication requires AWS access key and secret key pair
  - No support for Identity Pool operations
  - Cannot modify Cognito user attributes via FDW

- **Resource Usage**:
  - Full result sets must be loaded into memory before processing
  - Each query requires a complete API request-response cycle
  - No connection pooling or caching support
  - Memory usage scales with user count and attribute complexity
  - AWS API rate limiting may affect resource availability
  - Large result sets may require significant local memory

- **Known Issues**:
  - Materialized views using these foreign tables may fail during logical backups
  - Complex JSON attributes require manual parsing in the attributes column
  - Time zone differences may affect timestamp data interpretation
  - AWS API version changes may impact data structure compatibility
  - Error messages from AWS API may not be fully preserved
  - AWS credentials must be kept in sync with IAM policies

## Examples

### Basic example

This will create a "foreign table" inside your Postgres database called `cognito_table`:

```sql
create foreign table cognito.users (
  username text,
  email text,
  status text,
  enabled boolean,
  created_at timestamp,
  updated_at timestamp,
  attributes jsonb
)
server cognito_server
options (
   object 'users'
);
```

You can now fetch your Cognito data from within your Postgres database:

```sql
select * from cognito.users;
```
