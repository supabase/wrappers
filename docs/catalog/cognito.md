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

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper cognito_wrapper
  handler cognito_fdw_handler
  validator cognito_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inide `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers are designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

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

## Entities

### Users

This is an object representing Cognito User Records.

Ref: [AWS Cognito User Records](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-identity-pools.html)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Users  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table cognito (
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

## Foreign Table Options

- `object`: type of object we are querying. For now, only `users` is supported

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

### Basic example

This will create a "foreign table" inside your Postgres database called `cognito_table`:

```sql
create foreign table cognito_table (
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
select * from cognito_table;
```
