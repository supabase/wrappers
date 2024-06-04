# [Auth0](https://auth0.com/) is a flexible, drop-in solution to add authentication and authorization services to your applications

The Auth0 Wrapper allows you to read data from your Auth0 tenant for use within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper auth0_wrapper
  handler auth0_fdw_handler
  validator auth0_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inide `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

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

## Creating Foreign Tables

The Auth0 Wrapper supports data reads from Auth0's [Management API List users endpoint](https://auth0.com/docs/api/management/v2/users/get-users) endpoint (_read only_).

| Auth0 | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| Records  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
create foreign table my_foreign_table (
  name text
  -- other fields
)
server auth0_server
options (
  object 'users',
);
```

### Foreign table options

The full list of foreign table options are below:

- `objects` - Auth0 object to select from. Currently only supports `users`

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

Some examples on how to use Auth0 foreign tables.

### Basic example

This will create a "foreign table" inside your Postgres database called `auth0_table`:

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

You can now fetch your Auth0 data from within your Postgres database:

```sql
select * from auth0;
```

