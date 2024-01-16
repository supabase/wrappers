[AWS Cognito](https://docs.aws.amazon.com/cognito/latest/developerguide/what-is-amazon-cognito.html) is an identity platform for web and mobile apps. 

The Cognito Wrappers allows you to read data from your Cognito Userpool within your Postgres database.

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

By default, Postgres stores FDW credentials inide `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.



### Connecting to Cognito

We need to provide Postgres with the credentials to connect to Cognito, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server cognito_server
      foreign data wrapper cognito_wrapper
      options (
        -- TODO
      );
    ```

=== "Without Vault"

    ```sql
    create server cognito_server
      foreign data wrapper cognito_wrapper
      options (
        -- TODO
      );
    ```

## Creating Foreign Tables

The Cognito Wrapper supports data reads from Cognito's [User Records](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-identity-pools.html) endpoint (_read only_).

| Cognito  | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| Records  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
create foreign table my_foreign_table (
  name text
  -- other fields
)
server cognito_server
options (
  -- TODO
);
```

### Foreign table options

The full list of foreign table options are below:

-- TODO: list optinos

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

Some examples on how to use Cognito foreign tables.

### Basic example

This will create a "foreign table" inside your Postgres database called `cognito_table`:

```sql
create foreign table cognito_table (
  name text,
  updated_at timestamp
)
server cognito_server
options (
  -- TODO
);
```

You can now fetch your Cognito data from within your Postgres database:

```sql
select * from cognito_table;
```

We can also create a foreign table from an Cognito View called `cognito_view`:

```sql
create foreign table cognito_view (
  -- TODO
)
server cognito_server
options (
);

select * from cognito_view;
```
