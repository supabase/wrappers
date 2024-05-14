# [Notion](https://notion.so/) is a flexible, drop-in solution to add authentication and authorization services to your applications

The Notion Wrapper allows you to read data from your Notion tenant for use within your Postgres database. Only the users endpoint is supported at the moment.

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper notion_wrapper
  handler notion_fdw_handler
  validator notion_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inide `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Auth0 API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'notion',
  '<Notion API Key>' -- Notion API key
)
returning key_id;
```

### Connecting to Notion

We need to provide Postgres with the credentials to connect to Notion, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server notion_server
      foreign data wrapper notion_wrapper
      options (
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    -- create server and specify custom options
    create server notion_server
    foreign data wrapper notion_wrapper
    options (
        api_key '<your_api_key>',
        notion_version '<notion_version>', -- optional, default is '2022-06-28'
        api_url '<api_url>' -- optional, default is 'https://api.notion.com/v1/'
    );
    ```

## Creating Foreign Tables

The Notion Wrapper supports data reads from Notion's [Users endpoint](https://developers.notion.com/reference/get-users) endpoint (_read only_).

| Notion   | Select | Insert | Update | Delete | Truncate |
| ------- | :----: | :----: | :----: | :----: | :------: |
| Records |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
create foreign table my_foreign_table (
  id text,
  name text,
  type text
  -- other fields
)
server notion_server
options (
  object 'users',
);
```

### Foreign table options

The full list of foreign table options are below:

- `objects` - Notion object to select from. Currently only supports `users`

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

Some examples on how to use Notion foreign tables.

### Basic example

This will create a "foreign table" inside your Postgres database called `notion_users`:

```sql
create foreign table notion_users (
  id text,
  name text,
  type text
)
  server notion_server
  options (
    object 'users'
  );

```

You can now fetch your Notion data from within your Postgres database:

```sql
select * from notion_users;
```
