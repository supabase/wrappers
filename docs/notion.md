# [Notion](https://notion.so/) provides a versatile, ready-to-use solution for managing your data.


The Notion Wrapper allows you to read data from your Notion workspace for use within your Postgres database. Only the users endpoint is supported at the moment.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
CREATE extension IF NOT EXISTS wrappers SCHEMA extensions;
```

Then, create the foreign data wrapper:

```sql
CREATE FOREIGN DATA WRAPPER notion_wrapper
  HANDLER notion_fdw_handler
  VALIDATOR notion_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials in plain text within the `pg_catalog.pg_foreign_server` table, making them visible to anyone with access to this table. To enhance security, it is advisable to use [Vault](https://supabase.com/docs/guides/database/vault) for credential storage. Vault integrates seamlessly with Wrappers to provide a secure storage solution for sensitive information. We strongly recommend utilizing Vault to safeguard your credentials.

```sql
-- Save your Notion API key in Vault and retrieve the `key_id`
INSERT INTO vault.secrets (name, secret)
VALUES (
  'notion',
  '<Notion API Key>' -- Notion API key
)
RETURNING key_id;
```

### Connecting to Notion

We need to provide Postgres with the credentials to connect to Notion, and any additional options. We can do this using the `CREATE SERVER` command:

- With Vault (recommended)

  ```sql
  CREATE SERVER notion_server
    FOREIGN DATA WRAPPER notion_wrapper
    OPTIONS (
      api_key_id '<key_ID>', -- The Key ID from the Vault
      notion_version '<notion_version>', -- optional, default is '2022-06-28'
      api_url '<api_url>' -- optional, default is 'https://api.notion.com/v1/'
    );
  ```

- Without Vault

  ```sql
  CREATE SERVER notion_server
    FOREIGN DATA WRAPPER notion_wrapper
    OPTIONS (
      api_key '<your_api_key>', -- Your Notion API key
      notion_version '<notion_version>', -- optional, default is '2022-06-28'
      api_url '<api_url>' -- optional, default is 'https://api.notion.com/v1/'
  );
  ```

## Creating Foreign Tables

The Notion Wrapper supports data reads from the [Notion API](https://developers.notion.com/reference).

| Object                                                     | Select | Insert | Update | Delete | Truncate |
| ---------------------------------------------------------- | :----: | :----: | :----: | :----: | :------: |
| [Users](https://developers.notion.com/reference/get-users) |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
CREATE FOREIGN TABLE my_foreign_table (
  id text,
  name text,
  type text,
  person jsonb,
  bot jsonb
  -- other fields
)
  SERVER notion_server
  OPTIONS (
    object 'users',
);
```

## Query Pushdown Support

This FDW supports `where` clause pushdown. You can specify a filter in `where` clause and it will be passed to Notion API call.

For example, this query

```sql
SELECT * from notion_users where id = 'xxx';
```

will be translated Notion API call: `https://api.notion.com/v1/users/xxx`.

## Examples

Some examples on how to use Notion foreign tables.

### Users foreign table

The following command creates a "foreign table" in your Postgres database named `notion_users`:

```sql
CREATE FOREIGN TABLE notion_users (
  id text,
  name text,
  type text,
  person jsonb,
  bot jsonb
)
  SERVER notion_server
  OPTIONS (
    object 'users'
  );
```

You can now fetch your Notion data from within your Postgres database:

```sql
SELECT * FROM notion_users;
```

You can also query with filters:

```sql
SELECT * FROM notion_users WHERE id = 'xxx';
```
