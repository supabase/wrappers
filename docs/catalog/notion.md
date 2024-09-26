---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Notion

[Notion](https://notion.so/) provides a versatile, ready-to-use solution for managing your data.

The Notion Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read data from your Notion workspace for use within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Supported Data Types

| Postgres Data Type | Notion Data Type |
| ------------------ | ---------------- |
| boolean            | Boolean          |
| text               | String           |
| timestamp          | Time             |
| timestamptz        | Time             |
| jsonb              | Json             |

The Notion API uses JSON formatted data, please refer to [Notion API docs](https://developers.notion.com/reference/intro) for more details.

## Available Versions

| Version | Wasm Package URL                                                                                | Checksum                                                           |
| ------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_notion_fdw_v0.1.0/notion_fdw.wasm` | `tbd` |

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Notion API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'notion',
  '<Notion API Key>' -- Notion API key
)
returning key_id;
```

### Connecting to Notion

We need to provide Postgres with the credentials to access Notion, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server notion_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_notion_fdw_v0.1.0/notion_fdw.wasm',
        fdw_package_name 'supabase:notion-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
        api_url 'https://api.notion.com/v1',  -- optional
        api_version '2022-06-28',  -- optional
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server notion_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_notion_fdw_v0.1.0/notion_fdw.wasm',
        fdw_package_name 'supabase:notion-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
        api_url 'https://api.notion.com/v1',  -- optional
        api_version '2022-06-28',  -- optional
        api_key 'secret_xxxxx'  -- Notion API key
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists notion;
```

## Creating Foreign Tables

The Notion Wrapper supports data reads from below objects in Notion.

| Integration | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| Block       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Page        |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Database    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| User        |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
-- Note: the 'page_id' isn't presented in the Notion Block's fields, it is
-- added by the foreign data wrapper for development convenience. All blocks,
-- including nested children blocks, belong to one page will have a same
-- 'page_id' of that page.
create foreign table notion.blocks (
  id text,
  page_id text,
  type text,
  created_time timestamp,
  last_edited_time timestamp,
  archived boolean,
  attrs jsonb
)
  server notion_server
  options (
    object 'block'
  );

create foreign table notion.pages (
  id text,
  url text,
  created_time timestamp,
  last_edited_time timestamp,
  archived boolean,
  attrs jsonb
)
  server notion_server
  options (
    object 'page'
  );

create foreign table notion.databases (
  id text,
  url text,
  created_time timestamp,
  last_edited_time timestamp,
  archived boolean,
  attrs jsonb
)
  server notion_server
  options (
    object 'database'
  );

create foreign table notion.users (
  id text,
  name text,
  type text,
  avatar_url text,
  attrs jsonb
)
  server notion_server
  options (
    object 'user'
  );
```

!!! note

    - All the supported columns are listed above, other columns are not allowd.
    - The `attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Foreign table options

The full list of foreign table options are below:

- `object` - Object name in Notion, required.

  Supported objects are listed below:

  | Object name           |
  | --------------------- |
  | block                 |
  | page                  |
  | database              |
  | user                  |

## Query Pushdown Support

This FDW supports `where` clause pushdown with `id` as the filter. For example,

```sql
select * from notion.pages
where id = '5a67c86f-d0da-4d0a-9dd7-f4cf164e6247';
```

will be translated Notion API call: `https://api.notion.com/v1/pages/5a67c86f-d0da-4d0a-9dd7-f4cf164e6247`.

In addition to `id` column pushdown, `page_id` column pushdown is also supported for `Block` object. For example,

```sql
select * from notion.blocks
where page_id = '5a67c86f-d0da-4d0a-9dd7-f4cf164e6247';
```

will recursively fetch all children blocks of the Page with id '5a67c86f-d0da-4d0a-9dd7-f4cf164e6247'. This can dramatically reduce number of API calls and improve query speed.

!!! note

    Below query will request ALL the blocks of ALL pages recursively, it may take very long time to run if there are many pages in Notion. So it is recommended to always query Block object with an `id` or `page_id` filter like above.

    ```sql
    select * from notion.blocks;
    ```

## Examples

Below are some examples on how to use Notion foreign tables.

### Basic example

This example will create a "foreign table" inside your Postgres database and query its data. First, we can create a schema to hold all the Notion foreign tables.

```sql
create schema if not exists notion;
```

Then create the foreign table and query it, for example:

```sql
create foreign table notion.pages (
  id text,
  url text,
  created_time timestamp,
  last_edited_time timestamp,
  archived boolean,
  attrs jsonb
)
  server notion_server
  options (
    object 'page'
  );

-- query all pages
select * from notion.pages;

-- query one page
select * from notion.pages
where id = '5a67c86f-d0da-4d0a-9dd7-f4cf164e6247';
```

`attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Query JSON attributes

```sql
create foreign table notion.users (
  id text,
  name text,
  type text,
  avatar_url text,
  attrs jsonb
)
  server notion_server
  options (
    object 'user'
  );

-- extract user's email address
select id, attrs->'person'->>'email' as email
from notion.users
where id = 'fd0ed76c-44bd-413a-9448-18ff4b1d6a5e';
```

### Query Blocks

```sql
-- query ALL blocks of ALL pages recursively, may take long time!
select * from notion.blocks;

-- query a single block by block id
select * from notion.blocks
where id = 'fc248547-83ef-4069-b7c9-18897edb7150';

-- query all block of a page by page id
select * from notion.blocks
where page_id = '5a67c86f-d0da-4d0a-9dd7-f4cf164e6247';
```
