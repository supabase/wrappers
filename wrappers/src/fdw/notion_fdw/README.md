# Notion Foreign Data Wrapper

This is a foreign data wrapper for [Notion](https://notion.so/) developed using [Wrappers](https://github.com/supabase/wrappers).

## Documentation

[https://supabase.github.io/wrappers/notion](https://supabase.github.io/wrappers/notion/)

## Basic usage

These steps outline how to use the this FDW:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgrx with feature:

```bash
cd wrappers/wrappers
cargo pgrx run --features notion_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
create extension wrappers;
```

```sql
-- create foreign data wrapper and enable 'Auth0Fdw'
create foreign data wrapper notion_wrapper
  handler notion_fdw_handler
  validator notion_fdw_validator;
```

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

```sql
-- create an example foreign table
-- Number of fields are illustrative
create foreign table notion_users (
  id text,
  name text,
  type text,
)
  server notion_server
  options (
    object 'users'
  );
```

````

4. Run a query to check if it is working:

```sql
wrappers=# select id, name, type from notion_users;

id             | d40e767c-d7af-4b18-a86d-55c61f1e39a4
name           | John Doe
type           | person
````

## Changelog

| Version | Date       | Notes                                         |
| ------- | ---------- | --------------------------------------------- |
| 0.1.0   | 2024-05-13 | Initial version with users capabilities only. |
