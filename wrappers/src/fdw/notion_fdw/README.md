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
-- Create the extension (if not already created)
CREATE extension wrappers;
```

```sql
-- Create the foreign data wrapper and specify the handler and validator functions
CREATE FOREIGN DATA WRAPPER notion_wrapper
  HANDLER notion_fdw_handler
  VALIDATOR notion_fdw_validator;
```

```sql
-- Create a server object with the necessary options
CREATE SERVER notion_server
  FOREIGN DATA WRAPPER notion_wrapper
  OPTIONS (
    api_key '<your_api_key>',
    notion_version '2022-06-28', -- optional, default is '2022-06-28'
    api_url 'https://api.notion.com/v1/' -- optional, default is 'https://api.notion.com/v1/'
  );
```

```sql
-- Create an example foreign tabl
-- The number of fields are illustrative
CREATE FOREIGN TABLE notion_users (
    id text,
    name text,
    type text,
    person jsonb,
    bot jsonb
) SERVER notion_server OPTIONS (
    object 'users'
);
```


4. Run a query to check if it is working:

```bash
wrappers=# SELECT * FROM notion_users;
-[ RECORD 1 ]--------------------------------------------------------------------------------------------
id     | ad4d1b3b-4f3b-4b7e-8f1b-3f1f2f3f1f2f
name   | John Doe
type   | person
person | {"email": "john@doe.com", "name": "John Doe"}
bot    | 
-[ RECORD 2 ]--------------------------------------------------------------------------------------------
id     | 45f3b4b3-4f3b-4b7e-8f1b-12a3b4c5d6e7
name   | Beep Boop
type   | bot
person | 
bot    | {"owner": {"type": "workspace", "workspace": true}, "workspace_name": "John's workspace"}
````

## Changelog

| Version | Date       | Notes                                         |
| ------- | ---------- | --------------------------------------------- |
| 0.1.0   | 2024-05-13 | Initial version with users capabilities only. |
