# Airtable Foreign Data Wrapper

This is a foreign data wrapper for [Airtable](https://airtable.com/). It is developed using [Wrappers](https://github.com/supabase/wrappers).

## Basic usage

These steps outline how to use the Airtable FDW:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgx with feature:

```bash
cd wrappers/wrappers
cargo pgx run --features airtable_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
drop extension if exists wrappers cascade;
create extension wrappers;

-- create foreign data wrapper and enable 'AirtableFdw'
drop foreign data wrapper if exists airtable_wrapper cascade;
create foreign data wrapper airtable_wrapper
  handler wrappers_handler
  validator wrappers_validator
  options (
    wrapper 'AirtableFdw'
  );

-- create a wrappers Airtable server and specify connection info
drop server if exists my_airtable_server cascade;
create server my_airtable_server
  foreign data wrapper airtable_wrapper
  options (
    api_url 'https://api.airtable.com/v0',  -- Airtable API base URL, optional
    api_key 'at_test_key'  -- Airtable API Key, required
  );

-- create an example foreign table
drop foreign table if exists t1;
create foreign table t1(
    id text, -- The builtin "id" field in Airtable
    name text, -- The fields in your Airtable table. Airtable is case insensitive so capitalization does not matter.
    status text,
)
  server my_airtable_server
  options (
    base_id 'at_base_id' -- Airtable Base ID, required
    table 'My Table Name' -- Airtable Table Name (or ID), required
  );
```

4. Run some queries to check if it is working:

On Postgres:

```sql
select * from t1;
```
