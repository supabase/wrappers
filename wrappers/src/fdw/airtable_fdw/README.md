# Airtable Foreign Data Wrapper

This is a foreign data wrapper for [Airtable](https://airtable.com/). It is developed using [Wrappers](https://github.com/supabase/wrappers).

## Basic usage

These steps outline how to use the Airtable FDW:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgrx with feature:

```bash
cd wrappers/wrappers
cargo pgrx run --features airtable_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
create extension wrappers;

-- create foreign data wrapper and enable 'AirtableFdw'
create foreign data wrapper airtable_wrapper
  handler airtable_fdw_handler
  validator airtable_fdw_validator;

-- create a wrappers Airtable server and specify connection info
create server my_airtable_server
  foreign data wrapper airtable_wrapper
  options (
    api_url 'https://api.airtable.com/v0',  -- Airtable API base URL, optional
    api_key 'at_test_key'  -- Airtable API Key, required
  );

-- create an example foreign table
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

## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.0   | 2022-11-30 | Initial version                                      |
