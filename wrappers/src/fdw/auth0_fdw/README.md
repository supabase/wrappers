# Auth0 Foreign Data Wrapper

This is a demo foreign data wrapper which is developed using [Wrappers](https://github.com/supabase/wrappers).

## Basic usage

These steps outline how to use the this FDW:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgrx with feature:

```bash
cd wrappers/wrappers
cargo pgrx run --features auth0_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
create extension wrappers;

-- create foreign data wrapper and enable 'HelloWorldFdw'
create foreign data wrapper auth0_wrapper
  handler auth0_fdw_handler
  validator auth0_fdw_validator;

-- create server and specify custom options
create server auth0_server
  foreign data wrapper auth0_wrapper
  options (
    foo 'bar'
  );

-- create an example foreign table
create foreign table auth0 (
  id bigint,
  col text
)
  server auth0_server
  options (
    foo 'bar'
  );
```

4. Run a query to check if it is working:

```sql
wrappers=# select * from auth0;
 id |    col
----+-------------
  0 | Hello world
(1 row)
```

## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.1   | 2023-09-20 | Error reporting refactoring                          |
| 0.1.0   | 2022-11-30 | Initial version                                      |
