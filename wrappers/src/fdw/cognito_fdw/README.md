# Cognito (AWS) Foreign Data Wrapper

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
cargo pgrx run --features cognito_fdw
```

3. Create the extension, foreign data wrapper and related objects:



``` sql
-- create extension
create extension wrappers;
```

``` sql
-- create foreign data wrapper and enable 'CognitoFdw'
create foreign data wrapper cognito0_wrapper
  handler cognito_fdw_handler
  validator cognito_fdw_validator;
```



``` sql
-- create server and specify custom options
create server cognito_server
  foreign data wrapper cognito_wrapper
  options (
     -- TODO: Fill this up
  );
```


``` sql
-- create an example foreign table
-- Number of fields are illustrative
create foreign table cognito (
  created_at text,
  email text,
  email_verified bool,
  identities jsonb
)
  server cognito_server
  options (
    object 'users'
  );
```


```

4. Run a query to check if it is working:

```sql
wrappers=# select * from cognito;
-- TODO: finish this

```

## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.0   | 2024-01-02 | Initial version                                      |