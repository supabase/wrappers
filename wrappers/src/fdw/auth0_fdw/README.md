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



``` sql
-- create extension
create extension wrappers;
```

``` sql
-- create foreign data wrapper and enable 'Auth0Fdw'
create foreign data wrapper auth0_wrapper
  handler auth0_fdw_handler
  validator auth0_fdw_validator;
```



``` sql
-- create server and specify custom options
create server auth0_server
  foreign data wrapper auth0_wrapper
  options (
    url 'https://dev-<tenant-id>.us.auth0.com/api/v2/users',
    api_key '<your_api_key>'
  );
```


``` sql
-- create an example foreign table
-- Number of fields are illustrative
create foreign table auth0 (
  created_at text,
  email text,
  email_verified bool,
  identities jsonb
)
  server auth0_server
  options (
    object 'users'
  );
```


```

4. Run a query to check if it is working:

```sql
wrappers=# select * from auth0;

created_at     | 2023-11-22T09:52:17.326Z
email          | myname@supabase.io
email_verified | t
identities     | [{"user_id": "<my_user_id>", "isSocial": false, "provider": "auth0", "connection": "Username-Password-Authentication"}]

```

## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.1   | 2023-09-20 | Error reporting refactoring                          |
| 0.1.0   | 2022-11-30 | Initial version                                      |
