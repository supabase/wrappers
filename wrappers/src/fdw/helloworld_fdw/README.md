# HelloWorld Foreign Data Wrapper

This is a demo foreign data wrapper which is developed using [Wrappers](https://github.com/supabase/wrappers).

## Basic usage

These steps outline how to use the this FDW:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgx with feature:

```bash
cd wrappers/wrappers
cargo pgx run --features helloworld_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
drop extension if exists wrappers cascade;
create extension wrappers;

-- create foreign data wrapper and enable 'HelloWorldFdw'
drop foreign data wrapper if exists wrappers_helloworld cascade;
create foreign data wrapper wrappers_helloworld
  handler wrappers_handler
  validator wrappers_validator
  options (
    wrapper 'HelloWorldFdw'
  );

-- create server and specify custom options
drop server if exists my_helloworld_server cascade;
create server my_helloworld_server
  foreign data wrapper wrappers_helloworld
  options (
    foo 'bar'
  );

-- create an example foreign table
drop foreign table if exists balance;
create foreign table hello (
  id bigint,
  col text
)
  server my_helloworld_server
  options (
    foo 'bar'
  );
```

4. Run some queries to check if it is working:

```sql
wrappers=# select * from hello;
 id |    col
----+-------------
  0 | Hello world
(1 row)
```


