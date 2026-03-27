# Developing a Native Wrapper

!!! warning

    We are not accepting native community Wrappers this repo until the Wrappers API is stabilized. If you have an idea for a Wrapper, you can [vote your favorite Wrapper](https://github.com/orgs/supabase/discussions/26627). Once we release Wrappers 1.0, we will support native community Wrappers within the Wrappers repo.

    In the meantime you can develop a Wasm wrapper, which can be installed on any Postgres instance with `wrappers v0.4.0+`.

To develop a FDW using `Wrappers`, you only need to implement the [ForeignDataWrapper](https://github.com/supabase/wrappers/blob/main/supabase-wrappers/src/interface.rs) trait.

```rust
pub trait ForeignDataWrapper {
    // create a FDW instance
    fn new(...) -> Self;

    // functions for data scan, e.g. select
    fn begin_scan(...);
    fn iter_scan(...) -> Option<Row>;
    fn end_scan(...);

    // functions for data modify, e.g. insert, update and delete
    fn begin_modify(...);
    fn insert(...);
    fn update(...);
    fn delete(...);
    fn end_modify(...);

    // functions for aggregate pushdown (optional)
    fn supported_aggregates(...) -> Vec<AggregateKind>;
    fn supports_group_by(...) -> bool;
    fn get_aggregate_rel_size(...) -> (i64, i32);
    fn begin_aggregate_scan(...);

    // other optional functions
    ...
}
```

In a minimum FDW, which supports data scan only, `new()`, `begin_scan()`, `iter_scan()` and `end_scan()` are required, all the other functions are optional.

To know more about FDW development, please visit the [Wrappers documentation](https://docs.rs/supabase-wrappers/latest/supabase_wrappers/).

## Basic usage

These steps outline how to use the a demo FDW [HelloWorldFdw](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/helloworld_fdw), which only outputs a single line of fake data:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgrx with feature:

```bash
cd wrappers/wrappers
cargo pgrx run pg15 --features helloworld_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
create extension wrappers;

-- create foreign data wrapper and enable 'HelloWorldFdw'
create foreign data wrapper helloworld_wrapper
  handler hello_world_fdw_handler
  validator hello_world_fdw_validator;

-- create server and specify custom options
create server my_helloworld_server
  foreign data wrapper helloworld_wrapper
  options (
    foo 'bar'
  );

-- create an example foreign table
create foreign table hello (
  id bigint,
  col text
)
  server my_helloworld_server
  options (
    foo 'bar'
  );
```

4. Run a query to check if it is working:

```sql
wrappers=# select * from hello;
 id |    col
----+-------------
  0 | Hello world
(1 row)
```

## Running tests

In order to run tests in `wrappers`:

```bash
docker-compose -f .ci/docker-compose.yaml up -d
cargo pgrx test --features all_fdws,pg15
```

## Contribution

All contributions, feature requests, bug report or ideas are welcomed.
