# Wrappers

[![crates.io badge](https://img.shields.io/crates/v/supabase-wrappers.svg)](https://crates.io/crates/supabase-wrappers)
[![docs.rs badge](https://docs.rs/supabase-wrappers/badge.svg)](https://docs.rs/supabase-wrappers)
[![Test Status](https://img.shields.io/github/actions/workflow/status/supabase/wrappers/test_wrappers.yml?branch=main&label=test)](https://github.com/supabase/wrappers/actions/workflows/test_wrappers.yml)
[![MIT/Apache-2 licensed](https://img.shields.io/crates/l/supabase-wrappers.svg)](./LICENSE)
[![Contributors](https://img.shields.io/github/contributors/supabase/wrappers)](https://github.com/supabase/wrappers/graphs/contributors)

`Wrappers` is a development framework for Postgres Foreign Data Wrappers ([FDW](https://wiki.postgresql.org/wiki/Foreign_data_wrappers)), written in Rust. Its goal is to make Postgres FDW development easier while keeping Rust language's modern capabilities, such as high performance, strong types, and safety.

`Wrappers` is also a collection of FDWs built by [Supabase](https://www.supabase.com). We currently support the following FDWs, with more are under development:

- [HelloWorld](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/helloworld_fdw): A demo FDW to show how to develop a baisc FDW.
- [BigQuery](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/bigquery_fdw): A FDW for Google [BigQuery](https://cloud.google.com/bigquery) which supports data read and modify.
- [Clickhouse](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/clickhouse_fdw): A FDW for [ClickHouse](https://clickhouse.com/) which supports data read and modify.
- [Stripe](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/stripe_fdw): A FDW for [Stripe](https://stripe.com/) API which supports data read and modify.
- [Firebase](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/firebase_fdw): A FDW for Google [Firebase](https://firebase.google.com/) which supports data read only.
- [Airtable](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/airtable_fdw): A FDW for [Airtable](https://airtable.com/) API which supports data read only.
- [S3](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/s3_fdw): A FDW for [AWS S3](https://aws.amazon.com/s3/) which supports data read only.
- [Logflare](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/logflare_fdw): A FDW for [Logflare](https://logflare.app/) which supports data read only.
- [Auth0](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/auth0_fdw): A FDW for [Auth0](https://auth0.com/).
- [SQL Server](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/mssql_fdw): A FDW for [Microsoft SQL Server](https://www.microsoft.com/en-au/sql-server/) which supports data read only.
- [Redis](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/redis_fdw): A FDW for [Redis](https://redis.io/) which supports data read only.

## Features

- Minimum interface and easy to implement.
- Support for rich data types.
- Support both sync and async backends, such as RDBMS, RESTful APIs, flat files and etc.
- Built on top of [pgrx](https://github.com/tcdi/pgrx), providing higher level interfaces, without hiding lower-level C APIs.
- `WHERE`, `ORDER BY`, `LIMIT` pushdown are supported.

## Documentation

[Documentation on docs.rs](https://docs.rs/supabase-wrappers/latest/supabase_wrappers/).

## Installation

`Wrappers` is a pgrx extension, you can follow the [pgrx installation steps](https://github.com/tcdi/pgrx#system-requirements) to install Wrappers.

Basically, run below command to install FDW after `pgrx` is installed. For example,

```bash
cargo pgrx install --pg-config [path_to_pg_config] --features stripe_fdw
```

## Developing a FDW

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
cargo pgrx run --features helloworld_fdw
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

## Limitations

- Windows is not supported, that limitation inherits from [pgrx](https://github.com/tcdi/pgrx).
- Currently only supports PostgreSQL v14, v15 and v16.
- Generated column is not supported.

## Contribution

All contributions, feature requests, bug report or ideas are welcomed.

## License

[Apache License Version 2.0](./LICENSE)

