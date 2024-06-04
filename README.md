# Wrappers

`Wrappers` is a development framework for Postgres Foreign Data Wrappers ([FDW](https://wiki.postgresql.org/wiki/Foreign_data_wrappers)), written in Rust. Its goal is to make Postgres FDW development easier while keeping Rust language's modern capabilities, such as high performance, strong types, and safety.

`Wrappers` is also a collection of FDWs built by [Supabase](https://www.supabase.com). We currently support the following FDWs, with more under development:

| FDW                                             | Description                                                                   | Read | Modify |
| ----------------------------------------------- | ----------------------------------------------------------------------------- | ---- | ------ |
| [HelloWorld](./wrappers/src/fdw/helloworld_fdw) | A demo FDW to show how to develop a basic FDW.                                |      |        |
| [BigQuery](./wrappers/src/fdw/bigquery_fdw)     | A FDW for Google [BigQuery](https://cloud.google.com/bigquery)                | ✅   | ✅     |
| [Clickhouse](./wrappers/src/fdw/clickhouse_fdw) | A FDW for [ClickHouse](https://clickhouse.com/)                               | ✅   | ✅     |
| [Stripe](./wrappers/src/fdw/stripe_fdw)         | A FDW for [Stripe](https://stripe.com/) API                                   | ✅   | ✅     |
| [Firebase](./wrappers/src/fdw/firebase_fdw)     | A FDW for Google [Firebase](https://firebase.google.com/)                     | ✅   | ❌     |
| [Airtable](./wrappers/src/fdw/airtable_fdw)     | A FDW for [Airtable](https://airtable.com/) API                               | ✅   | ❌     |
| [S3](./wrappers/src/fdw/s3_fdw)                 | A FDW for [AWS S3](https://aws.amazon.com/s3/)                                | ✅   | ❌     |
| [Logflare](./wrappers/src/fdw/logflare_fdw)     | A FDW for [Logflare](https://logflare.app/)                                   | ✅   | ❌     |
| [Auth0](./wrappers/src/fdw/auth0_fdw)           | A FDW for [Auth0](https://auth0.com/)                                         | ✅   | ❌     |
| [SQL Server](./wrappers/src/fdw/mssql_fdw)      | A FDW for [Microsoft SQL Server](https://www.microsoft.com/en-au/sql-server/) | ✅   | ❌     |
| [Redis](./wrappers/src/fdw/redis_fdw)           | A FDW for [Redis](https://redis.io/)                                          | ✅   | ❌     |
| [AWS Cognito](./wrappers/src/fdw/cognito_fdw)   | A FDW for [AWS Cognito](https://aws.amazon.com/cognito/)                      | ✅   | ❌     |
| [Notion](./wrappers/src/fdw/notion_fdw)         | A FDW for [Notion](https://www.notion.so/)                                    | ✅   | ❌     |

### Warning

Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Features

- Minimum interface and easy to implement.
- Support for rich data types.
- Support both sync and async backends, such as RDBMS, RESTful APIs, flat files and etc.
- Built on top of [pgrx](https://github.com/tcdi/pgrx), providing higher level interfaces, without hiding lower-level C APIs.
- `WHERE`, `ORDER BY`, `LIMIT` pushdown are supported.

## Documentation

- [Usage Docs](https://supabase.github.io/wrappers/)
- [Developer Docs (docs.rs)](https://docs.rs/supabase-wrappers/latest/supabase_wrappers/)

## Installation

`Wrappers` is a pgrx extension, you can follow the [pgrx installation steps](https://github.com/tcdi/pgrx#system-requirements) to install Wrappers.

Basically, run below command to install FDW after `pgrx` is installed. For example,

```bash
cargo pgrx install --pg-config [path_to_pg_config] --features stripe_fdw
```

## Developing a FDW

To develop a FDW using `Wrappers`, you only need to implement the [ForeignDataWrapper](./supabase-wrappers/src/interface.rs) trait.

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

These steps outline how to use the a demo FDW [HelloWorldFdw](./wrappers/src/fdw/helloworld_fdw), which only outputs a single line of fake data:

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

## Limitations

- Windows is not supported, that limitation inherits from [pgrx](https://github.com/tcdi/pgrx).
- Currently only supports PostgreSQL v14, v15 and v16.
- Generated column is not supported.

## Contribution

All contributions, feature requests, bug report or ideas are welcomed.

## License

[Apache License Version 2.0](./LICENSE)

[![crates.io badge](https://img.shields.io/crates/v/supabase-wrappers.svg)](https://crates.io/crates/supabase-wrappers)
[![docs.rs badge](https://docs.rs/supabase-wrappers/badge.svg)](https://docs.rs/supabase-wrappers)
[![Test Status](https://img.shields.io/github/actions/workflow/status/supabase/wrappers/test_wrappers.yml?branch=main&label=test)](https://github.com/supabase/wrappers/actions/workflows/test_wrappers.yml)
[![MIT/Apache-2 licensed](https://img.shields.io/crates/l/supabase-wrappers.svg)](./LICENSE)
[![Contributors](https://img.shields.io/github/contributors/supabase/wrappers)](https://github.com/supabase/wrappers/graphs/contributors)
