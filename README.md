# Supabase Wrappers

Supabase Wrappers is a development framework for Postgres Foreign Data Wrappers ([FDW](https://wiki.postgresql.org/wiki/Foreign_data_wrappers)), written in Rust. Its goal is to make Postgres FDW development easier while keeping Rust language's modern capabilities, such as high performance, strong types, and safety.

## Features

- Minimum interface and easy to implement.
- Support for rich data types.
- Support both sync and async backends, such as RDBMS, RESTful APIs, flat files and etc.
- Built on top of [pgx](https://github.com/tcdi/pgx), providing higher level interfaces, without hiding lower-level C APIs.
- Pushdown is supported.

## Installation

Supabase Wrappers is a pgx extension, so you can follow the [installation steps](https://github.com/tcdi/pgx#system-requirements) as mentioned by pgx.

## Developing a FDW

To develop a FDW using Supabase Wrappers, you only need to implement the [ForeignDataWrapper](./supabase-wrappers/src/interface.rs) trait.

```rust
pub trait ForeignDataWrapper {
    // function for query planning
    fn get_rel_size(...) -> (i64, i32)

    // functions for data scan, e.g. select
    fn begin_scan(...);
    fn iter_scan(...) -> Option<Row>;
    fn re_scan(...);
    fn end_scan(...);

    // functions for data modify, e.g. insert, update and delete
    fn begin_modify(...);
    fn insert(...);
    fn update(...);
    fn delete(...);
    fn end_modify(...);
}
```

In a minimum FDW, which supports data scan only, `begin_scan()`, `iter_scan()` and `end_scan()` are required, all the other functions are optional.

## Supported Wrappers

We support the following FDWs, with more under development:

- `HelloWorld`: `/src/fdw/helloworld_fdw`. A demo FDW to show how to develop a baisc FDW.
- [BigQuery](https://cloud.google.com/bigquery): `/src/fdw/bigquery_fdw`. A FDW for BigQuery which supports async data scan only at this moment. 
- [Clickhouse](https://clickhouse.com/): `/src/fdw/clickhouse_fdw`. A FDW for ClickHouse which supports both async data scan and modify. 
- [Stripe](https://stripe.com/): `/src/fdw/stripe_fdw`. A FDW for Stripe API.

## Basic usage

These steps outline how to use the Clickhouse FDW:

1. Clone this repo

```
git clone https://github.com/supabase/remote.git
```

2. Run it using pgx with feature:

```bash
cargo pgx run --features clickhouse_fdw
```

3. Start a Clickhouse server with some data populated

```
cd test/remotes/clickhouse
docker-compose -f clickhouse.yaml up
```

4. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
create extension remote;

-- create foreign data wrapper and enable 'clickhouse_fdw'
drop foreign data wrapper if exists remote_clickhouse;
create foreign data wrapper remote_clickhouse
  handler remote_handler
  validator remote_validator
  options (
    wrapper 'clickhouse_fdw'
  );

-- create a remote ClickHouse server and specify connection string
drop server if exists my_clickhouse_server;
create server my_clickhouse_server
  foreign data wrapper remote_clickhouse
  options (
    conn_string 'tcp://default:@localhost:9000/default'
  );

-- create an example foreign table
drop foreign table if exists people;
create foreign table people (
  id bigint,
  name text
)
  server my_clickhouse_server
  options (
    table 'people',
    rowid_column 'id',
    startup_cost '42'
  );
```

5. Run some queries to check if it is working:

On Postgres:

```sql
-- data scan
select * from people;

-- data modify
insert into people values (4, 'Yoda') ;
update people set name = 'Princess Leia' where id = 2;
delete from people where id = 3;
```

6. Check the changes on Clickhouse:

```bash
cd test/remotes/clickhouse

# start ClickHouse server
docker-compose -f clickhouse.yaml up

# open another shell and connect to ClickHouse server
docker-compose -f clickhouse.yaml exec server clickhouse-client
```

Run the following SQL on ClickHouse

```sql
-- data scan
select * from people;

-- data modification in CH will appear in PG:
insert into people values (3, 'Han Solo');
```

## Limitations

- Windows is not supported, that limitation inherits from `pgx`.
- Currently only supports PostgreSQL v14.

## Contribution

All contributions, feature requests, bug report or ideas are welcomed.

## License

[Apache License Version 2.0](./LICENSE)

