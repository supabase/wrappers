# Wrappers

`Wrappers` is a development framework for Postgres Foreign Data Wrappers ([FDW](https://wiki.postgresql.org/wiki/Foreign_data_wrappers)), written in Rust. Its goal is to make Postgres FDW development easier while keeping Rust language's modern capabilities, such as high performance, strong types, and safety.

`Wrappers` is also a collection of FDWs built by [Supabase](https://www.supabase.com). We currently support the following FDWs, with more under development:

| FDW                                             | Description                                                                   | Read | Modify |
| ----------------------------------------------- | ----------------------------------------------------------------------------- | ---- | ------ |
| [AWS Cognito](./wrappers/src/fdw/cognito_fdw)   | A FDW for [AWS Cognito](https://aws.amazon.com/cognito/)                      | ✅   | ❌     |
| [Airtable](./wrappers/src/fdw/airtable_fdw)     | A FDW for [Airtable](https://airtable.com/) API                               | ✅   | ❌     |
| [Apache Iceberg](./wrappers/src/fdw/iceberg_fdw)| A FDW for [Apache Iceberg](https://iceberg.apache.org/)                       | ✅   | ❌     |
| [Auth0](./wrappers/src/fdw/auth0_fdw)           | A FDW for [Auth0](https://auth0.com/)                                         | ✅   | ❌     |
| [BigQuery](./wrappers/src/fdw/bigquery_fdw)     | A FDW for Google [BigQuery](https://cloud.google.com/bigquery)                | ✅   | ✅     |
| [Calendly](./wasm-wrappers/fdw/calendly_fdw)    | A Wasm FDW for [Calendly](https://www.calendly.com/)                          | ✅   | ❌     |
| [Cal.com](./wasm-wrappers/fdw/cal_fdw)          | A Wasm FDW for [Cal.com](https://www.cal.com/)                                | ✅   | ❌     |
| [Clerk](./wasm-wrappers/fdw/clerk_fdw)          | A Wasm FDW for [Clerk](https://www.clerk.com/)                                | ✅   | ❌     |
| [Clickhouse](./wrappers/src/fdw/clickhouse_fdw) | A FDW for [ClickHouse](https://clickhouse.com/)                               | ✅   | ✅     |
| [Cloudflare D1](./wasm-wrappers/fdw/cfd1_fdw)   | A Wasm FDW for [Cloudflare D1](https://developers.cloudflare.com/d1/)         | ✅   | ✅     |
| [DuckDB](./wrappers/src/fdw/duckdb_fdw)         | A FDW for [DuckDB](https://duckdb.org/)                                       | ✅   | ❌     |
| [Firebase](./wrappers/src/fdw/firebase_fdw)     | A FDW for Google [Firebase](https://firebase.google.com/)                     | ✅   | ❌     |
| [HelloWorld](./wrappers/src/fdw/helloworld_fdw) | A demo FDW to show how to develop a basic FDW.                                |      |        |
| [HubSpot](./wasm-wrappers/fdw/hubspot_fdw)      | A Wasm FDW for [HubSpot](https://www.hubspot.com/)                            | ✅   | ❌     |
| [Logflare](./wrappers/src/fdw/logflare_fdw)     | A FDW for [Logflare](https://logflare.app/)                                   | ✅   | ❌     |
| [Notion](./wasm-wrappers/fdw/notion_fdw)        | A Wasm FDW for [Notion](https://www.notion.so/)                               | ✅   | ❌     |
| [Orb](./wasm-wrappers/fdw/orb_fdw)              | A Wasm FDW for [Orb](https://www.withorb.com/)                                | ✅   | ❌     |
| [Paddle](./wasm-wrappers/fdw/paddle_fdw)        | A Wasm FDW for [Paddle](https://www.paddle.com/)                              | ✅   | ✅     |
| [Redis](./wrappers/src/fdw/redis_fdw)           | A FDW for [Redis](https://redis.io/)                                          | ✅   | ❌     |
| [S3](./wrappers/src/fdw/s3_fdw)                 | A FDW for [AWS S3](https://aws.amazon.com/s3/)                                | ✅   | ❌     |
| [S3 Vectors](./wrappers/src/fdw/s3vectors_fdw)  | A FDW for [AWS S3 Vectors](https://aws.amazon.com/s3/features/vectors/)       | ✅   | ✅     |
| [SQL Server](./wrappers/src/fdw/mssql_fdw)      | A FDW for [Microsoft SQL Server](https://www.microsoft.com/en-au/sql-server/) | ✅   | ❌     |
| [Slack](./wasm-wrappers/fdw/slack_fdw)          | A Wasm FDW for [Slack](https://www.slack.com/)                                | ✅   | ❌     |
| [Snowflake](./wasm-wrappers/fdw/snowflake_fdw)  | A Wasm FDW for [Snowflake](https://www.snowflake.com/)                        | ✅   | ✅     |
| [Stripe](./wrappers/src/fdw/stripe_fdw)         | A FDW for [Stripe](https://stripe.com/) API                                   | ✅   | ✅     |

### Warning

Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Features

- Minimum interface and easy to implement.
- Support for rich data types.
- Support both sync and async backends, such as RDBMS, RESTful APIs, flat files and etc.
- Built on top of [pgrx](https://github.com/tcdi/pgrx), providing higher level interfaces, without hiding lower-level C APIs.
- `WHERE`, `ORDER BY`, `LIMIT` pushdown are supported.

## Documentation

- [Usage Docs](https://fdw.dev/)
- [Developer Docs (docs.rs)](https://docs.rs/supabase-wrappers/latest/supabase_wrappers/)

## Installation

`Wrappers` is a pgrx extension, you can follow the [pgrx installation steps](https://github.com/tcdi/pgrx#system-requirements) to install Wrappers.

Basically, run below command to install FDW after `pgrx` is installed. For example,

```bash
cargo pgrx install --pg-config [path_to_pg_config] --features stripe_fdw
```

## Developing a FDW

Visit [Wrappers Docs](https://fdw.dev/) for more details.

## License

[Apache License Version 2.0](./LICENSE)

[![crates.io badge](https://img.shields.io/crates/v/supabase-wrappers.svg)](https://crates.io/crates/supabase-wrappers)
[![docs.rs badge](https://docs.rs/supabase-wrappers/badge.svg)](https://docs.rs/supabase-wrappers)
[![Test Status](https://img.shields.io/github/actions/workflow/status/supabase/wrappers/test_wrappers.yml?branch=main&label=test)](https://github.com/supabase/wrappers/actions/workflows/test_wrappers.yml)
[![MIT/Apache-2 licensed](https://img.shields.io/crates/l/supabase-wrappers.svg)](./LICENSE)
[![Contributors](https://img.shields.io/github/contributors/supabase/wrappers)](https://github.com/supabase/wrappers/graphs/contributors)
