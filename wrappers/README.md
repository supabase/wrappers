# Postgres Foreign Data Wrappers by Supabase

This is a collection of FDWs built by [Supabase](https://www.supabase.com). We currently support the following FDWs, with more are under development:

- [HelloWorld](./src/fdw/helloworld_fdw): A demo FDW to show how to develop a baisc FDW.
- [BigQuery](./src/fdw/bigquery_fdw): A FDW for [BigQuery](https://cloud.google.com/bigquery) which only supports async data scan at this moment. 
- [Clickhouse](./src/fdw/clickhouse_fdw): A FDW for [ClickHouse](https://clickhouse.com/) which supports both async data scan and modify. 
- [Stripe](./src/fdw/stripe_fdw): A FDW for [Stripe](https://stripe.com/) API.

