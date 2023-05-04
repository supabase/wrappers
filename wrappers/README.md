# Postgres Foreign Data Wrappers by Supabase

This is a collection of FDWs built by [Supabase](https://www.supabase.com). We currently support the following FDWs, with more are under development:

- [HelloWorld](./src/fdw/helloworld_fdw): A demo FDW to show how to develop a basic FDW.
- [BigQuery](./src/fdw/bigquery_fdw): A FDW for Google [BigQuery](https://cloud.google.com/bigquery) which supports data read and modify.
- [Clickhouse](./src/fdw/clickhouse_fdw): A FDW for [ClickHouse](https://clickhouse.com/) which supports data read and modify.
- [Stripe](./src/fdw/stripe_fdw): A FDW for [Stripe](https://stripe.com/) API which supports data read and modify.
- [Firebase](./src/fdw/firebase_fdw): A FDW for Google [Firebase](https://firebase.google.com/) which supports data read only.
- [Airtable](./src/fdw/airtable_fdw): A FDW for [Airtable](https://airtable.com/) API which supports data read only.
- [S3](./src/fdw/s3_fdw): A FDW for [AWS S3](https://aws.amazon.com/s3/). Currently read-only.

