---
hide:
  - navigation
---

# Postgres Wrappers

Wrappers is a Rust framework for developing PostgreSQL Foreign Data Wrappers.

## What is a Foreign Data Wrapper?

Foreign Data Wrappers (FDW) are a core feature of Postgres that allow you to access and query data stored in external data sources as if they were native Postgres tables.

Postgres includes several built-in foreign data wrappers, such as `postgres_fdw` for accessing other PostgreSQL databases, and `file_fdw` for reading data from files.

## The Wrappers Framework

The Wrappers framework extends the Postgres FDW feature. You can use it to query other databases or any other external systems. For example, developers can use the Stripe wrapper to query Stripe data and join the data with customer data inside Postgres:

```sql
select
  customer_id,
  name
from
  stripe.customers;
```

returns

```
    customer_id     | name
--------------------+-----------
 cus_NffrFeUfNV2Hib | Jenny Rosen
(1 row)
```

## Concepts

Postgres FDWs introduce the concept of a "remote server" and "foreign table":

![FDW](/wrappers/assets/fdw-light.png)

### Remote servers

A Remote Server is an external database, API, or any system containing data that you want to query from your Postgres database. Examples include:

- An external database, like Postgres or Firebase.
- A remote data warehouse, like ClickHouse, BigQuery, or Snowflake.
- An API, like Stripe or GitHub.

It's possible to connect to multiple remote servers of the same type. For example, you can connect to two different Firebase projects within the same Postgres database.

### Foreign tables

A table in your database which maps to some data inside a Remote Server.

Examples:

- An `analytics` table which maps to a table inside your data warehouse.
- A `subscriptions` table which maps to your Stripe subscriptions.
- A `collections` table which maps to a Firebase collection.

Although a foreign table behaves like any other table, the data is not stored inside your database. The data remains inside the Remote Server.

## Supported platforms

The following Postgres providers support Wrappers:

- [supabase.com](https://supabase.com)
