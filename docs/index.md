---
hide:
  - toc
  - navigation
---

# Postgres Wrappers

Wrappers is a framework for PostgreSQL Foreign Data Wrappers.

It helps developers to integrate with external sources using SQL. For example, developers can use the Stripe wrapper to query Stripe data and join the data with customer data inside Postgres:

```sql
select
  customer_id
  currency
from
   stripe.customers;
```

returns

```
    customer_id     | currency
--------------------+-----------
 cus_MJiBtCqOF1Bb3F | usd
(1 row)
```
