# `supabase/wrappers`

<p>
<a href=""><img src="https://img.shields.io/badge/postgresql-14+-blue.svg" alt="PostgreSQL version" height="18"></a>
<a href="https://github.com/supabase/wrappers/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/markdown-subtemplate.svg" alt="License" height="18"></a>
<a href="https://github.com/supabase/wrappers/actions"><img src="https://github.com/supabase/wrappers/actions/workflows/test_wrappers.yml/badge.svg" alt="Tests" height="18"></a>

</p>

---

**Documentation**: <a href="https://supabase.github.io/wrappers" target="_blank">https://supabase.github.io/wrappers</a>

**Source Code**: <a href="https://github.com/supabase/wrappers" target="_blank">https://github.com/supabase/wrappers</a>

---

## Overview

`supabase/wrappers` is a PostgreSQL extension that provides integrations with external data so you can interact with third-party data from SQL.

Currently `supabase/wrappers` supports:

| Integration | Status   | Select            | Insert            | Update            | Delete            | Truncate          |
| ----------- | -------- | :----:            | :----:            | :----:            | :----:            | :----:            |
| Firebase    | Stable   | :white_check_mark:| :x:               | :x:               | :x:               | :x:               |
| Stripe      | Stable   | :white_check_mark:| :x:               | :x:               | :x:               | :x:               |
| BigQuery    | Unstable | :white_check_mark:| :white_check_mark:| :white_check_mark:| :white_check_mark:| :x:               |
| ClickHouse  | Unstable | :white_check_mark:| :white_check_mark:| :white_check_mark:| :white_check_mark:| :x:               |

For example, setting up the Stripe extension enables:
```sql
-- Returns all of your stripe customers
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


Check out [usage](usage.md) for more info on how to get started.
