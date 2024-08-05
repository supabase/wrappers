# Query Pushdown

Remote subqueries enable the use of prepared data on a remote server, which is beneficial for complex queries or sensitive data protection.

### Static Subqueries

In its most basic form, you can map a query on the remote server into a foreign table in Postgres. For instance:

```sql
create foreign table clickhouse.people (
  id bigint,
  name text,
  age bigint
)
server clickhouse_server
options (
  table '(select * from people where age < 25)'
);
```

In this example, the foreign table `clickhouse.people` data is read from the result of the subquery `select * from people where age < 25` which runs on ClickHouse server.

### Dynamic Subqueries

What if the query is not fixed and needs to be dynamic? For example, ClickHouse provides [Parameterized Views](https://clickhouse.com/docs/en/sql-reference/statements/create/view#parameterized-view) which can accept parameters for a view. Wrappers supports this by defining a column for each parameter.

Let's take a look at an example:

```sql
create foreign table clickhouse.my_table (
  id bigint,
  col1 text,
  col2 bigint,
  _param1 text,
  _param2 bigint
)
server clickhouse_server
options (
  table '(select * from my_view(column1=${_param1}, column2=${_param2}))'
);
```

You can then pass values to these parameters in your query:

```sql
select id, col1, col2
from clickhouse.my_table
where _param1 = 'abc' and _param2 = 42;
```

Currently, this feature is supported by [ClickHouse FDW](https://supabase.com/docs/guides/database/extensions/wrappers/clickhouse) and [BigQuery FDW](https://supabase.com/docs/guides/database/extensions/wrappers/bigquery), with plans to expand support in the future.
