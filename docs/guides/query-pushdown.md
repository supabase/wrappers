# Query Pushdown

Query pushdown is a technique that enhances query performance by executing parts of the query directly on the data source. It reduces data transfer between the database and the application, enabling faster execution and improved performance.

### What is Query Pushdown?

Query pushdown is a technique that enhances query performance by executing parts of the query directly on the data source. It reduces data transfer between the database and the application, enabling faster execution and improved performance.

![assets/query-pushdown-dark.png](../assets/query-pushdown-dark.png)

### Using Query Pushdown

In Wrappers, the pushdown logic is integrated into each FDW. You don't need to modify your queries to benefit from this feature. For example, the [Stripe FDW](https://supabase.com/docs/guides/database/extensions/wrappers/stripe) automatically applies query pushdown for `id` within the `customer` object:

```sql
select *
from stripe.customers
where id = 'cus_N5WMk7pvQPkY3B';
```

This approach contrasts with fetching and filtering all customers locally, which is less efficient. Query pushdown translates this into a single API call, significantly speeding up the process:

```bash
https://api.stripe.com/v1/customers/cus_N5WMk7pvQPkY3B
```

We can use push down criteria and other query parameters too. For example, [ClickHouse FDW](https://supabase.com/docs/guides/database/extensions/wrappers/clickhouse) supports `order by` and `limit` pushdown:

```sql
select *
from clickhouse.people
order by name
limit 20;
```

This query executes `order by name limit 20` on ClickHouse before transferring the result to Postgres.

### Aggregate Pushdown

Aggregate pushdown allows aggregate functions like `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` to be executed directly on the foreign data source. This is especially valuable for analytics queries where you only need summary statistics rather than raw data.

```sql
select count(*), sum(amount), avg(amount)
from foreign_table
where status = 'active';
```

Instead of fetching all matching rows and computing aggregates locally, the FDW can push the entire aggregation to the remote source. This dramatically reduces data transfer - returning just a single row with the computed values.

#### GROUP BY Support

FDWs that support aggregate pushdown can also support `GROUP BY` pushdown:

```sql
select department, count(*), avg(salary)
from employees
group by department;
```

This executes the grouping and aggregation on the remote server, returning only the grouped results.

#### Supported Aggregate Functions

The Wrappers framework supports pushing down these aggregate functions:

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count all rows |
| `COUNT(column)` | Count non-null values |
| `COUNT(DISTINCT column)` | Count unique non-null values |
| `SUM(column)` | Sum of values |
| `AVG(column)` | Average of values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |

#### Implementing Aggregate Pushdown

FDW developers can enable aggregate pushdown by implementing these trait methods:

```rust
fn supported_aggregates(&self) -> Vec<AggregateKind> {
    vec![AggregateKind::Count, AggregateKind::Sum, AggregateKind::Avg]
}

fn supports_group_by(&self) -> bool {
    true
}

fn begin_aggregate_scan(
    &mut self,
    aggregates: &[Aggregate],
    group_by: &[Column],
    quals: &[Qual],
    options: &HashMap<String, String>,
) -> Result<(), Error> {
    // Build and execute remote aggregate query
    Ok(())
}
```

See the [API documentation](https://docs.rs/supabase-wrappers/latest/supabase_wrappers/) for detailed information on implementing aggregate pushdown.
