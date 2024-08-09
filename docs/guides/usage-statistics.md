# FDW Usage Statistics

Quantitative metrics are useful when working with Postgres FDWs because of their impact on performance optimization, monitoring, and query planning across distributed databases.

You can use the FDW statistics to identify bottlenecks, latency issues, and inefficiencies in data retrieval.

## Querying FDW Statistics

```sql
select *
from extensions.wrappers_fdw_stats;
```

## Statistics Reference

- `create_times` - number of times the FDW instance has been created
- `rows_in` - number of rows transferred from source
- `rows_out` - number of rows transferred to source
- `bytes_in` - number of bytes transferred from source
- `bytes_out` - number of bytes transferred to source
- `metadata` - additional usage statistics specific to a FDW
