# BigQuery Foreign Data Wrapper

This is a foreign data wrapper for [BigQuery](https://cloud.google.com/bigquery). It is developed using [Wrappers](https://github.com/supabase/wrappers) and only supports data scan at this moment.

## Documentation

[https://supabase.github.io/wrappers/bigquery/](https://supabase.github.io/wrappers/bigquery/)


## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.4   | 2023-07-13 | Added fdw stats collection                           |
| 0.1.3   | 2023-04-03 | Added support for `NUMERIC` type                     |
| 0.1.2   | 2023-03-15 | Added subquery support for `table` option            |
| 0.1.1   | 2023-02-15 | Upgrade bq client lib to v0.16.5, code improvement   |
| 0.1.0   | 2022-11-30 | Initial version                                      |
