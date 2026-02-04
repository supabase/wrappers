# ClickHouse Foreign Data Wrapper

This is a foreign data wrapper for [ClickHouse](https://clickhouse.com/). It is developed using [Wrappers](https://github.com/supabase/wrappers) and supports both data scan and modify.

## Documentation

[https://fdw.dev/catalog/clickhouse/](https://fdw.dev/catalog/clickhouse/)


## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.10  | 2026-02-04 | Implement re_scan() for nested loop joins            |
| 0.1.9   | 2025-11-08 | Added stream_buffer_size foreign table option        |
| 0.1.8   | 2025-10-27 | Refactor to read rows with async streaming           |
| 0.1.7   | 2025-05-22 | Added more data types support                        |
| 0.1.6   | 2025-05-06 | Added UUID data type support                         |
| 0.1.5   | 2024-09-30 | Support for pgrx 0.12.6                              |
| 0.1.4   | 2024-09-10 | Added Nullable type suppport                         |
| 0.1.3   | 2023-07-17 | Added sort and limit pushdown suppport               |
| 0.1.2   | 2023-07-13 | Added fdw stats collection                           |
| 0.1.1   | 2023-05-19 | Added custom sql support                             |
| 0.1.0   | 2022-11-30 | Initial version                                      |
