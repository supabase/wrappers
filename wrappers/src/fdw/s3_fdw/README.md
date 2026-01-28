# AWS S3 Foreign Data Wrapper

This is a foreign data wrapper for [AWS S3](https://aws.amazon.com/s3/). It is developed using [Wrappers](https://github.com/supabase/wrappers) and only supports data scan at this moment.

## Documentation

[https://fdw.dev/catalog/s3/](https://fdw.dev/catalog/s3/)

## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.6   | 2026-01-21 | Added csv delimiter foreign table option             |
| 0.1.5   | 2025-07-25 | Fixed parquet file reading position issue            |
| 0.1.4   | 2024-08-20 | Added `path_style_url` server option                 |
| 0.1.2   | 2023-07-13 | Added fdw stats collection                           |
| 0.1.1   | 2023-06-05 | Added Parquet file support                           |
| 0.1.0   | 2023-03-01 | Initial version                                      |
