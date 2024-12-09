---
hide:
  - toc
---

# Catalog

## Official

| Integration | Select | Insert | Update | Delete | Truncate | Push Down |
| ----------- | :----: | :----: | :----: | :----: | :------: | :-------: |
| Airtable    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Auth0       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| AWS Cognito |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| BigQuery    |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| Cal.com     |   ✅   |   ✅   |   ❌   |   ❌   |    ❌    |    ❌     |
| Calendly    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| ClickHouse  |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| Firebase    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Logflare    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Notion      |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Paddle      |   ✅   |   ✅   |   ✅   |   ❌   |    ❌    |    ✅     |
| Redis       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| S3          |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Snowflake   |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| Stripe      |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| SQL Server  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ✅     |

## Community

Wasm wrappers can be installed directly from GitHub or any external source.

See [Developing a Wasm Wrapper](../guides/create-wasm-wrapper.md) for instructions on how to build and develop your own.

| Integration |            Developer             |                 Docs                 |                                         Source                                         |
| ----------- | :------------------------------: | :----------------------------------: | :------------------------------------------------------------------------------------: |
| Cal.com     | [Supabase](https://supabase.com) | [Link](cal.md)       | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/cal_fdw) |
| Calendly    | [Supabase](https://supabase.com) | [Link](calendly.md)  | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/calendly_fdw) |
| Notion      | [Supabase](https://supabase.com) | [Link](notion.md)    | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/notion_fdw) |
| Paddle      | [Supabase](https://supabase.com) | [Link](paddle.md)    | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/paddle_fdw)   |
| Snowflake   | [Supabase](https://supabase.com) | [Link](snowflake.md) | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/snowflake_fdw) |
