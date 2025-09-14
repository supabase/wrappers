---
hide:
  - toc
---

# Catalog

Each FDW documentation includes a detailed "Limitations" section that describes important considerations and potential pitfalls when using that specific FDW.

## Official

| Integration   | Select | Insert | Update | Delete | Truncate | Push Down |
| ------------- | :----: | :----: | :----: | :----: | :------: | :-------: |
| Airtable      |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Auth0         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| AWS Cognito   |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| BigQuery      |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| Cal.com       |   ✅   |   ✅   |   ❌   |   ❌   |    ❌    |    ❌     |
| Calendly      |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Clerk         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| ClickHouse    |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| Cloudflare D1 |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| DuckDB        |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Firebase      |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Gravatar      |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| HubSpot       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Iceberg       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Logflare      |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Notion        |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Orb           |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| Paddle        |   ✅   |   ✅   |   ✅   |   ❌   |    ❌    |    ✅     |
| Redis         |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| S3            |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| S3 Vectors    |   ✅   |   ✅   |   ❌   |   ✅   |    ❌    |    ✅     |
| Shopify       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ❌     |
| Snowflake     |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| Stripe        |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |    ✅     |
| SQL Server    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |    ✅     |

## Community

Wasm wrappers can be installed directly from GitHub or any external source.

See [Developing a Wasm Wrapper](../guides/create-wasm-wrapper.md) for instructions on how to build and develop your own.

| Integration   |            Developer             |         Docs         |                                         Source                                         |
| :-----------: | :------------------------------: | :------------------: | :------------------------------------------------------------------------------------: |
| Cal.com       | [Supabase](https://supabase.com) | [Link](cal.md)       | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/cal_fdw)       |
| Calendly      | [Supabase](https://supabase.com) | [Link](calendly.md)  | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/calendly_fdw)  |
| Clerk         | [Supabase](https://supabase.com) | [Link](clerk.md)     | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/clerk_fdw)  |
| Cloudflare D1 | [Supabase](https://supabase.com) | [Link](cfd1.md)      | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/cfd1_fdw)      |
| Gravatar      | [Automattic](https://automattic.com) | [Link](gravatar.md)  | [Link](https://github.com/Automattic/gravatar-wasm-fdw)  |
| HubSpot       | [Supabase](https://supabase.com) | [Link](hubspot.md)   | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/hubspot_fdw)   |
| Notion        | [Supabase](https://supabase.com) | [Link](notion.md)    | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/notion_fdw)    |
| Orb           | [Supabase](https://supabase.com) | [Link](orb.md)       | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/orb_fdw)  |
| Paddle        | [Supabase](https://supabase.com) | [Link](paddle.md)    | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/paddle_fdw)    |
| Shopify       | [Supabase](https://supabase.com) | [Link](shopify.md)   | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/shopify_fdw)   |
| Snowflake     | [Supabase](https://supabase.com) | [Link](snowflake.md) | [Link](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/snowflake_fdw) |
