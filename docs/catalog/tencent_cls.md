---
source:
documentation:
author: 陈杨文(https://github.com/wenerme)
tags:
  - native
  - community
---

# Tencent CLS

[Tencent Cloud Log Service (CLS)](https://www.tencentcloud.com/products/cls) is a centralized log management service for collection, storage, search, and analysis.

The Tencent CLS Wrapper allows you to query CLS log data within your Postgres database using CQL (CLS Query Language).

## Preparation

Before you can query CLS, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Tencent CLS Wrapper

Enable the `tencent_cls_wrapper` FDW:

```sql
create foreign data wrapper tencent_cls_wrapper
  handler tencent_cls_fdw_handler
  validator tencent_cls_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Tencent Cloud SecretId in Vault
select vault.create_secret(
  'AKIDxxxxxxxxxxxxxxxx',
  'tencent_secret_id',
  'Tencent Cloud SecretId for CLS'
);

-- Save your Tencent Cloud SecretKey in Vault
select vault.create_secret(
  'xxxxxxxxxxxxxxxxxxxxxxxx',
  'tencent_secret_key',
  'Tencent Cloud SecretKey for CLS'
);
```

### Connecting to CLS

We need to provide Postgres with the credentials to connect to Tencent CLS. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server tencent_cls_server
      foreign data wrapper tencent_cls_wrapper
      options (
        secret_id_id '<secret_id vault id>',
        secret_key_id '<secret_key vault id>',
        region 'ap-shanghai'
      );
    ```

=== "Without Vault"

    ```sql
    create server tencent_cls_server
      foreign data wrapper tencent_cls_wrapper
      options (
        secret_id 'AKIDxxxxxxxxxxxxxxxx',
        secret_key 'xxxxxxxxxxxxxxxxxxxxxxxx',
        region 'ap-shanghai'
      );
    ```

#### Server options

| Option | Required | Description |
|--------|----------|-------------|
| `secret_id` | Yes* | Tencent Cloud SecretId (plain text) |
| `secret_id_id` | Yes* | Vault secret UUID or name for SecretId |
| `secret_key` | Yes* | Tencent Cloud SecretKey (plain text) |
| `secret_key_id` | Yes* | Vault secret UUID or name for SecretKey |
| `region` | No | Tencent Cloud region (default: `ap-shanghai`) |
| `endpoint` | No | Custom CLS API endpoint |
| `max_response_size` | No | Max response body size in bytes (default: 10MB) |

\* Either `secret_id` or `secret_id_id` is required. Same for `secret_key` / `secret_key_id`.

## Creating Foreign Tables

The CLS Wrapper maps CLS log records to Postgres rows. You define columns based on the fields in your logs.

```sql
create foreign table cls_logs (
  ts         timestamptz,  -- log timestamp (from CLS Time field)
  log        text,         -- raw LogJson string
  source     text,         -- log source
  topic_id   text,         -- CLS topic ID
  topic_name text,         -- CLS topic name
  file_name  text,         -- log file name
  host_name  text,         -- host name
  _result    text,         -- full CLS result JSON (meta column)
  -- Add any custom columns matching your LogJson fields:
  level      text,
  message    text,
  service    text
)
  server tencent_cls_server
  options (
    topic 'my-log-topic',
    query '*',
    limit '100'
  );
```

### Table options

| Option | Required | Description |
|--------|----------|-------------|
| `topic` | Yes | CLS topic name (auto-resolved to topic ID) or topic ID |
| `topic_id` | Yes | Alternative to `topic` — direct topic ID |
| `query` | No | Default CQL query expression (default: `*`) |
| `syntax_rule` | No | `0` for Lucene, `1` for CQL (default: `1`) |
| `limit` | No | Max rows to return (default: `1000`) |

### Column mapping

| Column name | Source | Description |
|-------------|--------|-------------|
| `ts` | `Time` (milliseconds) | Converted to `timestamptz` |
| `log` | `LogJson` | Raw JSON string |
| `source` | `Source` | Log source |
| `topic_id` | `TopicId` | Topic ID |
| `topic_name` | `TopicName` | Topic name |
| `file_name` | `FileName` | File name |
| `host_name` | `HostName` | Host name |
| `_result` | Full result | Complete CLS result as JSON |
| *other* | `LogJson.{name}` | Extracted from LogJson by column name |

## Query Pushdown

The CLS Wrapper supports WHERE clause pushdown for efficient log querying:

```sql
-- Timestamp range pushdown (required for efficient queries)
select * from cls_logs
where ts >= now() - interval '1 hour'
  and ts < now();

-- Direct CQL query via _query pseudo-column
select * from cls_logs
where ts >= now() - interval '1 hour'
  and _query = 'level:ERROR AND service:gateway';
```

The `query` table option serves as a base filter that is automatically combined with WHERE conditions using `AND`.

## Examples

### Basic log query

```sql
select ts, log, source
from cls_logs
where ts >= now() - interval '1 hour'
limit 10;
```

### Query with preset filter

```sql
-- Create a table preset to error logs
create foreign table cls_errors (
  ts      timestamptz,
  log     text,
  source  text,
  level   text,
  message text
)
  server tencent_cls_server
  options (
    topic 'my-log-topic',
    query 'level:ERROR'
  );

-- Just query it — base filter is applied automatically
select ts, message, source
from cls_errors
where ts >= now() - interval '30 minutes';
```

### Raw CQL query

```sql
select ts, log
from cls_logs
where ts >= now() - interval '1 hour'
  and _query = 'status:>=500 AND method:POST';
```
