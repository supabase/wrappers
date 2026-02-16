# Threads API Example

Query the [Meta Threads API](https://developers.facebook.com/docs/threads) using SQL. This example demonstrates authenticated API access, cursor-based pagination, path parameter substitution, and query param pushdown.

## Server Configuration

```sql
create server threads
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://graph.threads.net',
    api_key '<YOUR_ACCESS_TOKEN>',
    api_key_header 'access_token',
    api_key_location 'query'
  );
```

---

## 1. Your Profile

Single object response. The FDW returns one row with your Threads profile info.

```sql
create foreign table my_profile (
  id text,
  username text,
  name text,
  threads_profile_picture_url text,
  threads_biography text,
  is_verified boolean,
  attrs jsonb
)
  server threads
  options (
    endpoint '/me?fields=id,username,name,threads_profile_picture_url,threads_biography,is_verified',
    rowid_column 'id'
  );
```

```sql
SELECT username, name, threads_biography, is_verified
FROM my_profile;
```

| username | name | threads_biography | is_verified |
| --- | --- | --- | --- |
| youruser | Your Name | your bio here | false |

> Your results will reflect your own Threads profile.

## 2. Your Threads

Paginated list of your posts. The FDW auto-detects the `data` wrapper key and follows cursor-based pagination (`paging.cursors.after`).

```sql
create foreign table my_threads (
  id text,
  media_type text,
  text text,
  permalink text,
  username text,
  timestamp timestamptz,
  shortcode text,
  is_quote_post boolean,
  topic_tag text,
  link_attachment_url text,
  is_verified boolean,
  attrs jsonb
)
  server threads
  options (
    endpoint '/me/threads?fields=id,media_type,media_product_type,text,permalink,username,timestamp,shortcode,is_quote_post,topic_tag,link_attachment_url,is_verified',
    rowid_column 'id'
  );
```

```sql
SELECT id, text, media_type, timestamp
FROM my_threads
LIMIT 5;
```

| id | text | media_type | timestamp |
| --- | --- | --- | --- |
| 18555728842018816 | Your latest thread post text here... | TEXT_POST | 2026-02-12 04:46:47+00 |
| 18051838931694754 | | IMAGE | 2026-02-11 14:12:47+00 |
| 18099070105919840 | | REPOST_FACADE | 2026-02-09 00:20:23+00 |

> Your results will reflect your own posts.

Full thread details with permalink, shortcode, and quote/topic info:

```sql
SELECT id, text, media_type, permalink, shortcode,
       is_quote_post, topic_tag, link_attachment_url,
       is_verified, timestamp
FROM my_threads
LIMIT 5;
```

Filter by time in SQL:

```sql
SELECT text, timestamp, topic_tag
FROM my_threads
WHERE timestamp > '2024-01-01'
LIMIT 5;
```

Filter by media type after fetching:

```sql
SELECT id, text, media_type, timestamp
FROM my_threads
WHERE media_type = 'TEXT_POST'
LIMIT 5;
```

## 3. Your Replies

Same pagination pattern as threads, filtered to your replies:

```sql
create foreign table my_replies (
  id text,
  media_type text,
  text text,
  permalink text,
  username text,
  timestamp timestamptz,
  shortcode text,
  is_quote_post boolean,
  has_replies boolean,
  is_reply boolean,
  attrs jsonb
)
  server threads
  options (
    endpoint '/me/replies?fields=id,media_type,text,permalink,username,timestamp,shortcode,is_quote_post,has_replies,is_reply',
    rowid_column 'id'
  );
```

```sql
SELECT text, timestamp, is_reply, has_replies
FROM my_replies
LIMIT 5;
```

| text | timestamp | is_reply | has_replies |
| --- | --- | --- | --- |
| Your reply text here... | 2026-02-13 19:25:51+00 | true | false |
| Another reply... | 2026-02-13 19:22:01+00 | true | true |

Full reply details with permalink, media type, and quote status:

```sql
SELECT id, text, media_type, permalink, username, shortcode,
       is_quote_post, has_replies, is_reply, timestamp
FROM my_replies
LIMIT 5;
```

## 4. Thread Detail (Path Parameter)

Look up a specific thread by ID. The `{thread_id}` placeholder in the endpoint is replaced with the value from your WHERE clause.

```sql
create foreign table thread_detail (
  id text,
  media_type text,
  text text,
  permalink text,
  username text,
  timestamp timestamptz,
  is_quote_post boolean,
  has_replies boolean,
  topic_tag text,
  link_attachment_url text,
  reply_audience text,
  thread_id text,
  attrs jsonb
)
  server threads
  options (
    endpoint '/{thread_id}?fields=id,media_type,text,permalink,username,timestamp,is_quote_post,has_replies,topic_tag,link_attachment_url,reply_audience',
    rowid_column 'id'
  );
```

```sql
-- Get a thread ID from your posts first
SELECT id FROM my_threads LIMIT 1;

-- Then fetch full details
SELECT text, media_type, timestamp, reply_audience
FROM thread_detail
WHERE thread_id = '<THREAD_ID>';
```

| text | media_type | timestamp | reply_audience |
| --- | --- | --- | --- |
| Your thread text... | TEXT_POST | 2026-02-12 04:46:47+00 | EVERYONE |

## 5. Thread Replies

Top-level replies to a specific thread. Requires `thread_id` path parameter:

```sql
create foreign table thread_replies (
  id text,
  text text,
  username text,
  permalink text,
  timestamp timestamptz,
  media_type text,
  has_replies boolean,
  is_reply boolean,
  hide_status text,
  is_verified boolean,
  thread_id text,
  attrs jsonb
)
  server threads
  options (
    endpoint '/{thread_id}/replies?fields=id,text,username,permalink,timestamp,media_type,has_replies,is_reply,hide_status,is_verified',
    rowid_column 'id'
  );
```

```sql
SELECT username, text, timestamp, hide_status
FROM thread_replies
WHERE thread_id = '<THREAD_ID>'
LIMIT 10;
```

Full reply metadata with permalink, media type, and verification status:

```sql
SELECT id, username, text, media_type, permalink,
       has_replies, is_reply, hide_status, is_verified,
       timestamp
FROM thread_replies
WHERE thread_id = '<THREAD_ID>'
LIMIT 10;
```

## 6. Thread Conversation

All replies at all depths, flattened into a single list:

```sql
create foreign table thread_conversation (
  id text,
  text text,
  username text,
  permalink text,
  timestamp timestamptz,
  media_type text,
  has_replies boolean,
  is_reply boolean,
  hide_status text,
  thread_id text,
  attrs jsonb
)
  server threads
  options (
    endpoint '/{thread_id}/conversation?fields=id,text,username,permalink,timestamp,media_type,has_replies,is_reply,hide_status&reverse=false',
    rowid_column 'id'
  );
```

```sql
SELECT username, text, timestamp, is_reply
FROM thread_conversation
WHERE thread_id = '<THREAD_ID>'
LIMIT 20;
```

Full conversation with media and reply chain info:

```sql
SELECT id, username, text, media_type, permalink,
       has_replies, is_reply, hide_status,
       timestamp
FROM thread_conversation
WHERE thread_id = '<THREAD_ID>'
LIMIT 20;
```

## 7. Keyword Search (Query Param Pushdown)

When a WHERE clause references `q`, the FDW sends it as a query parameter to the `/keyword_search` endpoint. Requires the `threads_keyword_search` permission on your app.

```sql
create foreign table keyword_search (
  id text,
  text text,
  media_type text,
  permalink text,
  username text,
  timestamp timestamptz,
  has_replies boolean,
  is_quote_post boolean,
  is_reply boolean,
  topic_tag text,
  q text,
  attrs jsonb
)
  server threads
  options (
    endpoint '/keyword_search?fields=id,text,media_type,permalink,username,timestamp,has_replies,is_quote_post,is_reply,topic_tag',
    rowid_column 'id'
  );
```

```sql
-- Pushes down to: GET /keyword_search?q=threads
SELECT username, text, timestamp
FROM keyword_search
WHERE q = 'threads'
LIMIT 3;
```

| username | text | timestamp |
| --- | --- | --- |
| youruser | A matching post about threads... | 2025-12-25 20:09:53+00 |
| youruser | Another matching result... | 2025-11-09 01:47:56+00 |

Full search results with media type, engagement flags, and topic tags:

```sql
SELECT id, username, text, media_type, permalink,
       has_replies, is_quote_post, is_reply, topic_tag,
       timestamp
FROM keyword_search
WHERE q = 'threads'
LIMIT 5;
```

## 8. Profile Lookup

Look up any public profile by username. Requires the `threads_basic` permission.

```sql
create foreign table profile_lookup (
  username text,
  name text,
  biography text,
  profile_picture_url text,
  follower_count bigint,
  is_verified boolean,
  likes_count bigint,
  quotes_count bigint,
  reposts_count bigint,
  views_count bigint,
  attrs jsonb
)
  server threads
  options (
    endpoint '/profile_lookup',
    rowid_column 'username'
  );
```

```sql
SELECT name, biography, follower_count, is_verified
FROM profile_lookup
WHERE username = 'threads';
```

| name | biography | follower_count | is_verified |
| --- | --- | --- | --- |
| Threads | | 100000000 | true |

Full profile with engagement metrics:

```sql
SELECT username, name, biography, profile_picture_url,
       follower_count, likes_count, quotes_count,
       reposts_count, views_count, is_verified
FROM profile_lookup
WHERE username = 'threads';
```

## 9. Publishing Limit

Check your current rate limit usage:

```sql
create foreign table publishing_limit (
  quota_usage integer,
  config jsonb,
  reply_quota_usage integer,
  reply_config jsonb,
  attrs jsonb
)
  server threads
  options (
    endpoint '/me/threads_publishing_limit?fields=quota_usage,config,reply_quota_usage,reply_config'
  );
```

```sql
SELECT quota_usage, config, reply_quota_usage, reply_config
FROM publishing_limit;
```

| quota_usage | config | reply_quota_usage | reply_config |
| --- | --- | --- | --- |
| 0 | `{"quota_total": 250, "quota_duration": 86400}` | 0 | `{"quota_total": 1000, "quota_duration": 86400}` |

## 10. Debug Mode

The `keyword_search_debug` table uses the `threads_debug` server which has `debug 'true'`. This emits HTTP request details as PostgreSQL INFO messages.

```sql
SELECT id, text FROM keyword_search_debug WHERE q = 'meta' LIMIT 3;
```

Look for INFO output like:

```log
INFO:  [openapi_fdw] HTTP GET https://graph.threads.net/keyword_search?... -> 200 (1234 bytes)
INFO:  [openapi_fdw] Scan complete: 3 rows, 2 columns
```

## 11. IMPORT FOREIGN SCHEMA (Inline `spec_json`)

Meta's Threads API does not publish an official OpenAPI spec at a public URL. Instead of `spec_url`, this example uses `spec_json` to provide a hand-written spec directly in the server definition. The inline spec describes just the 8 GET endpoints used by this example.

This approach also works well for APIs that:

- Don't publish an OpenAPI spec at all (like Threads)
- Publish a spec that's too large, outdated, or inaccurate
- Need a customized subset of endpoints

The FDW parses the inline JSON the same way it would a fetched spec, auto-generating `CREATE FOREIGN TABLE` statements with correct column names and types. Endpoints with path parameters (`/{thread_id}/replies`, `/{thread_id}/conversation`) are skipped — those need manual table definitions like the ones above.

Auto-generate table definitions from the inline spec:

```sql
CREATE SCHEMA IF NOT EXISTS threads_auto;

IMPORT FOREIGN SCHEMA "unused"
FROM SERVER threads_import
INTO threads_auto;
```

See what was generated:

```sql
SELECT foreign_table_name FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'threads_auto';
```

## 12. The `attrs` Column

Every table includes an `attrs jsonb` column that captures all fields not mapped to named columns:

```sql
SELECT id, attrs->>'media_product_type' AS product_type,
       attrs->>'shortcode' AS shortcode
FROM my_threads
LIMIT 3;
```

| id | product_type | shortcode |
| --- | --- | --- |
| 18555728842018816 | THREADS | ABC123xyz |
| 18051838931694754 | THREADS | DEF456uvw |
| 18099070105919840 | THREADS | GHI789rst |

## Features Demonstrated

| Feature | Table(s) |
| --- | --- |
| API key auth (query param) | All tables |
| Cursor-based pagination (auto-detected) | `my_threads`, `my_replies`, `keyword_search` |
| Path parameter substitution | `thread_detail`, `thread_replies`, `thread_conversation` |
| Query parameter pushdown | `keyword_search` (with `WHERE q = ...`), `profile_lookup` (with `WHERE username = ...`) |
| Single object response | `my_profile`, `thread_detail`, `profile_lookup` |
| Endpoint query string (field selection) | All tables except `profile_lookup` |
| Type coercion (timestamptz, boolean, bigint) | `my_threads`, `profile_lookup` |
| Debug mode | `keyword_search_debug` |
| IMPORT FOREIGN SCHEMA | `threads_import` server |
| `attrs` catch-all column | All tables |
| `rowid_column` | `my_threads`, `keyword_search`, `profile_lookup` |
