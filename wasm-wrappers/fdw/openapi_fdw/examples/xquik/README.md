# Xquik API Example

Query public X (Twitter) data from [Xquik](https://xquik.com) with SQL. This example shows API key auth in a custom header, path parameters, query pushdown and cursor pagination.

You don't need an X developer account. Xquik publishes its OpenAPI spec at `https://xquik.com/openapi.json`.

## Prerequisites

You need an Xquik API key.

1. Create an account at [xquik.com](https://xquik.com).
2. Open the API keys page in your dashboard & create a key.
3. Add credits. Each read costs credits, & the [pricing page](https://xquik.com/pricing) lists every price.

## Server Configuration

```sql
create server xquik
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.1/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.1',
    fdw_package_checksum '12c902f3089e18142a1d8d35c66b9ceb85c193224229687bd929aff6b44cddde',
    base_url 'https://xquik.com/api/v1',
    api_key '<YOUR_XQUIK_API_KEY>',
    api_key_header 'x-api-key',
    cursor_param 'cursor',
    max_pages '5'
  );
```

Xquik reads the key from the `x-api-key` header. The FDW sends it as is, with no `Bearer` prefix. Store the key in Vault & pass its name with `api_key_id` in production.

`max_pages` caps the pages 1 scan reads. Each page is a paid read, so keep the cap low & add `LIMIT` to list queries.

---

## 1. A Profile (Path Parameter)

Single object response. `response_path ''` keeps the whole body as 1 row.

```sql
create foreign table x_user (
  username text,
  id text,
  name text,
  description text,
  followers bigint,
  following bigint,
  verified boolean,
  location text,
  created_at text,
  attrs jsonb
)
  server xquik
  options (
    endpoint '/x/users/{username}',
    response_path ''
  );
```

```sql
SELECT username, id, name, followers, verified
FROM x_user
WHERE username = 'NASA';
```

| username | id | name | followers | verified |
| --- | --- | --- | --- | --- |
| NASA | 11348282 | NASA | 92311382 | t |

X dates use the format `Wed Dec 19 20:20:32 +0000 2007`, so `created_at` is `text`. Convert it in SQL.

```sql
SELECT to_timestamp(created_at, 'Dy Mon DD HH24:MI:SS +0000 YYYY') AS joined
FROM x_user
WHERE username = 'NASA';
```

## 2. A Profile's Posts (Path Parameter & Cursor Pagination)

The response holds the posts in `tweets` & the next page token in `next_cursor`. The path parameter gets its own column, `account`, so `id` keeps each post's ID.

```sql
create foreign table user_tweets (
  account text,
  id text,
  text text,
  lang text,
  created_at text,
  like_count bigint,
  retweet_count bigint,
  reply_count bigint,
  view_count bigint,
  attrs jsonb
)
  server xquik
  options (
    endpoint '/x/users/{account}/tweets',
    response_path '/tweets',
    cursor_path '/next_cursor'
  );
```

```sql
SELECT id, text, like_count, view_count
FROM user_tweets
WHERE account = 'NASA'
LIMIT 10;
```

Column names match camelCase keys, so `like_count` reads `likeCount`.

## 3. Post Search (Query Pushdown)

`WHERE q = '...'` becomes the `q` query parameter. Search uses X's search operators.

```sql
create foreign table search_tweets (
  q text,
  id text,
  text text,
  lang text,
  created_at text,
  like_count bigint,
  view_count bigint,
  author jsonb,
  attrs jsonb
)
  server xquik
  options (
    endpoint '/x/tweets/search',
    response_path '/tweets',
    cursor_path '/next_cursor'
  );
```

```sql
SELECT id, text, author ->> 'username' AS author
FROM search_tweets
WHERE q = 'from:NASA'
LIMIT 20;
```

Any other `=` filter on a column becomes a query parameter too. Add a column named after the parameter, such as `"queryType"`, & quote mixed-case names.

## 4. Followers (Path Parameter & Cursor Pagination)

```sql
create foreign table user_followers (
  account text,
  id text,
  username text,
  name text,
  followers bigint,
  verified boolean,
  attrs jsonb
)
  server xquik
  options (
    endpoint '/x/users/{account}/followers',
    response_path '/users',
    cursor_path '/next_cursor'
  );
```

```sql
SELECT username, name, followers
FROM user_followers
WHERE account = 'NASA'
LIMIT 50;
```

## 5. Trends (`response_path`)

```sql
create foreign table trends (
  attrs jsonb
)
  server xquik
  options (
    endpoint '/trends',
    response_path '/trends'
  );
```

```sql
SELECT attrs ->> 'name' AS trend
FROM trends
LIMIT 10;
```

## 6. Debug Mode

```sql
SELECT id FROM search_tweets_debug WHERE q = 'from:NASA' LIMIT 1;
```

```
INFO:  [openapi_fdw] HTTP GET https://xquik.com/api/v1/x/tweets/search?q=from%3ANASA -> 200 (85412 bytes)
INFO:  [openapi_fdw] Scan complete: 1 rows, 2 columns
```

## 7. Every Read Operation

[supabase-xquik](https://github.com/Xquik-dev/supabase-xquik) creates 1 foreign table for each public Xquik read, from the same spec. It also calls write operations from SQL & checks webhook signatures in an Edge Function.

Don't run `IMPORT FOREIGN SCHEMA` on the full Xquik spec. It also imports write operations as POST tables & the event stream, which never ends.
