---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Redis

[Redis](https://redis.io/) is an open-source in-memory storage, used as a distributed, in-memory key–value database, cache and message broker, with optional durability.

The Redis Wrapper allows you to read data from Redis within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Preparation

Before you can query Redis, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Redis Wrapper

Enable the `redis_wrapper` FDW:

```sql
create foreign data wrapper redis_wrapper
  handler redis_fdw_handler
  validator redis_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Redis connection URL in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'redis_conn_url',
  'redis://username:password@127.0.0.1:6379/db'
)
returning key_id;
```

### Connecting to Redis

We need to provide Postgres with the credentials to connect to Redis. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server redis_server
      foreign data wrapper redis_wrapper
      options (
        conn_url_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server redis_server
      foreign data wrapper redis_wrapper
      options (
        conn_url 'redis://username:password@127.0.0.1:6379/db'
      );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists redis;
```

## Options

The following options are available when creating Redis foreign tables:

- `src_type` - Foreign table source type in Redis, required.

This can be one of below types,

| Source type | Description                                                        |
| ----------- | ------------------------------------------------------------------ |
| list        | [Single list](https://redis.io/docs/data-types/lists/)             |
| set         | [Single set](https://redis.io/docs/data-types/sets/)               |
| hash        | [Single hash](https://redis.io/docs/data-types/hashes/)            |
| zset        | [Single sorted set](https://redis.io/docs/data-types/sorted-sets/) |
| stream      | [Stream](https://redis.io/docs/data-types/streams/)                |
| multi_list  | Multiple lists, specified by `src_key` pattern                     |
| multi_set   | Multiple sets, specified by `src_key` pattern                      |
| multi_hash  | Multiple hashes, specified by `src_key` pattern                    |
| multi_zset  | Multiple sorted sets, specified by `src_key` pattern               |

- `src_key` - Source object key in Redis, required.

This key can be a pattern for `multi_*` type of foreign table. For other types, this key must return exact one value. For example,

| Source Type                                   | `src_key` examples                                      |
| --------------------------------------------- | ------------------------------------------------------- |
| list, set, hash, zset, stream                 | `my_list`, `list:001`, `hash_foo`, `zset:1000` and etc. |
| multi_list, multi_set, multi_hash, multi_zset | `my_list:*`, `set:*`, `zset:*` and etc.                 |


## Entities

### List

This is an object representing a Redis List.

Ref: [Redis docs](https://redis.io/docs/data-types/lists/)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| List   |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table redis.list (
  element text
)
  server redis_server
  options (
    src_type 'list',
    src_key 'my_list'
  );
```

#### Notes

- Elements are stored in insertion order
- Query returns all elements in the list
- No query pushdown support

### Set

This is an object representing a Redis Set.

Ref: [Redis docs](https://redis.io/docs/data-types/sets/)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Set    |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table redis.set (
  element text
)
  server redis_server
  options (
    src_type 'set',
    src_key 'set'
  );
```

#### Notes

- Elements are unique within the set
- No guaranteed order of elements
- No query pushdown support

### Hash

This is an object representing a Redis Hash.

Ref: [Redis docs](https://redis.io/docs/data-types/hashes/)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Hash   |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table redis.hash (
  key text,
  value text
)
  server redis_server
  options (
    src_type 'hash',
    src_key 'hash'
  );
```

#### Notes

- Key-value pairs within the hash
- No query pushdown support
- Both key and value are returned as text

### Sorted Set

This is an object representing a Redis Sorted Set.

Ref: [Redis docs](https://redis.io/docs/data-types/sorted-sets/)

#### Operations

| Object     | Select | Insert | Update | Delete | Truncate |
| ---------- | :----: | :----: | :----: | :----: | :------: |
| Sorted Set |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table redis.zset (
  element text
)
  server redis_server
  options (
    src_type 'zset',
    src_key 'zset'
  );
```

#### Notes

- Elements are ordered by their score
- Elements are unique within the set
- Score information is not exposed in the foreign table

### Stream

This is an object representing a Redis Stream.

Ref: [Redis docs](https://redis.io/docs/data-types/streams/)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Stream |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table redis.stream (
  id text,
  items jsonb
)
  server redis_server
  options (
    src_type 'stream',
    src_key 'stream'
  );
```

#### Notes

- Stream entries have unique IDs
- Items are stored in JSONB format
- Entries are ordered by their IDs

### Multiple Objects

Redis wrapper supports querying multiple objects of the same type using pattern matching.

#### Operations

| Object Type   | Select | Insert | Update | Delete | Truncate |
| ------------- | :----: | :----: | :----: | :----: | :------: |
| Multiple List |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |
| Multiple Set  |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |
| Multiple Hash |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |
| Multiple ZSet |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table redis.multi_lists (
  key text,
  items jsonb
)
  server redis_server
  options (
    src_type 'multi_list',
    src_key 'list:*'
  );
```

#### Notes

- Use pattern matching in `src_key` option
- Results include object key and items in JSONB format
- Items format varies by object type

## Query Pushdown Support

This FDW doesn't support pushdown.

## Supported Redis Data Types

All Redis values will be stored as `text` or `jsonb` columns in Postgres, below are the supported Redis data types:

| Redis Type          | Foreign Table Type (src_type) |
| ------------------- | ----------------------------- |
| List                | list                          |
| Set                 | set                           |
| Hash                | hash                          |
| Sorted Set          | zset                          |
| Stream              | stream                        |
| Multiple List       | multi_list                    |
| Multiple Set        | multi_set                     |
| Multiple Hash       | multi_hash                    |
| Multiple Sorted Set | multi_zset                    |

**See below for more descriptions for the `Multiple *` types and `src_type` foreign table option.**

## Limitations

This section describes important limitations and considerations when using this FDW:

- **Performance Limitations**:
  - Buffer size limited to 256 items per request for list and sorted set operations
  - No query pushdown support means all filtering happens locally after data retrieval
  - API requests use exponential backoff with maximum 3 retries on failures
  - Stream entries are processed in batches, which may impact real-time data access

- **Feature Limitations**:
  - Read-only access to Redis data structures (no Insert, Update, Delete, or Truncate operations)
  - Limited column type support (only text for basic types, jsonb for complex types)
  - Pattern matching in `multi_*` types only supports basic Redis glob patterns
  - Sorted set scores are not exposed in the foreign table structure
  - Stream entries must be processed sequentially from the last known ID

- **Resource Usage**:
  - Full result sets are loaded into memory before processing
  - Each query requires a complete Redis request-response cycle
  - Multiple object queries (multi_*) load all matching keys into memory at once
  - Failed requests consume additional resources due to retry attempts
  - Large hash or stream results may require significant PostgreSQL memory

- **Known Issues**:
  - Materialized views using these foreign tables may fail during logical backups (use physical backups instead)
  - 404 responses are treated as empty results rather than errors
  - Column type mismatches in response data will result in null values
  - Connection failures during scans may require re-establishing the connection
  - Memory pressure can occur when dealing with large datasets due to full result set loading

Some examples on how to use Redis foreign tables.

Let's prepare some source data in Redis CLI first:

```bash
127.0.0.1:6379> RPUSH list foo bar 42
127.0.0.1:6379> SADD set foo bar 42
127.0.0.1:6379> HSET hash foo bar baz qux
127.0.0.1:6379> ZADD zset 30 foo 20 bar 10 baz
127.0.0.1:6379> XADD stream * foo bar
127.0.0.1:6379> XADD stream * aa 42 bb 43

127.0.0.1:6379> RPUSH list:100 foo bar
127.0.0.1:6379> RPUSH list:200 baz

127.0.0.1:6379> SADD set:100 foo
127.0.0.1:6379> SADD set:200 bar

127.0.0.1:6379> HSET hash:100 foo bar
127.0.0.1:6379> HSET hash:200 baz qux

127.0.0.1:6379> ZADD zset:100 10 foo 20 bar
127.0.0.1:6379> ZADD zset:200 40 baz 30 qux
```

### Basic example

This example will create foreign tables inside your Postgres database and query their data:

- List

  ```sql
  create foreign table redis.list (
    element text
  )
  server redis_server
  options (
    src_type 'list',
    src_key 'list'
  );

  select * from redis.list;
  ```

  Query result:

  ```
   element
  ---------
   foo
   bar
   42
  (3 rows)
  ```

- Set

  ```sql
  create foreign table redis.set (
    element text
  )
  server redis_server
  options (
    src_type 'set',
    src_key 'set'
  );

  select * from redis.set;
  ```

  Query result:

  ```
   element
  ---------
   42
   foo
   bar
  (3 rows)
  ```

- Hash

  ```sql
  create foreign table redis.hash (
    key text,
    value text
  )
  server redis_server
  options (
    src_type 'hash',
    src_key 'hash'
  );

  select * from redis.hash;
  ```

  Query result:

  ```
   key | value
  -----+-------
   foo | bar
   baz | qux
  (2 rows)
  ```

- Sorted set

  ```sql
  create foreign table redis.zset (
    element text
  )
  server redis_server
  options (
    src_type 'zset',
    src_key 'zset'
  );

  select * from redis.zset;
  ```

  Query result:

  ```
   element
  ---------
   baz
   bar
   foo
  (3 rows)
  ```

- Stream

  ```sql
  create foreign table redis.stream (
    id text,
    items jsonb
  )
  server redis_server
  options (
    src_type 'stream',
    src_key 'stream'
  );

  select * from redis.stream;
  ```

  Query result:

  ```
         id        |          items
  -----------------+--------------------------
   1704343825989-0 | {"foo": "bar"}
   1704343829799-0 | {"aa": "42", "bb": "43"}
  (2 rows)
  ```

### Query multiple objects example

This example will create several foreign tables using pattern in key and query multiple objects from Redis:

- List

  ```sql
  create foreign table redis.multi_lists (
    key text,
    items jsonb
  )
    server redis_server
    options (
      src_type 'multi_list',
      src_key 'list:*'
    );

  select * from redis.multi_lists;
  ```

  Query result:

  ```
     key    |     items
  ----------+----------------
   list:100 | ["foo", "bar"]
   list:200 | ["baz"]
  (2 rows)
  ```

- Set

  ```sql
  create foreign table redis.multi_sets (
    key text,
    items jsonb
  )
    server redis_server
    options (
      src_type 'multi_set',
      src_key 'set:*'
    );

  select * from redis.multi_sets;
  ```

  Query result:

  ```
     key   |  items
  ---------+---------
   set:100 | ["foo"]
   set:200 | ["bar"]
  (2 rows)
  ```

- Hash

  ```sql
  create foreign table redis.multi_hashes (
    key text,
    items jsonb
  )
    server redis_server
    options (
      src_type 'multi_hash',
      src_key 'hash:*'
    );

  select * from redis.multi_hashes;
  ```

  Query result:

  ```
     key    |     items
  ----------+----------------
   hash:200 | {"baz": "qux"}
   hash:100 | {"foo": "bar"}
  (2 rows)
  ```

- Sorted set

  ```sql
  create foreign table redis.multi_zsets (
    key text,
    items jsonb
  )
    server redis_server
    options (
      src_type 'multi_zset',
      src_key 'zset:*'
    );

  select * from redis.multi_zsets;
  ```

  Query result:

  ```
     key    |     items
  ----------+----------------
   zset:200 | ["qux", "baz"]
   zset:100 | ["foo", "bar"]
  (2 rows)
  ```
