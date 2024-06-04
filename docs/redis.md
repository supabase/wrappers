[Redis](https://redis.io/) is an open-source in-memory storage, used as a distributed, in-memory key–value database, cache and message broker, with optional durability.

The Redis Wrapper allows you to read data from Redis within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Supported Redis Data Types

All Redis value will be stored as `text` or `jsonb` column in Postgres, below are the supported Redis data types:

| Redis Type          | Foreign Table Type (src_type)   |
| ------------------- | ------------------------------- |
| List                | list                            |
| Set                 | set                             |
| Hash                | hash                            |
| Sorted Set          | zset                            |
| Stream              | stream                          |
| Multiple List       | multi_list                      |
| Multiple Set        | multi_set                       |
| Multiple Hash       | multi_hash                      |
| Multiple Sorted Set | multi_zset                      |

**See below for more descriptions for the `Multiple *` types and `src_type` foreign table option.**

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper redis_wrapper
  handler redis_fdw_handler
  validator redis_fdw_validator;
```

### Secure your credentials (optional)

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

The connection URL format is:

```
redis://[<username>][:<password>@]<hostname>[:port][/<db>]
```

## Creating Foreign Tables

The Redis Wrapper supports data reads from Redis.

| Integration | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| Redis       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
create foreign table redis_list (
  element text
)
  server redis_server
  options (
	src_type 'list',
    src_key 'my_list'
  );
```

The foreign table columns names and types must be fixed for each source type, as listed below:

| src_type        | Column name  | Column type  |
| --------------- | ------------ | -------------|
| list, set, zset | `element`    | text         |
| hash            | `key`        | text         |
|                 | `value`      | text         |
| stream          | `id`         | text         |
|                 | `items`      | jsonb        |
| multi_*         | `key`        | text         |
|                 | `items`      | jsonb        |

**See below for the full list of `src_type` and descriptions.**

### Foreign table options

The full list of foreign table options are below:

- `src_type` - Foreign table source type in Redis, required.

   This can be one of below types,

   | Source type | Description                                                |
   | ----------- | ---------------------------------------------------------- |
   | list        | [Single list](https://redis.io/docs/data-types/lists/)     |
   | set         | [Single set](https://redis.io/docs/data-types/sets/)       |
   | hash        | [Single hash](https://redis.io/docs/data-types/hashes/)    |
   | zset        | [Single sorted set](https://redis.io/docs/data-types/sorted-sets/)  |
   | stream      | [Stream](https://redis.io/docs/data-types/streams/)        |
   | multi_list  | Multiple lists, specified by `src_key` pattern             |
   | multi_set   | Multiple sets, specified by `src_key` pattern              |
   | multi_hash  | Multiple hashes, specified by `src_key` pattern            |
   | multi_zset  | Multiple sorted sets, specified by `src_key` pattern       |

- `src_key` - Source object key in Redis, required.

   This key can be a pattern for `multi_*` type of foreign table. For other types, this key must return exact one value. For example,

   | Source Type                    | `src_key` examples                                      |
   | ------------------------------ | ------------------------------------------------------- |
   | list, set, hash, zset, stream  | `my_list`, `list:001`, `hash_foo`, `zset:1000` and etc. |
   | multi_list, multi_set, multi_hash, multi_zset  | `my_list:*`, `set:*`, `zset:*` and etc. |

## Query Pushdown Support

This FDW doesn't supports pushdown.

## Examples

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
  create foreign table redis_list (
    element text
  )
  server redis_server
  options (
    src_type 'list',
    src_key 'list'
  );

  select * from redis_list;
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
  create foreign table redis_set (
    element text
  )
  server redis_server
  options (
    src_type 'set',
    src_key 'set'
  );

  select * from redis_set;
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
  create foreign table redis_hash (
    key text,
    value text
  )
  server redis_server
  options (
    src_type 'hash',
    src_key 'hash'
  );

  select * from redis_hash;
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
  create foreign table redis_zset (
    element text
  )
  server redis_server
  options (
    src_type 'zset',
    src_key 'zset'
  );

  select * from redis_zset;
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
  create foreign table redis_stream (
    id text,
    items jsonb
  )
  server redis_server
  options (
    src_type 'stream',
    src_key 'stream'
  );

  select * from redis_stream;
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
  create foreign table redis_multi_lists (
    key text,
    items jsonb
  )
    server redis_server
    options (
      src_type 'multi_list',
      src_key 'list:*'
    );
  
  select * from redis_multi_lists;
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
  create foreign table redis_multi_sets (
    key text,
    items jsonb
  )
    server redis_server
    options (
      src_type 'multi_set',
      src_key 'set:*'
    );
  
  select * from redis_multi_sets;
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
  create foreign table redis_multi_hashes (
    key text,
    items jsonb
  )
    server redis_server
    options (
      src_type 'multi_hash',
      src_key 'hash:*'
    );
  
  select * from redis_multi_hashes;
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
  create foreign table redis_multi_zsets (
    key text,
    items jsonb
  )
    server redis_server
    options (
      src_type 'multi_zset',
      src_key 'zset:*'
    );
  
  select * from redis_multi_zsets;
  ```

  Query result:
  ```
     key    |     items
  ----------+----------------
   zset:200 | ["qux", "baz"]
   zset:100 | ["foo", "bar"]
  (2 rows)
  ```
