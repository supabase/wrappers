---
source:
documentation:
author: Supabase
tags:
  - native
---

# MongoDB

[MongoDB](https://www.mongodb.com/) is a popular open-source document database that stores data as flexible BSON documents.

The MongoDB Wrapper allows you to read and write data from MongoDB within your Postgres database. Top-level BSON fields are mapped to declared columns by exact name; missing fields surface as SQL `NULL`. A special `_doc jsonb` column receives the full document for nested and array access.

## Preparation

Before you can query MongoDB, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the MongoDB Wrapper

Enable the `mongodb_wrapper` FDW:

```sql
create foreign data wrapper mongodb_wrapper
  handler mongodb_fdw_handler
  validator mongodb_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your MongoDB connection string in Vault and retrieve the created `key_id`
select vault.create_secret(
  'mongodb://user:password@host:27017/?replicaSet=rs0',
  'mongodb',
  'MongoDB connection string for Wrappers'
);
```

### Connecting to MongoDB

We need to provide Postgres with the credentials to connect to MongoDB, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server mongo_server
      foreign data wrapper mongodb_wrapper
      options (
        conn_string_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server mongo_server
      foreign data wrapper mongodb_wrapper
      options (
        conn_string 'mongodb://user:password@host:27017/?replicaSet=rs0'
      );
    ```

The connection string follows the standard MongoDB URI format and supports `mongodb://` and `mongodb+srv://` schemes. Credentials, TLS options, replica set names, and other driver options can all be encoded in the URI.

Some connection string examples:

- `mongodb://root:secret@localhost:27017/`
- `mongodb://app_user:password@db.example.com:27017/?tls=true`
- `mongodb://user:pass@host1:27017,host2:27017/?replicaSet=rs0`
- `mongodb+srv://user:password@cluster0.mcqtkst.mongodb.net/?appName=Cluster0`

!!! warning "MongoDB Atlas IP Access List"

    If you connect to MongoDB Atlas, you must allow network access from your Supabase database server in Atlas **Network Access / IP Access List**.

    Supabase database server egress IP addresses are dynamic and there is no fixed IP range you can safelist. In practice, this often means allowing `0.0.0.0/0` in Atlas to make the connection work.

    If you are concerned about this security risk, place a proxy or gateway with a fixed public IP between your Supabase database instance and MongoDB, and safelist only that proxy IP in Atlas.

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists mongo;
```

## Options

The following options are available when creating MongoDB foreign tables:

- `database` - MongoDB database name, required
- `collection` - MongoDB collection name, required
- `rowid_column` - Column to use as the row identifier, optional for data scan, required for data modify. The conventional value is `_id`.

## Entities

### Collections

The MongoDB Wrapper supports data reads and writes from MongoDB collections.

#### Operations

| Object      | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| Collections |   ✅    |   ✅    |   ✅    |   ✅    |    ❌     |

#### Usage

```sql
create foreign table mongo.users (
  _id text,
  name text,
  age int,
  created_at timestamp,
  _doc jsonb
)
  server mongo_server
  options (
    database 'app',
    collection 'users',
    rowid_column '_id'
  );
```

#### Notes

- Supports `where`, `order by`, and `limit` clause pushdown
- Documents are streamed one at a time from MongoDB; no full buffering in memory
- A column named `_doc` of type `jsonb` receives the complete document and can be used with Postgres JSON operators (`->`, `->>`) to access nested fields and arrays
- When `rowid_column` is set, INSERT, UPDATE, and DELETE are supported

## Schema Mapping

Each column declared on the foreign table maps to a top-level BSON field of the same name (exact match):

- If a document does not contain a field, the corresponding column is set to `NULL`.
- Dots in column names are treated as literal characters — they do not traverse embedded documents. Use the `_doc` column for nested field access.
- If a column named `_doc` of type `jsonb` is declared, it receives the full BSON document serialized as JSON. When `_doc` is declared, projection is disabled so that the complete document is returned from MongoDB.

Example: access a nested `address.city` field via `_doc`:

```sql
select _doc->>'name', _doc->'address'->>'city'
from mongo.users;
```

## Query Pushdown Support

This FDW supports `where`, `order by`, and `limit` clause pushdown.

### Supported Operators

The following SQL predicates are translated to MongoDB filter operators:

| SQL predicate     | MongoDB filter          |
| ----------------- | ----------------------- |
| `=`               | `{field: {$eq: v}}`     |
| `!=`              | `{field: {$ne: v}}`     |
| `<`               | `{field: {$lt: v}}`     |
| `<=`              | `{field: {$lte: v}}`    |
| `>`               | `{field: {$gt: v}}`     |
| `>=`              | `{field: {$gte: v}}`    |
| `IN (...)`        | `{field: {$in: [...]}}`  |
| `NOT IN (...)`    | `{field: {$nin: [...]}}` |
| `IS NULL`         | `{field: {$eq: null}}`  |
| `IS NOT NULL`     | `{field: {$ne: null}}`  |

Multiple `where` predicates are AND'd at the top level of the filter document. Array-form predicates like `IN (...)` / `NOT IN (...)` (and the equivalent `= ANY(ARRAY[...])` / `<> ALL(ARRAY[...])`) are pushed down as `$in` / `$nin`. Arbitrary `OR` predicates between unrelated columns are not pushed down — they are re-checked by Postgres. Any predicate shape that is not supported is omitted from the MongoDB filter and re-checked by Postgres after the rows are returned, so the result is always correct.

## Supported Data Types

| BSON Type   | Postgres Type           | Notes                    |
| ----------- | ----------------------- | ------------------------ |
| Boolean     | bool                    |                          |
| Int32       | int2 / int4 / int8      |                          |
| Int64       | int8 / numeric          |                          |
| Double      | float8 / float4         |                          |
| Decimal128  | numeric                 |                          |
| String      | text / varchar          |                          |
| ObjectId    | text                    | Returned as a 24-character lowercase hex string. A 24-char hex value in a qual is coerced back to `ObjectId` for the filter. |
| DateTime    | timestamp / timestamptz |                          |
| Document    | jsonb                   |                          |
| Array       | jsonb                   |                          |
| Binary      | bytea                   |                          |
| Null / missing | any                  | Column is set to `NULL`  |

!!! note

    `_id` fields that are `ObjectId` values are returned as their 24-character hex representation. When querying by `_id` with a 24-character hex string, the value is automatically coerced back to an `ObjectId` for the filter — no explicit casting is required.

## Writes

INSERT, UPDATE, and DELETE are enabled when `rowid_column` is set on the foreign table.

- **INSERT**: Each non-null column is written as a top-level BSON field. Null columns are omitted entirely (not stored as explicit BSON nulls). If `_id` is not supplied, MongoDB generates an `ObjectId` automatically.
- **UPDATE**: Non-null columns are passed to `$set`; null columns are passed to `$unset`, which removes the field from the document. The document is located by matching `rowid_column`.
- **DELETE**: The document matching `rowid_column = rowid` is deleted via `delete_one`.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Aggregate pushdown is not supported — `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` are computed in Postgres after fetching rows
- `import foreign schema` is not supported; foreign table definitions must be declared manually
- `LIKE` predicates are not pushed down to MongoDB `$regex`; pattern matching happens in Postgres
- Nested field path access (e.g., `address.city`) is not supported as column names — use the `_doc jsonb` column and Postgres JSON operators instead
- Multi-document transactions and batched writes are not supported in v1
- Change streams are not supported
- Materialized views using foreign tables may fail during logical backups
- For MongoDB Atlas, Supabase egress IPs are dynamic (no fixed range). You may need to allow `0.0.0.0/0` in Atlas IP Access List, or use a proxy/gateway with a fixed IP to avoid broad public access

## Examples

### Basic example

This example shows how to query a MongoDB collection from Postgres.

```sql
-- Create the server
create server mongo_server
  foreign data wrapper mongodb_wrapper
  options (
    conn_string 'mongodb://localhost:27017/'
  );

-- Create the foreign table
create foreign table mongo.users (
  _id text,
  name text,
  age int,
  created_at timestamp,
  _doc jsonb
)
  server mongo_server
  options (
    database 'app',
    collection 'users'
  );

-- Query all users
select _id, name, age from mongo.users;

-- Filter with pushdown
select name from mongo.users where age > 25;

-- Access nested fields via _doc
select _doc->>'email', _doc->'address'->>'city'
from mongo.users
where name = 'Alice';
```

### Data modification example

This example demonstrates INSERT, UPDATE, and DELETE on a foreign table. The `rowid_column` option is required for data modification:

```sql
create foreign table mongo.users (
  _id text,
  name text,
  age int,
  _doc jsonb
)
  server mongo_server
  options (
    database 'app',
    collection 'users',
    rowid_column '_id'
  );

-- Insert a new document (_id is generated by MongoDB if omitted)
insert into mongo.users (name, age)
values ('Alice', 30);

-- Update a document (null columns remove the field via $unset)
update mongo.users
set age = 31
where _id = '507f1f77bcf86cd799439011';

-- Remove a field by setting it to null
update mongo.users
set age = null
where _id = '507f1f77bcf86cd799439011';

-- Delete a document
delete from mongo.users
where _id = '507f1f77bcf86cd799439011';
```

### Using Vault for credentials

```sql
-- Store the connection string in Vault
select vault.create_secret(
  'mongodb+srv://user:password@cluster.example.mongodb.net/',
  'mongodb_atlas',
  'MongoDB Atlas connection string'
);

-- Create the server using the Vault secret ID
create server mongo_atlas_server
  foreign data wrapper mongodb_wrapper
  options (
    conn_string_id '<key_ID>'
  );

create foreign table mongo.products (
  _id text,
  name text,
  price numeric,
  in_stock bool
)
  server mongo_atlas_server
  options (
    database 'shop',
    collection 'products',
    rowid_column '_id'
  );

-- Query with multiple pushed-down predicates
select name, price
from mongo.products
where in_stock = true
  and price < 100
order by price
limit 10;
```
