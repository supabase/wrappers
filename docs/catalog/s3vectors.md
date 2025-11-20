---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# S3 Vectors

[AWS S3 Vectors](https://aws.amazon.com/s3/features/vectors/) is a managed service that stores and queries high-dimensional vectors at scale, optimized for machine learning and artificial intelligence applications.

The S3 Vectors Wrapper allows you to read, write, and perform vector similarity search operations on S3 Vectors within your Postgres database.

## Preparation

Before you can query S3 Vectors, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the S3 Vectors Wrapper

Enable the `s3_vectors_wrapper` FDW:

```sql
create foreign data wrapper s3_vectors_wrapper
  handler s3_vectors_fdw_handler
  validator s3_vectors_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your AWS credentials in Vault and retrieve the created
-- `vault_access_key_id` and `vault_secret_access_key`
select vault.create_secret(
  '<access key id>',  -- secret to be encrypted
  'vault_access_key_id',  -- secret name
  'AWS access key for Wrappers'  -- secret description
);
select vault.create_secret(
  '<secret access key>',
  'vault_secret_access_key',
  'AWS secret access key for Wrappers'
);
```

### Connecting to S3 Vectors

We need to provide Postgres with the credentials to connect to S3 Vectors. We can do this using the `create server` command.

=== "With Vault"

    ```sql
    create server s3_vectors_server
      foreign data wrapper s3_vectors_wrapper
      options (
        -- The key id saved in Vault from above
        vault_access_key_id '<key_ID>',

        -- The secret id saved in Vault from above
        vault_secret_access_key '<secret_key>',

        -- AWS region
        aws_region 'us-east-1',

        -- Optional: Custom endpoint URL for alternative S3 services
        endpoint_url 'http://localhost:8080'
      );
    ```

=== "Without Vault"

    ```sql
    create server s3_vectors_server
      foreign data wrapper s3_vectors_wrapper
      options (
        -- The AWS access key ID
        aws_access_key_id '<key_ID>',

        -- The AWS secret access key
        aws_secret_access_key '<secret_key>',

        -- AWS region
        aws_region 'us-east-1',

        -- Optional: Custom endpoint URL for alternative S3 services
        endpoint_url 'http://localhost:8080'
      );
    ```

#### Additional Server Options

- `batch_size` - Controls the batch size of vectors read from or written to remote. Minimum value of 1, maximum value of 500, default value of 300.

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists s3_vectors;
```

## Options

The full list of foreign table options are below:

- `bucket_name` - The name of the S3 Vector bucket, required.
- `index_name` - The name of the S3 Vector index, required.
- `rowid_column` - The column to use as the row identifier for INSERT/DELETE operations, required.

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from S3 Vectors.

For example, using below SQL can automatically create foreign tables in the `s3_vectors` schema.

```sql
-- create foreign table for each index from S3 Vector bucket
import foreign schema s3_vectors
  from server s3_vectors_server into s3_vectors
  options (
    bucket_name 'my-vector-bucket'
  );
```

### S3 Vector Tables

This is an object representing S3 Vector index.

Ref: [S3 Vectors API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_S3_Vectors.html)

#### Operations

| Object     | Select | Insert | Update | Delete | Truncate |
| ---------- | :----: | :----: | :----: | :----: | :------: |
| table      |   ✅   |   ✅   |   ❌   |   ✅   |    ❌    |

#### Usage

You can also manually create the foreign table like below if you did not use `import foreign schema`.

```sql
create foreign table s3_vectors.embeddings (
  key text not null,
  data s3vec not null,
  metadata jsonb
)
  server s3_vectors_server
  options (
    bucket_name 'my-vector-bucket',
    index_name 'my-vector-index',
    rowid_column 'key'
  );
```
### Custom Data Types

#### s3vec

The `s3vec` type is a custom PostgreSQL data type designed to store and work with high-dimensional vectors for machine learning and AI applications.

**Structure:**

The `s3vec` type internally contains:

- Vector data as an array of 32-bit floating point numbers (Float32)
- Additional metadata fields for internal use

**Input Formats:**

The `s3vec` type accepts input in JSON array format:

```sql
-- Simple array format (most common)
'[0.1, 0.2, 0.3, 0.4, 0.5]'::s3vec

-- Full JSON object format (advanced)
'{"data": [0.1, 0.2, 0.3], "key": "vector_001"}'::s3vec
```

**Output Format:**

When displayed, the `s3vec` type shows a summary format:

```
s3vec:5  -- indicates an embedding with 5 dimensions
```

**Usage Examples:**

See the following sections for complete examples:

- [Inserting Vectors](#inserting-vectors) - Examples of inserting data with `s3vec` type
- [Querying Vectors](#querying-vectors) - Basic queries and vector similarity search
- [Vector Similarity Search with Filtering](#vector-similarity-search-with-filtering) - Advanced search with metadata filtering
- [Advanced Example: Semantic Search](#advanced-example-semantic-search) - Complete semantic search implementation

**Operations:**

- **Vector similarity search**: Use the `<==>` operator for approximate nearest neighbor search
- **Distance calculation**: Use `s3vec_distance()` function to get similarity scores
- **Type casting**: Convert JSON arrays to `s3vec` type using `::s3vec` cast

**Constraints:**

- Only supports 32-bit floating point vectors (Float32)
- Vector dimensions should be consistent within the same index
- Cannot be null when used as vector data in S3 Vectors tables

### Functions

#### s3vec_distance(s3vec)

Returns the distance score from the most recent vector similarity search operation.

**Syntax:**

```sql
s3vec_distance(vector_data) -> real
```

**Parameters:**

- `vector_data` - An `s3vec` type column containing vector data

**Returns:**

- `real` - The distance score from the vector similarity search. This value is only meaningful when used in queries with the `<==>` operator for vector similarity search.

**Usage:**

```sql
-- Get similarity search results with distance scores
select s3vec_distance(data) as distance, key, metadata
from s3_vectors.embeddings
where data <==> '[0.1, 0.2, 0.3, 0.4, 0.5]'::s3vec
order by 1
limit 5;
```

**Notes:**

- The distance value is only populated during vector similarity search operations using the `<==>` operator
- For other query types (key-based lookups, list all), the distance will be 0.0
- Lower distance values indicate higher similarity

## Query Pushdown Support

This FDW supports limited query pushdown with specific operators based on the type of operation:

### Vector Similarity Search (ANN)

For approximate nearest neighbor search using the `<==>` operator:

| Operation                              | Note                                     |
| -------------------------------------- | ---------------------------------------- |
| `data <==> vector_value`              | Vector similarity search with embeddings |
| `metadata <==> json_filter`           | Metadata filtering using S3 Vectors filter expressions |

**Metadata Filtering Syntax:**

The `json_filter` uses S3 Vectors metadata filtering expressions with the following operators:

- **Equality**: `$eq`, `$ne` - Exact match or not equal
- **Numeric Comparisons**: `$gt`, `$gte`, `$lt`, `$lte` - Greater than, less than comparisons
- **Array Operations**: `$in`, `$nin` - Match any/none of the values in array
- **Existence Check**: `$exists` - Check if field exists
- **Logical Operations**: `$and`, `$or` - Combine multiple conditions

**Examples:**
```sql
-- Simple equality
metadata <==> '{"category": "electronics"}'::jsonb

-- Numeric range
metadata <==> '{"price": {"$gte": 100, "$lte": 500}}'::jsonb

-- Array matching
metadata <==> '{"tags": {"$in": ["popular", "trending"]}}'::jsonb

-- Complex logical conditions
metadata <==> '{"$and": [{"category": "books"}, {"year": {"$gte": 2020}}]}'::jsonb
```

For more details on metadata filtering syntax, see the [AWS S3 Vectors metadata filtering documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-metadata-filtering.html).

### Key-based Queries

For exact key lookups:

| Operation                              | Note                                     |
| -------------------------------------- | ---------------------------------------- |
| `key = 'value'`                        | Exact key match                          |
| `key in ('val1', 'val2')`              | Multiple key lookup                      |

### Supported Query Patterns

1. **List all vectors** (no WHERE clause):
   ```sql
   select * from s3_vectors.embeddings;
   ```

2. **Get a specific vector by key**:
   ```sql
   select * from s3_vectors.embeddings where key = 'vector_001';
   ```

3. **Vector similarity search**:
   ```sql
   select s3vec_distance(data) as distance, *
   from s3_vectors.embeddings
   where data <==> '[0.1, 0.2, 0.3, ...]'::s3vec
   order by 1
   limit 10;
   ```

4. **Vector search with metadata filtering**:
   ```sql
   select s3vec_distance(data) as distance, *
   from s3_vectors.embeddings
   where data <==> '[0.1, 0.2, 0.3, ...]'::s3vec
   and metadata <==> '{"category": "product"}'::jsonb
   order by 1
   limit 5;
   ```

!!! note

    Only above specific query patterns are supported. Complex queries with unsupported operators or combinations may result in errors.

## Supported Data Types

| Postgres Type    | S3 Vectors Type                        |
| ---------------- | -------------------------------------- |
| text             | String (for vector key)                |
| s3vec             | Float32 vector data                    |
| jsonb            | Document metadata                      |

## Limitations

This section describes important limitations and considerations when using this FDW:

- Only supports specific query patterns as described in the Query Pushdown section
- Vector similarity search is limited to Float32 vectors
- UPDATE operations are not supported (use DELETE + INSERT instead)
- Complex WHERE clauses with AND/OR combinations are not supported except for specific vector search patterns
- Materialized views using these foreign tables may fail during logical backups

## Examples

### Basic Setup

First, create a server for S3 Vectors:

```sql
create server s3_vectors_server
  foreign data wrapper s3_vectors_wrapper
  options (
    aws_access_key_id '<AWS_access_key_ID>',
    aws_secret_access_key '<AWS_secret_access_key>',
    aws_region 'us-east-1'
  );
```

Import the foreign table:

```sql
-- Import all indexes from a vector bucket
import foreign schema s3_vectors
  from server s3_vectors_server into s3_vectors
  options (
    bucket_name 'my-vector-bucket'
  );

-- or, create the foreign table manually
create foreign table if not exists s3_vectors.embeddings (
  key text not null,
  data s3vec not null,
  metadata jsonb
)
  server s3_vectors_server
  options (
    bucket_name 'my-vector-bucket',
    index_name 'my-vector-index',
    rowid_column 'key'
  );
```

### Querying Vectors

```sql
-- List all vectors in an index
select * from s3_vectors.embeddings;

-- Get specific vector by key
select * from s3_vectors.embeddings where key = 'product_001';

-- Vector similarity search (top 5 similar vectors)
select s3vec_distance(data) as distance, key, metadata
from s3_vectors.embeddings
where data <==> '[0.1, 0.2, 0.3, 0.4, 0.5]'::s3vec
order by 1
limit 5;
```

### Inserting Vectors

```sql
-- Insert a single vector
insert into s3_vectors.embeddings (key, data, metadata)
values (
  'product_001',
  '[0.1, 0.2, 0.3, 0.4, 0.5]'::s3vec,
  '{"category": "electronics", "price": 299.99}'::jsonb
);

-- Insert multiple vectors
insert into s3_vectors.embeddings (key, data, metadata)
values
  ('product_002', '[0.2, 0.3, 0.4, 0.5, 0.6]'::s3vec, '{"category": "books"}'::jsonb),
  ('product_003', '[0.3, 0.4, 0.5, 0.6, 0.7]'::s3vec, '{"category": "clothing"}'::jsonb);
```

### Deleting Vectors

```sql
-- Delete a specific vector by key
delete from s3_vectors.embeddings where key = 'product_001';

-- Delete all vectors
delete from s3_vectors.embeddings;
```

### Vector Similarity Search with Filtering

```sql
-- Find similar vectors with metadata filtering
select s3vec_distance(data) as distance, key, metadata
from s3_vectors.embeddings
where data <==> '[0.1, 0.2, 0.3, 0.4, 0.5]'::s3vec
and metadata <==> '{"category": "electronics"}'::jsonb
order by 1
limit 3;
```

### Advanced Example: Semantic Search

```sql
-- Create a function to convert text to embeddings (pseudo-code)
-- This would typically use an external embedding service
create or replace function text_to_embedding(input_text text)
returns s3vec
language sql
as $$
  -- This is a placeholder - you would implement actual text embedding logic
  select '[0.1, 0.2, 0.3, 0.4, 0.5]'::s3vec;
$$;

-- Semantic search example
select s3vec_distance(data) as distance, key, metadata
from s3_vectors.embeddings
where data <==> text_to_embedding('Find similar products')
and metadata <==> '{"status": "active"}'::jsonb
order by 1
limit 10;
```
