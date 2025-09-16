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

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists s3_vectors;
```

## Options

The full list of foreign table options are below:

- `index_arn` - The ARN of the S3 Vector index, required.
- `rowid_column` - The column to use as the row identifier for INSERT/DELETE operations, required.

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from S3 Vectors.

For example, using below SQL can automatically create foreign tables in the `s3_vectors` schema.

```sql
-- create foreign table for each index from S3 Vector bucket
import foreign schema s3_vectors
  from server s3_vectors_server into s3_vectors
  options (
    bucket_arn 'arn:aws:s3vectors:us-east-1:123456789012:bucket/my-vector-bucket'
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
  data embd not null,
  metadata jsonb
)
  server s3_vectors_server
  options (
    index_arn 'arn:aws:s3vectors:us-east-1:123456789012:bucket/my-vector-bucket/index/my-vector-index',
    rowid_column 'key'
  );
```
### Custom Data Types

#### embd

The `embd` type is a custom PostgreSQL data type designed to store and work with high-dimensional vectors for machine learning and AI applications.

**Structure:**

The `embd` type internally contains:

- Vector data as an array of 32-bit floating point numbers (Float32)
- Additional metadata fields for internal use

**Input Formats:**

The `embd` type accepts input in JSON array format:

```sql
-- Simple array format (most common)
'[0.1, 0.2, 0.3, 0.4, 0.5]'::embd

-- Full JSON object format (advanced)
'{"data": [0.1, 0.2, 0.3], "key": "vector_001"}'::embd
```

**Output Format:**

When displayed, the `embd` type shows a summary format:

```
embd:5  -- indicates an embedding with 5 dimensions
```

**Usage Examples:**

See below sections for more examples.

**Operations:**

- **Vector similarity search**: Use the `<==>` operator for approximate nearest neighbor search
- **Distance calculation**: Use `embd_distance()` function to get similarity scores
- **Type casting**: Convert JSON arrays to `embd` type using `::embd` cast

**Constraints:**

- Only supports 32-bit floating point vectors (Float32)
- Vector dimensions should be consistent within the same index
- Cannot be null when used as vector data in S3 Vectors tables

### Functions

#### embd_distance(embd)

Returns the distance score from the most recent vector similarity search operation.

**Syntax:**

```sql
embd_distance(vector_data) -> real
```

**Parameters:**

- `vector_data` - An `embd` type column containing vector data

**Returns:**

- `real` - The distance score from the vector similarity search. This value is only meaningful when used in queries with the `<==>` operator for vector similarity search.

**Usage:**

```sql
-- Get similarity search results with distance scores
select embd_distance(data) as distance, key, metadata
from s3_vectors.embeddings
where data <==> '[0.1, 0.2, 0.3, 0.4, 0.5]'::embd
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
| `metadata <==> json_filter`           | Metadata filtering during vector search   |

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
   select embd_distance(data) as distance, *
   from s3_vectors.embeddings
   where data <==> '[0.1, 0.2, 0.3, ...]'::embd
   order by 1
   limit 10;
   ```

4. **Vector search with metadata filtering**:
   ```sql
   select embd_distance(data) as distance, *
   from s3_vectors.embeddings
   where data <==> '[0.1, 0.2, 0.3, ...]'::embd
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
| embd             | Float32 vector data                    |
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
    bucket_arn 'arn:aws:s3vectors:us-east-1:123456789012:bucket/my-vector-bucket'
  );

-- or, create the foreign table manually
create foreign table if not exists s3_vectors.embeddings (
  key text not null,
  data embd not null,
  metadata jsonb
)
  server s3_vectors_server
  options (
    index_arn 'arn:aws:s3vectors:us-east-1:123456789012:bucket/my-vector-bucket/index/my-vector-index',
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
select embd_distance(data) as distance, key, metadata
from s3_vectors.embeddings
where data <==> '[0.1, 0.2, 0.3, 0.4, 0.5]'::embd
order by 1
limit 5;
```

### Inserting Vectors

```sql
-- Insert a single vector
insert into s3_vectors.embeddings (key, data, metadata)
values (
  'product_001',
  '[0.1, 0.2, 0.3, 0.4, 0.5]'::embd,
  '{"category": "electronics", "price": 299.99}'::jsonb
);

-- Insert multiple vectors
insert into s3_vectors.embeddings (key, data, metadata)
values
  ('product_002', '[0.2, 0.3, 0.4, 0.5, 0.6]'::embd, '{"category": "books"}'::jsonb),
  ('product_003', '[0.3, 0.4, 0.5, 0.6, 0.7]'::embd, '{"category": "clothing"}'::jsonb);
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
select embd_distance(data) as distance, key, metadata
from s3_vectors.embeddings
where data <==> '[0.1, 0.2, 0.3, 0.4, 0.5]'::embd
and metadata <==> '{"category": "electronics"}'::jsonb
order by 1
limit 3;
```

### Advanced Example: Semantic Search

```sql
-- Create a function to convert text to embeddings (pseudo-code)
-- This would typically use an external embedding service
create or replace function text_to_embedding(input_text text)
returns embd
language sql
as $$
  -- This is a placeholder - you would implement actual text embedding logic
  select '[0.1, 0.2, 0.3, 0.4, 0.5]'::embd;
$$;

-- Semantic search example
select embd_distance(data) as distance, key, metadata
from s3_vectors.embeddings
where data <==> text_to_embedding('Find similar products')
and metadata <==> '{"status": "active"}'::jsonb
order by 1
limit 10;
```
