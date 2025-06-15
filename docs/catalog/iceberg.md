---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Apache Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is a high performance open-source format for large analytic tables.

The Iceberg Wrapper allows you to read data from Apache Iceberg within your Postgres database.

## Preparation

Before you can query Iceberg, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Iceberg Wrapper

Enable the `iceberg_wrapper` FDW:

```sql
create foreign data wrapper iceberg_wrapper
  handler iceberg_fdw_handler
  validator iceberg_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your AWS credentials in Vault and retrieve the created
-- `aws_access_key_id` and `aws_secret_access_key`
select vault.create_secret(
  '<access key id>',  -- secret to be encrypted
  'aws_access_key_id',  -- secret name
  'AWS access key for Wrappers'  -- secret description
);
select vault.create_secret(
  '<secret access key>'
  'aws_secret_access_key',
  'AWS secret access key for Wrappers'
);
```

### Connecting to Icerberg

We need to provide Postgres with the credentials to connect to Iceberg. We can do this using the `create server` command.

For any server options need to be stored in Vault, you can add a prefix `vault_` to its name and use the secret ID returned from the `select vault.create_secret()` statement as the option value.

#### Connecting to AWS S3 Tables

=== "With Vault"

    ```sql
    create server iceberg_server
      foreign data wrapper iceberg_wrapper
      options (
        -- The key id saved in Vault from above
        vault_aws_access_key_id '<key_ID>',

        -- The secret id saved in Vault from above
        vault_aws_secret_access_key '<secret_key>',

        -- AWS region
        region_name 'us-east-1',

        -- AWS S3 table bucket ARN
        aws_s3table_bucket_arn 'arn:aws:s3tables:us-east-1:204203087419:bucket/my-table-bucket'
      );
    ```

=== "Without Vault"

    ```sql
    create server iceberg_server
      foreign data wrapper iceberg_wrapper
      options (
        -- The AWS access key ID
        aws_access_key_id '<key_ID>',

        -- The AWS secret access key
        aws_secret_access_key '<secret_key>',

        -- AWS region
        region_name 'us-east-1',

        -- AWS S3 table bucket ARN
        aws_s3table_bucket_arn 'arn:aws:s3tables:us-east-1:204203087419:bucket/my-table-bucket'
      );
    ```

#### Connecting to Iceberg REST Catalog + AWS S3 (or compatible) storage

=== "With Vault"

    ```sql
    create server iceberg_server
      foreign data wrapper iceberg_wrapper
      options (
        -- The key id saved in Vault from above
        vault_aws_access_key_id '<key_ID>',

        -- The secret id saved in Vault from above
        vault_aws_secret_access_key '<secret_key>',

        -- AWS region
        region_name 'us-east-1',

        -- Iceberg REST Catalog URI
        catalog_uri 'https://rest-catalog/ws',

        -- Warehouse name
        warehouse 'warehouse',

        -- AWS S3 endpoint URL, optional
        "s3.endpoint" 'https://alternative-s3-storage:8000'
      );
    ```

=== "Without Vault"

    ```sql
    create server iceberg_server
      foreign data wrapper iceberg_wrapper
      options (
        -- The key id saved in Vault from above
        aws_access_key_id '<key_ID>',

        -- The secret id saved in Vault from above
        aws_secret_access_key '<secret_key>',

        -- AWS region
        region_name 'us-east-1',

        -- Iceberg REST Catalog URI
        catalog_uri 'https://rest-catalog/ws',

        -- Warehouse name
        warehouse 'warehouse',

        -- AWS S3 endpoint URL, optional
        "s3.endpoint" 'https://alternative-s3-storage:8000'
      );
    ```

!!! info

    For other optional S3 options, please refer to [PyIceberg S3 Configuration](https://py.iceberg.apache.org/configuration/#s3).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists iceberg;
```

## Options

The full list of foreign table options are below:

- `table` - Fully qualified source table name with all namespaces in Iceberg, required.

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from Iceberg.

For example, using below SQL can automatically create foreign tables in the `iceberg` schema.

```sql
-- create all the foreign tables from Iceberg "docs_example" namespace
import foreign schema "docs_example"
  from server iceberg_server into iceberg;

-- or, only create "readme" and "guides" foreign tables
import foreign schema "docs_example"
  limit to ("readme", "guides")
  from server iceberg_server into iceberg;

-- or, create all foreign tables except "readme"
import foreign schema "docs_example"
  except ("readme")
  from server iceberg_server into iceberg;
```

!!! note

    By default, the `import foreign schema` statement will silently skip all the incompatible columns. Use the option `strict` to prevent this behavior. For example,

    ```sql
    import foreign schema "docs_example" from server iceberg_server into iceberg
    options (
      -- this will fail the 'import foreign schema' statement when Iceberg table
      -- column cannot be mapped to Postgres
      strict 'true'
    );
    ```

### Iceberg Tables

This is an object representing Iceberg table.

Ref: [Iceberg Table Spec](https://iceberg.apache.org/spec/#iceberg-table-spec)

#### Operations

| Object     | Select | Insert | Update | Delete | Truncate |
| ---------- | :----: | :----: | :----: | :----: | :------: |
| table      |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

You can manually create the foreign table like below if you did not use `import foreign schema`.

```sql
create foreign table iceberg.guides (
  id bigint,
  title text,
  content text,
  created_at timestamp
)
  server iceberg_server
  options (
    table 'docs_example.guides'
  );
```

## Query Pushdown Support

This FDW supports `where` clause pushdown with below operators.

| Operator                                   | Note                             |
| ------------------------------------------ | -------------------------------- |
| `=`, `>`, `>=`, `<`, `<=`, `<>`, `!=`      |                                  |
| `is null`, `is not null`                   |                                  |
| `x`, `not x`, `x is true`, `x is not true` | column `x` data type is `boolean`|
| `x between a and b`                        | column `x` data type can be datetime or numeric types |
| `like 'abc%'`, `not like 'abc%'`           | only support `starts with` pattern |
| `in (x, y, z)`, `not in (x, y, z)`         |                                  |

!!! note

    For multiple filters, only logical `AND` is supported. For example,

    ```sql
    -- this can be pushed down
    select * from table where x = a and y = b;

    -- this cannot be pushed down
    select * from table where x = a or y = b;
    ```

## Supported Data Types

| Postgres Type    | Iceberg Type                   |
| ---------------- | ------------------------------ |
| boolean          | boolean                        |
| real             | float                          |
| integer          | int                            |
| double precision | double                         |
| bigint           | long                           |
| numeric          | decimal                        |
| text             | string                         |
| date             | date                           |
| time             | time                           |
| timestamp        | timestamp, timestamp_ns        |
| timestamptz      | timestamptz, timestamptz_ns    |
| jsonb            | struct, list, map              |
| bytea            | binary                         |
| uuid             | uuid                           |

## Limitations

This section describes important limitations and considerations when using this FDW:

- Only supports specific data type mappings between Postgres and Iceberg
- Only supports read operations (no INSERT, UPDATE, DELETE, or TRUNCATE)
- [Apache Iceberg schema evolution](https://iceberg.apache.org/spec/#schema-evolution) is not supported
- When using Iceberg REST catalog, only supports AWS S3 (or compatible) as the storage
- Materialized views using these foreign tables may fail during logical backups

## Examples

### Basic Example

First, create a server for AWS S3 Tables:

```sql
create server iceberg_server
  foreign data wrapper iceberg_wrapper
  options (
    aws_access_key_id '<AWS_access_key_ID>',
    aws_secret_access_key '<AWS_secret_access_key>',
    region_name 'us-east-1',
    aws_s3table_bucket_arn 'arn:aws:s3tables:us-east-1:204203087419:bucket/my-table-bucket'
  );
```

Import the foreign table:

```sql
-- Run below SQL to import all tables under namespace 'docs_example'
import foreign schema "docs_example"
  from server iceberg_server into iceberg;

-- or, create the foreign table manually
create foreign table if not exists iceberg.guides (
  id bigint,
  title text,
  content text,
  created_at timestamp
)
  server iceberg_server
  options (
    table 'docs_example.guides'
  );
```

Then query the foreign table:

```sql
select * from iceberg.guides;
```

### Read Cloudflare R2 Data Catalog

First, follow the steps in [Getting Started Guide](https://developers.cloudflare.com/r2/data-catalog/get-started/) to create a R2 Catalog on Cloudflare. Once it is completed, create a server like below:

```sql
create server iceberg_server
  foreign data wrapper iceberg_wrapper
  options (
    aws_access_key_id '<R2_access_key_ID>',
    aws_secret_access_key '<R2_secret_access_key>',
	token '<R2 API token>',
	warehouse 'xxx_r2-data-catalog-tutorial',
	"s3.endpoint" 'https://xxx.r2.cloudflarestorage.com',
    catalog_uri 'https://catalog.cloudflarestorage.com/xxx/r2-data-catalog-tutorial'
  );
```

Then, import all the tables in `default` namespace and query it:

```sql
import foreign schema "default" from server iceberg_server into iceberg;

select * from iceberg.people;
```

### Query Pushdown Examples

```sql
-- the filter 'id = 42' will be pushed down to Iceberg
select * from iceberg.guides where id = 42;

-- the pushdown filter can also be on the partition column 'created_at',
-- this can greatly reduce query cost
select * from iceberg.guides
where created_at >= timestamp '2025-05-16 12:34:56';

-- multiple filters must use logical 'AND'
select * from iceberg.guides where id > 42 and title like 'Supabase%';
```

