---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# BigQuery

[BigQuery](https://cloud.google.com/bigquery) is a completely serverless and cost-effective enterprise data warehouse that works across clouds and scales with your data, with BI, machine learning and AI built in.

The BigQuery Wrapper allows you to read and write data from BigQuery within your Postgres database.

## Preparation

Before you can query BigQuery, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the BigQuery Wrapper

Enable the `bigquery_wrapper` FDW:

```sql
create foreign data wrapper bigquery_wrapper
  handler big_query_fdw_handler
  validator big_query_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your BigQuery service account json in Vault
select vault.create_secret(
  '
    {
      "type": "service_account",
      "project_id": "your_gcp_project_id",
      "private_key_id": "your_private_key_id",
      "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
      ...
    }
  ',
  'bigquery',
  'BigQuery service account json for Wrappers'
);

-- Retrieve the `key_id`
select key_id from vault.decrypted_secrets where name = 'bigquery';
```

### Connecting to BigQuery

We need to provide Postgres with the credentials to connect to BigQuery, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server bigquery_server
      foreign data wrapper bigquery_wrapper
      options (
        sa_key_id '<key_ID>', -- The Key ID from above.
        project_id 'your_gcp_project_id',
        dataset_id 'your_gcp_dataset_id'
      );
    ```

=== "Without Vault"

    ```sql
    create server bigquery_server
      foreign data wrapper bigquery_wrapper
      options (
        sa_key '
        {
           "type": "service_account",
           "project_id": "your_gcp_project_id",
           ...
        }
       ',
        project_id 'your_gcp_project_id',
        dataset_id 'your_gcp_dataset_id'
      );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists bigquery;
```

## Options

The following options are available when creating BigQuery foreign tables:

- `table` - Source table or view name in BigQuery, required
- `location` - Source table location (default: 'US')
- `timeout` - Query request timeout in milliseconds (default: 30000)
- `rowid_column` - Primary key column name (required for data modification)

You can also use a subquery as the table option:

```sql
table '(select * except(props), to_json_string(props) as props from `my_project.my_dataset.my_table`)'
```

Note: When using subquery, full qualified table name must be used.

## Entites

### Tables

The BigQuery Wrapper supports data reads and writes from BigQuery tables and views.

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Tables |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table bigquery.my_bigquery_table (
  id bigint,
  name text,
  ts timestamp
)
  server bigquery_server
  options (
    table 'people',
    location 'EU'
  );
```

#### Notes

- Supports `where`, `order by` and `limit` clause pushdown
- When using `rowid_column`, it must be specified for data modification operations
- Data in the streaming buffer cannot be updated or deleted until the buffer is flushed (up to 90 minutes)

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown.

## Inserting Rows & the Streaming Buffer

This foreign data wrapper uses BigQuery’s `insertAll` API method to create a `streamingBuffer` with an associated partition time. **Within that partition time, the data cannot be updated, deleted, or fully exported**. Only after the time has elapsed (up to 90 minutes according to [BigQuery’s documentation](https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery)), can you perform operations.

If you attempt an `UPDATE` or `DELETE` statement on rows while in the streamingBuffer, you will get an error of `UPDATE` or `DELETE` statement over table datasetName - note that tableName would affect rows in the streaming buffer, which is not supported.

## Supported Data Types

| Postgres Type    | BigQuery Type |
| ---------------- | ------------- |
| boolean          | BOOL          |
| bigint           | INT64         |
| double precision | FLOAT64       |
| numeric          | NUMERIC       |
| text             | STRING        |
| varchar          | STRING        |
| date             | DATE          |
| timestamp        | DATETIME      |
| timestamp        | TIMESTAMP     |
| timestamptz      | TIMESTAMP     |
| jsonb            | JSON          |

## Limitations

This section describes important limitations and considerations when using this FDW:

- Large result sets may experience network latency during data transfer
- Data in streaming buffer cannot be modified for up to 90 minutes
- Only supports specific data type mappings between Postgres and BigQuery
- Materialized views using foreign tables may fail during logical backups

## Examples

Some examples on how to use BigQuery foreign tables.

Let's prepare the source table in BigQuery first:

```sql
-- Run below SQLs on BigQuery to create source table
create table your_project_id.your_dataset_id.people (
  id int64,
  name string,
  ts timestamp,
  props jsonb
);

-- Add some test data
insert into your_project_id.your_dataset_id.people values
  (1, 'Luke Skywalker', current_timestamp(), parse_json('{"coordinates":[10,20],"id":1}')),
  (2, 'Leia Organa', current_timestamp(), null),
  (3, 'Han Solo', current_timestamp(), null);
```

### Basic example

This example will create a "foreign table" inside your Postgres database called `people` and query its data:

```sql
create foreign table bigquery.people (
  id bigint,
  name text,
  ts timestamp,
  props jsonb
)
  server bigquery_server
  options (
    table 'people',
    location 'EU'
  );

select * from bigquery.people;
```

### Data modify example

This example will modify data in a "foreign table" inside your Postgres database called `people`, note that `rowid_column` option is mandatory:

```sql
create foreign table bigquery.people (
  id bigint,
  name text,
  ts timestamp,
  props jsonb
)
  server bigquery_server
  options (
    table 'people',
    location 'EU',
    rowid_column 'id'
  );

-- insert new data
insert into bigquery.people(id, name, ts, props)
values (4, 'Yoda', '2023-01-01 12:34:56', '{"coordinates":[10,20],"id":1}'::jsonb);

-- update existing data
update bigquery.people
set name = 'Anakin Skywalker', props = '{"coordinates":[30,40],"id":42}'::jsonb
where id = 1;

-- delete data
delete from bigquery.people
where id = 2;
```
