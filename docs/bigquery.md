[BigQuery](https://cloud.google.com/bigquery) is a completely serverless and cost-effective enterprise data warehouse that works across clouds and scales with your data, with BI, machine learning and AI built in.

The BigQuery Wrapper allows you to read and write data from BigQuery within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

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

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper bigquery_wrapper
  handler big_query_fdw_handler
  validator big_query_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inide `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your BigQuery service account json in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'bigquery',
  '
    {
      "type": "service_account",
      "project_id": "your_gcp_project_id",
      "private_key_id": "your_private_key_id",
      "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
      ...
    }
  '
)
returning key_id;
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

## Creating Foreign Tables

The BigQuery Wrapper supports data reads and writes from BigQuery.

| Integration | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| BigQuery    |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

For example:

```sql
create foreign table my_bigquery_table (
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

### Foreign table options

The full list of foreign table options are below:

- `table` - Source table or view name in BigQuery, required.

  This can also be a subquery enclosed in parentheses, for example,

  ```sql
  table '(select * except(props), to_json_string(props) as props from `my_project.my_dataset.my_table`)'
  ```

  **Note**: When using subquery in this option, full qualitified table name must be used.

- `location` - Source table location, optional. Default is 'US'.
- `timeout` - Query request timeout in milliseconds, optional. Default is '30000' (30 seconds).
- `rowid_column` - Primary key column name, optional for data scan, required for data modify

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown.

## Inserting Rows & the Streaming Buffer

This foreign data wrapper uses BigQuery’s `insertAll` API method to create a `streamingBuffer` with an associated partition time. **Within that partition time, the data cannot be updated, deleted, or fully exported**. Only after the time has elapsed (up to 90 minutes according to [BigQuery’s documentation](https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery)); can you perform operations.

If you attempt an `UPDATE` or `DELETE` statement on rows while in the streamingBuffer, you will get an error of `UPDATE` or `DELETE` statement over table datasetName - note that tableName would affect rows in the streaming buffer, which is not supported.

## Examples

Some examples on how to use BigQuery foreign tables.

Let's prepare the source table in BigQuery first:

```sql
-- Run below SQLs on BigQuery to create source table
create table your_project_id.your_dataset_id.people (
  id int64,
  name string,
  ts timestamp
);

-- Add some test data
insert into your_project_id.your_dataset_id.people values
  (1, 'Luke Skywalker', current_timestamp()),
  (2, 'Leia Organa', current_timestamp()),
  (3, 'Han Solo', current_timestamp());
```

### Basic example

This example will create a "foreign table" inside your Postgres database called `people` and query its data:

```sql
create foreign table people (
  id bigint,
  name text,
  ts timestamp
)
  server bigquery_server
  options (
    table 'people',
    location 'EU'
  );

select * from people;
```

### Data modify example

This example will modify data in a "foreign table" inside your Postgres database called `people`, note that `rowid_column` option is mandatory:

```sql
create foreign table people (
  id bigint,
  name text,
  ts timestamp
)
  server bigquery_server
  options (
    table 'people',
    location 'EU',
    rowid_column 'id'
  );

-- insert new data
insert into people(id, name, ts)
values (4, 'Yoda', '2023-01-01 12:34:56');

-- update existing data
update people
set name = 'Anakin Skywalker'
where id = 1;

-- delete data
delete from people
where id = 2;
```
