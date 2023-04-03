[BigQuery](https://cloud.google.com/bigquery) is a completely serverless and cost-effective enterprise data warehouse that works across clouds and scales with your data, with BI, machine learning and AI built in.

BigQuery FDW supports both data read and modify.

### Supported Data Types

| Postgres Type      | BigQuery Type   |
| ------------------ | --------------- |
| boolean            | BOOL            |
| bigint             | INT64           |
| double precision   | FLOAT64         |
| numeric            | NUMERIC         |
| text               | STRING          |
| varchar            | STRING          |
| date               | DATE            |
| timestamp          | DATETIME        |
| timestamp          | TIMESTAMP       |

### Wrapper 
To get started with the BigQuery wrapper, create a foreign data wrapper specifying `handler` and `validator` as below.

```sql
create extension if not exists wrappers;

create foreign data wrapper bigquery_wrapper
  handler big_query_fdw_handler
  validator big_query_fdw_validator;
```

### Server 

Next, we need to create a server for the FDW to hold options and credentials.

#### Auth (Supabase)

If you are using the Supabase platform, this is the recommended approach for securing your GCP credentials.

For example, to store GCP credential JSON in Vault and retrieve the `key_id`,

```sql
-- save BigQuery service account json in Vault and get its key id
select pgsodium.create_key(name := 'bigquery');
insert into vault.secrets (secret, key_id) values ('
{
  "type": "service_account",
  "project_id": "your_gcp_project_id",
  "private_key_id": "your_private_key_id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  ...
}
',
(select id from pgsodium.valid_key where name = 'bigquery')
) returning key_id;
```

Then create the foreign server,

```sql
do $$
declare
  key_id text;
begin
  select id into key_id from pgsodium.valid_key where name = 'bigquery' limit 1;

  execute format(
    E'create server bigquery_server \n'
    '   foreign data wrapper bigquery_wrapper \n'
    '   options ( \n'
    '     sa_key_id ''%s'', \n'
    '     project_id ''your_gcp_project_id'', \n'
    '     dataset_id ''your_gcp_dataset_id'' \n'
    ' );',
    key_id
  );
end $$;
```

#### Auth (Non-Supabase)

If the platform you are using does not support `pgsodium` and `Vault`, you can create a server by storing yourt GCP credentials directly.


!!! important

    Credentials stored using this method can be viewed as plain text by anyone with access to `pg_catalog.pg_foreign_server`


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


### Tables

BigQuery wrapper is implemented with [ELT](https://hevodata.com/learn/etl-vs-elt/) approach, so the data transformation is encouraged to be performed locally after data is extracted from remote data source.


#### Foreign Table Options

The full list of foreign table options are below:

- `table` - Source table or view name in BigQuery, required.

   This can also be a subquery enclosed in parentheses, for example,

   ```
   table '(select * except(props), to_json_string(props) as props from `my_project.my_dataset.my_table`)'
   ```

   **Note**: When using subquery in this option, full qualitified table name must be used.

- `location` - Source table location, optional. Default is 'US'.
- `rowid_column` - Primary key column name, optional for data scan, required for data modify

#### Examples

Create a source table on BigQuery and insert some data,

```sql
create table your_project_id.your_dataset_id.people (
  id int64,
  name string,
  ts timestamp
);

insert into your_project_id.your_dataset_id.people values
  (1, 'Luke Skywalker', current_timestamp()), 
  (2, 'Leia Organa', current_timestamp()), 
  (3, 'Han Solo', current_timestamp());
```

Create foreign table and run a query on Postgres to read BigQuery table,

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
