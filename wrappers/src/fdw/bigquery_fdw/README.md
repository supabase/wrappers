# BigQuery Foreign Data Wrapper

This is a foreign data wrapper for [BigQuery](https://cloud.google.com/bigquery). It is developed using [Supabase Remote](https://github.com/supabase/remote) and only supports data scan at this moment.

## Basic usage

These steps outline how to use the BigQuery FDW:

1. Clone this repo

```
git clone https://github.com/supabase/remote.git
```

2. Run it using pgx with feature:

```bash
cargo pgx run --features bigquery_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
drop extension remote cascade;
create extension remote;

-- create foreign data wrapper and enable 'bigquery_fdw'
drop foreign data wrapper if exists remote_bigquery cascade;
create foreign data wrapper remote_bigquery
  handler remote_handler
  validator remote_validator
  options (
    wrapper 'bigquery_fdw'
  );

-- create a remote BigQuery server and specify connection info
drop server if exists my_bigquery_server cascade;
create server my_bigquery_server
  foreign data wrapper remote_bigquery
  options (
    sa_key_file '/absolute/path/to/service_account_key.json',
    project_id 'your_gcp_project_id',
    dataset_id 'your_gcp_dataset_id'
  );

-- create an example foreign table
drop foreign table if exists people;
create foreign table people (
  id bigint,
  name text,
  ts timestamp
)
  server my_bigquery_server
  options (
    table 'people',     -- source table in BigQuery, required
    location 'EU',      -- table location, optional
    rowid_column 'id',  -- primary key column name, optional for scan, required for update
    startup_cost '42'   -- execution startup cost for exection planning, optional
  );
```

4. Create table in BigQuery and add some example data:

```
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
5. Run some queries to check if it is working:

On Postgres:

```sql
-- data scan
select * from people;
```

On BigQuery:

```sql
-- data scan
select * from people;

-- data modification in BigQuery will appear in Postgres:
insert into people values (4, 'Yoda', current_timestamp());
```

