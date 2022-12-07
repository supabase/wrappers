# BigQuery Foreign Data Wrapper

This is a foreign data wrapper for [BigQuery](https://cloud.google.com/bigquery). It is developed using [Wrappers](https://github.com/supabase/wrappers) and only supports data scan at this moment.

## Basic usage

These steps outline how to use the BigQuery FDW:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgx with feature:

```bash
cd wrappers/wrappers
cargo pgx run --features bigquery_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
create extension wrappers;

-- create foreign data wrapper and enable 'BigQueryFdw'
create foreign data wrapper bigquery_wrapper
  handler big_query_fdw_handler
  validator big_query_fdw_validator;

-- create a wrappers BigQuery server and specify connection info
-- Here we're using the service account key stored in Vault, if you don't want
-- to use Vault, you can directly specify the service account key using `sa_key`
-- option. For example,
--
-- create server my_bigquery_server
--   foreign data wrapper bigquery_wrapper
--   options (
--     sa_key '
--     {
--        "type": "service_account",
--        "project_id": "your_gcp_project_id",
--        ...
--     }
--    ',
--     project_id 'your_gcp_project_id',
--     dataset_id 'your_gcp_dataset_id'
--   );

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

do $$
declare
  key_id text;
begin
  select id into key_id from pgsodium.valid_key where name = 'bigquery' limit 1;

  execute format(
    E'create server my_bigquery_server \n'
    '   foreign data wrapper bigquery_wrapper \n'
    '   options ( \n'
    '     sa_key_id ''%s'', \n'
    '     project_id ''your_gcp_project_id'', \n'
    '     dataset_id ''your_gcp_dataset_id'' \n'
    ' );',
    key_id
  );
end $$;

-- create an example foreign table
create foreign table people (
  id bigint,
  name text,
  ts timestamp
)
  server my_bigquery_server
  options (
    table 'people',     -- source table in BigQuery, required
    location 'EU',      -- table location, optional
    rowid_column 'id',  -- primary key column name, optional for scan, required for modify
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

## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.0   | 2022-11-30 | Initial version                                      |
