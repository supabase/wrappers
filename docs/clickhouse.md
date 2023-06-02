[ClickHouse](https://clickhouse.com/) is a fast open-source column-oriented database management system that allows generating analytical data reports in real-time using SQL queries.

ClickHouse FDW supports both data read and modify.

### Supported Data Types

| Postgres Type      | ClickHouse Type   |
| ------------------ | ----------------- |
| boolean            | UInt8             |
| smallint           | Int16             |
| integer            | UInt16            |
| integer            | Int32             |
| bigint             | UInt32            |
| bigint             | Int64             |
| bigint             | UInt64            |
| real               | Float32           |
| double precision   | Float64           |
| text               | String            |
| date               | Date              |
| timestamp          | DateTime          |

### Wrapper 
To get started with the ClickHouse wrapper, create a foreign data wrapper specifying `handler` and `validator` as below.

```sql
create extension if not exists wrappers;

create foreign data wrapper clickhouse_wrapper
  handler click_house_fdw_handler
  validator click_house_fdw_validator;
```

### Server 

Next, we need to create a server for the FDW to hold options and credentials.

#### Auth (Supabase)

If you are using the Supabase platform, this is the recommended approach for securing your ClickHouse credentials.

For example, to store ClickHouse connection string in Vault and retrieve the `key_id`,

```sql
-- save ClickHouse connection string in Vault and get its key id
select pgsodium.create_key(name := 'clickhouse');
insert into vault.secrets (secret, key_id) values (
  'tcp://default:@localhost:9000/default',
  (select id from pgsodium.valid_key where name = 'clickhouse')
) returning key_id;
```

Then create the foreign server,

```sql
do $$
declare
  key_id text;
begin
  select id into key_id from pgsodium.valid_key where name = 'clickhouse' limit 1;

  execute format(
    E'create server clickhouse_server \n'
    '   foreign data wrapper clickhouse_server \n'
    '   options (conn_string_id ''%s'');',
    key_id
  );
end $$;
```

#### Auth (Non-Supabase)

If the platform you are using does not support `pgsodium` and `Vault`, you can create a server by storing your ClickHouse credentials directly.


!!! important

    Credentials stored using this method can be viewed as plain text by anyone with access to `pg_catalog.pg_foreign_server`


```sql
create server clickhouse_server
  foreign data wrapper clickhouse_wrapper
  options (
    conn_string 'tcp://default:@localhost:9000/default'
  );
```


### Tables

ClickHouse wrapper is implemented with [ELT](https://hevodata.com/learn/etl-vs-elt/) approach, so the data transformation is encouraged to be performed locally after data is extracted from remote data source.


#### Foreign Table Options

The full list of foreign table options are below:

- `table` - Source table name in ClickHouse, required.

   This can also be a subquery enclosed in parentheses, for example,

   ```
   table '(select * from my_table)'
   ```

   [Parametrized view](https://clickhouse.com/docs/en/sql-reference/statements/create/view#parameterized-view) is also supported in the subquery. In this case, you need to define a column for each parameter and use `where` to pass values to them. For example,

   ```
    create foreign table test_vw (
      id bigint,
      col1 text,
      col2 bigint,
      _param1 text,
      _param2 bigint
    )
      server clickhouse_server
      options (
        table '(select * from my_view(column1=${_param1}, column2=${_param2}))'
      );

    select * from test_vw where _param1='aaa' and _param2=32;
   ```

- `rowid_column` - Primary key column name, optional for data scan, required for data modify

#### Examples

Create a source table on ClickHouse and insert some data,

```sql
drop table if exists people;
create table people (
    id Int64, 
    name String
)
engine=MergeTree()
order by id;

insert into people values (1, 'Luke Skywalker'), (2, 'Leia Organa'), (3, 'Han Solo');
```

Create foreign table and run a query on Postgres to read ClickHouse table,

```sql
create foreign table people (
  id bigint,
  name text
)
  server clickhouse_server
  options (
    table 'people'
  );

-- data scan
select * from people;

-- data modify
insert into people values (4, 'Yoda');
update people set name = 'Princess Leia' where id = 2;
delete from people where id = 3;
```
