# ClickHouse Foreign Data Wrapper

This is a foreign data wrapper for [Clickhouse](https://clickhouse.com/). It is developed using [Wrappers](https://github.com/supabase/wrappers) and supports both data scan and modify. 

## Basic usage

These steps outline how to use the Clickhouse FDW:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgx with feature:

```bash
cd wrappers/wrappers
cargo pgx run --features clickhouse_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
drop extension if exists wrappers cascade;
create extension wrappers;

-- create foreign data wrapper and enable 'ClickHouseFdw'
drop foreign data wrapper if exists wrappers_clickhouse;
create foreign data wrapper wrappers_clickhouse
  handler wrappers_handler
  validator wrappers_validator
  options (
    wrapper 'ClickHouseFdw'
  );

-- create a wrappers ClickHouse server and specify connection string
drop server if exists my_clickhouse_server;
create server my_clickhouse_server
  foreign data wrapper wrappers_clickhouse
  options (
    conn_string 'tcp://default:@localhost:9000/default'
  );

-- create an example foreign table
drop foreign table if exists people;
create foreign table people (
  id bigint,
  name text
)
  server my_clickhouse_server
  options (
    table 'people',
    rowid_column 'id',
    startup_cost '42'
  );
```

4. Open another shell and start a Clickhouse server with some data populated

```
cd src/fdw/clickhouse_fdw/test_server
docker-compose -f clickhouse.yaml up
```

5. Run some queries to check if it is working:

On Postgres:

```sql
-- data scan
select * from people;

-- data modify
insert into people values (4, 'Yoda') ;
update people set name = 'Princess Leia' where id = 2;
delete from people where id = 3;
```

6. Open another shell and check the changes on Clickhouse:

```bash
cd src/fdw/clickhouse_fdw/test_server
docker-compose -f clickhouse.yaml exec server clickhouse-client
```

Run the following SQL on ClickHouse

```sql
-- data scan
select * from people;

-- data modification in ClickHouse will appear in Postgres:
insert into people values (3, 'Han Solo');
```

