# ClickHouse Foreign Data Wrapper

This is a foreign data wrapper for [Clickhouse](https://clickhouse.com/). It is developed using [Supabase Remote](https://github.com/supabase/remote) and supports both data scan and modify. 

## Basic usage

These steps outline how to use the Clickhouse FDW:

1. Clone this repo

```
git clone https://github.com/supabase/remote.git
```

2. Run it using pgx with feature:

```bash
cargo pgx run --features clickhouse_fdw
```

3. Start a Clickhouse server with some data populated

```
cd test/remotes/clickhouse
docker-compose -f clickhouse.yaml up
```

4. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
create extension remote;

-- create foreign data wrapper and enable 'clickhouse_fdw'
drop foreign data wrapper if exists remote_clickhouse;
create foreign data wrapper remote_clickhouse
  handler remote_handler
  validator remote_validator
  options (
    wrapper 'clickhouse_fdw'
  );

-- create a remote ClickHouse server and specify connection string
drop server if exists my_clickhouse_server;
create server my_clickhouse_server
  foreign data wrapper remote_clickhouse
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

6. Check the changes on Clickhouse:

```bash
cd test/remotes/clickhouse

# start ClickHouse server
docker-compose -f clickhouse.yaml up

# open another shell and connect to ClickHouse server
docker-compose -f clickhouse.yaml exec server clickhouse-client
```

Run the following SQL on ClickHouse

```sql
-- data scan
select * from people;

-- data modification in CH will appear in PG:
insert into people values (3, 'Han Solo');
```

