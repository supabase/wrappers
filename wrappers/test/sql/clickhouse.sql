-- create foreign data wrapper and enable 'ClickHouseFdw'
drop foreign data wrapper if exists clickhouse_wrapper;
create foreign data wrapper clickhouse_wrapper
  handler wrappers_handler
  validator wrappers_validator
  options (
    wrapper 'ClickHouseFdw'
  );

-- create a wrappers ClickHouse server and specify connection string
drop server if exists my_clickhouse_server;
create server my_clickhouse_server
  foreign data wrapper clickhouse_wrapper
  options (
    conn_string 'tcp://default:@clickhouse:9000/supa'
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
  
insert into people(id, name)
values (1, 'foo');

select * from people;