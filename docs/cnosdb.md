[CnosDB](https://www.cnosdb.com/) is an Open Source Distributed Time Series Database with high performance, high compression ratio and high usability.

The CnosDB Wrapper allows you to read and write data from CnosDB within your Postgres database.

## Supported Data Types

| Postgres Type      | CnosDB Type     |
| ------------------ |-----------------|
| boolean            | BOOLEAN         |
| bigint             | BIGINT          |
| bigint             | BIGINT UNSIGNED |
| double precision   | DOUBLE          |
| text               | STRING          |
| timestamp          | TIMESTAMP       |

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper cnosdb_wrapper
  handler cnosdb_fdw_handler
  validator cnosdb_fdw_validator;
```

### Connecting to CnosDB

We can do this using the `create server` command:

    ```sql
    CREATE SERVER my_cnosdb_server
    FOREIGN DATA WRAPPER cnosdb_wrapper
    OPTIONS (
      url 'http://localhost:8904',
      username 'root',
      password '',
      tenant 'cnosdb',
      db 'public'
    )
    ```

Check [more connection string parameters](https://github.com/suharev7/clickhouse-rs#dns).

## Creating Foreign Tables

The CnosDB Wrapper supports data reads from CnosDB.

| Integration | Select            | Insert | Update | Delete | Truncate          |
| ----------- | :----:            |:------:|:------:|:------:| :----:            |
| ClickHouse  | :white_check_mark:|  :x:   |  :x:   |  :x:   | :x:               |

For example:

```sql
CREATE FOREIGN TABLE air(
    time timestamp,
    station text,
    visibility double precision,
    temperature double precision,
    pressure double precision
    )
    SERVER my_cnosdb_server
    OPTIONS (
        table 'air'
    )
```

### Foreign table options

The full list of foreign table options are below:

- `table` - Source table name in CnosDB, required.

## Examples

Some examples on how to use CnosDB foreign tables.

### Basic example

This will create a "foreign table" inside your Postgres database called `air`:

```sql
-- Run below SQLs on CnosDB to create source table
drop table if exists air;
CREATE TABLE air (
    visibility DOUBLE,
    temperature DOUBLE,
    pressure DOUBLE,
    TAGS(station));

-- Add some test data
INSERT INTO air (time, station, visibility, temperature, pressure) VALUES('2023-01-01 01:10:00', 'XiaoMaiDao', 79, 80, 63);
INSERT INTO air (time, station, visibility, temperature, pressure) VALUES('2023-01-01 01:20:00', 'XiaoMai', 80, 60, 62);
INSERT INTO air (time, station, visibility, temperature, pressure) VALUES('2023-01-01 01:30:00', 'Xiao', 81, 70, 61);
```

Create foreign table on Postgres database:

```sql
CREATE FOREIGN TABLE air(
    time timestamp,
    station text,
    visibility double precision,
    temperature double precision,
    pressure double precision
  )
  SERVER my_cnosdb_server
  OPTIONS (
    table 'air'
  )

-- data scan
select * from air;
```




