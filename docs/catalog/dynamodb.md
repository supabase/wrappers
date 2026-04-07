---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# AWS DynamoDB

[AWS DynamoDB](https://aws.amazon.com/dynamodb/) is a fully managed, serverless, key-value NoSQL database designed to run high-performance applications at any scale.

The DynamoDB Wrapper allows you to read and write DynamoDB table data within your Postgres database.

## Preparation

Before you can query DynamoDB, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the DynamoDB Wrapper

Enable the `dynamodb_wrapper` FDW:

```sql
create foreign data wrapper dynamodb_wrapper
  handler dynamo_db_fdw_handler
  validator dynamo_db_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your AWS access key ID in Vault
select vault.create_secret(
  '<access key id>',
  'dynamodb_access_key_id',
  'AWS access key ID for DynamoDB Wrappers'
);

-- Save your AWS secret access key in Vault
select vault.create_secret(
  '<secret access key>',
  'dynamodb_secret_access_key',
  'AWS secret access key for DynamoDB Wrappers'
);
```

### Connecting to DynamoDB

We need to provide Postgres with the credentials to connect to DynamoDB. We can do this using the `create server` command.

=== "With Vault"

    ```sql
    create server dynamodb_server
      foreign data wrapper dynamodb_wrapper
      options (
        -- The key id saved in Vault from above
        vault_access_key_id '<vault_key_id>',

        -- The secret id saved in Vault from above
        vault_secret_access_key '<vault_secret_id>',

        -- AWS region where your DynamoDB table(s) reside
        region 'us-east-1'
      );
    ```

=== "Without Vault"

    ```sql
    create server dynamodb_server
      foreign data wrapper dynamodb_wrapper
      options (
        aws_access_key_id '<your_access_key_id>',
        aws_secret_access_key '<your_secret_access_key>',
        region 'us-east-1'
      );
    ```

#### Additional Server Options

| Option       | Required | Description                                                                 |
| ------------ | :------: | --------------------------------------------------------------------------- |
| `region`     | Yes      | AWS region where your DynamoDB tables reside (e.g. `us-east-1`)             |
| `endpoint_url` | No     | Custom endpoint URL, useful for local development with DynamoDB Local       |

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists dynamodb;
```

## Options

The full list of foreign table options are below:

| Option          | Required | Description                                                                             |
| --------------- | :------: | --------------------------------------------------------------------------------------- |
| `table`         | Yes      | The DynamoDB table name to query                                                        |
| `rowid_column`  | No       | The column to use as the row identifier for `UPDATE` and `DELETE` operations. Must be the partition key column. Required for write operations. |

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to automatically create foreign table definitions from your DynamoDB tables.

```sql
import foreign schema dynamodb from server dynamodb_server into dynamodb;
```

This will call `ListTables` and `DescribeTable` for each table and generate a `CREATE FOREIGN TABLE` statement with:

- Key columns (partition key and optional sort key) typed as `text`
- A `_attrs jsonb` catch-all column that captures every non-key attribute as a JSON object

For example, a DynamoDB table `users` with partition key `id` and attributes `name`, `age`, and `active` would produce:

```sql
create foreign table users (
  id    text not null,
  _attrs jsonb
)
server dynamodb_server
options (table 'users', rowid_column 'id');
```

You can then access individual attributes via standard JSON operators:

```sql
select
  id,
  _attrs->>'name'           as name,
  (_attrs->>'age')::integer as age,
  (_attrs->>'active')::boolean as active
from dynamodb.users;
```

The `_attrs` name was chosen to avoid conflicts with real DynamoDB attribute names. If you need typed columns or a different layout, create the foreign table manually instead (see [Create a foreign table manually](#create-a-foreign-table-manually)).

You can also limit the import to specific tables:

```sql
-- Import only specific tables
import foreign schema dynamodb
  limit to (users, orders)
  from server dynamodb_server into dynamodb;

-- Import all tables except specific ones
import foreign schema dynamodb
  except (staging_table)
  from server dynamodb_server into dynamodb;
```

### DynamoDB Tables

Each foreign table maps to one DynamoDB table.

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Table  |   ✅   |   ✅   |   ✅   |   ✅   |    ❌    |

#### Usage

```sql
create foreign table dynamodb.users (
  id text,
  name text,
  age bigint,
  active boolean,
  metadata jsonb
)
server dynamodb_server
options (
  table 'users',
  rowid_column 'id'
);
```

#### Notes

- Column names in the foreign table must match the DynamoDB attribute names exactly (case-sensitive).
- Any DynamoDB attribute not listed as a column is silently ignored during reads, **except** when a `jsonb` column has no matching DynamoDB attribute by name — in that case it acts as a catch-all and receives all attributes not covered by any other declared column. This is how the `_attrs` column generated by `import_foreign_schema` works.
- `INSERT` uses DynamoDB's `PutItem` operation, which **replaces the entire item** if an item with the same key already exists. Use `UPDATE` for partial attribute changes.
- `UPDATE` and `DELETE` require `rowid_column` to be set to the partition key column name.

## Query Pushdown Support

The DynamoDB Wrapper supports two scan strategies depending on the `WHERE` clause:

### Query (efficient — uses DynamoDB `Query` API)

When a `WHERE` clause includes an **equality filter on the partition key**, the wrapper uses the DynamoDB `Query` API. This reads only the items matching the partition key, consuming far fewer read capacity units than a full scan.

```sql
-- Uses Query API (efficient)
select * from dynamodb.users where id = 'user123';
```

Additional conditions on the sort key (if the table has one) are also pushed down as key condition expressions:

```sql
-- Uses Query API with sort key condition
select * from dynamodb.orders where customer_id = 'cust1' and order_date >= '2024-01-01';
```

### Scan (full table — uses DynamoDB `Scan` API)

When no partition key equality filter is present, the wrapper performs a full `Scan`. Non-key column filters are sent as a `FilterExpression`, which reduces the data transferred over the network but **does not reduce the read capacity units consumed** — DynamoDB reads every item before applying the filter.

```sql
-- Full Scan with FilterExpression (expensive on large tables)
select * from dynamodb.users where age > 30;

-- Full Scan, no filter
select * from dynamodb.users;
```

### Supported Operators

| Operator | Pushdown |
| -------- | :------: |
| `=`      | ✅       |
| `<`      | ✅       |
| `<=`     | ✅       |
| `>`      | ✅       |
| `>=`     | ✅       |
| `<>`     | ✅       |
| `LIKE`   | ❌       |
| `IN`     | ❌       |

!!! warning "Scan costs on large tables"
    Full table scans on large DynamoDB tables consume significant read capacity and can be slow. Always include a partition key equality filter when querying large tables.

## Supported Data Types

| Postgres Type  | DynamoDB Attribute Type                      |
| -------------- | -------------------------------------------- |
| `boolean`      | Boolean (BOOL)                               |
| `text`         | String (S), Number (N as string), or default |
| `smallint`     | Number (N)                                   |
| `integer`      | Number (N)                                   |
| `bigint`       | Number (N)                                   |
| `real`         | Number (N)                                   |
| `double precision` | Number (N)                               |
| `numeric`      | Number (N)                                   |
| `date`         | String (S, ISO 8601 format)                  |
| `timestamp`    | String (S, ISO 8601 format)                  |
| `timestamptz`  | String (S, ISO 8601 format)                  |
| `bytea`        | Binary (B)                                   |
| `jsonb`        | List (L), Map (M), String Set (SS), Number Set (NS), Binary Set (BS) |

DynamoDB stores numbers as strings internally. The wrapper coerces them to the declared Postgres column type at read time. If no numeric type is declared, numbers are returned as `text`.

Compound types (List, Map, String Set, Number Set, Binary Set) are always converted to `jsonb`.

## Limitations

This section describes important limitations and considerations when using this FDW:

- `TRUNCATE` is not supported. Use `DELETE` without a `WHERE` clause to remove all rows (this performs a full scan and individual `DeleteItem` calls, so it is slow on large tables).
- Full table scans (`Scan` API) do not reduce read capacity unit consumption even when a `FilterExpression` is used.
- `LIKE` and `IN` operators are not pushed down — filtering is done in Postgres after fetching rows.
- `UPDATE` cannot change partition key or sort key values, as DynamoDB does not support key updates in place.
- `INSERT` replaces the entire item if the key already exists (`PutItem` semantics). Use `UPDATE` to add or change individual attributes without replacing the whole item.
- Compound DynamoDB types (List, Map, sets) are read as `jsonb` and written as a JSON string (DynamoDB String attribute). To write native DynamoDB List or Map attributes, manage items directly through the AWS SDK.
- Materialized views using these foreign tables may fail during logical backups.
- DynamoDB transactions are not supported.

## Examples

### Basic setup

```sql
create extension if not exists wrappers with schema extensions;

create foreign data wrapper dynamodb_wrapper
  handler dynamo_db_fdw_handler
  validator dynamo_db_fdw_validator;

create server dynamodb_server
  foreign data wrapper dynamodb_wrapper
  options (
    aws_access_key_id '<aws_access_key>',
    aws_secret_access_key '<aws_secret_key>',
    region 'us-east-1'
  );

create schema if not exists dynamodb;
```

### Import all tables automatically

```sql
import foreign schema dynamodb
  from server dynamodb_server into dynamodb;
```

Each imported table has key columns plus `_attrs jsonb` for everything else:

```sql
-- Access non-key attributes via JSON operators
select
  id,
  _attrs->>'name'              as name,
  (_attrs->>'age')::integer    as age,
  (_attrs->>'active')::boolean as active,
  _attrs->'tags'               as tags   -- nested Map
from dynamodb.users;

-- Partition key pushdown still works on imported tables
select * from dynamodb.orders where order_id = 'ord-001';
```

### Create a foreign table manually

```sql
create foreign table dynamodb.products (
  product_id text,
  name text,
  price numeric,
  in_stock boolean,
  tags jsonb
)
server dynamodb_server
options (
  table 'products',
  rowid_column 'product_id'
);
```

### Query with partition key pushdown

```sql
-- Efficient: uses DynamoDB Query API
select * from dynamodb.products where product_id = 'prod-001';
```

### Query with filter (full scan)

```sql
-- Full scan with server-side FilterExpression
select * from dynamodb.products where price > 50;
```

### Insert a new item

```sql
insert into dynamodb.products (product_id, name, price, in_stock)
values ('prod-999', 'Widget', 9.99, true);
```

### Update specific attributes

```sql
-- Only updates 'price' and 'in_stock'; other attributes are preserved
update dynamodb.products
set price = 12.99, in_stock = false
where product_id = 'prod-999';
```

### Delete an item

```sql
delete from dynamodb.products where product_id = 'prod-999';
```

### Using DynamoDB Local for development

```sql
create server dynamodb_local_server
  foreign data wrapper dynamodb_wrapper
  options (
    aws_access_key_id 'test',
    aws_secret_access_key 'test',
    region 'us-east-1',
    endpoint_url 'http://localhost:8000'
  );
```
