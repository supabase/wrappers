[Logflare](https://logflare.app) is a centralized web-based log management solution to easily access Cloudflare, Vercel & Elixir logs.

The Logflare Wrapper allows you to read data from Logflare endpoints within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper logflare_wrapper
  handler logflare_fdw_handler
  validator logflare_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inide `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Logflare API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'logflare',
  'YOUR_SECRET'
)
returning key_id;
```

### Connecting to Logflare

We need to provide Postgres with the credentials to connect to Logflare, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server logflare_server
      foreign data wrapper logflare_wrapper
      options (
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server logflare_server
      foreign data wrapper logflare_wrapper
      options (
        api_key '<Logflare API Key>' -- Logflare API key, required
      );
    ```

## Creating Foreign Tables

The Logflare Wrapper supports data reads from Logflare's endpoints.

| Integration | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| Logflare    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
create foreign table my_logflare_table (
  id bigint,
  name text,
  _result text
)
  server logflare_server
  options (
    endpoint '9dd9a6f6-8e9b-4fa4-b682-4f2f5cd99da3'
  );
```

### Meta column

You can define a specific meta column `_result` (data type: `text`) in the foreign table. It will store the whole result record in JSON string format, so you can extract any fields from it using Postgres JSON queries like `_result::json->>'foo'`. See more examples below.

### Query parameters

Logflare endpoint query parameters can be passed using specific parameter columns like `_param_foo` and `_param_bar`. See more examples below.

### Foreign table options

The full list of foreign table options are below:

- `endpoint` - Logflare endpoint UUID or name, required.

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

Some examples on how to use Logflare foreign tables.

### Basic example

Assume the Logflare endpoint response is like below:

```json
[
  {
    "id": 123,
    "name": "foo"
  }
]
```

Then we can define a foreign table like this:

```sql
create foreign table people (
  id bigint,
  name text,
  _result text
)
  server logflare_server
  options (
    endpoint '9dd9a6f6-8e9b-4fa4-b682-4f2f5cd99da3'
  );

select * from people;
```

### Query parameters example

Suppose the Logflare endpoint accepts 3 parameters:

1. org_id
2. iso_timestamp_start
3. iso_timestamp_end

And its response is like below:

```json
[
  {
    "db_size": "large",
    "org_id": "123",
    "runtime_hours": 21.95,
    "runtime_minutes": 1317
  }
]
```

We can define a foreign table and parameter columns like this:

```sql
create foreign table runtime_hours (
  db_size text,
  org_id text,
  runtime_hours numeric,
  runtime_minutes bigint,
  _param_org_id bigint,
  _param_iso_timestamp_start text,
  _param_iso_timestamp_end text,
  _result text
)
  server logflare_server
  options (
    endpoint 'my.custom.endpoint'
  );
```

and query it with parameters like this:

```sql
select
  db_size,
  org_id,
  runtime_hours,
  runtime_minutes
from
  runtime_hours
where _param_org_id = 123
  and _param_iso_timestamp_start = '2023-07-01 02:03:04'
  and _param_iso_timestamp_end = '2023-07-02';
```
