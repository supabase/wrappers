[Logflare](https://logflare.app) is a centralized web-based log management solution to easily access Cloudflare, Vercel & Elixir logs.

### Wrapper 
To get started with the Logflare wrapper, create a foreign data wrapper specifying `handler` and `validator` as below.

```sql
create extension if not exists wrappers;

create foreign data wrapper logflare_wrapper
  handler logflare_fdw_handler
  validator logflare_fdw_validator;
```

### Server 

Next, we need to create a server for the FDW to hold options and credentials.

#### Auth (Supabase)

If you are using the Supabase platform, this is the recommended approach for securing your [Logflare access token](https://docs.logflare.app/concepts/access-tokens/).

Create a secure key using pgsodium
```sql
select pgsodium.create_key(name := 'logflare');
```

Save your Logflare access token in Vault and retrieve the `key_id`
```sql
insert into vault.secrets (secret, key_id)
values (
  'xxx',
  (select id from pgsodium.valid_key where name = 'logflare')
)
returning
  key_id;
```

Create the foreign server
```sql
create server logflare_server
  foreign data wrapper logflare_wrapper
  options (
    api_key_id '<your key_id from above>'
  );
```

#### Auth (Insecure)

If the platform you are using does not support `pgsodium` and `Vault` you can create a server by storing your [Logflare Access Token](https://docs.logflare.app/concepts/access-tokens/) directly.


!!! important

    Credentials stored using this method can be viewed as plain text by anyone with access to `pg_catalog.pg_foreign_server`

```sql
create server logflare_server
   foreign data wrapper logflare_wrapper
   options (
     api_key 'xxx'
   );
```


### Tables

Logflare wrapper is implemented with [ELT](https://hevodata.com/learn/etl-vs-elt/) approach, so the data transformation is encouraged to be performed locally after data is extracted from remote data source.


#### Foreign Table Options

The full list of foreign table options are below:

- `endpoint` - Logflare endpoint UUID or name, required.

#### Meta Column

You can define a specific meta column `_result` (data type: `text`) in the foreign table. It will store the whole result record in JSON string format, so you can extract any fields from it using Postgres JSON queries like `_result::json->>'foo'`. See more examples below.

#### Query Parameters

Logflare endpoint query parameters can be passed using specific parameter columns like `_param_foo` and `_param_bar`. See more examples below.

### Examples

Some examples on how to use Logflare foreign tables.

#### Simple Query

Assume the Logflare endpoint response is like below:

```json
[
  {
    "id": 123,
    "name": "foo"
  }
]
```

Then we can define foreign table like this:

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

#### Query with parameters

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

We can define foreign table and parameter columns like this:

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

