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

- `endpoint` - Logflare endpoint, required.

#### Examples

```sql
create foreign table people (
  id bigint,
  name text,
  ts timestamp
)
  server logflare_server
  options (
    endpoint '9dd9a6f6-8e9b-4fa4-b682-4f2f5cd99da3'
  );

select * from people;
```

