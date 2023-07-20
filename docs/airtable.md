[Airtable](https://www.airtable.com) is an easy-to-use online platform for creating and sharing relational databases. `supabase/wrappers` exposes below endpoints.

Airtable FDW supports data read from below objects:

1. [Records](https://airtable.com/developers/web/api/list-records) (*read only*)

### Wrapper
To get started with the Airtable wrapper, create a foreign data wrapper specifying `handler` and `validator` as below.

```sql
create extension if not exists wrappers;

create foreign data wrapper airtable_wrapper
  handler airtable_fdw_handler
  validator airtable_fdw_validator;
```

### Server

Next, we need to create a server for the FDW to hold options and credentials.

#### Auth (Supabase)

If you are using the Supabase platform, this is the recommended approach for securing your Airtable API key.

Create a secure key using pgsodium
```sql
select pgsodium.create_key(name := 'airtable');
```

Save your Airtable API key in Vault and retrieve the `key_id`
```sql
insert into vault.secrets (secret, key_id)
values (
  'xxx',
  (select id from pgsodium.valid_key where name = 'airtable')
)
returnin
  key_id;
```

Create the foreign server
```sql
create server airtable_server
  foreign data wrapper airtable_wrapper
  options (
    api_key_id '<your key_id from above>'
  );
```

#### Auth (Insecure)

If the platform you are using does not support `pgsodium` and `Vault` you can create a server by storing your API key directly.


!!! important

    Credentials stored using this method can be viewed as plain text by anyone with access to `pg_catalog.pg_foreign_server`

```sql
create server airtable_server
   foreign data wrapper airtable_wrapper
   options (
     api_url 'https://api.airtable.com/v0',  -- Airtable API url, optional
     api_key 'sk_test_xxx'  -- Airtable API key, required
   );
```

### Tables

The Airtable foreign data wrapper supports data read from [Records](https://airtable.com/developers/web/api/list-records) endpoint.

#### Foreign Table Options

The full list of foreign table options are below:

- `base_id` - Airtable base id the table belongs to, required.
- `table_id` - Airtable table id, required.

### Examples

Some examples on how to use Airtable foreign tables.

```sql
create foreign table airtable_table (
  name text,
  notes text,
  content text,
  amount numeric,
  updated_at timestamp
)
  server airtable_server
  options (
    base_id 'appTc3yI68KN6ukZc',
    table_id 'tbltiLinE56l3YKfn'
  );

select * from airtable_table;
```
