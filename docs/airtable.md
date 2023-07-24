[Airtable](https://www.airtable.com) is an easy-to-use online platform for creating and sharing relational databases. 

The Airtable Wrapper is allows you to read data from your Airtable bases/tables from within your Postgres database.

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inide `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your Airtable credentials.

```sql
-- Create a secure key using pgsodium:
select pgsodium.create_key(name := 'airtable');

-- Save your Airtable API key in Vault and retrieve the `key_id`
insert into vault.secrets (secret, key_id)
values (
  'YOUR_SECRET',
  (select id from pgsodium.valid_key where name = 'airtable')
)
returning key_id;
```

### Connecting to Airtable

We need to provide Postgres with the credentials to connect to Airtable, and any additional options. We can do this using the `create foreign data wrapper` command:


=== "With Vault"

    ```sql
    create server airtable_server
    foreign data wrapper airtable_wrapper
    options (
      api_key_id '<key_ID>' -- The Key ID from above.
    );
    ```

=== "Without Vault"

    ```sql
    create server airtable_server
      foreign data wrapper airtable_wrapper
      options (
        api_url 'https://api.airtable.com/v0',  -- Airtable API url, optional
        api_key 'sk_test_xxx'  -- Airtable API key, required
      );
    ```

## Creating Foreign Tables

The Airtable Wrapper supports data reads from Airtable's [Records](https://airtable.com/developers/web/api/list-records) endpoint (*read only*).

| Airtable    | Select            | Insert            | Update            | Delete            | Truncate          |
| ----------- | :----:            | :----:            | :----:            | :----:            | :----:            |
| Records     | :white_check_mark:| :x:               | :x:               | :x:               | :x:               |

For example:

```sql
create foreign table my_foreign_table (
  name text,
  -- other fields
)
server airtable_server
options (
  base_id 'appXXXX',
  table_id 'tblXXXX'
);
```

#### Foreign Table Options

The full list of foreign table options are below:

- `base_id` - Airtable Base ID the table belongs to, required.
- `table_id` - Airtable table ID, required.

## Examples

Some examples on how to use Airtable foreign tables.

### Basic example

This will create a "foreign table" inside your Postgres database called `airtable_table`: 

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
```

You can now fetch your Airtable data from within your Postgres database:

```sql
select * from airtable_table;
```
