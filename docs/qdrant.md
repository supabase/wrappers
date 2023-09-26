Qdrant foreign data wrapper allows you to read data from the [Qdrant vector database](https://qdrant.tech/).

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper qdrant_wrapper
  handler qdrant_fdw_handler
  validator qdrant_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inide `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Qdrant API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'qdrant_api_key',
  'YOUR_SECRET'
)
returning key_id;
```

### Connecting to Qdrant

We need to provide Postgres with the credentials to connect to Qdrant, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server qdrant_server
      foreign data wrapper qdrant_wrapper
      options (
        cluster_url '<Qdrant cluster URL>',  -- Qdrant cluster url, required
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server qdrant_server
      foreign data wrapper qdrant_wrapper
      options (
        cluster_url '<Qdrant cluster URL>',  -- Qdrant cluster url, required
        api_key '<Qdrant API Key>'  -- Qdrant API key, required
      );
    ```

!!! note
    The `cluster_url` URL must contain the port. The default port is 6333.

## Creating Foreign Tables

The Qdrant Wrapper supports data reads from Qdrant's [Scroll Points](https://qdrant.github.io/qdrant/redoc/index.html#tag/points/operation/scroll_points) endpoint (*read only*).

| Qdrant    | Select            | Insert            | Update            | Delete            | Truncate          |
| ----------- | :----:            | :----:            | :----:            | :----:            | :----:            |
| Records     | :white_check_mark:| :x:               | :x:               | :x:               | :x:               |

For example:

```sql
create foreign table my_foreign_table (
  id bigint,
  payload jsonb,
  vector real[]
)
server qdrant_server
options (
    collection_name '<collection name>'
);
```

!!! note
    The names and types of the columns in the foreign table should match exactly as shown in the example above.

### Foreign table options

The full list of foreign table options are below:

- `collection_name` - Qdrant collection to fetch data from, required.

## Examples

Some examples on how to use Qdrant foreign tables.

### Basic example

This will create a "foreign table" inside your Postgres database called `qdrant_table`: 

```sql
create foreign table qdrant_table (
  id bigint,
  payload jsonb,
  vector real[]
)
server qdrant_server
options (
  collection_name 'test_collection',
);
```

You can now fetch your Qdrant data from within your Postgres database:

```sql
select * from qdrant_table;
```
