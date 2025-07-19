---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Gravatar

[Gravatar](https://gravatar.com/) is a service for providing globally unique avatars.

The Gravatar Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read profile data from Gravatar for use within your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                                    | Checksum                                                           | Required Wrappers Version |
| ------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------- |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_gravatar_fdw_v0.1.0/gravatar_fdw.wasm` | `tbd` | >=0.5.0                   |

## Preparation

Before you can query Gravatar, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Gravatar Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Gravatar API key in Vault and retrieve the created `key_id`
select vault.create_secret(
  '<Gravatar API key>', -- Gravatar API key
  'Gravatar',
  'Gravatar API key for Wrappers'
);
```

### Connecting to Gravatar

We need to provide Postgres with the credentials to access Gravatar and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server gravatar_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_gravatar_fdw_v0.1.0/gravatar_fdw.wasm',
        fdw_package_name 'supabase:gravatar-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
        api_url 'https://api.gravatar.com/v3',  -- optional
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server gravatar_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_gravatar_fdw_v0.1.0/gravatar_fdw.wasm',
        fdw_package_name 'supabase:gravatar-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'tbd',
        api_url 'https://api.gravatar.com/v3',  -- optional
        api_key '<Gravatar API key>'  -- Gravatar API key
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists gravatar;
```

## Options

The full list of foreign table options are below:

- `table` - Component name in Gravatar, required.

Supported components are listed below:

| Component name           |
| ------------------------ |
| profiles                 |

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from Gravatar.

For example, using below SQL can automatically create foreign tables in the `gravatar` schema.

```sql
-- create all the foreign tables
import foreign schema gravatar from server gravatar_server into gravatar;
```

### Profiles

This is the user profile provided by Gravatar.

Ref: [Gravatar API docs](https://docs.gravatar.com/sdk/profiles/)

#### Operations

| Component             | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| profiles              |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table gravatar.profiles (
  hash text,
  email text,
  display_name text,
  profile_url text,
  avatar_url text,
  avatar_alt_text text,
  location text,
  description text,
  job_title text,
  company text,
  verified_accounts jsonb,
  pronunciation text,
  pronouns text,
  timezone text,
  language jsonb,
  first_name text,
  last_name text,
  is_organization boolean,
  links jsonb,
  interests jsonb,
  payments jsonb,
  contact_info jsonb,
  number_verified_accounts bigint,
  last_profile_edit text,
  registration_date text,
  attrs jsonb
)
  server gravatar_server
  options (
    table 'profiles'
  );
```

!!! note

    You can use `import foreign schema` statement to automatically create the `profiles` foreign table as [described above](#entities)

#### Notes

- The `attrs` column contains additional attributes in JSON format

## Query Pushdown Support

This FDW only supports `email equal condition (email = )` query pushdown and it is mandatory. For example,

```sql
select * from gravatar.profiles where email = 'email@example.com';

-- emapty result without the email equal condition
select * from gravatar.profiles
```

## Supported Data Types

| Postgres Data Type | Gravatar Data Type |
| ------------------ | ------------------ |
| boolean            | Boolean            |
| bigint             | Number             |
| text               | String             |
| timestamp          | Time               |
| jsonb              | Json               |

The Gravatar API uses JSON formatted data, please refer to [Gravatar API docs](https://docs.gravatar.com/sdk/profiles/) for more details.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Only supports `profiles` component
- Only `email equal condition (email = )` query pushdown is supported
- Materialized views using these foreign tables may fail during logical backups

## Examples

Below are some examples on how to use gravatar foreign tables.

### Basic example

This example will create a "foreign table" inside your Postgres database and query its data.

```sql
import foreign schema gravatar from server gravatar_server into gravatar;

-- query an user profile 
select * from gravatar.profiles where email = 'email@example.com';
```

`attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Query JSON attributes

```sql
select p.attrs->'section_visibility' as section_visibility
from gravatar.profiles p
where email = 'email@example.com';
```
