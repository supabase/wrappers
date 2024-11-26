---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Calendly

[Calendly](https://calendly.com/) is a scheduling platform used for teams to schedule, prepare and follow up on external meetings.

The Calendly Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read data from your Calendly for use within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Supported Data Types

| Postgres Data Type | Calendly Data Type |
| ------------------ | ------------------ |
| boolean            | Boolean            |
| bigint             | Number             |
| double precision   | Number             |
| text               | String             |
| timestamp          | Time               |
| timestamptz        | Time               |
| jsonb              | Json               |

The Calendly API uses JSON formatted data, please refer to [Calendly API docs](https://developer.calendly.com/api-docs) for more details.

## Available Versions

| Version | Wasm Package URL                                                                                | Checksum                                                           |
| ------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_calendly_fdw_v0.1.0/calendly_fdw.wasm` | `aa17f1ce2b48b5d8d6cee4f61df4d6b23e9a333c3e5c7a10cec9aae619c156b9` |

## Preparation

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

and then create the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Calendly API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'calendly',
  '<Calendly API Key>' -- Calendly personal access token
)
returning key_id;
```

### Connecting to Calendly

We need to provide Postgres with the credentials to access Calendly and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server calendly_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_calendly_fdw_v0.1.0/calendly_fdw.wasm',
        fdw_package_name 'supabase:calendly-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'aa17f1ce2b48b5d8d6cee4f61df4d6b23e9a333c3e5c7a10cec9aae619c156b9',
        -- find your organization uri using foreign table 'calendly.current_user', see below example for details
        organization 'https://api.calendly.com/organizations/81da9c7f-3e19-434a-c3d2-0325e375cdef',
        api_url 'https://api.calendly.com',  -- optional
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server calendly_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_calendly_fdw_v0.1.0/calendly_fdw.wasm',
        fdw_package_name 'supabase:calendly-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum 'aa17f1ce2b48b5d8d6cee4f61df4d6b23e9a333c3e5c7a10cec9aae619c156b9',
        -- find your organization uri using foreign table 'calendly.current_user', see below example for details
        organization 'https://api.calendly.com/organizations/81da9c7f-3e19-434a-c3d2-0325e375cdef',
        api_url 'https://api.calendly.com',  -- optional
        api_key 'eyJraWQiOiIxY2UxZ...'  -- Calendly personal access token
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists calendly;
```

## Creating Foreign Tables

The Calendly Wrapper supports data reads from below objects in calendly.

| Integration             | Select | Insert | Update | Delete | Truncate |
| ----------------------- | :----: | :----: | :----: | :----: | :------: |
| Current User            |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Event Types             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Groups                  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Organization Membership |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Scheduled Events        |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
-- Get the current user used for the API request
-- Note: we can query this table to retrieve the organization uri:
--   select attrs->>'current_organization' as org_uri
--   from calendly.current_user;
create foreign table calendly.current_user (
  uri text,
  slug text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server calendly_server
  options (
    object 'current_user'
  );

create foreign table calendly.event_types (
  uri text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server calendly_server
  options (
    object 'event_types'
  );
  
create foreign table calendly.groups (
  uri text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server calendly_server
  options (
    object 'groups'
  );

create foreign table calendly.organization_memberships (
  uri text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server calendly_server
  options (
    object 'organization_memberships'
  );

create foreign table calendly.scheduled_events (
  uri text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server calendly_server
  options (
    object 'scheduled_events'
  );
```

!!! note

    - All the supported columns are listed above, other columns are not allowd.
    - The `attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Foreign table options

The full list of foreign table options are below:

- `object` - Object name in Calendly, required.

  Supported objects are listed below:

  | Object name              |
  | ------------------------ |
  | current_user             |
  | event_types              |
  | groups                   |
  | organization_memberships |
  | scheduled_events         |

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

Below are some examples on how to use Calendly foreign tables.

### Basic example

This example will create a "foreign table" inside your Postgres database and query its data. First, we can create a schema to hold all the Calendly foreign tables.

```sql
create schema if not exists calendly;
```

Then create the foreign table and query it, for example:

```sql
create foreign table calendly.current_user (
  uri text,
  slug text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server calendly_server
  options (
    object 'current_user'
  );

-- query current user used for the Calendly API request
select * from calendly.current_user;
```

`attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Query JSON attributes

```sql
-- extract organization uri from current user
select attrs->>'current_organization' as org_uri
from calendly.current_user;

-- then update foreign server option using the organization uri
alter server calendly_server options (set organization '<org_uri>');
```

Some other examples,

```sql
create foreign table calendly.event_types (
  uri text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server calendly_server
  options (
    object 'event_types'
  );
  
select attrs->'profile'->>'name' as profile_name
from calendly.event_types;

select attrs->'custom_questions'->0->>'name' as first_question_name
from calendly.event_types;
```
