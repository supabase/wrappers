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

## Available Versions

| Version | Wasm Package URL                                                                                    | Checksum                                                           | Required Wrappers Version |
| ------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------- |
| 0.2.0   | `https://github.com/supabase/wrappers/releases/download/wasm_calendly_fdw_v0.2.0/calendly_fdw.wasm` | `tbd`                                                              | >=0.5.0                   |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_calendly_fdw_v0.1.0/calendly_fdw.wasm` | `51a19fa4b8c40afb5dcf6dc2e009189aceeba65f30eec75d56a951d78fc8893f` | >=0.4.0                   |

## Preparation

Before you can query Calendly, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Calendly Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Calendly API key in Vault and retrieve the created `key_id`
select vault.create_secret(
  '<Calendly API key>', -- Calendly personal access token
  'calendly',
  'Calendly API key for Wrappers'
);
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
        fdw_package_checksum '51a19fa4b8c40afb5dcf6dc2e009189aceeba65f30eec75d56a951d78fc8893f',
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
        fdw_package_checksum '51a19fa4b8c40afb5dcf6dc2e009189aceeba65f30eec75d56a951d78fc8893f',
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

## Options

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

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from Calendly.

For example, using below SQL can automatically create foreign tables in the `calendly` schema.

```sql
-- create all the foreign tables
import foreign schema calendly from server calendly_server into calendly;

-- or, create selected tables only
import foreign schema calendly
   limit to ("event_types", "groups")
   from server calendly_server into calendly;

-- or, create all foreign tables except selected tables
import foreign schema calendly
   except ("event_types")
   from server calendly_server into calendly;
```

### Current User

This is an object representing your Calendly user profile.

Ref: [Calendly API docs](https://developer.calendly.com/api-docs)

#### Operations

| Object       | Select | Insert | Update | Delete | Truncate |
| ------------ | :----: | :----: | :----: | :----: | :------: |
| Current User |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

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
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format
- Use this table to retrieve the organization URI for server configuration, for example:
  ```sql
  select attrs->>'current_organization' as org_uri
  from calendly.current_user;
  ```

### Event Types

This is an object representing Calendly event types.

Ref: [Calendly API docs](https://developer.calendly.com/api-docs)

#### Operations

| Object      | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| Event Types |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

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
```

#### Notes

- The `attrs` column contains all event type attributes in JSON format
- Access profile and custom questions through JSON attributes:
  ```sql
  select attrs->'profile'->>'name' as profile_name,
         attrs->'custom_questions'->0->>'name' as first_question_name
  from calendly.event_types;
  ```

### Groups

This is an object representing Calendly groups.

Ref: [Calendly API docs](https://developer.calendly.com/api-docs)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Groups |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
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
```

#### Notes

- The `attrs` column contains all group attributes in JSON format

### Organization Memberships

This is an object representing Calendly organization memberships.

Ref: [Calendly API docs](https://developer.calendly.com/api-docs)

#### Operations

| Object                  | Select | Insert | Update | Delete | Truncate |
| ----------------------- | :----: | :----: | :----: | :----: | :------: |
| Organization Membership |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
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
```

#### Notes

- The `attrs` column contains all membership attributes in JSON format

### Scheduled Events

This is an object representing Calendly scheduled events.

Ref: [Calendly API docs](https://developer.calendly.com/api-docs)

#### Operations

| Object           | Select | Insert | Update | Delete | Truncate |
| ---------------- | :----: | :----: | :----: | :----: | :------: |
| Scheduled Events |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
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

#### Notes

- The `attrs` column contains all event attributes in JSON format

## Query Pushdown Support

This FDW doesn't support query pushdown.

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

## Limitations

This section describes important limitations and considerations when using this FDW:

- Large result sets may experience slower performance due to full data transfer requirement
- Organization URI must be manually configured after initial setup
- Materialized views using these foreign tables may fail during logical backups

## Examples

Below are some examples on how to use Calendly foreign tables.

### Basic example

This example will create a "foreign table" inside your Postgres database and query its data.

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
