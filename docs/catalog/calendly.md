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

## Limitations

This section describes important limitations and considerations when using this FDW:

- **Performance Limitations**:
  - No query pushdown support, all filtering happens locally
  - Full result sets must be transferred from Calendly API
  - Network latency impacts query response times
  - API rate limits may affect query performance
  - WebAssembly execution overhead for data processing
  - Sequential scanning of all records for filtering

- **Feature Limitations**:
  - Read-only operations supported (no INSERT, UPDATE, DELETE, or TRUNCATE)
  - Limited data type mappings (see Supported Data Types section)
  - Complex Calendly event data must be handled through JSONB
  - No support for Calendly's webhook functionality
  - Authentication requires Calendly API token
  - WebAssembly runtime environment required

- **Resource Usage**:
  - API quota consumption scales with query frequency
  - Memory usage increases with result set size
  - Network bandwidth consumption based on response size
  - No connection pooling or result caching
  - WebAssembly memory limitations may affect large datasets
  - Each query requires fresh API authentication

- **Known Issues**:
  - Materialized views using foreign tables may fail during logical backups
  - JSON attribute extraction requires explicit path navigation
  - API version changes may affect data structure compatibility
  - Time zone handling may require explicit conversion
  - Error messages may not preserve full Calendly API context
  - Organization URI must be manually configured after initial setup

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

| Version | Wasm Package URL                                                                                    | Checksum                                                           |
| ------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_calendly_fdw_v0.1.0/calendly_fdw.wasm` | `51a19fa4b8c40afb5dcf6dc2e009189aceeba65f30eec75d56a951d78fc8893f` |

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
-- Save your Calendly API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'calendly',
  '<Calendly API key>' -- Calendly personal access token
)
returning key_id;
```

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
