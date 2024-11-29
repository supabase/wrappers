---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Cal.com

[Cal.com](https://cal.com/) is an open source scheduling platform.

The Cal Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read data from your Cal.com account for use within your Postgres database.

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Supported Data Types

| Postgres Data Type | Cal.com Data Type  |
| ------------------ | ------------------ |
| boolean            | Boolean            |
| bigint             | Number             |
| double precision   | Number             |
| text               | String             |
| jsonb              | Json               |

The Cal.com API uses JSON formatted data, please refer to [Cal.com API docs](https://cal.com/docs/api-reference/v2/introduction) for more details.

!!! note

    This foreign data wrapper only supports Cal.com API v2.

## Available Versions

| Version | Wasm Package URL                                                                                | Checksum                                                           |
| ------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_cal_fdw_v0.1.0/cal_fdw.wasm` | `4b8661caae0e4f7b5a1480ea297cf5681101320712cde914104b82f2b0954003` |

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
-- Save your Cal.com API key in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'cal',
  '<Cal.com API Key>' -- Cal.com API key
)
returning key_id;
```

### Connecting to Cal.com

We need to provide Postgres with the credentials to access Cal.com and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server cal_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_cal_fdw_v0.1.0/cal_fdw.wasm',
        fdw_package_name 'supabase:cal-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '4b8661caae0e4f7b5a1480ea297cf5681101320712cde914104b82f2b0954003',
        api_url 'https://api.cal.com/v2',  -- optional
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server cal_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_cal_fdw_v0.1.0/cal_fdw.wasm',
        fdw_package_name 'supabase:cal-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '4b8661caae0e4f7b5a1480ea297cf5681101320712cde914104b82f2b0954003',
        api_url 'https://api.cal.com/v2',  -- optional
        api_key 'cal_live_1234...'  -- Cal.com API key
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists cal;
```

## Creating Foreign Tables

The Cal.com Wrapper supports data reads from below objects in Cal.com.

| Integration  | Select | Insert | Update | Delete | Truncate |
| -------------| :----: | :----: | :----: | :----: | :------: |
| My Profile   |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Event Types  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Bookings     |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Calendars    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Schedules    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |
| Conferencing |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

For example:

```sql
create foreign table cal.my_profile (
  id bigint,
  username text,
  email text,
  attrs jsonb
)
  server cal_server
  options (
    object 'my_profile'
  );

create foreign table cal.event_types (
  attrs jsonb
)
  server cal_server
  options (
    object 'event-types'
  );
  
create foreign table cal.bookings (
  attrs jsonb
)
  server cal_server
  options (
    object 'bookings'
  );

create foreign table cal.calendars (
  attrs jsonb
)
  server cal_server
  options (
    object 'calendars'
  );

create foreign table cal.schedules (
  id bigint,
  name text,
  attrs jsonb
)
  server cal_server
  options (
    object 'schedules'
  );

create foreign table cal.conferencing (
  id bigint,
  attrs jsonb
)
  server cal_server
  options (
    object 'conferencing'
  );
```

!!! note

    - All the supported columns are listed above, other columns are not allowd.
    - The `attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Foreign table options

The full list of foreign table options are below:

- `object` - Object name in Cal.com, required.

  Supported objects are listed below:

  | Object name              |
  | ------------------------ |
  | my_profile               |
  | event-types              |
  | bookings                 |
  | calendars                |
  | schedules                |
  | conferencing             |

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

Below are some examples on how to use Cal.com foreign tables.

### Basic example

This example will create a "foreign table" inside your Postgres database and query its data. First, we can create a schema to hold all the Cal.com foreign tables.

```sql
create schema if not exists cal;
```

Then create the foreign table and query it, for example:

```sql
create foreign table cal.my_profile (
  id bigint,
  username text,
  email text,
  attrs jsonb
)
  server cal_server
  options (
    object 'my_profile'
  );

-- query current user used for the Cal.com API request
select * from cal.my_profile;
```

`attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Query JSON attributes

```sql
create foreign table cal.bookings (
  attrs jsonb
)
  server cal_server
  options (
    object 'bookings'
  );

create foreign table cal.event_types (
  attrs jsonb
)
  server cal_server
  options (
    object 'event-types'
  );
  
-- extract bookings
select
  bk->>'id' as id,
  bk->>'title' as title,
  bk->>'userPrimaryEmail' as email
from cal.bookings t
  cross join json_array_elements((attrs->'bookings')::json) bk;

-- extract event types
select
  etg->'profile'->>'name' as profile,
  et->>'id' as id,
  et->>'title' as title
from cal.event_types t
  cross join json_array_elements((attrs->'eventTypeGroups')::json) etg
  cross join json_array_elements((etg->'eventTypes')::json) et;
```
