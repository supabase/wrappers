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
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_cal_fdw_v0.1.0/cal_fdw.wasm` | `tbd` |

## Preparation

Before you can query Cal.com, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Cal.com Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

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

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists cal;
```

## Entities

### My Profile

This is an object representing your Cal.com user profile.

Ref: [Cal.com API docs](https://cal.com/docs/api-reference/v2/introduction)

#### Operations

| Object     | Select | Insert | Update | Delete | Truncate |
| ---------- | :----: | :----: | :----: | :----: | :------: |
| My Profile |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

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
```

#### Notes

- The `attrs` column contains all profile attributes in JSON format
- Query using standard SQL: `select * from cal.my_profile`

### Event Types

This is an object representing Cal.com event types.

Ref: [Cal.com API docs](https://cal.com/docs/api-reference/v2/introduction)

#### Operations

| Object      | Select | Insert | Update | Delete | Truncate |
| ----------- | :----: | :----: | :----: | :----: | :------: |
| Event Types |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table cal.event_types (
  attrs jsonb
)
  server cal_server
  options (
    object 'event-types'
  );
```

#### Notes

- The `attrs` column contains all event type attributes in JSON format
- Extract specific fields using JSON operators
- Example:
  ```sql
  select
    etg->'profile'->>'name' as profile,
    et->>'id' as id,
    et->>'title' as title
  from cal.event_types t
    cross join json_array_elements((attrs->'eventTypeGroups')::json) etg
    cross join json_array_elements((etg->'eventTypes')::json) et;
  ```

### Bookings

This is an object representing Cal.com bookings.

Ref: [Cal.com API docs](https://cal.com/docs/api-reference/v2/bookings/create-a-booking)

#### Operations

| Object   | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| Bookings |   ✅   |   ✅   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table cal.bookings (
  attrs jsonb
)
  server cal_server
  options (
    object 'bookings',
    rowid_column 'attrs'
  );
```

#### Notes

- Supports both reading and creating bookings
- The `attrs` column contains all booking attributes in JSON format
- Example of creating a booking:
  ```sql
  insert into cal.bookings(attrs)
  values (
    '{
      "start": "2024-12-12T10:30:00.000Z",
      "eventTypeId": 123456,
      "attendee": {
        "name": "Test Name",
        "email": "test.name@example.com",
        "timeZone": "America/New_York"
      }
    }'::jsonb
  );
  ```
- Additional fields like `guests` or `metadata` can be added to the booking JSON
- For more details on booking options, refer to [Cal.com documentation](https://cal.com/docs/api-reference/v2/bookings/create-a-booking)

### Calendars

This is an object representing Cal.com calendars.

Ref: [Cal.com API docs](https://cal.com/docs/api-reference/v2/introduction)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| Calendars |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table cal.calendars (
  attrs jsonb
)
  server cal_server
  options (
    object 'calendars'
  );
```

#### Notes

- The `attrs` column contains all calendar attributes in JSON format

### Schedules

This is an object representing Cal.com schedules.

Ref: [Cal.com API docs](https://cal.com/docs/api-reference/v2/introduction)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| Schedules |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table cal.schedules (
  id bigint,
  name text,
  attrs jsonb
)
  server cal_server
  options (
    object 'schedules'
  );
```

#### Notes

- The `attrs` column contains additional schedule attributes in JSON format

### Conferencing

This is an object representing Cal.com conferencing settings.

Ref: [Cal.com API docs](https://cal.com/docs/api-reference/v2/introduction)

#### Operations

| Object       | Select | Insert | Update | Delete | Truncate |
| ------------ | :----: | :----: | :----: | :----: | :------: |
| Conferencing |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table cal.conferencing (
  id bigint,
  attrs jsonb
)
  server cal_server
  options (
    object 'conferencing'
  );
```

#### Notes

- The `attrs` column contains all conferencing attributes in JSON format

## Foreign Table Options

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
    object 'bookings',
    rowid_column 'attrs'
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

### Make a booking

Once we know an event type ID (we can get it from above example, here we suppose it is `123456`), we can make a booking using below SQL.

```
insert into cal.bookings(attrs)
values (
	'{
		"start": "2024-12-12T10:30:00.000Z",
		"eventTypeId": 123456,
		"attendee": {
			"name": "Test Name",
			"email": "test.name@example.com",
			"timeZone": "America/New_York"
		}
	}'::jsonb
);
```

To add more details to the booking, such as `guests` or `metadata`, refer to [Cal.com documentation](https://cal.com/docs/api-reference/v2/bookings/create-a-booking).
