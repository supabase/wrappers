---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# HubSpot

[HubSpot](https://www.hubspot.com/) is a developer and marketer of software products for inbound marketing, sales, and customer service.

The HubSpot Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read data from your HubSpot for use within your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                                  | Checksum                                                           |
| ------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_hubspot_fdw_v0.1.0/hubspot_fdw.wasm` | `2cbf39e9e28aa732a225db09b2186a2342c44697d4fa047652d358e292ba5521` |

## Preparation

Before you can query HubSpot, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the HubSpot Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your HubSpot private apps access token in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'hubspot',
  '<HubSpot access token>' -- HubSpot private apps access token
)
returning key_id;
```

### Connecting to HubSpot

We need to provide Postgres with the credentials to access HubSpot and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server hubspot_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_hubspot_fdw_v0.1.0/hubspot_fdw.wasm',
        fdw_package_name 'supabase:hubspot-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '2cbf39e9e28aa732a225db09b2186a2342c44697d4fa047652d358e292ba5521',
        api_url 'https://api.hubapi.com/crm/v3',  -- optional
        api_key_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server hubspot_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_hubspot_fdw_v0.1.0/hubspot_fdw.wasm',
        fdw_package_name 'supabase:hubspot-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '2cbf39e9e28aa732a225db09b2186a2342c44697d4fa047652d358e292ba5521',
        api_url 'https://api.hubapi.com/crm/v3',  -- optional
        api_key 'pat-xxx-xxxxxxx-...'  -- HubSpot private apps access token
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists hubspot;
```

## Options

The full list of foreign table options are below:

- `object` - Object name in HubSpot, required.

Supported objects are listed below:

| Object name                   |
| ----------------------------- |
| objects/companies             |
| objects/contacts              |
| objects/deals                 |
| objects/feedback_submissions  |
| objects/goal_targets          | 
| objects/leads                 |
| objects/line_items            |
| objects/<objectType\>         |
| objects/partner_clients       |
| objects/products              |
| objects/tickets               |

!!! note

    The `objectType` in `objects/<objectType>` must be substituted with an object type ID, e.g. a custom object `2-3508482`.

## Entities

Below are all the entities supported by this FDW. Each entity must have `id`, `created_at`, `updated_at` and `attrs` columns, the other columns can be any properties defined on the corresponding HubSpot object. For example, if there is a custom property `user_id` defined on the `Contacts` object, the table DDL can be:

```sql
create foreign table hubspot.contacts (
  id text,
  user_id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/contacts'
  );
```

The column `user_id` is the custom property internal name, which can be found in the `Details` section of `Edit property` page on HubSpot `Settings` -> `Data Management` -> `Properties`.

### Companies

This is object represents the companies and organizations that interact with your business.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/companies)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| companies |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.companies (
  id text,
  name text,
  domain text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/companies'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Contacts

This is object represents the contacts.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/contacts)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| contacts  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.contacts (
  id text,
  email text,
  firstname text,
  lastname text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/contacts'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Deals

This is object represents the transactions with contacts and/or companies.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/deals)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| deals     |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.deals (
  id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/deals'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Feedback submissions

This is object represents the information submitted to your NPS, CSAT, CES, and custom feedback surveys.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/feedback-submissions)

#### Operations

| Object               | Select | Insert | Update | Delete | Truncate |
| -------------------- | :----: | :----: | :----: | :----: | :------: |
| feedback_submissions |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.feedback_submissions (
  id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/feedback_submissions'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Goals

This is object represents the user-specific quotas for their sales and services teams based on templates provided by HubSpot.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/goals)

#### Operations

| Object       | Select | Insert | Update | Delete | Truncate |
| ------------ | :----: | :----: | :----: | :----: | :------: |
| goal_targets |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.goals (
  id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/goal_targets'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Leads

This is object represents the contacts or companies that are potential customers who have shown interest in your products or services.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/leads)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| leads     |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.leads (
  id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/leads'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Deals

This is object represents the instances of products to deals and quotes.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/line-items)

#### Operations

| Object     | Select | Insert | Update | Delete | Truncate |
| ---------- | :----: | :----: | :----: | :----: | :------: |
| line_items |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.line_items (
  id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/line_items'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Objects

This is object represents the objects included in all accounts, such as contacts and companies, as well as for custom objects.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/objects)

#### Operations

| Object       | Select | Insert | Update | Delete | Truncate |
| ------------ | :----: | :----: | :----: | :----: | :------: |
| <objectType> |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.objects (
  id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/<objectType>'  -- objectType can be a custom objec type id, e.g. `2-3508482`
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Partner clients

This is object represents the customers that Solutions Partners have a sold or managed relationship with.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/partner-clients)

#### Operations

| Object          | Select | Insert | Update | Delete | Truncate |
| --------------- | :----: | :----: | :----: | :----: | :------: |
| partner_clients |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.partner_clients (
  id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/partner_clients'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Products

This is object represents the collection of goods and services that your company offers.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/products)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| products  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.products (
  id text,
  name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/products'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

### Tickets

This is object represents the customer service requests in your CRM.

Ref: [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/crm/objects/tickets)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| tickets   |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table hubspot.tickets (
  id text,
  subject text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/tickets'
  );
```

#### Notes

- The `attrs` column contains additional user attributes in JSON format

## Query Pushdown Support

### `where` clause pushdown

This FDW supports `where id = 'xxx'` clause pushdown for all entities. For example,

```sql
select * from hubspot.contacts where id = '1504';
```

### `limit` clause pushdown

This FDW supports `limit` clause pushdown for all the entities. For example,

```sql
select * from hubspot.contacts limit 200;
```

## Supported Data Types

| Postgres Data Type | HubSpot Data Type |
| ------------------ | ------------------ |
| boolean            | Boolean            |
| bigint             | Number             |
| double precision   | Number             |
| numeric            | Number             |
| text               | String             |
| timestamp          | Time               |
| timestamptz        | Time               |
| jsonb              | Json               |

The HubSpot API uses JSON formatted data, please refer to [HubSpot API docs](https://developers.hubspot.com/docs/reference/api/overview) for more details.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Large result sets may experience slower performance due to full data transfer requirement
- Query pushdown support limited to 'id' columns only
- Materialized views using these foreign tables may fail during logical backups

## Examples

Below are some examples on how to use HubSpot foreign tables.

### Basic example

This example will create a "foreign table" inside your Postgres database and query its data.

```sql
create foreign table hubspot.contacts (
  id text,
  email text,
  firstname text,
  lastname text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/contacts'
  );

-- query all contacts
select * from hubspot.contacts;

-- query one contact
select * from hubspot.contacts
where id = '1501';
```

`attrs` is a special column which stores all the object attributes in JSON format, you can extract any attributes needed from it. See more examples below.

### Query JSON attributes

```sql
-- extract `archived` flag
select attrs->>'archived' as is_archived
from hubspot.contacts
where id = '1501';
```

### Query custom properties

```sql
-- suppose the Contacts object has a custom property 'user_id', we can
-- define it as a column in the foreign table
create foreign table hubspot.contacts (
  id text,
  email text,
  firstname text,
  lastname text,
  user_id text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/contacts'
  );

select id, user_id from hubspot.contacts
where id = '1501';
```

Note that the column `user_id` is the custom property internal name, not its display name. It can be found in the `Details` section of `Edit property` page on HubSpot `Settings` -> `Data Management` -> `Properties`.

### Query custom objects

Suppose we have a HubSpot custom object `Projects` and its object type id is `2-3508482`. It also has a custom property `name`, we can define the foreign table and query it as below:

```sql
create foreign table hubspot.custom_projects (
  id text,
  name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server hubspot_server
  options (
    object 'objects/2-3508482'
  );

select * from hubspot.custom_projects;
```

Note that custom object type id `2-3508482` can be found in page URL on HubSpot `Settings` -> `Data Management` -> `Custom Objects`. For example,

```
https://app.hubspot.com/sales-products-settings/17328329/object/2-3508482
```
