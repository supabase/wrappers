---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Firebase

[Firebase](https://firebase.google.com/) is an app development platform built around non-relational technologies. The Firebase Wrapper supports connecting to below objects.

1. [Authentication Users](https://firebase.google.com/docs/auth/users) (_read only_)
2. [Firestore Database Documents](https://firebase.google.com/docs/firestore) (_read only_)

!!! warning

    Restoring a logical backup of a database with a materialized view using a foreign table can fail. For this reason, either do not use foreign tables in materialized views or use them in databases with physical backups enabled.

## Preparation

Before you can query Firebase, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Firebase Wrapper

Enable the `firebase_wrapper` FDW:

```sql
create foreign data wrapper firebase_wrapper
  handler firebase_fdw_handler
  validator firebase_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Firebase credentials in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'firebase',
  '{
      "type": "service_account",
      "project_id": "your_gcp_project_id",
      ...
  }'
)
returning key_id;
```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema firebase;
```

## Options

The full list of foreign table options are below:

- `object` - Object name in Firebase, required.

  For Authenciation users, the object name is fixed to `auth/users`. For Firestore documents, its format is `firestore/<collection_id>`, note that collection id must be a full path id. For example,

  - `firestore/my-collection`
  - `firestore/my-collection/my-document/another-collection`


## Entities

### Authentication Users

This is an object representing Firebase Authentication Users.

Ref: [Firebase Authentication Users](https://firebase.google.com/docs/auth/users)

#### Operations

| Object               | Select | Insert | Update | Delete | Truncate |
| -------------------- | :----: | :----: | :----: | :----: | :------: |
| Authentication Users |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table firebase_users (
  uid text,
  email text,
  created_at timestamp,
  attrs jsonb
)
  server firebase_server
  options (
    object 'auth/users'
  );
```

#### Notes

- The `attrs` column contains all user attributes in JSON format
- This is a special collection with unique metadata fields

### Firestore Database Documents

This is an object representing Firestore Database Documents.

Ref: [Firestore Database](https://firebase.google.com/docs/firestore)

#### Operations

| Object                       | Select | Insert | Update | Delete | Truncate |
| ---------------------------- | :----: | :----: | :----: | :----: | :------: |
| Firestore Database Documents |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table firebase_docs (
  name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server firebase_server
  options (
    object 'firestore/user-profiles'
  );
```

#### Notes

- The `name`, `created_at`, and `updated_at` are automatic metadata fields on all Firestore collections
- Collection ID must be a full path ID in the format `firestore/<collection_id>`
- Examples of valid collection paths:
  - `firestore/my-collection`
  - `firestore/my-collection/my-document/another-collection`
- The `attrs` column contains all document attributes in JSON format

## Query Pushdown Support

This FDW doesn't support query pushdown.

## Examples

Some examples on how to use Firebase foreign tables.

### firestore

To map a Firestore collection provide its location using the format `firestore/<collection_id>` as the `object` option as shown below.

```sql
create foreign table firebase_docs (
  name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server firebase_server
  options (
    object 'firestore/user-profiles'
  );
```

Note that `name`, `created_at`, and `updated_at`, are automatic metadata fields on all Firestore collections.

### auth/users

The `auth/users` collection is a special case with unique metadata. The following shows how to map Firebase users to PostgreSQL table.

```sql
create foreign table firebase_users (
  uid text,
  email text,
  created_at timestamp,
  attrs jsonb
)
  server firebase_server
  options (
    object 'auth/users'
  );
```
