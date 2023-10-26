[Firebase](https://firebase.google.com/) is an app development platform built around non-relational technologies. The Firebase Wrapper supports connecting to below objects.

1. [Authentication Users](https://firebase.google.com/docs/auth/users) (*read only*)
2. [Firestore Database Documents](https://firebase.google.com/docs/firestore) (*read only*)

## Preparation

Create a service account on [google cloud platform](https://console.cloud.google.com/iam-admin/serviceaccounts) with `Firebase Admin` role

Before you get started, make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers;
```

and then create the foreign data wrapper:

```sql
create foreign data wrapper firebase_wrapper
  handler firebase_fdw_handler
  validator firebase_fdw_validator;
```

### Secure your credentials (optional)

By default, Postgres stores FDW credentials inide `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

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

### Connecting to Firebase

We need to provide Postgres with the credentials to connect to Firebase, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server firebase_server
      foreign data wrapper firebase_wrapper
      options (
        sa_key_id '<key_ID>', -- The Key ID from above.
        project_id '<firebase_project_id>'
    );
    ```

=== "Without Vault"

    ```sql
    create server firebase_server
      foreign data wrapper firebase_wrapper
       options (
         sa_key '
         {
            "type": "service_account",
            "project_id": "your_gcp_project_id",
            ...
         }
        ',
         project_id 'firebase_project_id'
       );
    ```

## Creating Foreign Tables

The Firebase Wrapper supports reading data from below Firebase's objects:

| Firebase                     | Select            | Insert     | Update     | Delete   | Truncate          |
| -----------                  | :----:            | :----:     | :----:     | :----:   | :----:            |
| Authentication Users         | :white_check_mark:| :x:        | :x:        | :x:      | :x:               |
| Firestore Database Documents | :white_check_mark:| :x:        | :x:        | :x:      | :x:               |

For example:

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

Note there is a meta column `attrs` in the foreign table, which contains all the returned data from Firebase as json format. 

### Foreign table options

The full list of foreign table options are below:

- `object` - Object name in Firebase, required.

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
    object 'firestore/user-profiles'  -- format: 'firestore/[collection_id]'
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
