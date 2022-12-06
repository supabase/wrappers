Firebase is an app development platform built around non-relational technologies. The Firebase wrapper supports connecting to the [auth/users collection](https://firebase.google.com/docs/auth/users) and any [Firestore collection](https://firebase.google.com/docs/firestore). 


### Wrapper 
To get started with the Firebase wrapper, create a foreign data wrapper specifying `FirebaseFdw` as the `wrapper` key of the `options` section.


```sql
create extension if not exists wrappers;

create foreign data wrapper firebase_wrapper
  handler wrappers_handler
  validator wrappers_validator
  options (
    wrapper 'FirebaseFdw'
  );
```

### Server 

Next, we need to create a server for the FDW to hold options and credentials.

#### Auth (Supabase)

If you are using the Supabase platform, this is the recommended approach for securing your Firebase service account credentials.

Create a secure key using pgsodium
```sql
select pgsodium.create_key(name := 'firebase');
```

Save your [Firebase service account key](https://firebase.google.com/docs/admin/setup#add_firebase_to_your_app) in Vault and retrieve the `key_id`
```sql
insert into
  vault.secrets (secret, key_id)
values 
  (
    '{
      "type": "service_account",
      "project_id": "your_gcp_project_id",
      ...
    }',
    (select id from pgsodium.valid_key where name = 'firebase')
  );
returning
  key_id;
```

Create the foreign server
```sql
create server firebase_server
  foreign data wrapper firebase_wrapper
  options (
    sa_key_id '<your key_id from above>'
    project_id '<firebase_project_id>'
);
```

#### Auth (Insecure)

If the platform you are using does not support `pgsodium` and `Vault`, you can create a server by storing yourt [service account key](https://firebase.google.com/docs/admin/setup#add_firebase_to_your_app) directly.


!!! important

    Credentials stored using this method can be viewed as plain text by anyone with access to `pg_catalog.pg_foreign_server`


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
     project_id 'firebase_project_id',
   );
```


### Tables

Firebase collections are non-relational/documents. With the exception of metadata fields, all returned data are availble as a `fields` jsonb column. 

#### Firestore

To map a Firestore collection provide its location using the format `firestore/<collection_id>` as the `object` option as shown below.

```sql
create foreign table firebase_docs (
  name text,
  fields jsonb,
  create_time timestamp,
  update_time timestamp
)
  server my_firebase_server
  options (
    object 'firestore/user-profiles'  -- format: 'firestore/[collection_id]'
  );
```

Note that `name`, `create_time`, and `update_time`, are automatic metadata fields on all Firestore collections.


#### auth/users 

The `auth/users` collection is a special case with unique metadata. The following shows how to map Firebase users to PostgreSQL table.

```sql
create foreign table firebase_users (
  uid text,
  email text,
  fields jsonb
)
  server my_firebase_server
  options (
    object 'auth/users'
  );
```
