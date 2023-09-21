# Firebase Foreign Data Wrapper

This is a foreign data wrapper for [Firebase](https://firebase.google.com/) developed using [Wrappers](https://github.com/supabase/wrappers).

This FDW currently supports reading below data from Firebase:

1. Authentication Users
2. Firestore Database documents

## Installation

This FDW requires [pgrx](https://github.com/tcdi/pgrx), please refer to its installation page to install it first.

After `pgrx` is installed, run below command to install this FDW.

```bash
cargo pgrx install --pg-config [path_to_pg_config] --features firebase_fdw
```

## Basic usage

These steps outline how to use the this FDW locally:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgrx with feature:

```bash
cd wrappers/wrappers
cargo pgrx run --features firebase_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
create extension wrappers;

-- create foreign data wrapper and enable 'FirebaseFdw'
create foreign data wrapper firebase_wrapper
  handler firebase_fdw_handler
  validator firebase_fdw_validator;

-- Below we're using the service account key stored in Vault, if you don't want
-- to use Vault, you can directly specify the service account key in `sa_key`
-- option but it is less secure. For example,
--
-- create server my_firebase_server
--   foreign data wrapper firebase_wrapper
--   options (
--     sa_key '
--     {
--        "type": "service_account",
--        "project_id": "your_gcp_project_id",
--        ...
--     }
--    ',
--     project_id 'firebase_project_id',
--   );

-- save Firebase service account json in Vault and get its key id
select pgsodium.create_key(name := 'firebase');
insert into vault.secrets (secret, key_id) values ('
{
  "type": "service_account",
  "project_id": "your_gcp_project_id",
  ...
}
',
(select id from pgsodium.valid_key where name = 'firebase')
) returning key_id;

-- create server and specify custom options
do $$
declare
  key_id text;
begin
  select id into key_id from pgsodium.valid_key where name = 'firebase' limit 1;

  execute format(
    E'create server my_firebase_server \n'
    '   foreign data wrapper firebase_wrapper \n'
    '   options ( \n'
    '     sa_key_id ''%s'', \n'
    '     project_id ''firebase_project_id'' \n'
    ' );',
    key_id
  );
end $$;

-- create an example foreign table
create foreign table firebase_users (
  uid text,
  email text,
  created_at timestamp,
  attrs jsonb
)
  server my_firebase_server
  options (
    object 'auth/users'
  );

create foreign table firebase_docs (
  name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server my_firebase_server
  options (
    object 'firestore/user-profiles'  -- format: 'firestore/[collection_id]'
  );
```

4. Run a query to check if it is working:

```sql
select * from firebase_users;
select * from firebase_docs;
```

## Configurations

### Server options

Below are the options can be used in `CREATE SERVER`:

1. `sa_key` - service account key in JSON format, required if `sa_key_id` not specified
2. `sa_key_id` - service account id stored in Vault, required if `sa_key` not specified
3. `project_id` - Firebase project ID, required
4. `access_token` - OAuth2 token to access Firebase, optional 

### Foreign table options

Below are the options can be used in `CREATE FOREIGN TABLE`:

1. `object`, required

   - For Authentication users, it is fixed to `auth/users`.
   - For Firestore database, its format is `firestore/[collection_id]`, for example, `firestore/user-profiles`.

2. `base_url` - base URL of Firebase API, optional

   - For Authentication users, default is https://identitytoolkit.googleapis.com/v1/projects
   - For Firestore database, default is https://firestore.googleapis.com/v1beta1/projects

3. `limit` - maximum number of rows to read, optional, default is 10,000

## Limitations

- Firebase Storage is not supported, please refer to [Firebase to Supabase migration guide](https://supabase.com/docs/guides/migrations/firebase-storage) to learn more about how to read its data out.
- `WHERE`, `ORDER BY`, `LIMIT` pushdown are not supported.

## Changelog

| Version | Date       | Notes                                                |
| ------- | ---------- | ---------------------------------------------------- |
| 0.1.3   | 2023-09-20 | Error reporting refactoring                          |
| 0.1.2   | 2023-07-13 | Added fdw stats collection                           |
| 0.1.1   | 2022-12-07 | Added validator function                             |
| 0.1.0   | 2022-11-30 | Initial version                                      |
