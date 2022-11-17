# Firebase Foreign Data Wrapper

This is a foreign data wrapper for [Firebase](https://firebase.google.com/). It is developed using [Wrappers](https://github.com/supabase/wrappers).


## Basic usage

These steps outline how to use the this FDW:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgx with feature:

```bash
cd wrappers/wrappers
cargo pgx run --features firebase_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
drop extension if exists wrappers cascade;
create extension wrappers;

-- create foreign data wrapper and enable 'FirebaseFdw'
drop foreign data wrapper if exists firebase_wrapper cascade;
create foreign data wrapper firebase_wrapper
  handler wrappers_handler
  validator wrappers_validator
  options (
    wrapper 'FirebaseFdw'
  );

-- create server and specify custom options
drop server if exists my_firebase_server cascade;
create server my_firebase_server
  foreign data wrapper firebase_wrapper
  options (
    project_id 'firebase_project_id',
    sa_key_file '/absolute/path/to/service_account_key.json'
  );

-- create an example foreign table
drop foreign table if exists firebase_users;
create foreign table firebase_users (
  local_id text,
  email text,
  fields jsonb
)
  server my_firebase_server
  options (
    object 'auth/users',
    base_url 'https://identitytoolkit.googleapis.com/v1/projects'
  );

drop foreign table if exists firebase_docs;
create foreign table firebase_docs (
  name text,
  fields jsonb,
  create_time timestamp,
  update_time timestamp
)
  server my_firebase_server
  options (
    object 'firestore/user-profiles',  -- format: 'firestore/[collection_id]'
    base_url 'https://firestore.googleapis.com/v1beta1/projects'
  );
```

4. Run a query to check if it is working:

```sql
select * from firebase_users;
select * from firebase_docs;
```


