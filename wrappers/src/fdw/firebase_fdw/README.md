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
-- Here we're using the service account key stored in Vault, if you don't want
-- to use Vault, you can directly specify the service account key using `sa_key`
-- option. For example,
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
--
do $$
declare
  key_id text;
begin
  select id into key_id from pgsodium.valid_key where name = 'firebase' limit 1;

  drop server if exists my_firebase_server cascade;

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


