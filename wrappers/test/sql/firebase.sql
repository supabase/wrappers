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
    project_id 'supa',
    access_token 'owner'
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
    base_url 'http://firebase:9099/identitytoolkit.googleapis.com/v1/projects'
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
    object 'firestore/my-collection',  -- format: 'firestore/[collection_id]'
    base_url 'http://firebase:8080/v1/projects'
  );

select email from firebase_users;
select name, fields from firebase_docs;
