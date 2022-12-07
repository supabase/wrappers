select pg_sleep(20);
create extension pgsodium;
create schema vault;
create extension supabase_vault with schema vault;
create extension wrappers;
