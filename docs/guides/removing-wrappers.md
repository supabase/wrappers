# Removing Foreign Data Wrappers

This guide explains how to fully remove a foreign data wrapper from your PostgreSQL database.

## Components to Remove

When removing a foreign data wrapper, you need to remove several components in the correct order:

1. Foreign Tables
2. Foreign Servers
3. Extension
4. (For WASM wrappers only) Cache files

## Step-by-Step Removal Process

### 1. List and Remove Foreign Tables

First, list all foreign tables associated with your wrapper:

```sql
select ft.relname as foreign_table_name,
       n.nspname as schema_name,
       fdw.fdwname as wrapper_name
from pg_catalog.pg_foreign_table ft
join pg_catalog.pg_class c on c.oid = ft.ftrelid
join pg_catalog.pg_namespace n on n.oid = c.relnamespace
join pg_catalog.pg_foreign_server fs on fs.oid = ft.ftserver
join pg_catalog.pg_foreign_data_wrapper fdw on fdw.oid = fs.srvfdw;
```

Remove each foreign table:

```sql
drop foreign table if exists schema_name.table_name;
```

### 2. Remove Foreign Servers

List servers:

```sql
select fs.srvname as server_name,
       fdw.fdwname as wrapper_name
from pg_catalog.pg_foreign_server fs
join pg_catalog.pg_foreign_data_wrapper fdw on fdw.oid = fs.srvfdw;
```

Remove each server:

```sql
drop server if exists server_name cascade;
```

### 3. Remove the Extension

```sql
drop extension if exists wrappers cascade;
```

### 4. Additional Steps for WASM Wrappers

WASM wrappers cache their compiled code locally. To fully clean up:

1. Stop the PostgreSQL server
2. Remove cached WASM files:
   ```bash
   rm -rf /var/lib/postgresql/[version]/wasm_cache/*
   ```
3. Restart the PostgreSQL server

## Verification

After removal, verify that all components are gone:

```sql
-- Check for remaining foreign tables
select ft.relname as foreign_table_name,
       n.nspname as schema_name
from pg_catalog.pg_foreign_table ft
join pg_catalog.pg_class c on c.oid = ft.ftrelid
join pg_catalog.pg_namespace n on n.oid = c.relnamespace;

-- Check for remaining servers
select fs.srvname as server_name,
       fdw.fdwname as wrapper_name
from pg_catalog.pg_foreign_server fs
join pg_catalog.pg_foreign_data_wrapper fdw on fdw.oid = fs.srvfdw;

-- Check for the extension
select extname, extversion
from pg_catalog.pg_extension
where extname = 'wrappers';
```

## Common Issues

- Always use cascade when dropping servers if you're unsure about dependencies
- For production environments, take a backup before removing components
- Some wrappers might have additional cleanup steps; check wrapper-specific documentation
