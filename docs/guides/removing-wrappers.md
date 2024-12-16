# Removing Foreign Data Wrappers

This guide explains how to fully remove a foreign data wrapper from your PostgreSQL database.

> **Warning**: Dropping the wrappers extension will impact all Foreign Data Wrappers and should only be executed if no wrapper FDWs are present on the system.

## Components to Remove

When removing a foreign data wrapper, you need to remove several components in the correct order:

1. Foreign Tables
2. Foreign Servers
3. Extension

## Step-by-Step Removal Process

### 1. List and Remove Foreign Tables

First, list all foreign tables associated with your wrapper:

```sql
select pg_catalog.pg_class.relname as foreign_table_name,
       pg_catalog.pg_namespace.nspname as schema_name,
       pg_catalog.pg_foreign_data_wrapper.fdwname as wrapper_name
from pg_catalog.pg_foreign_table
join pg_catalog.pg_class on pg_catalog.pg_class.oid = pg_catalog.pg_foreign_table.ftrelid
join pg_catalog.pg_namespace on pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
join pg_catalog.pg_foreign_server on pg_catalog.pg_foreign_server.oid = pg_catalog.pg_foreign_table.ftserver
join pg_catalog.pg_foreign_data_wrapper on pg_catalog.pg_foreign_data_wrapper.oid = pg_catalog.pg_foreign_server.srvfdw;
```

Remove each foreign table:

```sql
drop foreign table if exists schema_name.table_name;
```

### 2. Remove Foreign Servers

List servers:

```sql
select pg_catalog.pg_foreign_server.srvname as server_name,
       pg_catalog.pg_foreign_data_wrapper.fdwname as wrapper_name
from pg_catalog.pg_foreign_server
join pg_catalog.pg_foreign_data_wrapper on pg_catalog.pg_foreign_data_wrapper.oid = pg_catalog.pg_foreign_server.srvfdw;
```

Remove each server:

```sql
drop server if exists server_name cascade;
```

### 3. Remove the Extension

```sql
drop extension if exists wrappers;
```

## Verification

After removal, verify that all components are gone:

```sql
-- Check for remaining foreign tables
select pg_catalog.pg_class.relname as foreign_table_name,
       pg_catalog.pg_namespace.nspname as schema_name
from pg_catalog.pg_foreign_table
join pg_catalog.pg_class on pg_catalog.pg_class.oid = pg_catalog.pg_foreign_table.ftrelid
join pg_catalog.pg_namespace on pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace;

-- Check for remaining servers
select pg_catalog.pg_foreign_server.srvname as server_name,
       pg_catalog.pg_foreign_data_wrapper.fdwname as wrapper_name
from pg_catalog.pg_foreign_server
join pg_catalog.pg_foreign_data_wrapper on pg_catalog.pg_foreign_data_wrapper.oid = pg_catalog.pg_foreign_server.srvfdw;

-- Check for the extension
select extname, extversion
from pg_catalog.pg_extension
where extname = 'wrappers';
```

## Common Issues

- Always use cascade when dropping servers if you're unsure about dependencies
- For production environments, take a backup before removing components
- Some wrappers might have additional cleanup steps; check wrapper-specific documentation
