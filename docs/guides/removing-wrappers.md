# Removing Foreign Data Wrappers

This guide explains how to fully remove all foreign data wrappers from your PostgreSQL database.


## Components to Remove

When removing a foreign data wrapper, you need to remove several components in the correct order:

1. Foreign Tables
2. Foreign Servers
3. Wrappers Extension

## Step-by-Step Removal Process

### 1. List and Remove Foreign Tables

First, list all foreign tables associated with your wrapper:

```sql
select c.relname as foreign_table_name,
       n.nspname as schema_name
from pg_catalog.pg_foreign_table ft
join pg_catalog.pg_class c on c.oid = ft.ftrelid
join pg_catalog.pg_namespace n on n.oid = c.relnamespace;
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

!!! note

    With `cascade` option, this will also drop all foreign tables using that foreign server.

### 3. Remove the Extension

```sql
drop extension if exists wrappers cascade;
```

!!! note

    With `cascade` option, this will also drop all foreign servers and foreign tables using Wrappers extension.

