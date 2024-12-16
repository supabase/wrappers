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
SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables;
```

Remove each foreign table:

```sql
DROP FOREIGN TABLE IF EXISTS [schema_name.]table_name;
```

### 2. Remove Foreign Servers

List servers:

```sql
SELECT srvname, fdwname
FROM pg_foreign_server fs
JOIN pg_foreign_data_wrapper fdw ON fs.srvfdw = fdw.oid;
```

Remove each server:

```sql
DROP SERVER IF EXISTS server_name CASCADE;
```

### 3. Remove the Extension

```sql
DROP EXTENSION IF EXISTS wrappers CASCADE;
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
SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables;

-- Check for remaining servers
SELECT srvname, fdwname
FROM pg_foreign_server fs
JOIN pg_foreign_data_wrapper fdw ON fs.srvfdw = fdw.oid;

-- Check for the extension
SELECT * FROM pg_extension WHERE extname = 'wrappers';
```

## Common Issues

- Always use CASCADE when dropping servers if you're unsure about dependencies
- For production environments, take a backup before removing components
- Some wrappers might have additional cleanup steps; check wrapper-specific documentation
