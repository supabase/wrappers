# Updating Foreign Data Wrappers

This guide explains how to update foreign data wrappers in your PostgreSQL database, with special focus on WebAssembly (WASM) wrappers.

## Types of Updates

There are two main types of updates for foreign data wrappers:

1. **Native wrapper updates**: Updates to native Postgres extensions
2. **WASM wrapper updates**: Updates to WebAssembly-based wrappers

## Updating WASM Foreign Data Wrappers

WASM wrappers can be updated without reinstalling the entire extension, by modifying the foreign server configuration.

### Step-by-Step Update Process

#### 1. Update the Foreign Server

To update a WASM wrapper, you need to update the URL, version, and checksum:

```sql
alter server slack_server options (
  set fdw_package_url 'https://github.com/supabase/wrappers/releases/download/....',
  set fdw_package_version '0.0.1',
  set fdw_package_checksum 'new-checksum-here'
);
```

**Important options:**

- `fdw_package_url`: The URL to the new version of the WASM package
- `fdw_package_version`: The new version number
- `fdw_package_checksum`: The checksum of the new version for verification

#### 3. Verify the Update

You can verify that the server has been updated by querying it again:

```sql
select option_name, option_value 
from pg_options_to_table(
  (select srvoptions from pg_foreign_server where srvname = 'slack_server')
);
```

You can change `srvname` to the name of the FDW when you installed it.

## Updating Native Foreign Data Wrappers

This is valid if your version of Postgres has multiple versions of the FDW installed.

```sql
alter extension wrappers update;
```

## Backward Compatibility Considerations

When updating wrappers, consider the following:

1. **Schema changes**: Some updates might change the schema of foreign tables, requiring you to recreate them
2. **API changes**: The wrapper might interact with a different version of an external API
3. **Connection parameters**: New parameters might be required or old ones deprecated

Always check the release notes for any breaking changes before updating.

## Troubleshooting

If you encounter issues after updating:

1. **Check server logs**: PostgreSQL logs may contain error messages
2. **Verify options**: Make sure all required options are set correctly
3. **Test with a simple query**: Start with basic queries to isolate problems