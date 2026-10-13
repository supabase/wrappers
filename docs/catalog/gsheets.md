---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Google Sheets

[Google Sheets](https://www.google.com/sheets/about/) is a web-based spreadsheet application.

The Google Sheets Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read data from a Google Sheets spreadsheet for use within your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                                    | Checksum | Required Wrappers Version |
| ------- | --------------------------------------------------------------------------------------------------- | -------- | ------------------------- |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_gsheets_fdw_v0.1.0/gsheets_fdw.wasm`   | tbd      | >=0.5.0                   |

## Preparation

Before you can query Google Sheets, you need to enable the Wrappers extension and create a Google API key.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Google Sheets Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Get a Google API key

1. In the [Google Cloud Console](https://console.cloud.google.com/), enable the **Google Sheets API** for your project.
2. Create an **API key** under _APIs & Services_ > _Credentials_.
3. Share the spreadsheet you want to query as **Anyone with the link can view**. An API key can only read publicly shared spreadsheets.

### Connecting to Google Sheets

We need to provide Postgres with the API key and any additional options. We can do this using the `create server` command:

```sql
create server gsheets_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_gsheets_fdw_v0.1.0/gsheets_fdw.wasm',
    fdw_package_name 'supabase:gsheets-fdw',
    fdw_package_version '0.1.0',
    fdw_package_checksum 'tbd',
    api_key '<Google API key>'
  );
```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists gsheets;
```

## Options

The following options are available.

### Server options

- `api_key` - Google API key, required.

### Foreign table options

- `spreadsheet_id` - ID of the spreadsheet, required. It is the part of the spreadsheet URL between `/d/` and `/edit`: `https://docs.google.com/spreadsheets/d/<spreadsheet_id>/edit`.
- `sheet` - Name of the sheet (tab) inside the spreadsheet, required.

## Entities

### Sheet

Each foreign table maps to one sheet of a spreadsheet.

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| Sheet  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

For a sheet named `People` with this content:

| Name  | Age | Joined     |
| ----- | --- | ---------- |
| Alice | 30  | 2024-01-15 |
| Bob   | 25  | 2023-11-02 |

```sql
create foreign table gsheets.people (
  name text,
  age bigint,
  joined date
)
  server gsheets_server
  options (
    spreadsheet_id '<spreadsheet id>',
    sheet 'People'
  );

select * from gsheets.people;
```

#### Notes

- The first row of the sheet must contain the column names.
- Foreign table columns are matched to the sheet header by name. Both the sheet header names and the foreign table column names are normalized before they are compared: the name is trimmed, lower-cased, and every character that is not a letter or a digit is replaced with `_`. So a header `First Name` matches the column `first_name`, and also the quoted column `"First Name"`. A foreign table column that is not found in the header returns an error.
- Foreign table columns can be in any order, and the sheet can have more columns than the foreign table.
- Empty cells are returned as `null`.
- Supported column types are:

  | Postgres type              | Sheet cell                                              |
  | -------------------------- | ------------------------------------------------------- |
  | `boolean`                  | checkbox or `TRUE`/`FALSE` value                        |
  | `int`, `bigint`            | whole number                                            |
  | `double precision`         | number                                                  |
  | `numeric`                  | number                                                  |
  | `text`                     | any value, numbers and booleans are converted to text   |
  | `date`                     | date or date time                                       |
  | `timestamp`, `timestamptz` | date time, the value is read as UTC                     |

  Other types, such as `real` or `jsonb`, are not supported.
- A value that cannot be converted to the column type returns an error with the sheet row number and column name.
- Values are read unformatted, so a cell showing `$1,234.50` is returned as the number `1234.5`.
- The whole sheet is fetched on every query, and no filter, sort or limit is pushed down to Google Sheets.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Read-only, `insert`, `update` and `delete` are not supported
- Only API key authentication is supported, so only spreadsheets shared as "Anyone with the link can view" can be queried
- No query pushdown, large sheets are fully fetched and filtered by Postgres
- Foreign table definitions cannot be imported with `import foreign schema`
- Materialized views using these foreign tables may fail during logical backups

## Examples

### Basic example

This example reads a public spreadsheet and filters it with SQL:

```sql
create foreign table gsheets.people (
  name text,
  age bigint,
  joined date
)
  server gsheets_server
  options (
    spreadsheet_id '<spreadsheet id>',
    sheet 'People'
  );

select name, age
from gsheets.people
where joined >= '2024-01-01'
order by age desc;
```
