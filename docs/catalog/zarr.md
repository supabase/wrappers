---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Zarr

The Zarr Wrapper provides read-only access to Zarr v2 arrays in S3-compatible
object storage. The current scan profile supports one rank-2 `[y, x]` or
rank-3 `[time, y, x]` value array with sibling coordinate arrays.

## Enable the wrapper

```sql
create extension if not exists wrappers with schema extensions;

create foreign data wrapper zarr_wrapper
  handler zarr_fdw_handler
  validator zarr_fdw_validator;
```

Create a server for the root of one Zarr store:

```sql
create server public_zarr_server
  foreign data wrapper zarr_wrapper
  options (
    store_url 's3://example-bucket/datasets/climate.zarr',
    aws_region 'us-east-1',
    anonymous 'true'
  );
```

For S3-compatible services such as MinIO, also set `endpoint_url` and, when
required, `path_style_url 'true'`. Authentication can use the AWS provider
chain, `anonymous 'true'`, a complete direct key pair, or a complete Vault key
pair. Authentication modes cannot be combined.

Inspection requires `s3:ListBucket` and `s3:GetObject`. Scanning requires
`s3:GetObject`.

## Inspect a dataset

Use `zarr_inspect` before defining a foreign table:

```sql
select path,
       kind,
       variable,
       dimensions,
       shape,
       chunks,
       dtype,
       units,
       calendar
from zarr_inspect('public_zarr_server')
order by path;
```

The caller must have `USAGE` on the foreign server. Inspection traverses the
group hierarchy and reads only `.zgroup`, `.zarray`, and `.zattrs`; it does not
read or decode chunk objects.

The function returns these fields:

| Field | Meaning |
| --- | --- |
| `path` | Path relative to the configured Zarr root; `/` is the root node |
| `kind` | `group` or `array` |
| `group_path` | Parent group for a non-root node |
| `variable` | Array name; `NULL` for groups |
| `zarr_format` | Format version recorded by `.zgroup` or `.zarray` |
| `shape`, `chunks` | Raw JSON arrays, preserving the metadata integer range |
| `dimensions` | Named dimensions from xarray `_ARRAY_DIMENSIONS` |
| `dtype` | Raw NumPy/Zarr dtype string |
| `codecs` | Zarr v2 `filters` and `compressor` metadata |
| `fill_value` | Raw `.zarray` fill value |
| `units`, `calendar` | Common scientific attributes when they are strings |
| `scale_factor`, `add_offset` | Finite numeric scientific attributes |
| `crs` | Best-effort projection of `crs`, `spatial_ref`, `crs_wkt`, or `grid_mapping` |
| `attributes` | Complete `.zattrs` JSON object |
| `warnings` | Non-fatal metadata issues, such as malformed named dimensions |

The inspection surface exposes scientific metadata but does not apply
scale/offset, calendar, units, or CRS transformations to scan results yet.
The complete `attributes` value remains authoritative because scientific
metadata conventions vary between datasets.

## Query an array

After inspection, define a foreign table using PostgreSQL types that match the
array dtype:

```sql
create foreign table climate_temperature (
  time timestamptz,
  y double precision,
  x double precision,
  temperature real
)
server public_zarr_server
options (
  array_group 'climate/temperature',
  time_unit 'seconds',
  time_origin 'unix'
);

select time, y, x, temperature
from climate_temperature
where time >= timestamptz '2025-01-01 00:00:00+00'
  and time <  timestamptz '2025-01-02 00:00:00+00'
  and y between 30 and 31
  and x between -8 and -7;
```

Supported value mappings are `<f4` to `real`, `<f8` to `double precision`,
`|i1`/`<i1` to PostgreSQL internal `"char"`, `<i2` to `smallint`, `<i4` to
`integer`, and `<i8` to `bigint`. Coordinate columns `x` and `y` must be
`double precision`; `time` must be `timestamptz`.

## Current limitations

- Read-only Zarr v2 on S3-compatible storage.
- Scans support rank 2 `[y, x]` and rank 3 `[time, y, x]` arrays only.
- Raw, gzip, zlib, and Blosc/LZ4 chunk compression is supported.
- Non-empty Zarr filters, Fortran order, Zarr v3, consolidated metadata,
  sharding, writes, and arbitrary-dimensional scan execution are not yet
  supported.
- `LIMIT` alone does not prevent coordinate metadata loading or matching-chunk
  enumeration; use selective coordinate predicates for large arrays.
- Inspection has hard depth, node, list-page, object-size, and total metadata
  limits and fails explicitly rather than returning a truncated hierarchy.
