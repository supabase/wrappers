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
object storage. A scan reads one rank-1 through rank-64 value array whose named
dimensions resolve to sibling coordinate arrays.

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
| `crs` | Best-effort CRS metadata from direct `crs`, `spatial_ref`, or `crs_wkt`, or from a resolved sibling `grid_mapping` reference |
| `attributes` | Complete `.zattrs` JSON object |
| `warnings` | Non-fatal metadata issues, such as malformed named dimensions |

The inspection surface exposes scientific metadata. Scans can opt into the
CF-style value and time-coordinate decoding described below; physical-unit and
CRS transformations are not applied yet. The complete `attributes` value remains
authoritative because scientific metadata conventions vary between datasets.

For CRS metadata, `zarr_inspect` keeps the raw `.zattrs` object in
`attributes` and fills `crs` as a convenience projection. Direct CRS metadata on
the current node wins in this order: `crs`, `spatial_ref`, then `crs_wkt`. If an
array has no direct CRS metadata but has a simple `grid_mapping` string, the
inspector attempts to resolve that name to a sibling array in the same group and
uses the sibling's direct CRS value. If that reference cannot be resolved, the
raw `grid_mapping` value remains visible in `crs` and a warning is emitted.
CRS strings, WKT, EPSG labels, and GeoTransform attributes are exposed as
metadata only; the wrapper does not validate them, assign SRIDs, transform
coordinates, or apply PostGIS spatial behavior during scans.

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

The selected value array must have a `_ARRAY_DIMENSIONS` attribute containing
one unique, safe name for every array dimension, in array order. Each name must
resolve to a same-group, same-name Zarr v2 coordinate array whose shape is one
dimensional and whose length matches the value-array extent. If a coordinate
array declares `_ARRAY_DIMENSIONS`, it must contain only its own name. Missing
or malformed dimension metadata fails instead of falling back to an inferred
`[y, x]` or `[time, y, x]` layout.

Dimension names are preserved for PostgreSQL column matching. Coordinate
metadata can classify dimensions as spatial X/Y, latitude/longitude, vertical,
time, band, channel, or unknown; roles do not rename dimensions. Recognized
`standard_name`, `axis`, and unambiguous units take precedence over conservative
name aliases. Incompatible recognized signals fail instead of being guessed.
Names such as `depth`, `height`, `altitude`, `level`, `lev`, and `z` are vertical
aliases, while band and channel remain distinct roles.

Supported value mappings are `<f4` to `real`, `<f8` to `double precision`,
`|i1`/`<i1` to PostgreSQL internal `"char"`, `<i2` to `smallint`, `<i4` to
`integer`, and `<i8` to `bigint`. A coordinate classified as time must be
declared `timestamptz`; every other currently supported numeric coordinate must
be `double precision`. Dimension columns do not need to be projected. The
wrapper reads coordinate metadata for every dimension but downloads coordinate
chunk values only for dimensions used by the query target or restrictions.

Finite monotonic coordinate values enable conservative chunk pruning. Finite
unordered coordinates remain projectable and filterable, but pruning is skipped
for that axis and PostgreSQL rechecks the original predicate. Integer coordinate
values must convert to `double precision` exactly; values that would lose
identity fail explicitly.

## Decode time coordinates from attributes

By default, raw values from the one coordinate classified as time are
interpreted from the manual table options
`time_unit` and `time_origin`, or as `seconds` since the Unix epoch when those
options are omitted.

Set `time_from_attrs 'true'` to derive the time conversion from the sibling
coordinate's `.zattrs` metadata instead:

```sql
create foreign table climate_temperature_from_attrs (
  time timestamptz,
  y double precision,
  x double precision,
  temperature real
)
server public_zarr_server
options (
  array_group 'climate/temperature',
  time_from_attrs 'true'
);
```

This mode is intentionally opt-in and cannot be combined with `time_unit` or
`time_origin`. It works at any supported rank and with any dimension name, but
requires exactly one coordinate classified as time whose `.zattrs` contains:

- `units` as `<unit> since <origin>`;
- `calendar` as `proleptic_gregorian`.

Supported constant-duration units are `seconds`, `milliseconds`,
`microseconds`, `nanoseconds`, `minutes`, `hours`, and `days`. The origin may
be a representable Gregorian date, date-time, or RFC 3339 date-time; an origin
without an explicit timezone is interpreted as UTC. Unsupported calendars,
malformed units, missing metadata, and out-of-range conversions fail clearly.

The resolved time conversion is used for both emitted `timestamptz` values and
timestamp predicate pruning. For sub-microsecond units, pruning conservatively
covers every raw value that can round to the PostgreSQL timestamp; PostgreSQL
still rechecks the exact predicate. The FDW performs no remote metadata I/O
during planning; metadata is read only when a scan starts.

## Decode packed scientific values

Set `decode_cf 'true'` on a foreign table to apply common CF-style missing-data
and packed-value attributes from the selected value array's `.zattrs`:

```sql
create foreign table decoded_temperature (
  time timestamptz,
  y double precision,
  x double precision,
  temperature double precision
)
server public_zarr_server
options (
  array_group 'climate/packed_temperature',
  time_unit 'seconds',
  time_origin 'unix',
  decode_cf 'true'
);
```

Decoded mode applies this order:

1. Decode the stored primitive value.
2. Map `_FillValue`, `missing_value`, and values outside `valid_range` or
   `valid_min`/`valid_max` to SQL `NULL`.
3. Return `raw * scale_factor + add_offset` as `double precision`.

Masking and valid-range checks happen before scale/offset, in the packed/raw
domain. A missing Zarr chunk is first materialized with the `.zarray`
`fill_value`; it becomes SQL `NULL` only when that raw value also matches the
scientific missing/validity metadata. The option defaults to `false`, which
preserves the raw dtype mappings above.

For floating-point arrays, the Zarr JSON spellings `"NaN"`, `"Infinity"`, and
`"-Infinity"` are accepted as missing sentinels. Declaring `"NaN"` masks every
NaN payload; an undeclared non-finite value remains a PostgreSQL non-finite
`double precision` value.

This value-decoding mode is independent from `time_from_attrs`. It does not
convert physical units, transform a CRS, or apply packing attributes to
coordinate arrays.

## Current limitations

- Read-only Zarr v2 on S3-compatible storage.
- Scans support one value array with rank 1 through 64 and mandatory
  `_ARRAY_DIMENSIONS`; scalar arrays remain unsupported.
- Every dimension currently requires a same-group, same-name, rank-1 numeric
  coordinate array. Synthesized ordinal coordinates, auxiliary or cross-group
  coordinates, curvilinear/multidimensional coordinates, and string or
  categorical band/channel coordinates are not supported.
- Coordinate packing, masks, valid ranges, and scale/offset are not decoded. If
  a coordinate used by a query declares those attributes, the scan fails rather
  than silently ignoring them.
- One temporal dimension is supported. Multiple temporal dimensions and
  per-axis calendars are not supported.
- A foreign-table scan still represents one value array and at most one queried
  non-dimension value column. Multi-variable scans and functional `bands`
  execution are not supported.
- Raw, gzip, zlib, and Blosc/LZ4 chunk compression is supported.
- Non-empty Zarr filters, Fortran order, Zarr v3, consolidated metadata,
  sharding, writes, and OME-Zarr are not supported.
- Zarr v3 is a separate storage-format implementation, not an alternate value
  decoder: it uses `zarr.json`, a chunk-grid and chunk-key encoding, and an
  ordered codec pipeline instead of the v2 `.zarray` layout. The dataset and
  scientific-semantics models are intended to accept a future v3 adapter, but
  current scans and inspection remain explicitly v2-only.
- `LIMIT` alone does not prevent coordinate metadata loading or matching-chunk
  enumeration; use selective coordinate predicates for large arrays.
- Scan execution limits each loaded coordinate and all loaded coordinates
  together to 16,777,216 values. It also limits an eager selection to one
  million chunk coordinates and 64 MiB of rank-sized chunk-index allocation;
  decoded chunks retain their existing 256 MiB limit. These are safety bounds,
  not Zarr format limits.
- Inspection has hard depth, node, list-page, object-size, and total metadata
  limits and fails explicitly rather than returning a truncated hierarchy.
