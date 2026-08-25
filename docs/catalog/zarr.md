---
source:
documentation:
author: HamzaMPSY(https://github.com/HamzaMPSY)
tags:
  - native
  - community
---

# Zarr

The Zarr Wrapper provides read-only access to Zarr v2 arrays and a core subset
of Zarr v3 arrays in S3-compatible object storage, trusted anonymous HTTP(S)
object stores, or a secured local filesystem directory. A scan reads one
rank-1 through rank-64 value array whose named dimensions resolve to sibling
coordinate arrays.

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

An absolute local directory can be used without copying the dataset into
object storage:

```sql
create server local_zarr_server
  foreign data wrapper zarr_wrapper
  options (
    store_url 'file:///srv/zarr/climate.zarr'
  );
```

Local URLs must have the exact `file:///absolute/path` form, without a host,
userinfo, query, or fragment. S3 authentication, endpoint, region, and
path-style options cannot be used with a local store. Creating or altering a
local Zarr server requires a PostgreSQL superuser, and the foreign server must
remain owned by a superuser at execution time. A superuser can grant `USAGE`
on a fixed server and `SELECT` on its foreign tables to other roles.

The configured local root and its contents must be administered outside
PostgreSQL and must not be writable by untrusted operating-system users. Reads
reject traversal and symbolic links that escape the configured root;
directory discovery does not follow symbolic-link entries, and final Zarr
objects must be regular files. Missing regular object paths retain normal Zarr
fill-value behavior; permission, file-type, containment, and mutation failures
are errors rather than missing chunks. Errors identify store-relative object
keys without exposing the ambient filesystem root.

A trusted server that exposes Zarr objects as ordinary anonymous HTTPS `GET`
requests can be used directly:

```sql
create server https_zarr_server
  foreign data wrapper zarr_wrapper
  options (
    store_url 'https://datasets.example.org/climate.zarr'
  );
```

HTTP(S) store URLs require a host and may contain a port and path, but not
userinfo, credentials, a query, or a fragment. S3 authentication, endpoint,
region, and path-style options cannot be used. The backend does not send
authorization headers or cookies, follow redirects, use ambient HTTP proxies,
retry requests, or transparently decode HTTP content encodings. Requests use
`Accept-Encoding: identity`, and any non-identity response encoding is an
error. Creating or altering an HTTP(S) Zarr server requires a PostgreSQL
superuser, and its foreign-server owner must remain a superuser. Grant fixed
servers to readers with PostgreSQL `USAGE` and `SELECT` privileges.

HTTPS is required by default and uses normal certificate and hostname
verification. Plain HTTP is unencrypted and must be enabled explicitly only
for a trusted network or local test server:

```sql
create server insecure_test_zarr_server
  foreign data wrapper zarr_wrapper
  options (
    store_url 'http://127.0.0.1:8787/climate.zarr',
    allow_insecure_http 'true'
  );
```

An HTTP object server must return `200` for a complete object and `404` only
when that object is absent. Missing chunks then retain normal Zarr fill-value
behavior; redirects and every other status are errors. Indexed Zarr v3 shards
additionally require single byte-range `GET` support with exact `206`,
`Content-Range`, an exact `Content-Length` when that header is present, and a
quoted strong `ETag`. Payload ranges use `If-Match`; a missing, changed, or
unsatisfiable conditioned object fails instead of combining two shard
generations. Responses remain bounded when `Content-Length` is absent, and
PostgreSQL cancellation is polled while awaiting headers and body chunks.

Chunk execution is bounded by three optional server settings:

| Option | Default | Range |
| --- | ---: | ---: |
| `max_concurrent_reads` | `4` | `1`–`32` |
| `max_inflight_bytes` | `269484036` | 1 MiB–1 GiB |
| `compressed_cache_bytes` | `67108864` | `0`–1 GiB; `0` disables caching |

Reads are prefetched in deterministic chunk order without background tasks.
S3 and HTTP(S) use the configured read concurrency; local stores use one
effective read at a time while retaining the same byte and cache limits.
The compressed cache belongs to one query execution, so bytes never cross
roles, credentials, server changes, or queries. A rescan within the same query
can reuse cached chunks.
For sharded arrays, that same byte budget is divided between decoded shard
indexes and encoded inner-payload ranges; sharding does not add a second
unbounded cache.

S3 inspection requires `s3:ListBucket` and `s3:GetObject`, while scanning
requires `s3:GetObject`. Local inspection and scans require the PostgreSQL
operating-system account to traverse the configured directory and read the
required metadata and chunk files. HTTP(S) supports exact-object scans,
including explicit OME multiscale selection, but not hierarchy listing;
`zarr_inspect` and `zarr_multiscales` therefore reject HTTP(S) servers before
making an object request.

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
group hierarchy and reads only v2 `.zgroup`, `.zarray`, and `.zattrs` objects or
v3 `zarr.json` objects; it does not read or decode chunk objects. Zarr v3 groups
must be explicit. A node that contains both v2 and v3 metadata is rejected
rather than interpreted using an arbitrary precedence rule.

The function returns these fields:

| Field | Meaning |
| --- | --- |
| `path` | Path relative to the configured Zarr root; `/` is the root node |
| `kind` | `group` or `array` |
| `group_path` | Parent group for a non-root node |
| `variable` | Array name; `NULL` for groups |
| `zarr_format` | Format version recorded by `.zgroup`, `.zarray`, or `zarr.json` |
| `shape`, `chunks` | Raw JSON arrays, preserving the metadata integer range |
| `dimensions` | Named dimensions from v2 xarray `_ARRAY_DIMENSIONS` or v3 `dimension_names` |
| `dtype` | Native v2 NumPy dtype string or v3 data-type identifier |
| `codecs` | Native v2 `{filters, compressor}` object or v3 ordered codec array |
| `fill_value` | Raw v2 or v3 fill value |
| `units`, `calendar` | Common scientific attributes when they are strings |
| `scale_factor`, `add_offset` | Finite numeric scientific attributes |
| `crs` | Best-effort CRS metadata from direct `crs`, `spatial_ref`, or `crs_wkt`, or from a resolved sibling `grid_mapping` reference |
| `attributes` | Complete v2 `.zattrs` object or v3 `attributes` object |
| `warnings` | Non-fatal metadata issues, such as malformed named dimensions |

The inspection surface exposes scientific metadata. Scans can opt into the
CF-style value and time-coordinate decoding described below; physical-unit and
CRS transformations are not applied yet. The complete `attributes` value remains
authoritative because scientific metadata conventions vary between datasets.

For CRS metadata, `zarr_inspect` keeps the raw node attributes in `attributes`
and fills `crs` as a convenience projection. Direct CRS metadata on
the current node wins in this order: `crs`, `spatial_ref`, then `crs_wkt`. If an
array has no direct CRS metadata but has a simple `grid_mapping` string, the
inspector attempts to resolve that name to a sibling array in the same group and
uses the sibling's direct CRS value. If that reference cannot be resolved, the
raw `grid_mapping` value remains visible in `crs` and a warning is emitted.
CRS strings, WKT, EPSG labels, and GeoTransform attributes remain visible as
metadata. Ordinary foreign-table scans do not assign SRIDs, transform
coordinates, or accept PostGIS predicates. The point-sampling function below
uses supported EPSG metadata from the selected array's resolved grid mapping.

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

Chunk indexes are generated lazily in C order. Their memory is proportional to
array rank rather than the number of selected chunks. PostgreSQL `LIMIT` can
therefore stop later chunk requests naturally, but it does not bypass metadata
reads or the bounded coordinate vectors required for projection and pruning.

`EXPLAIN ANALYZE` reports actual shape/chunk selection, request and byte counts,
cache activity, synthesized fill bytes, decoded cells, tuple counts, timings,
and aggregate mode. Plain `EXPLAIN` remains network-free, so runtime metadata is
not fabricated or fetched during planning. Chunk-statistic pruning is reported
as disabled until the separately validated statistics catalog is implemented.
The query-local EXPLAIN counters include work initiated before an early `LIMIT`.
Errors and cancellations clean up queued reads safely but do not return an
`EXPLAIN ANALYZE` plan. The older persistent `wrappers_fdw_stats` counters are
flushed only when the iterator reaches EOF; like other Wrappers FDWs, an executor
that stops early may not persist the final delta because SPI is not safe from
`EndForeignScan`.

The selected value array must have one unique, safe name for every array
dimension, in array order: v2 uses the xarray `_ARRAY_DIMENSIONS` attribute and
v3 uses native `dimension_names`. If a v3 array also carries the legacy xarray
attribute, the two declarations must match exactly. Each name must resolve to a
same-group, same-name coordinate array in the same Zarr format whose shape is
one dimensional and whose length matches the value-array extent. If a
coordinate array declares native or legacy dimension names, it must contain
only its own name. Missing or malformed dimension metadata fails instead of
falling back to an inferred `[y, x]` or `[time, y, x]` layout.

Dimension names are preserved for PostgreSQL column matching. Coordinate
metadata can classify dimensions as spatial X/Y, latitude/longitude, vertical,
time, band, channel, or unknown; roles do not rename dimensions. Recognized
`standard_name`, `axis`, and unambiguous units take precedence over conservative
name aliases. Incompatible recognized signals fail instead of being guessed.
Names such as `depth`, `height`, `altitude`, `level`, `lev`, and `z` are vertical
aliases, while band and channel remain distinct roles.

Supported v2 value mappings are `<f4` to `real`, `<f8` to `double precision`,
`|i1`/`<i1` to PostgreSQL internal `"char"`, `<i2` to `smallint`, `<i4` to
`integer`, and `<i8` to `bigint`. The corresponding supported v3 identifiers
are `float32`, `float64`, `int8`, `int16`, `int32`, and `int64`. A coordinate
classified as time must be declared `timestamptz`; every other currently
supported numeric coordinate must be `double precision`. Dimension columns do
not need to be projected. The wrapper reads coordinate metadata for every
dimension but downloads coordinate chunk values only for dimensions used by
the query target or restrictions.

Foreign tables can persist restrictions on any named numeric dimension with
`dimension_selectors`. The option is a JSON object whose keys are exact,
case-sensitive dimension names. Each dimension must use exactly one of these
forms:

- `{"index": n}` selects one zero-based physical index.
- `{"indices": [n, ...]}` selects a nonempty unique list of zero-based indexes.
- `{"index_range": {"start": n, "stop": n}}` selects the half-open physical
  index range `[start, stop)`.
- `{"value": x}` selects one raw numeric coordinate value.
- `{"values": [x, ...]}` selects a nonempty unique list of finite raw numeric
  coordinate values.
- `{"value_range": {"min": x, "max": x}}` selects the closed raw coordinate
  range `[min, max]`.

```sql
create foreign table temperature_850hpa (
  time timestamptz,
  y double precision,
  x double precision,
  temperature real
)
server public_zarr_server
options (
  array_group 'climate/temperature',
  time_from_attrs 'true',
  dimension_selectors '{"level":{"value":850},"ensemble":{"index":0}}'
);
```

Index selectors do not download coordinate values. Value selectors compare
exactly in the stored numeric coordinate domain and load only the required
coordinate vectors; they are not decoded as timestamps, strings, or CF-packed
values. List inputs are unordered API constraints, not output ordering hints:
rows are emitted in the array's native C order. Duplicate coordinate values
select every matching native index, while an unmatched value produces an empty
scan without data-chunk requests. Selector lists are bounded to 4096 members;
documents are limited to 64 KiB and 64 dimensions.

Selectors are always combined with SQL predicates using `AND`; they never
override a `WHERE` clause. String coordinates such as Sentinel band names
remain deferred. Spatial sampling and reduction functions require their
explicit selector-aware overloads to use selector-bearing tables. For those
spatial overloads, operation-owned X/Y dimensions, plus Time for by-time
operations, cannot be selected through `dimension_selectors`; every auxiliary
dimension must resolve to zero or one exact index.

### Zarr v3 subset

Zarr v3 arrays must use a regular chunk grid and a validated codec pipeline.
The supported order is an optional core `transpose` codec, exactly one core
`bytes` codec, at most one `gzip`, `blosc`, or `zstd` codec, and an optional core
`crc32c` codec. A transpose must declare one rank-matched permutation, gzip
levels range from 0 through 9, and CRC32C uses its four-byte little-endian
trailer. Codec stages cannot be duplicated or reordered. Multi-byte values
must declare little-endian byte order in the `bytes` codec.

The v3 Blosc codec accepts the compiled `blosclz`, `lz4`, and `lz4hc` cnames,
compression levels from 0 through 9, `noshuffle`, `shuffle`, or `bitshuffle`,
and a non-negative `blocksize`. A positive `typesize` is required for byte or
bit shuffle and optional for `noshuffle`. The spec-defined `zstd`, `snappy`,
and `zlib` Blosc cnames are rejected before any chunk read because they are not
enabled in this build. This is independent of the supported Zarr v3 `zstd`
codec.

The v3 Zstandard codec requires a compression `level` from -131072 through 22
and accepts the optional boolean `checksum` setting, which defaults to `false`.
Each object must contain
exactly one ordinary Zstandard frame. Its checksum flag must match the codec
metadata, and a present frame-content size must match the logical decoded chunk
size; an omitted content size is allowed. Frames advertising a dictionary ID
are rejected; decoding with dictionaries is unsupported. Skippable frames,
concatenated frames, and trailing bytes are also rejected. Decoder windows are
limited to 8 MiB in addition to the general decoded-chunk limit.

Decoding applies the declared stages in reverse, validates a CRC32C trailer
before decompressing the preceding gzip, Blosc, or Zstandard stream, and
restores transposed chunks to the executor's row-major representation. Blosc's
fixed header, declared compressed and uncompressed lengths, and trailing bytes
are validated before decompression. Zstandard frame checksums are verified by
the decoder when enabled. Encoded reads and every decoded intermediate are
bounded from the declared chunk shape; corrupt, truncated, or over-expanding
payloads fail instead of returning partial values.

The core `default` chunk-key encoding and the compatibility `v2` chunk-key
encoding are supported with their defined `.` or `/` separators; default
encoding uses keys beneath the `c` prefix. Missing chunks use the required v3
`fill_value` without running the codec pipeline and otherwise follow the same
scan, filter, CF decoding, aggregate-pushdown, resource-bound, and cancellation
behavior as v2 arrays.

A top-level `sharding_indexed` codec is also supported without outer wrapper
codecs. Its regular outer chunk grid defines shard objects, while its positive
inner `chunk_shape` must exactly divide every outer shard extent. Inner chunks
use the same bounded `transpose`/`bytes`/`gzip-or-blosc-or-zstd`/`crc32c`
subset described above.
The fixed-size shard index uses little-endian `uint64` offset/length pairs,
optionally followed by CRC32C, and may be stored at the start or end of the
shard. Whole-object absence and the required all-`uint64::MAX` index sentinel
both synthesize the array fill value. Inner payload order is not assumed; each
read follows the offset and length stored in the decoded index.

Sharded S3 scans read only the fixed index prefix or suffix and the exact byte
ranges for selected inner chunks; they never fall back to downloading a whole
shard. Decoded indexes and encoded inner ranges are cached only for the current
query and retain the existing concurrency, inflight-byte, cancellation, and
rescan rules. Range responses, index checksums, sentinels, offset arithmetic,
object bounds, and overlap with the index region are validated before decode.
`EXPLAIN ANALYZE` distinguishes the outer shard and inner logical chunk shapes,
index location, index and payload range requests, bytes, and cache activity.

This subset intentionally excludes outer codecs around sharding, nested
sharding, variable-size index codecs, full-shard fallback, range coalescing,
storage transformers, consolidated metadata, big-endian bytes, alternate
compressor/CRC32C order, bytes-to-bytes codecs other than gzip, Blosc, or
Zstandard, and extension data types.
Unsupported metadata fails explicitly instead of being ignored.
Unknown top-level Zarr 3.1 extensions are accepted only when their object is
explicitly marked `must_understand: false`; shorthand extension definitions are
outside this bounded subset.

Finite monotonic coordinate values enable conservative chunk pruning. Finite
unordered coordinates remain projectable and filterable, but pruning is skipped
for that axis and PostgreSQL rechecks the original predicate. Integer coordinate
values must convert to `double precision` exactly; values that would lose
identity fail explicitly.

### OME-Zarr 0.5 multiscales

Use `zarr_multiscales` to discover OME-Zarr 0.5 resolution levels before
creating a foreign table:

```sql
select group_path,
       multiscale_index,
       multiscale_name,
       level_index,
       array_path,
       axes,
       shape,
       chunks,
       dtype,
       scale,
       translation,
       supported,
       warnings
from zarr_multiscales('public_zarr_server')
order by group_path, multiscale_index, level_index;
```

The function verifies `USAGE` on the foreign server, traverses only bounded
metadata, and returns one row per declared level. It does not read chunk data.
Malformed OME metadata fails explicitly. A valid level outside the execution
subset remains discoverable with `supported = false` and an explanation in
`warnings`. Capability checks include the level rank and axes, native dtype,
codec and storage layout, synthesized-coordinate limits, and finite affine
endpoints. Exceeding the bounded derived discovery-output budget fails the
request explicitly instead of returning partial rows.

Select a level explicitly with all three multiscale table options:

```sql
create foreign table image_level_1 (
  y double precision,
  x double precision,
  intensity real
)
server public_zarr_server
options (
  multiscale_group 'image',
  multiscale_index '0',
  multiscale_level '1'
);
```

`multiscale_index` and `multiscale_level` are zero-based indexes into the
declared OME arrays. The three options are required together and cannot be
combined with `array_group`. The wrapper never chooses an image, multiscale,
or resolution level implicitly. Plain `EXPLAIN` remains metadata-I/O free.

Execution currently supports OME metadata version exactly `0.5` on Zarr v3,
with rank-2 axes exactly `y`, `x`, both declared as spatial axes. Every level's
native `dimension_names` must match those axes. A dataset transform must contain
one inline positive finite scale and may contain one following inline finite
translation. The multiscale may declare the same transform sequence, which is
applied after the dataset transform. For dataset scale/translation `sd`, `td`
and multiscale scale/translation `sg`, `tg`, effective coordinates are:

```text
scale       = sg * sd
translation = sg * td + tg
coordinate  = scale * array_index + translation
```

The operations are component-wise and do not add a half-pixel offset. The
synthesized coordinate vectors use the same bounds, pruning, aggregate,
allocation, arithmetic, and cancellation safeguards as stored coordinates.
The selected value array can use any numeric dtype, direct or sharded layout,
and codec pipeline from the supported Zarr v3 subset above.

This is a bounded reader subset of the
[OME-Zarr 0.5 specification](https://ngff.openmicroscopy.org/0.5/). It does not
implement automatic resolution selection, rank-3 through rank-5 execution,
time/channel/depth slicing, path-backed or general affine transforms,
resampling, labels, OMERO display metadata, plates, wells, series, OME-XML,
CRS inference, or writes.

## Sample a spatial point

`zarr_sample` performs a read-only point lookup on a rank-2 rectilinear array.
PostGIS is optional for ordinary Zarr inspection and scans, but must be installed
to construct or transform the EWKB point used by this function:

```sql
create schema if not exists gis;
create extension if not exists postgis with schema gis;

create foreign table spatial_temperature (
  y double precision,
  x double precision,
  temperature real
)
server public_zarr_server
options (
  array_group 'climate/spatial_temperature'
);

select *
from zarr_sample(
  foreign_table => 'public.spatial_temperature',
  point_ewkb    => gis.ST_AsEWKB(
                     gis.ST_SetSRID(gis.ST_MakePoint(110, 20), 3857)
                   ),
  method        => 'nearest'
);
```

The function signature is:

```sql
zarr_sample(
  foreign_table text,
  point_ewkb bytea,
  method text default 'nearest'
)
returns table (
  x double precision,
  y double precision,
  value double precision,
  x_index bigint,
  y_index bigint,
  coordinate_distance double precision,
  srid integer
)
```

Selector-bearing tables must use the explicit selector-aware overload:

```sql
zarr_sample(
  foreign_table text,
  point_ewkb bytea,
  method text,
  dimension_selectors text
)
```

The `dimension_selectors` argument uses the same JSON grammar as the foreign
table option and has no default. Pass `'{}'` when only the table option should
apply. The point lookup owns the horizontal X/Y dimensions, so selectors may
target auxiliary dimensions only. Every auxiliary dimension must resolve to
zero or one exact native index; zero returns no sample row, while more than one
index fails clearly.

Use a schema-qualified foreign-table name. The caller must have `SELECT` on the
foreign table and `USAGE` on its foreign server. The table must select one Zarr
value array with exactly two discovered, one-dimensional horizontal coordinate
axes and a supported, unambiguous EPSG CRS. The legacy signature requires a
rank-2 array. The selector-aware overload also accepts auxiliary dimensions
when each resolves to zero or one exact native index. EWKB must contain a
nonzero SRID; the point is transformed into the array CRS before coordinate
lookup.

`nearest` independently selects the closest stored x and y coordinate. A tie is
resolved to the lower logical array index, including on descending axes.
`exact` returns a sample only when the transformed point exactly matches both
stored coordinate values. `coordinate_distance` is zero for an exact match and
otherwise is Euclidean distance in the array CRS units; it is not a geodesic
distance. Neither method interpolates values. Point lookup currently supports
rectilinear cell centers only, not curvilinear coordinates, cell footprints, or
rotated affine grids.

The scalar result is widened to `double precision`. The function uses the
foreign table's scientific decoding options, so `decode_cf`, fill/missing
values, valid ranges, and scale/offset have the same meaning as an ordinary
scan. A scientifically missing value is returned as SQL `NULL`.

## Select polygon cells and calculate zonal statistics

`zarr_cells` returns the cell centers covered by a PostGIS Polygon or
MultiPolygon, while `zarr_zonal_stats` reduces the same selected values inside
the Zarr executor:

```sql
with region as (
  select gis.ST_AsEWKB(
           gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
         ) as ewkb
)
select cells.*
from region
cross join lateral zarr_cells(
  foreign_table => 'public.spatial_temperature',
  region_ewkb   => region.ewkb
) as cells
order by cells.y_index, cells.x_index;

with region as (
  select gis.ST_AsEWKB(
           gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
         ) as ewkb
)
select stats.*
from region
cross join lateral zarr_zonal_stats(
  foreign_table => 'public.spatial_temperature',
  region_ewkb   => region.ewkb
) as stats;
```

The function signatures are:

```sql
zarr_cells(foreign_table text, region_ewkb bytea)
returns table (
  x double precision,
  y double precision,
  value double precision,
  x_index bigint,
  y_index bigint,
  srid integer
);

zarr_zonal_stats(foreign_table text, region_ewkb bytea)
returns table (
  count bigint,
  valid_count bigint,
  min double precision,
  max double precision,
  sum double precision,
  avg double precision,
  srid integer
);
```

Selector-bearing tables must use the explicit selector-aware reduction
overload:

```sql
zarr_zonal_stats(
  foreign_table text,
  region_ewkb bytea,
  dimension_selectors text
)
```

The polygon owns horizontal X/Y selection. Table and call selectors intersect
for auxiliary dimensions only. Pass `'{}'` to opt into table selectors without
adding call selectors. If an auxiliary dimension resolves to no exact index,
the function returns one empty statistics row; if it resolves to multiple
indexes, the function fails rather than aggregating unlabeled slices.

Both legacy functions apply the same foreign-table privilege,
rectilinear-grid, CRS, rank-2, and scientific-decoding rules as the legacy
`zarr_sample` signature. The selector-aware `zarr_zonal_stats` overload also
accepts auxiliary dimensions when each resolves to zero or one exact native
index; `zarr_cells` remains rank-2 and has no selector overload. Region EWKB
must contain a valid, non-empty, two-dimensional Polygon or MultiPolygon with a
positive SRID. The geometry is transformed into the array CRS, and its envelope
is used only for conservative coordinate and chunk pruning.

Exact inclusion uses PostGIS `ST_Covers(region, cell_center)` semantics. A cell
center on the polygon boundary is therefore included. The returned cells are
center samples, not pixel footprints, and no partial-cell area weighting is
performed. Function output order is unspecified; add an `ORDER BY` when stable
ordering is required.

`count` is the number of covered logical cells, including cells whose decoded
value is SQL `NULL`. `valid_count`, `min`, `max`, `sum`, and `avg` ignore decoded
NULL values. With no valid values, `valid_count` is zero and the numeric
aggregates are NULL. Values and aggregates are widened to `double precision`.

### Add a time range to polygon queries

For an array with one discovered time dimension, use
`zarr_cells_by_time` to return covered cells at each stored timestamp, or
`zarr_zonal_stats_by_time` to return one aggregate row per logical time index:

```sql
create foreign table spatial_temperature_by_time (
  time timestamptz,
  y double precision,
  x double precision,
  temperature real
)
server public_zarr_server
options (
  array_group 'climate/spatial_temperature_by_time',
  time_from_attrs 'true'
);

with region as (
  select gis.ST_AsEWKB(
           gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
         ) as ewkb
)
select stats.*
from region
cross join lateral zarr_zonal_stats_by_time(
  foreign_table => 'public.spatial_temperature_by_time',
  region_ewkb   => region.ewkb,
  start_time    => timestamptz '2025-01-01 00:00:00+00',
  end_time      => timestamptz '2025-01-02 00:00:00+00'
) as stats
order by stats.time_index;
```

The function signatures are:

```sql
zarr_cells_by_time(
  foreign_table text,
  region_ewkb bytea,
  start_time timestamptz,
  end_time timestamptz
)
returns table (
  time timestamptz,
  x double precision,
  y double precision,
  value double precision,
  time_index bigint,
  x_index bigint,
  y_index bigint,
  srid integer
);

zarr_zonal_stats_by_time(
  foreign_table text,
  region_ewkb bytea,
  start_time timestamptz,
  end_time timestamptz
)
returns table (
  time timestamptz,
  time_index bigint,
  count bigint,
  valid_count bigint,
  min double precision,
  max double precision,
  sum double precision,
  avg double precision,
  srid integer
);
```

`zarr_zonal_stats_by_time` also has a selector-aware overload:

```sql
zarr_zonal_stats_by_time(
  foreign_table text,
  region_ewkb bytea,
  start_time timestamptz,
  end_time timestamptz,
  dimension_selectors text
)
```

The polygon owns X/Y and the time range owns the Time dimension; selectors may
target only other auxiliary dimensions. Pass `'{}'` to opt into selector-aware
execution with only table selectors. A non-overlapping time range returns no
rows. A spatial or auxiliary empty selection with matching time indexes returns
one empty statistics row per selected time index.

Time bounds are required and form a half-open range: `start_time` is included
and `end_time` is excluded. The FDW uses the same manual or attribute-derived
time conversion as an ordinary scan. It discovers Time, X, and Y roles rather
than relying on dimension names or positions, and supports any array-axis order.
Additional dimensions are accepted only when their extent is one; a
non-singleton band, level, channel, or unknown dimension must resolve to zero
or one exact index through the selector-aware overload and is otherwise
rejected.

Unordered time coordinates are scanned conservatively and checked exactly, so
matching timestamps are not pruned incorrectly. Duplicate stored timestamps
remain distinct rows through `time_index`. Zonal output contains one row for
each selected logical time index; a slice with no covered or valid values has
zero counts and NULL numeric aggregates. If no stored timestamp falls in the
requested range, both functions return no rows.

The spatial-time candidate window is limited to 10,000,000 logical cells,
`zarr_cells_by_time` returns at most 1,000,000 rows, and at most 1,000,000 time
slices may be selected. These functions preserve the same PostGIS boundary,
CRS, privilege, scientific-decoding, cache, and cancellation rules as the
rank-2 polygon functions.

## Decode time coordinates from attributes

By default, raw values from the one coordinate classified as time are
interpreted from the manual table options
`time_unit` and `time_origin`, or as `seconds` since the Unix epoch when those
options are omitted.

Set `time_from_attrs 'true'` to derive the time conversion from the sibling
coordinate's attributes instead:

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
requires exactly one coordinate classified as time whose attributes contain:

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
and packed-value attributes from the selected value array:

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
domain. A missing Zarr chunk is first materialized with the array's `fill_value`;
it becomes SQL `NULL` only when that raw value also matches the
scientific missing/validity metadata. The option defaults to `false`, which
preserves the raw dtype mappings above.

For floating-point arrays, the Zarr JSON spellings `"NaN"`, `"Infinity"`, and
`"-Infinity"` are accepted as missing sentinels. Declaring `"NaN"` masks every
NaN payload; an undeclared non-finite value remains a PostgreSQL non-finite
`double precision` value.

This value-decoding mode is independent from `time_from_attrs`. It does not
convert physical units, transform a CRS, or apply packing attributes to
coordinate arrays.

## Aggregate pushdown

The wrapper reduces ungrouped `count`, `sum`, `avg`, `min`, and `max` queries
inside the Zarr chunk scan and returns one result row to PostgreSQL. This avoids
creating one PostgreSQL tuple for every selected array cell:

```sql
select count(*) as selected_cells,
       count(temperature) as valid_cells,
       min(temperature),
       max(temperature),
       sum(temperature),
       avg(temperature)
from decoded_temperature
where time >= timestamptz '2025-01-01 00:00:00+00'
  and time <  timestamptz '2025-02-01 00:00:00+00'
  and y between 30 and 31
  and x in (-8.0, -7.5, -7.0);
```

Chunk ranges are still selected conservatively, but aggregate mode evaluates
each accepted predicate exactly before updating the reducer. This preserves
strict inequalities, non-contiguous `IN` membership, unordered coordinates,
value-column predicates, missing chunks, edge chunks, and decoded NULL
semantics. `count(*)` includes matching logical cells whose value is NULL;
`count(column)`, `sum`, `avg`, `min`, and `max` ignore NULL. Non-count
aggregates return NULL for an empty or all-NULL selection.

Pushdown currently applies only to scalar aggregates over plain columns. A
query with `GROUP BY`, `DISTINCT`, an aggregate `FILTER` or `HAVING` clause, an
aggregate expression such as `sum(temperature + 1)`, or a predicate the wrapper
cannot evaluate exactly remains a normal foreign scan with PostgreSQL doing
the aggregation. Use `EXPLAIN` to confirm whether a query is represented by a
single Foreign Scan or retains a local Aggregate node.

## Current limitations

- Read-only Zarr v2 and the core Zarr v3 subset described above on
  S3-compatible storage, trusted anonymous HTTP(S) object stores, or a secured
  local filesystem directory. Authenticated HTTP, redirects, proxies, custom
  certificate authorities, mutual TLS, WebDAV, GCS, Azure, and SSH filesystem
  URLs are not supported. Plain HTTP requires explicit opt-in and provides no
  transport confidentiality or integrity.
- Scans support one value array with rank 1 through 64 and mandatory v2
  `_ARRAY_DIMENSIONS` or v3 `dimension_names`; scalar arrays remain unsupported.
- Ordinary arrays require a same-group, same-name, rank-1 numeric coordinate
  array for every dimension. Explicitly selected supported OME-Zarr 0.5
  rank-2 levels instead synthesize `y` and `x` from their scale/translation
  metadata. Other synthesized ordinal coordinates, auxiliary or cross-group
  coordinates, curvilinear/multidimensional coordinates, and string or
  categorical band/channel coordinates are not supported. `dimension_selectors`
  accept only numeric coordinate values and zero-based physical indexes; string
  and categorical selector values are not supported.
- Coordinate packing, masks, valid ranges, and scale/offset are not decoded. If
  a coordinate used by a query declares those attributes, the scan fails rather
  than silently ignoring them.
- One temporal dimension is supported. Multiple temporal dimensions and
  per-axis calendars are not supported.
- A foreign-table scan still represents one value array and at most one queried
  non-dimension value column. Multi-variable scans and functional `bands`
  execution are not supported.
- Spatial functions are limited to rank-2 rectilinear arrays with one discovered
  horizontal x/longitude axis and one y/latitude axis. Polygon operations use
  center coverage only. They do not implement cell-footprint or area-weighted
  statistics, geographic distance, interpolation, curvilinear coordinates,
  rotated grids, or topology repair.
- Aggregate pushdown is limited to ungrouped `count`, `sum`, `avg`, `min`, and
  `max` over plain columns. Grouped, distinct, filtered, ordered, expression,
  and user-defined aggregates are computed by PostgreSQL.
- Raw, gzip, zlib, and Blosc/LZ4 chunk compression is supported for v2. The v3
  subset supports the ordered `transpose`, `bytes`, `gzip`, bounded Blosc, or
  bounded Zstandard, and `crc32c` pipeline described above.
- Non-empty v2 filters, Fortran order, consolidated metadata, storage
  transformers, writes, and OME-Zarr semantics outside the bounded 0.5
  multiscale subset above are not supported.
- `LIMIT` alone does not prevent coordinate metadata loading; use selective
  coordinate predicates for large arrays. It does stop later lazy data-chunk
  reads after PostgreSQL has accepted enough rows.
- Scan execution limits each loaded coordinate and all loaded coordinates
  together to 16,777,216 values. Chunk-index iteration is O(rank); decoded
  chunks retain their 256 MiB limit and storage concurrency/cache use the
  server byte limits above. These are safety bounds, not Zarr format limits.
- Inspection has hard depth, node, list-page, object-size, and total metadata
  limits and fails explicitly rather than returning a truncated hierarchy.
  HTTP(S) stores cannot be inspected because this backend deliberately has no
  directory-listing protocol.
