# zarr_fdw — Specification

Postgres Foreign Data Wrapper for querying cloud-native Zarr arrays (e.g. Sentinel-2 cubes)
with spatial/temporal predicate pushdown, without materializing the cube.

Status: draft v0.1
Author: MPSY
Scope: read-only MVP → PostGIS-integrated v1 → write/virtual-store v2

---

## 1. Problem statement

A Zarr cube (e.g. a year of Sentinel-2 reflectance, dims `[time, band, y, x]`) lives in
S3/GCS/Azure/SSH-mounted storage. Users want to query it with SQL: filter by AOI geometry
and date range, compute band math (NDVI/EVI/...), and get back a table
(`geometry | date | ndvi | evi | ...`) they can join against other PostGIS layers.

No existing Postgres extension does this. The closest prior art is `duckdb_zarr`
(DuckDB, not Postgres) which proves the mechanism — metadata-driven chunk pruning +
pushed filters — is sound. This spec adapts that mechanism to Postgres via the FDW API.

## 2. Naming

Recommended: **`zarr_fdw`**
- Matches the naming convention already used by `postgres_fdw`, `file_fdw`, and every
  wrapper in `supabase/wrappers` (`s3_fdw`, `stripe_fdw`, ...) — discoverable, no guessing.
- If distributed standalone (not upstreamed into `wrappers`), a distinct product name
  works too, e.g. **CubeLink** or **ZarrGate** — but `zarr_fdw` is what people will
  search for, so keep it as the crate/extension name regardless of marketing name.

## 3. Where this lives: PR vs. from scratch

| Path | What it means | Effort | Tradeoff |
|---|---|---|---|
| **PR into `supabase/wrappers`** | Add `wrappers/src/fdw/zarr_fdw/`, implement `ForeignDataWrapper` trait | Lowest — pushdown (WHERE/ORDER BY/LIMIT), type mapping, connection handling, packaging/release are already solved by the framework | You inherit their release cadence, review process, and Rust/pgrx-only constraint |
| **Standalone Postgres extension (C or Rust/pgrx)** | New repo, own FDW handler from scratch | Highest — you re-solve packaging, pushdown plumbing, CI | Full control, your own name/brand, no upstream review dependency |
| **Standalone Multicorn2 wrapper** | Python package, `multicorn.ForeignDataWrapper` subclass | Lowest of all for a *prototype* — zarr-python/xarray/fsspec do the hard parts | Multicorn calls into Python per-row; fine for prototyping and moderate workloads, not the ceiling you want long-term |

**Recommendation:** build the MVP as a **Multicorn2 wrapper** first (own repo, fast to
prove out the design and get real usage), then port the validated design into a PR
against `supabase/wrappers` once the chunk-pushdown logic and PostGIS integration are
settled. Porting a proven design is far cheaper than debugging FDW planner-hook
semantics in Rust while the design is still moving.

## 4. Language decision

| | Rust (`pgrx` / `supabase/wrappers`) | Python (Multicorn2) | Native C |
|---|---|---|---|
| Zarr spec parsing (v2 `.zarray`/v3 `zarr.json`) | Write it yourself, or shell out to a crate (`zarrs` crate exists) | `zarr-python` does this for you | Write it yourself |
| Codec support (blosc, zstd, gzip, delta) | Via `zarrs`/`numcodecs`-equivalent crates, still integration work | `numcodecs` — done | Manual, largest lift |
| Cloud auth (S3/GCS/Azure sig v4, token refresh) | Crates exist (`aws-sdk-s3`, etc.) but you wire it up | `s3fs`/`gcsfs`/`fsspec` — done | Manual |
| Per-row overhead | Near-zero | Python call overhead per row (real, but chunk-level batching mitigates it) | Near-zero |
| Pushdown plumbing (WHERE/LIMIT translation) | Already built in `supabase/wrappers` | You implement `can_pushdown_upqual` yourself in Multicorn | Manual |
| Time to a working prototype | Weeks | Days | Months |

**Verdict:** Python/Multicorn2 for v0 — the entire Zarr ecosystem (spec parsing, codecs,
cloud filesystem auth, CRS/CF-convention handling) is already mature there, so you're
only building the genuinely new part: the chunk-index-from-predicate translation and the
row-materialization loop. Rewrite the scan path in Rust later only if profiling shows the
Python call overhead actually matters at your query volumes — for AOI-scoped queries
over a handful to a few hundred chunks, it likely won't.

Native C/SQL only: not recommended. The performance ceiling is highest but you'd be
reimplementing Zarr spec parsing and cloud auth by hand for a benefit that mostly matters
at chunk-fetch scale, not per-row scale — and chunk fetch is I/O-bound (network), not
CPU-bound, so the language doing the fetching barely changes the wall-clock time anyway.

## 5. Architecture

```
                     ┌─────────────────────────────┐
 SQL query  ────────▶│  Postgres planner            │
 (WHERE geom &&,     │  GetForeignRelSize            │
  date BETWEEN)       │  GetForeignPaths              │
                     └───────────────┬───────────────┘
                                     │ predicate info (bbox, time range, columns)
                                     ▼
                     ┌─────────────────────────────┐
                     │  zarr_fdw planner hook        │
                     │  1. read cached array metadata│  (.zarray / zarr.json, consolidated)
                     │  2. translate predicates       │  bbox/time -> chunk index range
                     │  3. estimate row count/cost    │
                     └───────────────┬───────────────┘
                                     │ chunk key list
                                     ▼
                     ┌─────────────────────────────┐
                     │  BeginForeignScan/IterateForeignScan│
                     │  parallel range/GetObject fetch │  (S3/GCS/Azure/local/sshfs)
                     │  decompress chunk (blosc/zstd)  │
                     │  mask by exact geometry (if any)│
                     │  yield rows                     │
                     └───────────────┬───────────────┘
                                     ▼
                              rows to Postgres
                        (joinable/indexable like any table)
```

Key design point: metadata read happens **once per planning**, not once per row. The
metadata document is small (bytes to low KB even with consolidated metadata for a
multi-variable store), so this cost is negligible compared to chunk fetches.

## 6. DDL surface

```sql
CREATE EXTENSION zarr_fdw;

CREATE SERVER sentinel2_2025 FOREIGN DATA WRAPPER zarr_fdw
  OPTIONS (
    store_url      's3://my-bucket/sentinel2/2025.zarr',
    region         'eu-west-1',
    -- credentials via a Postgres USER MAPPING, not inline options
    consolidated   'true'
  );

CREATE USER MAPPING FOR CURRENT_USER SERVER sentinel2_2025
  OPTIONS (aws_access_key_id '...', aws_secret_access_key '...');

CREATE FOREIGN TABLE sentinel2_cells (
    x       double precision,
    y       double precision,
    time    timestamptz,
    b04     real,          -- red
    b08     real,          -- nir
    geom    geometry(Point, 4326)   -- computed column, see §7
)
SERVER sentinel2_2025
OPTIONS (
    array_group '/reflectance',
    bands       'B04,B08',
    crs         'EPSG:32630'   -- fallback if not present in Zarr attrs/GeoZarr metadata
);
```

Design notes:
- Credentials go through `CREATE USER MAPPING`, matching Postgres FDW convention — never
  inline them in `OPTIONS` on the foreign table itself.
- `bands`/`array_group` options scope which data variables become columns, so a
  multi-band cube doesn't force you to materialize every band on every query.
- `crs` option is a fallback; prefer reading CRS from Zarr attrs following the emerging
  **GeoZarr** convention when present, since that's the direction the ecosystem is
  standardizing on for geospatial Zarr metadata.

## 7. PostGIS integration

Three pieces, in increasing sophistication:

**7.1 — Computed geometry column.** The FDW builds a `geometry(Point, 4326)` per row
from the array's `x`/`y` coordinate arrays (or `lat`/`lon`) and CRS, reprojecting to 4326
via PROJ bindings if the source CRS differs. This makes the foreign table's output
directly usable with any PostGIS function downstream (`ST_Distance`, `ST_Within`, etc.)
once it's rows.

**7.2 — Predicate pushdown for spatial filters.** The planner hook inspects the query
for `ST_Intersects(geom, :aoi)` / `geom && :aoi` clauses, extracts the AOI's bounding box,
and — same principle as the date-range translation — converts it into a chunk index
range before any fetch happens. Exact-geometry masking (for non-rectangular AOIs) still
happens in `IterateForeignScan` after decompression, since a chunk can't be partially
decompressed.

**7.3 — Chunk-extent catalog table (the part that lets PostGIS's own index help you).**
FDWs can't carry a GiST index themselves. So on `CREATE FOREIGN TABLE`, `zarr_fdw`
materializes a small companion table:

```sql
-- maintained automatically by zarr_fdw, not hand-edited
CREATE TABLE sentinel2_cells_chunk_index (
    chunk_key   text PRIMARY KEY,   -- e.g. '3.14.22' (v2) or 'c/3/14/22' (v3)
    time_start  timestamptz,
    time_end    timestamptz,
    extent      geometry(Polygon, 4326)
);
CREATE INDEX ON sentinel2_cells_chunk_index USING GIST (extent);
```

Queries can join against this table explicitly for very cheap "which chunks touch my
AOI" checks using Postgres's native GiST index — faster than re-deriving the bbox math
every planning cycle — and the planner hook can also consult it directly. This is the
one piece of state `zarr_fdw` needs to persist in Postgres proper; everything else stays
lazy/stateless against the object store.

## 8. Functionality roadmap

**MVP (v0)**
- Read-only, single Zarr array (one `array_group`), S3 backend
- WHERE pushdown on bbox (`&&`, `ST_Intersects`) and time range
- Flat row output: `x, y, time, <one band per configured column>`
- Multicorn2, no PostGIS dependency required to run (geometry column optional)

**v1**
- Multi-variable/band support with column pruning (only fetch bands actually selected)
- Computed `geom` column + CRS handling (§7.1)
- Chunk-extent catalog table + GiST pushdown (§7.3)
- GCS and Azure backends (already covered if built on `fsspec`)
- sshfs/local path backend

**v2**
- Sharded Zarr v3 stores (shard-internal byte-range reads)
- Virtual Zarr / kerchunk references (query legacy NetCDF/HDF5 through the same interface)
- In-wrapper aggregation (zonal mean/sum per polygon, mirroring `xvec.zonal_stats`,
  so heavy reduction doesn't have to round-trip full pixel rows through Postgres)
- Write support (rare need, low priority unless a concrete use case shows up)

## 9. Example end-to-end query (the target use case)

```sql
SELECT
    p.parcel_id,
    c.time::date AS obs_date,
    (c.b08 - c.b04) / NULLIF(c.b08 + c.b04, 0) AS ndvi
FROM parcels p
JOIN sentinel2_cells c
  ON ST_Intersects(c.geom, p.geom)
WHERE c.time BETWEEN '2025-04-01' AND '2025-09-30'
ORDER BY p.parcel_id, obs_date;
```

The bbox from `p.geom` (via the join) and the `time BETWEEN` clause both push down to
chunk selection before any S3 GET happens; `ndvi` is computed in plain SQL over whatever
rows come back, no special support needed for band math itself.

## 10. Open risks / questions

- **Chunk/geometry mismatch.** Whole chunks are always fetched even for irregular
  polygons — masking happens post-decompression. Fine for correctness, just don't expect
  fetch volume to shrink below "bounding box of geometry."
- **Long-running scans and credential expiry.** STS/session tokens can expire mid-scan
  on large date-range queries; the fetch layer needs refresh-on-401, not just fail.
- **Consolidated metadata staleness.** If the store is actively being appended to
  (e.g. new Sentinel-2 acquisitions land daily), cached consolidated metadata can go
  stale — need a TTL or explicit `REFRESH FOREIGN TABLE`-style invalidation hook.
- **CRS/geo-metadata isn't fully standardized yet.** Plain Zarr has no required
  geospatial convention; GeoZarr is emerging but not universal, so `zarr_fdw` needs a
  fallback path (explicit `crs`/`x_dim`/`y_dim` options) for cubes that don't self-describe.