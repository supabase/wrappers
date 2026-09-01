//! Main `zarr_fdw` implementation.
//!
//! Given a query plan's pushed-down quals (WHERE) and target columns, this FDW
//! translates them into a lazy *chunk fetch stream* against S3, decompresses the
//! chunks and streams flat rows back to Postgres. Data model (MVP):
//!
//! - a single normalized named-dimension Zarr v2 or direct v3 array in C order,
//! - one same-group, same-name 1D numeric coordinate array per dimension,
//! - flat row output where every non-dimension target column receives the
//!   selected array's scalar value.
//!
//! Pushdown: a qual on any finite monotonic coordinate is converted into an
//! index range over that dimension's coordinate vector, which prunes the chunk
//! list before any data chunk is fetched. A time qual is interpreted via either the
//! `time_unit`/`time_origin` table options or, when `time_from_attrs` is true,
//! the discovered time coordinate's CF `units`/`calendar` attributes.
//!
//! Spatial PostGIS predicates (`ST_Intersects`, `geom && box`) do *not* reach
//! this code as `Qual`s — the framework only extracts simple Var-op-Const
//! expressions — so strict geometry pushdown is deferred to v1 (chunk-extent
//! catalog table); the MVP prunes on the `x`/`y`/`time` columns directly.

use crate::stats;
use futures_util::FutureExt;
use pgrx::datum::TimestampWithTimeZone;
use pgrx::pg_sys;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use supabase_wrappers::prelude::*;

use super::aggregate::{
    AggregateReducer, aggregate_signature_supported, qual_matches, qual_shape_supported,
};
use super::cache::{CachedObject, CompressedChunkCache};
use super::chunk::{ChunkIndexCursor, IndexBounds, chunk_key};
use super::codec::{CodecDecode, CodecPipeline};
use super::dataset::{
    CoordinateSource, Dataset, DimensionRole, named_array_dataset, named_dimensions,
    ome_rank2_dataset,
};
use super::decode::{
    DType, coord_bytes_to_f64, coord_fill_value_to_f64, coordinate_itemsize, fill_value_bytes,
};
use super::meta::{
    ArrayMeta, ArrayNode, NodeMeta, ZarrFormat, parse_v2_array, parse_v2_group, parse_v3_node,
};
use super::metrics::{ReadKind, ZarrExplainContext, ZarrScanMetrics};
use super::ome::{
    ResolvedOmeLevel, canonical_ome_group_path, resolve_ome_05_level,
    validate_optional_ome_05_attributes,
};
use super::prefetch::{
    OrderedPrefetch, PrefetchNext, PrefetchRequest, PrefetchSource, ScheduleError,
};
use super::scan_plan::{CoordinateRange, ScanPlan, ScanPlanner};
use super::scientific::{ScientificValueDecoder, time::TimeSpec};
use super::selection::Selection;
use super::selectors::{BoundDimensionSelectors, DimensionSelectors, OPT_DIMENSION_SELECTORS};
use super::sharding::{
    CachedShardIndex, MAX_SHARD_INDEX_BYTES, ShardIndex, ShardIndexCache, ShardIndexDecode,
    ShardingConfig, StorageLayout,
};
use super::spatial::crs::{
    GridMappingMetadata, ResolvedCrs, grid_mapping_sibling_path, resolve_crs,
};
use super::spatial::grid::{
    HorizontalAxes, HorizontalCell, RectilinearGrid, discover_horizontal_axes_from_roles,
    exact_center_index, inclusive_center_bounds, nearest_center_index,
};
use super::store::{
    MAX_METADATA_OBJECT_BYTES, RangedObject, ReadIdentity, ReadRange, ZarrStore, join_key,
    validate_store_definition_privilege, validate_store_options,
};
use super::{ZarrFdwError, ZarrFdwResult};

const FDW_NAME: &str = "ZarrFdw";

// Table option names.
const OPT_ARRAY_GROUP: &str = "array_group";
const OPT_MULTISCALE_GROUP: &str = "multiscale_group";
const OPT_MULTISCALE_INDEX: &str = "multiscale_index";
const OPT_MULTISCALE_LEVEL: &str = "multiscale_level";
const OPT_BANDS: &str = "bands";
const OPT_TIME_UNIT: &str = "time_unit";
const OPT_TIME_ORIGIN: &str = "time_origin";
const OPT_TIME_FROM_ATTRS: &str = "time_from_attrs";
const OPT_DECODE_CF: &str = "decode_cf";
const OPT_MAX_CONCURRENT_READS: &str = "max_concurrent_reads";
const OPT_MAX_INFLIGHT_BYTES: &str = "max_inflight_bytes";
const OPT_COMPRESSED_CACHE_BYTES: &str = "compressed_cache_bytes";

const DEFAULT_MAX_CONCURRENT_READS: usize = 4;
const MAX_CONCURRENT_READS: usize = 32;
// One maximum-size decoded chunk, gzip/zlib's bounded framing allowance, and
// the optional v3 CRC32C trailer must all fit under the default request budget.
const DEFAULT_MAX_INFLIGHT_BYTES: usize = 257 * 1024 * 1024 + 4;
const MIN_MAX_INFLIGHT_BYTES: usize = 1024 * 1024;
const MAX_MAX_INFLIGHT_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_COMPRESSED_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MAX_COMPRESSED_CACHE_BYTES: usize = 1024 * 1024 * 1024;
const MAX_COMPRESSED_CACHE_ENTRIES: usize = 4096;
const SHARD_INDEX_CACHE_FRACTION: usize = 4;
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(25);

// Planning must stay deterministic and network-free. Until metadata-backed or
// configured estimates are available, use a deliberately non-zero cardinality
// for remote arrays so PostgreSQL does not price every scan at startup cost.
const DEFAULT_PLANNER_ROWS: i64 = 1_000_000;
const DEFAULT_EMPTY_PROJECTION_WIDTH: i32 = 8;
const DEFAULT_UNKNOWN_TYPE_WIDTH: i32 = 32;

// The executor decodes one data chunk and every required coordinate vector in
// a PostgreSQL backend. Chunk coordinates themselves are streamed lazily.
// Keep the remaining remote-metadata-driven allocations bounded.
const MAX_DECODED_CHUNK_BYTES: usize = 256 * 1024 * 1024;
const MAX_COORDINATE_VALUES: usize = 16 * 1024 * 1024;
const MAX_TOTAL_COORDINATE_VALUES: usize = MAX_COORDINATE_VALUES;
const SPATIAL_TIME_INTERRUPT_POLL_VALUES: usize = 1_024;
const SELECTOR_INTERRUPT_POLL_CELLS: usize = 1_024;
const UNSUPPORTED_COORDINATE_DECODING_ATTRIBUTES: [&str; 7] = [
    "_FillValue",
    "missing_value",
    "valid_range",
    "valid_min",
    "valid_max",
    "scale_factor",
    "add_offset",
];

#[derive(Clone, Debug, PartialEq, Eq)]
struct MultiscaleSelectionOptions {
    group: String,
    index: usize,
    level: usize,
}

fn multiscale_selection_options(
    options: &HashMap<String, String>,
) -> ZarrFdwResult<Option<MultiscaleSelectionOptions>> {
    let group = options.get(OPT_MULTISCALE_GROUP);
    let index = options.get(OPT_MULTISCALE_INDEX);
    let level = options.get(OPT_MULTISCALE_LEVEL);
    let present = [group.is_some(), index.is_some(), level.is_some()];
    if !present.iter().any(|present| *present) {
        return Ok(None);
    }
    if !present.iter().all(|present| *present) {
        return Err(ZarrFdwError::InvalidOptionValue {
            option: OPT_MULTISCALE_GROUP.to_string(),
            message:
                "multiscale_group, multiscale_index, and multiscale_level must be provided together"
                    .to_string(),
        });
    }
    if options.contains_key(OPT_ARRAY_GROUP) {
        return Err(ZarrFdwError::InvalidOptionValue {
            option: OPT_MULTISCALE_GROUP.to_string(),
            message: "array_group cannot be combined with multiscale selection options".to_string(),
        });
    }

    let raw_group = group.expect("all multiscale options were checked");
    let group =
        canonical_ome_group_path(raw_group).map_err(|_| ZarrFdwError::InvalidOptionValue {
            option: OPT_MULTISCALE_GROUP.to_string(),
            message: "must be '/' or a safe relative Zarr group path".to_string(),
        })?;

    let parse_index = |option: &'static str, value: &str| {
        value
            .parse::<usize>()
            .map_err(|_| ZarrFdwError::InvalidOptionValue {
                option: option.to_string(),
                message: "must be a zero-based non-negative integer".to_string(),
            })
    };
    Ok(Some(MultiscaleSelectionOptions {
        group,
        index: parse_index(
            OPT_MULTISCALE_INDEX,
            index.expect("all multiscale options were checked"),
        )?,
        level: parse_index(
            OPT_MULTISCALE_LEVEL,
            level.expect("all multiscale options were checked"),
        )?,
    }))
}

enum ArrayMetadataDocument {
    V2(Vec<u8>),
    V3(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChunkFetchContext {
    logical_indices: Vec<u64>,
    object_key: String,
}

enum ResolvedChunkRequest {
    Fetch(PrefetchRequest<ChunkFetchContext>),
    Synthesized(PrefetchRequest<ChunkFetchContext>),
}

enum DeferredChunkRequest {
    Logical(Vec<u64>),
    Resolved(ResolvedChunkRequest),
}

enum ShardIndexResolution {
    Ready(Option<Arc<ShardIndex>>),
    WouldBlock,
}

/// Array-axis positions required by a spatial-time operation.
///
/// Horizontal bounds exposed by the operation layer remain in semantic
/// `[x, y]` order. `horizontal` maps them back to the selected array's actual
/// dimension order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SpatialTimeLayout {
    pub(super) time: usize,
    pub(super) horizontal: HorizontalAxes,
}

/// Exact selected time indexes plus the conservative full-rank scan window.
///
/// `time_indices` contains only coordinates inside the requested half-open
/// interval. `bounds` may span rejected indexes on an unordered time axis, so
/// callers must use `time_indices` for exact row acceptance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SpatialTimeSelection {
    pub(super) layout: SpatialTimeLayout,
    pub(super) time_indices: Vec<usize>,
    pub(super) bounds: Vec<Option<IndexBounds>>,
    pub(super) candidate_cells: usize,
}

#[cfg(test)]
fn discover_spatial_time_layout(
    rank: usize,
    axis_roles: &[DimensionRole],
    shape: &[u64],
) -> ZarrFdwResult<SpatialTimeLayout> {
    let layout = discover_spatial_time_layout_with_auxiliary_dimensions(rank, axis_roles, shape)?;
    for (axis, &extent) in shape.iter().enumerate() {
        if axis != layout.time
            && axis != layout.horizontal.x
            && axis != layout.horizontal.y
            && extent != 1
        {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "spatial-time execution requires auxiliary dimension {axis} to have extent 1, found {extent}"
            )));
        }
    }
    Ok(layout)
}

fn discover_spatial_time_layout_with_auxiliary_dimensions(
    rank: usize,
    axis_roles: &[DimensionRole],
    shape: &[u64],
) -> ZarrFdwResult<SpatialTimeLayout> {
    if rank < 3 || shape.len() != rank || axis_roles.len() != rank {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "spatial-time execution requires an array of rank 3 or greater, found rank {rank}"
        )));
    }

    let horizontal = discover_horizontal_axes_from_roles(axis_roles.iter().copied())?;
    let time_axes = axis_roles
        .iter()
        .enumerate()
        .filter_map(|(axis, role)| (*role == DimensionRole::Time).then_some(axis))
        .collect::<Vec<_>>();
    if time_axes.len() != 1 {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "spatial-time execution requires exactly one time axis, found {}",
            time_axes.len()
        )));
    }
    let time = time_axes[0];
    if time == horizontal.x || time == horizontal.y {
        return Err(ZarrFdwError::InvalidMetadata(
            "spatial-time axes must be distinct".to_string(),
        ));
    }

    Ok(SpatialTimeLayout { time, horizontal })
}

fn spatial_time_value_in_range(
    time_spec: TimeSpec,
    raw: f64,
    start_micros: i64,
    end_micros: i64,
) -> ZarrFdwResult<bool> {
    let micros = time_spec.raw_to_pg_micros(raw)?;
    Ok(start_micros <= micros && micros < end_micros)
}

#[wrappers_fdw(
    version = "0.0.1",
    author = "MPSY",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/zarr_fdw",
    error_type = "ZarrFdwError"
)]
pub(crate) struct ZarrFdw {
    store: ZarrStore,

    // --- scan state, (re)built in begin_scan ------------------------------
    tgt_cols: Vec<Column>,
    // object-key path of the cube array, relative to the store prefix
    array_dir: String,
    // discovered dimension names in array order
    axes: Vec<String>,
    // selected value array attributes retained for operation-layer metadata
    // such as strict CRS resolution
    array_attributes: Map<String, JsonValue>,
    // explicit OME-Zarr 0.5 selection, retained for EXPLAIN ANALYZE only
    selected_ome_level: Option<ResolvedOmeLevel>,
    // scientific meaning assigned by the metadata adapter, in array order
    axis_roles: Vec<DimensionRole>,
    rank: usize,
    axis_meta: Option<ArrayMeta>,
    dtype: Option<DType>,
    codec: Option<CodecPipeline>,
    scientific_decoder: Option<ScientificValueDecoder>,
    // one decoded scalar, repeated when a data chunk is absent
    fill_bytes: Option<Vec<u8>>,
    // coordinate values per axis when required by projection or restrictions
    coords: Vec<Option<Vec<f64>>>,
    // conservative physical cell window shared by ordinary and spatial scans
    selection: Selection,
    // Persistent foreign-table selectors and one optional selector-aware
    // spatial call are kept as separate AND sources so same-axis constraints
    // retain exact residual semantics.
    dimension_selectors: BoundDimensionSelectors,
    call_dimension_selectors: DimensionSelectors,
    bound_call_dimension_selectors: BoundDimensionSelectors,
    // lazy chunk indexes to read, in row-major order
    chunk_cursor: ChunkIndexCursor,
    current_chunk: Vec<u64>,
    current_object_key: String,
    deferred_prefetch: Option<DeferredChunkRequest>,
    prefetch: OrderedPrefetch<ChunkFetchContext, ZarrFdwError>,
    compressed_cache: CompressedChunkCache,
    shard_index_cache: ShardIndexCache,
    payload_cache_bytes: usize,
    shard_index_cache_bytes: usize,
    cache_layout_sharded: bool,
    max_concurrent_reads: usize,
    max_inflight_bytes: usize,
    compressed_cache_bytes: usize,
    metrics: ZarrScanMetrics,
    remote_data_get_calls: Arc<AtomicU64>,
    remote_data_encoded_bytes: Arc<AtomicU64>,
    remote_shard_payload_get_calls: Arc<AtomicU64>,
    remote_shard_payload_encoded_bytes: Arc<AtomicU64>,
    flushed_encoded_bytes: u64,
    flushed_cells: u64,
    flushed_tuples: u64,

    // --- per-chunk iteration state ---------------------------------------
    chunk_bytes: Vec<u8>,
    chunk_shape: Vec<usize>,
    sub_lo: Vec<usize>,
    sub_hi: Vec<usize>,
    sub_idx: Vec<usize>,
    capture_spatial_indices: bool,
    last_emitted_indices: Option<Vec<usize>>,
    pending: bool,

    // --- scalar aggregate execution state -------------------------------
    aggregate_defs: Vec<Aggregate>,
    aggregate_quals: Vec<Qual>,
    aggregate_reducer: Option<AggregateReducer>,
    aggregate_emitted: bool,

    time_spec: TimeSpec,
    rows_out: i64,
}

fn zeroed_scan_cursors(rank: usize) -> [Vec<usize>; 3] {
    std::array::from_fn(|_| vec![0; rank])
}

fn postgres_interrupt_pending() -> bool {
    // PostgreSQL's signal handlers update this `volatile sig_atomic_t`
    // asynchronously. Preserve the C macro's volatile-read semantics here.
    unsafe { std::ptr::read_volatile(&raw const pg_sys::InterruptPending) != 0 }
}

fn process_postgres_interrupts() {
    unsafe {
        if std::ptr::read_volatile(&raw const pg_sys::InterruptPending) != 0 {
            pg_sys::ProcessInterrupts();
        }
    }
}

fn atomic_saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

async fn observe_data_fetch<F>(
    future: F,
    remote_get_calls: Arc<AtomicU64>,
    remote_encoded_bytes: Arc<AtomicU64>,
) -> ZarrFdwResult<Option<Vec<u8>>>
where
    F: Future<Output = ZarrFdwResult<Option<Vec<u8>>>>,
{
    // The async body is lazy: this is an initiated GET, not merely a future
    // placed behind an earlier request in FuturesOrdered.
    atomic_saturating_add(&remote_get_calls, 1);
    let result = future.await;
    if let Ok(Some(bytes)) = &result {
        atomic_saturating_add(
            &remote_encoded_bytes,
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        );
    }
    result
}

async fn observe_shard_payload_fetch<F>(
    future: F,
    shard_key: String,
    remote_get_calls: Arc<AtomicU64>,
    remote_encoded_bytes: Arc<AtomicU64>,
    shard_payload_get_calls: Arc<AtomicU64>,
    shard_payload_encoded_bytes: Arc<AtomicU64>,
) -> ZarrFdwResult<Option<Vec<u8>>>
where
    F: Future<Output = ZarrFdwResult<Option<RangedObject>>>,
{
    atomic_saturating_add(&remote_get_calls, 1);
    atomic_saturating_add(&shard_payload_get_calls, 1);
    let response = future.await?.ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!(
            "indexed shard object '{shard_key}' disappeared before its payload range was read"
        ))
    })?;
    let bytes = response.bytes;
    let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    atomic_saturating_add(&remote_encoded_bytes, len);
    atomic_saturating_add(&shard_payload_encoded_bytes, len);
    Ok(Some(bytes))
}

fn checked_chunk_layout(
    meta: &ArrayMeta,
    itemsize: usize,
) -> ZarrFdwResult<(Vec<usize>, usize, usize)> {
    let storage_shape = (0..meta.chunks.len())
        .map(|axis| meta.chunk_extent(axis))
        .collect::<ZarrFdwResult<Vec<_>>>()?;
    let storage_cells = meta.chunk_cell_count()?;
    let decoded_bytes = storage_cells.checked_mul(itemsize).ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(
            "declared chunk byte length exceeds this platform's index capacity".to_string(),
        )
    })?;
    if decoded_bytes > MAX_DECODED_CHUNK_BYTES {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "declared chunk decodes to {decoded_bytes} bytes, exceeding the safety limit of {MAX_DECODED_CHUNK_BYTES}"
        )));
    }
    Ok((storage_shape, storage_cells, decoded_bytes))
}

fn checked_flat_offset(indices: &[usize], shape: &[usize]) -> ZarrFdwResult<usize> {
    if indices.len() != shape.len() {
        return Err(ZarrFdwError::InvalidMetadata(
            "chunk index rank does not match the declared chunk shape".to_string(),
        ));
    }
    let mut offset = 0usize;
    let mut stride = 1usize;
    for axis in (0..shape.len()).rev() {
        if indices[axis] >= shape[axis] {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "within-chunk index {} is outside dimension {axis} with extent {}",
                indices[axis], shape[axis]
            )));
        }
        let contribution = indices[axis].checked_mul(stride).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("chunk cell offset overflow".to_string())
        })?;
        offset = offset.checked_add(contribution).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("chunk cell offset overflow".to_string())
        })?;
        stride = stride
            .checked_mul(shape[axis])
            .ok_or_else(|| ZarrFdwError::InvalidMetadata("chunk stride overflow".to_string()))?;
    }
    Ok(offset)
}

fn checked_chunk_byte_range(
    cell_offset: usize,
    itemsize: usize,
    available: usize,
) -> ZarrFdwResult<std::ops::Range<usize>> {
    let start = cell_offset
        .checked_mul(itemsize)
        .ok_or_else(|| ZarrFdwError::InvalidMetadata("chunk byte offset overflow".to_string()))?;
    let end = start
        .checked_add(itemsize)
        .ok_or_else(|| ZarrFdwError::InvalidMetadata("chunk byte range overflow".to_string()))?;
    if end > available {
        return Err(ZarrFdwError::ReadError(std::io::Error::other(format!(
            "chunk cell byte range {start}..{end} exceeds decoded length {available}"
        ))));
    }
    Ok(start..end)
}

fn require_exact_decoded_len(key: &str, actual: usize, expected: usize) -> ZarrFdwResult<()> {
    if actual != expected {
        return Err(ZarrFdwError::ReadError(std::io::Error::other(format!(
            "chunk '{key}' decoded to {actual} bytes, expected exactly {expected}"
        ))));
    }
    Ok(())
}

fn filled_chunk_bytes(
    fill_bytes: Option<&[u8]>,
    cell_count: usize,
    key: &str,
) -> ZarrFdwResult<Vec<u8>> {
    let fill_bytes = fill_bytes.ok_or_else(|| ZarrFdwError::MissingChunkWithoutFillValue {
        key: key.to_string(),
    })?;
    if fill_bytes.is_empty() {
        return Err(ZarrFdwError::InvalidMetadata(
            "decoded fill value must contain at least one byte".to_string(),
        ));
    }
    let byte_count = fill_bytes.len().checked_mul(cell_count).ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(
            "filled chunk byte length exceeds this platform's index capacity".to_string(),
        )
    })?;
    if byte_count > MAX_DECODED_CHUNK_BYTES {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "filled chunk requires {byte_count} bytes, exceeding the safety limit of {MAX_DECODED_CHUNK_BYTES}"
        )));
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(byte_count).map_err(|_| {
        ZarrFdwError::InvalidMetadata(format!(
            "could not allocate a filled chunk of {byte_count} bytes"
        ))
    })?;
    for _ in 0..cell_count {
        bytes.extend_from_slice(fill_bytes);
    }
    Ok(bytes)
}

fn filled_coordinate_values(
    fill_value: Option<f64>,
    cell_count: usize,
    key: &str,
    axis: &str,
) -> ZarrFdwResult<Vec<f64>> {
    let fill_value = fill_value.ok_or_else(|| ZarrFdwError::CoordinateReadError {
        axis: axis.to_string(),
        error: ZarrFdwError::MissingChunkWithoutFillValue {
            key: key.to_string(),
        }
        .to_string(),
    })?;
    if cell_count > MAX_COORDINATE_VALUES {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: axis.to_string(),
            error: format!(
                "coordinate chunk has {cell_count} values, exceeding the safety limit of {MAX_COORDINATE_VALUES}"
            ),
        });
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(cell_count)
        .map_err(|_| ZarrFdwError::CoordinateReadError {
            axis: axis.to_string(),
            error: format!("could not allocate a coordinate chunk of {cell_count} values"),
        })?;
    values.resize(cell_count, fill_value);
    Ok(values)
}

fn affine_coordinate_values(
    axis: &str,
    length: u64,
    scale: f64,
    translation: f64,
) -> ZarrFdwResult<Vec<f64>> {
    if !scale.is_finite() || scale <= 0.0 || !translation.is_finite() {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: axis.to_string(),
            error:
                "OME-Zarr affine coordinates require a finite positive scale and finite translation"
                    .to_string(),
        });
    }
    let length = usize::try_from(length).map_err(|_| ZarrFdwError::CoordinateReadError {
        axis: axis.to_string(),
        error: "coordinate length exceeds this platform's index capacity".to_string(),
    })?;
    if length > MAX_COORDINATE_VALUES {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: axis.to_string(),
            error: format!(
                "coordinate has {length} values, exceeding the safety limit of {MAX_COORDINATE_VALUES}"
            ),
        });
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(|_| ZarrFdwError::CoordinateReadError {
            axis: axis.to_string(),
            error: format!("could not allocate {length} synthesized coordinate values"),
        })?;
    for index in 0..length {
        if index % SPATIAL_TIME_INTERRUPT_POLL_VALUES == 0 {
            process_postgres_interrupts();
        }
        let value = scale.mul_add(index as f64, translation);
        if !value.is_finite() {
            return Err(ZarrFdwError::CoordinateReadError {
                axis: axis.to_string(),
                error: format!("synthesized coordinate at index {index} is not finite"),
            });
        }
        values.push(value);
    }
    Ok(values)
}

fn expected_value_pg_type(dtype: DType, decode_cf: bool) -> (pg_sys::Oid, &'static str) {
    if decode_cf {
        return (pg_sys::FLOAT8OID, "double precision");
    }
    match dtype {
        DType::F32 => (pg_sys::FLOAT4OID, "real"),
        DType::F64 => (pg_sys::FLOAT8OID, "double precision"),
        DType::I8 => (pg_sys::CHAROID, r#""char""#),
        DType::I16 => (pg_sys::INT2OID, "smallint"),
        DType::I32 => (pg_sys::INT4OID, "integer"),
        DType::I64 => (pg_sys::INT8OID, "bigint"),
    }
}

fn require_column_type(
    column: &Column,
    expected_oid: pg_sys::Oid,
    expected_name: &'static str,
) -> ZarrFdwResult<()> {
    if column.type_oid != expected_oid {
        return Err(ZarrFdwError::ColumnTypeMismatch {
            column: column.name.clone(),
            actual: column.type_oid.to_u32(),
            expected: expected_name,
            expected_oid: expected_oid.to_u32(),
        });
    }
    Ok(())
}

fn validate_column_types(
    columns: &[Column],
    dataset: &Dataset,
    dtype: DType,
    decode_cf: bool,
) -> ZarrFdwResult<()> {
    let (value_oid, value_name) = expected_value_pg_type(dtype, decode_cf);
    for column in columns {
        match dataset.dimension(&column.name) {
            Some(dimension) if dimension.semantic_role() == DimensionRole::Time => {
                require_column_type(column, pg_sys::TIMESTAMPTZOID, "timestamp with time zone")?;
            }
            Some(_) => {
                require_column_type(column, pg_sys::FLOAT8OID, "double precision")?;
            }
            None => require_column_type(column, value_oid, value_name)?,
        }
    }
    Ok(())
}

fn validate_coordinate_values(axis: &str, values: &[f64]) -> ZarrFdwResult<()> {
    if let Some((index, value)) = values
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: axis.to_string(),
            error: format!("coordinate value at index {index} is not finite ({value})"),
        });
    }

    Ok(())
}

fn validate_coordinate_decoding_attributes(
    axis: &str,
    attributes: &Map<String, JsonValue>,
) -> ZarrFdwResult<()> {
    if let Some(attribute) = UNSUPPORTED_COORDINATE_DECODING_ATTRIBUTES
        .iter()
        .copied()
        .find(|attribute| attributes.contains_key(*attribute))
    {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: axis.to_string(),
            error: format!(
                "attribute '{attribute}' requires coordinate decoding, which is not supported yet"
            ),
        });
    }
    Ok(())
}

fn checked_total_coordinate_values<'a>(
    dimensions: impl IntoIterator<Item = (&'a str, u64, bool)>,
    limit: usize,
) -> ZarrFdwResult<usize> {
    let mut total = 0usize;
    for (name, length, required) in dimensions {
        if !required {
            continue;
        }
        let length = usize::try_from(length).map_err(|_| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: "coordinate length exceeds this platform's index capacity".to_string(),
        })?;
        total = total.checked_add(length).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "total required coordinate value count overflowed".to_string(),
            )
        })?;
        if total > limit {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "required coordinate arrays contain {total} values, exceeding the cumulative safety limit of {limit}"
            )));
        }
    }
    Ok(total)
}

fn estimated_pg_type_width(type_oid: pg_sys::Oid) -> i32 {
    match type_oid {
        pg_sys::CHAROID => 1,
        pg_sys::INT2OID => 2,
        pg_sys::FLOAT4OID | pg_sys::INT4OID => 4,
        pg_sys::FLOAT8OID | pg_sys::INT8OID | pg_sys::TIMESTAMPTZOID => 8,
        _ => DEFAULT_UNKNOWN_TYPE_WIDTH,
    }
}

fn conservative_rel_size(columns: &[Column]) -> (i64, i32) {
    let width = if columns.is_empty() {
        DEFAULT_EMPTY_PROJECTION_WIDTH
    } else {
        columns.iter().fold(0_i32, |sum, column| {
            sum.saturating_add(estimated_pg_type_width(column.type_oid))
        })
    };
    (DEFAULT_PLANNER_ROWS, width)
}

impl ZarrFdw {
    fn value_cell(dt: DType, b: &[u8]) -> ZarrFdwResult<Cell> {
        let ok = |n: usize| {
            ZarrFdwError::ReadError(std::io::Error::other(format!(
                "chunk cell data too short: need {n} bytes, got {}",
                b.len()
            )))
        };
        Ok(match dt {
            DType::F32 => Cell::F32(f32::from_le_bytes(b.try_into().map_err(|_| ok(4))?)),
            DType::F64 => Cell::F64(f64::from_le_bytes(b.try_into().map_err(|_| ok(8))?)),
            DType::I8 => Cell::I8(b.first().copied().ok_or_else(|| ok(1))? as i8),
            DType::I16 => Cell::I16(i16::from_le_bytes(b.try_into().map_err(|_| ok(2))?)),
            DType::I32 => Cell::I32(i32::from_le_bytes(b.try_into().map_err(|_| ok(4))?)),
            DType::I64 => Cell::I64(i64::from_le_bytes(b.try_into().map_err(|_| ok(8))?)),
        })
    }

    /// Install one already-parsed selector document supplied by an explicit
    /// spatial overload. It remains a separate AND source from the foreign
    /// table option throughout binding, pruning, and exact residual checks.
    pub(super) fn set_call_dimension_selectors(
        &mut self,
        selectors: DimensionSelectors,
    ) -> ZarrFdwResult<()> {
        if self.axis_meta.is_some() {
            return Err(ZarrFdwError::InvalidMetadata(
                "spatial call selectors must be installed before scan initialization".to_string(),
            ));
        }
        self.call_dimension_selectors = selectors;
        Ok(())
    }

    fn spatial_horizontal_coordinates(&self) -> ZarrFdwResult<(HorizontalAxes, &[f64], &[f64])> {
        if self.rank < 2 {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "spatial execution requires an array of rank 2 or greater, found rank {}",
                self.rank
            )));
        }
        let axes = discover_horizontal_axes_from_roles(self.axis_roles.iter().copied())?;
        let x = self
            .coords
            .get(axes.x)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "spatial coordinate '{}' was not loaded",
                    self.axes.get(axes.x).map_or("x", String::as_str)
                ))
            })?;
        let y = self
            .coords
            .get(axes.y)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "spatial coordinate '{}' was not loaded",
                    self.axes.get(axes.y).map_or("y", String::as_str)
                ))
            })?;
        Ok((axes, x, y))
    }

    pub(super) fn spatial_horizontal_axes(&self) -> ZarrFdwResult<HorizontalAxes> {
        self.spatial_horizontal_coordinates()
            .map(|(axes, _, _)| axes)
    }

    /// Resolve one exact horizontal center on a rank-2-or-greater array.
    pub(super) fn spatial_exact_horizontal_cell(
        &self,
        target_x: f64,
        target_y: f64,
    ) -> ZarrFdwResult<Option<(HorizontalAxes, HorizontalCell)>> {
        let (axes, x, y) = self.spatial_horizontal_coordinates()?;
        let Some(x_index) = exact_center_index(x, target_x)? else {
            return Ok(None);
        };
        let Some(y_index) = exact_center_index(y, target_y)? else {
            return Ok(None);
        };
        Ok(Some((
            axes,
            HorizontalCell {
                x_index,
                y_index,
                x: x[x_index],
                y: y[y_index],
                distance: 0.0,
            },
        )))
    }

    /// Resolve one nearest horizontal center on a rank-2-or-greater array.
    pub(super) fn spatial_nearest_horizontal_cell(
        &self,
        target_x: f64,
        target_y: f64,
    ) -> ZarrFdwResult<(HorizontalAxes, HorizontalCell)> {
        let (axes, x, y) = self.spatial_horizontal_coordinates()?;
        let x_index = nearest_center_index(x, target_x)?.ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("spatial x coordinate is empty".to_string())
        })?;
        let y_index = nearest_center_index(y, target_y)?.ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("spatial y coordinate is empty".to_string())
        })?;
        let selected_x = x[x_index];
        let selected_y = y[y_index];
        Ok((
            axes,
            HorizontalCell {
                x_index,
                y_index,
                x: selected_x,
                y: selected_y,
                distance: (selected_x - target_x).hypot(selected_y - target_y),
            },
        ))
    }

    /// Convert a transformed geometry envelope to inclusive semantic `[x, y]`
    /// bounds for a rank-2-or-greater array.
    pub(super) fn spatial_horizontal_window(
        &self,
        xmin: f64,
        ymin: f64,
        xmax: f64,
        ymax: f64,
    ) -> ZarrFdwResult<Option<(HorizontalAxes, [IndexBounds; 2], usize)>> {
        let (axes, x, y) = self.spatial_horizontal_coordinates()?;
        let Some(x_bounds) = inclusive_center_bounds(x, xmin, xmax)? else {
            return Ok(None);
        };
        let Some(y_bounds) = inclusive_center_bounds(y, ymin, ymax)? else {
            return Ok(None);
        };
        let candidate_cells = [x_bounds, y_bounds]
            .iter()
            .try_fold(1usize, |total, bounds| {
                let extent = bounds
                    .end
                    .checked_sub(bounds.start)
                    .and_then(|extent| extent.checked_add(1))
                    .ok_or_else(|| {
                        ZarrFdwError::InvalidMetadata(
                            "spatial horizontal candidate count overflowed".to_string(),
                        )
                    })?;
                total.checked_mul(extent).ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(
                        "spatial horizontal candidate count overflowed".to_string(),
                    )
                })
            })?;
        Ok(Some((axes, [x_bounds, y_bounds], candidate_cells)))
    }

    /// Borrow the fully decoded horizontal coordinate grid prepared by
    /// `begin_scan`. Spatial operations use this view to choose cells without
    /// creating a second storage or metadata path.
    pub(super) fn rectilinear_grid(&self) -> ZarrFdwResult<RectilinearGrid<'_>> {
        if self.rank != 2 {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "point sampling requires a rank-2 array, found rank {}",
                self.rank
            )));
        }
        let axes = discover_horizontal_axes_from_roles(self.axis_roles.iter().copied())?;
        let x = self
            .coords
            .get(axes.x)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "spatial coordinate '{}' was not loaded",
                    self.axes.get(axes.x).map_or("x", String::as_str)
                ))
            })?;
        let y = self
            .coords
            .get(axes.y)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "spatial coordinate '{}' was not loaded",
                    self.axes.get(axes.y).map_or("y", String::as_str)
                ))
            })?;
        RectilinearGrid::new(axes, x, y)
    }

    /// Validate the dimension contract shared by spatial-time operations.
    ///
    /// Arrays may contain singleton auxiliary dimensions, but execution needs
    /// exactly one time axis and one unambiguous horizontal pair.
    pub(super) fn spatial_time_layout(&self) -> ZarrFdwResult<SpatialTimeLayout> {
        let meta = self.axis_meta.as_ref().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "spatial-time execution requires an initialized array scan".to_string(),
            )
        })?;
        let layout = discover_spatial_time_layout_with_auxiliary_dimensions(
            self.rank,
            &self.axis_roles,
            &meta.shape,
        )?;

        for axis in [layout.time, layout.horizontal.x, layout.horizontal.y] {
            if self.coords.get(axis).and_then(Option::as_deref).is_none() {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "spatial-time coordinate '{}' was not loaded",
                    self.axes.get(axis).map_or("unknown", String::as_str)
                )));
            }
        }

        Ok(layout)
    }

    /// Convert a transformed geometry envelope to inclusive horizontal index
    /// bounds in semantic `[x, y]` order. The complete spatial-time layout is
    /// validated before an empty window is returned.
    pub(super) fn spatial_time_horizontal_window(
        &self,
        xmin: f64,
        ymin: f64,
        xmax: f64,
        ymax: f64,
    ) -> ZarrFdwResult<Option<(HorizontalAxes, [IndexBounds; 2], usize)>> {
        let layout = self.spatial_time_layout()?;
        let x = self.coords[layout.horizontal.x]
            .as_deref()
            .expect("spatial_time_layout validated the x coordinate");
        let y = self.coords[layout.horizontal.y]
            .as_deref()
            .expect("spatial_time_layout validated the y coordinate");
        let Some(x_bounds) = inclusive_center_bounds(x, xmin, xmax)? else {
            return Ok(None);
        };
        let Some(y_bounds) = inclusive_center_bounds(y, ymin, ymax)? else {
            return Ok(None);
        };
        let candidate_cells = x_bounds
            .end
            .checked_sub(x_bounds.start)
            .and_then(|extent| extent.checked_add(1))
            .and_then(|x_extent| {
                y_bounds
                    .end
                    .checked_sub(y_bounds.start)
                    .and_then(|extent| extent.checked_add(1))
                    .and_then(|y_extent| x_extent.checked_mul(y_extent))
            })
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "spatial-time horizontal candidate count overflowed".to_string(),
                )
            })?;
        Ok(Some((
            layout.horizontal,
            [x_bounds, y_bounds],
            candidate_cells,
        )))
    }

    /// Build a conservative full-rank scan window from exact time indexes
    /// previously returned by `spatial_time_indices`. Unordered time
    /// coordinates are accepted; exact row filtering must still use
    /// `time_indices` after the conservative scan.
    pub(super) fn spatial_time_selection(
        &self,
        time_indices: Vec<usize>,
        horizontal_bounds: [IndexBounds; 2],
        max_candidates: usize,
    ) -> ZarrFdwResult<SpatialTimeSelection> {
        let layout = self.spatial_time_layout()?;

        if time_indices.is_empty() {
            return Ok(SpatialTimeSelection {
                layout,
                time_indices,
                bounds: vec![None; self.rank],
                candidate_cells: 0,
            });
        }

        let meta = self
            .axis_meta
            .as_ref()
            .expect("spatial_time_layout validated metadata");
        let time_bounds = IndexBounds {
            start: *time_indices
                .first()
                .expect("non-empty time selection has a first index"),
            end: *time_indices
                .last()
                .expect("non-empty time selection has a last index"),
        };
        let mut bounds = self.selection.axis_bounds().to_vec();
        bounds[layout.time] = Some(time_bounds);
        bounds[layout.horizontal.x] = Some(horizontal_bounds[0]);
        bounds[layout.horizontal.y] = Some(horizontal_bounds[1]);

        let mut candidate_cells = 1usize;
        for (axis, axis_bounds) in bounds.iter().enumerate() {
            let length = meta.shape_extent(axis)?;
            let extent = match axis_bounds {
                Some(axis_bounds)
                    if axis_bounds.start <= axis_bounds.end && axis_bounds.end < length =>
                {
                    axis_bounds
                        .end
                        .checked_sub(axis_bounds.start)
                        .and_then(|extent| extent.checked_add(1))
                        .expect("validated inclusive bounds have a positive extent")
                }
                Some(axis_bounds) => {
                    return Err(ZarrFdwError::InvalidMetadata(format!(
                        "spatial-time index bounds {}..={} are invalid for dimension {axis} length {length}",
                        axis_bounds.start, axis_bounds.end
                    )));
                }
                None => length,
            };
            candidate_cells = candidate_cells.checked_mul(extent).ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "spatial-time candidate cell count overflowed".to_string(),
                )
            })?;
            if candidate_cells > max_candidates {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "spatial-time request has {candidate_cells} candidate cells, exceeding the limit of {max_candidates}"
                )));
            }
        }

        Ok(SpatialTimeSelection {
            layout,
            time_indices,
            bounds,
            candidate_cells,
        })
    }

    /// Resolve exact native time-axis indexes inside the requested half-open
    /// interval independently of any horizontal overlap.
    pub(super) fn spatial_time_indices(
        &mut self,
        start: TimestampWithTimeZone,
        end: TimestampWithTimeZone,
        max_time_slices: usize,
    ) -> ZarrFdwResult<Vec<usize>> {
        let layout = self.spatial_time_layout()?;
        let start_micros = start.into_inner();
        let end_micros = end.into_inner();
        if start_micros >= end_micros {
            return Err(ZarrFdwError::InvalidMetadata(
                "spatial-time start must be earlier than end".to_string(),
            ));
        }

        let time_value_count = self.coords[layout.time]
            .as_deref()
            .expect("spatial_time_layout validated the time coordinate")
            .len();
        let mut time_indices = Vec::new();
        for index in 0..time_value_count {
            if index.is_multiple_of(SPATIAL_TIME_INTERRUPT_POLL_VALUES) {
                self.spatial_check_for_interrupt()?;
            }
            let raw = self.coords[layout.time]
                .as_deref()
                .expect("spatial_time_layout validated the time coordinate")[index];
            if spatial_time_value_in_range(self.time_spec, raw, start_micros, end_micros)? {
                if time_indices.len() >= max_time_slices {
                    return Err(ZarrFdwError::InvalidMetadata(format!(
                        "spatial-time request exceeds the limit of {max_time_slices} time slices"
                    )));
                }
                time_indices.push(index);
            }
        }
        Ok(time_indices)
    }

    /// Strictly resolve the selected array's operation-time CRS. The
    /// inspection surface remains permissive; spatial execution requires an
    /// explicit, conflict-free EPSG identifier.
    pub(super) fn resolved_spatial_crs(&mut self) -> ZarrFdwResult<ResolvedCrs> {
        if self.axis_meta.is_none() {
            return Err(ZarrFdwError::InvalidMetadata(
                "spatial CRS resolution requires an initialized array scan".to_string(),
            ));
        }
        let array_path = self.array_dir.clone();
        let array_attributes = self.array_attributes.clone();
        let group_path = array_parent_path(&array_path).to_string();
        let group_attributes =
            read_array_attributes_optional(&self.store, &mut self.metrics, &group_path)?;
        let mapping_path = grid_mapping_sibling_path(&array_path, &array_attributes)?;
        let mapping_attributes = match mapping_path.as_deref() {
            Some(path) => read_array_attributes_optional(&self.store, &mut self.metrics, path)?,
            None => None,
        };
        resolve_crs(
            &array_path,
            &array_attributes,
            group_attributes.as_ref(),
            mapping_path
                .as_deref()
                .zip(mapping_attributes.as_ref())
                .map(|(path, attributes)| GridMappingMetadata { path, attributes }),
        )
    }

    /// Return the one non-dimension column selected by the foreign table.
    pub(super) fn spatial_value_column(&self) -> ZarrFdwResult<&str> {
        let mut values = self
            .tgt_cols
            .iter()
            .filter(|column| !self.axes.iter().any(|axis| axis == &column.name));
        let value = values.next().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "spatial operations require exactly one value column".to_string(),
            )
        })?;
        if values.next().is_some() {
            return Err(ZarrFdwError::InvalidMetadata(
                "spatial operations require exactly one value column".to_string(),
            ));
        }
        Ok(&value.name)
    }

    pub(super) fn spatial_array_path(&self) -> &str {
        &self.array_dir
    }

    /// Global array indexes for the row most recently returned by
    /// `iter_scan`, in the selected array's native dimension order.
    pub(super) fn spatial_last_emitted_global_indices(&self) -> ZarrFdwResult<&[usize]> {
        self.last_emitted_indices.as_deref().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "spatial row indexes are unavailable before a row is emitted".to_string(),
            )
        })
    }

    /// Resolve one loaded coordinate by native array-axis and global index.
    pub(super) fn spatial_coordinate_at_index(
        &self,
        axis: usize,
        index: usize,
    ) -> ZarrFdwResult<f64> {
        let axis_name = self.axes.get(axis).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "spatial coordinate axis {axis} is outside array rank {}",
                self.rank
            ))
        })?;
        let values = self
            .coords
            .get(axis)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "spatial coordinate '{axis_name}' was not loaded"
                ))
            })?;
        values.get(index).copied().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "spatial coordinate index {index} is outside axis {axis} length {}",
                values.len()
            ))
        })
    }

    /// Convert one coordinate on an already-resolved time axis to PostgreSQL's
    /// timestamptz representation without rediscovering the complete layout
    /// for every emitted cell.
    pub(super) fn spatial_time_at_index(
        &self,
        time_axis: usize,
        index: usize,
    ) -> ZarrFdwResult<TimestampWithTimeZone> {
        if self.axis_roles.get(time_axis) != Some(&DimensionRole::Time) {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "spatial-time axis {time_axis} is not the discovered time axis"
            )));
        }
        let raw = self.spatial_coordinate_at_index(time_axis, index)?;
        let micros = self.time_spec.raw_to_pg_micros(raw)?;
        TimestampWithTimeZone::try_from(micros).map_err(|_| ZarrFdwError::TimeOutOfRange(raw))
    }

    /// Resolve the most recently emitted row to one horizontal cell in constant
    /// time. The coordinate vectors and horizontal axes were already fully
    /// validated when the spatial window was prepared, so polygon execution
    /// must not rebuild and revalidate the complete grid for every row.
    pub(super) fn spatial_last_emitted_cell(
        &self,
        axes: HorizontalAxes,
    ) -> ZarrFdwResult<HorizontalCell> {
        let array_indices = self.spatial_last_emitted_global_indices()?;
        let x_index = *array_indices.get(axes.x).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "spatial x axis {} is outside rank-{} array indexes",
                axes.x, self.rank
            ))
        })?;
        let y_index = *array_indices.get(axes.y).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "spatial y axis {} is outside rank-{} array indexes",
                axes.y, self.rank
            ))
        })?;
        let x_values = self
            .coords
            .get(axes.x)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata("spatial x coordinate was not loaded".to_string())
            })?;
        let y_values = self
            .coords
            .get(axes.y)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata("spatial y coordinate was not loaded".to_string())
            })?;
        let x = *x_values.get(x_index).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "spatial x index {x_index} is outside coordinate length {}",
                x_values.len()
            ))
        })?;
        let y = *y_values.get(y_index).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "spatial y index {y_index} is outside coordinate length {}",
                y_values.len()
            ))
        })?;
        Ok(HorizontalCell {
            x_index,
            y_index,
            x,
            y,
            distance: 0.0,
        })
    }

    /// Poll PostgreSQL cancellation while a spatial SRF is consuming many
    /// rows from one decoded chunk. This preserves the prefetch cleanup
    /// invariant enforced by the ordinary scan path.
    pub(super) fn spatial_check_for_interrupt(&mut self) -> ZarrFdwResult<()> {
        self.process_pending_interrupt()
    }

    /// Resolve every operation-auxiliary dimension to zero or one exact native
    /// index under the intersection of table and call selectors.
    ///
    /// Operation-owned axes cannot be named by either selector source. All
    /// auxiliary axes are checked even after one resolves empty so an ambiguous
    /// axis cannot be hidden by an unrelated no-match selector.
    pub(super) fn apply_spatial_dimension_selectors(
        &mut self,
        operation: &str,
        operation_axes: &[usize],
    ) -> ZarrFdwResult<bool> {
        let meta = self.axis_meta.as_ref().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("spatial scan was not initialized".to_string())
        })?;
        if self.axes.len() != self.rank || meta.shape.len() != self.rank {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "spatial dimension metadata does not match array rank {}",
                self.rank
            )));
        }
        let axis_lengths = (0..self.rank)
            .map(|axis| meta.shape_extent(axis))
            .collect::<ZarrFdwResult<Vec<_>>>()?;

        let mut owned = vec![false; self.rank];
        for &axis in operation_axes {
            let Some(slot) = owned.get_mut(axis) else {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "spatial operation axis {axis} is outside array rank {}",
                    self.rank
                )));
            };
            *slot = true;
            if self.dimension_selectors.selects_axis(axis)
                || self.bound_call_dimension_selectors.selects_axis(axis)
            {
                return Err(ZarrFdwError::InvalidOptionValue {
                    option: OPT_DIMENSION_SELECTORS.to_string(),
                    message: format!(
                        "{operation} owns dimension '{}'; selectors may target auxiliary dimensions only",
                        self.axes[axis]
                    ),
                });
            }
        }

        let mut auxiliary_bounds = vec![None; self.rank];
        let mut any_empty = false;
        for axis in 0..self.rank {
            if owned[axis] {
                continue;
            }
            let length = axis_lengths[axis];
            let mut selected = None;
            for index in 0..length {
                if index.is_multiple_of(SELECTOR_INTERRUPT_POLL_CELLS) {
                    self.process_pending_interrupt()?;
                }
                if !self
                    .dimension_selectors
                    .matches_axis_index(axis, index, &self.coords)?
                    || !self.bound_call_dimension_selectors.matches_axis_index(
                        axis,
                        index,
                        &self.coords,
                    )?
                {
                    continue;
                }
                if selected.is_some() {
                    return Err(ZarrFdwError::InvalidOptionValue {
                        option: OPT_DIMENSION_SELECTORS.to_string(),
                        message: format!(
                            "auxiliary dimension '{}' resolves to more than one index; spatial operations require zero or one",
                            self.axes[axis]
                        ),
                    });
                }
                selected = Some(index);
            }
            match selected {
                Some(index) => {
                    auxiliary_bounds[axis] = Some(IndexBounds {
                        start: index,
                        end: index,
                    });
                }
                None => any_empty = true,
            }
        }

        let selection = if any_empty {
            Selection::empty(self.rank)
        } else {
            self.selection
                .clone()
                .intersect(Selection::from_axis_bounds(auxiliary_bounds))
        };
        let nonempty = !selection.is_empty();
        self.apply_selection(selection, true)?;
        Ok(nonempty)
    }

    /// Narrow an already-prepared rank-2 scan to one global array cell. This
    /// preserves the existing chunk loader, missing-chunk semantics, cache,
    /// cancellation, metrics, and scientific decoder.
    pub(super) fn restrict_to_spatial_cell(
        &mut self,
        array_indices: [usize; 2],
    ) -> ZarrFdwResult<()> {
        let array_bounds = array_indices.map(|index| IndexBounds {
            start: index,
            end: index,
        });
        self.restrict_to_spatial_bounds(array_bounds)
    }

    /// Narrow an already-prepared rank-2 scan to inclusive global array-index
    /// bounds. Spatial polygon operations derive these bounds from the
    /// transformed geometry envelope, then apply an exact PostGIS mask to the
    /// candidate cell centers.
    pub(super) fn restrict_to_spatial_bounds(
        &mut self,
        array_bounds: [IndexBounds; 2],
    ) -> ZarrFdwResult<()> {
        let meta = self.axis_meta.as_ref().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("spatial scan was not initialized".to_string())
        })?;
        if self.rank != 2 || meta.shape.len() != 2 {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "spatial execution requires a rank-2 array, found rank {}",
                self.rank
            )));
        }
        self.restrict_to_axis_bounds(array_bounds.into_iter().map(Some).collect())
    }

    /// Narrow a rank-2-or-greater scan to one semantic horizontal cell while
    /// preserving the exact singleton bounds already chosen for auxiliaries.
    pub(super) fn restrict_to_horizontal_cell(
        &mut self,
        axes: HorizontalAxes,
        x_index: usize,
        y_index: usize,
    ) -> ZarrFdwResult<()> {
        self.restrict_to_horizontal_bounds(
            axes,
            [
                IndexBounds {
                    start: x_index,
                    end: x_index,
                },
                IndexBounds {
                    start: y_index,
                    end: y_index,
                },
            ],
        )
    }

    /// Narrow a rank-2-or-greater scan with semantic `[x, y]` bounds.
    pub(super) fn restrict_to_horizontal_bounds(
        &mut self,
        axes: HorizontalAxes,
        horizontal_bounds: [IndexBounds; 2],
    ) -> ZarrFdwResult<()> {
        if axes.x >= self.rank || axes.y >= self.rank || axes.x == axes.y {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "spatial horizontal axes ({}, {}) are invalid for array rank {}",
                axes.x, axes.y, self.rank
            )));
        }
        let mut axis_bounds = vec![None; self.rank];
        axis_bounds[axes.x] = Some(horizontal_bounds[0]);
        axis_bounds[axes.y] = Some(horizontal_bounds[1]);
        self.restrict_to_axis_bounds(axis_bounds)
    }

    /// Narrow an initialized scan to optional inclusive bounds for every
    /// native array axis. `None` preserves the complete extent of that axis.
    pub(super) fn restrict_to_axis_bounds(
        &mut self,
        axis_bounds: Vec<Option<IndexBounds>>,
    ) -> ZarrFdwResult<()> {
        let meta = self.axis_meta.as_ref().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("spatial scan was not initialized".to_string())
        })?;
        if axis_bounds.len() != self.rank || meta.shape.len() != self.rank {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "spatial bounds rank {} does not match array rank {}",
                axis_bounds.len(),
                self.rank
            )));
        }
        for (axis, bounds) in axis_bounds.iter().enumerate() {
            let Some(bounds) = bounds else {
                continue;
            };
            let length = meta.shape_extent(axis)?;
            if bounds.start > bounds.end || bounds.end >= length {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "spatial index bounds {}..={} are invalid for dimension {axis} length {length}",
                    bounds.start, bounds.end
                )));
            }
        }

        let selection = self
            .selection
            .clone()
            .intersect(Selection::from_axis_bounds(axis_bounds));
        self.apply_selection(selection, true)
    }

    /// Install a conservative candidate window and reset every cursor/buffer
    /// derived from the previous window. Exact SQL, temporal, and PostGIS
    /// residual checks remain with their existing owners.
    fn apply_selection(
        &mut self,
        selection: Selection,
        capture_emitted_indices: bool,
    ) -> ZarrFdwResult<()> {
        let meta = self.axis_meta.as_ref().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "scan selection requires initialized metadata".to_string(),
            )
        })?;
        let plan = ScanPlanner::new(meta).plan(selection)?;
        self.apply_scan_plan(plan, capture_emitted_indices)
    }

    /// Install an already validated executor-time plan. The plan contains only
    /// rank-sized axis ranges; `ChunkIndexCursor` remains the sole lazy chunk
    /// enumerator.
    fn apply_scan_plan(
        &mut self,
        plan: ScanPlan,
        capture_emitted_indices: bool,
    ) -> ZarrFdwResult<()> {
        let chunk_cursor = ChunkIndexCursor::new(plan.axis_chunk_ranges())?;
        self.metrics
            .set_chunk_selection(plan.chunks_total(), plan.chunks_selected());
        self.selection = plan.into_selection();
        self.chunk_cursor = chunk_cursor;
        self.current_chunk.clear();
        self.prefetch.clear();
        self.deferred_prefetch = None;
        self.chunk_bytes.clear();
        self.chunk_shape.clear();
        [self.sub_lo, self.sub_hi, self.sub_idx] = zeroed_scan_cursors(self.rank);
        self.capture_spatial_indices = capture_emitted_indices;
        self.last_emitted_indices = None;
        self.pending = false;
        self.rows_out = 0;
        Ok(())
    }

    fn value_cell_at_cursor(&self) -> ZarrFdwResult<Option<Cell>> {
        let dt = self.dtype.expect("dtype set in begin_scan");
        let offset = checked_flat_offset(&self.sub_idx, &self.chunk_shape)?;
        let byte_range = checked_chunk_byte_range(offset, dt.itemsize(), self.chunk_bytes.len())?;
        let raw_value = &self.chunk_bytes[byte_range];
        match &self.scientific_decoder {
            Some(decoder) => Ok(decoder.decode(raw_value)?.map(Cell::F64)),
            None => Ok(Some(Self::value_cell(dt, raw_value)?)),
        }
    }

    fn coordinate_cell_at_cursor(&self, axis: usize) -> ZarrFdwResult<Cell> {
        let meta = self
            .axis_meta
            .as_ref()
            .expect("begin_scan must be called before iter_scan");
        let chunk_indices = &self.current_chunk;
        let chunk_index = usize::try_from(chunk_indices[axis]).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "chunk index for axis {axis} exceeds this platform's index capacity"
            ))
        })?;
        let chunk_len = meta.chunk_extent(axis)?;
        let global = chunk_index
            .checked_mul(chunk_len)
            .and_then(|base| base.checked_add(self.sub_idx[axis]))
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!("coordinate index overflow on axis {axis}"))
            })?;
        let coords = self.coords[axis].as_deref().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "coordinate '{}' is required for row output or predicate evaluation but was not loaded",
                self.axes[axis]
            ))
        })?;
        let coord = *coords.get(global).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "coordinate index {global} is outside axis {axis} length {}",
                coords.len()
            ))
        })?;
        if self.axis_roles[axis] == DimensionRole::Time {
            let micros = self.time_spec.raw_to_pg_micros(coord)?;
            let timestamp = TimestampWithTimeZone::try_from(micros)
                .map_err(|_| ZarrFdwError::TimeOutOfRange(coord))?;
            Ok(Cell::Timestamptz(timestamp))
        } else {
            Ok(Cell::F64(coord))
        }
    }

    fn global_index_at_cursor(&self, axis: usize) -> ZarrFdwResult<usize> {
        let meta = self
            .axis_meta
            .as_ref()
            .expect("begin_scan must be called before iter_scan");
        if self.current_chunk.len() != self.rank || self.sub_idx.len() != self.rank {
            return Err(ZarrFdwError::InvalidMetadata(
                "scan cursor rank does not match array rank".to_string(),
            ));
        }
        let chunk_index = usize::try_from(self.current_chunk[axis]).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "chunk index for axis {axis} exceeds this platform's index capacity"
            ))
        })?;
        let chunk_len = meta.chunk_extent(axis)?;
        chunk_index
            .checked_mul(chunk_len)
            .and_then(|base| base.checked_add(self.sub_idx[axis]))
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "global coordinate index overflow on axis {axis}"
                ))
            })
    }

    fn column_cell_at_cursor(
        &self,
        column_name: &str,
        value_cell: Option<&Cell>,
    ) -> ZarrFdwResult<Option<Cell>> {
        match self.axes.iter().position(|axis| axis == column_name) {
            Some(axis) => self.coordinate_cell_at_cursor(axis).map(Some),
            None => Ok(value_cell.cloned()),
        }
    }

    fn advance_cursor(&mut self) {
        // Advance in C order (last axis varies fastest).
        for axis in (0..self.rank).rev() {
            if self.sub_idx[axis] < self.sub_hi[axis] {
                self.sub_idx[axis] += 1;
                return;
            }
            self.sub_idx[axis] = self.sub_lo[axis];
        }
        self.pending = false;
    }

    fn configure_sharded_cache_budget(&mut self) {
        if self.cache_layout_sharded {
            return;
        }
        let index_bytes =
            (self.compressed_cache_bytes / SHARD_INDEX_CACHE_FRACTION).min(MAX_SHARD_INDEX_BYTES);
        let payload_bytes = self.compressed_cache_bytes.saturating_sub(index_bytes);
        let index_entries = if index_bytes == 0 {
            0
        } else {
            MAX_COMPRESSED_CACHE_ENTRIES / SHARD_INDEX_CACHE_FRACTION
        };
        let payload_entries = MAX_COMPRESSED_CACHE_ENTRIES.saturating_sub(index_entries);
        self.compressed_cache = CompressedChunkCache::new(payload_bytes, payload_entries);
        self.shard_index_cache = ShardIndexCache::new(index_bytes, index_entries);
        self.payload_cache_bytes = payload_bytes;
        self.shard_index_cache_bytes = index_bytes;
        self.cache_layout_sharded = true;
    }

    fn resolve_shard_index(
        &mut self,
        config: &ShardingConfig,
        shard_key: &str,
        read_kind: ReadKind,
        allow_remote: bool,
    ) -> ZarrFdwResult<ShardIndexResolution> {
        let request = config.index_read_identity(shard_key.to_string())?;
        if let Some(cached) = self.shard_index_cache.get(&request) {
            self.metrics.record_shard_index_cache_lookup(true);
            return Ok(ShardIndexResolution::Ready(match cached {
                CachedShardIndex::Present(index) => Some(index),
                CachedShardIndex::Missing => None,
            }));
        }
        if !allow_remote {
            return Ok(ShardIndexResolution::WouldBlock);
        }
        if config.encoded_index_bytes > self.max_inflight_bytes {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "shard index read limit {} exceeds max_inflight_bytes {}",
                config.encoded_index_bytes, self.max_inflight_bytes
            )));
        }

        self.metrics.record_shard_index_cache_lookup(false);
        self.metrics.record_remote_request(read_kind);
        let response = self.store.get_object_range_sync(request.clone())?;
        let response_bytes = response.as_ref().map(|response| response.bytes.len());
        if let Some(bytes) = response_bytes {
            self.metrics.record_remote_response_bytes(read_kind, bytes);
        }
        self.metrics.record_shard_index_get(response_bytes);

        let evictions_before = self.shard_index_cache.evictions();
        let Some(response) = response else {
            self.shard_index_cache.insert_missing(request);
            self.metrics.record_shard_index_cache_evictions(
                self.shard_index_cache
                    .evictions()
                    .saturating_sub(evictions_before),
            );
            return Ok(ShardIndexResolution::Ready(None));
        };
        let index =
            match ShardIndex::decode_interruptible(config, response, postgres_interrupt_pending)? {
                ShardIndexDecode::Decoded(index) => Arc::new(index),
                ShardIndexDecode::Interrupted => {
                    self.process_pending_interrupt()?;
                    return Err(ZarrFdwError::InvalidMetadata(
                        "query interruption was requested".to_string(),
                    ));
                }
            };
        self.shard_index_cache
            .insert_present(request, Arc::clone(&index));
        self.metrics.record_shard_index_cache_evictions(
            self.shard_index_cache
                .evictions()
                .saturating_sub(evictions_before),
        );
        Ok(ShardIndexResolution::Ready(Some(index)))
    }

    fn chunk_request(
        &mut self,
        indices: Vec<u64>,
        allow_remote_index: bool,
    ) -> ZarrFdwResult<Option<ResolvedChunkRequest>> {
        let meta = self
            .axis_meta
            .as_ref()
            .expect("begin_scan must be called before iter_scan")
            .clone();
        let dtype = self.dtype.expect("dtype set in begin_scan");
        let codec = self.codec.as_ref().expect("codec set in begin_scan");
        let (_, _, expected) = checked_chunk_layout(&meta, dtype.itemsize())?;
        let encoded_limit = codec.encoded_read_limit(expected)?;
        let context = |object_key: String| ChunkFetchContext {
            logical_indices: indices.clone(),
            object_key,
        };

        match &meta.storage_layout {
            StorageLayout::Direct => {
                if encoded_limit > self.max_inflight_bytes {
                    return Err(ZarrFdwError::InvalidMetadata(format!(
                        "encoded chunk read limit {encoded_limit} exceeds max_inflight_bytes {}",
                        self.max_inflight_bytes
                    )));
                }
                let key = join_key(
                    &self.array_dir,
                    &chunk_key(&meta.chunk_key_encoding, &indices),
                );
                Ok(Some(ResolvedChunkRequest::Fetch(PrefetchRequest {
                    context: context(key.clone()),
                    identity: ReadIdentity::whole(key),
                    max_bytes: encoded_limit,
                })))
            }
            StorageLayout::Sharded(config) => {
                let address = config.chunk_address(&indices)?;
                let shard_key = join_key(
                    &self.array_dir,
                    &chunk_key(&meta.chunk_key_encoding, &address.shard_indices),
                );
                let index = match self.resolve_shard_index(
                    config,
                    &shard_key,
                    ReadKind::Data,
                    allow_remote_index,
                )? {
                    ShardIndexResolution::Ready(index) => index,
                    ShardIndexResolution::WouldBlock => return Ok(None),
                };
                let request_context = context(shard_key.clone());
                let Some(index) = index else {
                    return Ok(Some(ResolvedChunkRequest::Synthesized(PrefetchRequest {
                        context: request_context,
                        identity: config.index_read_identity(shard_key)?,
                        max_bytes: 0,
                    })));
                };
                let entry = index.entry(&address.inner_indices)?;
                let Some(identity) = index.payload_read_identity(entry)? else {
                    return Ok(Some(ResolvedChunkRequest::Synthesized(PrefetchRequest {
                        context: request_context,
                        identity: index.index_identity().clone(),
                        max_bytes: 0,
                    })));
                };
                let max_bytes = match &identity.range {
                    ReadRange::Exact { length, .. } => usize::try_from(*length).map_err(|_| {
                        ZarrFdwError::InvalidMetadata(format!(
                            "inner chunk range in shard '{shard_key}' exceeds this platform's index capacity"
                        ))
                    })?,
                    ReadRange::Whole | ReadRange::Suffix { .. } => {
                        return Err(ZarrFdwError::InvalidMetadata(format!(
                            "inner chunk in shard '{shard_key}' did not resolve to an exact byte range"
                        )));
                    }
                };
                if max_bytes > encoded_limit {
                    return Err(ZarrFdwError::InvalidMetadata(format!(
                        "inner chunk range in shard '{shard_key}' is {max_bytes} bytes, exceeding its encoded read limit of {encoded_limit}"
                    )));
                }
                if max_bytes > self.max_inflight_bytes {
                    return Err(ZarrFdwError::InvalidMetadata(format!(
                        "inner chunk range in shard '{shard_key}' is {max_bytes} bytes, exceeding max_inflight_bytes {}",
                        self.max_inflight_bytes
                    )));
                }
                Ok(Some(ResolvedChunkRequest::Fetch(PrefetchRequest {
                    context: request_context,
                    identity,
                    max_bytes,
                })))
            }
        }
    }

    fn schedule_chunk_request(&mut self, resolved: ResolvedChunkRequest) -> ZarrFdwResult<bool> {
        let result = match resolved {
            ResolvedChunkRequest::Synthesized(request) => self
                .prefetch
                .try_schedule_synthesized(request, CachedObject::Missing)
                .map(|source| (source, false))
                .map_err(|error| (error, true)),
            ResolvedChunkRequest::Fetch(request) => {
                let store = &self.store;
                let remote_get_calls = Arc::clone(&self.remote_data_get_calls);
                let remote_encoded_bytes = Arc::clone(&self.remote_data_encoded_bytes);
                let shard_payload_get_calls = Arc::clone(&self.remote_shard_payload_get_calls);
                let shard_payload_encoded_bytes =
                    Arc::clone(&self.remote_shard_payload_encoded_bytes);
                self.prefetch
                    .try_schedule(
                        request,
                        &mut self.compressed_cache,
                        move |identity, max_bytes| match identity.range.clone() {
                            ReadRange::Whole => {
                                let fetch =
                                    store.get_object_optional_owned(identity.key, max_bytes);
                                observe_data_fetch(fetch, remote_get_calls, remote_encoded_bytes)
                                    .boxed_local()
                            }
                            ReadRange::Exact { .. } | ReadRange::Suffix { .. } => {
                                let shard_key = identity.key.clone();
                                let fetch = store.get_object_range_owned(identity);
                                observe_shard_payload_fetch(
                                    fetch,
                                    shard_key,
                                    remote_get_calls,
                                    remote_encoded_bytes,
                                    shard_payload_get_calls,
                                    shard_payload_encoded_bytes,
                                )
                                .boxed_local()
                            }
                        },
                    )
                    .map(|source| (source, true))
                    .map_err(|error| (error, false))
            }
        };

        match result {
            Ok((source, has_payload_cache_lookup)) => {
                self.metrics.record_chunk_request();
                if has_payload_cache_lookup {
                    self.metrics
                        .record_cache_lookup(source == PrefetchSource::Cache);
                }
                Ok(true)
            }
            Err((ScheduleError::WindowFull(request), synthesized)) => {
                let request = if synthesized {
                    ResolvedChunkRequest::Synthesized(request)
                } else {
                    ResolvedChunkRequest::Fetch(request)
                };
                self.deferred_prefetch = Some(DeferredChunkRequest::Resolved(request));
                Ok(false)
            }
            Err((
                ScheduleError::RequestTooLarge {
                    request,
                    max_inflight_bytes,
                },
                _,
            )) => Err(ZarrFdwError::InvalidMetadata(format!(
                "object '{}' read limit {} exceeds max_inflight_bytes {max_inflight_bytes}",
                request.identity.key, request.max_bytes
            ))),
            Err((
                ScheduleError::CachedObjectTooLarge {
                    request,
                    actual_bytes,
                },
                _,
            )) => Err(ZarrFdwError::InvalidMetadata(format!(
                "cached object '{}' is {actual_bytes} bytes, exceeding its read limit of {}",
                request.identity.key, request.max_bytes
            ))),
        }
    }

    fn fill_prefetch_window(&mut self) -> ZarrFdwResult<()> {
        loop {
            let logical = if let Some(deferred) = self.deferred_prefetch.take() {
                match deferred {
                    DeferredChunkRequest::Logical(indices) => indices,
                    DeferredChunkRequest::Resolved(request) => {
                        if !self.schedule_chunk_request(request)? {
                            break;
                        }
                        continue;
                    }
                }
            } else {
                let mut indices = Vec::new();
                if !self.chunk_cursor.next_into(&mut indices) {
                    break;
                }
                indices
            };
            let Some(request) = self.chunk_request(logical.clone(), self.prefetch.is_empty())?
            else {
                self.deferred_prefetch = Some(DeferredChunkRequest::Logical(logical));
                break;
            };
            if !self.schedule_chunk_request(request)? {
                break;
            }
        }
        Ok(())
    }

    fn process_pending_interrupt(&mut self) -> ZarrFdwResult<()> {
        if postgres_interrupt_pending() {
            // No Rust future may remain owned by scan state when PostgreSQL's
            // cancellation path raises ERROR through the backend stack.
            self.prefetch.clear();
            self.deferred_prefetch = None;
            process_postgres_interrupts();
            // PostgreSQL can defer interrupts while they are held. Never
            // continue after dropping an already-advanced prefetch window.
            return Err(ZarrFdwError::InvalidMetadata(
                "query interruption was requested".to_string(),
            ));
        }
        Ok(())
    }

    fn next_prefetched_chunk(&mut self) -> ZarrFdwResult<Option<CachedObject>> {
        self.fill_prefetch_window()?;
        let evictions_before = self.compressed_cache.evictions();
        let outcome = {
            let runtime = &self.store.rt;
            let prefetch = &mut self.prefetch;
            let cache = &mut self.compressed_cache;
            runtime.block_on(prefetch.next_interruptible(cache, postgres_interrupt_pending))
        };
        self.metrics.record_cache_evictions(
            self.compressed_cache
                .evictions()
                .saturating_sub(evictions_before),
        );

        match outcome {
            PrefetchNext::Ready(result) => {
                self.current_chunk = result.request.context.logical_indices;
                self.current_object_key = result.request.context.object_key;
                self.metrics
                    .record_chunk_result(matches!(&result.object, CachedObject::Present(_)));
                Ok(Some(result.object))
            }
            PrefetchNext::FetchError { error, .. } => Err(error),
            PrefetchNext::Interrupted => {
                // All queued Rust futures have been dropped. Raise PostgreSQL's
                // canonical cancellation only after leaving Runtime::block_on.
                self.deferred_prefetch = None;
                self.process_pending_interrupt()?;
                Err(ZarrFdwError::InvalidMetadata(
                    "query interruption was requested".to_string(),
                ))
            }
            PrefetchNext::Empty => Ok(None),
        }
    }

    /// Decode `self.current_chunk`, priming the within-chunk
    /// index window from the active selection.
    fn load_chunk(&mut self, encoded: CachedObject) -> ZarrFdwResult<()> {
        self.process_pending_interrupt()?;
        let meta = self
            .axis_meta
            .as_ref()
            .expect("begin_scan must be called before iter_scan");
        let dt = self.dtype.expect("dtype set in begin_scan");
        let codec = self.codec.as_ref().expect("codec set in begin_scan");
        let ci = self.current_chunk.clone();
        debug_assert_eq!(self.sub_lo.len(), self.rank);
        debug_assert_eq!(self.sub_hi.len(), self.rank);
        debug_assert_eq!(self.sub_idx.len(), self.rank);

        // Effective (edge) chunk shape. Regular Zarr chunks retain the full
        // declared shape; `eff` only controls which logical cells are emitted.
        if ci.len() != self.rank {
            return Err(ZarrFdwError::InvalidMetadata(
                "chunk index rank does not match the array rank".to_string(),
            ));
        }
        let (storage_shape, storage_cells, expected) = checked_chunk_layout(meta, dt.itemsize())?;
        let mut eff = Vec::with_capacity(self.rank);
        for (axis, &ci_d) in ci.iter().enumerate() {
            let dim = meta.shape_extent(axis)?;
            let chunk_len = storage_shape[axis];
            let chunk_index = usize::try_from(ci_d).map_err(|_| {
                ZarrFdwError::InvalidMetadata(format!(
                    "chunk index for axis {axis} exceeds this platform's index capacity"
                ))
            })?;
            let start = chunk_index.checked_mul(chunk_len).ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!("chunk start offset overflow on axis {axis}"))
            })?;
            if start >= dim {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "chunk index {ci_d} starts outside array dimension {axis} with extent {dim}"
                )));
            }
            let remaining = dim.checked_sub(start).ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "chunk start offset exceeds array dimension {axis}"
                ))
            })?;
            eff.push(remaining.min(chunk_len));
        }

        if eff.contains(&0) {
            // zero-length dimension: yield nothing from this chunk
            self.sub_lo = (0..self.rank).map(|_| 1).collect();
            self.sub_hi = vec![0; self.rank];
            self.chunk_bytes.clear();
            return Ok(());
        }

        // Regular edge chunks retain the declared chunk shape. Use that full
        // shape for byte validation and C-order strides; `eff` ignores the
        // out-of-array region when deciding which cells to emit.
        let object_key = self.current_object_key.clone();
        let (decoded, synthesized_fill) = match encoded {
            CachedObject::Present(raw) => {
                let started = Instant::now();
                let decoded = self
                    .store
                    .rt
                    .block_on(codec.decode_interruptible(
                        raw.as_ref().to_vec(),
                        &storage_shape,
                        dt.itemsize(),
                        postgres_interrupt_pending,
                    ))
                    .map_err(|error| {
                        ZarrFdwError::ReadError(std::io::Error::other(format!(
                            "chunk '{object_key}': {error}"
                        )))
                    })?;
                self.metrics.record_decompression_time(started.elapsed());
                let decoded = match decoded {
                    CodecDecode::Decoded(decoded) => decoded,
                    CodecDecode::Interrupted => {
                        self.process_pending_interrupt()?;
                        return Err(ZarrFdwError::InvalidMetadata(
                            "query interruption was requested".to_string(),
                        ));
                    }
                };
                (decoded, false)
            }
            CachedObject::Missing => (
                filled_chunk_bytes(self.fill_bytes.as_deref(), storage_cells, &object_key)?,
                true,
            ),
        };
        require_exact_decoded_len(&object_key, decoded.len(), expected)?;
        self.metrics
            .record_decoded_bytes(ReadKind::Data, decoded.len(), synthesized_fill);
        self.chunk_bytes.clear();
        self.chunk_bytes.try_reserve_exact(expected).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "could not allocate a decoded chunk of {expected} bytes"
            ))
        })?;
        self.chunk_bytes.extend_from_slice(&decoded[..expected]);
        self.chunk_shape = storage_shape;

        // within-chunk index window for this chunk
        for d in 0..self.rank {
            let chunk_len = self.chunk_shape[d];
            let chunk_index = usize::try_from(ci[d]).map_err(|_| {
                ZarrFdwError::InvalidMetadata(format!(
                    "chunk index for axis {d} exceeds this platform's index capacity"
                ))
            })?;
            let base = chunk_index.checked_mul(chunk_len).ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!("chunk start offset overflow on axis {d}"))
            })?;
            let hi_def = eff[d].saturating_sub(1);
            match &self.selection.axis_bounds()[d] {
                Some(b) => {
                    self.sub_lo[d] = b.start.saturating_sub(base).min(hi_def);
                    self.sub_hi[d] = b.end.saturating_sub(base).min(hi_def);
                }
                None => {
                    self.sub_lo[d] = 0;
                    self.sub_hi[d] = hi_def;
                }
            }
        }
        self.sub_idx.clone_from(&self.sub_lo);
        Ok(())
    }

    /// Emit the row at the current `sub_idx`, then advance to the next cell.
    /// Returns `Ok(())`; the row should be consumed regardless.
    fn emit_and_advance(&mut self, row: &mut Row) -> ZarrFdwResult<()> {
        let decode_started = Instant::now();
        let value_cell = self.value_cell_at_cursor()?;
        self.metrics.record_decoding_time(decode_started.elapsed());
        if self.capture_spatial_indices {
            let mut global_indices = self.last_emitted_indices.take().unwrap_or_default();
            global_indices.clear();
            global_indices.try_reserve(self.rank).map_err(|_| {
                ZarrFdwError::InvalidMetadata(format!(
                    "could not allocate {0} emitted array indexes",
                    self.rank
                ))
            })?;
            for axis in 0..self.rank {
                global_indices.push(self.global_index_at_cursor(axis)?);
            }
            self.last_emitted_indices = Some(global_indices);
        }

        for col in &self.tgt_cols {
            let cell = self.column_cell_at_cursor(&col.name, value_cell.as_ref())?;
            row.push(col.name.as_str(), cell);
        }

        self.metrics.record_cells(1, None);
        self.metrics.record_tuple_emitted();
        self.advance_cursor();
        Ok(())
    }

    fn reduce_current_cell(&mut self) -> ZarrFdwResult<()> {
        let aggregate_started = Instant::now();
        let decode_started = Instant::now();
        let value_cell = self.value_cell_at_cursor()?;
        self.metrics.record_decoding_time(decode_started.elapsed());
        let mut matches = true;
        for qual in &self.aggregate_quals {
            let cell = self.column_cell_at_cursor(&qual.field, value_cell.as_ref())?;
            if !qual_matches(qual, cell.as_ref())? {
                matches = false;
                break;
            }
        }
        if !matches {
            self.metrics.record_cells(1, Some(0));
            self.metrics
                .record_aggregate_time(aggregate_started.elapsed());
            self.advance_cursor();
            return Ok(());
        }

        let values = self
            .aggregate_defs
            .iter()
            .map(|aggregate| {
                aggregate
                    .column
                    .as_ref()
                    .map(|column| self.column_cell_at_cursor(&column.name, value_cell.as_ref()))
                    .transpose()
                    .map(Option::flatten)
            })
            .collect::<ZarrFdwResult<Vec<_>>>()?;
        let value_refs = values.iter().map(Option::as_ref).collect::<Vec<_>>();
        self.aggregate_reducer
            .as_mut()
            .expect("aggregate reducer set in begin_aggregate_scan")
            .observe(&value_refs)?;
        self.metrics.record_cells(1, Some(1));
        self.metrics
            .record_aggregate_time(aggregate_started.elapsed());
        self.advance_cursor();
        Ok(())
    }

    fn flush_persistent_stats_at_eof(&mut self) {
        let metrics = self.metrics_snapshot();
        let encoded = metrics
            .total_encoded_bytes()
            .saturating_sub(self.flushed_encoded_bytes);
        let cells = metrics
            .logical_cells_examined
            .saturating_sub(self.flushed_cells);
        let tuples = metrics.tuples_emitted.saturating_sub(self.flushed_tuples);
        let as_i64 = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
        if encoded > 0 {
            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, as_i64(encoded));
        }
        if cells > 0 {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, as_i64(cells));
        }
        if tuples > 0 {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, as_i64(tuples));
        }
        self.flushed_encoded_bytes = metrics.total_encoded_bytes();
        self.flushed_cells = metrics.logical_cells_examined;
        self.flushed_tuples = metrics.tuples_emitted;
        self.rows_out = 0;
    }

    fn metrics_snapshot(&self) -> ZarrScanMetrics {
        let mut metrics = self.metrics.clone();
        metrics.data_get_calls = metrics
            .data_get_calls
            .saturating_add(self.remote_data_get_calls.load(Ordering::Relaxed));
        metrics.data_encoded_bytes = metrics
            .data_encoded_bytes
            .saturating_add(self.remote_data_encoded_bytes.load(Ordering::Relaxed));
        metrics.shard_payload_get_calls = metrics
            .shard_payload_get_calls
            .saturating_add(self.remote_shard_payload_get_calls.load(Ordering::Relaxed));
        metrics.shard_payload_encoded_bytes = metrics.shard_payload_encoded_bytes.saturating_add(
            self.remote_shard_payload_encoded_bytes
                .load(Ordering::Relaxed),
        );
        metrics
    }

    /// Make the next selected cell ready for a consumer.
    ///
    /// This is the shared chunk-execution state machine for tuple and aggregate
    /// scans: preserve an already pending cell, otherwise fetch/cache/decode
    /// chunks lazily until one has a non-empty selected window. Consumers own
    /// cell decoding, residual filtering, cursor advancement, and EOF output.
    fn ensure_cell_ready(&mut self) -> ZarrFdwResult<bool> {
        loop {
            if self.pending {
                return Ok(true);
            }
            let Some(encoded) = self.next_prefetched_chunk()? else {
                return Ok(false);
            };
            self.load_chunk(encoded)?;
            let empty_window = (0..self.rank).any(|axis| self.sub_lo[axis] > self.sub_hi[axis]);
            if empty_window {
                continue;
            }
            self.pending = true;
            return Ok(true);
        }
    }

    fn current_cell_matches_selection(&self) -> ZarrFdwResult<bool> {
        if self.dimension_selectors.is_empty() && self.bound_call_dimension_selectors.is_empty() {
            return Ok(true);
        }
        for axis in 0..self.rank {
            let index = self.global_index_at_cursor(axis)?;
            if !self
                .dimension_selectors
                .matches_axis_index(axis, index, &self.coords)?
                || !self.bound_call_dimension_selectors.matches_axis_index(
                    axis,
                    index,
                    &self.coords,
                )?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn ensure_selected_cell_ready(&mut self) -> ZarrFdwResult<bool> {
        let mut skipped = 0usize;
        while self.ensure_cell_ready()? {
            if self.current_cell_matches_selection()? {
                return Ok(true);
            }
            let matched = (!self.aggregate_defs.is_empty()).then_some(0);
            self.metrics.record_cells(1, matched);
            self.advance_cursor();
            skipped = skipped.saturating_add(1);
            if skipped.is_multiple_of(SELECTOR_INTERRUPT_POLL_CELLS) {
                self.process_pending_interrupt()?;
            }
        }
        Ok(false)
    }

    fn iter_aggregate_scan(&mut self, row: &mut Row) -> ZarrFdwResult<Option<()>> {
        if self.aggregate_emitted {
            self.flush_persistent_stats_at_eof();
            return Ok(None);
        }

        while self.ensure_selected_cell_ready()? {
            self.reduce_current_cell()?;
        }

        let results = self
            .aggregate_reducer
            .take()
            .expect("aggregate reducer set in begin_aggregate_scan")
            .finish()?;
        row.clear();
        for (alias, cell) in results {
            row.push(&alias, cell);
        }
        self.aggregate_emitted = true;
        self.rows_out = 1;
        self.metrics.record_tuple_emitted();
        Ok(Some(()))
    }

    fn iter_scalar_scan(&mut self, row: &mut Row) -> ZarrFdwResult<Option<()>> {
        if !self.ensure_selected_cell_ready()? {
            self.flush_persistent_stats_at_eof();
            return Ok(None);
        }
        row.clear();
        self.emit_and_advance(row)?;
        self.rows_out += 1;
        Ok(Some(()))
    }
}

fn cell_to_f64_bounds(cell: &Cell, is_time: bool, spec: TimeSpec) -> Option<(f64, f64)> {
    if is_time {
        match cell {
            Cell::Timestamptz(v) => spec.pg_micros_to_raw_bounds((*v).into_inner()),
            Cell::Timestamp(v) => spec.pg_micros_to_raw_bounds((*v).into_inner()),
            _ => None,
        }
    } else {
        let value = match cell {
            Cell::F64(v) => Some(*v),
            Cell::F32(v) => Some(*v as f64),
            Cell::I64(v) => Some(*v as f64),
            Cell::I32(v) => Some(*v as f64),
            Cell::I16(v) => Some(*v as f64),
            Cell::I8(v) => Some(*v as f64),
            _ => None,
        }?;
        // Coordinate vectors are finite, while PostgreSQL gives NaN a total
        // ordering above every non-NaN float. Binary-search range math uses
        // ordinary IEEE comparisons, so a NaN bound could narrow the scan
        // incorrectly (for example, every finite x satisfies x < NaN in
        // PostgreSQL). Disable pruning and let the exact/local qual decide.
        if value.is_nan() {
            return None;
        }
        Some((value, value))
    }
}

/// Translate a single qual into an optional `(lo, hi)` value range over an
/// axis's coordinate space. Returns `None` when the qual cannot be (or should
/// not be) used for pruning.
fn qual_to_range(
    q: &Qual,
    is_time: bool,
    spec: TimeSpec,
) -> ZarrFdwResult<Option<(Option<f64>, Option<f64>)>> {
    let evaluated_value = q.param.as_ref().map(|_| q.evaluated_value());
    let value = match evaluated_value.as_ref() {
        None => &q.value,
        Some(ParamValue::Value(value)) => value,
        // NULL comparisons cannot select a row, but a full scan is the safe
        // pruning choice for normal scans whose clauses PostgreSQL rechecks.
        Some(ParamValue::Null | ParamValue::Unevaluated) => return Ok(None),
    };

    if q.use_or {
        // `IN (...)` -> bounding box over the values (over-approximated)
        let Value::Array(cells) = value else {
            return Ok(None);
        };
        let mut lo = f64::INFINITY;
        let mut hi = f64::NEG_INFINITY;
        let mut found = false;
        for c in cells {
            if let Some((value_lo, value_hi)) = cell_to_f64_bounds(c, is_time, spec) {
                lo = lo.min(value_lo);
                hi = hi.max(value_hi);
                found = true;
            }
        }
        return if found {
            Ok(Some((Some(lo), Some(hi))))
        } else {
            Ok(None)
        };
    }

    let Value::Cell(cell) = value else {
        return Ok(None);
    };
    let Some((value_lo, value_hi)) = cell_to_f64_bounds(cell, is_time, spec) else {
        return Ok(None);
    };
    let r = match q.operator.as_str() {
        "=" => (Some(value_lo), Some(value_hi)),
        ">" => (Some(value_hi), None),
        ">=" => (Some(value_lo), None),
        "<" => (None, Some(value_lo)),
        "<=" => (None, Some(value_hi)),
        // `<>`/LIKE/etc. cannot prune this axis
        _ => return Ok(None),
    };
    Ok(Some(r))
}

fn array_parent_path(array_path: &str) -> &str {
    array_path
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or_default()
}

fn select_array_metadata_document(
    array_path: &str,
    v3: Option<Vec<u8>>,
    v2: Option<Vec<u8>>,
) -> ZarrFdwResult<ArrayMetadataDocument> {
    match (v3, v2) {
        (Some(_), Some(_)) => Err(ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' contains both zarr.json and .zarray metadata"
        ))),
        (Some(bytes), None) => Ok(ArrayMetadataDocument::V3(bytes)),
        (None, Some(bytes)) => Ok(ArrayMetadataDocument::V2(bytes)),
        (None, None) => Err(ZarrFdwError::InvalidMetadata(format!(
            "array '{array_path}' contains neither zarr.json nor .zarray metadata"
        ))),
    }
}

fn read_optional_metadata_object(
    store: &ZarrStore,
    metrics: &mut ZarrScanMetrics,
    key: &str,
) -> ZarrFdwResult<Option<Vec<u8>>> {
    let bytes = store.get_object_optional_sync(key, MAX_METADATA_OBJECT_BYTES)?;
    metrics.record_remote_get(ReadKind::Metadata, bytes.as_ref().map(Vec::len));
    Ok(bytes)
}

/// Read and normalize exactly one array node without exposing format-specific
/// metadata to the scan executor.
fn read_array_node(
    store: &ZarrStore,
    metrics: &mut ZarrScanMetrics,
    array_path: &str,
) -> ZarrFdwResult<ArrayNode> {
    let v3_key = join_key(array_path, "zarr.json");
    let v2_key = join_key(array_path, ".zarray");
    let v3 = read_optional_metadata_object(store, metrics, &v3_key)?;
    let v2 = read_optional_metadata_object(store, metrics, &v2_key)?;

    let node = match select_array_metadata_document(array_path, v3, v2)? {
        ArrayMetadataDocument::V3(bytes) => match parse_v3_node(&bytes)? {
            NodeMeta::Array(node) => Ok(*node),
            NodeMeta::Group(_) => Err(ZarrFdwError::InvalidMetadata(format!(
                "node '{array_path}' is a Zarr v3 group, expected an array"
            ))),
        },
        ArrayMetadataDocument::V2(bytes) => {
            let attributes =
                read_array_attributes_optional(store, metrics, array_path)?.unwrap_or_default();
            parse_v2_array(&bytes, attributes)
        }
    }?;
    validate_array_ancestors(store, metrics, array_path, node.format)?;
    Ok(node)
}

fn read_ome_group_attributes(
    store: &ZarrStore,
    metrics: &mut ZarrScanMetrics,
    group_path: &str,
) -> ZarrFdwResult<Map<String, JsonValue>> {
    let v3_key = join_key(group_path, "zarr.json");
    let v2_group_key = join_key(group_path, ".zgroup");
    let v2_array_key = join_key(group_path, ".zarray");
    let v3 = read_optional_metadata_object(store, metrics, &v3_key)?;
    let v2_group = read_optional_metadata_object(store, metrics, &v2_group_key)?;
    let v2_array = read_optional_metadata_object(store, metrics, &v2_array_key)?;
    if v2_group.is_some() || v2_array.is_some() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "OME-Zarr group '{}' must be a Zarr v3 group",
            if group_path.is_empty() {
                "/"
            } else {
                group_path
            }
        )));
    }
    let bytes = v3.ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!(
            "OME-Zarr group '{}' must be a Zarr v3 group",
            if group_path.is_empty() {
                "/"
            } else {
                group_path
            }
        ))
    })?;
    let group = match parse_v3_node(&bytes)? {
        NodeMeta::Group(group) => group,
        NodeMeta::Array(_) => {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "OME-Zarr group '{}' must be a Zarr v3 group",
                if group_path.is_empty() {
                    "/"
                } else {
                    group_path
                }
            )));
        }
    };
    validate_array_ancestors(store, metrics, group_path, ZarrFormat::V3)?;
    Ok(group.attributes)
}

fn array_ancestor_paths(array_path: &str) -> Vec<String> {
    let components = array_path
        .split('/')
        .filter(|component| !component.is_empty())
        .collect::<Vec<_>>();
    if components.is_empty() {
        return Vec::new();
    }
    let mut paths = vec![String::new()];
    let mut current = String::new();
    for component in components.iter().take(components.len() - 1) {
        current = join_key(&current, component);
        paths.push(current.clone());
    }
    paths
}

fn validate_array_ancestors(
    store: &ZarrStore,
    metrics: &mut ZarrScanMetrics,
    array_path: &str,
    format: ZarrFormat,
) -> ZarrFdwResult<()> {
    for ancestor in array_ancestor_paths(array_path) {
        let v3_key = join_key(&ancestor, "zarr.json");
        let v2_group_key = join_key(&ancestor, ".zgroup");
        let v2_array_key = join_key(&ancestor, ".zarray");
        let v3 = read_optional_metadata_object(store, metrics, &v3_key)?;
        let v2_group = read_optional_metadata_object(store, metrics, &v2_group_key)?;
        let v2_array = read_optional_metadata_object(store, metrics, &v2_array_key)?;
        if v2_array.is_some() {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "ancestor '{}' is an array, expected a group",
                if ancestor.is_empty() { "/" } else { &ancestor }
            )));
        }
        match format {
            ZarrFormat::V3 => {
                if v2_group.is_some() {
                    return Err(ZarrFdwError::InvalidMetadata(format!(
                        "Zarr v3 array '{array_path}' has a Zarr v2 ancestor group '{}'",
                        if ancestor.is_empty() { "/" } else { &ancestor }
                    )));
                }
                let bytes = v3.ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(format!(
                        "Zarr v3 array '{array_path}' requires explicit zarr.json metadata on ancestor group '{}'",
                        if ancestor.is_empty() { "/" } else { &ancestor }
                    ))
                })?;
                if !matches!(parse_v3_node(&bytes)?, NodeMeta::Group(_)) {
                    return Err(ZarrFdwError::InvalidMetadata(format!(
                        "ancestor '{}' is not a Zarr v3 group",
                        if ancestor.is_empty() { "/" } else { &ancestor }
                    )));
                }
            }
            ZarrFormat::V2 => {
                if v3.is_some() {
                    return Err(ZarrFdwError::InvalidMetadata(format!(
                        "Zarr v2 array '{array_path}' has a Zarr v3 ancestor group '{}'",
                        if ancestor.is_empty() { "/" } else { &ancestor }
                    )));
                }
                if let Some(bytes) = v2_group {
                    parse_v2_group(&bytes, Map::new())?;
                }
            }
        }
    }
    Ok(())
}

fn validate_ome_hierarchy_versions(
    store: &ZarrStore,
    metrics: &mut ZarrScanMetrics,
    array_path: &str,
) -> ZarrFdwResult<()> {
    for ancestor in array_ancestor_paths(array_path) {
        let key = join_key(&ancestor, "zarr.json");
        let bytes = read_optional_metadata_object(store, metrics, &key)?.ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "OME-Zarr array '{array_path}' requires explicit zarr.json metadata on ancestor group '{}'",
                if ancestor.is_empty() { "/" } else { &ancestor }
            ))
        })?;
        let attributes = match parse_v3_node(&bytes)? {
            NodeMeta::Group(group) => group.attributes,
            NodeMeta::Array(_) => {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "OME-Zarr ancestor '{}' is an array, expected a group",
                    if ancestor.is_empty() { "/" } else { &ancestor }
                )));
            }
        };
        validate_optional_ome_05_attributes(
            if ancestor.is_empty() { "/" } else { &ancestor },
            Some(&attributes),
        )?;
    }
    Ok(())
}

fn codec_pipeline_for_execution(meta: &ArrayMeta) -> ZarrFdwResult<CodecPipeline> {
    if meta.zarr_format == 2 {
        CodecPipeline::from_v2(&meta.compressor)
    } else {
        Ok(meta.codec_pipeline.clone())
    }
}

fn read_coordinate_metadata(
    store: &ZarrStore,
    metrics: &mut ZarrScanMetrics,
    prefix: &str,
    name: &str,
    expected_length: u64,
) -> ZarrFdwResult<(ArrayNode, Option<f64>)> {
    let dir = join_key(prefix, name);
    let node =
        read_array_node(store, metrics, &dir).map_err(|e| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: format!("coordinate array metadata: {e}"),
        })?;
    let meta = &node.meta;
    meta.validate_coordinate()
        .map_err(|e| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: e.to_string(),
        })?;

    let itemsize =
        coordinate_itemsize(&meta.dtype).map_err(|error| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: format!("coordinate array dtype: {error}"),
        })?;
    let fill_value = coord_fill_value_to_f64(&meta.dtype, &meta.fill_value).map_err(|error| {
        ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: format!("coordinate array fill value: {error}"),
        }
    })?;
    let coordinate_len =
        meta.shape_extent(0)
            .map_err(|error| ZarrFdwError::CoordinateReadError {
                axis: name.to_string(),
                error: error.to_string(),
            })?;
    if coordinate_len > MAX_COORDINATE_VALUES {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: format!(
                "coordinate array has {coordinate_len} values, exceeding the safety limit of {MAX_COORDINATE_VALUES}"
            ),
        });
    }
    let (storage_shape, _, _) = checked_chunk_layout(meta, itemsize).map_err(|error| {
        ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: error.to_string(),
        }
    })?;
    if storage_shape[0] > MAX_COORDINATE_VALUES {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: format!(
                "coordinate chunk has {} values, exceeding the safety limit of {MAX_COORDINATE_VALUES}",
                storage_shape[0]
            ),
        });
    }
    codec_pipeline_for_execution(meta).map_err(|error| ZarrFdwError::CoordinateReadError {
        axis: name.to_string(),
        error: format!("coordinate array codec pipeline: {error}"),
    })?;
    if meta.shape[0] != expected_length {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: format!(
                "coordinate array has shape {} but the dimension has shape {expected_length}",
                meta.shape[0]
            ),
        });
    }
    Ok((node, fill_value))
}

/// Read a validated 1D numeric coordinate array and return its values as `f64`.
fn read_coordinate_values(
    fdw: &mut ZarrFdw,
    prefix: &str,
    name: &str,
    coordinate: &ArrayNode,
    fill_value: Option<f64>,
) -> ZarrFdwResult<Vec<f64>> {
    let dir = join_key(prefix, name);
    let meta = &coordinate.meta;

    let coordinate_len = meta
        .shape_extent(0)
        .map_err(|e| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: e.to_string(),
        })?;
    if coordinate_len > MAX_COORDINATE_VALUES {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: format!(
                "coordinate array has {coordinate_len} values, exceeding the safety limit of {MAX_COORDINATE_VALUES}"
            ),
        });
    }
    let itemsize =
        coordinate_itemsize(&meta.dtype).map_err(|e| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: e.to_string(),
        })?;
    let (storage_shape, _, expected_bytes) =
        checked_chunk_layout(meta, itemsize).map_err(|e| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: e.to_string(),
        })?;
    let chunk_len = storage_shape[0];
    if chunk_len > MAX_COORDINATE_VALUES {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: format!(
                "coordinate chunk has {chunk_len} values, exceeding the safety limit of {MAX_COORDINATE_VALUES}"
            ),
        });
    }

    let codec = codec_pipeline_for_execution(meta)?;
    let per_axis = meta.chunks_per_axis();
    let chunk_count =
        usize::try_from(per_axis[0]).map_err(|_| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: "coordinate chunk count exceeds this platform's index capacity".to_string(),
        })?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(coordinate_len)
        .map_err(|_| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: format!("could not allocate {coordinate_len} coordinate values"),
        })?;
    for chunk_index in 0..chunk_count {
        let ci = u64::try_from(chunk_index).map_err(|_| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: "coordinate chunk index exceeds the Zarr u64 index capacity".to_string(),
        })?;
        let encoded_limit = codec.encoded_read_limit(expected_bytes)?;
        let (object_key, encoded) = match &meta.storage_layout {
            StorageLayout::Direct => {
                let chunk = chunk_key(&meta.chunk_key_encoding, &[ci]);
                let object_key = join_key(&dir, &chunk);
                let encoded = fdw
                    .store
                    .get_object_optional_sync(&object_key, encoded_limit)?;
                fdw.metrics
                    .record_remote_get(ReadKind::Coordinate, encoded.as_ref().map(Vec::len));
                (object_key, encoded)
            }
            StorageLayout::Sharded(config) => {
                let address = config.chunk_address(&[ci])?;
                let shard_key = join_key(
                    &dir,
                    &chunk_key(&meta.chunk_key_encoding, &address.shard_indices),
                );
                let index = match fdw.resolve_shard_index(
                    config,
                    &shard_key,
                    ReadKind::Coordinate,
                    true,
                )? {
                    ShardIndexResolution::Ready(index) => index,
                    ShardIndexResolution::WouldBlock => {
                        return Err(ZarrFdwError::CoordinateReadError {
                            axis: name.to_string(),
                            error: "coordinate shard index resolution unexpectedly blocked"
                                .to_string(),
                        });
                    }
                };
                match index {
                    None => (shard_key, None),
                    Some(index) => {
                        let entry = index.entry(&address.inner_indices)?;
                        match index.payload_read_identity(entry)? {
                            None => (shard_key, None),
                            Some(identity) => {
                                let payload_bytes = match &identity.range {
                                    ReadRange::Exact { length, .. } => usize::try_from(*length)
                                        .map_err(|_| ZarrFdwError::CoordinateReadError {
                                            axis: name.to_string(),
                                            error: format!(
                                                "inner chunk range in shard '{shard_key}' exceeds this platform's index capacity"
                                            ),
                                        })?,
                                    ReadRange::Whole | ReadRange::Suffix { .. } => {
                                        return Err(ZarrFdwError::CoordinateReadError {
                                            axis: name.to_string(),
                                            error: format!(
                                                "inner chunk in shard '{shard_key}' did not resolve to an exact byte range"
                                            ),
                                        });
                                    }
                                };
                                if payload_bytes > encoded_limit {
                                    return Err(ZarrFdwError::CoordinateReadError {
                                        axis: name.to_string(),
                                        error: format!(
                                            "inner chunk range in shard '{shard_key}' is {payload_bytes} bytes, exceeding its encoded read limit of {encoded_limit}"
                                        ),
                                    });
                                }
                                if payload_bytes > fdw.max_inflight_bytes {
                                    return Err(ZarrFdwError::CoordinateReadError {
                                        axis: name.to_string(),
                                        error: format!(
                                            "inner chunk range in shard '{shard_key}' is {payload_bytes} bytes, exceeding max_inflight_bytes {}",
                                            fdw.max_inflight_bytes
                                        ),
                                    });
                                }
                                fdw.metrics.record_remote_request(ReadKind::Coordinate);
                                let response = fdw.store.get_object_range_sync(identity)?;
                                let response_bytes =
                                    response.as_ref().map(|response| response.bytes.len());
                                if let Some(bytes) = response_bytes {
                                    fdw.metrics
                                        .record_remote_response_bytes(ReadKind::Coordinate, bytes);
                                }
                                fdw.metrics.record_shard_payload_get(response_bytes);
                                let response = response.ok_or_else(|| {
                                    ZarrFdwError::CoordinateReadError {
                                        axis: name.to_string(),
                                        error: format!(
                                            "indexed shard object '{shard_key}' disappeared before its payload range was read"
                                        ),
                                    }
                                })?;
                                (shard_key, Some(response.bytes))
                            }
                        }
                    }
                }
            }
        };
        let decoded_values = match encoded {
            Some(raw) => {
                let started = Instant::now();
                let decoded = fdw
                    .store
                    .rt
                    .block_on(codec.decode_interruptible(
                        raw,
                        &storage_shape,
                        itemsize,
                        postgres_interrupt_pending,
                    ))
                    .map_err(|e| ZarrFdwError::CoordinateReadError {
                        axis: name.to_string(),
                        error: format!("chunk '{object_key}': {e}"),
                    })?;
                let decoded = match decoded {
                    CodecDecode::Decoded(decoded) => decoded,
                    CodecDecode::Interrupted => {
                        process_postgres_interrupts();
                        return Err(ZarrFdwError::CoordinateReadError {
                            axis: name.to_string(),
                            error: "query interruption was requested".to_string(),
                        });
                    }
                };
                fdw.metrics.record_decompression_time(started.elapsed());
                fdw.metrics
                    .record_decoded_bytes(ReadKind::Coordinate, decoded.len(), false);
                coord_bytes_to_f64(&meta.dtype, &decoded[..expected_bytes])?
            }
            None => {
                fdw.metrics
                    .record_decoded_bytes(ReadKind::Coordinate, expected_bytes, true);
                filled_coordinate_values(fill_value, chunk_len, &object_key, name)?
            }
        };
        let start = chunk_index.checked_mul(chunk_len).ok_or_else(|| {
            ZarrFdwError::CoordinateReadError {
                axis: name.to_string(),
                error: "coordinate chunk start offset overflow".to_string(),
            }
        })?;
        let remaining =
            coordinate_len
                .checked_sub(start)
                .ok_or_else(|| ZarrFdwError::CoordinateReadError {
                    axis: name.to_string(),
                    error: format!(
                        "coordinate chunk '{object_key}' starts beyond the declared array length"
                    ),
                })?;
        let effective_len = remaining.min(chunk_len);
        values.extend_from_slice(&decoded_values[..effective_len]);
    }
    Ok(values)
}

fn read_array_attributes_optional(
    store: &ZarrStore,
    metrics: &mut ZarrScanMetrics,
    array_dir: &str,
) -> ZarrFdwResult<Option<Map<String, JsonValue>>> {
    let key = join_key(array_dir, ".zattrs");
    let Some(bytes) = store.get_object_optional_sync(&key, MAX_METADATA_OBJECT_BYTES)? else {
        metrics.record_remote_get(ReadKind::Metadata, None);
        return Ok(None);
    };
    metrics.record_remote_get(ReadKind::Metadata, Some(bytes.len()));
    let value = serde_json::from_slice::<JsonValue>(&bytes).map_err(|error| {
        ZarrFdwError::InvalidMetadata(format!("could not parse '{key}': {error}"))
    })?;
    let attributes = value.as_object().cloned().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!("'{key}' must contain a JSON object"))
    })?;
    Ok(Some(attributes))
}

fn parse_boolean_option(name: &str, value: &str) -> ZarrFdwResult<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ZarrFdwError::InvalidOptionValue {
            option: name.to_string(),
            message: "must be 'true' or 'false'".to_string(),
        }),
    }
}

fn boolean_table_option(options: &HashMap<String, String>, name: &str) -> ZarrFdwResult<bool> {
    options
        .get(name)
        .map(|value| parse_boolean_option(name, value))
        .transpose()
        .map(|value| value.unwrap_or(false))
}

fn bounded_server_usize_option(
    options: &HashMap<String, String>,
    name: &str,
    default: usize,
    min: usize,
    max: usize,
) -> ZarrFdwResult<usize> {
    let Some(raw) = options.get(name) else {
        return Ok(default);
    };
    let value = raw
        .parse::<usize>()
        .map_err(|_| ZarrFdwError::InvalidOptionValue {
            option: name.to_string(),
            message: format!("must be an integer between {min} and {max}"),
        })?;
    if !(min..=max).contains(&value) {
        return Err(ZarrFdwError::InvalidOptionValue {
            option: name.to_string(),
            message: format!("must be between {min} and {max}"),
        });
    }
    Ok(value)
}

fn compressed_cache_bytes_option(options: &HashMap<String, String>) -> ZarrFdwResult<usize> {
    let Some(raw) = options.get(OPT_COMPRESSED_CACHE_BYTES) else {
        return Ok(DEFAULT_COMPRESSED_CACHE_BYTES);
    };
    let value = raw
        .parse::<usize>()
        .map_err(|_| ZarrFdwError::InvalidOptionValue {
            option: OPT_COMPRESSED_CACHE_BYTES.to_string(),
            message: format!("must be an integer between 0 and {MAX_COMPRESSED_CACHE_BYTES}"),
        })?;
    if value > MAX_COMPRESSED_CACHE_BYTES {
        return Err(ZarrFdwError::InvalidOptionValue {
            option: OPT_COMPRESSED_CACHE_BYTES.to_string(),
            message: format!("must be between 0 and {MAX_COMPRESSED_CACHE_BYTES}"),
        });
    }
    Ok(value)
}

fn scalable_execution_options(
    options: &HashMap<String, String>,
) -> ZarrFdwResult<(usize, usize, usize)> {
    Ok((
        bounded_server_usize_option(
            options,
            OPT_MAX_CONCURRENT_READS,
            DEFAULT_MAX_CONCURRENT_READS,
            1,
            MAX_CONCURRENT_READS,
        )?,
        bounded_server_usize_option(
            options,
            OPT_MAX_INFLIGHT_BYTES,
            DEFAULT_MAX_INFLIGHT_BYTES,
            MIN_MAX_INFLIGHT_BYTES,
            MAX_MAX_INFLIGHT_BYTES,
        )?,
        compressed_cache_bytes_option(options)?,
    ))
}

fn validate_time_from_attrs_options(
    time_from_attrs: bool,
    has_time_unit: bool,
    has_time_origin: bool,
) -> ZarrFdwResult<()> {
    if time_from_attrs && (has_time_unit || has_time_origin) {
        return Err(ZarrFdwError::InvalidOptionValue {
            option: OPT_TIME_FROM_ATTRS.to_string(),
            message: format!("cannot be combined with '{OPT_TIME_UNIT}' or '{OPT_TIME_ORIGIN}'"),
        });
    }
    Ok(())
}

fn option_value(options: &[Option<String>], name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    options
        .iter()
        .flatten()
        .find_map(|kv| kv.strip_prefix(&prefix).map(|v| v.to_string()))
}

impl ForeignDataWrapper<ZarrFdwError> for ZarrFdw {
    fn new(server: ForeignServer) -> ZarrFdwResult<Self> {
        let (configured_max_concurrent_reads, max_inflight_bytes, compressed_cache_bytes) =
            scalable_execution_options(&server.options)?;
        let store = ZarrStore::new(&server)?;
        let max_concurrent_reads =
            store.effective_max_concurrent_reads(configured_max_concurrent_reads);
        let prefetch = OrderedPrefetch::new(
            max_concurrent_reads,
            max_inflight_bytes,
            INTERRUPT_POLL_INTERVAL,
        )
        .map_err(|error| ZarrFdwError::InvalidOptionValue {
            option: OPT_MAX_CONCURRENT_READS.to_string(),
            message: error.to_string(),
        })?;
        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);
        Ok(Self {
            store,
            tgt_cols: Vec::new(),
            array_dir: String::new(),
            axes: Vec::new(),
            array_attributes: Map::new(),
            selected_ome_level: None,
            axis_roles: Vec::new(),
            rank: 0,
            axis_meta: None,
            dtype: None,
            codec: None,
            scientific_decoder: None,
            fill_bytes: None,
            coords: Vec::new(),
            selection: Selection::default(),
            dimension_selectors: BoundDimensionSelectors::default(),
            call_dimension_selectors: DimensionSelectors::default(),
            bound_call_dimension_selectors: BoundDimensionSelectors::default(),
            chunk_cursor: ChunkIndexCursor::default(),
            current_chunk: Vec::new(),
            current_object_key: String::new(),
            deferred_prefetch: None,
            prefetch,
            compressed_cache: CompressedChunkCache::new(
                compressed_cache_bytes,
                MAX_COMPRESSED_CACHE_ENTRIES,
            ),
            shard_index_cache: ShardIndexCache::new(0, 0),
            payload_cache_bytes: compressed_cache_bytes,
            shard_index_cache_bytes: 0,
            cache_layout_sharded: false,
            max_concurrent_reads,
            max_inflight_bytes,
            compressed_cache_bytes,
            metrics: ZarrScanMetrics::default(),
            remote_data_get_calls: Arc::new(AtomicU64::new(0)),
            remote_data_encoded_bytes: Arc::new(AtomicU64::new(0)),
            remote_shard_payload_get_calls: Arc::new(AtomicU64::new(0)),
            remote_shard_payload_encoded_bytes: Arc::new(AtomicU64::new(0)),
            flushed_encoded_bytes: 0,
            flushed_cells: 0,
            flushed_tuples: 0,
            chunk_bytes: Vec::new(),
            chunk_shape: Vec::new(),
            sub_lo: Vec::new(),
            sub_hi: Vec::new(),
            sub_idx: Vec::new(),
            capture_spatial_indices: false,
            last_emitted_indices: None,
            pending: false,
            aggregate_defs: Vec::new(),
            aggregate_quals: Vec::new(),
            aggregate_reducer: None,
            aggregate_emitted: false,
            time_spec: TimeSpec::default(),
            rows_out: 0,
        })
    }

    fn supported_aggregates(&self) -> Vec<AggregateKind> {
        vec![
            AggregateKind::Count,
            AggregateKind::CountColumn,
            AggregateKind::Sum,
            AggregateKind::Avg,
            AggregateKind::Min,
            AggregateKind::Max,
        ]
    }

    fn explain(&self) -> Vec<ExplainProperty> {
        let Some(meta) = self.axis_meta.as_ref() else {
            return Vec::new();
        };
        let chunk_shape = meta
            .chunks
            .iter()
            .map(|&extent| usize::try_from(extent).unwrap_or(usize::MAX))
            .collect::<Vec<_>>();
        let dtype = self
            .dtype
            .map(|dtype| format!("{dtype:?}"))
            .unwrap_or_else(|| meta.dtype.clone());
        let codec = self
            .codec
            .as_ref()
            .map(CodecPipeline::ordered_label)
            .unwrap_or_else(|| "unknown".to_string());
        let storage_layout = meta.storage_layout.ordered_label();
        let (shard_shape, index_location) = match &meta.storage_layout {
            StorageLayout::Direct => (None, None),
            StorageLayout::Sharded(config) => (
                Some(config.shard_shape.as_slice()),
                Some(config.index_location.label()),
            ),
        };
        let aggregate_mode = if self.aggregate_defs.is_empty() {
            "none".to_string()
        } else {
            self.aggregate_defs
                .iter()
                .map(|aggregate| aggregate.kind.sql_name())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut properties = self
            .metrics_snapshot()
            .explain_properties(ZarrExplainContext {
                array: &self.array_dir,
                dimensions: &self.axes,
                shape: &meta.shape,
                chunk_shape: &chunk_shape,
                dtype: &dtype,
                codec: &codec,
                storage_backend: self.store.backend_label(),
                storage_layout: &storage_layout,
                shard_shape,
                index_location,
                aggregate_mode: &aggregate_mode,
                max_concurrent_reads: self.max_concurrent_reads,
                max_inflight_bytes: self.max_inflight_bytes,
                compressed_cache_bytes: self.payload_cache_bytes,
                cache_entries: self.compressed_cache.len(),
                cache_resident_bytes: self.compressed_cache.resident_bytes(),
                shard_index_cache_bytes: self.shard_index_cache_bytes,
                shard_index_cache_entries: self.shard_index_cache.len(),
                shard_index_cache_resident_bytes: self.shard_index_cache.resident_bytes(),
            });
        if let Some(level) = &self.selected_ome_level {
            properties.push(ExplainProperty::text(
                "Zarr OME Group",
                if level.group_path.is_empty() {
                    "/"
                } else {
                    &level.group_path
                },
            ));
            properties.push(ExplainProperty::unsigned(
                "Zarr OME Multiscale Index",
                u64::try_from(level.multiscale_index).unwrap_or(u64::MAX),
            ));
            properties.push(ExplainProperty::unsigned(
                "Zarr OME Level Index",
                u64::try_from(level.level_index).unwrap_or(u64::MAX),
            ));
            properties.push(ExplainProperty::text(
                "Zarr OME Effective Scale",
                format!("{:?}", level.transform.scale),
            ));
            properties.push(ExplainProperty::text(
                "Zarr OME Effective Translation",
                format!("{:?}", level.transform.translation),
            ));
        }
        properties
    }

    #[allow(clippy::too_many_arguments)]
    fn can_pushdown_aggregate(
        &mut self,
        aggregates: &[Aggregate],
        group_by: &[Column],
        quals: &[Qual],
        base_columns: &[Column],
        all_base_quals_extracted: bool,
        _options: &HashMap<String, String>,
    ) -> ZarrFdwResult<bool> {
        if aggregates.is_empty() || !group_by.is_empty() || !all_base_quals_extracted {
            return Ok(false);
        }
        if aggregates.iter().any(|aggregate| {
            !aggregate_signature_supported(aggregate)
                || aggregate.column.as_ref().is_some_and(|column| {
                    !base_columns
                        .iter()
                        .any(|base| base.name == column.name && base.type_oid == column.type_oid)
                })
        }) {
            return Ok(false);
        }
        if quals.iter().any(|qual| {
            !qual_shape_supported(qual)
                || !base_columns.iter().any(|column| column.name == qual.field)
        }) {
            return Ok(false);
        }
        Ok(true)
    }

    fn get_aggregate_rel_size(
        &mut self,
        aggregates: &[Aggregate],
        group_by: &[Column],
        _quals: &[Qual],
        _options: &HashMap<String, String>,
    ) -> ZarrFdwResult<(i64, i32)> {
        debug_assert!(group_by.is_empty());
        let width = aggregates.iter().fold(0_i32, |sum, aggregate| {
            sum.saturating_add(estimated_pg_type_width(aggregate.type_oid))
        });
        Ok((1, width.max(DEFAULT_EMPTY_PROJECTION_WIDTH)))
    }

    fn get_rel_size(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> ZarrFdwResult<(i64, i32)> {
        // Do not fetch array metadata here: this callback also runs for plain
        // EXPLAIN, where remote latency/auth failures would be surprising.
        Ok(conservative_rel_size(columns))
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> ZarrFdwResult<()> {
        self.aggregate_defs.clear();
        self.aggregate_quals.clear();
        self.aggregate_reducer = None;
        self.aggregate_emitted = false;
        self.dimension_selectors = BoundDimensionSelectors::default();
        self.bound_call_dimension_selectors = BoundDimensionSelectors::default();
        self.tgt_cols = columns.to_vec();
        let dimension_selectors =
            DimensionSelectors::parse(options.get(OPT_DIMENSION_SELECTORS).map(String::as_str))?;
        let time_from_attrs = boolean_table_option(options, OPT_TIME_FROM_ATTRS)?;
        validate_time_from_attrs_options(
            time_from_attrs,
            options.contains_key(OPT_TIME_UNIT),
            options.contains_key(OPT_TIME_ORIGIN),
        )?;
        let decode_cf = boolean_table_option(options, OPT_DECODE_CF)?;

        let multiscale_selection = multiscale_selection_options(options)?;
        let (value_node, dataset, coordinate_nodes, coordinate_fill_values, selected_ome_level) =
            if let Some(selection) = multiscale_selection {
                let group_attributes =
                    read_ome_group_attributes(&self.store, &mut self.metrics, &selection.group)?;
                let level = resolve_ome_05_level(
                    &selection.group,
                    &group_attributes,
                    selection.index,
                    selection.level,
                )?;
                validate_ome_hierarchy_versions(&self.store, &mut self.metrics, &level.array_path)?;
                let value_node =
                    read_array_node(&self.store, &mut self.metrics, &level.array_path)?;
                validate_optional_ome_05_attributes(
                    &level.array_path,
                    Some(&value_node.attributes),
                )?;
                value_node.meta.validate()?;
                let dataset = ome_rank2_dataset(&level.array_path, &value_node, &level)?;
                let rank = dataset.dimensions().len();
                (
                    value_node,
                    dataset,
                    vec![None; rank],
                    vec![None; rank],
                    Some(level),
                )
            } else {
                // `array_group` scopes one ordinary Zarr array; the default is
                // the store root. Existing behavior is unchanged outside OME
                // selection mode.
                let array_dir = options
                    .get(OPT_ARRAY_GROUP)
                    .map(|path| path.trim_matches('/').to_string())
                    .unwrap_or_default();
                let value_node = read_array_node(&self.store, &mut self.metrics, &array_dir)?;
                value_node.meta.validate()?;
                let dimension_names = named_dimensions(&value_node, &array_dir)?;
                let coordinate_parent = array_parent_path(&array_dir);
                let mut coordinate_nodes = Vec::with_capacity(dimension_names.len());
                let mut coordinate_fill_values = Vec::with_capacity(dimension_names.len());
                let mut aligned_nodes = Vec::with_capacity(dimension_names.len());
                for (name, &length) in dimension_names.iter().zip(value_node.meta.shape.iter()) {
                    let (coordinate, fill_value) = read_coordinate_metadata(
                        &self.store,
                        &mut self.metrics,
                        coordinate_parent,
                        name,
                        length,
                    )?;
                    aligned_nodes.push(coordinate.clone());
                    coordinate_nodes.push(Some(coordinate));
                    coordinate_fill_values.push(fill_value);
                }
                let dataset =
                    named_array_dataset(&array_dir, &value_node, &dimension_names, &aligned_nodes)?;
                (
                    value_node,
                    dataset,
                    coordinate_nodes,
                    coordinate_fill_values,
                    None,
                )
            };
        self.selected_ome_level = selected_ome_level;
        let meta = &value_node.meta;
        let value_attributes = &value_node.attributes;
        let variable = dataset.variable();
        self.array_dir = variable.path().to_string();
        self.axes = dataset.axis_names();
        let bound_dimension_selectors = dimension_selectors.bind(&self.axes, &meta.shape)?;
        let bound_call_dimension_selectors = self
            .call_dimension_selectors
            .bind(&self.axes, &meta.shape)?;
        debug_assert_eq!(variable.dimensions(), self.axes.as_slice());
        self.axis_roles = dataset
            .dimensions()
            .iter()
            .map(|dimension| dimension.semantic_role())
            .collect();
        let rank = dataset.dimensions().len();
        self.rank = rank;
        let dtype = DType::parse(variable.dtype())?;
        let time_axis = dataset
            .dimensions()
            .iter()
            .position(|dimension| dimension.semantic_role() == DimensionRole::Time);
        let has_manual_time_options =
            options.contains_key(OPT_TIME_UNIT) || options.contains_key(OPT_TIME_ORIGIN);
        if (time_from_attrs || has_manual_time_options) && time_axis.is_none() {
            let option = if time_from_attrs {
                OPT_TIME_FROM_ATTRS
            } else if options.contains_key(OPT_TIME_UNIT) {
                OPT_TIME_UNIT
            } else {
                OPT_TIME_ORIGIN
            };
            return Err(ZarrFdwError::InvalidOptionValue {
                option: option.to_string(),
                message: "requires exactly one discovered Time dimension".to_string(),
            });
        }
        let time_spec = if time_from_attrs {
            let axis = time_axis.ok_or_else(|| ZarrFdwError::InvalidOptionValue {
                option: OPT_TIME_FROM_ATTRS.to_string(),
                message: "requires exactly one discovered Time dimension".to_string(),
            })?;
            let coordinate = coordinate_nodes[axis].as_ref().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "time_from_attrs requires a stored time coordinate array".to_string(),
                )
            })?;
            TimeSpec::from_cf_attributes(&coordinate.attributes)?
        } else {
            TimeSpec::from_legacy_options(
                options.get(OPT_TIME_UNIT).map(String::as_str),
                options.get(OPT_TIME_ORIGIN).map(String::as_str),
            )?
        };
        let scientific_decoder = if decode_cf {
            Some(ScientificValueDecoder::from_attributes(
                dtype,
                value_attributes,
            )?)
        } else {
            None
        };
        validate_column_types(columns, &dataset, dtype, decode_cf)?;

        // single-array MVP: at most one non-dimension (value) column allowed
        let value_cols = columns
            .iter()
            .filter(|column| !dataset.is_dimension(&column.name))
            .count();
        if value_cols > 1 {
            return Err(ZarrFdwError::InvalidOptionValue {
                option: OPT_BANDS.to_string(),
                message: format!(
                    "single-array execution supports at most one value column, got {value_cols}"
                ),
            });
        }
        self.axis_meta = Some(meta.clone());
        self.array_attributes = value_attributes.clone();
        self.dtype = Some(dtype);
        self.codec = Some(codec_pipeline_for_execution(meta)?);
        self.scientific_decoder = scientific_decoder;
        self.fill_bytes = fill_value_bytes(dtype, &meta.fill_value)?;
        self.time_spec = time_spec;
        if matches!(&meta.storage_layout, StorageLayout::Sharded(_))
            || coordinate_nodes.iter().flatten().any(|coordinate| {
                matches!(&coordinate.meta.storage_layout, StorageLayout::Sharded(_))
            })
        {
            self.configure_sharded_cache_budget();
        }

        // Coordinate metadata is required for every dimension, but coordinate
        // chunk values are needed only for projected or restricted dimensions.
        let required_coordinates = dataset
            .dimensions()
            .iter()
            .enumerate()
            .map(|(axis, dimension)| {
                columns.iter().any(|column| column.name == dimension.name())
                    || bound_dimension_selectors.requires_coordinate(axis)
                    || bound_call_dimension_selectors.requires_coordinate(axis)
            })
            .collect::<Vec<_>>();
        checked_total_coordinate_values(
            dataset
                .dimensions()
                .iter()
                .zip(required_coordinates.iter())
                .map(|(dimension, &required)| (dimension.name(), dimension.length(), required)),
            MAX_TOTAL_COORDINATE_VALUES,
        )?;
        let mut coords = Vec::with_capacity(rank);
        for (axis, dimension) in dataset.dimensions().iter().enumerate() {
            if !required_coordinates[axis] {
                coords.push(None);
                continue;
            }
            let values = match dimension.coordinate_source() {
                CoordinateSource::Stored(coordinate) => {
                    let node = coordinate_nodes[axis].as_ref().ok_or_else(|| {
                        ZarrFdwError::InvalidMetadata(format!(
                            "stored coordinate '{}' has no loaded metadata",
                            dimension.name()
                        ))
                    })?;
                    validate_coordinate_decoding_attributes(dimension.name(), &node.attributes)?;
                    read_coordinate_values(
                        self,
                        coordinate.parent(),
                        coordinate.name(),
                        node,
                        coordinate_fill_values[axis],
                    )?
                }
                CoordinateSource::Affine { scale, translation } => affine_coordinate_values(
                    dimension.name(),
                    dimension.length(),
                    *scale,
                    *translation,
                )?,
            };
            validate_coordinate_values(dimension.name(), &values)?;
            coords.push(Some(values));
        }
        self.coords = coords;

        // translate quals into per-axis value ranges and intersect them
        let mut ranges: Vec<CoordinateRange> = vec![(None, None); rank];
        for q in quals {
            let Some(axis) = self.axes.iter().position(|a| a == &q.field) else {
                continue;
            };
            let is_time = self.axis_roles[axis] == DimensionRole::Time;
            let Some((lo, hi)) = qual_to_range(q, is_time, self.time_spec)? else {
                continue;
            };
            let cur = &mut ranges[axis];
            if let Some(l) = lo {
                cur.0 = Some(match cur.0 {
                    Some(x) => x.max(l),
                    None => l,
                });
            }
            if let Some(h) = hi {
                cur.1 = Some(match cur.1 {
                    Some(x) => x.min(h),
                    None => h,
                });
            }
        }

        let planner = ScanPlanner::new(meta);
        let qual_selection =
            planner.selection_from_coordinate_ranges(&self.axes, &self.coords, &ranges)?;
        let selector_selection =
            bound_dimension_selectors.resolve(&meta.shape, &self.coords, || {
                process_postgres_interrupts();
                Ok(())
            })?;
        let call_selector_selection =
            bound_call_dimension_selectors.resolve(&meta.shape, &self.coords, || {
                process_postgres_interrupts();
                Ok(())
            })?;
        let plan = planner.plan(
            qual_selection
                .intersect(selector_selection)
                .intersect(call_selector_selection),
        )?;
        self.dimension_selectors = bound_dimension_selectors;
        self.bound_call_dimension_selectors = bound_call_dimension_selectors;
        self.apply_scan_plan(plan, false)
    }

    fn begin_aggregate_scan_with_base_columns(
        &mut self,
        aggregates: &[Aggregate],
        group_by: &[Column],
        quals: &[Qual],
        base_columns: &[Column],
        options: &HashMap<String, String>,
    ) -> ZarrFdwResult<()> {
        debug_assert!(!aggregates.is_empty());
        debug_assert!(group_by.is_empty());
        <Self as ForeignDataWrapper<ZarrFdwError>>::begin_scan(
            self,
            quals,
            base_columns,
            &[],
            &None,
            options,
        )?;
        self.aggregate_defs = aggregates.to_vec();
        self.aggregate_quals = quals.to_vec();
        self.aggregate_reducer = Some(AggregateReducer::new(aggregates)?);
        self.aggregate_emitted = false;
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> ZarrFdwResult<Option<()>> {
        let result = if !self.aggregate_defs.is_empty() {
            self.iter_aggregate_scan(row)
        } else {
            self.iter_scalar_scan(row)
        };
        if result.is_err() {
            // PostgreSQL may unwind immediately after receiving the error.
            // Drop all owned I/O futures before leaving the callback.
            self.prefetch.clear();
            self.deferred_prefetch = None;
        }
        result
    }

    fn re_scan(&mut self) -> ZarrFdwResult<()> {
        self.chunk_cursor.reset();
        self.current_chunk.clear();
        self.prefetch.clear();
        self.deferred_prefetch = None;
        self.metrics.record_rescan();
        self.chunk_bytes.clear();
        self.chunk_shape.clear();
        [self.sub_lo, self.sub_hi, self.sub_idx] = zeroed_scan_cursors(self.rank);
        self.last_emitted_indices = None;
        self.pending = false;
        self.rows_out = 0;
        if !self.aggregate_defs.is_empty() {
            self.aggregate_reducer = Some(AggregateReducer::new(&self.aggregate_defs)?);
            self.aggregate_emitted = false;
        }
        Ok(())
    }

    fn end_scan(&mut self) -> ZarrFdwResult<()> {
        self.prefetch.clear();
        self.deferred_prefetch = None;
        self.tgt_cols.clear();
        self.array_dir.clear();
        self.axes.clear();
        self.array_attributes.clear();
        self.selected_ome_level = None;
        self.chunk_bytes.clear();
        self.chunk_shape.clear();
        self.chunk_cursor = ChunkIndexCursor::default();
        self.current_chunk.clear();
        self.coords.clear();
        self.selection = Selection::default();
        self.dimension_selectors = BoundDimensionSelectors::default();
        self.call_dimension_selectors = DimensionSelectors::default();
        self.bound_call_dimension_selectors = BoundDimensionSelectors::default();
        self.axis_roles.clear();
        self.sub_lo.clear();
        self.sub_hi.clear();
        self.sub_idx.clear();
        self.capture_spatial_indices = false;
        self.last_emitted_indices = None;
        self.rank = 0;
        self.axis_meta = None;
        self.dtype = None;
        self.codec = None;
        self.scientific_decoder = None;
        self.fill_bytes = None;
        self.pending = false;
        self.aggregate_defs.clear();
        self.aggregate_quals.clear();
        self.aggregate_reducer = None;
        self.aggregate_emitted = false;
        self.time_spec = TimeSpec::default();
        self.rows_out = 0;
        Ok(())
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) -> ZarrFdwResult<()> {
        if let Some(oid) = catalog {
            match oid {
                FOREIGN_SERVER_RELATION_ID => {
                    check_options_contain(&options, "store_url")?;
                    let server_options = options
                        .iter()
                        .flatten()
                        .filter_map(|option| option.split_once('='))
                        .map(|(name, value)| (name.to_string(), value.to_string()))
                        .collect::<HashMap<_, _>>();
                    let backend = validate_store_options(&server_options)?;
                    validate_store_definition_privilege(backend)?;
                    scalable_execution_options(&server_options)?;
                }
                FOREIGN_TABLE_RELATION_ID => {
                    let table_options = options
                        .iter()
                        .flatten()
                        .filter_map(|option| option.split_once('='))
                        .map(|(name, value)| (name.to_string(), value.to_string()))
                        .collect::<HashMap<_, _>>();
                    multiscale_selection_options(&table_options)?;
                    if let Some(v) = option_value(&options, OPT_TIME_UNIT) {
                        TimeSpec::from_legacy_options(Some(&v), None)?;
                    }
                    if let Some(v) = option_value(&options, OPT_TIME_ORIGIN) {
                        TimeSpec::from_legacy_options(None, Some(&v))?;
                    }
                    if let Some(v) = option_value(&options, OPT_TIME_FROM_ATTRS) {
                        let time_from_attrs = parse_boolean_option(OPT_TIME_FROM_ATTRS, &v)?;
                        validate_time_from_attrs_options(
                            time_from_attrs,
                            option_value(&options, OPT_TIME_UNIT).is_some(),
                            option_value(&options, OPT_TIME_ORIGIN).is_some(),
                        )?;
                    }
                    if let Some(v) = option_value(&options, OPT_DECODE_CF) {
                        parse_boolean_option(OPT_DECODE_CF, &v)?;
                    }
                    DimensionSelectors::parse(
                        option_value(&options, OPT_DIMENSION_SELECTORS).as_deref(),
                    )?;
                    if let Some(v) = option_value(&options, OPT_ARRAY_GROUP)
                        && (v.trim_matches('/').is_empty() || v.contains(".."))
                    {
                        return Err(ZarrFdwError::InvalidOptionValue {
                            option: OPT_ARRAY_GROUP.to_string(),
                            message: "must be a non-empty array path inside the store".to_string(),
                        });
                    }
                    if let Some(v) = option_value(&options, OPT_BANDS)
                        && v.split(',').any(|b| b.trim().is_empty())
                    {
                        return Err(ZarrFdwError::InvalidOptionValue {
                            option: OPT_BANDS.to_string(),
                            message: "must be a comma-separated list of band column names"
                                .to_string(),
                        });
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod unit_tests {
    use super::super::meta::{ChunkKeyEncoding, ZarrFormat};
    use super::*;

    fn column(name: &str, type_oid: pg_sys::Oid) -> Column {
        Column {
            name: name.to_string(),
            num: 1,
            type_oid,
        }
    }

    fn array_meta(shape: Vec<u64>, chunks: Vec<u64>) -> ArrayMeta {
        ArrayMeta {
            zarr_format: 2,
            shape,
            chunks,
            dtype: "<f4".to_string(),
            fill_value: serde_json::Value::from(0),
            compressor: None,
            codec_pipeline: CodecPipeline::raw_v2(),
            storage_layout: StorageLayout::Direct,
            chunk_key_encoding: ChunkKeyEncoding::V2 { separator: '.' },
            order: 'C',
            filters: None,
        }
    }

    fn array_node(
        shape: Vec<u64>,
        chunks: Vec<u64>,
        attributes: Map<String, JsonValue>,
    ) -> ArrayNode {
        ArrayNode {
            format: ZarrFormat::V2,
            meta: array_meta(shape, chunks),
            attributes,
            dimension_names: None,
            native_dtype: "<f4".to_string(),
            native_codecs: serde_json::json!({
                "filters": null,
                "compressor": null,
            }),
        }
    }

    fn attributes(value: JsonValue) -> Map<String, JsonValue> {
        value.as_object().unwrap().clone()
    }

    fn named_dataset(
        names: &[&str],
        coordinate_attributes: Vec<Map<String, JsonValue>>,
    ) -> Dataset {
        let names = names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let value_node = array_node(
            vec![2; names.len()],
            vec![1; names.len()],
            attributes(serde_json::json!({ "_ARRAY_DIMENSIONS": names })),
        );
        let coordinate_nodes = coordinate_attributes
            .into_iter()
            .map(|attributes| array_node(vec![2], vec![1], attributes))
            .collect::<Vec<_>>();
        named_array_dataset("nested/value", &value_node, &names, &coordinate_nodes).unwrap()
    }

    #[test]
    fn array_metadata_document_requires_exactly_one_format() {
        assert!(matches!(
            select_array_metadata_document("value", None, Some(vec![2])),
            Ok(ArrayMetadataDocument::V2(bytes)) if bytes == vec![2]
        ));
        assert!(matches!(
            select_array_metadata_document("value", Some(vec![3]), None),
            Ok(ArrayMetadataDocument::V3(bytes)) if bytes == vec![3]
        ));
        assert!(matches!(
            select_array_metadata_document("value", Some(vec![3]), Some(vec![2])),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message.contains("both zarr.json and .zarray")
        ));
        assert!(matches!(
            select_array_metadata_document("value", None, None),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message.contains("neither zarr.json nor .zarray")
        ));
    }

    #[test]
    fn array_ancestor_paths_include_root_and_every_parent_group() {
        assert!(array_ancestor_paths("").is_empty());
        assert_eq!(array_ancestor_paths("value"), vec![""]);
        assert_eq!(
            array_ancestor_paths("outer/inner/value"),
            vec!["", "outer", "outer/inner"]
        );
    }

    #[test]
    fn scan_cursors_are_sized_to_the_array_rank() {
        let [lo, hi, idx] = zeroed_scan_cursors(3);
        assert_eq!(lo, vec![0; 3]);
        assert_eq!(hi, vec![0; 3]);
        assert_eq!(idx, vec![0; 3]);
    }

    #[test]
    fn spatial_time_layout_accepts_axis_orders_and_singleton_extras() {
        let time_y_x = discover_spatial_time_layout(
            3,
            &[
                DimensionRole::Time,
                DimensionRole::SpatialY,
                DimensionRole::SpatialX,
            ],
            &[2, 5, 6],
        )
        .unwrap();
        assert_eq!(time_y_x.time, 0);
        assert_eq!((time_y_x.horizontal.x, time_y_x.horizontal.y), (2, 1));

        let x_time_y = discover_spatial_time_layout(
            3,
            &[
                DimensionRole::Longitude,
                DimensionRole::Time,
                DimensionRole::Latitude,
            ],
            &[6, 2, 5],
        )
        .unwrap();
        assert_eq!(x_time_y.time, 1);
        assert_eq!((x_time_y.horizontal.x, x_time_y.horizontal.y), (0, 2));

        let with_singleton_band = discover_spatial_time_layout(
            4,
            &[
                DimensionRole::Band,
                DimensionRole::SpatialY,
                DimensionRole::SpatialX,
                DimensionRole::Time,
            ],
            &[1, 5, 6, 2],
        )
        .unwrap();
        assert_eq!(with_singleton_band.time, 3);
        assert_eq!(
            (
                with_singleton_band.horizontal.x,
                with_singleton_band.horizontal.y,
            ),
            (2, 1)
        );
    }

    #[test]
    fn spatial_time_layout_rejects_invalid_rank_roles_and_extras() {
        assert!(
            discover_spatial_time_layout(
                2,
                &[DimensionRole::SpatialY, DimensionRole::SpatialX],
                &[5, 6],
            )
            .is_err()
        );
        assert!(
            discover_spatial_time_layout(
                4,
                &[
                    DimensionRole::Band,
                    DimensionRole::SpatialY,
                    DimensionRole::SpatialX,
                    DimensionRole::Time,
                ],
                &[2, 5, 6, 2],
            )
            .is_err()
        );
        assert!(
            discover_spatial_time_layout(
                4,
                &[
                    DimensionRole::Time,
                    DimensionRole::SpatialY,
                    DimensionRole::SpatialX,
                    DimensionRole::Time,
                ],
                &[2, 5, 6, 1],
            )
            .is_err()
        );
    }

    #[test]
    fn spatial_time_range_is_exact_for_unordered_duplicate_coordinates() {
        let spec = TimeSpec::default();
        let start = spec.raw_to_pg_micros(1.0).unwrap();
        let end = spec.raw_to_pg_micros(3.0).unwrap();
        let values = [2.0, 0.0, 2.0, 1.0, 3.0];
        let selected = values
            .into_iter()
            .enumerate()
            .filter_map(|(index, raw)| {
                spatial_time_value_in_range(spec, raw, start, end)
                    .unwrap()
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(selected, vec![0, 2, 3]);
    }

    #[test]
    fn missing_chunk_repeats_typed_fill_or_rejects_null() {
        assert_eq!(
            filled_chunk_bytes(Some(&[0x00, 0x40]), 3, "nested/raw/0.0").unwrap(),
            vec![0x00, 0x40, 0x00, 0x40, 0x00, 0x40]
        );
        assert!(matches!(
            filled_chunk_bytes(None, 3, "nested/raw/0.0"),
            Err(ZarrFdwError::MissingChunkWithoutFillValue { key })
                if key == "nested/raw/0.0"
        ));
    }

    #[test]
    fn missing_coordinate_chunk_uses_fill_or_rejects_null() {
        assert_eq!(
            filled_coordinate_values(Some(4.5), 3, "nested/x/0", "x").unwrap(),
            vec![4.5; 3]
        );
        let err = filled_coordinate_values(None, 3, "nested/x/0", "x").unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed to read coordinate 'x': zarr chunk 'nested/x/0' is absent and fill_value is null, so its contents are undefined"
        );

        let non_finite = filled_coordinate_values(Some(f64::NAN), 2, "nested/x/0", "x").unwrap();
        assert!(validate_coordinate_values("x", &non_finite).is_err());
    }

    #[test]
    fn chunk_layout_uses_checked_bounded_arithmetic() {
        let normal = array_meta(vec![2, 3, 4], vec![2, 3, 4]);
        let (shape, cells, bytes) = checked_chunk_layout(&normal, 4).unwrap();
        assert_eq!(shape, vec![2, 3, 4]);
        assert_eq!(cells, 24);
        assert_eq!(bytes, 96);

        let too_large = array_meta(
            vec![(MAX_DECODED_CHUNK_BYTES / 8 + 1) as u64, 1],
            vec![(MAX_DECODED_CHUNK_BYTES / 8 + 1) as u64, 1],
        );
        assert!(matches!(
            checked_chunk_layout(&too_large, 8),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message.contains("exceeding the safety limit")
        ));
        assert!(filled_chunk_bytes(Some(&[0]), MAX_DECODED_CHUNK_BYTES + 1, "oversized").is_err());
        assert!(
            filled_coordinate_values(Some(0.0), MAX_COORDINATE_VALUES + 1, "x/0", "x").is_err()
        );
    }

    #[test]
    fn flat_chunk_offsets_and_byte_ranges_are_checked() {
        assert_eq!(checked_flat_offset(&[1, 2, 3], &[2, 3, 4]).unwrap(), 23);
        assert!(checked_flat_offset(&[2, 0, 0], &[2, 3, 4]).is_err());
        assert!(checked_flat_offset(&[0, 0], &[usize::MAX, 2]).is_err());
        assert_eq!(checked_chunk_byte_range(23, 4, 96).unwrap(), 92..96);
        assert!(checked_chunk_byte_range(24, 4, 96).is_err());
        assert!(checked_chunk_byte_range(usize::MAX, 8, usize::MAX).is_err());
    }

    #[test]
    fn decoded_chunks_must_have_exact_declared_length() {
        require_exact_decoded_len("0.0", 96, 96).unwrap();
        for actual in [95, 97] {
            assert!(matches!(
                require_exact_decoded_len("0.0", actual, 96),
                Err(ZarrFdwError::ReadError(_))
            ));
        }
    }

    #[test]
    fn value_dtypes_map_to_exact_postgres_types() {
        let cases = [
            (DType::F32, pg_sys::FLOAT4OID, "real"),
            (DType::F64, pg_sys::FLOAT8OID, "double precision"),
            (DType::I8, pg_sys::CHAROID, r#""char""#),
            (DType::I16, pg_sys::INT2OID, "smallint"),
            (DType::I32, pg_sys::INT4OID, "integer"),
            (DType::I64, pg_sys::INT8OID, "bigint"),
        ];
        for (dtype, oid, name) in cases {
            assert_eq!(expected_value_pg_type(dtype, false), (oid, name));
            assert_eq!(
                expected_value_pg_type(dtype, true),
                (pg_sys::FLOAT8OID, "double precision")
            );
        }
    }

    #[test]
    fn planner_estimate_is_positive_bounded_and_uses_projected_types() {
        let columns = vec![
            column("forecast_time", pg_sys::TIMESTAMPTZOID),
            column("easting", pg_sys::FLOAT8OID),
            column("value", pg_sys::FLOAT4OID),
        ];
        assert_eq!(conservative_rel_size(&columns), (1_000_000, 20));
        assert_eq!(conservative_rel_size(&[]), (1_000_000, 8));
        assert_eq!(
            conservative_rel_size(&[column("unknown", pg_sys::TEXTOID)]),
            (1_000_000, 32)
        );
    }

    #[test]
    fn accepts_exact_coordinate_and_value_column_types() {
        let dataset = named_dataset(
            &["forecast_time", "level", "band", "channel"],
            vec![
                attributes(serde_json::json!({"standard_name": "time"})),
                attributes(serde_json::json!({"axis": "Z"})),
                Map::new(),
                Map::new(),
            ],
        );
        let columns = vec![
            column("forecast_time", pg_sys::TIMESTAMPTZOID),
            column("level", pg_sys::FLOAT8OID),
            column("band", pg_sys::FLOAT8OID),
            column("channel", pg_sys::FLOAT8OID),
            column("value", pg_sys::FLOAT4OID),
        ];
        validate_column_types(&columns, &dataset, DType::F32, false).unwrap();
    }

    #[test]
    fn coordinates_do_not_need_to_be_projected() {
        let dataset = named_dataset(&["latitude", "longitude"], vec![Map::new(), Map::new()]);
        validate_column_types(
            &[column("value", pg_sys::FLOAT4OID)],
            &dataset,
            DType::F32,
            false,
        )
        .unwrap();
        validate_column_types(
            &[
                column("longitude", pg_sys::FLOAT8OID),
                column("value", pg_sys::FLOAT4OID),
            ],
            &dataset,
            DType::F32,
            false,
        )
        .unwrap();
    }

    #[test]
    fn rejects_incompatible_discovered_coordinate_type() {
        let dataset = named_dataset(&["level"], vec![Map::new()]);
        assert!(matches!(
            validate_column_types(
                &[column("level", pg_sys::INT4OID)],
                &dataset,
                DType::F32,
                false
            ),
            Err(ZarrFdwError::ColumnTypeMismatch { column, .. }) if column == "level"
        ));
    }

    #[test]
    fn rejects_incompatible_discovered_time_type() {
        let dataset = named_dataset(
            &["forecast_time"],
            vec![attributes(serde_json::json!({"axis": "T"}))],
        );
        assert!(matches!(
            validate_column_types(
                &[column("forecast_time", pg_sys::TIMESTAMPOID)],
                &dataset,
                DType::F32,
                false
            ),
            Err(ZarrFdwError::ColumnTypeMismatch { column, .. }) if column == "forecast_time"
        ));
    }

    #[test]
    fn arbitrary_non_dimension_name_is_a_value_column() {
        let dataset = named_dataset(&["latitude", "longitude"], vec![Map::new(), Map::new()]);
        validate_column_types(
            &[column("time", pg_sys::FLOAT4OID)],
            &dataset,
            DType::F32,
            false,
        )
        .unwrap();
    }

    #[test]
    fn cf_decoding_requires_double_precision_value_columns() {
        let dataset = named_dataset(&["latitude", "longitude"], vec![Map::new(), Map::new()]);
        validate_column_types(
            &[column("value", pg_sys::FLOAT8OID)],
            &dataset,
            DType::F32,
            true,
        )
        .unwrap();
        assert!(matches!(
            validate_column_types(
                &[column("value", pg_sys::FLOAT4OID)],
                &dataset,
                DType::F32,
                true,
            ),
            Err(ZarrFdwError::ColumnTypeMismatch { column, .. }) if column == "value"
        ));
    }

    #[test]
    fn coordinate_values_must_be_finite_but_may_be_unordered() {
        for finite in [
            vec![0.0, 1.0, 2.0],
            vec![2.0, 1.0, 0.0],
            vec![1.0, 1.0, 2.0],
            vec![1.0, 1.0, 1.0],
            vec![0.0, 2.0, 1.0],
        ] {
            validate_coordinate_values("x", &finite).unwrap();
        }

        for invalid in [
            vec![0.0, f64::NAN],
            vec![0.0, f64::INFINITY],
            vec![0.0, f64::NEG_INFINITY],
        ] {
            assert!(validate_coordinate_values("x", &invalid).is_err());
        }
    }

    #[test]
    fn coordinate_nan_qualifiers_never_narrow_chunk_ranges() {
        for operator in ["=", "<", "<=", ">", ">="] {
            let qual = Qual {
                field: "x".to_string(),
                operator: operator.to_string(),
                value: Value::Cell(Cell::F64(f64::NAN)),
                use_or: false,
                param: None,
            };
            assert_eq!(
                qual_to_range(&qual, false, TimeSpec::default()).unwrap(),
                None
            );
        }

        let finite_and_nan = Qual {
            field: "x".to_string(),
            operator: "=".to_string(),
            value: Value::Array(vec![Cell::F64(f64::NAN), Cell::F64(5.0)]),
            use_or: true,
            param: None,
        };
        assert_eq!(
            qual_to_range(&finite_and_nan, false, TimeSpec::default()).unwrap(),
            Some((Some(5.0), Some(5.0)))
        );
    }

    #[test]
    fn required_coordinates_have_a_cumulative_value_budget() {
        assert_eq!(
            checked_total_coordinate_values([("a", 3, true), ("b", 2, false), ("c", 4, true)], 7)
                .unwrap(),
            7
        );
        assert!(checked_total_coordinate_values([("a", 4, true), ("b", 4, true)], 7).is_err());
    }

    #[test]
    fn required_coordinates_reject_unsupported_scientific_decoding() {
        for attribute in UNSUPPORTED_COORDINATE_DECODING_ATTRIBUTES {
            let mut attributes = Map::new();
            attributes.insert(attribute.to_string(), serde_json::json!(1));
            assert!(validate_coordinate_decoding_attributes("level", &attributes).is_err());
        }
        validate_coordinate_decoding_attributes("level", &Map::new()).unwrap();
    }

    #[test]
    fn scalable_execution_options_are_bounded_and_cache_can_be_disabled() {
        assert_eq!(
            scalable_execution_options(&HashMap::new()).unwrap(),
            (
                DEFAULT_MAX_CONCURRENT_READS,
                DEFAULT_MAX_INFLIGHT_BYTES,
                DEFAULT_COMPRESSED_CACHE_BYTES,
            )
        );
        let valid = HashMap::from([
            (OPT_MAX_CONCURRENT_READS.to_string(), "32".to_string()),
            (
                OPT_MAX_INFLIGHT_BYTES.to_string(),
                MIN_MAX_INFLIGHT_BYTES.to_string(),
            ),
            (OPT_COMPRESSED_CACHE_BYTES.to_string(), "0".to_string()),
        ]);
        assert_eq!(
            scalable_execution_options(&valid).unwrap(),
            (32, MIN_MAX_INFLIGHT_BYTES, 0)
        );

        for invalid in [
            HashMap::from([(OPT_MAX_CONCURRENT_READS.to_string(), "0".to_string())]),
            HashMap::from([(
                OPT_MAX_INFLIGHT_BYTES.to_string(),
                (MIN_MAX_INFLIGHT_BYTES - 1).to_string(),
            )]),
            HashMap::from([(
                OPT_COMPRESSED_CACHE_BYTES.to_string(),
                (MAX_COMPRESSED_CACHE_BYTES + 1).to_string(),
            )]),
        ] {
            assert!(scalable_execution_options(&invalid).is_err());
        }
    }

    #[test]
    fn remote_io_metrics_count_polled_and_completed_work_only() {
        let calls = Arc::new(AtomicU64::new(0));
        let bytes = Arc::new(AtomicU64::new(0));
        let unpolled = observe_data_fetch(
            async { Ok::<_, ZarrFdwError>(Some(vec![1, 2, 3])) },
            Arc::clone(&calls),
            Arc::clone(&bytes),
        );
        drop(unpolled);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(bytes.load(Ordering::Relaxed), 0);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let object = runtime
            .block_on(observe_data_fetch(
                async { Ok::<_, ZarrFdwError>(Some(vec![1, 2, 3, 4])) },
                Arc::clone(&calls),
                Arc::clone(&bytes),
            ))
            .unwrap();
        assert_eq!(object, Some(vec![1, 2, 3, 4]));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(bytes.load(Ordering::Relaxed), 4);
    }
}
