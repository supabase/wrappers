//! Main `zarr_fdw` implementation.
//!
//! Given a query plan's pushed-down quals (WHERE) and target columns, this FDW
//! translates them into a *chunk fetch list* against S3, decompresses the
//! chunks and streams flat rows back to Postgres. Data model (MVP):
//!
//! - a single Zarr v2 array, rank 2 `[y, x]` or rank 3 `[time, y, x]` (C order),
//! - 1D coordinate arrays `x`, `y` (and `time` when rank 3) stored as siblings
//!   of the array in the store,
//! - flat row output: `x, y, time, <value>` where every non-coordinate target
//!   column receives the array's scalar value.
//!
//! Pushdown: any `[x|y|time] <op> <const>` qual is converted into an index
//! range over the dimension's coordinate vector, which prunes the chunk list
//! before any chunk is fetched. A time qual is interpreted via either the
//! `time_unit`/`time_origin` table options or, when `time_from_attrs` is true,
//! the sibling time coordinate's CF `units`/`calendar` attributes.
//!
//! Spatial PostGIS predicates (`ST_Intersects`, `geom && box`) do *not* reach
//! this code as `Qual`s — the framework only extracts simple Var-op-Const
//! expressions — so strict geometry pushdown is deferred to v1 (chunk-extent
//! catalog table); the MVP prunes on the `x`/`y`/`time` columns directly.

use crate::stats;
use pgrx::datum::TimestampWithTimeZone;
use pgrx::pg_sys;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::chunk::{
    IndexBounds, axis_chunk_ranges, chunk_key, enumerate_chunks, index_bounds_from_value_range,
};
use super::dataset::{Dataset, DimensionRole, legacy_array_dataset};
use super::decode::{
    Codec, DType, coord_bytes_to_f64, coord_fill_value_to_f64, coordinate_itemsize,
    fill_value_bytes,
};
use super::meta::ArrayMeta;
use super::scientific::{ScientificValueDecoder, time::TimeSpec};
use super::store::{MAX_METADATA_OBJECT_BYTES, ZarrStore, join_key, validate_auth_options};
use super::{ZarrFdwError, ZarrFdwResult};

const FDW_NAME: &str = "ZarrFdw";

// Table option names.
const OPT_ARRAY_GROUP: &str = "array_group";
const OPT_BANDS: &str = "bands";
const OPT_TIME_UNIT: &str = "time_unit";
const OPT_TIME_ORIGIN: &str = "time_origin";
const OPT_TIME_FROM_ATTRS: &str = "time_from_attrs";
const OPT_DECODE_CF: &str = "decode_cf";

// Coordinate column/axis names.
const AXIS_TIME: &str = "time";
const AXIS_Y: &str = "y";
const AXIS_X: &str = "x";

// Planning must stay deterministic and network-free. Until metadata-backed or
// configured estimates are available, use a deliberately non-zero cardinality
// for remote arrays so PostgreSQL does not price every scan at startup cost.
const DEFAULT_PLANNER_ROWS: i64 = 1_000_000;
const DEFAULT_EMPTY_PROJECTION_WIDTH: i32 = 8;
const DEFAULT_UNKNOWN_TYPE_WIDTH: i32 = 32;

// The MVP eagerly decodes one data chunk, all coordinate vectors, and the
// selected chunk-coordinate list in a PostgreSQL backend. Keep those remote-
// metadata-driven allocations bounded until the executor becomes streaming.
const MAX_DECODED_CHUNK_BYTES: usize = 256 * 1024 * 1024;
const MAX_COORDINATE_VALUES: usize = 16 * 1024 * 1024;

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
    // dimension names in array order, e.g. ["time", "y", "x"] or ["y", "x"]
    axes: Vec<String>,
    // scientific meaning assigned by the metadata adapter, in array order
    axis_roles: Vec<DimensionRole>,
    rank: usize,
    axis_meta: Option<ArrayMeta>,
    dtype: Option<DType>,
    codec: Option<Codec>,
    scientific_decoder: Option<ScientificValueDecoder>,
    // one decoded scalar, repeated when a data chunk is absent
    fill_bytes: Option<Vec<u8>>,
    // coordinate values per axis (raw, as given by the store)
    coords: Vec<Vec<f64>>,
    // per-axis global index bounds from pushed-down quals
    bounds: Vec<Option<IndexBounds>>,
    // chunk index vectors to read, in row-major order
    chunks: Vec<Vec<u64>>,

    // --- per-chunk iteration state ---------------------------------------
    chunk_pos: usize,
    chunk_bytes: Vec<u8>,
    chunk_shape: Vec<usize>,
    sub_lo: Vec<usize>,
    sub_hi: Vec<usize>,
    sub_idx: Vec<usize>,
    pending: bool,

    time_spec: TimeSpec,
    rows_out: i64,
}

fn zeroed_scan_cursors(rank: usize) -> [Vec<usize>; 3] {
    std::array::from_fn(|_| vec![0; rank])
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

fn validate_fixed_coordinate_column_types(columns: &[Column]) -> ZarrFdwResult<()> {
    for column in columns {
        match column.name.as_str() {
            AXIS_X | AXIS_Y => {
                require_column_type(column, pg_sys::FLOAT8OID, "double precision")?;
            }
            AXIS_TIME => {
                require_column_type(column, pg_sys::TIMESTAMPTZOID, "timestamp with time zone")?;
            }
            _ => {}
        }
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
            None if column.name == AXIS_TIME => {
                return Err(ZarrFdwError::InvalidCoordinateColumn {
                    column: column.name.clone(),
                    rank: dataset.dimensions().len(),
                });
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

    let nondecreasing = values.windows(2).all(|pair| pair[0] <= pair[1]);
    let nonincreasing = values.windows(2).all(|pair| pair[0] >= pair[1]);
    if !nondecreasing && !nonincreasing {
        return Err(ZarrFdwError::CoordinateReadError {
            axis: axis.to_string(),
            error: "coordinate values must be monotonic".to_string(),
        });
    }
    Ok(())
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

    /// Fetch and decode the chunk at `self.chunk_pos`, priming the within-chunk
    /// index window from `self.bounds`.
    fn load_chunk(&mut self) -> ZarrFdwResult<()> {
        let meta = self
            .axis_meta
            .as_ref()
            .expect("begin_scan must be called before iter_scan");
        let dt = self.dtype.expect("dtype set in begin_scan");
        let codec = self.codec.as_ref().expect("codec set in begin_scan");
        let ci = &self.chunks[self.chunk_pos];
        debug_assert_eq!(self.sub_lo.len(), self.rank);
        debug_assert_eq!(self.sub_hi.len(), self.rank);
        debug_assert_eq!(self.sub_idx.len(), self.rank);

        // Effective (edge) chunk shape. Zarr v2 still stores the full declared
        // chunk shape; `eff` only controls which logical cells are emitted.
        let key = chunk_key(&meta.dimension_separator, ci);
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

        // Zarr v2 edge chunks retain the declared chunk shape. Use that full
        // shape for byte validation and C-order strides; `eff` ignores the
        // out-of-array region when deciding which cells to emit.
        let object_key = join_key(&self.array_dir, &key);
        let encoded_limit = codec.encoded_read_limit(expected)?;
        let decoded = match self
            .store
            .get_object_optional_sync(&object_key, encoded_limit)?
        {
            Some(raw) => {
                let decoded = self.store.rt.block_on(codec.decompress(raw, expected))?;
                let bytes_in = i64::try_from(decoded.len()).map_err(|_| {
                    ZarrFdwError::InvalidMetadata(
                        "decoded chunk length exceeds statistics capacity".to_string(),
                    )
                })?;
                stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, bytes_in);
                decoded
            }
            None => filled_chunk_bytes(self.fill_bytes.as_deref(), storage_cells, &object_key)?,
        };
        require_exact_decoded_len(&key, decoded.len(), expected)?;
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
            match &self.bounds[d] {
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
        let meta = self
            .axis_meta
            .as_ref()
            .expect("begin_scan must be called before iter_scan");
        let dt = self.dtype.expect("dtype set in begin_scan");
        let ci = &self.chunks[self.chunk_pos];

        // flat offset within the chunk (C order)
        let off = checked_flat_offset(&self.sub_idx, &self.chunk_shape)?;
        let item = dt.itemsize();
        let byte_range = checked_chunk_byte_range(off, item, self.chunk_bytes.len())?;
        let raw_value = &self.chunk_bytes[byte_range];
        let value_cell = match &self.scientific_decoder {
            Some(decoder) => decoder.decode(raw_value)?.map(Cell::F64),
            None => Some(Self::value_cell(dt, raw_value)?),
        };

        for col in &self.tgt_cols {
            match self.axes.iter().position(|a| a == &col.name) {
                Some(d) => {
                    let chunk_index = usize::try_from(ci[d]).map_err(|_| {
                        ZarrFdwError::InvalidMetadata(format!(
                            "chunk index for axis {d} exceeds this platform's index capacity"
                        ))
                    })?;
                    let chunk_len = meta.chunk_extent(d)?;
                    let global = chunk_index
                        .checked_mul(chunk_len)
                        .and_then(|base| base.checked_add(self.sub_idx[d]))
                        .ok_or_else(|| {
                            ZarrFdwError::InvalidMetadata(format!(
                                "coordinate index overflow on axis {d}"
                            ))
                        })?;
                    let coord = *self.coords[d].get(global).ok_or_else(|| {
                        ZarrFdwError::InvalidMetadata(format!(
                            "coordinate index {global} is outside axis {d} length {}",
                            self.coords[d].len()
                        ))
                    })?;
                    if self.axis_roles[d] == DimensionRole::Time {
                        let micros = self.time_spec.raw_to_pg_micros(coord)?;
                        let ts = TimestampWithTimeZone::try_from(micros)
                            .map_err(|_| ZarrFdwError::TimeOutOfRange(coord))?;
                        row.push(col.name.as_str(), Some(Cell::Timestamptz(ts)));
                    } else {
                        row.push(col.name.as_str(), Some(Cell::F64(coord)));
                    }
                }
                None => {
                    row.push(col.name.as_str(), value_cell.clone());
                }
            }
        }

        // advance in C order (last axis varies fastest)
        for d in (0..self.rank).rev() {
            if self.sub_idx[d] < self.sub_hi[d] {
                self.sub_idx[d] += 1;
                return Ok(());
            }
            self.sub_idx[d] = self.sub_lo[d];
        }
        // chunk exhausted
        self.chunk_pos += 1;
        self.pending = false;
        Ok(())
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
    if q.use_or {
        // `IN (...)` -> bounding box over the values (over-approximated)
        let Value::Array(cells) = &q.value else {
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

    let Value::Cell(cell) = &q.value else {
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

/// Read a 1D coordinate array (sibling of the cube array in the store) and
/// return its values as `f64`.
fn read_coordinate_array(store: &ZarrStore, prefix: &str, name: &str) -> ZarrFdwResult<Vec<f64>> {
    let dir = if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    };
    let meta_bytes = store.get_object_sync(&format!("{dir}/.zarray"), MAX_METADATA_OBJECT_BYTES)?;
    let meta = ArrayMeta::parse(&meta_bytes).map_err(|e| ZarrFdwError::CoordinateReadError {
        axis: name.to_string(),
        error: format!("coordinate array metadata: {e}"),
    })?;
    meta.validate_coordinate()
        .map_err(|e| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: e.to_string(),
        })?;

    let fill_value = coord_fill_value_to_f64(&meta.dtype, &meta.fill_value).map_err(|e| {
        ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: e.to_string(),
        }
    })?;

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
        checked_chunk_layout(&meta, itemsize).map_err(|e| ZarrFdwError::CoordinateReadError {
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

    let codec = Codec::from_compressor_json(&meta.compressor)?;
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
            error: "coordinate chunk index exceeds the Zarr v2 u64 index capacity".to_string(),
        })?;
        let chunk = chunk_key(&meta.dimension_separator, &[ci]);
        let object_key = join_key(&dir, &chunk);
        let encoded_limit = codec.encoded_read_limit(expected_bytes)?;
        let decoded_values = match store.get_object_optional_sync(&object_key, encoded_limit)? {
            Some(raw) => {
                let decoded = store
                    .rt
                    .block_on(codec.decompress(raw, expected_bytes))
                    .map_err(|e| ZarrFdwError::CoordinateReadError {
                        axis: name.to_string(),
                        error: e.to_string(),
                    })?;
                coord_bytes_to_f64(&meta.dtype, &decoded[..expected_bytes])?
            }
            None => filled_coordinate_values(fill_value, chunk_len, &object_key, name)?,
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
                        "coordinate chunk '{chunk}' starts beyond the declared array length"
                    ),
                })?;
        let effective_len = remaining.min(chunk_len);
        values.extend_from_slice(&decoded_values[..effective_len]);
    }
    Ok(values)
}

fn read_array_attributes_optional(
    store: &ZarrStore,
    array_dir: &str,
) -> ZarrFdwResult<Option<Map<String, JsonValue>>> {
    let key = join_key(array_dir, ".zattrs");
    let Some(bytes) = store.get_object_optional_sync(&key, MAX_METADATA_OBJECT_BYTES)? else {
        return Ok(None);
    };
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
        let store = ZarrStore::new(&server)?;
        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);
        Ok(Self {
            store,
            tgt_cols: Vec::new(),
            array_dir: String::new(),
            axes: Vec::new(),
            axis_roles: Vec::new(),
            rank: 0,
            axis_meta: None,
            dtype: None,
            codec: None,
            scientific_decoder: None,
            fill_bytes: None,
            coords: Vec::new(),
            bounds: Vec::new(),
            chunks: Vec::new(),
            chunk_pos: 0,
            chunk_bytes: Vec::new(),
            chunk_shape: Vec::new(),
            sub_lo: Vec::new(),
            sub_hi: Vec::new(),
            sub_idx: Vec::new(),
            pending: false,
            time_spec: TimeSpec::default(),
            rows_out: 0,
        })
    }

    fn get_rel_size(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> ZarrFdwResult<(i64, i32)> {
        // Do not fetch `.zarray` here: this callback also runs for plain
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
        self.tgt_cols = columns.to_vec();
        let time_from_attrs = boolean_table_option(options, OPT_TIME_FROM_ATTRS)?;
        validate_time_from_attrs_options(
            time_from_attrs,
            options.contains_key(OPT_TIME_UNIT),
            options.contains_key(OPT_TIME_ORIGIN),
        )?;
        let decode_cf = boolean_table_option(options, OPT_DECODE_CF)?;
        // These fixed output Cells are known before fetching remote metadata.
        // Reject unsafe mappings before any network request.
        validate_fixed_coordinate_column_types(columns)?;

        // the array_group option scopes which Zarr array in the store this
        // foreign table reads; the default is the store root itself
        let array_group = options
            .get(OPT_ARRAY_GROUP)
            .map(|s| s.trim_matches('/').to_string())
            .unwrap_or_default();
        self.array_dir = array_group;

        // load array metadata
        let meta_bytes = self.store.get_object_sync(
            &join_key(&self.array_dir, ".zarray"),
            MAX_METADATA_OBJECT_BYTES,
        )?;
        let meta = ArrayMeta::from_bytes(&meta_bytes)?;
        let dataset = legacy_array_dataset(&self.array_dir, &meta)?;
        let variable = dataset.variable();
        self.array_dir = variable.path().to_string();
        self.axes = dataset.axis_names();
        debug_assert_eq!(variable.dimensions(), self.axes.as_slice());
        self.axis_roles = dataset
            .dimensions()
            .iter()
            .map(|dimension| dimension.semantic_role())
            .collect();
        let rank = dataset.dimensions().len();
        self.rank = rank;
        let dtype = DType::parse(variable.dtype())?;
        let time_spec = if time_from_attrs {
            let time_dimension = dataset
                .dimensions()
                .iter()
                .find(|dimension| dimension.semantic_role() == DimensionRole::Time)
                .ok_or_else(|| ZarrFdwError::InvalidOptionValue {
                    option: OPT_TIME_FROM_ATTRS.to_string(),
                    message: "requires a rank-3 Zarr array with a time dimension".to_string(),
                })?;
            let coordinate = time_dimension.coordinate();
            let time_path = join_key(coordinate.parent(), coordinate.name());
            let attributes =
                read_array_attributes_optional(&self.store, &time_path)?.ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(format!(
                        "required time coordinate attributes '{time_path}/.zattrs' do not exist"
                    ))
                })?;
            TimeSpec::from_cf_attributes(&attributes)?
        } else {
            TimeSpec::from_legacy_options(
                options.get(OPT_TIME_UNIT).map(String::as_str),
                options.get(OPT_TIME_ORIGIN).map(String::as_str),
            )?
        };
        let scientific_decoder = if decode_cf {
            let attributes = read_array_attributes_optional(&self.store, variable.path())?
                .unwrap_or_else(Map::new);
            Some(ScientificValueDecoder::from_attributes(dtype, &attributes)?)
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
                    "single-array MVP supports exactly one value column, got {value_cols}"
                ),
            });
        }
        self.axis_meta = Some(meta.clone());
        self.dtype = Some(dtype);
        self.codec = Some(Codec::from_compressor_json(&meta.compressor)?);
        self.scientific_decoder = scientific_decoder;
        self.fill_bytes = fill_value_bytes(dtype, &meta.fill_value)?;
        self.time_spec = time_spec;

        // load coordinate arrays
        let mut coords = Vec::with_capacity(rank);
        for dimension in dataset.dimensions() {
            let coordinate = dimension.coordinate();
            coords.push(read_coordinate_array(
                &self.store,
                coordinate.parent(),
                coordinate.name(),
            )?);
        }
        for (d, dimension) in dataset.dimensions().iter().enumerate() {
            if coords[d].len() as u64 != dimension.length() {
                return Err(ZarrFdwError::CoordinateReadError {
                    axis: dimension.name().to_string(),
                    error: format!(
                        "coordinate array has {} values but the axis has shape {}",
                        coords[d].len(),
                        dimension.length()
                    ),
                });
            }
            validate_coordinate_values(dimension.name(), &coords[d])?;
        }
        self.coords = coords;

        // translate quals into per-axis value ranges and intersect them
        let mut ranges: Vec<(Option<f64>, Option<f64>)> = vec![(None, None); rank];
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

        // value ranges -> per-axis index bounds -> chunk list
        let mut bounds: Vec<Option<IndexBounds>> = Vec::with_capacity(rank);
        let mut no_rows = false;
        for (axis, &(lo, hi)) in ranges.iter().enumerate() {
            let b = index_bounds_from_value_range(&self.coords[axis], lo, hi);
            if b.is_none() {
                no_rows = true;
            }
            bounds.push(b);
        }
        self.bounds = bounds;

        self.chunks = if no_rows {
            Vec::new()
        } else {
            let chunk_ranges = axis_chunk_ranges(&meta, &self.bounds)?;
            enumerate_chunks(&chunk_ranges)?
        };
        self.chunk_pos = 0;
        self.chunk_bytes.clear();
        self.chunk_shape.clear();
        [self.sub_lo, self.sub_hi, self.sub_idx] = zeroed_scan_cursors(rank);
        self.pending = false;
        self.rows_out = 0;
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> ZarrFdwResult<Option<()>> {
        loop {
            if self.pending {
                row.clear();
                self.emit_and_advance(row)?;
                self.rows_out += 1;
                return Ok(Some(()));
            }
            if self.chunk_pos >= self.chunks.len() {
                if self.rows_out > 0 {
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, self.rows_out);
                    self.rows_out = 0;
                }
                return Ok(None);
            }
            self.load_chunk()?;
            let empty_window = (0..self.rank).any(|d| self.sub_lo[d] > self.sub_hi[d]);
            if empty_window {
                self.chunk_pos += 1;
                continue;
            }
            self.pending = true;
        }
    }

    fn re_scan(&mut self) -> ZarrFdwResult<()> {
        self.chunk_pos = 0;
        self.chunk_bytes.clear();
        self.pending = false;
        Ok(())
    }

    fn end_scan(&mut self) -> ZarrFdwResult<()> {
        self.chunk_bytes.clear();
        self.chunks.clear();
        self.coords.clear();
        self.bounds.clear();
        self.axis_roles.clear();
        self.scientific_decoder = None;
        self.fill_bytes = None;
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
                    validate_auth_options(&server_options)?;
                }
                FOREIGN_TABLE_RELATION_ID => {
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
                    if let Some(v) = option_value(&options, OPT_ARRAY_GROUP) {
                        if v.trim_matches('/').is_empty() || v.contains("..") {
                            return Err(ZarrFdwError::InvalidOptionValue {
                                option: OPT_ARRAY_GROUP.to_string(),
                                message: "must be a non-empty array path inside the store"
                                    .to_string(),
                            });
                        }
                    }
                    if let Some(v) = option_value(&options, OPT_BANDS) {
                        if v.split(',').any(|b| b.trim().is_empty()) {
                            return Err(ZarrFdwError::InvalidOptionValue {
                                option: OPT_BANDS.to_string(),
                                message: "must be a comma-separated list of band column names"
                                    .to_string(),
                            });
                        }
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
            dimension_separator: ".".to_string(),
            order: 'C',
            filters: None,
        }
    }

    #[test]
    fn scan_cursors_are_sized_to_the_array_rank() {
        let [lo, hi, idx] = zeroed_scan_cursors(3);
        assert_eq!(lo, vec![0; 3]);
        assert_eq!(hi, vec![0; 3]);
        assert_eq!(idx, vec![0; 3]);
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
            filled_coordinate_values(Some(4.5), 3, "nested/x/0", AXIS_X).unwrap(),
            vec![4.5; 3]
        );
        let err = filled_coordinate_values(None, 3, "nested/x/0", AXIS_X).unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed to read coordinate 'x': zarr chunk 'nested/x/0' is absent and fill_value is null, so its contents are undefined"
        );

        let non_finite = filled_coordinate_values(Some(f64::NAN), 2, "nested/x/0", AXIS_X).unwrap();
        assert!(validate_coordinate_values(AXIS_X, &non_finite).is_err());
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
            filled_coordinate_values(Some(0.0), MAX_COORDINATE_VALUES + 1, "x/0", AXIS_X).is_err()
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
            column(AXIS_TIME, pg_sys::TIMESTAMPTZOID),
            column(AXIS_X, pg_sys::FLOAT8OID),
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
        let dataset =
            legacy_array_dataset("value", &array_meta(vec![2, 5, 6], vec![1, 5, 3])).unwrap();
        let columns = vec![
            column(AXIS_TIME, pg_sys::TIMESTAMPTZOID),
            column(AXIS_Y, pg_sys::FLOAT8OID),
            column(AXIS_X, pg_sys::FLOAT8OID),
            column("value", pg_sys::FLOAT4OID),
        ];
        validate_column_types(&columns, &dataset, DType::F32, false).unwrap();
    }

    #[test]
    fn coordinates_do_not_need_to_be_projected() {
        let dataset = legacy_array_dataset("value", &array_meta(vec![5, 6], vec![5, 3])).unwrap();
        validate_column_types(
            &[column("value", pg_sys::FLOAT4OID)],
            &dataset,
            DType::F32,
            false,
        )
        .unwrap();
        validate_column_types(
            &[
                column(AXIS_X, pg_sys::FLOAT8OID),
                column("value", pg_sys::FLOAT4OID),
            ],
            &dataset,
            DType::F32,
            false,
        )
        .unwrap();
    }

    #[test]
    fn rejects_incompatible_column_type() {
        let columns = vec![column(AXIS_X, pg_sys::INT4OID)];
        assert!(matches!(
            validate_fixed_coordinate_column_types(&columns),
            Err(ZarrFdwError::ColumnTypeMismatch { column, .. }) if column == AXIS_X
        ));
    }

    #[test]
    fn rejects_incompatible_time_type_before_metadata() {
        let columns = vec![column(AXIS_TIME, pg_sys::TIMESTAMPOID)];
        assert!(matches!(
            validate_fixed_coordinate_column_types(&columns),
            Err(ZarrFdwError::ColumnTypeMismatch { column, .. }) if column == AXIS_TIME
        ));
    }

    #[test]
    fn rejects_time_column_for_rank_two_array() {
        let dataset = legacy_array_dataset("value", &array_meta(vec![5, 6], vec![5, 3])).unwrap();
        let columns = vec![column(AXIS_TIME, pg_sys::TIMESTAMPTZOID)];
        assert!(matches!(
            validate_column_types(&columns, &dataset, DType::F64, false),
            Err(ZarrFdwError::InvalidCoordinateColumn { column, rank: 2 })
                if column == AXIS_TIME
        ));
    }

    #[test]
    fn cf_decoding_requires_double_precision_value_columns() {
        let dataset = legacy_array_dataset("value", &array_meta(vec![5, 6], vec![5, 3])).unwrap();
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
    fn coordinate_values_must_be_finite_and_monotonic() {
        for valid in [
            vec![0.0, 1.0, 2.0],
            vec![2.0, 1.0, 0.0],
            vec![1.0, 1.0, 2.0],
            vec![1.0, 1.0, 1.0],
        ] {
            validate_coordinate_values("x", &valid).unwrap();
        }

        for invalid in [
            vec![0.0, f64::NAN],
            vec![0.0, f64::INFINITY],
            vec![0.0, f64::NEG_INFINITY],
            vec![0.0, 2.0, 1.0],
        ] {
            assert!(validate_coordinate_values("x", &invalid).is_err());
        }
    }
}
