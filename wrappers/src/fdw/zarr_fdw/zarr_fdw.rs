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
//! before any chunk is fetched. A time qual is interpreted via the `time_unit`
//! and `time_origin` table options, which describe how the raw `time`
//! coordinate values map to instants.
//!
//! Spatial PostGIS predicates (`ST_Intersects`, `geom && box`) do *not* reach
//! this code as `Qual`s — the framework only extracts simple Var-op-Const
//! expressions — so strict geometry pushdown is deferred to v1 (chunk-extent
//! catalog table); the MVP prunes on the `x`/`y`/`time` columns directly.

use crate::stats;
use pgrx::datum::TimestampWithTimeZone;
use pgrx::pg_sys;
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::chunk::{
    IndexBounds, axis_chunk_ranges, chunk_key, enumerate_chunks, index_bounds_from_value_range,
};
use super::decode::{Codec, DType, coord_bytes_to_f64};
use super::meta::ArrayMeta;
use super::store::{ZarrStore, join_key};
use super::{ZarrFdwError, ZarrFdwResult};

const FDW_NAME: &str = "ZarrFdw";

// PG epoch (2000-01-01 00:00:00 UTC) in microseconds since 1970-01-01.
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
const PG_EPOCH_SECONDS: i64 = 946_684_800;

// Table option names.
const OPT_ARRAY_GROUP: &str = "array_group";
const OPT_BANDS: &str = "bands";
const OPT_TIME_UNIT: &str = "time_unit";
const OPT_TIME_ORIGIN: &str = "time_origin";

// Coordinate column/axis names.
const AXIS_TIME: &str = "time";
const AXIS_Y: &str = "y";
const AXIS_X: &str = "x";

/// Unit of the raw `time` coordinate values, used to convert them into
/// timestamps and back. Mirrors the CF `units: "X since ..."` time encoding
/// for the small set of units we support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeUnit {
    Seconds,
    Milliseconds,
    Nanoseconds,
    Hours,
    Days,
}

impl TimeUnit {
    fn parse(s: &str) -> ZarrFdwResult<Self> {
        match s {
            "seconds" => Ok(Self::Seconds),
            "milliseconds" => Ok(Self::Milliseconds),
            "nanoseconds" => Ok(Self::Nanoseconds),
            "hours" => Ok(Self::Hours),
            "days" => Ok(Self::Days),
            _ => Err(ZarrFdwError::InvalidOptionValue {
                option: s.to_string(),
                message: "must be one of: seconds, milliseconds, nanoseconds, hours, days"
                    .to_string(),
            }),
        }
    }

    /// Factor converting a raw coordinate value into seconds.
    fn to_seconds_factor(self) -> f64 {
        match self {
            Self::Seconds => 1.0,
            Self::Milliseconds => 1e-3,
            Self::Nanoseconds => 1e-9,
            Self::Hours => 3600.0,
            Self::Days => 86400.0,
        }
    }
}

/// Describes how raw `time` coordinate values map to instants.
#[derive(Debug, Clone, Copy)]
struct TimeSpec {
    unit: TimeUnit,
    /// Seconds after 1970-01-01 corresponding to a raw coordinate value of 0.
    origin_epoch_seconds: f64,
}

impl TimeSpec {
    fn default() -> Self {
        Self {
            unit: TimeUnit::Seconds,
            origin_epoch_seconds: 0.0,
        }
    }

    fn from_options(options: &HashMap<String, String>) -> ZarrFdwResult<Self> {
        let unit = match options.get(OPT_TIME_UNIT) {
            Some(u) => TimeUnit::parse(u)?,
            None => TimeUnit::Seconds,
        };
        let origin_epoch_seconds = match options.get(OPT_TIME_ORIGIN).map(String::as_str) {
            Some("unix") => 0.0,
            Some("postgres") => PG_EPOCH_SECONDS as f64,
            Some(other) => {
                return Err(ZarrFdwError::InvalidOptionValue {
                    option: other.to_string(),
                    message: "must be 'unix' or 'postgres'".to_string(),
                });
            }
            None => 0.0,
        };
        Ok(Self {
            unit,
            origin_epoch_seconds,
        })
    }

    /// Convert a raw coordinate value into PG-epoch microseconds.
    fn raw_to_pg_micros(&self, raw: f64) -> i64 {
        ((raw * self.unit.to_seconds_factor() + self.origin_epoch_seconds) * 1.0e6
            - PG_EPOCH_MICROS as f64)
            .round() as i64
    }

    /// Convert PG-epoch microseconds into a raw coordinate value.
    fn pg_micros_to_raw(&self, pg_micros: i64) -> f64 {
        ((pg_micros as f64 + PG_EPOCH_MICROS as f64) / 1.0e6 - self.origin_epoch_seconds)
            / self.unit.to_seconds_factor()
    }
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
    // dimension names in array order, e.g. ["time", "y", "x"] or ["y", "x"]
    axes: Vec<String>,
    rank: usize,
    axis_meta: Option<ArrayMeta>,
    dtype: Option<DType>,
    codec: Option<Codec>,
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

        // fetch + decompress
        let key = chunk_key(&meta.dimension_separator, ci);
        let raw = self
            .store
            .get_object_sync(&join_key(&self.array_dir, &key))?;
        let decoded = self.store.rt.block_on(codec.decompress(&raw))?;
        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, decoded.len() as i64);

        // effective (edge) chunk shape
        let mut eff = Vec::with_capacity(self.rank);
        for ((&dim, &chunk_len), &ci_d) in meta.shape.iter().zip(meta.chunks.iter()).zip(ci.iter())
        {
            let chunk_len = chunk_len as usize;
            let start = ci_d as usize * chunk_len;
            eff.push((dim as usize - start).min(chunk_len));
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
        let storage_shape = meta.chunks.iter().map(|&n| n as usize).collect::<Vec<_>>();
        let expected = storage_shape.iter().product::<usize>() * dt.itemsize();
        if decoded.len() < expected {
            return Err(ZarrFdwError::ReadError(std::io::Error::other(format!(
                "chunk '{key}' decoded to {} bytes, expected at least {expected}",
                decoded.len()
            ))));
        }
        self.chunk_bytes = decoded[..expected].to_vec();
        self.chunk_shape = storage_shape;

        // within-chunk index window for this chunk
        for d in 0..self.rank {
            let chunk_len = meta.chunks[d] as usize;
            let base = ci[d] as usize * chunk_len;
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
        let mut off = 0usize;
        let mut stride = 1usize;
        for d in (0..self.rank).rev() {
            off += self.sub_idx[d] * stride;
            stride *= self.chunk_shape[d];
        }

        let item = dt.itemsize();
        let start = off * item;
        let value_cell = Self::value_cell(dt, &self.chunk_bytes[start..start + item])?;

        for col in &self.tgt_cols {
            match self.axes.iter().position(|a| a == &col.name) {
                Some(d) => {
                    let global = ci[d] as usize * meta.chunks[d] as usize + self.sub_idx[d];
                    let coord = self.coords[d][global];
                    if self.axes[d] == AXIS_TIME {
                        let micros = self.time_spec.raw_to_pg_micros(coord);
                        let ts = TimestampWithTimeZone::try_from(micros)
                            .map_err(|_| ZarrFdwError::TimeOutOfRange(coord))?;
                        row.push(col.name.as_str(), Some(Cell::Timestamptz(ts)));
                    } else {
                        row.push(col.name.as_str(), Some(Cell::F64(coord)));
                    }
                }
                None => {
                    row.push(col.name.as_str(), Some(value_cell.clone()));
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

fn cell_to_f64(cell: &Cell, is_time: bool, spec: TimeSpec) -> Option<f64> {
    if is_time {
        match cell {
            Cell::Timestamptz(v) => Some(spec.pg_micros_to_raw((*v).into_inner())),
            Cell::Timestamp(v) => Some(spec.pg_micros_to_raw((*v).into_inner())),
            _ => None,
        }
    } else {
        match cell {
            Cell::F64(v) => Some(*v),
            Cell::F32(v) => Some(*v as f64),
            Cell::I64(v) => Some(*v as f64),
            Cell::I32(v) => Some(*v as f64),
            Cell::I16(v) => Some(*v as f64),
            Cell::I8(v) => Some(*v as f64),
            _ => None,
        }
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
            if let Some(v) = cell_to_f64(c, is_time, spec) {
                lo = lo.min(v);
                hi = hi.max(v);
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
    let Some(v) = cell_to_f64(cell, is_time, spec) else {
        return Ok(None);
    };
    let r = match q.operator.as_str() {
        "=" => (Some(v), Some(v)),
        ">" => (Some(v), None),
        ">=" => (Some(v), None),
        "<" => (None, Some(v)),
        "<=" => (None, Some(v)),
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
    let meta_bytes = store.get_object_sync(&format!("{dir}/.zarray"))?;
    let meta = ArrayMeta::parse(&meta_bytes).map_err(|e| ZarrFdwError::CoordinateReadError {
        axis: name.to_string(),
        error: format!("coordinate array metadata: {e}"),
    })?;
    meta.validate_coordinate()
        .map_err(|e| ZarrFdwError::CoordinateReadError {
            axis: name.to_string(),
            error: e.to_string(),
        })?;

    let codec = Codec::from_compressor_json(&meta.compressor);
    let per_axis = meta.chunks_per_axis();
    let mut ranges = Vec::with_capacity(per_axis.len());
    for n in &per_axis {
        ranges.push((0usize, n.saturating_sub(1) as usize));
    }

    let mut values = Vec::new();
    for ci in enumerate_chunks(&ranges) {
        let chunk = chunk_key(&meta.dimension_separator, &ci);
        let raw = store.get_object_sync(&join_key(&dir, &chunk))?;
        let decoded = store.rt.block_on(codec.decompress(&raw))?;
        let decoded_values = coord_bytes_to_f64(&meta.dtype, &decoded)?;
        let chunk_len = meta.chunks[0] as usize;
        let start = ci[0] as usize * chunk_len;
        let effective_len = (meta.shape[0] as usize - start).min(chunk_len);
        if decoded_values.len() < effective_len {
            return Err(ZarrFdwError::CoordinateReadError {
                axis: name.to_string(),
                error: format!(
                    "chunk '{chunk}' decoded to {} values, expected at least {effective_len}",
                    decoded_values.len()
                ),
            });
        }
        values.extend_from_slice(&decoded_values[..effective_len]);
    }
    Ok(values)
}

fn option_value(options: &[Option<String>], name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    options
        .iter()
        .flatten()
        .find_map(|kv| kv.strip_prefix(&prefix).map(|v| v.to_string()))
}

/// True when a foreign table column name is one of the coordinate columns.
fn is_coordinate_col(name: &str) -> bool {
    matches!(name, AXIS_X | AXIS_Y | AXIS_TIME)
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
            rank: 0,
            axis_meta: None,
            dtype: None,
            codec: None,
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

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> ZarrFdwResult<()> {
        self.tgt_cols = columns.to_vec();
        self.time_spec = TimeSpec::from_options(options)?;

        // the array_group option scopes which Zarr array in the store this
        // foreign table reads; the default is the store root itself
        let array_group = options
            .get(OPT_ARRAY_GROUP)
            .map(|s| s.trim_matches('/').to_string())
            .unwrap_or_default();
        self.array_dir = array_group;

        // single-band MVP: at most one non-coordinate (value) column allowed
        let value_cols = columns
            .iter()
            .filter(|c| !is_coordinate_col(&c.name))
            .count();
        if value_cols > 1 {
            return Err(ZarrFdwError::InvalidOptionValue {
                option: OPT_BANDS.to_string(),
                message: format!(
                    "single-array MVP supports exactly one value column, got {value_cols}"
                ),
            });
        }

        // rows are keyed by their (x, y) coordinates, so both must be projected
        let has_x = columns.iter().any(|c| c.name == AXIS_X);
        let has_y = columns.iter().any(|c| c.name == AXIS_Y);
        if !has_x || !has_y {
            return Err(ZarrFdwError::MissingCoordinateColumn);
        }

        // load array metadata
        let meta_bytes = self
            .store
            .get_object_sync(&join_key(&self.array_dir, ".zarray"))?;
        let meta = ArrayMeta::from_bytes(&meta_bytes)?;
        let rank = meta.shape.len();
        self.axes = if rank == 3 {
            vec![
                AXIS_TIME.to_string(),
                AXIS_Y.to_string(),
                AXIS_X.to_string(),
            ]
        } else {
            vec![AXIS_Y.to_string(), AXIS_X.to_string()]
        };
        self.rank = rank;
        self.axis_meta = Some(meta.clone());
        self.dtype = Some(DType::parse(&meta.dtype)?);
        self.codec = Some(Codec::from_compressor_json(&meta.compressor));

        // load coordinate arrays
        let mut coords = Vec::with_capacity(rank);
        for axis in &self.axes {
            coords.push(read_coordinate_array(&self.store, "", axis)?);
        }
        for (d, axis) in self.axes.iter().enumerate() {
            if coords[d].len() as u64 != meta.shape[d] {
                return Err(ZarrFdwError::CoordinateReadError {
                    axis: axis.clone(),
                    error: format!(
                        "coordinate array has {} values but the axis has shape {}",
                        coords[d].len(),
                        meta.shape[d]
                    ),
                });
            }
        }
        self.coords = coords;

        // translate quals into per-axis value ranges and intersect them
        let mut ranges: Vec<(Option<f64>, Option<f64>)> = vec![(None, None); rank];
        for q in quals {
            let Some(axis) = self.axes.iter().position(|a| a == &q.field) else {
                continue;
            };
            let is_time = self.axes[axis] == AXIS_TIME;
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
            let chunk_ranges = axis_chunk_ranges(&meta, &self.bounds);
            enumerate_chunks(&chunk_ranges)
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
        self.rows_out = 0;
        Ok(())
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) -> ZarrFdwResult<()> {
        if let Some(oid) = catalog {
            match oid {
                FOREIGN_SERVER_RELATION_ID => {
                    check_options_contain(&options, "store_url")?;
                    if let Some(v) = option_value(&options, "anonymous") {
                        if v != "true" && v != "false" {
                            return Err(ZarrFdwError::InvalidOptionValue {
                                option: "anonymous".to_string(),
                                message: "must be 'true' or 'false'".to_string(),
                            });
                        }
                    }
                }
                FOREIGN_TABLE_RELATION_ID => {
                    if let Some(v) = option_value(&options, OPT_TIME_UNIT) {
                        TimeUnit::parse(&v)?;
                    }
                    if let Some(v) = option_value(&options, OPT_TIME_ORIGIN) {
                        if v != "unix" && v != "postgres" {
                            return Err(ZarrFdwError::InvalidOptionValue {
                                option: OPT_TIME_ORIGIN.to_string(),
                                message: "must be 'unix' or 'postgres'".to_string(),
                            });
                        }
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
    use super::zeroed_scan_cursors;

    #[test]
    fn scan_cursors_are_sized_to_the_array_rank() {
        let [lo, hi, idx] = zeroed_scan_cursors(3);
        assert_eq!(lo, vec![0; 3]);
        assert_eq!(hi, vec![0; 3]);
        assert_eq!(idx, vec![0; 3]);
    }
}
