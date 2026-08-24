//! Polygon-masked cell extraction and zonal statistics for rank-2 grids.

use std::cmp::Ordering;

use pgrx::pg_sys::panic::{ErrorReport, ErrorReportable};
use pgrx::prelude::*;
use supabase_wrappers::prelude::{ForeignDataWrapper, Row};

use super::super::aggregate::{checked_float_add_f64, compare_f64};
use super::super::zarr_fdw::ZarrFdw;
use super::super::{ZarrFdwError, ZarrFdwResult};
use super::catalog::load_zarr_foreign_table;
use super::grid::{CoordinateEnvelope, GridCell};
use super::point::numeric_cell_to_f64;
use super::postgis::{MAX_COVERAGE_CANDIDATES, PostgisCatalog};

const MAX_SPATIAL_CANDIDATE_CELLS: usize = 10_000_000;
const MAX_SPATIAL_OUTPUT_CELLS: usize = 1_000_000;
const SPATIAL_INTERRUPT_POLL_CELLS: usize = 1_024;

#[derive(Debug, Clone, PartialEq)]
struct SpatialCellRow {
    x: f64,
    y: f64,
    value: Option<f64>,
    x_index: i64,
    y_index: i64,
    srid: i32,
}

impl SpatialCellRow {
    fn sql_row(self) -> (f64, f64, Option<f64>, i64, i64, i32) {
        (
            self.x,
            self.y,
            self.value,
            self.x_index,
            self.y_index,
            self.srid,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ZonalStatsRow {
    count: i64,
    valid_count: i64,
    min: Option<f64>,
    max: Option<f64>,
    sum: Option<f64>,
    avg: Option<f64>,
    srid: i32,
}

impl ZonalStatsRow {
    #[allow(clippy::type_complexity)]
    fn sql_row(
        self,
    ) -> (
        i64,
        i64,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        i32,
    ) {
        (
            self.count,
            self.valid_count,
            self.min,
            self.max,
            self.sum,
            self.avg,
            self.srid,
        )
    }
}

#[allow(clippy::type_complexity)]
#[pg_extern(create_or_replace, volatile, parallel_unsafe)]
fn zarr_cells(
    foreign_table: &str,
    region_ewkb: &[u8],
) -> TableIterator<
    'static,
    (
        name!(x, f64),
        name!(y, f64),
        name!(value, Option<f64>),
        name!(x_index, i64),
        name!(y_index, i64),
        name!(srid, i32),
    ),
> {
    let mut cells = Vec::new();
    visit_foreign_table_cells(foreign_table, region_ewkb, |cell| {
        if cells.len() >= MAX_SPATIAL_OUTPUT_CELLS {
            return Err(ZarrFdwError::InvalidGeometry(format!(
                "polygon selects more than the {MAX_SPATIAL_OUTPUT_CELLS}-cell output limit"
            )));
        }
        cells.push(cell);
        Ok(())
    })
    .map_err(ErrorReport::from)
    .unwrap_or_report();
    TableIterator::new(cells.into_iter().map(SpatialCellRow::sql_row))
}

#[allow(clippy::type_complexity)]
#[pg_extern(create_or_replace, volatile, parallel_unsafe)]
fn zarr_zonal_stats(
    foreign_table: &str,
    region_ewkb: &[u8],
) -> TableIterator<
    'static,
    (
        name!(count, i64),
        name!(valid_count, i64),
        name!(min, Option<f64>),
        name!(max, Option<f64>),
        name!(sum, Option<f64>),
        name!(avg, Option<f64>),
        name!(srid, i32),
    ),
> {
    let mut accumulator = ZonalAccumulator::default();
    let srid = visit_foreign_table_cells(foreign_table, region_ewkb, |cell| {
        accumulator.observe(cell.value)
    })
    .map_err(ErrorReport::from)
    .unwrap_or_report();
    let row = accumulator
        .finish(srid)
        .map_err(ErrorReport::from)
        .unwrap_or_report();
    TableIterator::once(row.sql_row())
}

fn visit_foreign_table_cells(
    foreign_table: &str,
    region_ewkb: &[u8],
    mut visitor: impl FnMut(SpatialCellRow) -> ZarrFdwResult<()>,
) -> ZarrFdwResult<i32> {
    // Fail before any remote metadata request when the optional spatial
    // dependency is absent or incomplete.
    let postgis = PostgisCatalog::require()?;
    let table = load_zarr_foreign_table(foreign_table)?;
    let mut fdw = ZarrFdw::new(table.server)?;
    <ZarrFdw as ForeignDataWrapper<ZarrFdwError>>::begin_scan(
        &mut fdw,
        &[],
        &table.columns,
        &[],
        &None,
        &table.options,
    )?;

    let result = visit_prepared_scan(&mut fdw, &postgis, region_ewkb, &mut visitor);
    let cleanup = <ZarrFdw as ForeignDataWrapper<ZarrFdwError>>::end_scan(&mut fdw);
    match (result, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn visit_prepared_scan(
    fdw: &mut ZarrFdw,
    postgis: &PostgisCatalog,
    region_ewkb: &[u8],
    visitor: &mut impl FnMut(SpatialCellRow) -> ZarrFdwResult<()>,
) -> ZarrFdwResult<i32> {
    // Validate the table contract before a non-overlapping envelope can
    // return an otherwise-successful empty result.
    let value_column = fdw.spatial_value_column()?.to_string();
    let crs = fdw.resolved_spatial_crs()?;
    let envelope = postgis.transform_ewkb_geometry_envelope(
        region_ewkb,
        fdw.spatial_array_path(),
        crs.epsg,
    )?;
    let (axes, bounds, candidate_count) = {
        let grid = fdw.rectilinear_grid()?;
        let envelope =
            CoordinateEnvelope::new(envelope.xmin, envelope.ymin, envelope.xmax, envelope.ymax)?;
        let plan = grid.window_plan(envelope)?;
        let bounds = plan.as_ref().map(|plan| plan.bounds());
        let candidate_count = plan.as_ref().map_or(0, |plan| plan.total_cells());
        (grid.axes(), bounds, candidate_count)
    };
    if candidate_count > MAX_SPATIAL_CANDIDATE_CELLS {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "polygon envelope selects {candidate_count} candidate cells, exceeding the {MAX_SPATIAL_CANDIDATE_CELLS}-cell safety limit"
        )));
    }
    let Some(bounds) = bounds else {
        return Ok(crs.epsg);
    };
    fdw.restrict_to_spatial_bounds(bounds)?;

    let mut candidates = Vec::with_capacity(MAX_COVERAGE_CANDIDATES);
    let mut row = Row::new();
    let mut visited = 0usize;
    while <ZarrFdw as ForeignDataWrapper<ZarrFdwError>>::iter_scan(fdw, &mut row)?.is_some() {
        if visited.is_multiple_of(SPATIAL_INTERRUPT_POLL_CELLS) {
            fdw.spatial_check_for_interrupt()?;
        }
        visited = visited.saturating_add(1);
        let cell = fdw.spatial_last_emitted_cell(axes)?;
        let value = row
            .iter()
            .find(|(name, _)| name.as_str() == value_column)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "spatial result did not contain value column '{value_column}'"
                ))
            })?
            .1
            .as_ref()
            .map(numeric_cell_to_f64)
            .transpose()?;
        candidates.push((cell, value));
        if candidates.len() == MAX_COVERAGE_CANDIDATES {
            visit_covered_batch(
                postgis,
                region_ewkb,
                fdw,
                crs.epsg,
                &mut candidates,
                visitor,
            )?;
        }
    }
    if !candidates.is_empty() {
        visit_covered_batch(
            postgis,
            region_ewkb,
            fdw,
            crs.epsg,
            &mut candidates,
            visitor,
        )?;
    }
    Ok(crs.epsg)
}

fn visit_covered_batch(
    postgis: &PostgisCatalog,
    region_ewkb: &[u8],
    fdw: &mut ZarrFdw,
    srid: i32,
    candidates: &mut Vec<(GridCell, Option<f64>)>,
    visitor: &mut impl FnMut(SpatialCellRow) -> ZarrFdwResult<()>,
) -> ZarrFdwResult<()> {
    fdw.spatial_check_for_interrupt()?;
    let centers = candidates
        .iter()
        .map(|(cell, _)| (cell.x, cell.y))
        .collect::<Vec<_>>();
    let covered = postgis.covers_ewkb_geometry_points(
        region_ewkb,
        fdw.spatial_array_path(),
        srid,
        &centers,
    )?;
    if covered.len() != candidates.len() {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "PostGIS returned {} mask results for {} candidate cells",
            covered.len(),
            candidates.len()
        )));
    }
    for ((cell, value), covered) in candidates.drain(..).zip(covered) {
        if covered {
            visitor(spatial_cell_row(cell, value, srid)?)?;
        }
    }
    Ok(())
}

fn spatial_cell_row(
    cell: GridCell,
    value: Option<f64>,
    srid: i32,
) -> ZarrFdwResult<SpatialCellRow> {
    Ok(SpatialCellRow {
        x: cell.x,
        y: cell.y,
        value,
        x_index: i64::try_from(cell.x_index).map_err(|_| {
            ZarrFdwError::InvalidMetadata(
                "spatial x index exceeds PostgreSQL bigint range".to_string(),
            )
        })?,
        y_index: i64::try_from(cell.y_index).map_err(|_| {
            ZarrFdwError::InvalidMetadata(
                "spatial y index exceeds PostgreSQL bigint range".to_string(),
            )
        })?,
        srid,
    })
}

#[derive(Debug, Default)]
struct ZonalAccumulator {
    count: i64,
    valid_count: i64,
    min: Option<f64>,
    max: Option<f64>,
    sum: Option<f64>,
}

impl ZonalAccumulator {
    fn observe(&mut self, value: Option<f64>) -> ZarrFdwResult<()> {
        self.count = self.count.checked_add(1).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("zonal COUNT overflowed bigint".to_string())
        })?;
        let Some(value) = value else { return Ok(()) };
        self.valid_count = self.valid_count.checked_add(1).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("zonal valid COUNT overflowed bigint".to_string())
        })?;
        if self
            .min
            .is_none_or(|current| compare_f64(value, current) != Ordering::Greater)
        {
            self.min = Some(value);
        }
        if self
            .max
            .is_none_or(|current| compare_f64(value, current) != Ordering::Less)
        {
            self.max = Some(value);
        }
        self.sum = Some(match self.sum {
            Some(current) => checked_float_add_f64(current, value)?,
            None => value,
        });
        Ok(())
    }

    fn finish(self, srid: i32) -> ZarrFdwResult<ZonalStatsRow> {
        let avg = match (self.sum, self.valid_count) {
            (Some(sum), count) if count > 0 => Some(sum / count as f64),
            _ => None,
        };
        Ok(ZonalStatsRow {
            count: self.count,
            valid_count: self.valid_count,
            min: self.min,
            max: self.max,
            sum: self.sum,
            avg,
            srid,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zonal_accumulator_preserves_null_and_float_semantics() {
        let mut accumulator = ZonalAccumulator::default();
        for value in [Some(2.0), None, Some(-1.0), Some(3.0)] {
            accumulator.observe(value).unwrap();
        }
        assert_eq!(
            accumulator.finish(3857).unwrap(),
            ZonalStatsRow {
                count: 4,
                valid_count: 3,
                min: Some(-1.0),
                max: Some(3.0),
                sum: Some(4.0),
                avg: Some(4.0 / 3.0),
                srid: 3857,
            }
        );
    }

    #[test]
    fn empty_zonal_accumulator_returns_sql_null_statistics() {
        assert_eq!(
            ZonalAccumulator::default().finish(4326).unwrap(),
            ZonalStatsRow {
                count: 0,
                valid_count: 0,
                min: None,
                max: None,
                sum: None,
                avg: None,
                srid: 4326,
            }
        );
    }
}
