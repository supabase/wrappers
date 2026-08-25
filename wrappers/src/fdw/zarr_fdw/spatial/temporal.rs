//! Time-aware polygon cell extraction and zonal statistics for rank-3+ grids.

use std::collections::BTreeMap;

use pgrx::datum::TimestampWithTimeZone;
use pgrx::pg_sys::panic::{ErrorReport, ErrorReportable};
use pgrx::prelude::*;
use supabase_wrappers::prelude::{ForeignDataWrapper, Row};

use super::super::selectors::{DimensionSelectors, OPT_DIMENSION_SELECTORS};
use super::super::zarr_fdw::ZarrFdw;
use super::super::{ZarrFdwError, ZarrFdwResult};
use super::catalog::{load_zarr_foreign_table, load_zarr_foreign_table_with_selectors};
use super::point::numeric_cell_to_f64;
use super::postgis::{MAX_COVERAGE_CANDIDATES, PostgisCatalog};
use super::zonal::{
    MAX_SPATIAL_CANDIDATE_CELLS, MAX_SPATIAL_OUTPUT_CELLS, SPATIAL_INTERRUPT_POLL_CELLS,
    ZonalAccumulator,
};

#[derive(Debug)]
struct TemporalCellRow {
    time: TimestampWithTimeZone,
    x: f64,
    y: f64,
    value: Option<f64>,
    time_index: i64,
    x_index: i64,
    y_index: i64,
    srid: i32,
}

impl TemporalCellRow {
    #[allow(clippy::type_complexity)]
    fn sql_row(
        self,
    ) -> (
        TimestampWithTimeZone,
        f64,
        f64,
        Option<f64>,
        i64,
        i64,
        i64,
        i32,
    ) {
        (
            self.time,
            self.x,
            self.y,
            self.value,
            self.time_index,
            self.x_index,
            self.y_index,
            self.srid,
        )
    }
}

#[derive(Debug)]
struct TemporalZonalStatsRow {
    time: TimestampWithTimeZone,
    time_index: i64,
    count: i64,
    valid_count: i64,
    min: Option<f64>,
    max: Option<f64>,
    sum: Option<f64>,
    avg: Option<f64>,
    srid: i32,
}

impl TemporalZonalStatsRow {
    #[allow(clippy::type_complexity)]
    fn sql_row(
        self,
    ) -> (
        TimestampWithTimeZone,
        i64,
        i64,
        i64,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        i32,
    ) {
        (
            self.time,
            self.time_index,
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

#[derive(Debug)]
struct TemporalVisitSummary {
    times: Vec<(TimestampWithTimeZone, i64)>,
    srid: i32,
}

#[allow(clippy::type_complexity)]
#[pg_extern(create_or_replace, volatile, parallel_unsafe)]
fn zarr_cells_by_time(
    foreign_table: &str,
    region_ewkb: &[u8],
    start_time: TimestampWithTimeZone,
    end_time: TimestampWithTimeZone,
) -> TableIterator<
    'static,
    (
        name!(time, TimestampWithTimeZone),
        name!(x, f64),
        name!(y, f64),
        name!(value, Option<f64>),
        name!(time_index, i64),
        name!(x_index, i64),
        name!(y_index, i64),
        name!(srid, i32),
    ),
> {
    let rows = cells_foreign_table(foreign_table, region_ewkb, start_time, end_time)
        .map_err(ErrorReport::from)
        .unwrap_or_report();
    TableIterator::new(rows.into_iter().map(TemporalCellRow::sql_row))
}

#[allow(clippy::type_complexity)]
#[pg_extern(create_or_replace, volatile, parallel_unsafe)]
fn zarr_zonal_stats_by_time(
    foreign_table: &str,
    region_ewkb: &[u8],
    start_time: TimestampWithTimeZone,
    end_time: TimestampWithTimeZone,
) -> TableIterator<
    'static,
    (
        name!(time, TimestampWithTimeZone),
        name!(time_index, i64),
        name!(count, i64),
        name!(valid_count, i64),
        name!(min, Option<f64>),
        name!(max, Option<f64>),
        name!(sum, Option<f64>),
        name!(avg, Option<f64>),
        name!(srid, i32),
    ),
> {
    let rows = zonal_foreign_table(foreign_table, region_ewkb, start_time, end_time, None)
        .map_err(ErrorReport::from)
        .unwrap_or_report();
    TableIterator::new(rows.into_iter().map(TemporalZonalStatsRow::sql_row))
}

#[allow(clippy::type_complexity)]
#[pg_extern(
    name = "zarr_zonal_stats_by_time",
    create_or_replace,
    volatile,
    parallel_unsafe
)]
fn zarr_zonal_stats_by_time_with_selectors(
    foreign_table: &str,
    region_ewkb: &[u8],
    start_time: TimestampWithTimeZone,
    end_time: TimestampWithTimeZone,
    dimension_selectors: &str,
) -> TableIterator<
    'static,
    (
        name!(time, TimestampWithTimeZone),
        name!(time_index, i64),
        name!(count, i64),
        name!(valid_count, i64),
        name!(min, Option<f64>),
        name!(max, Option<f64>),
        name!(sum, Option<f64>),
        name!(avg, Option<f64>),
        name!(srid, i32),
    ),
> {
    let rows = zonal_foreign_table(
        foreign_table,
        region_ewkb,
        start_time,
        end_time,
        Some(dimension_selectors),
    )
    .map_err(ErrorReport::from)
    .unwrap_or_report();
    TableIterator::new(rows.into_iter().map(TemporalZonalStatsRow::sql_row))
}

fn cells_foreign_table(
    foreign_table: &str,
    region_ewkb: &[u8],
    start: TimestampWithTimeZone,
    end: TimestampWithTimeZone,
) -> ZarrFdwResult<Vec<TemporalCellRow>> {
    let mut cells = Vec::new();
    visit_foreign_table_cells(foreign_table, region_ewkb, start, end, None, |cell| {
        if cells.len() >= MAX_SPATIAL_OUTPUT_CELLS {
            return Err(ZarrFdwError::InvalidGeometry(format!(
                "spatiotemporal polygon selects more than the {MAX_SPATIAL_OUTPUT_CELLS}-cell output limit"
            )));
        }
        cells.push(cell);
        Ok(())
    })?;
    Ok(cells)
}

fn zonal_foreign_table(
    foreign_table: &str,
    region_ewkb: &[u8],
    start: TimestampWithTimeZone,
    end: TimestampWithTimeZone,
    call_selectors: Option<&str>,
) -> ZarrFdwResult<Vec<TemporalZonalStatsRow>> {
    let mut accumulators = BTreeMap::<i64, ZonalAccumulator>::new();
    let summary = visit_foreign_table_cells(
        foreign_table,
        region_ewkb,
        start,
        end,
        call_selectors,
        |cell| {
            accumulators
                .entry(cell.time_index)
                .or_default()
                .observe(cell.value)
        },
    )?;

    summary
        .times
        .into_iter()
        .map(|(time, time_index)| {
            let stats = accumulators
                .remove(&time_index)
                .unwrap_or_default()
                .finish(summary.srid)?;
            Ok(TemporalZonalStatsRow {
                time,
                time_index,
                count: stats.count,
                valid_count: stats.valid_count,
                min: stats.min,
                max: stats.max,
                sum: stats.sum,
                avg: stats.avg,
                srid: stats.srid,
            })
        })
        .collect()
}

fn visit_foreign_table_cells(
    foreign_table: &str,
    region_ewkb: &[u8],
    start: TimestampWithTimeZone,
    end: TimestampWithTimeZone,
    call_selectors: Option<&str>,
    mut visitor: impl FnMut(TemporalCellRow) -> ZarrFdwResult<()>,
) -> ZarrFdwResult<TemporalVisitSummary> {
    // Fail before any remote metadata request when the optional spatial
    // dependency is absent or incomplete.
    let postgis = PostgisCatalog::require()?;
    let selector_aware = call_selectors.is_some();
    let table = if selector_aware {
        load_zarr_foreign_table_with_selectors(foreign_table)?
    } else {
        load_zarr_foreign_table(foreign_table)?
    };
    let call_selectors = match call_selectors {
        Some(raw) => {
            DimensionSelectors::parse(
                table
                    .options
                    .get(OPT_DIMENSION_SELECTORS)
                    .map(String::as_str),
            )?;
            DimensionSelectors::parse(Some(raw))?
        }
        None => DimensionSelectors::default(),
    };
    let mut fdw = ZarrFdw::new(table.server)?;
    fdw.set_call_dimension_selectors(call_selectors)?;
    let begin = <ZarrFdw as ForeignDataWrapper<ZarrFdwError>>::begin_scan(
        &mut fdw,
        &[],
        &table.columns,
        &[],
        &None,
        &table.options,
    );
    if let Err(error) = begin {
        let _ = <ZarrFdw as ForeignDataWrapper<ZarrFdwError>>::end_scan(&mut fdw);
        return Err(error);
    }

    let result = visit_prepared_scan(&mut fdw, &postgis, region_ewkb, start, end, &mut visitor);
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
    start: TimestampWithTimeZone,
    end: TimestampWithTimeZone,
    visitor: &mut impl FnMut(TemporalCellRow) -> ZarrFdwResult<()>,
) -> ZarrFdwResult<TemporalVisitSummary> {
    // Validate the table contract before a non-overlapping envelope can
    // return an otherwise-successful empty result.
    let value_column = fdw.spatial_value_column()?.to_string();
    let operation_layout = fdw.spatial_time_layout()?;
    let time_axis = operation_layout.time;
    let time_indices = fdw.spatial_time_indices(start, end, MAX_SPATIAL_OUTPUT_CELLS)?;
    let mut times = Vec::with_capacity(time_indices.len());
    for (selected_position, &time_index) in time_indices.iter().enumerate() {
        if selected_position.is_multiple_of(SPATIAL_INTERRUPT_POLL_CELLS) {
            fdw.spatial_check_for_interrupt()?;
        }
        times.push((
            fdw.spatial_time_at_index(time_axis, time_index)?,
            pg_index("time", time_index)?,
        ));
    }
    let crs = fdw.resolved_spatial_crs()?;
    let envelope = postgis.transform_ewkb_geometry_envelope(
        region_ewkb,
        fdw.spatial_array_path(),
        crs.epsg,
    )?;
    if !fdw.apply_spatial_dimension_selectors(
        "zarr_zonal_stats_by_time",
        &[
            operation_layout.time,
            operation_layout.horizontal.x,
            operation_layout.horizontal.y,
        ],
    )? {
        return Ok(TemporalVisitSummary {
            times,
            srid: crs.epsg,
        });
    }
    let Some((window_axes, horizontal_bounds, _spatial_candidates)) = fdw
        .spatial_time_horizontal_window(
            envelope.xmin,
            envelope.ymin,
            envelope.xmax,
            envelope.ymax,
        )?
    else {
        return Ok(TemporalVisitSummary {
            times,
            srid: crs.epsg,
        });
    };
    if time_indices.is_empty() {
        return Ok(TemporalVisitSummary {
            times,
            srid: crs.epsg,
        });
    }
    let selection =
        fdw.spatial_time_selection(time_indices, horizontal_bounds, MAX_SPATIAL_CANDIDATE_CELLS)?;
    debug_assert_eq!(window_axes, selection.layout.horizontal);
    debug_assert!(selection.candidate_cells <= MAX_SPATIAL_CANDIDATE_CELLS);
    let layout = selection.layout;
    let exact_time_indices = selection.time_indices;
    fdw.restrict_to_axis_bounds(selection.bounds)?;

    let mut candidates = Vec::with_capacity(MAX_COVERAGE_CANDIDATES);
    let mut row = Row::new();
    let mut visited = 0usize;
    while <ZarrFdw as ForeignDataWrapper<ZarrFdwError>>::iter_scan(fdw, &mut row)?.is_some() {
        if visited.is_multiple_of(SPATIAL_INTERRUPT_POLL_CELLS) {
            fdw.spatial_check_for_interrupt()?;
        }
        visited = visited.saturating_add(1);

        let indices = fdw.spatial_last_emitted_global_indices()?;
        let time_index = axis_index(indices, layout.time, "time")?;
        let Ok(time_position) = exact_time_indices.binary_search(&time_index) else {
            continue;
        };
        let x_index = axis_index(indices, layout.horizontal.x, "x")?;
        let y_index = axis_index(indices, layout.horizontal.y, "y")?;
        let value = row
            .iter()
            .find(|(name, _)| name.as_str() == value_column)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "spatiotemporal result did not contain value column '{value_column}'"
                ))
            })?
            .1
            .as_ref()
            .map(numeric_cell_to_f64)
            .transpose()?;
        candidates.push(TemporalCellRow {
            time: times[time_position].0,
            x: fdw.spatial_coordinate_at_index(layout.horizontal.x, x_index)?,
            y: fdw.spatial_coordinate_at_index(layout.horizontal.y, y_index)?,
            value,
            time_index: pg_index("time", time_index)?,
            x_index: pg_index("x", x_index)?,
            y_index: pg_index("y", y_index)?,
            srid: crs.epsg,
        });
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

    Ok(TemporalVisitSummary {
        times,
        srid: crs.epsg,
    })
}

fn visit_covered_batch(
    postgis: &PostgisCatalog,
    region_ewkb: &[u8],
    fdw: &mut ZarrFdw,
    srid: i32,
    candidates: &mut Vec<TemporalCellRow>,
    visitor: &mut impl FnMut(TemporalCellRow) -> ZarrFdwResult<()>,
) -> ZarrFdwResult<()> {
    fdw.spatial_check_for_interrupt()?;
    let centers = candidates
        .iter()
        .map(|cell| (cell.x, cell.y))
        .collect::<Vec<_>>();
    let covered = postgis.covers_ewkb_geometry_points(
        region_ewkb,
        fdw.spatial_array_path(),
        srid,
        &centers,
    )?;
    if covered.len() != candidates.len() {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "PostGIS returned {} polygon coverage results for {} spatiotemporal candidate cells",
            covered.len(),
            candidates.len()
        )));
    }
    for (cell, covered) in candidates.drain(..).zip(covered) {
        if covered {
            visitor(cell)?;
        }
    }
    Ok(())
}

fn axis_index(indices: &[usize], axis: usize, name: &str) -> ZarrFdwResult<usize> {
    indices.get(axis).copied().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!(
            "spatiotemporal {name} axis {axis} is outside emitted array indexes"
        ))
    })
}

fn pg_index(name: &str, index: usize) -> ZarrFdwResult<i64> {
    i64::try_from(index).map_err(|_| {
        ZarrFdwError::InvalidMetadata(format!(
            "spatiotemporal {name} index exceeds PostgreSQL bigint range"
        ))
    })
}
