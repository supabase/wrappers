//! Read-only point sampling over one rank-2 rectilinear Zarr foreign table.

use pgrx::pg_sys::panic::{ErrorReport, ErrorReportable};
use pgrx::prelude::*;
use supabase_wrappers::prelude::{Cell, ForeignDataWrapper, Row};

use super::super::zarr_fdw::ZarrFdw;
use super::super::{ZarrFdwError, ZarrFdwResult};
use super::catalog::load_zarr_foreign_table;
use super::grid::GridCell;
use super::postgis::PostgisCatalog;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SampleMethod {
    Exact,
    Nearest,
}

impl SampleMethod {
    fn parse(value: &str) -> ZarrFdwResult<Self> {
        match value {
            "exact" => Ok(Self::Exact),
            "nearest" => Ok(Self::Nearest),
            _ => Err(ZarrFdwError::InvalidGeometry(format!(
                "point sampling method must be 'exact' or 'nearest', got '{value}'"
            ))),
        }
    }
}

#[derive(Debug, PartialEq)]
struct SampleRow {
    x: f64,
    y: f64,
    value: Option<f64>,
    x_index: i64,
    y_index: i64,
    coordinate_distance: f64,
    srid: i32,
}

impl SampleRow {
    fn sql_row(self) -> (f64, f64, Option<f64>, i64, i64, f64, i32) {
        (
            self.x,
            self.y,
            self.value,
            self.x_index,
            self.y_index,
            self.coordinate_distance,
            self.srid,
        )
    }
}

#[allow(clippy::type_complexity)]
#[pg_extern(create_or_replace, volatile, parallel_unsafe)]
fn zarr_sample(
    foreign_table: &str,
    point_ewkb: &[u8],
    method: default!(&str, "'nearest'"),
) -> TableIterator<
    'static,
    (
        name!(x, f64),
        name!(y, f64),
        name!(value, Option<f64>),
        name!(x_index, i64),
        name!(y_index, i64),
        name!(coordinate_distance, f64),
        name!(srid, i32),
    ),
> {
    let rows = sample_foreign_table(foreign_table, point_ewkb, method)
        .map_err(ErrorReport::from)
        .unwrap_or_report();
    TableIterator::new(rows.into_iter().map(SampleRow::sql_row))
}

fn sample_foreign_table(
    foreign_table: &str,
    point_ewkb: &[u8],
    method: &str,
) -> ZarrFdwResult<Vec<SampleRow>> {
    let method = SampleMethod::parse(method)?;
    // Fail before remote metadata reads when the optional spatial dependency is
    // absent or incomplete.
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

    let result = sample_prepared_scan(&mut fdw, &postgis, point_ewkb, method);
    let cleanup = <ZarrFdw as ForeignDataWrapper<ZarrFdwError>>::end_scan(&mut fdw);
    match (result, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn sample_prepared_scan(
    fdw: &mut ZarrFdw,
    postgis: &PostgisCatalog,
    point_ewkb: &[u8],
    method: SampleMethod,
) -> ZarrFdwResult<Vec<SampleRow>> {
    // Validate the table contract before exact lookup can return no rows.
    let value_column = fdw.spatial_value_column()?.to_string();
    let crs = fdw.resolved_spatial_crs()?;
    let point = postgis.transform_ewkb_point(point_ewkb, fdw.spatial_array_path(), crs.epsg)?;
    let cell = {
        let grid = fdw.rectilinear_grid()?;
        match method {
            SampleMethod::Exact => grid.exact(point.x, point.y)?,
            SampleMethod::Nearest => Some(grid.nearest(point.x, point.y)?),
        }
    };
    let Some(cell) = cell else {
        return Ok(Vec::new());
    };
    fdw.restrict_to_spatial_cell(cell.array_indices)?;

    let mut row = Row::new();
    if <ZarrFdw as ForeignDataWrapper<ZarrFdwError>>::iter_scan(fdw, &mut row)?.is_none() {
        return Err(ZarrFdwError::InvalidMetadata(
            "point sampling selected a logical cell but the Zarr executor returned no row"
                .to_string(),
        ));
    }
    let value = row
        .iter()
        .find(|(name, _)| name.as_str() == value_column)
        .ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "point sampling result did not contain value column '{value_column}'"
            ))
        })?
        .1
        .as_ref()
        .map(numeric_cell_to_f64)
        .transpose()?;

    row.clear();
    if <ZarrFdw as ForeignDataWrapper<ZarrFdwError>>::iter_scan(fdw, &mut row)?.is_some() {
        return Err(ZarrFdwError::InvalidMetadata(
            "point sampling selected more than one logical cell".to_string(),
        ));
    }
    Ok(vec![sample_row(cell, value, crs.epsg)?])
}

fn sample_row(cell: GridCell, value: Option<f64>, srid: i32) -> ZarrFdwResult<SampleRow> {
    Ok(SampleRow {
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
        coordinate_distance: cell.distance,
        srid,
    })
}

pub(super) fn numeric_cell_to_f64(cell: &Cell) -> ZarrFdwResult<f64> {
    let value = match cell {
        Cell::I8(value) => f64::from(*value),
        Cell::I16(value) => f64::from(*value),
        Cell::I32(value) => f64::from(*value),
        Cell::I64(value) => {
            let converted = *value as f64;
            if converted as i128 != i128::from(*value) {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "point sample bigint value {value} cannot be represented exactly as double precision"
                )));
            }
            converted
        }
        Cell::F32(value) => f64::from(*value),
        Cell::F64(value) => *value,
        Cell::Numeric(value) => value.to_string().parse::<f64>().map_err(|_| {
            ZarrFdwError::InvalidMetadata(
                "point sample numeric value cannot be represented as double precision".to_string(),
            )
        })?,
        _ => {
            return Err(ZarrFdwError::InvalidMetadata(
                "point sampling supports only numeric Zarr value columns".to_string(),
            ));
        }
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_documented_sampling_methods() {
        assert_eq!(SampleMethod::parse("exact").unwrap(), SampleMethod::Exact);
        assert_eq!(
            SampleMethod::parse("nearest").unwrap(),
            SampleMethod::Nearest
        );
        assert!(SampleMethod::parse("Nearest").is_err());
        assert!(SampleMethod::parse("").is_err());
    }

    #[test]
    fn widens_supported_numeric_cells_without_silent_bigint_rounding() {
        assert_eq!(numeric_cell_to_f64(&Cell::F32(1.5)).unwrap(), 1.5);
        assert_eq!(numeric_cell_to_f64(&Cell::I32(42)).unwrap(), 42.0);
        assert!(numeric_cell_to_f64(&Cell::I64(9_007_199_254_740_993)).is_err());
        assert!(numeric_cell_to_f64(&Cell::String("42".to_string())).is_err());
    }
}
