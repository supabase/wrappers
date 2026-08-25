use super::super::chunk::IndexBounds;
use super::super::dataset::DimensionRole;
use super::super::{ZarrFdwError, ZarrFdwResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HorizontalGridKind {
    Projected,
    Geographic,
}

/// Array-axis positions for one unambiguous horizontal coordinate pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HorizontalAxes {
    pub(crate) x: usize,
    pub(crate) y: usize,
    pub(crate) kind: HorizontalGridKind,
}

pub(crate) fn discover_horizontal_axes_from_roles(
    roles: impl IntoIterator<Item = DimensionRole>,
) -> ZarrFdwResult<HorizontalAxes> {
    let roles = roles.into_iter().collect::<Vec<_>>();
    let positions = |wanted| {
        roles
            .iter()
            .enumerate()
            .filter_map(|(axis, role)| (*role == wanted).then_some(axis))
            .collect::<Vec<_>>()
    };
    let projected_x = positions(DimensionRole::SpatialX);
    let projected_y = positions(DimensionRole::SpatialY);
    let longitude = positions(DimensionRole::Longitude);
    let latitude = positions(DimensionRole::Latitude);
    let horizontal_count = projected_x
        .len()
        .saturating_add(projected_y.len())
        .saturating_add(longitude.len())
        .saturating_add(latitude.len());

    let projected = projected_x.len() == 1
        && projected_y.len() == 1
        && longitude.is_empty()
        && latitude.is_empty();
    if projected {
        return Ok(HorizontalAxes {
            x: projected_x[0],
            y: projected_y[0],
            kind: HorizontalGridKind::Projected,
        });
    }

    let geographic = longitude.len() == 1
        && latitude.len() == 1
        && projected_x.is_empty()
        && projected_y.is_empty();
    if geographic {
        return Ok(HorizontalAxes {
            x: longitude[0],
            y: latitude[0],
            kind: HorizontalGridKind::Geographic,
        });
    }

    Err(ZarrFdwError::InvalidMetadata(format!(
        "spatial execution requires exactly one compatible horizontal pair (SpatialX/SpatialY or Longitude/Latitude), found {horizontal_count} horizontal role assignments"
    )))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AxisOrder {
    Ascending,
    Descending,
    Unordered,
}

/// Classify strict axis direction. Empty and singleton axes are ascending;
/// repeated values are unordered because they do not have a strict direction.
pub(crate) fn axis_order(values: &[f64]) -> AxisOrder {
    if values.windows(2).all(|pair| pair[0] < pair[1]) {
        AxisOrder::Ascending
    } else if values.windows(2).all(|pair| pair[0] > pair[1]) {
        AxisOrder::Descending
    } else {
        AxisOrder::Unordered
    }
}

fn validate_finite_values(name: &str, values: &[f64]) -> ZarrFdwResult<()> {
    if let Some((index, value)) = values
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "spatial coordinate '{name}' contains non-finite value {value} at index {index}"
        )));
    }
    Ok(())
}

fn validate_finite_target(name: &str, value: f64) -> ZarrFdwResult<()> {
    if !value.is_finite() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "spatial {name} must be finite, got {value}"
        )));
    }
    Ok(())
}

/// Return an inclusive, conservative array-index interval containing every
/// center in `[lo, hi]`. Unordered axes may over-select between the first and
/// last matching global indexes, but never omit a matching center.
pub(crate) fn inclusive_center_bounds(
    values: &[f64],
    lo: f64,
    hi: f64,
) -> ZarrFdwResult<Option<IndexBounds>> {
    validate_finite_values("axis", values)?;
    validate_finite_target("lower bound", lo)?;
    validate_finite_target("upper bound", hi)?;
    if values.is_empty() || lo > hi {
        return Ok(None);
    }

    let bounds = match axis_order(values) {
        AxisOrder::Ascending => {
            let start = values.partition_point(|value| *value < lo);
            let end_exclusive = values.partition_point(|value| *value <= hi);
            (start < end_exclusive).then(|| IndexBounds {
                start,
                end: end_exclusive - 1,
            })
        }
        AxisOrder::Descending => {
            let start = values.partition_point(|value| *value > hi);
            let end_exclusive = values.partition_point(|value| *value >= lo);
            (start < end_exclusive).then(|| IndexBounds {
                start,
                end: end_exclusive - 1,
            })
        }
        AxisOrder::Unordered => {
            let mut matches = values
                .iter()
                .enumerate()
                .filter_map(|(index, value)| (lo <= *value && *value <= hi).then_some(index));
            matches.next().map(|start| IndexBounds {
                start,
                end: matches.next_back().unwrap_or(start),
            })
        }
    };
    Ok(bounds)
}

/// Locate an exactly equal center, preferring the lowest global array index.
pub(crate) fn exact_center_index(values: &[f64], target: f64) -> ZarrFdwResult<Option<usize>> {
    validate_finite_values("axis", values)?;
    validate_finite_target("coordinate", target)?;
    Ok(values.iter().position(|value| *value == target))
}

/// Locate the nearest center, preferring the lowest global array index on a
/// distance tie. The linear pass is also correct for unordered coordinates.
pub(crate) fn nearest_center_index(values: &[f64], target: f64) -> ZarrFdwResult<Option<usize>> {
    validate_finite_values("axis", values)?;
    validate_finite_target("coordinate", target)?;
    let mut best: Option<(usize, f64)> = None;
    for (index, value) in values.iter().enumerate() {
        let distance = (*value - target).abs();
        if best.is_none_or(|(_, best_distance)| distance < best_distance) {
            best = Some((index, distance));
        }
    }
    Ok(best.map(|(index, _)| index))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct GridCell {
    /// Indexes in the original two-dimensional array axis order.
    pub(crate) array_indices: [usize; 2],
    pub(crate) x_index: usize,
    pub(crate) y_index: usize,
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) distance: f64,
}

/// One resolved horizontal cell independent of the selected array's rank.
///
/// Native array-axis placement remains with `HorizontalAxes`; this value keeps
/// only the semantic X/Y indexes and coordinates needed by operation results
/// and exact PostGIS masking.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct HorizontalCell {
    pub(crate) x_index: usize,
    pub(crate) y_index: usize,
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) distance: f64,
}

impl From<GridCell> for HorizontalCell {
    fn from(cell: GridCell) -> Self {
        Self {
            x_index: cell.x_index,
            y_index: cell.y_index,
            x: cell.x,
            y: cell.y,
            distance: cell.distance,
        }
    }
}

/// Finite transformed geometry bounds in the grid's coordinate reference
/// system. Bounds are inclusive because exact polygon masking happens after
/// this conservative center-window selection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct CoordinateEnvelope {
    x_min: f64,
    y_min: f64,
    x_max: f64,
    y_max: f64,
}

impl CoordinateEnvelope {
    pub(crate) fn new(x_min: f64, y_min: f64, x_max: f64, y_max: f64) -> ZarrFdwResult<Self> {
        for (name, value) in [
            ("x minimum", x_min),
            ("y minimum", y_min),
            ("x maximum", x_max),
            ("y maximum", y_max),
        ] {
            if !value.is_finite() {
                return Err(ZarrFdwError::InvalidGeometry(format!(
                    "transformed envelope {name} must be finite, got {value}"
                )));
            }
        }
        if x_min > x_max || y_min > y_max {
            return Err(ZarrFdwError::InvalidGeometry(format!(
                "spatial envelope minimums must not exceed maximums, got ({x_min}, {y_min})..({x_max}, {y_max})"
            )));
        }
        Ok(Self {
            x_min,
            y_min,
            x_max,
            y_max,
        })
    }
}

/// Checked index bounds for one conservative rank-2 center window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GridWindowPlan {
    bounds: [IndexBounds; 2],
    total: usize,
}

impl GridWindowPlan {
    fn new(bounds: [IndexBounds; 2]) -> ZarrFdwResult<Self> {
        let total = checked_window_cell_count(&bounds)?;
        Ok(Self { bounds, total })
    }

    pub(crate) fn bounds(&self) -> [IndexBounds; 2] {
        self.bounds
    }

    pub(crate) fn total_cells(&self) -> usize {
        self.total
    }
}

fn checked_window_cell_count(bounds: &[IndexBounds; 2]) -> ZarrFdwResult<usize> {
    bounds.iter().try_fold(1usize, |total, bounds| {
        let extent = bounds
            .end
            .checked_sub(bounds.start)
            .and_then(|span| span.checked_add(1))
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "spatial window extent exceeds this platform's index capacity".to_string(),
                )
            })?;
        total.checked_mul(extent).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "spatial window cell count exceeds this platform's index capacity".to_string(),
            )
        })
    })
}

/// A two-dimensional rectilinear grid whose semantic X/Y axes may occur in
/// either array order.
#[derive(Debug)]
pub(crate) struct RectilinearGrid<'a> {
    axes: HorizontalAxes,
    x: &'a [f64],
    y: &'a [f64],
}

impl<'a> RectilinearGrid<'a> {
    pub(crate) fn new(axes: HorizontalAxes, x: &'a [f64], y: &'a [f64]) -> ZarrFdwResult<Self> {
        if axes.x > 1 || axes.y > 1 || axes.x == axes.y {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "rectilinear grid requires two distinct rank-2 array axes, got x axis {} and y axis {}",
                axes.x, axes.y
            )));
        }
        if x.is_empty() || y.is_empty() {
            return Err(ZarrFdwError::InvalidMetadata(
                "rectilinear grid coordinate axes must not be empty".to_string(),
            ));
        }
        validate_finite_values("x", x)?;
        validate_finite_values("y", y)?;
        Ok(Self { axes, x, y })
    }

    pub(crate) fn axes(&self) -> HorizontalAxes {
        self.axes
    }

    pub(crate) fn exact(&self, x: f64, y: f64) -> ZarrFdwResult<Option<GridCell>> {
        let Some(x_index) = exact_center_index(self.x, x)? else {
            return Ok(None);
        };
        let Some(y_index) = exact_center_index(self.y, y)? else {
            return Ok(None);
        };
        self.cell(x_index, y_index, x, y).map(Some)
    }

    pub(crate) fn nearest(&self, x: f64, y: f64) -> ZarrFdwResult<GridCell> {
        let x_index = nearest_center_index(self.x, x)?.expect("grid x axis is non-empty");
        let y_index = nearest_center_index(self.y, y)?.expect("grid y axis is non-empty");
        self.cell(x_index, y_index, x, y)
    }

    pub(crate) fn cell(
        &self,
        x_index: usize,
        y_index: usize,
        target_x: f64,
        target_y: f64,
    ) -> ZarrFdwResult<GridCell> {
        validate_finite_target("x coordinate", target_x)?;
        validate_finite_target("y coordinate", target_y)?;
        let x = *self.x.get(x_index).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "x index {x_index} is outside coordinate length {}",
                self.x.len()
            ))
        })?;
        let y = *self.y.get(y_index).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "y index {y_index} is outside coordinate length {}",
                self.y.len()
            ))
        })?;
        let mut array_indices = [0; 2];
        array_indices[self.axes.x] = x_index;
        array_indices[self.axes.y] = y_index;
        Ok(GridCell {
            array_indices,
            x_index,
            y_index,
            x,
            y,
            distance: (x - target_x).hypot(y - target_y),
        })
    }

    /// Return X/Y center bounds placed in original array axis order.
    pub(crate) fn inclusive_bounds(
        &self,
        x_lo: f64,
        y_lo: f64,
        x_hi: f64,
        y_hi: f64,
    ) -> ZarrFdwResult<Option<[IndexBounds; 2]>> {
        let Some(x_bounds) = inclusive_center_bounds(self.x, x_lo, x_hi)? else {
            return Ok(None);
        };
        let Some(y_bounds) = inclusive_center_bounds(self.y, y_lo, y_hi)? else {
            return Ok(None);
        };
        let mut array_bounds = [x_bounds; 2];
        array_bounds[self.axes.x] = x_bounds;
        array_bounds[self.axes.y] = y_bounds;
        Ok(Some(array_bounds))
    }

    /// Plan a bounded, lazy C-order stream of candidate centers for one
    /// transformed geometry envelope. `None` means no grid center can match.
    pub(crate) fn window_plan(
        &self,
        envelope: CoordinateEnvelope,
    ) -> ZarrFdwResult<Option<GridWindowPlan>> {
        let Some(bounds) = self.inclusive_bounds(
            envelope.x_min,
            envelope.y_min,
            envelope.x_max,
            envelope.y_max,
        )?
        else {
            return Ok(None);
        };
        GridWindowPlan::new(bounds).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projected(x: usize, y: usize) -> HorizontalAxes {
        HorizontalAxes {
            x,
            y,
            kind: HorizontalGridKind::Projected,
        }
    }

    #[test]
    fn discovers_only_complete_compatible_horizontal_pairs() {
        assert_eq!(
            discover_horizontal_axes_from_roles([
                DimensionRole::SpatialY,
                DimensionRole::Time,
                DimensionRole::SpatialX,
            ])
            .unwrap(),
            HorizontalAxes {
                x: 2,
                y: 0,
                kind: HorizontalGridKind::Projected,
            }
        );
        assert_eq!(
            discover_horizontal_axes_from_roles([
                DimensionRole::Latitude,
                DimensionRole::Longitude,
            ])
            .unwrap(),
            HorizontalAxes {
                x: 1,
                y: 0,
                kind: HorizontalGridKind::Geographic,
            }
        );

        for roles in [
            vec![],
            vec![DimensionRole::SpatialX],
            vec![DimensionRole::SpatialX, DimensionRole::Latitude],
            vec![
                DimensionRole::SpatialX,
                DimensionRole::SpatialX,
                DimensionRole::SpatialY,
            ],
            vec![
                DimensionRole::SpatialX,
                DimensionRole::SpatialY,
                DimensionRole::Longitude,
                DimensionRole::Latitude,
            ],
        ] {
            assert!(discover_horizontal_axes_from_roles(roles).is_err());
        }
    }

    #[test]
    fn classifies_strict_axis_order() {
        assert_eq!(axis_order(&[]), AxisOrder::Ascending);
        assert_eq!(axis_order(&[7.0]), AxisOrder::Ascending);
        assert_eq!(axis_order(&[1.0, 2.0, 3.0]), AxisOrder::Ascending);
        assert_eq!(axis_order(&[3.0, 2.0, 1.0]), AxisOrder::Descending);
        assert_eq!(axis_order(&[1.0, 1.0, 2.0]), AxisOrder::Unordered);
        assert_eq!(axis_order(&[1.0, 3.0, 2.0]), AxisOrder::Unordered);
    }

    #[test]
    fn computes_inclusive_bounds_for_every_axis_order() {
        assert_eq!(
            inclusive_center_bounds(&[10.0, 20.0, 30.0, 40.0], 20.0, 30.0).unwrap(),
            Some(IndexBounds { start: 1, end: 2 })
        );
        assert_eq!(
            inclusive_center_bounds(&[40.0, 30.0, 20.0, 10.0], 20.0, 30.0).unwrap(),
            Some(IndexBounds { start: 1, end: 2 })
        );
        assert_eq!(
            inclusive_center_bounds(&[50.0, 10.0, 40.0, 20.0, 30.0], 20.0, 40.0).unwrap(),
            Some(IndexBounds { start: 2, end: 4 })
        );
        assert_eq!(
            inclusive_center_bounds(&[50.0, 10.0, 40.0, 20.0, 30.0], 21.0, 29.0).unwrap(),
            None
        );
        assert_eq!(
            inclusive_center_bounds(&[10.0, 20.0], 21.0, 20.0).unwrap(),
            None
        );
    }

    #[test]
    fn exact_and_nearest_choose_lowest_global_index() {
        assert_eq!(
            exact_center_index(&[30.0, 20.0, 20.0, 10.0], 20.0).unwrap(),
            Some(1)
        );
        assert_eq!(
            nearest_center_index(&[50.0, 40.0, 30.0, 20.0], 35.0).unwrap(),
            Some(1)
        );
        assert_eq!(
            nearest_center_index(&[0.0, 100.0, 20.0, 40.0], 30.0).unwrap(),
            Some(2)
        );
        assert_eq!(nearest_center_index(&[], 1.0).unwrap(), None);
    }

    #[test]
    fn grid_preserves_array_axis_order_and_reports_distance() {
        let y = [50.0, 40.0, 30.0, 20.0, 10.0];
        let x = [100.0, 110.0, 120.0, 130.0, 140.0, 150.0];
        let grid = RectilinearGrid::new(projected(1, 0), &x, &y).unwrap();

        assert_eq!(axis_order(&x), AxisOrder::Ascending);
        assert_eq!(axis_order(&y), AxisOrder::Descending);
        let nearest = grid.nearest(121.0, 39.0).unwrap();
        assert_eq!(nearest.array_indices, [1, 2]);
        assert_eq!((nearest.x_index, nearest.y_index), (2, 1));
        assert_eq!((nearest.x, nearest.y), (120.0, 40.0));
        assert_eq!(nearest.distance, 2.0_f64.sqrt());

        let tie = grid.nearest(125.0, 35.0).unwrap();
        assert_eq!(tie.array_indices, [1, 2]);
        assert_eq!((tie.x, tie.y), (120.0, 40.0));
        assert_eq!(
            grid.exact(120.0, 40.0).unwrap(),
            Some(grid.cell(2, 1, 120.0, 40.0).unwrap())
        );
        assert_eq!(grid.exact(121.0, 40.0).unwrap(), None);
    }

    #[test]
    fn grid_bounds_follow_array_axis_order() {
        let grid = RectilinearGrid::new(
            projected(0, 1),
            &[100.0, 110.0, 120.0, 130.0],
            &[40.0, 30.0, 20.0, 10.0],
        )
        .unwrap();
        assert_eq!(
            grid.inclusive_bounds(110.0, 20.0, 120.0, 30.0).unwrap(),
            Some([
                IndexBounds { start: 1, end: 2 },
                IndexBounds { start: 1, end: 2 },
            ])
        );
    }

    #[test]
    fn grid_rejects_invalid_rank_axes_values_and_indexes() {
        assert!(RectilinearGrid::new(projected(0, 0), &[1.0], &[2.0]).is_err());
        assert!(RectilinearGrid::new(projected(0, 2), &[1.0], &[2.0]).is_err());
        assert!(RectilinearGrid::new(projected(0, 1), &[], &[2.0]).is_err());
        assert!(RectilinearGrid::new(projected(0, 1), &[f64::NAN], &[2.0]).is_err());

        let grid = RectilinearGrid::new(projected(0, 1), &[1.0], &[2.0]).unwrap();
        assert!(grid.cell(1, 0, 1.0, 2.0).is_err());
        assert!(grid.nearest(f64::INFINITY, 2.0).is_err());
    }

    #[test]
    fn envelope_requires_finite_ordered_bounds() {
        assert_eq!(
            CoordinateEnvelope::new(1.0, 2.0, 3.0, 4.0).unwrap(),
            CoordinateEnvelope {
                x_min: 1.0,
                y_min: 2.0,
                x_max: 3.0,
                y_max: 4.0,
            }
        );
        assert!(CoordinateEnvelope::new(3.0, 2.0, 1.0, 4.0).is_err());
        assert!(CoordinateEnvelope::new(1.0, 4.0, 3.0, 2.0).is_err());
        assert!(CoordinateEnvelope::new(f64::NAN, 2.0, 3.0, 4.0).is_err());
        assert!(CoordinateEnvelope::new(1.0, 2.0, f64::INFINITY, 4.0).is_err());
    }

    #[test]
    fn window_plan_preserves_array_order_and_checked_count() {
        let grid = RectilinearGrid::new(
            projected(1, 0),
            &[100.0, 110.0, 120.0, 130.0],
            &[40.0, 30.0, 20.0, 10.0],
        )
        .unwrap();
        let envelope = CoordinateEnvelope::new(110.0, 20.0, 120.0, 30.0).unwrap();
        let plan = grid.window_plan(envelope).unwrap().unwrap();
        assert_eq!(
            plan.bounds(),
            [
                IndexBounds { start: 1, end: 2 },
                IndexBounds { start: 1, end: 2 },
            ]
        );
        assert_eq!(plan.total_cells(), 4);
    }

    #[test]
    fn unordered_windows_overfetch_conservatively() {
        let grid = RectilinearGrid::new(
            projected(0, 1),
            &[300.0, 999.0, 100.0, 200.0, 400.0],
            &[10.0],
        )
        .unwrap();
        let envelope = CoordinateEnvelope::new(200.0, 10.0, 400.0, 10.0).unwrap();
        let plan = grid.window_plan(envelope).unwrap().unwrap();
        assert_eq!(
            plan.bounds(),
            [
                IndexBounds { start: 0, end: 4 },
                IndexBounds { start: 0, end: 0 },
            ]
        );
        assert_eq!(plan.total_cells(), 5);
    }

    #[test]
    fn window_plan_handles_empty_singleton_and_checked_limits() {
        let grid = RectilinearGrid::new(projected(0, 1), &[1.0], &[2.0]).unwrap();
        let empty_envelope = CoordinateEnvelope::new(3.0, 2.0, 4.0, 2.0).unwrap();
        assert!(grid.window_plan(empty_envelope).unwrap().is_none());

        let envelope = CoordinateEnvelope::new(1.0, 2.0, 1.0, 2.0).unwrap();
        let plan = grid.window_plan(envelope).unwrap().unwrap();
        assert_eq!(plan.total_cells(), 1);

        assert!(
            checked_window_cell_count(&[
                IndexBounds {
                    start: 0,
                    end: usize::MAX,
                },
                IndexBounds { start: 0, end: 0 },
            ])
            .is_err()
        );
        assert!(
            checked_window_cell_count(&[
                IndexBounds {
                    start: 1,
                    end: usize::MAX,
                },
                IndexBounds { start: 0, end: 1 },
            ])
            .is_err()
        );
    }
}
