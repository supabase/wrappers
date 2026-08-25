//! Pure executor-time planning for conservative coordinate and chunk pruning.
//!
//! Planning happens only after array metadata and required coordinate values
//! are loaded. A plan stores one inclusive chunk range per axis and never
//! materializes the Cartesian set of chunk coordinates.

use super::chunk::{axis_chunk_ranges, index_bounds_from_value_range};
use super::meta::ArrayMeta;
use super::selection::Selection;
use super::{ZarrFdwError, ZarrFdwResult};

pub(crate) type CoordinateRange = (Option<f64>, Option<f64>);

/// Rank-sized, lazily executable scan plan.
///
/// Exact SQL, temporal, selector, and spatial residual checks remain with
/// their existing execution layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScanPlan {
    selection: Selection,
    axis_chunk_ranges: Vec<(usize, usize)>,
    chunks_total: u64,
    chunks_selected: u64,
}

impl ScanPlan {
    #[cfg(test)]
    pub(crate) fn selection(&self) -> &Selection {
        &self.selection
    }

    pub(crate) fn axis_chunk_ranges(&self) -> &[(usize, usize)] {
        &self.axis_chunk_ranges
    }

    pub(crate) fn chunks_total(&self) -> u64 {
        self.chunks_total
    }

    pub(crate) fn chunks_selected(&self) -> u64 {
        self.chunks_selected
    }

    pub(crate) fn into_selection(self) -> Selection {
        self.selection
    }
}

pub(crate) struct ScanPlanner<'a> {
    meta: &'a ArrayMeta,
}

impl<'a> ScanPlanner<'a> {
    pub(crate) fn new(meta: &'a ArrayMeta) -> Self {
        Self { meta }
    }

    pub(crate) fn plan(&self, selection: Selection) -> ZarrFdwResult<ScanPlan> {
        selection.validate(self.meta)?;
        let axis_chunk_ranges = if selection.is_empty() {
            Vec::new()
        } else {
            axis_chunk_ranges(self.meta, selection.axis_bounds())?
        };
        let chunks_total = saturating_product(self.meta.chunks_per_axis());
        let chunks_selected = if selection.is_empty() {
            0
        } else {
            saturating_range_product(&axis_chunk_ranges)
        };

        Ok(ScanPlan {
            selection,
            axis_chunk_ranges,
            chunks_total,
            chunks_selected,
        })
    }

    /// Convert conservative coordinate-space ranges into index bounds, then
    /// derive their lazy chunk plan. Unordered coordinates disable pruning for
    /// that axis; exact consumers still apply their residual predicates.
    pub(crate) fn plan_coordinate_ranges(
        &self,
        axis_names: &[String],
        coordinate_values: &[Option<Vec<f64>>],
        ranges: &[CoordinateRange],
    ) -> ZarrFdwResult<ScanPlan> {
        let rank = self.meta.shape.len();
        if axis_names.len() != rank || coordinate_values.len() != rank || ranges.len() != rank {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "scan planning inputs do not match array rank {rank}"
            )));
        }

        let mut bounds = Vec::with_capacity(rank);
        let mut empty = false;
        for (axis, &(lo, hi)) in ranges.iter().enumerate() {
            if lo.is_none() && hi.is_none() {
                bounds.push(None);
                continue;
            }
            let coords = coordinate_values[axis].as_deref().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "coordinate '{}' is required for predicate pruning but was not loaded",
                    axis_names[axis]
                ))
            })?;
            if !coordinate_values_are_monotonic(coords) {
                bounds.push(None);
                continue;
            }
            let axis_bounds = index_bounds_from_value_range(coords, lo, hi);
            if axis_bounds.is_none() {
                empty = true;
            }
            bounds.push(axis_bounds);
        }

        let selection = if empty {
            Selection::empty(rank)
        } else {
            Selection::from_axis_bounds(bounds)
        };
        self.plan(selection)
    }
}

fn coordinate_values_are_monotonic(values: &[f64]) -> bool {
    values.windows(2).all(|pair| pair[0] <= pair[1])
        || values.windows(2).all(|pair| pair[0] >= pair[1])
}

fn saturating_product(extents: impl IntoIterator<Item = u64>) -> u64 {
    extents
        .into_iter()
        .fold(1u64, |total, extent| total.saturating_mul(extent))
}

fn saturating_range_product(ranges: &[(usize, usize)]) -> u64 {
    ranges.iter().fold(1u64, |total, &(start, end)| {
        let extent = end
            .checked_sub(start)
            .and_then(|span| span.checked_add(1))
            .and_then(|count| u64::try_from(count).ok())
            .unwrap_or(u64::MAX);
        total.saturating_mul(extent)
    })
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::super::chunk::IndexBounds;
    use super::super::codec::CodecPipeline;
    use super::super::meta::ChunkKeyEncoding;
    use super::super::sharding::StorageLayout;
    use super::*;

    fn meta(shape: Vec<u64>, chunks: Vec<u64>) -> ArrayMeta {
        ArrayMeta {
            zarr_format: 2,
            shape,
            chunks,
            dtype: "<f4".to_string(),
            compressor: None,
            codec_pipeline: CodecPipeline::raw_v2(),
            storage_layout: StorageLayout::Direct,
            fill_value: Value::Null,
            chunk_key_encoding: ChunkKeyEncoding::V2 { separator: '.' },
            order: 'C',
            filters: None,
        }
    }

    fn plan(meta: &ArrayMeta, selection: Selection) -> ScanPlan {
        ScanPlanner::new(meta).plan(selection).unwrap()
    }

    #[test]
    fn plans_full_and_bounded_selections_without_chunk_enumeration() {
        let meta = meta(vec![48, 100, 100], vec![4, 10, 10]);
        let full = plan(&meta, Selection::full(3));
        assert_eq!(full.axis_chunk_ranges(), &[(0, 11), (0, 9), (0, 9)]);
        assert_eq!(full.chunks_total(), 1_200);
        assert_eq!(full.chunks_selected(), 1_200);

        let bounded = plan(
            &meta,
            Selection::from_axis_bounds(vec![
                Some(IndexBounds { start: 5, end: 11 }),
                None,
                Some(IndexBounds { start: 20, end: 39 }),
            ]),
        );
        assert_eq!(bounded.axis_chunk_ranges(), &[(1, 2), (0, 9), (2, 3)]);
        assert_eq!(bounded.chunks_total(), 1_200);
        assert_eq!(bounded.chunks_selected(), 40);
    }

    #[test]
    fn explicit_empty_selection_preserves_total_and_selects_zero_chunks() {
        let meta = meta(vec![8, 8], vec![4, 4]);
        let empty = plan(&meta, Selection::empty(2));
        let full = plan(&meta, Selection::full(2));

        assert!(empty.selection().is_empty());
        assert!(empty.axis_chunk_ranges().is_empty());
        assert_eq!(empty.chunks_total(), 4);
        assert_eq!(empty.chunks_selected(), 0);
        assert_eq!(full.axis_chunk_ranges(), &[(0, 1), (0, 1)]);
        assert_eq!(full.chunks_selected(), 4);
    }

    #[test]
    fn coordinate_ranges_plan_ascending_descending_and_no_overlap() {
        let meta = meta(vec![5, 5], vec![2, 2]);
        let planner = ScanPlanner::new(&meta);
        let axes = vec!["ascending".to_string(), "descending".to_string()];
        let coords = vec![
            Some(vec![0.0, 10.0, 20.0, 30.0, 40.0]),
            Some(vec![40.0, 30.0, 20.0, 10.0, 0.0]),
        ];
        let plan = planner
            .plan_coordinate_ranges(
                &axes,
                &coords,
                &[(Some(10.0), Some(30.0)), (Some(10.0), Some(30.0))],
            )
            .unwrap();

        assert_eq!(
            plan.selection().axis_bounds(),
            &[
                Some(IndexBounds { start: 1, end: 3 }),
                Some(IndexBounds { start: 1, end: 3 })
            ]
        );
        assert_eq!(plan.axis_chunk_ranges(), &[(0, 1), (0, 1)]);

        let empty = planner
            .plan_coordinate_ranges(&axes, &coords, &[(Some(100.0), None), (None, None)])
            .unwrap();
        assert!(empty.selection().is_empty());
        assert_eq!(empty.chunks_selected(), 0);
        assert!(empty.axis_chunk_ranges().is_empty());
    }

    #[test]
    fn unordered_coordinates_disable_only_that_axis_pruning() {
        let meta = meta(vec![4, 4], vec![2, 2]);
        let plan = ScanPlanner::new(&meta)
            .plan_coordinate_ranges(
                &["unordered".to_string(), "ordered".to_string()],
                &[
                    Some(vec![30.0, 10.0, 20.0, 0.0]),
                    Some(vec![0.0, 10.0, 20.0, 30.0]),
                ],
                &[(Some(10.0), Some(20.0)), (Some(10.0), Some(20.0))],
            )
            .unwrap();

        assert_eq!(
            plan.selection().axis_bounds(),
            &[None, Some(IndexBounds { start: 1, end: 2 })]
        );
        assert_eq!(plan.axis_chunk_ranges(), &[(0, 1), (0, 1)]);
    }

    #[test]
    fn coordinate_range_inputs_must_be_rank_aligned_and_loaded() {
        let meta = meta(vec![4], vec![2]);
        let planner = ScanPlanner::new(&meta);

        let rank_error = planner.plan_coordinate_ranges(&[], &[], &[]).unwrap_err();
        assert!(rank_error.to_string().contains("do not match array rank 1"));

        let missing_error = planner
            .plan_coordinate_ranges(&["x".to_string()], &[None], &[(Some(1.0), Some(2.0))])
            .unwrap_err();
        assert!(
            missing_error
                .to_string()
                .contains("coordinate 'x' is required for predicate pruning but was not loaded")
        );
    }

    #[test]
    fn rank_64_plan_stays_rank_sized_and_saturates_counts() {
        let meta = meta(vec![2; 64], vec![1; 64]);
        let plan = plan(&meta, Selection::full(64));

        assert_eq!(plan.axis_chunk_ranges().len(), 64);
        assert!(
            plan.axis_chunk_ranges()
                .iter()
                .all(|range| *range == (0, 1))
        );
        assert_eq!(plan.chunks_total(), u64::MAX);
        assert_eq!(plan.chunks_selected(), u64::MAX);
    }

    #[test]
    fn selection_validation_errors_propagate_before_chunk_math() {
        let meta = meta(vec![8, 8], vec![4, 4]);
        let rank_error = ScanPlanner::new(&meta)
            .plan(Selection::full(3))
            .unwrap_err();
        assert!(rank_error.to_string().contains(
            "zarr array metadata missing or invalid: selection rank 3 does not match array rank 2"
        ));

        for bounds in [
            IndexBounds { start: 5, end: 4 },
            IndexBounds { start: 0, end: 8 },
        ] {
            let error = ScanPlanner::new(&meta)
                .plan(Selection::from_axis_bounds(vec![Some(bounds), None]))
                .unwrap_err();
            assert!(error.to_string().contains("selection index bounds"));
        }
    }
}
