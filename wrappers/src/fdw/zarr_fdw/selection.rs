//! Pure candidate-cell selection for Zarr scans.
//!
//! A selection is a conservative, rank-aligned rectangular window. Exact SQL,
//! temporal, and spatial predicates remain the responsibility of their current
//! execution layers.

use super::chunk::{IndexBounds, axis_chunk_ranges};
use super::meta::ArrayMeta;
use super::{ZarrFdwError, ZarrFdwResult};

/// Conservative array-index bounds selected for one scan.
///
/// `None` leaves an axis unconstrained. Since that cannot also represent a
/// selection with no cells, emptiness is tracked explicitly for the complete
/// Cartesian product.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Selection {
    axis_bounds: Vec<Option<IndexBounds>>,
    empty: bool,
}

impl Selection {
    #[cfg(test)]
    pub(crate) fn full(rank: usize) -> Self {
        Self {
            axis_bounds: vec![None; rank],
            empty: false,
        }
    }

    pub(crate) fn empty(rank: usize) -> Self {
        Self {
            axis_bounds: vec![None; rank],
            empty: true,
        }
    }

    pub(crate) fn from_axis_bounds(axis_bounds: Vec<Option<IndexBounds>>) -> Self {
        Self {
            axis_bounds,
            empty: false,
        }
    }

    pub(crate) fn axis_bounds(&self) -> &[Option<IndexBounds>] {
        &self.axis_bounds
    }

    pub(crate) fn rank(&self) -> usize {
        self.axis_bounds.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.empty
    }

    /// Validate this rank-sized selection and derive its inclusive chunk
    /// ranges. An explicitly empty selection never addresses a chunk.
    pub(crate) fn chunk_ranges(&self, meta: &ArrayMeta) -> ZarrFdwResult<Vec<(usize, usize)>> {
        if self.rank() != meta.shape.len() {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "selection rank {} does not match array rank {}",
                self.rank(),
                meta.shape.len()
            )));
        }

        for (axis, bounds) in self.axis_bounds.iter().enumerate() {
            let Some(bounds) = bounds else {
                continue;
            };
            let length = meta.shape_extent(axis)?;
            if bounds.start > bounds.end || bounds.end >= length {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "selection index bounds {}..={} are invalid for dimension {axis} length {length}",
                    bounds.start, bounds.end
                )));
            }
        }

        if self.empty {
            Ok(Vec::new())
        } else {
            axis_chunk_ranges(meta, &self.axis_bounds)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

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

    #[test]
    fn full_selection_keeps_every_axis_unconstrained() {
        let meta = meta(vec![48, 100, 25], vec![4, 10, 5]);
        let selection = Selection::full(3);

        assert_eq!(selection.rank(), 3);
        assert_eq!(selection.axis_bounds(), &[None, None, None]);
        assert!(!selection.is_empty());
        assert_eq!(
            selection.chunk_ranges(&meta).unwrap(),
            vec![(0, 11), (0, 9), (0, 4)]
        );
    }

    #[test]
    fn bounded_selection_derives_inclusive_chunk_ranges() {
        let meta = meta(vec![48, 100, 100], vec![4, 10, 10]);
        let bounds = vec![
            Some(IndexBounds { start: 5, end: 11 }),
            None,
            Some(IndexBounds { start: 20, end: 39 }),
        ];
        let selection = Selection::from_axis_bounds(bounds.clone());

        assert_eq!(selection.axis_bounds(), bounds.as_slice());
        assert_eq!(
            selection.chunk_ranges(&meta).unwrap(),
            vec![(1, 2), (0, 9), (2, 3)]
        );
    }

    #[test]
    fn empty_selection_is_distinct_from_an_unconstrained_selection() {
        let meta = meta(vec![8, 8], vec![4, 4]);
        let empty = Selection::empty(2);
        let full = Selection::full(2);

        assert_eq!(empty.axis_bounds(), full.axis_bounds());
        assert!(empty.is_empty());
        assert!(!full.is_empty());
        assert!(empty.chunk_ranges(&meta).unwrap().is_empty());
        assert_eq!(full.chunk_ranges(&meta).unwrap(), vec![(0, 1), (0, 1)]);
    }

    #[test]
    fn rank_mismatch_is_rejected_before_chunk_math() {
        let meta = meta(vec![8, 8], vec![4, 4]);
        let error = Selection::full(3).chunk_ranges(&meta).unwrap_err();

        assert!(error.to_string().contains(
            "zarr array metadata missing or invalid: selection rank 3 does not match array rank 2"
        ));
    }

    #[test]
    fn selection_matches_existing_axis_chunk_range_math() {
        let meta = meta(vec![12, 17], vec![5, 4]);
        let bounds = vec![Some(IndexBounds { start: 4, end: 10 }), None];
        let expected = axis_chunk_ranges(&meta, &bounds).unwrap();

        assert_eq!(
            Selection::from_axis_bounds(bounds)
                .chunk_ranges(&meta)
                .unwrap(),
            expected
        );
    }

    #[test]
    fn invalid_axis_bounds_are_rejected() {
        let meta = meta(vec![8], vec![4]);

        for bounds in [
            IndexBounds { start: 5, end: 4 },
            IndexBounds { start: 0, end: 8 },
        ] {
            let error = Selection::from_axis_bounds(vec![Some(bounds)])
                .chunk_ranges(&meta)
                .unwrap_err();
            assert!(error.to_string().contains("selection index bounds"));
        }
    }
}
