//! Pure chunk-selection math for querying Zarr cubes.
//!
//! The core idea (borrowed from `duckdb_zarr`): translate a predicate on a
//! dimension (time, x, y) into a *chunk index range* using that dimension's
//! coordinate vector, so that only the chunks that can contain matching cells
//! are fetched from the object store. All functions here are pure and unit
//! testable without any object store.

use super::meta::{ArrayMeta, ChunkKeyEncoding};
use super::{ZarrFdwError, ZarrFdwResult};

/// Inclusive 1-based? No — plain 0-based inclusive index bounds `(start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexBounds {
    pub start: usize,
    pub end: usize, // inclusive
}

impl IndexBounds {
    /// Full bounds for an axis of `len` elements.
    pub fn full(len: usize) -> Self {
        Self {
            start: 0,
            end: len.saturating_sub(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.end < self.start
    }
}

/// Translate a `[lo, hi]` value range into inclusive index bounds over a
/// coordinate vector. The coordinate vector is assumed to be sorted (either
/// ascending or descending); direction is detected from the endpoint values.
///
/// If `lo`/`hi` are `None`, the bound is unbounded on that side.
/// Returns `None` if the range misses the coordinate vector entirely (no
/// overlap), which callers use to skip the axis/chunk entirely.
pub fn index_bounds_from_value_range(
    coords: &[f64],
    lo: Option<f64>,
    hi: Option<f64>,
) -> Option<IndexBounds> {
    if coords.is_empty() {
        return Some(IndexBounds::full(0));
    }
    if let (Some(lo), Some(hi)) = (lo, hi) {
        // quick reject: the [lo, hi] window nowhere overlaps the coord span
        let (cmin, cmax) = if coords[0] <= coords[coords.len() - 1] {
            (coords[0], coords[coords.len() - 1])
        } else {
            (coords[coords.len() - 1], coords[0])
        };
        if hi < cmin || lo > cmax {
            return None;
        }
    }

    let ascending = match (coords.first(), coords.last()) {
        (Some(&first), Some(&last)) => last >= first,
        _ => true,
    };

    let n = coords.len();
    let bounds = if ascending {
        // find first index with coord >= lo (or 0), last index with coord <= hi (or n-1)
        let start = match lo {
            Some(lo) => coords.partition_point(|&c| c < lo),
            None => 0,
        };
        let end = match hi {
            Some(hi) => {
                // first index with coord > hi, then step back
                let mut p = coords.partition_point(|&c| c <= hi);
                p = p.saturating_sub(1);
                p
            }
            None => n - 1,
        };
        IndexBounds { start, end }
    } else {
        // descending: index i holds value descending. value >= lo and <= hi
        // correspond to the first index where value drops below hi, up to the
        // last index where value is still >= lo.
        let start = match hi {
            Some(hi) => coords.partition_point(|&c| c > hi),
            None => 0,
        };
        let end = match lo {
            Some(lo) => {
                let mut p = coords.partition_point(|&c| c >= lo);
                p = p.saturating_sub(1);
                p
            }
            None => n - 1,
        };
        IndexBounds { start, end }
    };

    if bounds.is_empty() {
        return None;
    }
    Some(bounds)
}

impl IndexBounds {
    /// Convert index bounds into an inclusive chunk range for an axis: which
    /// chunk numbers (0-based) cover `[start, end]`.
    pub fn chunk_range(&self, chunk_len: usize) -> ZarrFdwResult<(usize, usize)> {
        if chunk_len == 0 {
            return Err(ZarrFdwError::InvalidMetadata(
                "chunk length must be greater than zero".to_string(),
            ));
        }
        let first = self.start / chunk_len;
        let last = self.end / chunk_len;
        Ok((first, last))
    }
}

/// Build the storage key for one logical chunk coordinate.
pub fn chunk_key(encoding: &ChunkKeyEncoding, indices: &[u64]) -> String {
    let separator = match encoding {
        ChunkKeyEncoding::Default { separator } | ChunkKeyEncoding::V2 { separator } => separator,
    };
    let coordinates = indices
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(&separator.to_string());
    match encoding {
        ChunkKeyEncoding::Default { .. } if coordinates.is_empty() => "c".to_string(),
        ChunkKeyEncoding::Default { .. } => format!("c{separator}{coordinates}"),
        ChunkKeyEncoding::V2 { .. } if coordinates.is_empty() => "0".to_string(),
        ChunkKeyEncoding::V2 { .. } => coordinates,
    }
}

/// Lazy Cartesian product of per-axis chunk index ranges.
///
/// The cursor stores only rank-sized state. `next_into` reuses a caller-owned
/// output vector and visits chunks in row-major (C) order, with the last axis
/// varying fastest.
#[derive(Debug, Default)]
pub struct ChunkIndexCursor {
    starts: Vec<u64>,
    ends: Vec<u64>,
    next: Vec<u64>,
    has_next: bool,
}

impl ChunkIndexCursor {
    pub fn new(axis_chunk_ranges: &[(usize, usize)]) -> ZarrFdwResult<Self> {
        if axis_chunk_ranges.is_empty() || axis_chunk_ranges.iter().any(|&(start, end)| start > end)
        {
            return Ok(Self::default());
        }

        let starts = axis_chunk_ranges
            .iter()
            .map(|&(start, _)| {
                u64::try_from(start).map_err(|_| {
                    ZarrFdwError::InvalidMetadata(
                        "chunk index exceeds the Zarr v2 u64 index capacity".to_string(),
                    )
                })
            })
            .collect::<ZarrFdwResult<Vec<_>>>()?;
        let ends = axis_chunk_ranges
            .iter()
            .map(|&(_, end)| {
                u64::try_from(end).map_err(|_| {
                    ZarrFdwError::InvalidMetadata(
                        "chunk index exceeds the Zarr v2 u64 index capacity".to_string(),
                    )
                })
            })
            .collect::<ZarrFdwResult<Vec<_>>>()?;
        let next = starts.clone();

        Ok(Self {
            starts,
            ends,
            next,
            has_next: true,
        })
    }

    /// Copy the next chunk index into `destination`, reusing its allocation.
    /// Returns `false` after the cursor is exhausted and clears `destination`
    /// so callers cannot accidentally reuse a stale chunk index.
    pub fn next_into(&mut self, destination: &mut Vec<u64>) -> bool {
        if !self.has_next {
            destination.clear();
            return false;
        }

        destination.clone_from(&self.next);
        for axis in (0..self.next.len()).rev() {
            if self.next[axis] < self.ends[axis] {
                self.next[axis] += 1;
                return true;
            }
            self.next[axis] = self.starts[axis];
        }

        self.has_next = false;
        true
    }

    pub fn reset(&mut self) {
        self.next.clone_from(&self.starts);
        self.has_next = !self.starts.is_empty();
    }
}

/// Compute the per-axis chunk ranges for a cube given per-axis index bounds.
pub fn axis_chunk_ranges(
    meta: &ArrayMeta,
    bounds: &[Option<IndexBounds>],
) -> ZarrFdwResult<Vec<(usize, usize)>> {
    let full = meta.chunks_per_axis();
    bounds
        .iter()
        .enumerate()
        .map(|(axis, b)| match b {
            Some(b) => b.chunk_range(meta.chunk_extent(axis)?),
            None => {
                let last = full[axis].checked_sub(1).ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(format!("axis {axis} has no addressable chunks"))
                })?;
                Ok((
                    0,
                    usize::try_from(last).map_err(|_| {
                        ZarrFdwError::InvalidMetadata(format!(
                            "chunk count for axis {axis} exceeds this platform's index capacity"
                        ))
                    })?,
                ))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::meta::ArrayMeta;
    use super::*;

    fn meta(shape: Vec<u64>, chunks: Vec<u64>) -> ArrayMeta {
        ArrayMeta {
            zarr_format: 2,
            shape,
            chunks,
            dtype: "<f4".to_string(),
            compressor: None,
            codec_pipeline: super::super::codec::CodecPipeline::raw_v2(),
            fill_value: serde_json::Value::Null,
            chunk_key_encoding: ChunkKeyEncoding::V2 { separator: '.' },
            order: 'C',
            filters: None,
        }
    }

    fn coords_asc() -> Vec<f64> {
        (0..100).map(|i| i as f64 * 10.0).collect() // 0,10,...990
    }

    #[test]
    fn test_index_bounds_ascending() {
        let c = coords_asc();
        let b = index_bounds_from_value_range(&c, Some(20.0), Some(50.0)).unwrap();
        assert_eq!(b, IndexBounds { start: 2, end: 5 });
    }

    #[test]
    fn test_index_bounds_unbounded_hi() {
        let c = coords_asc();
        let b = index_bounds_from_value_range(&c, Some(990.0), None).unwrap();
        assert_eq!(b, IndexBounds { start: 99, end: 99 });
    }

    #[test]
    fn test_index_bounds_no_overlap() {
        let c = coords_asc();
        assert!(index_bounds_from_value_range(&c, Some(5000.0), None).is_none());
    }

    #[test]
    fn test_index_bounds_descending() {
        let c: Vec<f64> = (0..100).rev().map(|i| i as f64 * 10.0).collect(); // 990..0
        let b = index_bounds_from_value_range(&c, Some(20.0), Some(50.0)).unwrap();
        // values 20..50 are at original indices 94..97
        assert_eq!(b, IndexBounds { start: 94, end: 97 });
    }

    #[test]
    fn test_chunk_range() {
        let b = IndexBounds { start: 2, end: 5 };
        assert_eq!(b.chunk_range(3).unwrap(), (0, 1));
        let b2 = IndexBounds { start: 6, end: 8 };
        assert_eq!(b2.chunk_range(3).unwrap(), (2, 2));
        assert!(b2.chunk_range(0).is_err());
    }

    #[test]
    fn test_chunk_key() {
        assert_eq!(
            chunk_key(&ChunkKeyEncoding::V2 { separator: '.' }, &[3, 14, 22]),
            "3.14.22"
        );
        assert_eq!(
            chunk_key(&ChunkKeyEncoding::V2 { separator: '/' }, &[3, 14, 22]),
            "3/14/22"
        );
        assert_eq!(
            chunk_key(&ChunkKeyEncoding::Default { separator: '/' }, &[3, 14, 22]),
            "c/3/14/22"
        );
        assert_eq!(
            chunk_key(&ChunkKeyEncoding::Default { separator: '.' }, &[3, 14, 22]),
            "c.3.14.22"
        );
        assert_eq!(
            chunk_key(&ChunkKeyEncoding::Default { separator: '/' }, &[]),
            "c"
        );
        assert_eq!(
            chunk_key(&ChunkKeyEncoding::V2 { separator: '.' }, &[]),
            "0"
        );
    }

    fn collect_chunks(cursor: &mut ChunkIndexCursor) -> Vec<Vec<u64>> {
        let mut chunks = Vec::new();
        let mut current = Vec::new();
        while cursor.next_into(&mut current) {
            chunks.push(current.clone());
        }
        chunks
    }

    #[test]
    fn chunk_cursor_is_row_major() {
        let ranges = vec![(0, 1), (1, 2), (0, 0)];
        let mut cursor = ChunkIndexCursor::new(&ranges).unwrap();
        let out = collect_chunks(&mut cursor);
        let keys: Vec<String> = out
            .iter()
            .map(|i| chunk_key(&ChunkKeyEncoding::V2 { separator: '.' }, i))
            .collect();
        assert_eq!(keys, vec!["0.1.0", "0.2.0", "1.1.0", "1.2.0"]);
    }

    #[test]
    fn test_axis_chunk_ranges_from_bounds() {
        let m = meta(vec![48, 100, 100], vec![4, 10, 10]);
        let bounds = vec![
            Some(IndexBounds { start: 5, end: 11 }),
            None,
            Some(IndexBounds { start: 0, end: 99 }),
        ];
        let ranges = axis_chunk_ranges(&m, &bounds).unwrap();
        assert_eq!(ranges[0], (1, 2)); // indices 5..11 -> chunks 1..2
        assert_eq!(ranges[1], (0, 9)); // full y axis
        assert_eq!(ranges[2], (0, 9)); // full x axis
    }

    #[test]
    fn empty_and_inverted_ranges_are_exhausted() {
        for ranges in [vec![], vec![(2, 1)], vec![(0, 1), (3, 2)]] {
            let mut cursor = ChunkIndexCursor::new(&ranges).unwrap();
            let mut current = vec![99];
            assert!(!cursor.next_into(&mut current));
            assert!(current.is_empty());
            cursor.reset();
            assert!(!cursor.next_into(&mut current));
        }
    }

    #[test]
    fn cursor_reset_replays_partial_and_exhausted_ranges() {
        let mut cursor = ChunkIndexCursor::new(&[(0, 1), (4, 5)]).unwrap();
        let mut current = Vec::new();
        assert!(cursor.next_into(&mut current));
        assert_eq!(current, vec![0, 4]);
        assert!(cursor.next_into(&mut current));
        assert_eq!(current, vec![0, 5]);

        cursor.reset();
        assert_eq!(
            collect_chunks(&mut cursor),
            vec![vec![0, 4], vec![0, 5], vec![1, 4], vec![1, 5]]
        );

        cursor.reset();
        assert_eq!(collect_chunks(&mut cursor).len(), 4);
    }

    #[test]
    fn cursor_accepts_more_than_one_million_chunks_without_enumerating_them() {
        let mut cursor = ChunkIndexCursor::new(&[(0, 1_000_000)]).unwrap();
        let mut current = Vec::new();
        assert!(cursor.next_into(&mut current));
        assert_eq!(current, vec![0]);
        assert!(cursor.next_into(&mut current));
        assert_eq!(current, vec![1]);
    }

    #[test]
    fn cursor_supports_rank_64_with_rank_sized_state() {
        let ranges = vec![(0, 0); 64];
        let mut cursor = ChunkIndexCursor::new(&ranges).unwrap();
        let mut current = Vec::new();
        assert!(cursor.next_into(&mut current));
        assert_eq!(current, vec![0; 64]);
        assert!(!cursor.next_into(&mut current));
        assert!(current.is_empty());

        cursor.reset();
        assert!(cursor.next_into(&mut current));
        assert_eq!(current, vec![0; 64]);
    }

    #[test]
    fn cursor_exhausts_at_the_platform_maximum_without_overflow() {
        let mut cursor = ChunkIndexCursor::new(&[(usize::MAX, usize::MAX)]).unwrap();
        let mut current = Vec::new();
        assert!(cursor.next_into(&mut current));
        assert_eq!(current, vec![u64::try_from(usize::MAX).unwrap()]);
        assert!(!cursor.next_into(&mut current));
        assert!(current.is_empty());
    }
}
