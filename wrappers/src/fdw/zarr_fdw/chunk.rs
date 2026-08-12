//! Pure chunk-selection math for querying Zarr cubes.
//!
//! The core idea (borrowed from `duckdb_zarr`): translate a predicate on a
//! dimension (time, x, y) into a *chunk index range* using that dimension's
//! coordinate vector, so that only the chunks that can contain matching cells
//! are fetched from the object store. All functions here are pure and unit
//! testable without any object store.

use super::meta::ArrayMeta;

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
    pub fn chunk_range(&self, chunk_len: usize) -> (usize, usize) {
        let first = self.start / chunk_len;
        let last = self.end / chunk_len;
        (first, last)
    }
}

/// Build a chunk key for one chunk, given the full chunk coordinate indices
/// across dims and the array's dimension separator.
///
/// Zarr v2 default is `.` (e.g. `4.0.0`); stores configured with `/` produce
/// `4/0/0`. Zarr v3 uses `c/4/0/0` but that is out of MVP scope (v2 only).
pub fn chunk_key(sep: &str, indices: &[u64]) -> String {
    indices
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(sep)
}

/// Cartesian product of per-axis chunk index ranges.
///
/// `axis_chunk_ranges` is one `(start, end)` inclusive pair per dimension, in
/// array dimension order. Returns the list of chunk index vectors, in
/// row-major (C order) iteration: last axis varies fastest, matching both the
/// byte layout of the unfiltered cube and Zarr chunk naming.
pub fn enumerate_chunks(axis_chunk_ranges: &[(usize, usize)]) -> Vec<Vec<u64>> {
    let mut out = Vec::new();
    let n = axis_chunk_ranges.len();
    if n == 0 {
        return out;
    }
    let mut idx: Vec<u64> = axis_chunk_ranges.iter().map(|&(s, _)| s as u64).collect();
    let count: Vec<usize> = axis_chunk_ranges.iter().map(|&(s, e)| e - s + 1).collect();
    let total: usize = count.iter().product();
    for _ in 0..total {
        out.push(idx.clone());
        // increment in row-major order
        let mut d = n;
        while d > 0 {
            d -= 1;
            idx[d] += 1;
            if idx[d] as usize <= axis_chunk_ranges[d].1 {
                break;
            }
            idx[d] = axis_chunk_ranges[d].0 as u64;
        }
    }
    out
}

/// Compute the per-axis chunk ranges for a cube given per-axis index bounds.
pub fn axis_chunk_ranges(meta: &ArrayMeta, bounds: &[Option<IndexBounds>]) -> Vec<(usize, usize)> {
    let full = meta.chunks_per_axis();
    bounds
        .iter()
        .zip(meta.chunks.iter())
        .enumerate()
        .map(|(axis, (b, chunk_len))| match b {
            Some(b) => b.chunk_range(*chunk_len as usize),
            None => (0, full[axis].saturating_sub(1) as usize),
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
            dimension_separator: ".".to_string(),
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
        assert_eq!(b.chunk_range(3), (0, 1));
        let b2 = IndexBounds { start: 6, end: 8 };
        assert_eq!(b2.chunk_range(3), (2, 2));
    }

    #[test]
    fn test_chunk_key() {
        assert_eq!(chunk_key(".", &[3, 14, 22]), "3.14.22");
        assert_eq!(chunk_key("/", &[3, 14, 22]), "3/14/22");
    }

    #[test]
    fn test_enumerate_chunks_row_major() {
        let ranges = vec![(0, 1), (1, 2), (0, 0)];
        let out = enumerate_chunks(&ranges);
        let keys: Vec<String> = out.iter().map(|i| chunk_key(".", i)).collect();
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
        let ranges = axis_chunk_ranges(&m, &bounds);
        assert_eq!(ranges[0], (1, 2)); // indices 5..11 -> chunks 1..2
        assert_eq!(ranges[1], (0, 9)); // full y axis
        assert_eq!(ranges[2], (0, 9)); // full x axis
    }
}
