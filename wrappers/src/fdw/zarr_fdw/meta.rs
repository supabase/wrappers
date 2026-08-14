//! Zarr array metadata abstraction.
//!
//! Scope: Zarr **v2** arrays (`.zarray` JSON, chunk keys with a `.` or `/`
//! dimension separator). Zarr v3 (`zarr.json`) is explicitly out of scope.

use super::{ZarrFdwError, ZarrFdwResult};
use serde::Deserialize;

/// Highest supported Zarr format version.
const SUPPORTED_ZARR_FORMAT: u32 = 2;
const MAX_SCAN_RANK: usize = 64;

/// Parsed Zarr v2 `.zarray` metadata.
#[derive(Debug, Clone, Deserialize)]
pub struct ArrayMeta {
    /// Zarr format version (must be 2 for MVP).
    pub zarr_format: u32,

    /// Array shape, one entry per dimension.
    pub shape: Vec<u64>,

    /// Chunk shape, one entry per dimension.
    pub chunks: Vec<u64>,

    /// Numpy-style dtype string, e.g. `<f4`, `|u1`, `<i2`.
    pub dtype: String,

    /// Scalar used for uninitialized chunks. Required by the Zarr v2
    /// metadata schema; `null` means missing-chunk contents are undefined.
    pub fill_value: serde_json::Value,

    /// Compressor id from the `compressor` field (`None` = raw, no compression).
    /// The full JSON object is kept so codec params can be inspected later.
    pub compressor: Option<serde_json::Value>,

    /// Dot (`.`, default) or slash (`/`) separators in chunk key paths.
    #[serde(default = "default_dimension_separator")]
    pub dimension_separator: String,

    /// Byte order for non-byte dtypes: `C` row-major, `F` column-major.
    #[serde(default = "default_order")]
    pub order: char,

    /// Optional filters (transpose, shuffle, etc.). MVP: unsupported.
    pub filters: Option<Vec<serde_json::Value>>,
}

fn default_dimension_separator() -> String {
    ".".to_string()
}

fn default_order() -> char {
    'C'
}

impl ArrayMeta {
    /// Parse `.zarray` JSON content into `ArrayMeta`, then run full scan
    /// validation (Zarr v2, bounded positive rank, C order, no filters).
    pub fn from_bytes(bytes: &[u8]) -> ZarrFdwResult<Self> {
        let meta = Self::parse(bytes)?;
        meta.validate()?;
        Ok(meta)
    }

    /// Parse `.zarray` JSON content into `ArrayMeta` without validating ranks.
    /// Used by coordinate arrays, which are legitimately rank-1.
    pub fn parse(bytes: &[u8]) -> ZarrFdwResult<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn validate(&self) -> ZarrFdwResult<()> {
        if self.zarr_format != SUPPORTED_ZARR_FORMAT {
            return Err(ZarrFdwError::UnsupportedZarrFormat {
                version: self.zarr_format,
            });
        }
        if !(1..=MAX_SCAN_RANK).contains(&self.shape.len()) {
            return Err(ZarrFdwError::UnsupportedRank {
                rank: self.shape.len(),
            });
        }
        self.validate_common()
    }

    /// Validation common to cube arrays and coordinate arrays: Zarr v2, C
    /// order, no filters, matching shape/chunks. Rank is intentionally not
    /// checked here.
    fn validate_common(&self) -> ZarrFdwResult<()> {
        if self.shape.len() != self.chunks.len() {
            return Err(ZarrFdwError::InvalidMetadata(
                "shape and chunks lengths differ".to_string(),
            ));
        }
        if let Some(axis) = self.shape.iter().position(|&extent| extent == 0) {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "shape dimension {axis} must be greater than zero"
            )));
        }
        if let Some(axis) = self.chunks.iter().position(|&extent| extent == 0) {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "chunk dimension {axis} must be greater than zero"
            )));
        }
        // Scan math and allocations use `usize`. Reject metadata that cannot
        // be represented on this PostgreSQL build, and reject a declared
        // chunk whose cell count overflows before it reaches offset/allocation
        // code in the scan path.
        for axis in 0..self.shape.len() {
            self.shape_extent(axis)?;
            self.chunk_extent(axis)?;
        }
        self.chunk_cell_count()?;
        if self
            .filters
            .as_ref()
            .is_some_and(|filters| !filters.is_empty())
        {
            return Err(ZarrFdwError::InvalidMetadata(
                "zarr filters are not supported yet".to_string(),
            ));
        }
        if self.order != 'C' {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "only row-major (C order) arrays are supported, got '{}'",
                self.order
            )));
        }
        if !matches!(self.dimension_separator.as_str(), "." | "/") {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "dimension_separator must be '.' or '/', got '{}'",
                self.dimension_separator
            )));
        }
        Ok(())
    }

    /// Validate a coordinate array (rank-1 is allowed).
    pub fn validate_coordinate(&self) -> ZarrFdwResult<()> {
        if self.zarr_format != SUPPORTED_ZARR_FORMAT {
            return Err(ZarrFdwError::UnsupportedZarrFormat {
                version: self.zarr_format,
            });
        }
        if self.shape.len() != 1 {
            return Err(ZarrFdwError::CoordinateReadError {
                axis: String::new(),
                error: format!("coordinate array must be 1D, got rank {}", self.shape.len()),
            });
        }
        self.validate_common()
    }

    /// Number of chunks along each dimension.
    pub fn chunks_per_axis(&self) -> Vec<u64> {
        self.shape
            .iter()
            .zip(self.chunks.iter())
            .map(|(s, c)| s.div_ceil(*c))
            .collect()
    }

    /// Shape extent converted to the index type used by the scan executor.
    pub fn shape_extent(&self, axis: usize) -> ZarrFdwResult<usize> {
        usize::try_from(self.shape[axis]).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "shape dimension {axis} exceeds this platform's index capacity"
            ))
        })
    }

    /// Chunk extent converted to the index type used by the scan executor.
    pub fn chunk_extent(&self, axis: usize) -> ZarrFdwResult<usize> {
        usize::try_from(self.chunks[axis]).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "chunk dimension {axis} exceeds this platform's index capacity"
            ))
        })
    }

    /// Number of logical cells in a declared (including edge) chunk.
    pub fn chunk_cell_count(&self) -> ZarrFdwResult<usize> {
        self.chunks
            .iter()
            .enumerate()
            .try_fold(1usize, |cells, (axis, &extent)| {
                let extent = usize::try_from(extent).map_err(|_| {
                    ZarrFdwError::InvalidMetadata(format!(
                        "chunk dimension {axis} exceeds this platform's index capacity"
                    ))
                })?;
                cells.checked_mul(extent).ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(
                        "declared chunk cell count exceeds this platform's index capacity"
                            .to_string(),
                    )
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(shape: Vec<u64>, chunks: Vec<u64>) -> ArrayMeta {
        ArrayMeta {
            zarr_format: 2,
            shape,
            chunks,
            dtype: "<f4".to_string(),
            fill_value: serde_json::Value::Null,
            compressor: None,
            dimension_separator: ".".to_string(),
            order: 'C',
            filters: None,
        }
    }

    #[test]
    fn parse_v2_metadata() {
        let json = br#"{
            "zarr_format": 2,
            "shape": [10, 100, 100],
            "chunks": [1, 32, 32],
            "dtype": "<f4",
            "compressor": {"id": "zstd", "level": 1},
            "fill_value": null,
            "order": "C"
        }"#;
        let m = ArrayMeta::from_bytes(json).unwrap();
        assert_eq!(m.shape, vec![10, 100, 100]);
        assert_eq!(m.dimension_separator, ".");
        assert_eq!(m.compressor.as_ref().unwrap()["id"], "zstd");
        assert_eq!(m.chunks_per_axis(), vec![10, 4, 4]);
    }

    #[test]
    fn reject_v3_metadata() {
        // only the format version is checked here, but the struct needs the other
        // required fields to deserialize
        let json = br#"{
            "zarr_format": 3,
            "shape": [10],
            "chunks": [5],
            "dtype": "<f4",
            "fill_value": null,
            "compressor": null
        }"#;
        assert!(matches!(
            ArrayMeta::from_bytes(json),
            Err(ZarrFdwError::UnsupportedZarrFormat { version: 3 })
        ));
    }

    #[test]
    fn accepts_arbitrary_positive_rank_within_limit() {
        for rank in [1, 4, MAX_SCAN_RANK] {
            meta(vec![1; rank], vec![1; rank]).validate().unwrap();
        }
    }

    #[test]
    fn rejects_rank_outside_scan_limit() {
        for rank in [0, MAX_SCAN_RANK + 1] {
            assert!(matches!(
                meta(vec![1; rank], vec![1; rank]).validate(),
                Err(ZarrFdwError::UnsupportedRank { rank: actual }) if actual == rank
            ));
        }
    }

    #[test]
    fn slash_separator() {
        let json = br#"{
            "zarr_format": 2,
            "shape": [10, 100, 100],
            "chunks": [1, 32, 32],
            "dtype": "<f4",
            "fill_value": null,
            "compressor": null,
            "dimension_separator": "/"
        }"#;
        let m = ArrayMeta::from_bytes(json).unwrap();
        assert_eq!(m.dimension_separator, "/");
    }

    #[test]
    fn reject_zero_shape_dimension() {
        assert!(matches!(
            meta(vec![10, 0], vec![5, 5]).validate(),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message == "shape dimension 1 must be greater than zero"
        ));
    }

    #[test]
    fn reject_zero_chunk_dimension() {
        assert!(matches!(
            meta(vec![10, 10], vec![5, 0]).validate(),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message == "chunk dimension 1 must be greater than zero"
        ));
    }

    #[test]
    fn reject_cube_shape_chunk_rank_mismatch() {
        assert!(matches!(
            meta(vec![10, 10], vec![5]).validate(),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message == "shape and chunks lengths differ"
        ));
    }

    #[test]
    fn reject_coordinate_zero_dimensions() {
        assert!(meta(vec![0], vec![5]).validate_coordinate().is_err());
        assert!(meta(vec![10], vec![0]).validate_coordinate().is_err());
    }

    #[test]
    fn reject_coordinate_shape_chunk_rank_mismatch() {
        assert!(matches!(
            meta(vec![10], vec![5, 5]).validate_coordinate(),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message == "shape and chunks lengths differ"
        ));
    }

    #[test]
    fn reject_invalid_dimension_separator() {
        let mut m = meta(vec![10, 10], vec![5, 5]);
        m.dimension_separator = "-".to_string();
        assert!(matches!(
            m.validate(),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message == "dimension_separator must be '.' or '/', got '-'"
        ));
    }

    #[test]
    fn accept_absent_null_and_empty_filters() {
        for filters in ["", r#", "filters": null"#, r#", "filters": []"#] {
            let json = format!(
                r#"{{
                    "zarr_format": 2,
                    "shape": [2, 2],
                    "chunks": [1, 1],
                    "dtype": "<f4",
                    "fill_value": 0,
                    "compressor": null
                    {filters}
                }}"#
            );
            ArrayMeta::from_bytes(json.as_bytes()).unwrap();
        }
    }

    #[test]
    fn reject_non_empty_filters() {
        let json = br#"{
            "zarr_format": 2,
            "shape": [2, 2],
            "chunks": [1, 1],
            "dtype": "<f4",
            "fill_value": 0,
            "compressor": null,
            "filters": [{"id": "delta"}]
        }"#;
        assert!(matches!(
            ArrayMeta::from_bytes(json),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message == "zarr filters are not supported yet"
        ));
    }

    #[test]
    fn fill_value_is_required() {
        let json = br#"{
            "zarr_format": 2,
            "shape": [2, 2],
            "chunks": [1, 1],
            "dtype": "<f4",
            "compressor": null
        }"#;
        assert!(matches!(
            ArrayMeta::from_bytes(json),
            Err(ZarrFdwError::JsonParseError(_))
        ));
    }

    #[test]
    fn reject_chunk_cell_count_overflow() {
        assert!(matches!(
            meta(vec![u64::MAX, u64::MAX], vec![u64::MAX, u64::MAX]).validate(),
            Err(ZarrFdwError::InvalidMetadata(message))
                if message == "declared chunk cell count exceeds this platform's index capacity"
        ));
    }
}
