//! Zarr array metadata abstraction.
//!
//! MVP scope: Zarr **v2** arrays (`.zarray` JSON, chunk keys with a `.` or `/`
//! dimension separator). Zarr v3 (`zarr.json`) is explicitly out of scope for
//! the MVP — the enum below reserves the wrapper so v3 support slots in later.

use super::{ZarrFdwError, ZarrFdwResult};
use serde::Deserialize;

/// Highest supported Zarr format version.
const SUPPORTED_ZARR_FORMAT: u32 = 2;

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
    /// Parse `.zarray` JSON content into `ArrayMeta`, then run full validation
    /// (Zarr v2, rank 2D/3D cube array, C order, no filters).
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
        if self.shape.len() != 2 && self.shape.len() != 3 {
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
        if self.filters.is_some() {
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
            "compressor": null
        }"#;
        assert!(matches!(
            ArrayMeta::from_bytes(json),
            Err(ZarrFdwError::UnsupportedZarrFormat { version: 3 })
        ));
    }

    #[test]
    fn reject_rank() {
        let json = br#"{
            "zarr_format": 2,
            "shape": [10],
            "chunks": [5],
            "dtype": "<f4",
            "compressor": null
        }"#;
        assert!(matches!(
            ArrayMeta::from_bytes(json),
            Err(ZarrFdwError::UnsupportedRank { rank: 1 })
        ));
    }

    #[test]
    fn slash_separator() {
        let json = br#"{
            "zarr_format": 2,
            "shape": [10, 100, 100],
            "chunks": [1, 32, 32],
            "dtype": "<f4",
            "compressor": null,
            "dimension_separator": "/"
        }"#;
        let m = ArrayMeta::from_bytes(json).unwrap();
        assert_eq!(m.dimension_separator, "/");
    }
}
