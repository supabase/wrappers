//! Generic dataset descriptors and metadata adapters for `zarr_fdw`.
//!
//! Scan execution remains on the proven G0 2D/3D profile. The adapter isolates
//! that compatibility rule so later named-dimension discovery can populate the
//! same model without rewriting the executor again.

mod discovery;
pub(crate) mod model;

pub(crate) use discovery::legacy_array_dataset;
pub(crate) use model::{Dataset, DimensionRole};
