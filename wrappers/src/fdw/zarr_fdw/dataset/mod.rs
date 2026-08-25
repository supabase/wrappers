//! Generic dataset descriptors and metadata adapters for `zarr_fdw`.
//!
//! Scan execution consumes xarray named dimensions and same-group coordinate
//! metadata through this format-neutral model.

mod discovery;
pub(crate) mod model;

pub(crate) use discovery::{
    named_array_dataset, named_dimensions, ome_rank2_dataset, parse_named_dimensions,
};
pub(crate) use model::{CoordinateSource, Dataset, DimensionRole};
