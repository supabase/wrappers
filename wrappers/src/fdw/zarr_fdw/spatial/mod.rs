//! Spatial metadata, optional PostGIS adapters, and rectilinear-grid math.
//!
//! PostGIS adapters and spatial execution build on these helpers, but this
//! module does not depend on geometry types, SPI, storage, or scan state.

mod catalog;
pub(crate) mod crs;
pub(crate) mod grid;
mod point;
pub(crate) mod postgis;
mod temporal;
mod zonal;
