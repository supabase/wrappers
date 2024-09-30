use pgrx::{
    datum::datetime_support::to_timestamp,
    prelude::{Date, Timestamp, TimestampWithTimeZone},
    AnyNumeric, JsonB,
};
use wasmtime::component::bindgen;
use wasmtime::Error as WasmError;

use crate::stats::Metric as HostMetric;
use supabase_wrappers::prelude::{Cell as HostCell, Param as HostParam, Value as HostValue};

bindgen!(in "../wasm-wrappers/wit");

use super::bindings::supabase::wrappers::{
    stats::Metric as GuestMetric,
    types::{Cell as GuestCell, Param as GuestParam, Value as GuestValue},
};

impl TryFrom<GuestCell> for HostCell {
    type Error = WasmError;

    fn try_from(value: GuestCell) -> Result<Self, Self::Error> {
        match value {
            GuestCell::Bool(v) => Ok(Self::Bool(v)),
            GuestCell::I8(v) => Ok(Self::I8(v)),
            GuestCell::I16(v) => Ok(Self::I16(v)),
            GuestCell::F32(v) => Ok(Self::F32(v)),
            GuestCell::I32(v) => Ok(Self::I32(v)),
            GuestCell::F64(v) => Ok(Self::F64(v)),
            GuestCell::I64(v) => Ok(Self::I64(v)),
            GuestCell::Numeric(v) => {
                let ret = AnyNumeric::try_from(v).map(Self::Numeric)?;
                Ok(ret)
            }
            GuestCell::String(v) => Ok(Self::String(v.clone())),
            GuestCell::Date(v) => {
                let ts = to_timestamp(v as f64);
                Ok(Self::Date(Date::from(ts)))
            }
            // convert 'pg epoch' (2000-01-01 00:00:00) to unix epoch
            GuestCell::Timestamp(v) => Timestamp::try_from(v - 946_684_800_000_000)
                .map(Self::Timestamp)
                .map_err(Self::Error::msg),
            GuestCell::Timestamptz(v) => TimestampWithTimeZone::try_from(v - 946_684_800_000_000)
                .map(Self::Timestamptz)
                .map_err(Self::Error::msg),
            GuestCell::Json(v) => {
                let ret = serde_json::from_str(&v).map(|j| Self::Json(JsonB(j)))?;
                Ok(ret)
            }
        }
    }
}

impl From<&HostCell> for GuestCell {
    fn from(value: &HostCell) -> Self {
        match value {
            HostCell::Bool(v) => Self::Bool(*v),
            HostCell::I8(v) => Self::I8(*v),
            HostCell::I16(v) => Self::I16(*v),
            HostCell::F32(v) => Self::F32(*v),
            HostCell::I32(v) => Self::I32(*v),
            HostCell::F64(v) => Self::F64(*v),
            HostCell::I64(v) => Self::I64(*v),
            HostCell::Numeric(v) => Self::Numeric(v.clone().try_into().unwrap()),
            HostCell::String(v) => Self::String(v.clone()),
            HostCell::Date(v) => {
                // convert 'pg epoch' (2000-01-01 00:00:00) to unix epoch
                let ts = Timestamp::from(*v);
                Self::Date(ts.into_inner() / 1_000_000 + 946_684_800)
            }
            HostCell::Timestamp(v) => {
                // convert 'pg epoch' (2000-01-01 00:00:00) in macroseconds to unix epoch
                Self::Timestamp(v.into_inner() + 946_684_800_000_000)
            }
            HostCell::Timestamptz(v) => {
                // convert 'pg epoch' (2000-01-01 00:00:00) in macroseconds to unix epoch
                Self::Timestamptz(v.into_inner() + 946_684_800_000_000)
            }
            HostCell::Json(v) => Self::Json(v.0.to_string()),
            _ => todo!("Add array type support for Wasm FDW"),
        }
    }
}

impl From<HostValue> for GuestValue {
    fn from(value: HostValue) -> Self {
        match value {
            HostValue::Cell(c) => Self::Cell(GuestCell::from(&c)),
            HostValue::Array(a) => {
                let a: Vec<GuestCell> = a.iter().map(GuestCell::from).collect();
                Self::Array(a)
            }
        }
    }
}

impl From<HostParam> for GuestParam {
    fn from(value: HostParam) -> Self {
        Self {
            id: value.id as u32,
            type_oid: value.type_oid.as_u32(),
        }
    }
}

impl From<GuestMetric> for HostMetric {
    fn from(value: GuestMetric) -> Self {
        match value {
            GuestMetric::CreateTimes => HostMetric::CreateTimes,
            GuestMetric::RowsIn => HostMetric::RowsIn,
            GuestMetric::RowsOut => HostMetric::RowsOut,
            GuestMetric::BytesIn => HostMetric::BytesIn,
            GuestMetric::BytesOut => HostMetric::BytesOut,
        }
    }
}
