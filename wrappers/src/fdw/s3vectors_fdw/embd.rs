use super::conv::document_to_json_value;
use aws_sdk_s3vectors::types::{GetOutputVector, ListOutputVector, QueryOutputVector, VectorData};
use pgrx::{pg_sys::bytea, prelude::*, stringinfo::StringInfo, JsonB};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::ffi::CStr;

#[derive(Debug, Default, PostgresType, Serialize, Deserialize)]
#[inoutfuncs]
pub(super) struct Embd {
    pub key: String,
    pub data: Vec<f32>,
    pub metadata: Option<JsonValue>,
    pub distance: f32,
}

impl InOutFuncs for Embd {
    const NULL_ERROR_MESSAGE: Option<&'static str> = Some("cannot insert NULL to embd column");

    fn input(input: &CStr) -> Self {
        let value: JsonValue = serde_json::from_str(input.to_str().unwrap_or_default())
            .expect("embd input should be a valid JSON string");

        if value.is_array() {
            Self {
                data: serde_json::from_value(value).expect("embd data should be a float32 array"),
                ..Default::default()
            }
        } else {
            let ret: Self =
                serde_json::from_value(value).expect("embd should be in valid JSON format");
            ret
        }
    }

    fn output(&self, buffer: &mut StringInfo) {
        buffer.push_str(&format!("embd:{}", self.data.len()));
    }
}

impl From<&ListOutputVector> for Embd {
    fn from(v: &ListOutputVector) -> Self {
        let data = if let Some(VectorData::Float32(vector_data)) = &v.data {
            vector_data.clone()
        } else {
            Vec::new()
        };
        let metadata = v.metadata.clone().map(|doc| document_to_json_value(&doc));

        Self {
            key: v.key.clone(),
            data,
            metadata,
            distance: 0.0,
        }
    }
}

impl From<&GetOutputVector> for Embd {
    fn from(v: &GetOutputVector) -> Self {
        let data = if let Some(VectorData::Float32(vector_data)) = &v.data {
            vector_data.clone()
        } else {
            Vec::new()
        };
        let metadata = v.metadata.clone().map(|doc| document_to_json_value(&doc));

        Self {
            key: v.key.clone(),
            data,
            metadata,
            distance: 0.0,
        }
    }
}

impl From<&QueryOutputVector> for Embd {
    fn from(v: &QueryOutputVector) -> Self {
        let data = if let Some(VectorData::Float32(vector_data)) = &v.data {
            vector_data.clone()
        } else {
            Vec::new()
        };
        let metadata = v.metadata.clone().map(|doc| document_to_json_value(&doc));

        Self {
            key: v.key.clone(),
            data,
            metadata,
            distance: v.distance.unwrap_or_default(),
        }
    }
}

impl From<*mut bytea> for Embd {
    fn from(v: *mut bytea) -> Self {
        let ret: Self = unsafe { pgrx::datum::cbor_decode(v) };
        ret
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(<==>)]
fn embd_knn(_left: Embd, _right: Embd) -> bool {
    // always return true here, actual calculation will be done in the wrapper
    true
}

#[pg_operator(immutable, parallel_safe)]
#[opname(<==>)]
fn metadata_filter(_left: JsonB, _right: JsonB) -> bool {
    // always return true here, actual calculation will be done in the wrapper
    true
}

#[pg_extern]
fn embd_distance(embd: Embd) -> f32 {
    embd.distance
}
