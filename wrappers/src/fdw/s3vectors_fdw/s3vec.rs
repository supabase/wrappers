use super::conv::document_to_json_value;
use super::S3VectorsFdwError;
use aws_sdk_s3vectors::types::{GetOutputVector, ListOutputVector, QueryOutputVector, VectorData};
use pgrx::{pg_sys::bytea, prelude::*, stringinfo::StringInfo, JsonB};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::ffi::CStr;

#[derive(Debug, Default, PostgresType, Serialize, Deserialize)]
#[inoutfuncs]
pub(super) struct S3Vec {
    pub key: String,
    pub data: Vec<f32>,
    pub metadata: Option<JsonValue>,
    pub distance: f32,
}

impl InOutFuncs for S3Vec {
    const NULL_ERROR_MESSAGE: Option<&'static str> = Some("cannot insert NULL to s3vec column");

    fn input(input: &CStr) -> Self {
        let value: JsonValue = serde_json::from_str(input.to_str().unwrap_or_default())
            .expect("s3vec input should be a valid JSON string");

        if value.is_array() {
            Self {
                data: serde_json::from_value(value).expect("s3vec data should be a float32 array"),
                ..Default::default()
            }
        } else {
            let ret: Self =
                serde_json::from_value(value).expect("s3vec should be in valid JSON format");
            ret
        }
    }

    fn output(&self, buffer: &mut StringInfo) {
        buffer.push_str(&format!("s3vec:{}", self.data.len()));
    }
}

impl From<&ListOutputVector> for S3Vec {
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

impl From<&GetOutputVector> for S3Vec {
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

impl From<&QueryOutputVector> for S3Vec {
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

impl TryFrom<*mut bytea> for S3Vec {
    type Error = S3VectorsFdwError;

    fn try_from(v: *mut bytea) -> Result<Self, Self::Error> {
        if v.is_null() {
            return Err(S3VectorsFdwError::InvalidS3Vec(
                "input bytea pointer is null".to_string(),
            ));
        }
        let ret: Self = unsafe { pgrx::datum::cbor_decode(v) };
        Ok(ret)
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(<==>)]
fn s3vec_knn(_left: S3Vec, _right: S3Vec) -> bool {
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
fn s3vec_distance(s3vec: S3Vec) -> f32 {
    s3vec.distance
}
