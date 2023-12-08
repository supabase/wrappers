use crate::fdw::auth0_fdw::auth0_client::row::UserResponseError::ApiError;
use crate::fdw::auth0_fdw::auth0_client::row::UserResponseError::MissingResult;
use pgrx::JsonB;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use supabase_wrappers::prelude::Cell;
use supabase_wrappers::prelude::Column;
use supabase_wrappers::prelude::Row;
use thiserror::Error;

#[derive(Debug, Serialize, PartialEq)]
pub(crate) struct UserRequest {
    limit: Option<u64>,
    offset: Option<u64>,
}

// pub(crate) struct UserRequestBuilder {
//     request: UserRequest,
// }

// impl UserRequestBuilder {
//     pub(crate) fn new() -> Self {
//         Self {
//             request: UserRequest {
//                 limit: None,
//                 offset: None,
//             },
//         }
//     }

//     pub(crate) fn limit(mut self, limit: Option<u64>) -> Self {
//         self.request.limit = limit;
//         self
//     }

//     pub(crate) fn offset(mut self, offset: Option<u64>) -> Self {
//         self.request.offset = offset;
//         self
//     }

//     pub(crate) fn build(self) -> UserRequest {
//         self.request
//     }
// }

#[derive(Debug, Deserialize, PartialEq)]
pub struct ResultPayload {
    pub(crate) users: Vec<Auth0User>,
    pub(crate) next_page_offset: Option<u64>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct Success {
    status: String,
    result: ResultPayload,
    time: f64,
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct UserResponse {
    error: Option<String>,
    status: Option<String>,
    result: Option<ResultPayload>,
    time: Option<f64>,
}

#[derive(Error, Debug)]
pub(crate) enum UserResponseError {
    #[error("api response error: {0}")]
    ApiError(String),

    #[error("{0}")]
    MissingResult(String),
}

#[derive(Debug)]
pub struct Auth0Fields(HashMap<String, Value>);

#[derive(Debug, Deserialize, PartialEq)]
pub struct Auth0User {
    pub created_at: String,
    pub email: String,
    pub email_verified: bool,
    pub identities: Option<serde_json::Value>,
}

impl UserResponse {
    pub(crate) fn get_user_result(self) -> Result<ResultPayload, UserResponseError> {
        if let Some(e) = self.error {
            return Err(ApiError(e));
        }

        let Some(result) = self.result else {
            return Err(MissingResult(
                "`result` key missing in response".to_string(),
            ));
        };

        Ok(result)
    }
}

impl Auth0User {
    pub(crate) fn into_row(mut self, columns: &[Column]) -> Row {
        let mut row = Row::new();
        for tgt_col in columns {
            if tgt_col.name == "created_at" {
                let cell_value = Some(Cell::String(self.created_at.clone()));
                //    None => None, // Or use a default value or handle the error
                // };
                row.push("created_at", cell_value);
            } else if tgt_col.name == "email" {
                row.push("email", Some(Cell::String(self.email.clone())))
            } else if tgt_col.name == "email_verified" {
                row.push("email_verified", Some(Cell::Bool(self.email_verified)))
            } else if tgt_col.name == "identities" {
                let attrs = self
                    .identities
                    .take()
                    .expect("Column `identities` missing in response");

                row.push("identities", Some(Cell::Json(JsonB(attrs))))
            }
        }

        row
    }
}