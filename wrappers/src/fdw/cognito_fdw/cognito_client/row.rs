use aws_sdk_cognitoidentityprovider::types::UserType;
use pgrx::JsonB;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use supabase_wrappers::prelude::Cell;
use supabase_wrappers::prelude::Column;
use supabase_wrappers::prelude::Row;

#[derive(Debug, Serialize, PartialEq)]
pub(crate) struct UserRequest {
    limit: Option<u64>,
    offset: Option<u64>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ResultPayload {
    pub(crate) users: Vec<CognitoUser>,
    pub(crate) next_page_offset: Option<u64>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct Success {
    status: String,
    result: ResultPayload,
    time: f64,
}

#[derive(Debug)]
pub struct CognitoFields(HashMap<String, Value>);

#[derive(Debug, Deserialize, PartialEq)]
pub struct CognitoUser {
    pub created_at: String,
    pub email: String,
    pub email_verified: bool,
    pub identities: Option<serde_json::Value>,
    // Additional fields from UserType
    pub username: String,
    pub status: Option<String>,
}

pub trait IntoRow {
    fn into_row(self, columns: &[Column]) -> Row;
}

impl IntoRow for UserType {
    fn into_row(mut self, columns: &[Column]) -> Row {
        let mut row = Row::new(); // Assuming Row has a constructor `new`
        for column in columns {
            match column.name.as_str() {
                "username" => {
                    // Similar to above, replace with actual logic for `email`
                    let cell_value = self.username.clone().map(Cell::String);
                    row.push("username", cell_value);
                }
                // Add cases for other fields of UserType
                _ => (), // Ignore unknown columns or handle them appropriately
            }
        }

        row
    }
}
