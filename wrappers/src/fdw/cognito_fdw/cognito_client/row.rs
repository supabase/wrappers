use aws_sdk_cognitoidentityprovider::types::UserType;
use pgrx::notice;
use pgrx::JsonB;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use supabase_wrappers::prelude::Cell;
use supabase_wrappers::prelude::Column;
use supabase_wrappers::prelude::Row;

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
    fn into_row(self, columns: &[Column]) -> Row {
        let mut row = Row::new(); // Assuming Row has a constructor `new`

        for column in columns {
            match column.name.as_str() {
                "username" => {
                    if let Some(ref username) = self.username {
                        row.push("username", Some(Cell::String(username.to_string())));
                    }
                }
                "created_at" => {
                    if let Some(created_at) = self.extract_attribute_value("created_at") {
                        row.push("created_at", Some(Cell::String(created_at)));
                    }
                }
                "email" => {
                    if let Some(email) = self.extract_attribute_value("email") {
                        row.push("email", Some(Cell::String(email)));
                    }
                }
                // "email_verified" => {
                //     if let Some(email_verified) = self.extract_attribute_value("email_verified") {
                //         row.push("email_verified", Some(Cell::Bool(email_verified.parse().unwrap_or(false))));
                //     } else {

                //     }
                // },
                "status" => {
                    if let Some(status) = self.extract_attribute_value("status") {
                        row.push("status", Some(Cell::String(status)));
                    }
                }
                // Add additional cases as needed for other fields
                _ => (), // Ignore unknown columns or handle them appropriately
            }
        }
        notice!("this is row {:?}", row);

        row
    }
}

pub trait UserTypeExt {
    fn extract_attribute_value(&self, attr_name: &str) -> Option<String>;
}

impl UserTypeExt for UserType {
    fn extract_attribute_value(&self, attr_name: &str) -> Option<String> {
        self.attributes
            .iter()
            .flat_map(|vec| vec.iter()) // Iterate over each AttributeType in the Vec<AttributeType>
            .find(|attr| attr.name == attr_name)
            .and_then(|attr| attr.value.clone())
    }
}
