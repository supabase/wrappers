use aws_sdk_cognitoidentityprovider::types::UserType;
use chrono::DateTime;
use serde::Deserialize;
use serde_json::Value;
use supabase_wrappers::prelude::Cell;
use supabase_wrappers::prelude::Column;
use supabase_wrappers::prelude::Row;

use aws_sdk_cognitoidentityprovider::types::AttributeType;
use serde_json::json;

#[derive(Debug, Deserialize, PartialEq)]
pub struct ResultPayload {
    pub(crate) users: Vec<CognitoUser>,
    pub(crate) next_page_offset: Option<u64>,
}

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

#[derive(Debug)]
pub enum IntoRowError {
    UnsupportedColumnType(String),
}

pub trait IntoRow {
    fn into_row(self, columns: &[Column]) -> Result<Row, IntoRowError>;
}

fn serialize_attributes(attributes: &Vec<AttributeType>) -> Value {
    let mut attrs = vec![];

    for attr in attributes {
        // Convert each AttributeType to a serde_json::Value
        let attr_json = json!({ attr.name.clone(): attr.value });
        attrs.push(attr_json);
    }

    json!(attrs)
}

impl IntoRow for UserType {
    fn into_row(self, columns: &[Column]) -> Result<Row, IntoRowError> {
        let mut row = Row::new();

        for column in columns {
            match column.name.as_str() {
                "username" => {
                    if let Some(ref username) = self.username {
                        row.push("username", Some(Cell::String(username.to_string())));
                    }
                }
                "attributes" => {
                    if let Some(ref attributes) = self.attributes {
                        let serialized_attributes = serialize_attributes(attributes);

                        let attributes_json_b = pgrx::JsonB(serialized_attributes);
                        row.push("attributes", Some(Cell::Json(attributes_json_b)));
                    }
                }
                "created_at" => {
                    if let Some(created_at) = self.extract_attribute_value("created_at") {
                        let parsed_date = DateTime::parse_from_rfc3339(&created_at)
                            .expect("Failed to parse date");
                        row.push(
                            "created_at",
                            Some(Cell::Timestamp(parsed_date.timestamp().into())),
                        );
                    }
                }
                "email" => {
                    if let Some(email) = self.extract_attribute_value("email") {
                        row.push("email", Some(Cell::String(email)));
                    }
                }
                "status" => {
                    if let Some(status) = self.extract_attribute_value("status") {
                        row.push("status", Some(Cell::String(status)));
                    }
                }
                _ => {
                    return Err(IntoRowError::UnsupportedColumnType(column.name.clone()));
                }
            }
        }

        Ok(row)
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
