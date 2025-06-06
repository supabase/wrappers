#![allow(clippy::result_large_err)]
use aws_sdk_cognitoidentityprovider::primitives::DateTime;
use aws_sdk_cognitoidentityprovider::types::{AttributeType, UserType};
use serde_json::{json, Value};
use supabase_wrappers::prelude::{Cell, Column, Row};

use super::super::CognitoFdwError;

pub(in super::super) trait IntoRow {
    fn into_row(self, columns: &[Column]) -> Result<Row, CognitoFdwError>;
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

fn convert_to_timestamp(dt: DateTime) -> Cell {
    let millis = dt.to_millis().expect("timestamp should be valid");
    // convert Unix epoch to Postgres epoch
    let ts = pgrx::prelude::Timestamp::try_from(millis * 1000 - 946_684_800_000_000)
        .expect("timestamp should be converted Postgres epoch");
    Cell::Timestamp(ts)
}

impl IntoRow for UserType {
    fn into_row(self, columns: &[Column]) -> Result<Row, CognitoFdwError> {
        let mut row = Row::new();

        for column in columns {
            match column.name.as_str() {
                "username" => {
                    row.push("username", self.username.clone().map(Cell::String));
                }
                "attributes" => {
                    if let Some(ref attributes) = self.attributes {
                        let serialized_attributes = serialize_attributes(attributes);
                        let attributes_json_b = pgrx::JsonB(serialized_attributes);
                        row.push("attributes", Some(Cell::Json(attributes_json_b)));
                    }
                }
                "created_at" => {
                    row.push(
                        "created_at",
                        self.user_create_date.map(convert_to_timestamp),
                    );
                }
                "updated_at" => {
                    row.push(
                        "updated_at",
                        self.user_last_modified_date.map(convert_to_timestamp),
                    );
                }
                "email" => {
                    let value = self.extract_attribute_value("email").map(Cell::String);
                    row.push("email", value);
                }
                "enabled" => {
                    row.push("enabled", Some(Cell::Bool(self.enabled)));
                }
                "status" => {
                    row.push(
                        "status",
                        self.user_status
                            .clone()
                            .map(|s| Cell::String(s.as_str().to_owned())),
                    );
                }
                _ => {
                    return Err(CognitoFdwError::UnsupportedColumn(column.name.clone()));
                }
            }
        }

        Ok(row)
    }
}

pub(in super::super) trait UserTypeExt {
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
