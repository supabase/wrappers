use aws_smithy_types::Document;
use serde_json::Value as JsonValue;

pub(super) fn json_value_to_document(value: &JsonValue) -> Document {
    match value {
        JsonValue::Null => Document::Null,
        JsonValue::Bool(b) => Document::from(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Document::from(i)
            } else if let Some(f) = n.as_f64() {
                Document::from(f)
            } else {
                Document::from(n.to_string())
            }
        }
        JsonValue::String(s) => Document::from(s.as_str()),
        JsonValue::Array(arr) => {
            let docs: Vec<Document> = arr.iter().map(json_value_to_document).collect();
            Document::from(docs)
        }
        JsonValue::Object(obj) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_value_to_document(v));
            }
            Document::from(map)
        }
    }
}

pub(super) fn document_to_json_value(doc: &Document) -> JsonValue {
    match doc {
        Document::Null => JsonValue::Null,
        Document::Bool(b) => JsonValue::Bool(*b),
        Document::Number(n) => match n {
            aws_smithy_types::Number::PosInt(i) => JsonValue::Number(serde_json::Number::from(*i)),
            aws_smithy_types::Number::NegInt(i) => JsonValue::Number(serde_json::Number::from(*i)),
            aws_smithy_types::Number::Float(f) => JsonValue::Number(
                serde_json::Number::from_f64(*f).unwrap_or_else(|| serde_json::Number::from(0)),
            ),
        },
        Document::String(s) => JsonValue::String(s.clone()),
        Document::Array(arr) => {
            let values: Vec<JsonValue> = arr.iter().map(document_to_json_value).collect();
            JsonValue::Array(values)
        }
        Document::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                map.insert(k.clone(), document_to_json_value(v));
            }
            JsonValue::Object(map)
        }
    }
}
