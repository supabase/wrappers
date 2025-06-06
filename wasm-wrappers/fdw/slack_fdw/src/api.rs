use std::collections::HashMap;

#[cfg(test)]
pub fn apply_conditions_test(
    endpoint: &str,
    conditions: &[(String, String, String)], // (column, operator, value)
) -> HashMap<String, String> {
    let mut params = HashMap::new();

    for (column, operator, value) in conditions {
        match endpoint {
            "conversations.history" => {
                if column == "channel_id" && operator == "eq" {
                    params.insert("channel".to_string(), value.clone());
                } else if column == "oldest" && operator == "ge" {
                    params.insert("oldest".to_string(), value.clone());
                } else if column == "latest" && operator == "le" {
                    params.insert("latest".to_string(), value.clone());
                }
            }
            "users.list" => {
                // Users API doesn't support many filters
            }
            "conversations.list" => {
                if column == "types" && operator == "eq" {
                    params.insert("types".to_string(), value.clone());
                }
            }
            "files.list" => {
                if column == "channel_id" && operator == "eq" {
                    params.insert("channel".to_string(), value.clone());
                } else if column == "user_id" && operator == "eq" {
                    params.insert("user".to_string(), value.clone());
                } else if column == "ts_from" && operator == "ge" {
                    params.insert("ts_from".to_string(), value.clone());
                } else if column == "ts_to" && operator == "le" {
                    params.insert("ts_to".to_string(), value.clone());
                }
            }
            _ => {}
        }
    }

    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_conditions_conversations_history() {
        let conditions = vec![
            (
                "channel_id".to_string(),
                "eq".to_string(),
                "C12345".to_string(),
            ),
            (
                "oldest".to_string(),
                "ge".to_string(),
                "1234567890.123456".to_string(),
            ),
        ];

        let params = apply_conditions_test("conversations.history", &conditions);

        assert_eq!(params.get("channel"), Some(&"C12345".to_string()));
        assert_eq!(params.get("oldest"), Some(&"1234567890.123456".to_string()));
    }

    #[test]
    fn test_apply_conditions_files_list() {
        let conditions = vec![
            (
                "channel_id".to_string(),
                "eq".to_string(),
                "C12345".to_string(),
            ),
            (
                "user_id".to_string(),
                "eq".to_string(),
                "U12345".to_string(),
            ),
            (
                "ts_from".to_string(),
                "ge".to_string(),
                "1234567890.123456".to_string(),
            ),
        ];

        let params = apply_conditions_test("files.list", &conditions);

        assert_eq!(params.get("channel"), Some(&"C12345".to_string()));
        assert_eq!(params.get("user"), Some(&"U12345".to_string()));
        assert_eq!(
            params.get("ts_from"),
            Some(&"1234567890.123456".to_string())
        );
    }

    #[test]
    fn test_apply_conditions_conversations_list() {
        let conditions = vec![(
            "types".to_string(),
            "eq".to_string(),
            "public_channel".to_string(),
        )];

        let params = apply_conditions_test("conversations.list", &conditions);

        assert_eq!(params.get("types"), Some(&"public_channel".to_string()));
    }

    #[test]
    fn test_apply_conditions_unsupported_endpoint() {
        let conditions = vec![("test".to_string(), "eq".to_string(), "test".to_string())];

        let params = apply_conditions_test("unsupported.endpoint", &conditions);

        // Should return empty params for unsupported endpoint
        assert!(params.is_empty());
    }
}
