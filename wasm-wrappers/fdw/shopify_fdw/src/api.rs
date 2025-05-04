// This module will contain test utilities for API interactions
// and helper functions for building API requests

#[cfg(test)]
use std::collections::HashMap;

#[cfg(test)]
pub fn apply_conditions_test(
    endpoint: &str,
    conditions: &[(String, String, String)], // (column, operator, value)
) -> HashMap<String, String> {
    let mut params = HashMap::new();

    for (column, operator, value) in conditions {
        match endpoint {
            "products.json" => {
                if column == "id" && operator == "eq" {
                    params.insert("ids".to_string(), value.clone());
                } else if column == "title" && operator == "eq" {
                    params.insert("title".to_string(), value.clone());
                } else if column == "vendor" && operator == "eq" {
                    params.insert("vendor".to_string(), value.clone());
                } else if column == "product_type" && operator == "eq" {
                    params.insert("product_type".to_string(), value.clone());
                } else if column == "created_at_min" && operator == "ge" {
                    params.insert("created_at_min".to_string(), value.clone());
                } else if column == "created_at_max" && operator == "le" {
                    params.insert("created_at_max".to_string(), value.clone());
                } else if column == "updated_at_min" && operator == "ge" {
                    params.insert("updated_at_min".to_string(), value.clone());
                } else if column == "updated_at_max" && operator == "le" {
                    params.insert("updated_at_max".to_string(), value.clone());
                } else if column == "published_status" && operator == "eq" {
                    params.insert("published_status".to_string(), value.clone());
                }
            }
            "customers.json" => {
                if column == "id" && operator == "eq" {
                    params.insert("ids".to_string(), value.clone());
                } else if column == "email" && operator == "eq" {
                    params.insert("email".to_string(), value.clone());
                } else if column == "created_at_min" && operator == "ge" {
                    params.insert("created_at_min".to_string(), value.clone());
                } else if column == "created_at_max" && operator == "le" {
                    params.insert("created_at_max".to_string(), value.clone());
                } else if column == "updated_at_min" && operator == "ge" {
                    params.insert("updated_at_min".to_string(), value.clone());
                } else if column == "updated_at_max" && operator == "le" {
                    params.insert("updated_at_max".to_string(), value.clone());
                }
            }
            "orders.json" => {
                if column == "id" && operator == "eq" {
                    params.insert("ids".to_string(), value.clone());
                } else if column == "status" && operator == "eq" {
                    params.insert("status".to_string(), value.clone());
                } else if column == "financial_status" && operator == "eq" {
                    params.insert("financial_status".to_string(), value.clone());
                } else if column == "fulfillment_status" && operator == "eq" {
                    params.insert("fulfillment_status".to_string(), value.clone());
                } else if column == "created_at_min" && operator == "ge" {
                    params.insert("created_at_min".to_string(), value.clone());
                } else if column == "created_at_max" && operator == "le" {
                    params.insert("created_at_max".to_string(), value.clone());
                } else if column == "updated_at_min" && operator == "ge" {
                    params.insert("updated_at_min".to_string(), value.clone());
                } else if column == "updated_at_max" && operator == "le" {
                    params.insert("updated_at_max".to_string(), value.clone());
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
    fn test_apply_conditions_products() {
        let conditions = vec![
            (
                "id".to_string(),
                "eq".to_string(),
                "12345678".to_string(),
            ),
            (
                "title".to_string(),
                "eq".to_string(),
                "Test Product".to_string(),
            ),
        ];

        let params = apply_conditions_test("products.json", &conditions);

        assert_eq!(params.get("ids"), Some(&"12345678".to_string()));
        assert_eq!(params.get("title"), Some(&"Test Product".to_string()));
    }

    #[test]
    fn test_apply_conditions_customers() {
        let conditions = vec![
            (
                "email".to_string(),
                "eq".to_string(),
                "test@example.com".to_string(),
            ),
            (
                "created_at_min".to_string(),
                "ge".to_string(),
                "2023-01-01T00:00:00Z".to_string(),
            ),
        ];

        let params = apply_conditions_test("customers.json", &conditions);

        assert_eq!(params.get("email"), Some(&"test@example.com".to_string()));
        assert_eq!(params.get("created_at_min"), Some(&"2023-01-01T00:00:00Z".to_string()));
    }

    #[test]
    fn test_apply_conditions_orders() {
        let conditions = vec![
            (
                "financial_status".to_string(),
                "eq".to_string(),
                "paid".to_string(),
            ),
            (
                "fulfillment_status".to_string(),
                "eq".to_string(),
                "shipped".to_string(),
            ),
        ];

        let params = apply_conditions_test("orders.json", &conditions);

        assert_eq!(params.get("financial_status"), Some(&"paid".to_string()));
        assert_eq!(params.get("fulfillment_status"), Some(&"shipped".to_string()));
    }

    #[test]
    fn test_apply_conditions_unsupported_endpoint() {
        let conditions = vec![("test".to_string(), "eq".to_string(), "test".to_string())];

        let params = apply_conditions_test("unsupported.endpoint", &conditions);

        // Should return empty params for unsupported endpoint
        assert!(params.is_empty());
    }
}