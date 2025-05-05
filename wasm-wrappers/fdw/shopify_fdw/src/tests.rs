//! Tests for Shopify FDW
//!
//! This module contains unit and integration tests for the Shopify FDW.

#[cfg(test)]
mod tests {
    use crate::ShopifyFdw;
    use crate::models::*;
    use std::collections::HashMap;
    
    #[test]
    fn test_product_deserialization() {
        // Sample product JSON from Shopify API
        let product_json = r#"
        {
            "id": 632910392,
            "title": "Test Product",
            "body_html": "<p>This is a test product</p>",
            "vendor": "Test Vendor",
            "product_type": "Test Type",
            "created_at": "2023-10-03T13:05:25-04:00",
            "handle": "test-product",
            "updated_at": "2023-10-03T13:05:25-04:00",
            "published_at": "2023-10-03T13:05:25-04:00",
            "template_suffix": null,
            "status": "active",
            "published_scope": "web",
            "tags": "Tag1, Tag2",
            "admin_graphql_api_id": "gid://shopify/Product/632910392",
            "variants": [
                {
                    "id": 808950810,
                    "product_id": 632910392,
                    "title": "Default Title",
                    "price": "19.99",
                    "sku": "TEST-SKU-1",
                    "position": 1,
                    "inventory_policy": "deny",
                    "compare_at_price": null,
                    "fulfillment_service": "manual",
                    "inventory_management": "shopify",
                    "option1": "Default Title",
                    "option2": null,
                    "option3": null,
                    "created_at": "2023-10-03T13:05:25-04:00",
                    "updated_at": "2023-10-03T13:05:25-04:00",
                    "taxable": true,
                    "barcode": "",
                    "grams": 0,
                    "image_id": null,
                    "weight": 0.0,
                    "weight_unit": "kg",
                    "inventory_item_id": 808950810,
                    "inventory_quantity": 10,
                    "old_inventory_quantity": 10,
                    "requires_shipping": true,
                    "admin_graphql_api_id": "gid://shopify/ProductVariant/808950810"
                }
            ],
            "options": [
                {
                    "id": 594680422,
                    "product_id": 632910392,
                    "name": "Title",
                    "position": 1,
                    "values": ["Default Title"]
                }
            ],
            "images": [],
            "image": null
        }
        "#;
        
        // Deserialize JSON to Product
        let product: Product = serde_json::from_str(product_json).expect("Failed to deserialize product");
        
        // Verify fields
        assert_eq!(product.id, 632910392);
        assert_eq!(product.title, "Test Product");
        assert_eq!(product.body_html, Some("<p>This is a test product</p>".to_string()));
        assert_eq!(product.vendor, Some("Test Vendor".to_string()));
        assert_eq!(product.product_type, Some("Test Type".to_string()));
        assert_eq!(product.created_at, "2023-10-03T13:05:25-04:00");
        assert_eq!(product.updated_at, "2023-10-03T13:05:25-04:00");
        assert_eq!(product.published_at, Some("2023-10-03T13:05:25-04:00".to_string()));
        assert_eq!(product.status, "active");
        assert_eq!(product.tags, Some("Tag1, Tag2".to_string()));
        
        // Verify variants
        assert_eq!(product.variants.len(), 1);
        let variant = &product.variants[0];
        assert_eq!(variant.id, 808950810);
        assert_eq!(variant.price, "19.99");
        assert_eq!(variant.sku, Some("TEST-SKU-1".to_string()));
        
        // Verify options
        assert_eq!(product.options.len(), 1);
        let option = &product.options[0];
        assert_eq!(option.name, "Title");
        assert_eq!(option.values, vec!["Default Title"]);
    }
    
    #[test]
    fn test_products_response_parsing() {
        // Sample products response from Shopify API
        let products_json = r#"
        {
            "products": [
                {
                    "id": 632910392,
                    "title": "Product 1",
                    "body_html": "<p>Description 1</p>",
                    "vendor": "Vendor 1",
                    "product_type": "Type 1",
                    "created_at": "2023-10-03T13:05:25-04:00",
                    "handle": "product-1",
                    "updated_at": "2023-10-03T13:05:25-04:00",
                    "published_at": "2023-10-03T13:05:25-04:00",
                    "status": "active",
                    "tags": "Tag1, Tag2",
                    "variants": [],
                    "options": [],
                    "images": [],
                    "image": null
                },
                {
                    "id": 632910393,
                    "title": "Product 2",
                    "body_html": "<p>Description 2</p>",
                    "vendor": "Vendor 2",
                    "product_type": "Type 2",
                    "created_at": "2023-10-04T13:05:25-04:00",
                    "handle": "product-2",
                    "updated_at": "2023-10-04T13:05:25-04:00",
                    "published_at": "2023-10-04T13:05:25-04:00",
                    "status": "active",
                    "tags": "Tag3, Tag4",
                    "variants": [],
                    "options": [],
                    "images": [],
                    "image": null
                }
            ]
        }
        "#;
        
        // Parse JSON
        let json: serde_json::Value = serde_json::from_str(products_json).expect("Failed to parse JSON");
        
        // Extract products
        let products = json.get("products").and_then(|p| p.as_array()).expect("Products not found");
        
        // Verify products
        assert_eq!(products.len(), 2);
        
        // Deserialize products
        let products: Vec<Product> = products
            .iter()
            .map(|p| serde_json::from_value(p.clone()).expect("Failed to deserialize product"))
            .collect();
        
        // Verify product fields
        assert_eq!(products[0].id, 632910392);
        assert_eq!(products[0].title, "Product 1");
        assert_eq!(products[1].id, 632910393);
        assert_eq!(products[1].title, "Product 2");
    }
    
    #[test]
    fn test_link_header_parsing() {
        // Create a mock Link header
        let link_header = r#"<https://example.myshopify.com/admin/api/2023-10/products.json?page_info=abc123>; rel="next", <https://example.myshopify.com/admin/api/2023-10/products.json?page_info=xyz987>; rel="previous""#;
        
        // Create a FDW instance
        let mut fdw = ShopifyFdw::default();
        fdw.shop_domain = "example.myshopify.com".to_string();
        fdw.api_token = "test_token".to_string();
        fdw.api_version = "2023-10".to_string();
        
        // Parse the Link header
        let mut headers = HashMap::new();
        headers.insert("Link".to_string(), link_header.to_string());
        
        // Extract the next page token
        let next_page_token = fdw.extract_next_page_info(&headers);
        
        // Verify the token
        assert_eq!(next_page_token, Some("abc123".to_string()));
    }
}