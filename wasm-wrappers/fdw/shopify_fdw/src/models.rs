use serde::{Deserialize, Serialize};

// This file will contain all the Shopify API data models
// These are just placeholder structs that will be expanded with proper fields

/// General Shopify API response structure
#[derive(Debug, Deserialize)]
pub struct ShopifyResponse<T> {
    pub data: Option<T>,
    pub errors: Option<Vec<ShopifyError>>,
}

#[derive(Debug, Deserialize)]
pub struct ShopifyError {
    pub message: String,
    pub code: Option<String>,
}

/// Products resource models - placeholder structure
#[derive(Debug, Deserialize, Clone)]
pub struct Product {
    pub id: i64,
    pub title: String,
    // Other fields will be added
}

/// Collections resource models - placeholder structure
#[derive(Debug, Deserialize, Clone)]
pub struct CustomCollection {
    pub id: i64,
    pub title: String,
    // Other fields will be added
}

#[derive(Debug, Deserialize, Clone)]
pub struct SmartCollection {
    pub id: i64,
    pub title: String,
    // Other fields will be added
}

/// Customers resource models - placeholder structure
#[derive(Debug, Deserialize, Clone)]
pub struct Customer {
    pub id: i64,
    pub email: Option<String>,
    // Other fields will be added
}

/// Orders resource models - placeholder structure
#[derive(Debug, Deserialize, Clone)]
pub struct Order {
    pub id: i64,
    pub order_number: String,
    // Other fields will be added
}

/// Inventory resource models - placeholder structure
#[derive(Debug, Deserialize, Clone)]
pub struct InventoryItem {
    pub id: i64,
    pub sku: Option<String>,
    // Other fields will be added
}

#[derive(Debug, Deserialize, Clone)]
pub struct InventoryLevel {
    pub inventory_item_id: i64,
    pub location_id: i64,
    pub available: i32,
    // Other fields will be added
}

/// Shop information - placeholder structure
#[derive(Debug, Deserialize, Clone)]
pub struct Shop {
    pub id: i64,
    pub name: String,
    // Other fields will be added
}

/// Metafields - placeholder structure
#[derive(Debug, Deserialize, Clone)]
pub struct Metafield {
    pub id: i64,
    pub namespace: String,
    pub key: String,
    // Other fields will be added
}

// The full implementation will include all model structs
// with complete field definitions and helper methods