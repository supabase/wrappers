//! Shopify Foreign Data Wrapper for PostgreSQL
//!
//! This module implements a Shopify FDW that allows querying Shopify stores directly from PostgreSQL.
//! It supports various resource types including products, collections, customers, orders, inventory, and shop info.
//! The FDW leverages the Shopify REST API to fetch data and supports query pushdown for filtering and sorting.
//!
//! Resources supported:
//! - products: Query store products with variants
//! - product_variants: Query product variants
//! - custom_collections: Query custom (manual) collections
//! - smart_collections: Query smart (automated) collections
//! - customers: Query customer information
//! - orders: Query order information
//! - inventory_items: Query inventory items
//! - inventory_levels: Query inventory levels by location
//! - shop: Query shop information

#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

// Shopify FDW implementation modules
pub mod api;
pub mod models;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{
            Cell, Context, FdwError, FdwResult, ImportForeignSchemaStmt, Limit, OptionsType, Row,
            Sort, Value,
        },
        utils,
    },
};

// Import all model types needed for the FDW
use models::{
    CustomCollection, Customer, InventoryItem, InventoryLevel, Order, Product, ProductVariant,
    Shop, SmartCollection,
};

/// Shopify Foreign Data Wrapper implementation
///
/// This struct encapsulates all the functionality to interact with the Shopify API,
/// translate Shopify data to PostgreSQL rows, and handle query pushdown.
#[derive(Debug, Default)]
struct ShopifyFdw {
    // Connection state
    /// Shopify API access token
    api_token: String,
    /// Shopify shop domain (e.g., "your-shop.myshopify.com")
    shop_domain: String,
    /// Shopify API version (e.g., "2023-07")
    api_version: String,

    // Request state for pagination
    /// Current resource type being accessed
    resource: String,
    /// Next page URL for pagination
    next_page_url: Option<String>,

    // Cache for API responses
    /// Cached products from the store
    products: Vec<Product>,
    /// Cached product variants
    product_variants: Vec<ProductVariant>,
    /// Cached custom collections from the store
    custom_collections: Vec<CustomCollection>,
    /// Cached smart collections from the store
    smart_collections: Vec<SmartCollection>,
    /// Cached customers from the store
    customers: Vec<Customer>,
    /// Cached orders from the store
    orders: Vec<Order>,
    /// Cached inventory items from the store
    inventory_items: Vec<InventoryItem>,
    /// Cached inventory levels from the store
    inventory_levels: Vec<InventoryLevel>,
    /// Cached shop information
    shop: Option<Shop>,

    /// Current position in the result set for iteration
    result_index: usize,

    // Query pushdown support
    /// Sorting criteria for query pushdown
    sorts: Vec<Sort>,
    /// Limit and offset for query pushdown
    limit: Option<Limit>,

    /// Filter conditions for query pushdown
    conditions: Vec<(String, String, String)>,
}

/// Global instance of the FDW as required by the PostgreSQL FDW API
static mut INSTANCE: *mut ShopifyFdw = std::ptr::null_mut::<ShopifyFdw>();

/// Name of the FDW for logging and metrics
static FDW_NAME: &str = "ShopifyFdw";

/// Maximum number of items returned per API request for pagination
static BATCH_SIZE: u32 = 50;

impl ShopifyFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // Map Shopify Product to PostgreSQL Row
    fn product_to_row(&self, product: &Product, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::I64(product.id)));
        row.push(Some(&Cell::String(product.title.clone())));
        
        // Optional fields
        if let Some(body_html) = &product.body_html {
            row.push(Some(&Cell::String(body_html.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(vendor) = &product.vendor {
            row.push(Some(&Cell::String(vendor.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(product_type) = &product.product_type {
            row.push(Some(&Cell::String(product_type.clone())));
        } else {
            row.push(None);
        }
        
        // Timestamps
        row.push(Some(&Cell::String(product.created_at.clone())));
        row.push(Some(&Cell::String(product.updated_at.clone())));
        
        if let Some(published_at) = &product.published_at {
            row.push(Some(&Cell::String(published_at.clone())));
        } else {
            row.push(None);
        }
        
        // Status
        row.push(Some(&Cell::String(product.status.clone())));
        
        // Tags
        if let Some(tags) = &product.tags {
            row.push(Some(&Cell::String(tags.clone())));
        } else {
            row.push(None);
        }
        
        // Variant count
        row.push(Some(&Cell::I32(product.variants.len() as i32)));
        
        // First image URL, if any
        if let Some(image) = &product.image {
            row.push(Some(&Cell::String(image.src.clone())));
        } else if !product.images.is_empty() {
            row.push(Some(&Cell::String(product.images[0].src.clone())));
        } else {
            row.push(None);
        }

        Ok(())
    }

    // Map Shopify ProductVariant to PostgreSQL Row
    fn product_variant_to_row(&self, variant: &ProductVariant, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::I64(variant.id)));
        row.push(Some(&Cell::I64(variant.product_id)));
        row.push(Some(&Cell::String(variant.title.clone())));
        row.push(Some(&Cell::String(variant.price.clone())));
        
        // Optional fields
        if let Some(sku) = &variant.sku {
            row.push(Some(&Cell::String(sku.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::I32(variant.position)));
        
        if let Some(inventory_policy) = &variant.inventory_policy {
            row.push(Some(&Cell::String(inventory_policy.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(compare_at_price) = &variant.compare_at_price {
            row.push(Some(&Cell::String(compare_at_price.clone())));
        } else {
            row.push(None);
        }
        
        // Options
        if let Some(option1) = &variant.option1 {
            row.push(Some(&Cell::String(option1.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(option2) = &variant.option2 {
            row.push(Some(&Cell::String(option2.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(option3) = &variant.option3 {
            row.push(Some(&Cell::String(option3.clone())));
        } else {
            row.push(None);
        }
        
        // Timestamps
        row.push(Some(&Cell::String(variant.created_at.clone())));
        row.push(Some(&Cell::String(variant.updated_at.clone())));
        
        // Inventory information
        row.push(Some(&Cell::Bool(variant.taxable)));
        
        if let Some(barcode) = &variant.barcode {
            row.push(Some(&Cell::String(barcode.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::I32(variant.grams)));
        
        if let Some(image_id) = variant.image_id {
            row.push(Some(&Cell::I64(image_id)));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::F64(variant.weight)));
        row.push(Some(&Cell::String(variant.weight_unit.clone())));
        row.push(Some(&Cell::I64(variant.inventory_item_id)));
        row.push(Some(&Cell::I32(variant.inventory_quantity)));
        row.push(Some(&Cell::I32(variant.old_inventory_quantity)));
        row.push(Some(&Cell::Bool(variant.requires_shipping)));

        Ok(())
    }

    // Map Shopify CustomCollection to PostgreSQL Row
    fn custom_collection_to_row(&self, collection: &CustomCollection, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::I64(collection.id)));
        row.push(Some(&Cell::String(collection.title.clone())));
        
        // Optional fields
        if let Some(body_html) = &collection.body_html {
            row.push(Some(&Cell::String(body_html.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(collection.handle.clone())));
        row.push(Some(&Cell::String(collection.updated_at.clone())));
        
        if let Some(published_at) = &collection.published_at {
            row.push(Some(&Cell::String(published_at.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(collection.sort_order.clone())));
        
        if let Some(template_suffix) = &collection.template_suffix {
            row.push(Some(&Cell::String(template_suffix.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(collection.published_scope.clone())));
        
        // Collection image, if any
        if let Some(image) = &collection.image {
            row.push(Some(&Cell::String(image.src.clone())));
        } else {
            row.push(None);
        }

        Ok(())
    }

    // Map Shopify SmartCollection to PostgreSQL Row
    fn smart_collection_to_row(&self, collection: &SmartCollection, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::I64(collection.id)));
        row.push(Some(&Cell::String(collection.title.clone())));
        
        // Optional fields
        if let Some(body_html) = &collection.body_html {
            row.push(Some(&Cell::String(body_html.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(collection.handle.clone())));
        row.push(Some(&Cell::String(collection.updated_at.clone())));
        
        if let Some(published_at) = &collection.published_at {
            row.push(Some(&Cell::String(published_at.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(collection.sort_order.clone())));
        
        if let Some(template_suffix) = &collection.template_suffix {
            row.push(Some(&Cell::String(template_suffix.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(collection.published_scope.clone())));
        
        // Collection image, if any
        if let Some(image) = &collection.image {
            row.push(Some(&Cell::String(image.src.clone())));
        } else {
            row.push(None);
        }
        
        // Smart collection specific fields
        row.push(Some(&Cell::Bool(collection.disjunctive)));
        
        // Rules count
        row.push(Some(&Cell::I32(collection.rules.len() as i32)));

        Ok(())
    }

    // Map Shopify Customer to PostgreSQL Row
    fn customer_to_row(&self, customer: &Customer, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::I64(customer.id)));
        
        if let Some(email) = &customer.email {
            row.push(Some(&Cell::String(email.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::Bool(customer.accepts_marketing)));
        row.push(Some(&Cell::String(customer.created_at.clone())));
        row.push(Some(&Cell::String(customer.updated_at.clone())));
        
        // Name fields
        if let Some(first_name) = &customer.first_name {
            row.push(Some(&Cell::String(first_name.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(last_name) = &customer.last_name {
            row.push(Some(&Cell::String(last_name.clone())));
        } else {
            row.push(None);
        }
        
        // Order information
        row.push(Some(&Cell::I32(customer.orders_count)));
        row.push(Some(&Cell::String(customer.state.clone())));
        row.push(Some(&Cell::String(customer.total_spent.clone())));
        
        if let Some(last_order_id) = customer.last_order_id {
            row.push(Some(&Cell::I64(last_order_id)));
        } else {
            row.push(None);
        }
        
        // Additional fields
        if let Some(note) = &customer.note {
            row.push(Some(&Cell::String(note.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::Bool(customer.verified_email)));
        row.push(Some(&Cell::Bool(customer.tax_exempt)));
        
        if let Some(phone) = &customer.phone {
            row.push(Some(&Cell::String(phone.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(customer.tags.clone())));
        
        if let Some(last_order_name) = &customer.last_order_name {
            row.push(Some(&Cell::String(last_order_name.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(customer.currency.clone())));
        
        // Address count
        row.push(Some(&Cell::I32(customer.addresses.len() as i32)));

        Ok(())
    }

    // Map Shopify Order to PostgreSQL Row
    fn order_to_row(&self, order: &Order, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::I64(order.id)));
        
        if let Some(email) = &order.email {
            row.push(Some(&Cell::String(email.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(closed_at) = &order.closed_at {
            row.push(Some(&Cell::String(closed_at.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(order.created_at.clone())));
        row.push(Some(&Cell::String(order.updated_at.clone())));
        row.push(Some(&Cell::I32(order.number)));
        
        if let Some(note) = &order.note {
            row.push(Some(&Cell::String(note.clone())));
        } else {
            row.push(None);
        }
        
        // Financial information
        row.push(Some(&Cell::String(order.token.clone())));
        
        if let Some(gateway) = &order.gateway {
            row.push(Some(&Cell::String(gateway.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::Bool(order.test)));
        row.push(Some(&Cell::String(order.total_price.clone())));
        row.push(Some(&Cell::String(order.subtotal_price.clone())));
        row.push(Some(&Cell::I32(order.total_weight)));
        row.push(Some(&Cell::String(order.total_tax.clone())));
        row.push(Some(&Cell::Bool(order.taxes_included)));
        row.push(Some(&Cell::String(order.currency.clone())));
        row.push(Some(&Cell::String(order.financial_status.clone())));
        row.push(Some(&Cell::Bool(order.confirmed)));
        row.push(Some(&Cell::String(order.total_discounts.clone())));
        row.push(Some(&Cell::String(order.total_line_items_price.clone())));
        
        // Additional fields
        if let Some(cart_token) = &order.cart_token {
            row.push(Some(&Cell::String(cart_token.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::Bool(order.buyer_accepts_marketing)));
        row.push(Some(&Cell::String(order.name.clone())));
        
        // Cancellation information
        if let Some(cancelled_at) = &order.cancelled_at {
            row.push(Some(&Cell::String(cancelled_at.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(cancel_reason) = &order.cancel_reason {
            row.push(Some(&Cell::String(cancel_reason.clone())));
        } else {
            row.push(None);
        }
        
        // Processing information
        row.push(Some(&Cell::String(order.processed_at.clone())));
        
        // Customer information
        if let Some(customer) = &order.customer {
            row.push(Some(&Cell::I64(customer.id)));
        } else {
            row.push(None);
        }
        
        // Fulfillment information
        if let Some(fulfillment_status) = &order.fulfillment_status {
            row.push(Some(&Cell::String(fulfillment_status.clone())));
        } else {
            row.push(None);
        }
        
        // Line item count
        row.push(Some(&Cell::I32(order.line_items.len() as i32)));
        
        // Shipping line count
        row.push(Some(&Cell::I32(order.shipping_lines.len() as i32)));
        
        // Fulfillment count
        row.push(Some(&Cell::I32(order.fulfillments.len() as i32)));
        
        // Refund count
        row.push(Some(&Cell::I32(order.refunds.len() as i32)));

        Ok(())
    }

    // Map Shopify InventoryItem to PostgreSQL Row
    fn inventory_item_to_row(&self, item: &InventoryItem, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::I64(item.id)));
        
        if let Some(sku) = &item.sku {
            row.push(Some(&Cell::String(sku.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(item.created_at.clone())));
        row.push(Some(&Cell::String(item.updated_at.clone())));
        row.push(Some(&Cell::Bool(item.requires_shipping)));
        
        // Cost
        if let Some(cost) = &item.cost {
            row.push(Some(&Cell::String(cost.clone())));
        } else {
            row.push(None);
        }
        
        // Origin information
        if let Some(country_code) = &item.country_code_of_origin {
            row.push(Some(&Cell::String(country_code.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(province_code) = &item.province_code_of_origin {
            row.push(Some(&Cell::String(province_code.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(harmonized_code) = &item.harmonized_system_code {
            row.push(Some(&Cell::String(harmonized_code.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::Bool(item.tracked)));
        
        // Count of country harmonized system codes
        row.push(Some(&Cell::I32(item.country_harmonized_system_codes.len() as i32)));

        Ok(())
    }

    // Map Shopify InventoryLevel to PostgreSQL Row
    fn inventory_level_to_row(&self, level: &InventoryLevel, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::I64(level.inventory_item_id)));
        row.push(Some(&Cell::I64(level.location_id)));
        row.push(Some(&Cell::I32(level.available)));
        row.push(Some(&Cell::String(level.updated_at.clone())));

        Ok(())
    }

    // Map Shopify Shop to PostgreSQL Row
    fn shop_to_row(&self, shop: &Shop, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::I64(shop.id)));
        row.push(Some(&Cell::String(shop.name.clone())));
        row.push(Some(&Cell::String(shop.email.clone())));
        row.push(Some(&Cell::String(shop.domain.clone())));
        
        // Location information
        if let Some(province) = &shop.province {
            row.push(Some(&Cell::String(province.clone())));
        } else {
            row.push(None);
        }
        
        row.push(Some(&Cell::String(shop.country.clone())));
        
        if let Some(address1) = &shop.address1 {
            row.push(Some(&Cell::String(address1.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(zip) = &shop.zip {
            row.push(Some(&Cell::String(zip.clone())));
        } else {
            row.push(None);
        }
        
        if let Some(city) = &shop.city {
            row.push(Some(&Cell::String(city.clone())));
        } else {
            row.push(None);
        }
        
        // Phone
        if let Some(phone) = &shop.phone {
            row.push(Some(&Cell::String(phone.clone())));
        } else {
            row.push(None);
        }
        
        // Timestamps
        row.push(Some(&Cell::String(shop.created_at.clone())));
        row.push(Some(&Cell::String(shop.updated_at.clone())));
        
        // Country information
        row.push(Some(&Cell::String(shop.country_code.clone())));
        row.push(Some(&Cell::String(shop.country_name.clone())));
        
        // Currency and locale
        row.push(Some(&Cell::String(shop.currency.clone())));
        row.push(Some(&Cell::String(shop.primary_locale.clone())));
        row.push(Some(&Cell::String(shop.timezone.clone())));
        
        // Shop configuration
        row.push(Some(&Cell::String(shop.shop_owner.clone())));
        row.push(Some(&Cell::String(shop.money_format.clone())));
        row.push(Some(&Cell::String(shop.weight_unit.clone())));
        row.push(Some(&Cell::String(shop.plan_display_name.clone())));
        row.push(Some(&Cell::Bool(shop.has_storefront)));

        Ok(())
    }

    // Create a request to the Shopify API client
    fn create_shopify_config(&self) -> api::ShopifyConfig {
        api::ShopifyConfig {
            shop_domain: self.shop_domain.clone(),
            api_version: self.api_version.clone(),
            access_token: self.api_token.clone(),
            api_key: None,
            api_secret: None,
            rate_limit: 2.0, // Default to 2 requests per second
        }
    }

    // Transform PostgreSQL quals to Shopify API filter conditions
    fn transform_quals_to_conditions(&self, ctx: &Context) -> Vec<(String, String, String)> {
        let mut conditions = Vec::new();
        
        // Get all quals from the context
        let quals = ctx.get_quals();
        if !quals.is_empty() {
            for qual in quals.iter() {
                if qual.use_or() {
                    // Skip OR conditions as they can't be pushed down reliably
                    continue;
                }
                
                // Map PostgreSQL operators to Shopify API operators
                let operator = match qual.operator().as_str() {
                    "=" => "eq",
                    ">" => "gt",
                    ">=" => "ge",
                    "<" => "lt",
                    "<=" => "le",
                    "LIKE" => "like",
                    _ => continue, // Skip unsupported operators
                };
                
                // Extract value based on its type
                let value = match qual.value() {
                    Value::Cell(Cell::String(val)) => val.clone(),
                    Value::Cell(Cell::I64(val)) => val.to_string(),
                    Value::Cell(Cell::I32(val)) => val.to_string(),
                    Value::Cell(Cell::Bool(val)) => val.to_string(),
                    Value::Cell(Cell::F64(val)) => val.to_string(),
                    _ => continue, // Skip unsupported value types
                };
                
                // Add condition to the list
                conditions.push((qual.field().to_string(), operator.to_string(), value));
            }
        }
        
        conditions
    }

    // Fetch products with query pushdown
    async fn fetch_products(&mut self, ctx: &Context) -> FdwResult {
        // Clear previous results
        self.products.clear();
        
        // Transform quals to API conditions
        let conditions = self.transform_quals_to_conditions(ctx);
        
        // Create Shopify API client
        let config = self.create_shopify_config();
        let mut client = api::ShopifyClient::new(config);
        
        // Fetch products with conditions
        match client.get_products(if conditions.is_empty() { None } else { Some(&conditions) }).await {
            Ok(products) => {
                self.products = products;
                
                // Apply sorting if requested
                if !self.sorts.is_empty() {
                    self.products.sort_by(|a, b| {
                        for sort in &self.sorts {
                            match sort.field().as_str() {
                                "id" => {
                                    let ordering = a.id.cmp(&b.id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "title" => {
                                    let ordering = a.title.cmp(&b.title);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "created_at" => {
                                    let ordering = a.created_at.cmp(&b.created_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "updated_at" => {
                                    let ordering = a.updated_at.cmp(&b.updated_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "vendor" => {
                                    let a_vendor = a.vendor.as_ref().unwrap_or(&String::new());
                                    let b_vendor = b.vendor.as_ref().unwrap_or(&String::new());
                                    let ordering = a_vendor.cmp(b_vendor);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                _ => {}
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                
                // Apply LIMIT and OFFSET if specified
                if let Some(limit) = &self.limit {
                    let start = limit.offset() as usize;
                    let end = (limit.offset() + limit.count()) as usize;
                    
                    // Handle offset - trim the beginning of the results
                    if start < self.products.len() {
                        self.products = self.products[start..].to_vec();
                    } else {
                        self.products.clear();
                    }
                    
                    // Handle count - trim the end of the results if needed
                    if self.products.len() > end - start {
                        self.products.truncate(end - start);
                    }
                }
                
                // Reset position
                self.result_index = 0;
                
                Ok(())
            }
            Err(e) => Err(format!("Failed to fetch products: {}", e)),
        }
    }

    // Fetch product variants with query pushdown
    async fn fetch_product_variants(&mut self, ctx: &Context) -> FdwResult {
        // Clear previous results
        self.product_variants.clear();
        
        // Get products first, as variants come from products
        match self.fetch_products(ctx) {
            Ok(_) => {
                // Extract variants from products
                for product in &self.products {
                    self.product_variants.extend(product.variants.clone());
                }
                
                // Apply sorting if requested
                if !self.sorts.is_empty() {
                    self.product_variants.sort_by(|a, b| {
                        for sort in &self.sorts {
                            match sort.field().as_str() {
                                "id" => {
                                    let ordering = a.id.cmp(&b.id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "product_id" => {
                                    let ordering = a.product_id.cmp(&b.product_id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "title" => {
                                    let ordering = a.title.cmp(&b.title);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "price" => {
                                    let ordering = a.price.cmp(&b.price);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "position" => {
                                    let ordering = a.position.cmp(&b.position);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "created_at" => {
                                    let ordering = a.created_at.cmp(&b.created_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "updated_at" => {
                                    let ordering = a.updated_at.cmp(&b.updated_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                _ => {}
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                
                // Apply LIMIT and OFFSET if specified
                if let Some(limit) = &self.limit {
                    let start = limit.offset() as usize;
                    let end = (limit.offset() + limit.count()) as usize;
                    
                    // Handle offset - trim the beginning of the results
                    if start < self.product_variants.len() {
                        self.product_variants = self.product_variants[start..].to_vec();
                    } else {
                        self.product_variants.clear();
                    }
                    
                    // Handle count - trim the end of the results if needed
                    if self.product_variants.len() > end - start {
                        self.product_variants.truncate(end - start);
                    }
                }
                
                // Reset position
                self.result_index = 0;
                
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    // Fetch custom collections with query pushdown
    async fn fetch_custom_collections(&mut self, ctx: &Context) -> FdwResult {
        // Clear previous results
        self.custom_collections.clear();
        
        // Transform quals to API conditions
        let conditions = self.transform_quals_to_conditions(ctx);
        
        // Create Shopify API client
        let config = self.create_shopify_config();
        let mut client = api::ShopifyClient::new(config);
        
        // Fetch custom collections with conditions
        match client.get_custom_collections(if conditions.is_empty() { None } else { Some(&conditions) }).await {
            Ok(collections) => {
                self.custom_collections = collections;
                
                // Apply sorting if requested
                if !self.sorts.is_empty() {
                    self.custom_collections.sort_by(|a, b| {
                        for sort in &self.sorts {
                            match sort.field().as_str() {
                                "id" => {
                                    let ordering = a.id.cmp(&b.id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "title" => {
                                    let ordering = a.title.cmp(&b.title);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "handle" => {
                                    let ordering = a.handle.cmp(&b.handle);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "updated_at" => {
                                    let ordering = a.updated_at.cmp(&b.updated_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                _ => {}
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                
                // Apply LIMIT and OFFSET if specified
                if let Some(limit) = &self.limit {
                    let start = limit.offset() as usize;
                    let end = (limit.offset() + limit.count()) as usize;
                    
                    // Handle offset - trim the beginning of the results
                    if start < self.custom_collections.len() {
                        self.custom_collections = self.custom_collections[start..].to_vec();
                    } else {
                        self.custom_collections.clear();
                    }
                    
                    // Handle count - trim the end of the results if needed
                    if self.custom_collections.len() > end - start {
                        self.custom_collections.truncate(end - start);
                    }
                }
                
                // Reset position
                self.result_index = 0;
                
                Ok(())
            }
            Err(e) => Err(format!("Failed to fetch custom collections: {}", e)),
        }
    }

    // Fetch smart collections with query pushdown
    async fn fetch_smart_collections(&mut self, ctx: &Context) -> FdwResult {
        // Clear previous results
        self.smart_collections.clear();
        
        // Transform quals to API conditions
        let conditions = self.transform_quals_to_conditions(ctx);
        
        // Create Shopify API client
        let config = self.create_shopify_config();
        let mut client = api::ShopifyClient::new(config);
        
        // Fetch smart collections with conditions
        match client.get_smart_collections(if conditions.is_empty() { None } else { Some(&conditions) }).await {
            Ok(collections) => {
                self.smart_collections = collections;
                
                // Apply sorting if requested
                if !self.sorts.is_empty() {
                    self.smart_collections.sort_by(|a, b| {
                        for sort in &self.sorts {
                            match sort.field().as_str() {
                                "id" => {
                                    let ordering = a.id.cmp(&b.id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "title" => {
                                    let ordering = a.title.cmp(&b.title);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "handle" => {
                                    let ordering = a.handle.cmp(&b.handle);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "updated_at" => {
                                    let ordering = a.updated_at.cmp(&b.updated_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                _ => {}
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                
                // Apply LIMIT and OFFSET if specified
                if let Some(limit) = &self.limit {
                    let start = limit.offset() as usize;
                    let end = (limit.offset() + limit.count()) as usize;
                    
                    // Handle offset - trim the beginning of the results
                    if start < self.smart_collections.len() {
                        self.smart_collections = self.smart_collections[start..].to_vec();
                    } else {
                        self.smart_collections.clear();
                    }
                    
                    // Handle count - trim the end of the results if needed
                    if self.smart_collections.len() > end - start {
                        self.smart_collections.truncate(end - start);
                    }
                }
                
                // Reset position
                self.result_index = 0;
                
                Ok(())
            }
            Err(e) => Err(format!("Failed to fetch smart collections: {}", e)),
        }
    }

    // Fetch customers with query pushdown
    async fn fetch_customers(&mut self, ctx: &Context) -> FdwResult {
        // Clear previous results
        self.customers.clear();
        
        // Transform quals to API conditions
        let conditions = self.transform_quals_to_conditions(ctx);
        
        // Create Shopify API client
        let config = self.create_shopify_config();
        let mut client = api::ShopifyClient::new(config);
        
        // Fetch customers with conditions
        match client.get_customers(if conditions.is_empty() { None } else { Some(&conditions) }).await {
            Ok(customers) => {
                self.customers = customers;
                
                // Apply sorting if requested
                if !self.sorts.is_empty() {
                    self.customers.sort_by(|a, b| {
                        for sort in &self.sorts {
                            match sort.field().as_str() {
                                "id" => {
                                    let ordering = a.id.cmp(&b.id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "email" => {
                                    let a_email = a.email.as_ref().unwrap_or(&String::new());
                                    let b_email = b.email.as_ref().unwrap_or(&String::new());
                                    let ordering = a_email.cmp(b_email);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "created_at" => {
                                    let ordering = a.created_at.cmp(&b.created_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "updated_at" => {
                                    let ordering = a.updated_at.cmp(&b.updated_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "orders_count" => {
                                    let ordering = a.orders_count.cmp(&b.orders_count);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                _ => {}
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                
                // Apply LIMIT and OFFSET if specified
                if let Some(limit) = &self.limit {
                    let start = limit.offset() as usize;
                    let end = (limit.offset() + limit.count()) as usize;
                    
                    // Handle offset - trim the beginning of the results
                    if start < self.customers.len() {
                        self.customers = self.customers[start..].to_vec();
                    } else {
                        self.customers.clear();
                    }
                    
                    // Handle count - trim the end of the results if needed
                    if self.customers.len() > end - start {
                        self.customers.truncate(end - start);
                    }
                }
                
                // Reset position
                self.result_index = 0;
                
                Ok(())
            }
            Err(e) => Err(format!("Failed to fetch customers: {}", e)),
        }
    }

    // Fetch orders with query pushdown
    async fn fetch_orders(&mut self, ctx: &Context) -> FdwResult {
        // Clear previous results
        self.orders.clear();
        
        // Transform quals to API conditions
        let conditions = self.transform_quals_to_conditions(ctx);
        
        // Create Shopify API client
        let config = self.create_shopify_config();
        let mut client = api::ShopifyClient::new(config);
        
        // Fetch orders with conditions
        match client.get_orders(if conditions.is_empty() { None } else { Some(&conditions) }).await {
            Ok(orders) => {
                self.orders = orders;
                
                // Apply sorting if requested
                if !self.sorts.is_empty() {
                    self.orders.sort_by(|a, b| {
                        for sort in &self.sorts {
                            match sort.field().as_str() {
                                "id" => {
                                    let ordering = a.id.cmp(&b.id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "created_at" => {
                                    let ordering = a.created_at.cmp(&b.created_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "updated_at" => {
                                    let ordering = a.updated_at.cmp(&b.updated_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "processed_at" => {
                                    let ordering = a.processed_at.cmp(&b.processed_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "total_price" => {
                                    let ordering = a.total_price.cmp(&b.total_price);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                _ => {}
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                
                // Apply LIMIT and OFFSET if specified
                if let Some(limit) = &self.limit {
                    let start = limit.offset() as usize;
                    let end = (limit.offset() + limit.count()) as usize;
                    
                    // Handle offset - trim the beginning of the results
                    if start < self.orders.len() {
                        self.orders = self.orders[start..].to_vec();
                    } else {
                        self.orders.clear();
                    }
                    
                    // Handle count - trim the end of the results if needed
                    if self.orders.len() > end - start {
                        self.orders.truncate(end - start);
                    }
                }
                
                // Reset position
                self.result_index = 0;
                
                Ok(())
            }
            Err(e) => Err(format!("Failed to fetch orders: {}", e)),
        }
    }

    // Fetch inventory items with query pushdown
    async fn fetch_inventory_items(&mut self, ctx: &Context) -> FdwResult {
        // Clear previous results
        self.inventory_items.clear();
        
        // Transform quals to API conditions
        let conditions = self.transform_quals_to_conditions(ctx);
        
        // Create Shopify API client
        let config = self.create_shopify_config();
        let mut client = api::ShopifyClient::new(config);
        
        // Fetch inventory items with conditions
        match client.get_inventory_items(if conditions.is_empty() { None } else { Some(&conditions) }).await {
            Ok(items) => {
                self.inventory_items = items;
                
                // Apply sorting if requested
                if !self.sorts.is_empty() {
                    self.inventory_items.sort_by(|a, b| {
                        for sort in &self.sorts {
                            match sort.field().as_str() {
                                "id" => {
                                    let ordering = a.id.cmp(&b.id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "sku" => {
                                    let a_sku = a.sku.as_ref().unwrap_or(&String::new());
                                    let b_sku = b.sku.as_ref().unwrap_or(&String::new());
                                    let ordering = a_sku.cmp(b_sku);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "created_at" => {
                                    let ordering = a.created_at.cmp(&b.created_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "updated_at" => {
                                    let ordering = a.updated_at.cmp(&b.updated_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                _ => {}
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                
                // Apply LIMIT and OFFSET if specified
                if let Some(limit) = &self.limit {
                    let start = limit.offset() as usize;
                    let end = (limit.offset() + limit.count()) as usize;
                    
                    // Handle offset - trim the beginning of the results
                    if start < self.inventory_items.len() {
                        self.inventory_items = self.inventory_items[start..].to_vec();
                    } else {
                        self.inventory_items.clear();
                    }
                    
                    // Handle count - trim the end of the results if needed
                    if self.inventory_items.len() > end - start {
                        self.inventory_items.truncate(end - start);
                    }
                }
                
                // Reset position
                self.result_index = 0;
                
                Ok(())
            }
            Err(e) => Err(format!("Failed to fetch inventory items: {}", e)),
        }
    }

    // Fetch inventory levels with query pushdown
    async fn fetch_inventory_levels(&mut self, ctx: &Context) -> FdwResult {
        // Clear previous results
        self.inventory_levels.clear();
        
        // Transform quals to API conditions
        let conditions = self.transform_quals_to_conditions(ctx);
        
        // Create Shopify API client
        let config = self.create_shopify_config();
        let mut client = api::ShopifyClient::new(config);
        
        // Fetch inventory levels with conditions
        match client.get_inventory_levels(if conditions.is_empty() { None } else { Some(&conditions) }).await {
            Ok(levels) => {
                self.inventory_levels = levels;
                
                // Apply sorting if requested
                if !self.sorts.is_empty() {
                    self.inventory_levels.sort_by(|a, b| {
                        for sort in &self.sorts {
                            match sort.field().as_str() {
                                "inventory_item_id" => {
                                    let ordering = a.inventory_item_id.cmp(&b.inventory_item_id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "location_id" => {
                                    let ordering = a.location_id.cmp(&b.location_id);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "available" => {
                                    let ordering = a.available.cmp(&b.available);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                "updated_at" => {
                                    let ordering = a.updated_at.cmp(&b.updated_at);
                                    if sort.reversed() {
                                        return ordering.reverse();
                                    }
                                    if ordering != std::cmp::Ordering::Equal {
                                        return ordering;
                                    }
                                }
                                _ => {}
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                
                // Apply LIMIT and OFFSET if specified
                if let Some(limit) = &self.limit {
                    let start = limit.offset() as usize;
                    let end = (limit.offset() + limit.count()) as usize;
                    
                    // Handle offset - trim the beginning of the results
                    if start < self.inventory_levels.len() {
                        self.inventory_levels = self.inventory_levels[start..].to_vec();
                    } else {
                        self.inventory_levels.clear();
                    }
                    
                    // Handle count - trim the end of the results if needed
                    if self.inventory_levels.len() > end - start {
                        self.inventory_levels.truncate(end - start);
                    }
                }
                
                // Reset position
                self.result_index = 0;
                
                Ok(())
            }
            Err(e) => Err(format!("Failed to fetch inventory levels: {}", e)),
        }
    }

    // Fetch shop information
    async fn fetch_shop(&mut self, _ctx: &Context) -> FdwResult {
        // Create Shopify API client
        let config = self.create_shopify_config();
        let mut client = api::ShopifyClient::new(config);
        
        // Fetch shop information
        match client.get_shop().await {
            Ok(shop) => {
                self.shop = Some(shop);
                self.result_index = 0;
                Ok(())
            }
            Err(e) => Err(format!("Failed to fetch shop information: {}", e)),
        }
    }
}

impl Guest for ShopifyFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.2.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // Get foreign server options
        let opts = ctx.get_options(&OptionsType::Server);

        // Get API access token from options or vault
        let api_token = match opts.get("api_token") {
            Some(token) => token,
            None => {
                let token_id = opts.require("api_token_id")?;
                utils::get_vault_secret(&token_id).unwrap_or_default()
            }
        };

        // Get required shop domain
        let shop_domain = opts.require("shop_domain")?;

        // Get API version (default to 2023-07 if not specified)
        let api_version = opts.get("api_version").unwrap_or_else(|| "2023-07".to_string());

        // Store options in the instance
        this.api_token = api_token;
        this.shop_domain = shop_domain;
        this.api_version = api_version;

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Get resource from table options
        let opts = ctx.get_options(&OptionsType::Table);
        let resource = opts.require("resource")?;

        // Reset pagination state
        this.resource = resource.clone();
        this.next_page_url = None;
        this.result_index = 0;

        // Store the sort and limit information for query pushdown
        this.sorts = ctx.get_sorts();
        this.limit = ctx.get_limit();

        // Transform quals to conditions for query pushdown
        this.conditions = this.transform_quals_to_conditions(ctx);

        // Fetch the appropriate resource data
        match resource.as_str() {
            "products" => unimplemented!("Async operations not yet supported"),
            "product_variants" => unimplemented!("Async operations not yet supported"),
            "custom_collections" => unimplemented!("Async operations not yet supported"),
            "smart_collections" => unimplemented!("Async operations not yet supported"),
            "customers" => unimplemented!("Async operations not yet supported"),
            "orders" => unimplemented!("Async operations not yet supported"),
            "inventory_items" => unimplemented!("Async operations not yet supported"),
            "inventory_levels" => unimplemented!("Async operations not yet supported"),
            "shop" => unimplemented!("Async operations not yet supported"),
            _ => Err(format!("Unsupported resource type: {}. Supported resources are 'products', 'product_variants', 'custom_collections', 'smart_collections', 'customers', 'orders', 'inventory_items', 'inventory_levels', and 'shop'.", resource))
        }
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        match this.resource.as_str() {
            "products" => {
                // If we've reached the end of our current batch of products
                if this.result_index >= this.products.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.products.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.products.len() as i64);
                    
                    // If there's a next page URL, we'd fetch the next batch in an async context
                    // For now, we'll just return None to indicate no more results
                    return Ok(None);
                }

                // Get the product from the current batch
                let product = &this.products[this.result_index];

                // Convert product to row
                this.product_to_row(product, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "product_variants" => {
                // If we've reached the end of our current batch of variants
                if this.result_index >= this.product_variants.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.product_variants.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.product_variants.len() as i64);
                    
                    return Ok(None);
                }

                // Get the variant from the current batch
                let variant = &this.product_variants[this.result_index];

                // Convert variant to row
                this.product_variant_to_row(variant, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "custom_collections" => {
                // If we've reached the end of our current batch of collections
                if this.result_index >= this.custom_collections.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.custom_collections.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.custom_collections.len() as i64);
                    
                    return Ok(None);
                }

                // Get the collection from the current batch
                let collection = &this.custom_collections[this.result_index];

                // Convert collection to row
                this.custom_collection_to_row(collection, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "smart_collections" => {
                // If we've reached the end of our current batch of collections
                if this.result_index >= this.smart_collections.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.smart_collections.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.smart_collections.len() as i64);
                    
                    return Ok(None);
                }

                // Get the collection from the current batch
                let collection = &this.smart_collections[this.result_index];

                // Convert collection to row
                this.smart_collection_to_row(collection, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "customers" => {
                // If we've reached the end of our current batch of customers
                if this.result_index >= this.customers.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.customers.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.customers.len() as i64);
                    
                    return Ok(None);
                }

                // Get the customer from the current batch
                let customer = &this.customers[this.result_index];

                // Convert customer to row
                this.customer_to_row(customer, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "orders" => {
                // If we've reached the end of our current batch of orders
                if this.result_index >= this.orders.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.orders.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.orders.len() as i64);
                    
                    return Ok(None);
                }

                // Get the order from the current batch
                let order = &this.orders[this.result_index];

                // Convert order to row
                this.order_to_row(order, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "inventory_items" => {
                // If we've reached the end of our current batch of inventory items
                if this.result_index >= this.inventory_items.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.inventory_items.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.inventory_items.len() as i64);
                    
                    return Ok(None);
                }

                // Get the inventory item from the current batch
                let item = &this.inventory_items[this.result_index];

                // Convert inventory item to row
                this.inventory_item_to_row(item, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "inventory_levels" => {
                // If we've reached the end of our current batch of inventory levels
                if this.result_index >= this.inventory_levels.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.inventory_levels.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.inventory_levels.len() as i64);
                    
                    return Ok(None);
                }

                // Get the inventory level from the current batch
                let level = &this.inventory_levels[this.result_index];

                // Convert inventory level to row
                this.inventory_level_to_row(level, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "shop" => {
                // Shop info has only one row
                if this.result_index > 0 || this.shop.is_none() {
                    return Ok(None);
                }

                // Get the shop info
                let shop = this.shop.as_ref().unwrap();

                // Convert shop info to row
                this.shop_to_row(shop, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            _ => Err(format!("Unsupported resource type: {}", this.resource)),
        }
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Reset pagination state
        this.next_page_url = None;
        this.result_index = 0;

        // Update sort and limit info in case they changed
        this.sorts = ctx.get_sorts();
        this.limit = ctx.get_limit();

        // Re-transform quals to conditions for query pushdown
        this.conditions = this.transform_quals_to_conditions(ctx);

        // Re-fetch the appropriate resource data (async functions not fully implemented yet)
        match this.resource.as_str() {
            "products" => unimplemented!("Async operations not yet supported"),
            "product_variants" => unimplemented!("Async operations not yet supported"),
            "custom_collections" => unimplemented!("Async operations not yet supported"),
            "smart_collections" => unimplemented!("Async operations not yet supported"),
            "customers" => unimplemented!("Async operations not yet supported"),
            "orders" => unimplemented!("Async operations not yet supported"),
            "inventory_items" => unimplemented!("Async operations not yet supported"),
            "inventory_levels" => unimplemented!("Async operations not yet supported"),
            "shop" => unimplemented!("Async operations not yet supported"),
            _ => Err(format!("Unsupported resource type: {}", this.resource)),
        }
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Clear cached data based on resource type
        match this.resource.as_str() {
            "products" => this.products.clear(),
            "product_variants" => this.product_variants.clear(),
            "custom_collections" => this.custom_collections.clear(),
            "smart_collections" => this.smart_collections.clear(),
            "customers" => this.customers.clear(),
            "orders" => this.orders.clear(),
            "inventory_items" => this.inventory_items.clear(),
            "inventory_levels" => this.inventory_levels.clear(),
            "shop" => this.shop = None,
            _ => {} // No action for unknown resource
        }

        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("Shopify FDW is read-only".to_string())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Err("Shopify FDW is read-only".to_string())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Err("Shopify FDW is read-only".to_string())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Err("Shopify FDW is read-only".to_string())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Err("Shopify FDW is read-only".to_string())
    }

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        let ret = vec![
            format!(
                r#"create foreign table if not exists products (
                    id bigint,
                    title text,
                    body_html text,
                    vendor text,
                    product_type text,
                    created_at timestamp,
                    updated_at timestamp,
                    published_at timestamp,
                    status text,
                    tags text,
                    variant_count integer,
                    image_url text
                )
                server {} options (
                    resource 'products'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists product_variants (
                    id bigint,
                    product_id bigint,
                    title text,
                    price text,
                    sku text,
                    position integer,
                    inventory_policy text,
                    compare_at_price text,
                    option1 text,
                    option2 text,
                    option3 text,
                    created_at timestamp,
                    updated_at timestamp,
                    taxable boolean,
                    barcode text,
                    grams integer,
                    image_id bigint,
                    weight double precision,
                    weight_unit text,
                    inventory_item_id bigint,
                    inventory_quantity integer,
                    old_inventory_quantity integer,
                    requires_shipping boolean
                )
                server {} options (
                    resource 'product_variants'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists custom_collections (
                    id bigint,
                    title text,
                    body_html text,
                    handle text,
                    updated_at timestamp,
                    published_at timestamp,
                    sort_order text,
                    template_suffix text,
                    published_scope text,
                    image_url text
                )
                server {} options (
                    resource 'custom_collections'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists smart_collections (
                    id bigint,
                    title text,
                    body_html text,
                    handle text,
                    updated_at timestamp,
                    published_at timestamp,
                    sort_order text,
                    template_suffix text,
                    published_scope text,
                    image_url text,
                    disjunctive boolean,
                    rules_count integer
                )
                server {} options (
                    resource 'smart_collections'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists customers (
                    id bigint,
                    email text,
                    accepts_marketing boolean,
                    created_at timestamp,
                    updated_at timestamp,
                    first_name text,
                    last_name text,
                    orders_count integer,
                    state text,
                    total_spent text,
                    last_order_id bigint,
                    note text,
                    verified_email boolean,
                    tax_exempt boolean,
                    phone text,
                    tags text,
                    last_order_name text,
                    currency text,
                    addresses_count integer
                )
                server {} options (
                    resource 'customers'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists orders (
                    id bigint,
                    email text,
                    closed_at timestamp,
                    created_at timestamp,
                    updated_at timestamp,
                    number integer,
                    note text,
                    token text,
                    gateway text,
                    test boolean,
                    total_price text,
                    subtotal_price text,
                    total_weight integer,
                    total_tax text,
                    taxes_included boolean,
                    currency text,
                    financial_status text,
                    confirmed boolean,
                    total_discounts text,
                    total_line_items_price text,
                    cart_token text,
                    buyer_accepts_marketing boolean,
                    name text,
                    cancelled_at timestamp,
                    cancel_reason text,
                    processed_at timestamp,
                    customer_id bigint,
                    fulfillment_status text,
                    line_items_count integer,
                    shipping_lines_count integer,
                    fulfillments_count integer,
                    refunds_count integer
                )
                server {} options (
                    resource 'orders'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists inventory_items (
                    id bigint,
                    sku text,
                    created_at timestamp,
                    updated_at timestamp,
                    requires_shipping boolean,
                    cost text,
                    country_code_of_origin text,
                    province_code_of_origin text,
                    harmonized_system_code text,
                    tracked boolean,
                    country_harmonized_system_codes_count integer
                )
                server {} options (
                    resource 'inventory_items'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists inventory_levels (
                    inventory_item_id bigint,
                    location_id bigint,
                    available integer,
                    updated_at timestamp
                )
                server {} options (
                    resource 'inventory_levels'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists shop (
                    id bigint,
                    name text,
                    email text,
                    domain text,
                    province text,
                    country text,
                    address1 text,
                    zip text,
                    city text,
                    phone text,
                    created_at timestamp,
                    updated_at timestamp,
                    country_code text,
                    country_name text,
                    currency text,
                    primary_locale text,
                    timezone text,
                    shop_owner text,
                    money_format text,
                    weight_unit text,
                    plan_display_name text,
                    has_storefront boolean
                )
                server {} options (
                    resource 'shop'
                )"#,
                stmt.server_name,
            ),
        ];
        Ok(ret)
    }
}

bindings::export!(ShopifyFdw with_types_in bindings);