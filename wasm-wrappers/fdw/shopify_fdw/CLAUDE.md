# Shopify WASM Foreign Data Wrapper - Implementation Plan

This document outlines the implementation plan for a Shopify Foreign Data Wrapper (FDW) for PostgreSQL, similar to the existing Slack FDW.

## 1. Overview

The Shopify FDW will allow querying Shopify store data directly from PostgreSQL using SQL. It will support all major Shopify entities including:

- **Products**: Store items with variants, images, and metadata
- **Collections**: Custom and smart collections of products
- **Customers**: Store customers with addresses and metadata
- **Orders**: Customer orders with line items, fulfillments, and transactions
- **Inventory**: Product inventory levels across locations
- **Metafields**: Custom metadata for various Shopify resources
- **Shop**: Store information and configuration

## 2. Implementation Plan Checklist

### Phase 1: Project Setup
- [x] Create project directory structure
- [x] Set up Cargo.toml with dependencies
- [x] Configure WebAssembly interface types (WIT)
- [x] Create documentation (README.md)
- [x] Create implementation plan (CLAUDE.md)
- [x] Create mkdocs documentation (docs/catalog/shopify.md)
- [x] Update mkdocs.yaml to include Shopify FDW

### Phase 2: Core Models
- [x] Create initial structure for models.rs with placeholder definitions
- [x] Define complete product models (Product, ProductVariant, ProductImage, etc.)
- [x] Define complete collection models (CustomCollection, SmartCollection)
- [x] Define complete customer models (Customer, CustomerAddress)
- [x] Define complete order models (Order, LineItem, Transaction, etc.)
- [x] Define complete inventory models (InventoryItem, InventoryLevel, Location)
- [x] Define complete metafield model
- [x] Define complete shop model
- [x] Implement serialization/deserialization for all models
- [x] Add helper methods for row conversions

### Phase 3: API Communication
- [x] Create initial structure for api.rs with test helpers
- [x] Implement authentication with Shopify API
- [x] Create HTTP request/response handling
- [x] Implement pagination support with Link headers
- [x] Add rate limiting handling (leaky bucket algorithm)
- [x] Implement error handling

### Phase 4: FDW Core Structure
- [x] Create initial structure for lib.rs with FDW skeleton
- [x] Create bindings.rs for WebAssembly interface
- [x] Implement host_version_requirement
- [x] Implement init method
- [x] Implement begin_scan method
- [x] Implement iter_scan method
- [x] Implement re_scan method
- [x] Implement end_scan method
- [x] Implement import_foreign_schema method

### Phase 5: Resource Methods
- [ ] Implement fetch_products method
- [ ] Implement fetch_product_variants method
- [ ] Implement fetch_collections methods
- [ ] Implement fetch_customers method
- [ ] Implement fetch_customer_addresses method
- [ ] Implement fetch_orders method
- [ ] Implement fetch_order_line_items method
- [ ] Implement fetch_inventory_items method
- [ ] Implement fetch_inventory_levels method
- [ ] Implement fetch_metafields method
- [ ] Implement fetch_shop method

### Phase 6: Row Conversion Methods
- [ ] Implement product_to_row method
- [ ] Implement product_variant_to_row method
- [ ] Implement collection_to_row methods
- [ ] Implement customer_to_row method
- [ ] Implement customer_address_to_row method
- [ ] Implement order_to_row method
- [ ] Implement order_line_item_to_row method
- [ ] Implement inventory_item_to_row method
- [ ] Implement inventory_level_to_row method
- [ ] Implement metafield_to_row method
- [ ] Implement shop_to_row method

### Phase 7: Query Pushdown & Optimization
- [ ] Add filtering support for products
- [ ] Add filtering support for customers
- [ ] Add filtering support for orders
- [ ] Add filtering support for inventory
- [ ] Add filtering support for metafields
- [ ] Implement sorting for supported resources
- [ ] Implement limit/offset handling

### Phase 8: Testing & Deployment
- [ ] Write unit tests for models
- [ ] Write unit tests for API communication
- [ ] Write integration tests with mock Shopify API
- [ ] Create sample SQL queries for documentation
- [ ] Build WebAssembly binary
- [ ] Generate checksums
- [ ] Prepare release package

## 3. Architecture

The Shopify FDW will use the same architecture as the Slack FDW:

```
+------------------------------------------+
| PostgreSQL        (SQL queries)          |
+------------------------------------------+
| Supabase Wrappers (WASM host interface)  |
+------------------------------------------+
| Shopify FDW       (Resource handling)    |
|   |-- API Layer   (Shopify API calls)    |
|   |-- Data Models (Response structures)  |
|   |-- Query Push  (Filter optimization)  |
+------------------------------------------+
| Shopify API       (REST/GraphQL)         |
+------------------------------------------+
```

## 4. Project Structure

```
shopify_fdw/
├── Cargo.toml               # Rust package configuration
├── src/
│   ├── lib.rs               # Core FDW implementation
│   ├── models.rs            # Shopify data structures
│   ├── api.rs               # Shopify API interaction
│   └── bindings.rs          # Generated WASM bindings
├── wit/
│   └── world.wit            # WebAssembly interface types
├── README.md                # Documentation
└── tests/                   # Unit and integration tests
```

## 5. Dependencies

```toml
[package]
name = "shopify_fdw"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen-rt = "0.41.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

[package.metadata.component]
package = "supabase:shopify-fdw"

[package.metadata.component.target]
path = "wit"

[package.metadata.component.target.dependencies]
"supabase:wrappers" = { path = "../../wit/v2" }

[profile.release]
lto = true
opt-level = 's'
```

## 6. WIT Interface

```wit
package supabase:shopify-fdw@0.0.1;

world shopify {
    import supabase:wrappers/http@0.2.0;
    import supabase:wrappers/jwt@0.2.0;
    import supabase:wrappers/stats@0.2.0;
    import supabase:wrappers/time@0.2.0;
    import supabase:wrappers/utils@0.2.0;
    export supabase:wrappers/routines@0.2.0;
}
```

## 7. Entity Models

All entity models will be defined in `models.rs`:

### 7.1 Products

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct Product {
    pub id: i64,
    pub title: String,
    pub body_html: Option<String>,
    pub vendor: Option<String>,
    pub product_type: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub published_at: Option<String>,
    pub status: String,
    pub tags: Option<String>,
    pub variants: Vec<ProductVariant>,
    pub options: Vec<ProductOption>,
    pub images: Vec<ProductImage>,
    pub image: Option<ProductImage>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProductVariant {
    pub id: i64,
    pub product_id: i64,
    pub title: String,
    pub price: String,
    pub sku: Option<String>,
    pub position: i32,
    pub inventory_policy: String,
    pub compare_at_price: Option<String>,
    pub fulfillment_service: String,
    pub inventory_management: Option<String>,
    pub option1: Option<String>,
    pub option2: Option<String>,
    pub option3: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub taxable: bool,
    pub barcode: Option<String>,
    pub grams: i32,
    pub image_id: Option<i64>,
    pub weight: f64,
    pub weight_unit: String,
    pub inventory_item_id: i64,
    pub inventory_quantity: i32,
    pub old_inventory_quantity: i32,
    pub requires_shipping: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProductOption {
    pub id: i64,
    pub product_id: i64,
    pub name: String,
    pub position: i32,
    pub values: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProductImage {
    pub id: i64,
    pub product_id: i64,
    pub position: i32,
    pub created_at: String,
    pub updated_at: String,
    pub alt: Option<String>,
    pub width: i32,
    pub height: i32,
    pub src: String,
    pub variant_ids: Vec<i64>,
}
```

### 7.2 Collections

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct CustomCollection {
    pub id: i64,
    pub title: String,
    pub body_html: Option<String>,
    pub handle: String,
    pub updated_at: String,
    pub published_at: Option<String>,
    pub sort_order: String,
    pub template_suffix: Option<String>,
    pub published_scope: String,
    pub image: Option<CollectionImage>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SmartCollection {
    pub id: i64,
    pub title: String,
    pub body_html: Option<String>,
    pub handle: String,
    pub updated_at: String,
    pub published_at: Option<String>,
    pub sort_order: String,
    pub template_suffix: Option<String>,
    pub published_scope: String,
    pub image: Option<CollectionImage>,
    pub rules: Vec<CollectionRule>,
    pub disjunctive: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CollectionImage {
    pub created_at: String,
    pub alt: Option<String>,
    pub width: i32,
    pub height: i32,
    pub src: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CollectionRule {
    pub column: String,
    pub relation: String,
    pub condition: String,
}
```

### 7.3 Customers

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct Customer {
    pub id: i64,
    pub email: Option<String>,
    pub accepts_marketing: bool,
    pub created_at: String,
    pub updated_at: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub orders_count: i32,
    pub state: String,
    pub total_spent: String,
    pub last_order_id: Option<i64>,
    pub note: Option<String>,
    pub verified_email: bool,
    pub multipass_identifier: Option<String>,
    pub tax_exempt: bool,
    pub phone: Option<String>,
    pub tags: String,
    pub last_order_name: Option<String>,
    pub currency: String,
    pub addresses: Vec<CustomerAddress>,
    pub default_address: Option<CustomerAddress>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CustomerAddress {
    pub id: i64,
    pub customer_id: i64,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub company: Option<String>,
    pub address1: Option<String>,
    pub address2: Option<String>,
    pub city: Option<String>,
    pub province: Option<String>,
    pub country: Option<String>,
    pub zip: Option<String>,
    pub phone: Option<String>,
    pub name: Option<String>,
    pub province_code: Option<String>,
    pub country_code: Option<String>,
    pub country_name: Option<String>,
    pub default: bool,
}
```

### 7.4 Orders

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct Order {
    pub id: i64,
    pub email: Option<String>,
    pub closed_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub number: i32,
    pub note: Option<String>,
    pub token: String,
    pub gateway: Option<String>,
    pub test: bool,
    pub total_price: String,
    pub subtotal_price: String,
    pub total_weight: i32,
    pub total_tax: String,
    pub taxes_included: bool,
    pub currency: String,
    pub financial_status: String,
    pub confirmed: bool,
    pub total_discounts: String,
    pub total_line_items_price: String,
    pub cart_token: Option<String>,
    pub buyer_accepts_marketing: bool,
    pub name: String,
    pub referring_site: Option<String>,
    pub landing_site: Option<String>,
    pub cancelled_at: Option<String>,
    pub cancel_reason: Option<String>,
    pub total_price_usd: Option<String>,
    pub checkout_token: Option<String>,
    pub reference: Option<String>,
    pub user_id: Option<i64>,
    pub location_id: Option<i64>,
    pub source_identifier: Option<String>,
    pub source_url: Option<String>,
    pub processed_at: String,
    pub device_id: Option<i64>,
    pub phone: Option<String>,
    pub customer_locale: Option<String>,
    pub app_id: Option<i64>,
    pub browser_ip: Option<String>,
    pub landing_site_ref: Option<String>,
    pub order_number: String,
    pub discount_applications: Vec<DiscountApplication>,
    pub discount_codes: Vec<DiscountCode>,
    pub note_attributes: Vec<NoteAttribute>,
    pub payment_gateway_names: Vec<String>,
    pub processing_method: String,
    pub checkout_id: Option<i64>,
    pub source_name: String,
    pub fulfillment_status: Option<String>,
    pub tax_lines: Vec<TaxLine>,
    pub tags: String,
    pub contact_email: Option<String>,
    pub order_status_url: String,
    pub presentment_currency: String,
    pub total_line_items_price_set: PriceSet,
    pub total_discounts_set: PriceSet,
    pub total_shipping_price_set: PriceSet,
    pub subtotal_price_set: PriceSet,
    pub total_price_set: PriceSet,
    pub total_tax_set: PriceSet,
    pub line_items: Vec<LineItem>,
    pub shipping_lines: Vec<ShippingLine>,
    pub billing_address: Option<Address>,
    pub shipping_address: Option<Address>,
    pub fulfillments: Vec<Fulfillment>,
    pub refunds: Vec<Refund>,
    pub customer: Option<Customer>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LineItem {
    pub id: i64,
    pub variant_id: Option<i64>,
    pub title: String,
    pub quantity: i32,
    pub sku: Option<String>,
    pub variant_title: Option<String>,
    pub vendor: Option<String>,
    pub fulfillment_service: String,
    pub product_id: Option<i64>,
    pub requires_shipping: bool,
    pub taxable: bool,
    pub gift_card: bool,
    pub name: String,
    pub variant_inventory_management: Option<String>,
    pub properties: Vec<LineItemProperty>,
    pub product_exists: bool,
    pub fulfillable_quantity: i32,
    pub grams: i32,
    pub price: String,
    pub total_discount: String,
    pub fulfillment_status: Option<String>,
    pub price_set: PriceSet,
    pub total_discount_set: PriceSet,
    pub discount_allocations: Vec<DiscountAllocation>,
    pub duties: Vec<Duty>,
    pub tax_lines: Vec<TaxLine>,
    pub origin_location: Option<Location>,
}

// ... Other related structs for Orders
```

### 7.5 Inventory

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct InventoryItem {
    pub id: i64,
    pub sku: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub requires_shipping: bool,
    pub cost: Option<String>,
    pub country_code_of_origin: Option<String>,
    pub province_code_of_origin: Option<String>,
    pub harmonized_system_code: Option<String>,
    pub tracked: bool,
    pub country_harmonized_system_codes: Vec<CountryHarmonizedSystemCode>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct InventoryLevel {
    pub inventory_item_id: i64,
    pub location_id: i64,
    pub available: i32,
    pub updated_at: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Location {
    pub id: i64,
    pub name: String,
    pub address1: Option<String>,
    pub address2: Option<String>,
    pub city: Option<String>,
    pub zip: Option<String>,
    pub province: Option<String>,
    pub country: Option<String>,
    pub phone: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub country_code: Option<String>,
    pub country_name: Option<String>,
    pub province_code: Option<String>,
    pub legacy: bool,
    pub active: bool,
    pub admin_graphql_api_id: String,
}
```

### 7.6 Shop

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct Shop {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub domain: String,
    pub province: Option<String>,
    pub country: String,
    pub address1: Option<String>,
    pub zip: Option<String>,
    pub city: Option<String>,
    pub source: Option<String>,
    pub phone: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub primary_locale: String,
    pub address2: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub country_code: String,
    pub country_name: String,
    pub currency: String,
    pub customer_email: Option<String>,
    pub timezone: String,
    pub iana_timezone: String,
    pub shop_owner: String,
    pub money_format: String,
    pub money_with_currency_format: String,
    pub weight_unit: String,
    pub province_code: Option<String>,
    pub taxes_included: Option<bool>,
    pub tax_shipping: Option<bool>,
    pub county_taxes: bool,
    pub plan_display_name: String,
    pub plan_name: String,
    pub has_discounts: bool,
    pub has_gift_cards: bool,
    pub myshopify_domain: String,
    pub google_apps_domain: Option<String>,
    pub google_apps_login_enabled: Option<bool>,
    pub money_in_emails_format: String,
    pub money_with_currency_in_emails_format: String,
    pub eligible_for_payments: bool,
    pub requires_extra_payments_agreement: bool,
    pub password_enabled: bool,
    pub has_storefront: bool,
    pub eligible_for_card_reader_giveaway: bool,
    pub finances: bool,
    pub primary_location_id: i64,
    pub checkout_api_supported: bool,
    pub multi_location_enabled: bool,
    pub setup_required: bool,
    pub pre_launch_enabled: bool,
    pub enabled_presentment_currencies: Vec<String>,
}
```

### 7.7 Metafields

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct Metafield {
    pub id: i64,
    pub namespace: String,
    pub key: String,
    pub value: String,
    pub description: Option<String>,
    pub owner_id: i64,
    pub created_at: String,
    pub updated_at: String,
    pub owner_resource: String,
    pub value_type: String,
}
```

## 8. Core Implementation

The core implementation will follow a similar structure to the Slack FDW:

### 8.1 Main FDW Structure

```rust
#[derive(Debug, Default)]
struct ShopifyFdw {
    // Connection state
    api_key: String,
    access_token: String,
    shop_domain: String,
    headers: Vec<(String, String)>,
    api_version: String,

    // Request state for pagination
    resource: String,
    has_more: bool,
    next_page_token: Option<String>,
    
    // Cache for API responses
    products: Vec<Product>,
    product_variants: Vec<ProductVariant>,
    product_images: Vec<ProductImage>,
    custom_collections: Vec<CustomCollection>,
    smart_collections: Vec<SmartCollection>,
    customers: Vec<Customer>,
    customer_addresses: Vec<CustomerAddress>,
    orders: Vec<Order>,
    order_line_items: Vec<LineItem>,
    inventory_items: Vec<InventoryItem>,
    inventory_levels: Vec<InventoryLevel>,
    locations: Vec<Location>,
    metafields: Vec<Metafield>,
    shop: Option<Shop>,

    // Current position in the result set for iteration
    result_index: usize,

    // Query pushdown support
    sorts: Vec<Sort>,
    limit: Option<Limit>,
}
```

### 8.2 API Communication Methods

```rust
// Create a request for the Shopify API
fn create_request(
    &self,
    endpoint: &str,
    params: &[(String, String)],
) -> Result<http::Request, FdwError> {
    let mut url = format!(
        "https://{}/admin/api/{}/{}",
        self.shop_domain, self.api_version, endpoint
    );

    // Add query parameters if any
    if !params.is_empty() {
        let query_string = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<String>>()
            .join("&");
        url = format!("{}?{}", url, query_string);
    }

    Ok(http::Request {
        method: http::Method::Get,
        url,
        headers: self.headers.clone(),
        body: String::default(),
    })
}

// Make a request to the Shopify API with retries for rate limiting
fn make_request(&self, req: &http::Request) -> Result<JsonValue, FdwError> {
    loop {
        let resp = match req.method {
            http::Method::Get => http::get(req)?,
            _ => unreachable!("invalid request method"),
        };

        // Handle rate limiting
        if resp.status_code == 429 {
            if let Some(retry) = resp.headers.iter().find(|h| h.0 == "retry-after") {
                let delay_secs = retry.1.parse::<u64>().map_err(|e| e.to_string())?;
                time::sleep(delay_secs * 1000);
                continue;
            }
        }

        // Check for errors
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

        // Transform response to JSON
        let resp_json: JsonValue =
            serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

        return Ok(resp_json);
    }
}
```

### 8.3 Resource Fetching Methods

One example method for fetching products:

```rust
// Fetch products
fn fetch_products(&mut self, ctx: &Context) -> FdwResult {
    // Create request parameters
    let mut params = Vec::new();

    // Push down WHERE filters if possible
    let quals = ctx.get_quals();
    if !quals.is_empty() {
        for qual in quals.iter() {
            if qual.operator().as_str() == "=" && !qual.use_or() {
                match qual.field().as_str() {
                    "id" => {
                        if let Value::Cell(Cell::I64(id)) = qual.value() {
                            params.push(("ids".to_string(), id.to_string()));
                        }
                    }
                    "title" => {
                        if let Value::Cell(Cell::String(title)) = qual.value() {
                            params.push(("title".to_string(), title.clone()));
                        }
                    }
                    "vendor" => {
                        if let Value::Cell(Cell::String(vendor)) = qual.value() {
                            params.push(("vendor".to_string(), vendor.clone()));
                        }
                    }
                    "product_type" => {
                        if let Value::Cell(Cell::String(product_type)) = qual.value() {
                            params.push(("product_type".to_string(), product_type.clone()));
                        }
                    }
                    "created_at_min" => {
                        if let Value::Cell(Cell::String(created_at_min)) = qual.value() {
                            params.push(("created_at_min".to_string(), created_at_min.clone()));
                        }
                    }
                    "created_at_max" => {
                        if let Value::Cell(Cell::String(created_at_max)) = qual.value() {
                            params.push(("created_at_max".to_string(), created_at_max.clone()));
                        }
                    }
                    "updated_at_min" => {
                        if let Value::Cell(Cell::String(updated_at_min)) = qual.value() {
                            params.push(("updated_at_min".to_string(), updated_at_min.clone()));
                        }
                    }
                    "updated_at_max" => {
                        if let Value::Cell(Cell::String(updated_at_max)) = qual.value() {
                            params.push(("updated_at_max".to_string(), updated_at_max.clone()));
                        }
                    }
                    "published_status" => {
                        if let Value::Cell(Cell::String(status)) = qual.value() {
                            params.push(("published_status".to_string(), status.clone()));
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // Add pagination parameters
    if let Some(page_token) = &self.next_page_token {
        params.push(("page_info".to_string(), page_token.clone()));
    }

    // Add limit parameter
    if let Some(limit) = &self.limit {
        params.push(("limit".to_string(), limit.count().to_string()));
    } else {
        params.push(("limit".to_string(), "50".to_string())); // Default limit
    }

    // Create request and send it
    let req = self.create_request("products.json", &params)?;
    let resp_json = self.make_request(&req)?;

    // Extract products
    if let Some(products) = resp_json.get("products").and_then(|p| p.as_array()) {
        // Convert JSON products to our model
        let mut new_products = Vec::new();
        for product_json in products {
            if let Ok(product) = serde_json::from_value(product_json.clone()) {
                new_products.push(product);
            }
        }

        self.products = new_products;

        // Get pagination info from Link header
        if let Some(link) = resp_json.get("links") {
            if let Some(next) = link.get("next") {
                if let Some(next_url) = next.as_str() {
                    // Parse next_url to extract page_info
                    if let Some(page_info) = self.extract_page_info(next_url) {
                        self.next_page_token = Some(page_info);
                        self.has_more = true;
                    } else {
                        self.next_page_token = None;
                        self.has_more = false;
                    }
                } else {
                    self.next_page_token = None;
                    self.has_more = false;
                }
            } else {
                self.next_page_token = None;
                self.has_more = false;
            }
        } else {
            self.next_page_token = None;
            self.has_more = false;
        }

        // Apply sorting if requested
        if !self.sorts.is_empty() {
            self.products.sort_by(|a, b| {
                for sort in &self.sorts {
                    match sort.field().as_str() {
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
                        _ => {}
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Reset position
        self.result_index = 0;

        Ok(())
    } else {
        Err("Failed to parse products from response".to_string())
    }
}

// Helper to extract page_info from a Link URL
fn extract_page_info(&self, url: &str) -> Option<String> {
    if let Some(query_start) = url.find('?') {
        let query_str = &url[query_start + 1..];
        for pair in query_str.split('&') {
            if let Some(index) = pair.find('=') {
                let key = &pair[0..index];
                let value = &pair[index + 1..];
                if key == "page_info" {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}
```

### 8.4 Row Conversion Methods

For each entity type, we need methods to convert to PostgreSQL rows:

```rust
// Map Shopify Product to PostgreSQL Row
fn product_to_row(&self, product: &Product, row: &Row) -> Result<(), FdwError> {
    row.push(Some(&Cell::I64(product.id)));
    row.push(Some(&Cell::String(product.title.clone())));
    
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
    
    row.push(Some(&Cell::String(product.created_at.clone())));
    row.push(Some(&Cell::String(product.updated_at.clone())));
    
    if let Some(published_at) = &product.published_at {
        row.push(Some(&Cell::String(published_at.clone())));
    } else {
        row.push(None);
    }
    
    row.push(Some(&Cell::String(product.status.clone())));
    
    if let Some(tags) = &product.tags {
        row.push(Some(&Cell::String(tags.clone())));
    } else {
        row.push(None);
    }
    
    Ok(())
}
```

### 8.5 Required FDW API Implementation

Implement all required methods for the Foreign Data Wrapper interface:

```rust
impl Guest for ShopifyFdw {
    fn host_version_requirement() -> String {
        "^0.2.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // Get foreign server options
        let opts = ctx.get_options(&OptionsType::Server);

        // Get API credentials from options or vault
        let api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id).unwrap_or_default()
            }
        };

        let access_token = match opts.get("access_token") {
            Some(token) => token,
            None => {
                let token_id = opts.require("access_token_id")?;
                utils::get_vault_secret(&token_id).unwrap_or_default()
            }
        };

        // Get required shop domain
        let shop_domain = opts.require("shop_domain")?;
        
        // Get optional API version, default to latest
        let api_version = opts.get("api_version").unwrap_or_else(|| "2023-10".to_string());

        // Set up authorization headers
        this.headers.push(("X-Shopify-Access-Token".to_owned(), access_token.clone()));
        this.headers.push(("Content-Type".to_owned(), "application/json".to_string()));
        this.headers.push(("User-Agent".to_owned(), "Supabase Shopify FDW".to_string()));

        // Store options in the instance
        this.api_key = api_key;
        this.access_token = access_token;
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
        this.has_more = false;
        this.next_page_token = None;
        this.result_index = 0;

        // Store the sort and limit information for query pushdown
        this.sorts = ctx.get_sorts();
        this.limit = ctx.get_limit();

        // Fetch the appropriate resource data
        match resource.as_str() {
            "products" => this.fetch_products(ctx),
            "product_variants" => this.fetch_product_variants(ctx),
            "product_images" => this.fetch_product_images(ctx),
            "custom_collections" => this.fetch_custom_collections(ctx),
            "smart_collections" => this.fetch_smart_collections(ctx),
            "customers" => this.fetch_customers(ctx),
            "customer_addresses" => this.fetch_customer_addresses(ctx),
            "orders" => this.fetch_orders(ctx),
            "order_line_items" => this.fetch_order_line_items(ctx),
            "inventory_items" => this.fetch_inventory_items(ctx),
            "inventory_levels" => this.fetch_inventory_levels(ctx),
            "locations" => this.fetch_locations(ctx),
            "metafields" => this.fetch_metafields(ctx),
            "shop" => this.fetch_shop(ctx),
            _ => Err(format!("Unsupported resource type: {}. Supported resources are 'products', 'product_variants', 'product_images', 'custom_collections', 'smart_collections', 'customers', 'customer_addresses', 'orders', 'order_line_items', 'inventory_items', 'inventory_levels', 'locations', 'metafields', and 'shop'.", resource))
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

                    // If there's a next page and we don't have a limit or haven't reached it yet, fetch the next batch
                    if this.has_more {
                        // If we have a limit, check if we've already reached it
                        if let Some(limit) = &this.limit {
                            if this.products.len() >= limit.count() as usize {
                                // We've already met our limit, don't fetch more
                                return Ok(None);
                            }
                        }

                        this.fetch_products(ctx)?;

                        // If the new batch is empty, we're done
                        if this.products.is_empty() {
                            return Ok(None);
                        }
                    } else {
                        // No more results
                        return Ok(None);
                    }
                }

                // Get the product from the current batch
                let product = &this.products[this.result_index];

                // Convert product to row
                this.product_to_row(product, row)?;

                this.result_index += 1;
                Ok(Some(0))
            },
            // ... Implement for other resource types
            _ => Err(format!("Unsupported resource type: {}", this.resource)),
        }
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Reset pagination state
        this.has_more = false;
        this.next_page_token = None;
        this.result_index = 0;

        // Update sort and limit info in case they changed
        this.sorts = ctx.get_sorts();
        this.limit = ctx.get_limit();

        // Re-fetch the appropriate resource data
        match this.resource.as_str() {
            "products" => this.fetch_products(ctx),
            "product_variants" => this.fetch_product_variants(ctx),
            "product_images" => this.fetch_product_images(ctx),
            "custom_collections" => this.fetch_custom_collections(ctx),
            "smart_collections" => this.fetch_smart_collections(ctx),
            "customers" => this.fetch_customers(ctx),
            "customer_addresses" => this.fetch_customer_addresses(ctx),
            "orders" => this.fetch_orders(ctx),
            "order_line_items" => this.fetch_order_line_items(ctx),
            "inventory_items" => this.fetch_inventory_items(ctx),
            "inventory_levels" => this.fetch_inventory_levels(ctx),
            "locations" => this.fetch_locations(ctx),
            "metafields" => this.fetch_metafields(ctx),
            "shop" => this.fetch_shop(ctx),
            _ => Err(format!("Unsupported resource type: {}", this.resource)),
        }
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Clear cached data based on resource type
        match this.resource.as_str() {
            "products" => this.products.clear(),
            "product_variants" => this.product_variants.clear(),
            "product_images" => this.product_images.clear(),
            "custom_collections" => this.custom_collections.clear(),
            "smart_collections" => this.smart_collections.clear(),
            "customers" => this.customers.clear(),
            "customer_addresses" => this.customer_addresses.clear(),
            "orders" => this.orders.clear(),
            "order_line_items" => this.order_line_items.clear(),
            "inventory_items" => this.inventory_items.clear(),
            "inventory_levels" => this.inventory_levels.clear(),
            "locations" => this.locations.clear(),
            "metafields" => this.metafields.clear(),
            "shop" => this.shop = None,
            _ => {} // No action for unknown resource
        }

        Ok(())
    }

    // Implement CRUD methods - Shopify FDW will be read-only initially, 
    // but could support INSERT/UPDATE/DELETE in the future for some entities

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
}
```

## 9. Key Differences from Slack FDW

While the overall architecture is similar to the Slack FDW, the Shopify implementation has some key differences:

1. **Authentication**: Shopify uses API key + access token authentication rather than OAuth tokens

2. **API Structure**: Shopify's API follows RESTful conventions with predictable endpoints

3. **Pagination**: Shopify uses Link headers and page_info cursors rather than next_cursor fields

4. **Resource Hierarchy**: Shopify has more nested resources (products contain variants, orders contain line items)

5. **Rate Limiting**: Shopify uses a "leaky bucket" algorithm with different headers

6. **Data Types**: Shopify has more standardized data types with ISO-8601 dates and consistent IDs

7. **Admin vs Storefront**: This implementation uses the Admin API only, as it provides more data access

## 10. Handling Shopify API Rate Limits

Shopify enforces rate limits with a "leaky bucket" algorithm. The FDW will:

1. Read rate limit headers from responses: `X-Shopify-Shop-Api-Call-Limit` shows current API usage (e.g., "39/40")
2. Implement automatic retry with backoff for rate-limited requests
3. Track API usage and throttle requests when nearing limits
4. Use bulk endpoints where available for more efficient fetching

## 11. Future Enhancements

1. **Write Support**: Add INSERT/UPDATE/DELETE for supported endpoints

2. **GraphQL Support**: Add GraphQL API support for more efficient queries

3. **Webhooks Integration**: Add support for real-time data updates via webhooks

4. **Caching Layer**: Add caching for frequently accessed data

5. **Multi-Store Support**: Allow connecting to multiple Shopify stores

6. **Analytics Tables**: Add specialized tables for analytics queries

7. **Type Conversions**: Improve handling of dates, money values, and other specific types

8. **Filtering Improvements**: Add support for more complex filtering operations