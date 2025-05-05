# Shopify WASM Foreign Data Wrapper - Implementation Status

This document outlines the implementation status for the Shopify Foreign Data Wrapper (FDW) for PostgreSQL.

## Current Version: 0.3.0

The Shopify FDW has been refactored to match the Slack FDW implementation pattern, which is the recommended approach for WASM foreign data wrappers in the Supabase Wrappers ecosystem.

## 1. Overview

The Shopify FDW allows querying Shopify store data directly from PostgreSQL using SQL. It supports the following entities:

- **Products**: Store items with variants, images, and metadata
- **Product Variants**: Variations of products with different options, pricing, and inventory

Additional entities planned for future implementation:
- **Collections**: Custom and smart collections of products
- **Customers**: Store customers with addresses and metadata
- **Orders**: Customer orders with line items, fulfillments, and transactions
- **Inventory**: Product inventory levels across locations
- **Metafields**: Custom metadata for various Shopify resources
- **Shop**: Store information and configuration

## 2. Implementation Status Checklist

### Phase 1: Project Setup ✅
- [x] Create project directory structure
- [x] Set up Cargo.toml with dependencies
- [x] Configure WebAssembly interface types (WIT)
- [x] Create documentation (README.md)
- [x] Create implementation plan (CLAUDE.md)
- [x] Create mkdocs documentation (docs/catalog/shopify.md)
- [x] Update mkdocs.yaml to include Shopify FDW

### Phase 2: Core Models ✅
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

### Phase 3: Core Architecture ✅
- [x] Refactor to match Slack FDW pattern 
- [x] Implement singleton pattern with static instance
- [x] Integrate API client directly into FDW struct
- [x] Implement WebAssembly exports with proper bindings

### Phase 4: API Communication ✅
- [x] Create initial structure for API communication
- [x] Implement authentication with Shopify API
- [x] Create HTTP request/response handling
- [x] Implement pagination support with Link headers
- [x] Add rate limiting handling (leaky bucket algorithm)
- [x] Implement error handling

### Phase 5: FDW Core Structure ✅
- [x] Create initial structure for lib.rs with FDW skeleton
- [x] Create bindings.rs for WebAssembly interface
- [x] Implement host_version_requirement
- [x] Implement init method
- [x] Implement begin_scan method
- [x] Implement iter_scan method
- [x] Implement re_scan method
- [x] Implement end_scan method
- [x] Implement import_foreign_schema method

### Phase 6: Resource Methods ✅
- [x] Implement fetch_products method
- [x] Implement fetch_product_variants method
- [ ] Implement fetch_collections methods
- [ ] Implement fetch_customers method
- [ ] Implement fetch_customer_addresses method
- [ ] Implement fetch_orders method
- [ ] Implement fetch_order_line_items method
- [ ] Implement fetch_inventory_items method
- [ ] Implement fetch_inventory_levels method
- [ ] Implement fetch_metafields method
- [ ] Implement fetch_shop method

### Phase 7: Row Conversion Methods ✅
- [x] Implement product_to_row method
- [x] Implement product_variant_to_row method
- [ ] Implement collection_to_row methods
- [ ] Implement customer_to_row method
- [ ] Implement customer_address_to_row method
- [ ] Implement order_to_row method
- [ ] Implement order_line_item_to_row method
- [ ] Implement inventory_item_to_row method
- [ ] Implement inventory_level_to_row method
- [ ] Implement metafield_to_row method
- [ ] Implement shop_to_row method

### Phase 8: Query Pushdown & Optimization ✅
- [x] Add filtering support for products
- [x] Add filtering support for product variants
- [ ] Add filtering support for customers
- [ ] Add filtering support for orders
- [ ] Add filtering support for inventory
- [ ] Add filtering support for metafields
- [x] Implement sorting for supported resources
- [x] Implement limit/offset handling

### Phase 9: Testing & Deployment ✅
- [x] Write unit tests for models
- [x] Write unit tests for API communication
- [x] Build WebAssembly component
- [x] Bump version to 0.3.0
- [x] Update documentation with new version

## 3. Key Implementation Details

1. **Singleton Pattern**
   - Uses a static mutable pointer to the instance `static mut INSTANCE: *mut ShopifyFdw = std::ptr::null_mut::<ShopifyFdw>()`
   - Initialized with `Box::leak` to ensure memory remains valid for the lifetime of the program
   - Accessed via `this_mut()` to get a mutable reference to the singleton

2. **API Client Integration**
   - API client functionality has been integrated directly into the ShopifyFdw struct
   - Methods like `create_request`, `make_request`, etc. are now methods on ShopifyFdw
   - Pagination is handled through the `next_page_token` field

3. **Model Structure**
   - Separate model definitions in `models.rs` for all Shopify entities
   - Strong type definitions with appropriate handling of optional fields

4. **Request/Response Flow**
   - Authentication via API token in request headers
   - Rate limiting handled by checking response headers and implementing retry logic
   - Pagination via Link header parsing

5. **Query Pushdown**
   - Filters pushed down to the Shopify API where supported
   - Sorting applied in memory for fields not supported by the API

## 4. Build Instructions

To build the WASM component:

```bash
cargo component build --release
```

## 5. Future Improvements

1. Implement remaining resource types:
   - collections
   - customers
   - orders
   - inventory
   - metafields
   - shop information

2. Add more query pushdown optimizations

3. Improve error handling and rate limit management

4. Implement caching for frequently accessed data

5. Add support for write operations (where applicable)

6. Add bulk data fetching optimizations

