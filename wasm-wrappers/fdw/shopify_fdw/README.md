# Shopify WASM Foreign Data Wrapper

This is a WASM-based Foreign Data Wrapper (FDW) for integrating Shopify store data into PostgreSQL through Supabase Wrappers.

## Overview

The Shopify FDW allows querying Shopify store data directly from PostgreSQL using SQL. It supports all major Shopify entities:

- **Products**: Store items with variants, images, and metadata
- **Collections**: Custom and smart collections of products
- **Customers**: Store customers with addresses and metadata
- **Orders**: Customer orders with line items, fulfillments, and transactions
- **Inventory**: Product inventory levels across locations
- **Metafields**: Custom metadata for various Shopify resources
- **Shop**: Store information and configuration

## Architecture

This FDW is implemented in Rust and compiled to WebAssembly (WASM) to run within PostgreSQL using Supabase Wrappers. It follows a layered architecture:

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

## Project Structure

- **src/lib.rs**: Core FDW implementation with control flow
- **src/models.rs**: Data structures for Shopify API responses
- **src/api.rs**: Shopify API interaction utilities
- **wit/world.wit**: WebAssembly interface definitions
- **Cargo.toml**: Rust dependencies and build configuration

## Implementation Details

### Entity Models

All entity models are defined in `models.rs`:

1. **Products**:
   - `Product` struct with variants, options, images
   - Mapping to PostgreSQL via `product_to_row()`
   - Support for filtering by various attributes

2. **Product Variants**:
   - `ProductVariant` struct with SKU, pricing, inventory information
   - Mapping via `product_variant_to_row()`
   - Tied to parent product via product_id

3. **Collections**:
   - `CustomCollection` and `SmartCollection` structs
   - Support for rules-based and manual collections
   - Mapping via respective row conversion functions

4. **Customers**:
   - `Customer` struct with order history and contact information
   - `CustomerAddress` for shipping/billing addresses
   - Support for filtering and pagination

5. **Orders**:
   - `Order` struct with complex nested structure
   - Related entities: LineItems, Fulfillments, Transactions
   - Filtering by date range, status, customer

6. **Inventory**:
   - `InventoryItem` and `InventoryLevel` structs
   - `Location` for physical and virtual inventory locations
   - Cross-referenced with products and variants

7. **Shop**:
   - `Shop` struct with store configuration and metadata
   - Single-row resource with store details

### Data Flow

1. **Request Handling**:
   - SQL queries map to resource types via table options
   - WHERE clauses analyzed for query pushdown opportunities
   - PostgreSQL types mapped to Shopify API parameters

2. **API Communication**:
   - Authenticated requests using API key and access token
   - Rate limiting and error handling with leaky bucket algorithm
   - Pagination via Link headers and page_info cursors

3. **Result Processing**:
   - Efficient pagination handling for large result sets
   - Type conversion between Shopify and PostgreSQL
   - Null handling for optional fields

### Query Optimization

The FDW implements query pushdown for:

| Resource          | Supported Filters                   | Sorting                | Limit/Offset |
|-------------------|-------------------------------------|------------------------|--------------|
| products          | id, title, vendor, product_type, created_at_min/max, updated_at_min/max, published_status | title, created_at, updated_at | Yes |
| product_variants  | product_id, sku                     | title, price           | Yes |
| customers         | id, email, created_at_min/max, updated_at_min/max | email, created_at   | Yes |
| orders            | id, customer_id, financial_status, fulfillment_status, created_at_min/max | created_at, updated_at | Yes |
| inventory_items   | ids                                 | None                   | Yes |
| inventory_levels  | inventory_item_ids, location_ids    | None                   | Yes |
| metafields        | owner_id, owner_resource, namespace, key | None              | Yes |
| shop              | *(no filter support - single row)*  | N/A                    | N/A |

## Usage

### Server Setup

```sql
CREATE SERVER shopify_server
FOREIGN DATA WRAPPER wasm_wrapper
OPTIONS (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_shopify_fdw_v0.0.1/shopify_fdw.wasm',
    fdw_package_name 'supabase:shopify-fdw',
    fdw_package_version '0.0.1',
    fdw_package_checksum '(checksum will be generated on release)',
    api_key 'your-shopify-api-key',
    access_token 'your-shopify-access-token',
    shop_domain 'your-store.myshopify.com',
    api_version '2023-10'
);
```

### Table Definitions

```sql
-- Products table
CREATE FOREIGN TABLE shopify.products (
  id bigint,
  title text,
  body_html text,
  vendor text,
  product_type text,
  created_at text,
  updated_at text,
  published_at text,
  status text,
  tags text
)
SERVER shopify_server
OPTIONS (
  resource 'products'
);

-- Product variants table
CREATE FOREIGN TABLE shopify.product_variants (
  id bigint,
  product_id bigint,
  title text,
  price text,
  sku text,
  position integer,
  inventory_policy text,
  compare_at_price text,
  fulfillment_service text,
  inventory_management text,
  option1 text,
  option2 text,
  option3 text,
  created_at text,
  updated_at text,
  taxable boolean,
  barcode text,
  grams integer,
  image_id bigint,
  weight numeric,
  weight_unit text,
  inventory_item_id bigint,
  inventory_quantity integer,
  old_inventory_quantity integer,
  requires_shipping boolean
)
SERVER shopify_server
OPTIONS (
  resource 'product_variants'
);

-- Customers table
CREATE FOREIGN TABLE shopify.customers (
  id bigint,
  email text,
  accepts_marketing boolean,
  created_at text,
  updated_at text,
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
  currency text
)
SERVER shopify_server
OPTIONS (
  resource 'customers'
);

-- Orders table
CREATE FOREIGN TABLE shopify.orders (
  id bigint,
  email text,
  closed_at text,
  created_at text,
  updated_at text,
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
  referring_site text,
  landing_site text,
  cancelled_at text,
  cancel_reason text,
  processed_at text,
  customer_id bigint,
  user_id bigint,
  location_id bigint,
  source_identifier text,
  source_url text,
  device_id bigint,
  phone text,
  customer_locale text,
  app_id bigint,
  browser_ip text,
  landing_site_ref text,
  order_number text,
  processing_method text,
  checkout_id bigint,
  source_name text,
  fulfillment_status text,
  tags text,
  contact_email text,
  order_status_url text,
  presentment_currency text
)
SERVER shopify_server
OPTIONS (
  resource 'orders'
);

-- Shop information table
CREATE FOREIGN TABLE shopify.shop (
  id bigint,
  name text,
  email text,
  domain text,
  province text,
  country text,
  address1 text,
  zip text,
  city text,
  source text,
  phone text,
  latitude numeric,
  longitude numeric,
  primary_locale text,
  address2 text,
  created_at text,
  updated_at text,
  country_code text,
  country_name text,
  currency text,
  customer_email text,
  timezone text,
  iana_timezone text,
  shop_owner text,
  money_format text,
  money_with_currency_format text,
  weight_unit text,
  province_code text,
  taxes_included boolean,
  tax_shipping boolean,
  county_taxes boolean,
  plan_display_name text,
  plan_name text,
  has_discounts boolean,
  has_gift_cards boolean,
  myshopify_domain text,
  google_apps_domain text,
  google_apps_login_enabled boolean,
  money_in_emails_format text,
  money_with_currency_in_emails_format text,
  eligible_for_payments boolean,
  requires_extra_payments_agreement boolean,
  password_enabled boolean,
  has_storefront boolean,
  eligible_for_card_reader_giveaway boolean,
  finances boolean,
  primary_location_id bigint,
  checkout_api_supported boolean,
  multi_location_enabled boolean,
  setup_required boolean,
  pre_launch_enabled boolean
)
SERVER shopify_server
OPTIONS (
  resource 'shop'
);
```

### Query Examples

```sql
-- Get all products
SELECT * FROM shopify.products;

-- Get variants for a specific product
SELECT * FROM shopify.product_variants WHERE product_id = 12345678;

-- Get customers with email addresses from a specific domain
SELECT * FROM shopify.customers WHERE email LIKE '%@example.com';

-- Get orders with a specific status
SELECT * FROM shopify.orders WHERE financial_status = 'paid';

-- Get store information
SELECT * FROM shopify.shop;
```

## Configuration Options

### Server Options

| Option | Description | Required |
|--------|-------------|----------|
| api_key | Shopify API key | Yes |
| api_key_id | Vault key ID for API key (alternative to api_key) | Yes (if api_key not provided) |
| access_token | Shopify access token | Yes |
| access_token_id | Vault key ID for access token (alternative to access_token) | Yes (if access_token not provided) |
| shop_domain | Shopify store domain (e.g., 'your-store.myshopify.com') | Yes |
| api_version | Shopify API version (e.g., '2023-10') | No (defaults to latest) |

### Table Options

| Option | Description | Required |
|--------|-------------|----------|
| resource | Resource type to query (products, customers, orders, etc.) | Yes |

## Shopify API Rate Limits

This FDW implements rate limiting handling according to Shopify's leaky bucket algorithm:

- Reads rate limit headers from responses (X-Shopify-Shop-Api-Call-Limit)
- Implements automatic retry with backoff for rate-limited requests
- Tracks API usage and throttles requests when nearing limits
- Uses bulk endpoints where available for more efficient fetching

## Required Scopes for Shopify API Access

The following OAuth scopes are needed for the various entities:

| Entity Type       | Required Scopes                     |
|-------------------|-------------------------------------|
| products          | `read_products`                     |
| product_variants  | `read_products`                     |
| product_images    | `read_products`                     |
| collections       | `read_products`                     |
| customers         | `read_customers`                    |
| customer_addresses| `read_customers`                    |
| orders            | `read_orders`                       |
| order_line_items  | `read_orders`                       |
| inventory_items   | `read_inventory`                    |
| inventory_levels  | `read_inventory, read_locations`    |
| locations         | `read_locations`                    |
| metafields        | `read_metafields`                   |
| shop              | `read_shop_information`             |

## Development

### Building

```bash
cargo build --target wasm32-wasi
```

### Running Tests

```bash
cargo test
```

### Adding New Entities

1. Add model structs in `models.rs`
2. Implement row conversion method in `lib.rs` (`entity_to_row`)
3. Implement fetch method in `lib.rs` (`fetch_entity`)
4. Update resource handling in `begin_scan`, `iter_scan`, `re_scan`, and `end_scan`

## Maintenance Tasks

If extending or modifying this FDW, consider:

1. **API Changes**: Shopify API evolves - check latest endpoints and fields
2. **Query Pushdown**: Add new filter conditions when possible
3. **Error Handling**: Rate limiting and API errors require careful handling
4. **Response Mapping**: Ensure consistent handling of optional fields
5. **Pagination**: All list operations should support pagination

## Status

All core features are planned to be implemented. Future enhancements could include:
- Write support (INSERT/UPDATE/DELETE) for supported endpoints
- GraphQL API support for more efficient queries
- Webhooks integration for real-time data updates
- Caching layer for frequently accessed data
- Multi-store support for managing multiple Shopify stores