---
source: https://shopify.dev/
documentation: https://shopify.dev/docs/api
author: supabase
tags:
  - wasm
  - ecommerce
---

# Shopify

[Shopify](https://shopify.com/) is a leading e-commerce platform that allows anyone to set up an online store and sell products. It offers a comprehensive suite of tools for managing products, inventory, customers, orders, and more.

The Shopify Wrapper is a WebAssembly (Wasm) foreign data wrapper which allows you to query Shopify store data directly from your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                                  | Checksum | Required Wrappers Version |
| ------- | ------------------------------------------------------------------------------------------------- | -------- | ------------------------- |
| 0.3.0   | `https://github.com/supabase/wrappers/releases/download/wasm_shopify_fdw_v0.3.0/shopify_fdw.wasm` | `tbd`    | >=0.5.0                   |
| 0.2.0   | `https://github.com/supabase/wrappers/releases/download/wasm_shopify_fdw_v0.2.0/shopify_fdw.wasm` | `tbd`    | >=0.5.0                   |

## Preparation

Before you can query Shopify, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Shopify Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Create a Shopify App and API Credentials

1. Visit the [Shopify Partners page](https://partners.shopify.com/) and create a Partner account if you don't have one
2. Navigate to "Apps" in the Partner Dashboard
3. Click "Create app" and select "Public app"
4. Give your app a name and select the app type
5. In the "App URL" section, enter a placeholder URL (you can update this later)
6. Under "Admin API integration", click "Configure Admin API scopes"
7. Select the following scopes:
   - `read_products` - Read product data
   - `read_customers` - Read customer data
   - `read_orders` - Read order data
   - `read_inventory` - Read inventory data
   - `read_locations` - Read location data
   - `read_metafields` - Read metafield data
   - `read_shop_information` - Read shop information
8. Save the changes
9. Note your API key and API secret key
10. Generate an API access token for your store

### Store credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Shopify API key in Vault and retrieve the created `key_id`
select vault.create_secret(
  'your-shopify-api-key',
  'shopify',
  'Shopify API key for Wrappers'
);

-- Save your Shopify access token in Vault and retrieve the created `key_id`
select vault.create_secret(
  'your-shopify-access-token',
  'shopify',
  'Shopify Access Token for Wrappers'
);
```

### Connecting to Shopify

We need to provide Postgres with the credentials to access Shopify and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server shopify_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_name 'supabase:shopify-fdw',
        fdw_package_url '{See: "Available Versions"}',
        fdw_package_checksum '{See: "Available Versions"}',
        fdw_package_version '{See: "Available Versions"}', -- eg: 0.1.0
        api_key_id '<key_ID>', -- The Key ID from Vault for API key
        access_token_id '<key_ID>', -- The Key ID from Vault for access token
        shop_domain 'your-store.myshopify.com',
        api_version '2023-10' -- Optional, defaults to latest
      );
    ```

=== "Without Vault"

    ```sql
    create server shopify_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_name 'supabase:shopify-fdw',
        fdw_package_url '{See: "Available Versions"}',
        fdw_package_checksum '{See: "Available Versions"}',
        fdw_package_version '{See: "Available Versions"}', -- eg: 0.1.0
        api_key 'your-shopify-api-key',
        access_token 'your-shopify-access-token',
        shop_domain 'your-store.myshopify.com',
        api_version '2023-10' -- Optional, defaults to latest
      );
    ```

The full list of server options are below:

- `fdw_package_*`: required. Specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).
- `api_key` | `api_key_id` 
    - `api_key`: Shopify API key, required if not using Vault.
    - `api_key_id`: Vault secret key ID storing the Shopify API key, required if using Vault.
- `access_token` | `access_token_id` 
    - `access_token`: Shopify access token, required if not using Vault.
    - `access_token_id`: Vault secret key ID storing the Shopify access token, required if using Vault.
- `shop_domain`: Shopify store domain (e.g., 'your-store.myshopify.com'), required.
- `api_version`: Shopify API version (e.g., '2023-10'), optional, defaults to latest.

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists shopify;
```

## Entities

The Shopify Wrapper supports data reads from the Shopify API.

### Products

This represents all products in the Shopify store.

Ref: [Shopify Products API](https://shopify.dev/docs/api/admin-rest/latest/resources/product)

#### Operations

| Object   | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| products |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table shopify.products (
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
  server shopify_server
  options (
    resource 'products'
  );
```

#### Notes

- The `id` field is the product ID and is used as the primary key
- Supports query pushdown for filtering by ID, title, vendor, product_type, created_at, and updated_at
- Requires the `read_products` scope

### Product Variants

This represents variants of products with different options, prices, and inventory.

Ref: [Shopify Product Variants API](https://shopify.dev/docs/api/admin-rest/latest/resources/product-variant)

#### Operations

| Object           | Select | Insert | Update | Delete | Truncate |
| ---------------- | :----: | :----: | :----: | :----: | :------: |
| product_variants |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table shopify.product_variants (
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
  server shopify_server
  options (
    resource 'product_variants'
  );
```

#### Notes

- The `id` field is the variant ID and is used as the primary key
- Supports query pushdown for filtering by product_id and sku
- Requires the `read_products` scope

### Customers

This represents customers who have made or attempted to make purchases.

Ref: [Shopify Customers API](https://shopify.dev/docs/api/admin-rest/latest/resources/customer)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| customers |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table shopify.customers (
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
  server shopify_server
  options (
    resource 'customers'
  );
```

#### Notes

- The `id` field is the customer ID and is used as the primary key
- Supports query pushdown for filtering by ID, email, created_at, and updated_at
- Requires the `read_customers` scope

### Orders

This represents orders placed by customers.

Ref: [Shopify Orders API](https://shopify.dev/docs/api/admin-rest/latest/resources/order)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| orders |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table shopify.orders (
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
  server shopify_server
  options (
    resource 'orders'
  );
```

#### Notes

- The `id` field is the order ID and is used as the primary key
- Supports query pushdown for filtering by ID, status, financial_status, fulfillment_status, created_at, and updated_at
- Requires the `read_orders` scope

### Inventory Items

This represents inventory items, which are used to track quantity of products at various locations.

Ref: [Shopify Inventory Item API](https://shopify.dev/docs/api/admin-rest/latest/resources/inventoryitem)

#### Operations

| Object          | Select | Insert | Update | Delete | Truncate |
| --------------- | :----: | :----: | :----: | :----: | :------: |
| inventory_items |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table shopify.inventory_items (
  id bigint,
  sku text,
  created_at text,
  updated_at text,
  requires_shipping boolean,
  cost text,
  country_code_of_origin text,
  province_code_of_origin text,
  harmonized_system_code text,
  tracked boolean
)
  server shopify_server
  options (
    resource 'inventory_items'
  );
```

#### Notes

- The `id` field is the inventory item ID and is used as the primary key
- Supports query pushdown for filtering by IDs
- Requires the `read_inventory` scope

### Shop

This represents information about the Shopify store.

Ref: [Shopify Shop API](https://shopify.dev/docs/api/admin-rest/latest/resources/shop)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| shop   |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table shopify.shop (
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
  server shopify_server
  options (
    resource 'shop'
  );
```

#### Notes

- Returns a single row with shop information
- Requires the `read_shop_information` scope

## Query Pushdown Support

This FDW supports the following condition pushdowns:

| Resource         | Supported Filters                                                                         | Sorting                       | Limit/Offset |
| ---------------- | ----------------------------------------------------------------------------------------- | ----------------------------- | ------------ |
| products         | id, title, vendor, product_type, created_at_min/max, updated_at_min/max, published_status | title, created_at, updated_at | Yes          |
| product_variants | product_id, sku                                                                           | title, price                  | Yes          |
| customers        | id, email, created_at_min/max, updated_at_min/max                                         | email, created_at             | Yes          |
| orders           | id, customer_id, financial_status, fulfillment_status, created_at_min/max                 | created_at, updated_at        | Yes          |
| inventory_items  | ids                                                                                       | None                          | Yes          |
| inventory_levels | inventory_item_ids, location_ids                                                          | None                          | Yes          |
| metafields       | owner_id, owner_resource, namespace, key                                                  | None                          | Yes          |
| shop             | *(no filter support - single row)*                                                        | N/A                           | N/A          |

## Handling Rate Limits

Shopify uses a "leaky bucket" algorithm for its rate limits. The FDW handles this by:

1. Reading the `X-Shopify-Shop-Api-Call-Limit` header to track usage
2. Automatically retrying requests when rate limited
3. Using pagination and batching to minimize API calls

Current rate limits for most Shopify API calls are 2 requests per second, with a bucket size of 40.

## Required Scopes for Shopify API Access

| Entity Type        | Required Scopes                  |
| ------------------ | -------------------------------- |
| products           | `read_products`                  |
| product_variants   | `read_products`                  |
| product_images     | `read_products`                  |
| collections        | `read_products`                  |
| customers          | `read_customers`                 |
| customer_addresses | `read_customers`                 |
| orders             | `read_orders`                    |
| order_line_items   | `read_orders`                    |
| inventory_items    | `read_inventory`                 |
| inventory_levels   | `read_inventory, read_locations` |
| locations          | `read_locations`                 |
| metafields         | `read_metafields`                |
| shop               | `read_shop_information`          |

## Limitations

This section describes important limitations and considerations when using this FDW:

- Rate limits are enforced by the Shopify API (typically 2 requests/second with burst up to 40)
- GraphQL support is not yet implemented, which could provide more efficient queries
- Some complex data structures are flattened to fit the SQL table model
- For large stores, pagination may be required to retrieve all data

## Troubleshooting

### Error: "Shopify API error: [429] Too Many Requests"

This error occurs when you've exceeded Shopify's rate limits.

**Solution:**
- Add more specific WHERE clauses to reduce the number of API calls
- Use smaller LIMIT values
- Consider materialized views for frequent queries:
```sql
CREATE MATERIALIZED VIEW shopify_products AS 
SELECT * FROM shopify.products;

-- Query the materialized view instead
SELECT * FROM shopify_products;

-- Refresh when needed (less frequently)
REFRESH MATERIALIZED VIEW shopify_products;
```

### Error: "Scope read_X is required for this API call"

This occurs when your API credentials don't have the necessary scopes for the resource.

**Solution:**
1. Visit your Shopify partner dashboard
2. Navigate to your app
3. Under "Admin API integration", update the app's scopes
4. Reinstall the app to the store to get a new access token with the required scopes

## Examples

Below are some examples on how to use Shopify foreign tables.

### Basic Example

This example will create a "foreign table" inside your Postgres database and query its data.

```sql
create foreign table shopify.products (
  id bigint,
  title text,
  vendor text,
  product_type text,
  created_at text,
  updated_at text,
  status text
)
server shopify_server
options (
  resource 'products'
);

-- Query all products
select * from shopify.products;
```

### Product Inventory Analysis

```sql
create foreign table shopify.products (
  id bigint,
  title text,
  product_type text,
  vendor text,
  created_at text
)
server shopify_server
options (
  resource 'products'
);

create foreign table shopify.product_variants (
  id bigint,
  product_id bigint,
  title text,
  price text,
  sku text,
  inventory_quantity integer
)
server shopify_server
options (
  resource 'product_variants'
);

-- Find products that are low on inventory
select 
  p.id as product_id, 
  p.title as product_title, 
  v.id as variant_id,
  v.title as variant_title,
  v.sku,
  v.inventory_quantity,
  v.price
from 
  shopify.products p
join 
  shopify.product_variants v on p.id = v.product_id
where 
  v.inventory_quantity < 10
order by 
  v.inventory_quantity asc;
```

### Sales Analysis

```sql
create foreign table shopify.orders (
  id bigint,
  created_at text,
  financial_status text,
  total_price text,
  currency text,
  customer_id bigint
)
server shopify_server
options (
  resource 'orders'
);

-- Get total sales by month
select 
  to_char(created_at::timestamp, 'YYYY-MM') as month,
  count(*) as order_count,
  sum(total_price::numeric) as total_sales
from 
  shopify.orders
where 
  financial_status = 'paid'
group by 
  to_char(created_at::timestamp, 'YYYY-MM')
order by 
  month desc;
```

### Customer Analysis

```sql
create foreign table shopify.customers (
  id bigint,
  email text,
  first_name text,
  last_name text,
  orders_count integer,
  total_spent text,
  created_at text
)
server shopify_server
options (
  resource 'customers'
);

-- Get top customers by total spend
select 
  id,
  email,
  first_name,
  last_name,
  orders_count,
  total_spent::numeric as total_spent
from 
  shopify.customers
order by 
  total_spent::numeric desc
limit 10;
```