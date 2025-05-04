//! Shopify Foreign Data Wrapper for PostgreSQL
//!
//! This module implements a Shopify FDW that allows querying Shopify stores directly from PostgreSQL.
//! It supports various resource types including products, collections, customers, orders, inventory, and shop info.
//! The FDW leverages the Shopify API to fetch data and supports query pushdown for filtering and sorting.
//!
//! Resources supported:
//! - products: Query product information including variants, images, and metadata
//! - product_variants: Query variant details across all products
//! - customers: (To be implemented) Query customer information
//! - orders: (To be implemented) Query order details including line items and fulfillments

#[allow(warnings)]
mod bindings;
pub mod models;
#[cfg(test)]
mod api;
#[cfg(test)]
mod tests;

use serde_json::Value as JsonValue;
use std::collections::HashMap;

use models::*;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time, utils,
        types::{
            Cell, Context, FdwError, FdwResult, Limit, OptionsType, Row,
            Sort, Value,
        },
    },
};

/// Name of this FDW for statistics tracking
static FDW_NAME: &str = "ShopifyFdw";

/// Global instance of the FDW as required by the PostgreSQL FDW API
static mut INSTANCE: *mut ShopifyFdw = std::ptr::null_mut::<ShopifyFdw>();

/// ShopifyFdw represents the instance of the Shopify foreign data wrapper.
/// It maintains all the state needed to interact with the Shopify API.
#[derive(Debug, Default)]
struct ShopifyFdw {
    // Connection state
    api_token: String,
    shop_domain: String,
    api_version: String,
    headers: Vec<(String, String)>,
    
    // Request state for pagination
    resource: String,
    has_more: bool,
    next_page_token: Option<String>,
    
    // Cache for API responses
    products: Vec<Product>,
    product_variants: Vec<ProductVariant>,
    
    // Current position in the result set for iteration
    result_index: usize,
    
    // Query pushdown support
    sorts: Vec<Sort>,
    limit: Option<Limit>,
}

/// Implementation of the ShopifyFdw methods
impl ShopifyFdw {
    /// Initialize the singleton instance
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }
    
    /// Get a mutable reference to the singleton instance
    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }
    
    /// Create a request for the Shopify API
    fn create_request(
        &self,
        endpoint: &str,
        params: &[(String, String)],
    ) -> Result<http::Request, String> {
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

    /// Make a request to the Shopify API with retries for rate limiting
    fn make_request(&self, req: &http::Request) -> Result<(JsonValue, HashMap<String, String>), String> {
        loop {
            let resp = match req.method {
                http::Method::Get => http::get(req).map_err(|e| e.to_string())?,
                http::Method::Post => http::post(req).map_err(|e| e.to_string())?,
                http::Method::Put => http::put(req).map_err(|e| e.to_string())?,
                http::Method::Delete => http::delete(req).map_err(|e| e.to_string())?,
                _ => return Err("Unsupported HTTP method".to_string()),
            };

            // Handle rate limiting
            if resp.status_code == 429 {
                if let Some(retry) = resp.headers.iter().find(|(name, _)| name == "Retry-After") {
                    let delay_secs = retry.1.parse::<u64>().map_err(|e| e.to_string())?;
                    time::sleep(delay_secs * 1000);
                    continue;
                }
            }

            // Check for errors
            if resp.status_code < 200 || resp.status_code >= 300 {
                return Err(format!("HTTP error {}: {}", resp.status_code, resp.body));
            }

            // Transform response to JSON
            let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

            // Convert headers to HashMap for easier access
            let headers = resp.headers.iter().cloned().collect::<HashMap<String, String>>();

            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

            return Ok((resp_json, headers));
        }
    }

    /// Extract page_info token from Link header
    fn extract_next_page_info(&self, headers: &HashMap<String, String>) -> Option<String> {
        headers.get("Link").and_then(|link| {
            // Parse the Link header value which might contain multiple links
            let links = link.split(',').collect::<Vec<&str>>();
            for link in links {
                if link.contains("rel=\"next\"") {
                    // Extract the URL
                    if let Some(url_start) = link.find('<') {
                        if let Some(url_end) = link[url_start + 1..].find('>') {
                            let url = &link[url_start + 1..url_start + 1 + url_end];
                            // Extract page_info parameter
                            if let Some(query_start) = url.find('?') {
                                let query_params = &url[query_start + 1..];
                                for param in query_params.split('&') {
                                    if let Some(eq_pos) = param.find('=') {
                                        let name = &param[..eq_pos];
                                        let value = &param[eq_pos + 1..];
                                        if name == "page_info" {
                                            return Some(value.to_string());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            None
        })
    }
    
    /// Apply filter conditions from quals to params for products
    fn apply_product_filter_conditions(
        &self,
        quals: &[bindings::supabase::wrappers::types::Qual],
        params: &mut Vec<(String, String)>,
    ) {
        // Convert postgres quals to Shopify API filter parameters
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
                    "published_at_min" => {
                        if let Value::Cell(Cell::String(published_at_min)) = qual.value() {
                            params.push(("published_at_min".to_string(), published_at_min.clone()));
                        }
                    }
                    "published_at_max" => {
                        if let Value::Cell(Cell::String(published_at_max)) = qual.value() {
                            params.push(("published_at_max".to_string(), published_at_max.clone()));
                        }
                    }
                    "published_status" => {
                        if let Value::Cell(Cell::String(status)) = qual.value() {
                            params.push(("published_status".to_string(), status.clone()));
                        }
                    }
                    "status" => {
                        if let Value::Cell(Cell::String(status)) = qual.value() {
                            params.push(("status".to_string(), status.clone()));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    
    /// Get products with optional filtering and pagination
    fn get_products(&self, filter_params: &[(String, String)], page_token: Option<String>) -> Result<(Vec<Product>, bool, Option<String>), String> {
        let mut params = filter_params.to_vec();

        // Add pagination if needed
        if let Some(token) = page_token {
            params.push(("page_info".to_string(), token));
        }

        // Default to 50 items per page if not specified
        if !params.iter().any(|(name, _)| name == "limit") {
            params.push(("limit".to_string(), "50".to_string()));
        }

        // Create and execute the request
        let req = self.create_request("products.json", &params)?;
        let (json, headers) = self.make_request(&req)?;

        // Extract products from response
        let products = match json.get("products") {
            Some(products_json) => {
                let products_vec = products_json.as_array()
                    .ok_or("Expected 'products' to be an array")?;

                // Deserialize each product
                products_vec.iter()
                    .map(|product_json| serde_json::from_value(product_json.clone())
                        .map_err(|e| format!("Error deserializing product: {}", e)))
                    .collect::<Result<Vec<Product>, String>>()?
            },
            None => return Err("No 'products' field in response".to_string()),
        };

        // Extract pagination info
        let next_page_token = self.extract_next_page_info(&headers);
        let has_more = next_page_token.is_some();

        Ok((products, has_more, next_page_token))
    }
    
    /// Get a single product by ID
    fn get_product(&self, id: i64) -> Result<Product, String> {
        let req = self.create_request(&format!("products/{}.json", id), &[])?;
        let (json, _) = self.make_request(&req)?;

        match json.get("product") {
            Some(product_json) => {
                let product = serde_json::from_value(product_json.clone())
                    .map_err(|e| format!("Error deserializing product: {}", e))?;
                Ok(product)
            },
            None => Err("No 'product' field in response".to_string()),
        }
    }
    
    /// Convert a product to a PostgreSQL row
    fn product_to_row(&self, product: &Product, row: &Row) -> Result<(), FdwError> {
        row.push(Some(&Cell::I64(product.id)));
        row.push(Some(&Cell::String(product.title.clone())));
        
        match &product.body_html {
            Some(body_html) => row.push(Some(&Cell::String(body_html.clone()))),
            None => row.push(None),
        }
        
        match &product.vendor {
            Some(vendor) => row.push(Some(&Cell::String(vendor.clone()))),
            None => row.push(None),
        }
        
        match &product.product_type {
            Some(product_type) => row.push(Some(&Cell::String(product_type.clone()))),
            None => row.push(None),
        }
        
        row.push(Some(&Cell::String(product.created_at.clone())));
        row.push(Some(&Cell::String(product.updated_at.clone())));
        
        match &product.published_at {
            Some(published_at) => row.push(Some(&Cell::String(published_at.clone()))),
            None => row.push(None),
        }
        
        row.push(Some(&Cell::String(product.status.clone())));
        
        match &product.tags {
            Some(tags) => row.push(Some(&Cell::String(tags.clone()))),
            None => row.push(None),
        }
        
        // Variant count as int
        row.push(Some(&Cell::I32(product.variants.len() as i32)));
        
        // Image count as int
        row.push(Some(&Cell::I32(product.images.len() as i32)));
        
        // Option count as int
        row.push(Some(&Cell::I32(product.options.len() as i32)));
        
        Ok(())
    }
    
    /// Convert a product variant to a PostgreSQL row
    fn product_variant_to_row(&self, variant: &ProductVariant, row: &Row) -> Result<(), FdwError> {
        row.push(Some(&Cell::I64(variant.id)));
        row.push(Some(&Cell::I64(variant.product_id)));
        row.push(Some(&Cell::String(variant.title.clone())));
        row.push(Some(&Cell::String(variant.price.clone())));
        
        match &variant.sku {
            Some(sku) => row.push(Some(&Cell::String(sku.clone()))),
            None => row.push(None),
        }
        
        row.push(Some(&Cell::I32(variant.position)));
        row.push(Some(&Cell::String(variant.inventory_policy.clone())));
        
        match &variant.compare_at_price {
            Some(compare_at_price) => row.push(Some(&Cell::String(compare_at_price.clone()))),
            None => row.push(None),
        }
        
        row.push(Some(&Cell::String(variant.fulfillment_service.clone())));
        
        match &variant.inventory_management {
            Some(inventory_management) => row.push(Some(&Cell::String(inventory_management.clone()))),
            None => row.push(None),
        }
        
        match &variant.option1 {
            Some(option1) => row.push(Some(&Cell::String(option1.clone()))),
            None => row.push(None),
        }
        
        match &variant.option2 {
            Some(option2) => row.push(Some(&Cell::String(option2.clone()))),
            None => row.push(None),
        }
        
        match &variant.option3 {
            Some(option3) => row.push(Some(&Cell::String(option3.clone()))),
            None => row.push(None),
        }
        
        row.push(Some(&Cell::String(variant.created_at.clone())));
        row.push(Some(&Cell::String(variant.updated_at.clone())));
        row.push(Some(&Cell::Bool(variant.taxable)));
        
        match &variant.barcode {
            Some(barcode) => row.push(Some(&Cell::String(barcode.clone()))),
            None => row.push(None),
        }
        
        row.push(Some(&Cell::I32(variant.grams)));
        
        match &variant.image_id {
            Some(image_id) => row.push(Some(&Cell::I64(*image_id))),
            None => row.push(None),
        }
        
        row.push(Some(&Cell::F64(variant.weight)));
        row.push(Some(&Cell::String(variant.weight_unit.clone())));
        row.push(Some(&Cell::I64(variant.inventory_item_id)));
        row.push(Some(&Cell::I32(variant.inventory_quantity)));
        row.push(Some(&Cell::I32(variant.old_inventory_quantity)));
        row.push(Some(&Cell::Bool(variant.requires_shipping)));
        
        Ok(())
    }
    
    /// Fetch products from Shopify API
    fn fetch_products(&mut self, ctx: &Context) -> FdwResult {
        // Create request parameters with filter conditions
        let mut params = Vec::new();
        
        // Apply query pushdown for filtering
        let quals = ctx.get_quals();
        self.apply_product_filter_conditions(&quals, &mut params);
        
        // Add limit parameter if specified
        if let Some(limit) = &self.limit {
            params.push(("limit".to_string(), limit.count().to_string()));
        }
        
        // Sort parameters will be applied in memory since Shopify API 
        // only supports limited sorting options
        
        // Fetch products from API with pagination
        match self.get_products(&params, self.next_page_token.clone()) {
            Ok((products, has_more, next_page_token)) => {
                self.products = products;
                self.has_more = has_more;
                self.next_page_token = next_page_token;
                
                // Apply sorting if requested (local sort since API sort options are limited)
                if !self.sorts.is_empty() {
                    self.apply_sorts();
                }
                
                // Reset position
                self.result_index = 0;
                
                Ok(())
            },
            Err(e) => Err(format!("Error fetching products: {}", e)),
        }
    }
    
    /// Apply sorts to the fetched products
    fn apply_sorts(&mut self) {
        self.products.sort_by(|a, b| {
            for sort in &self.sorts {
                let ordering = match sort.field().as_str() {
                    "id" => a.id.cmp(&b.id),
                    "title" => a.title.cmp(&b.title),
                    "created_at" => a.created_at.cmp(&b.created_at),
                    "updated_at" => a.updated_at.cmp(&b.updated_at),
                    "status" => a.status.cmp(&b.status),
                    _ => continue,
                };
                
                if ordering != std::cmp::Ordering::Equal {
                    return if sort.reversed() {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    
    /// Apply sorts to the fetched product variants
    fn apply_variant_sorts(&mut self) {
        self.product_variants.sort_by(|a, b| {
            for sort in &self.sorts {
                let ordering = match sort.field().as_str() {
                    "id" => a.id.cmp(&b.id),
                    "product_id" => a.product_id.cmp(&b.product_id),
                    "title" => a.title.cmp(&b.title),
                    "position" => a.position.cmp(&b.position),
                    "price" => a.price.cmp(&b.price),
                    "created_at" => a.created_at.cmp(&b.created_at),
                    "updated_at" => a.updated_at.cmp(&b.updated_at),
                    _ => continue,
                };
                
                if ordering != std::cmp::Ordering::Equal {
                    return if sort.reversed() {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    
    /// Fetch product variants for a given product ID or all product variants
    fn fetch_product_variants(&mut self, ctx: &Context) -> FdwResult {
        // Create request parameters for filtering
        let mut params = Vec::new();
        
        // Check if we have a product_id filter
        let mut product_id: Option<i64> = None;
        let quals = ctx.get_quals();
        for qual in quals.iter() {
            if qual.field().as_str() == "product_id" && qual.operator().as_str() == "=" && !qual.use_or() {
                if let Value::Cell(Cell::I64(id)) = qual.value() {
                    product_id = Some(id);
                    break;
                }
            }
        }
        
        // Clear existing variants
        self.product_variants.clear();
        
        if let Some(id) = product_id {
            // Fetch variants for a specific product
            match self.get_product(id) {
                Ok(product) => {
                    // Add all variants from the product
                    self.product_variants = product.variants;
                    self.has_more = false;
                    self.next_page_token = None;
                },
                Err(e) => return Err(format!("Error fetching product variants: {}", e)),
            }
        } else {
            // Fetch all products and extract their variants
            // Add limit parameter if specified
            if let Some(limit) = &self.limit {
                params.push(("limit".to_string(), limit.count().to_string()));
            }
            
            // Fetch products from API with pagination
            match self.get_products(&params, self.next_page_token.clone()) {
                Ok((products, has_more, next_page_token)) => {
                    // Extract all variants from all products
                    for product in &products {
                        self.product_variants.extend(product.variants.clone());
                    }
                    
                    self.has_more = has_more;
                    self.next_page_token = next_page_token;
                },
                Err(e) => return Err(format!("Error fetching products for variants: {}", e)),
            }
        }
        
        // Apply sorting if requested
        if !self.sorts.is_empty() {
            self.apply_variant_sorts();
        }
        
        // Apply any limit constraints
        if let Some(limit) = &self.limit {
            let limit_count = limit.count() as usize;
            if self.product_variants.len() > limit_count {
                self.product_variants.truncate(limit_count);
                self.has_more = false;
            }
        }
        
        // Reset position
        self.result_index = 0;
        
        Ok(())
    }
}

/// Implementation of the FDW interface for PostgreSQL
impl Guest for ShopifyFdw {
    fn host_version_requirement() -> String {
        "^0.2.0".to_string()
    }
    
    /// Initialize the FDW with server options
    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();
        
        // Get foreign server options
        let opts = ctx.get_options(&OptionsType::Server);
        
        // Get API credentials from options
        let shop_domain = opts.require("shop_domain")?;
        
        let access_token = match opts.get("access_token") {
            Some(token) => token,
            None => {
                let token_id = opts.require("access_token_id")?;
                utils::get_vault_secret(&token_id).unwrap_or_default()
            }
        };
        
        // Get optional API version, default to latest stable
        let api_version = opts.get("api_version").unwrap_or_else(|| "2023-10".to_string());
        
        // Set up authorization headers
        this.headers.push(("X-Shopify-Access-Token".to_owned(), access_token.clone()));
        this.headers.push(("Content-Type".to_owned(), "application/json".to_string()));
        this.headers.push(("User-Agent".to_owned(), "Supabase Shopify FDW".to_string()));
        
        // Store options in the instance
        this.api_token = access_token;
        this.shop_domain = shop_domain;
        this.api_version = api_version;
        
        // Track creation statistic
        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);
        
        Ok(())
    }
    
    /// Begin scanning data from Shopify
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
            // TODO: Implement other resource types
            _ => Err(format!("Unsupported resource type: {}", resource)),
        }
    }
    
    /// Iterate through scan results
    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();
        
        // Ensure we have the right resource type
        match this.resource.as_str() {
            "products" => {
                // Check if we've reached the end of current batch
                if this.result_index >= this.products.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.products.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.products.len() as i64);
                    
                    // If there's more data and we don't have a limit or haven't reached it, fetch next page
                    if this.has_more {
                        // If we have a limit, check if we've already reached it
                        if let Some(limit) = &this.limit {
                            if this.products.len() >= limit.count() as usize {
                                // We've already met our limit, don't fetch more
                                return Ok(None);
                            }
                        }
                        
                        // Fetch next page
                        this.fetch_products(ctx)?;
                        
                        // If new batch is empty, we're done
                        if this.products.is_empty() {
                            return Ok(None);
                        }
                    } else {
                        // No more results
                        return Ok(None);
                    }
                }
                
                // Get product from current batch and convert to row
                let product = &this.products[this.result_index];
                this.product_to_row(product, row)?;
                
                // Increment position for next iteration
                this.result_index += 1;
                
                // Return success with no metadata
                Ok(Some(0))
            },
            "product_variants" => {
                // Check if we've reached the end of current batch
                if this.result_index >= this.product_variants.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.product_variants.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.product_variants.len() as i64);
                    
                    // If there's more data and we don't have a limit or haven't reached it, fetch next page
                    if this.has_more {
                        // If we have a limit, check if we've already reached it
                        if let Some(limit) = &this.limit {
                            if this.product_variants.len() >= limit.count() as usize {
                                // We've already met our limit, don't fetch more
                                return Ok(None);
                            }
                        }
                        
                        // Fetch next page
                        this.fetch_product_variants(ctx)?;
                        
                        // If new batch is empty, we're done
                        if this.product_variants.is_empty() {
                            return Ok(None);
                        }
                    } else {
                        // No more results
                        return Ok(None);
                    }
                }
                
                // Get variant from current batch and convert to row
                let variant = &this.product_variants[this.result_index];
                this.product_variant_to_row(variant, row)?;
                
                // Increment position for next iteration
                this.result_index += 1;
                
                // Return success with no metadata
                Ok(Some(0))
            },
            _ => Err(format!("Unsupported resource type: {}", this.resource)),
        }
    }
    
    /// Reset the scan to start from the beginning
    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        
        // Reset pagination state
        this.has_more = false;
        this.next_page_token = None;
        this.result_index = 0;
        
        // Update sort and limit in case they changed
        this.sorts = ctx.get_sorts();
        this.limit = ctx.get_limit();
        
        // Re-fetch the data
        match this.resource.as_str() {
            "products" => this.fetch_products(ctx),
            "product_variants" => this.fetch_product_variants(ctx),
            // TODO: Implement other resource types
            _ => Err(format!("Unsupported resource type: {}", this.resource)),
        }
    }
    
    /// End the scan and clean up resources
    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        
        // Clear cached data based on resource type
        match this.resource.as_str() {
            "products" => this.products.clear(),
            "product_variants" => this.product_variants.clear(),
            // TODO: Clear other resource types
            _ => {}
        }
        
        Ok(())
    }
    
    /// Import foreign schema
    fn import_foreign_schema(_ctx: &Context, stmt: bindings::supabase::wrappers::types::ImportForeignSchemaStmt) -> Result<Vec<String>, FdwError> {
        use bindings::supabase::wrappers::types::ImportSchemaType;
        
        // Get the list of tables to import
        let tables: Vec<String> = match stmt.list_type {
            ImportSchemaType::LimitTo => stmt.table_list.clone(),
            ImportSchemaType::Except => vec!["products".to_string(), "product_variants".to_string()]
                .into_iter()
                .filter(|t| !stmt.table_list.contains(t))
                .collect(),
            ImportSchemaType::All => vec!["products".to_string(), "product_variants".to_string()],
        };
        
        // Generate SQL for each table
        let mut result = Vec::new();
        
        for table in tables {
            match table.as_str() {
                "products" => result.push(r#"
CREATE FOREIGN TABLE %s (
    id bigint,
    title text,
    body_html text,
    vendor text,
    product_type text,
    created_at text,
    updated_at text,
    published_at text,
    status text,
    tags text,
    variant_count int,
    image_count int,
    option_count int
) SERVER %s OPTIONS (
    resource 'products'
);
                "#.to_string()),
                "product_variants" => result.push(r#"
CREATE FOREIGN TABLE %s (
    id bigint,
    product_id bigint,
    title text,
    price text,
    sku text,
    position int,
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
    grams int,
    image_id bigint,
    weight double precision,
    weight_unit text,
    inventory_item_id bigint,
    inventory_quantity int,
    old_inventory_quantity int,
    requires_shipping boolean
) SERVER %s OPTIONS (
    resource 'product_variants'
);
                "#.to_string()),
                // TODO: Add more table definitions
                _ => return Err(format!("Unknown table name: {}", table)),
            }
        }
        
        Ok(result)
    }
    
    // The Shopify FDW is initially read-only
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