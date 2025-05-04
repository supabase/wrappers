use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::models::{
    CustomCollection, CustomCollectionsResponse, Customer, CustomersResponse, InventoryItem,
    InventoryItemsResponse, InventoryLevel, InventoryLevelsResponse, Location, LocationsResponse,
    Metafield, MetafieldsResponse, Order, OrdersResponse, Product, ProductResponse, ProductsResponse,
    ResponseLinks, Shop, ShopResponse, SmartCollection, SmartCollectionsResponse,
};

/// Configuration options for the Shopify API client
#[derive(Debug, Clone)]
pub struct ShopifyConfig {
    /// Shop domain (e.g., "your-shop.myshopify.com")
    pub shop_domain: String,
    /// API version (e.g., "2023-07")
    pub api_version: String,
    /// Access token for authentication
    pub access_token: String,
    /// Optional API key (used for some authentication methods)
    pub api_key: Option<String>,
    /// Optional API secret (used for some authentication methods)
    pub api_secret: Option<String>,
    /// Maximum requests per second to respect API rate limits
    pub rate_limit: f64,
}

/// Shopify API client for interacting with the Shopify API
#[derive(Debug)]
pub struct ShopifyClient {
    /// Configuration for the API client
    config: ShopifyConfig,
    /// Base URL for API requests
    base_url: String,
    /// HTTP headers for requests
    headers: Vec<(String, String)>,
    /// Last request timestamp for rate limiting
    last_request_time: Option<Instant>,
    /// Debug mode flag
    debug: bool,
}

/// API response metadata for pagination and rate limit information
#[derive(Debug, Deserialize)]
pub struct ApiMetadata {
    /// Next page link, if available
    pub next_page: Option<String>,
    /// Previous page link, if available
    pub prev_page: Option<String>,
    /// Total number of items available
    pub total_count: Option<i32>,
    /// Rate limit information
    pub rate_limit: Option<RateLimitInfo>,
}

/// Rate limit information from API response headers
#[derive(Debug, Deserialize)]
pub struct RateLimitInfo {
    /// Current rate limit bucket
    pub bucket: String,
    /// Remaining requests in current time window
    pub remaining: i32,
    /// Maximum requests allowed in time window
    pub limit: i32,
    /// Time until rate limit resets (in seconds)
    pub reset_at: i64,
}

impl ShopifyClient {
    /// Create a new Shopify API client with the provided configuration
    pub fn new(config: ShopifyConfig) -> Self {
        let base_url = format!(
            "https://{}/admin/api/{}",
            config.shop_domain, config.api_version
        );

        // Set up standard headers including authentication
        let mut headers = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Accept".to_string(), "application/json".to_string()),
            (
                "X-Shopify-Access-Token".to_string(),
                config.access_token.clone(),
            ),
        ];

        // If using API key authentication, add those credentials too
        if let Some(api_key) = &config.api_key {
            headers.push(("X-Shopify-API-Key".to_string(), api_key.clone()));
        }

        Self {
            config,
            base_url,
            headers,
            last_request_time: None,
            debug: false,
        }
    }

    /// Enable debug mode for detailed logging
    pub fn with_debug(mut self, debug: bool) -> Self {
        self.debug = debug;
        self
    }

    /// Handle rate limiting by sleeping if necessary
    fn respect_rate_limit(&mut self) {
        // Respect the rate limit by ensuring minimum time between requests
        let min_request_interval = Duration::from_secs_f64(1.0 / self.config.rate_limit);

        if let Some(last_time) = self.last_request_time {
            let elapsed = last_time.elapsed();
            if elapsed < min_request_interval {
                let sleep_time = min_request_interval - elapsed;
                if self.debug {
                    println!(
                        "Rate limiting: sleeping for {}ms",
                        sleep_time.as_millis()
                    );
                }
                #[cfg(not(test))]
                std::thread::sleep(sleep_time);
            }
        }

        self.last_request_time = Some(Instant::now());
    }

    /// Make a GET request to the Shopify API
    async fn get(
        &mut self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<(JsonValue, ApiMetadata), String> {
        // Respect rate limiting
        self.respect_rate_limit();

        // Build the URL with query parameters if provided
        let mut url = format!("{}/{}", self.base_url, endpoint);
        if let Some(params) = params {
            if !params.is_empty() {
                url.push('?');
                let query_string = params
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("&");
                url.push_str(&query_string);
            }
        }

        if self.debug {
            println!("GET Request: {}", url);
        }

        // Make HTTP request
        let mut request_headers = HashMap::new();
        for (key, value) in &self.headers {
            request_headers.insert(key.clone(), value.clone());
        }

        // In WASM context, we'd use the http bindings
        // This is a placeholder for the real implementation
        #[cfg(not(test))]
        {
            let response = crate::bindings::http::request(
                "GET",
                &url,
                Some(request_headers),
                None::<Vec<u8>>,
            )
            .map_err(|e| format!("Failed to make HTTP request: {}", e))?;

            // Extract headers for rate limit info
            let mut rate_limit_info = None;
            let mut next_page = None;
            let mut prev_page = None;

            if let Some(headers) = response.headers {
                // Parse Link header for pagination
                if let Some(link_header) = headers.get("Link") {
                    let links = parse_link_header(link_header);
                    next_page = links.get("next").cloned();
                    prev_page = links.get("prev").cloned();
                }

                // Parse rate limit headers
                if let (Some(bucket), Some(remaining), Some(limit)) = (
                    headers.get("X-Shopify-Shop-Api-Call-Limit"),
                    headers.get("X-Shopify-API-Limit-Remaining"),
                    headers.get("X-Shopify-API-Limit"),
                ) {
                    let reset_at = headers
                        .get("X-Shopify-API-Limit-Reset")
                        .unwrap_or(&"0".to_string())
                        .parse::<i64>()
                        .unwrap_or(0);

                    rate_limit_info = Some(RateLimitInfo {
                        bucket: bucket.clone(),
                        remaining: remaining.parse::<i32>().unwrap_or(0),
                        limit: limit.parse::<i32>().unwrap_or(0),
                        reset_at,
                    });
                }
            }

            // Parse response body
            let response_body = response.body.unwrap_or_default();
            let json_response = serde_json::from_slice::<JsonValue>(&response_body)
                .map_err(|e| format!("Failed to parse response as JSON: {}", e))?;

            // Count total items if in a list response
            let total_count = json_response
                .as_object()
                .and_then(|obj| {
                    obj.iter().find_map(|(key, value)| {
                        if value.is_array() {
                            Some(value.as_array().unwrap().len() as i32)
                        } else {
                            None
                        }
                    })
                })
                .or_else(|| {
                    if let Some(headers) = response.headers {
                        headers
                            .get("X-Shopify-Total-Items")
                            .and_then(|count| count.parse::<i32>().ok())
                    } else {
                        None
                    }
                });

            let metadata = ApiMetadata {
                next_page,
                prev_page,
                total_count,
                rate_limit: rate_limit_info,
            };

            Ok((json_response, metadata))
        }

        #[cfg(test)]
        {
            // For testing, return mock data
            let json_response = json!({
                "products": [
                    {
                        "id": 123456789,
                        "title": "Test Product",
                        "status": "active",
                        "created_at": "2023-01-01T00:00:00Z",
                        "updated_at": "2023-01-02T00:00:00Z",
                        "variants": [],
                        "options": [],
                        "images": []
                    }
                ]
            });

            let metadata = ApiMetadata {
                next_page: Some("https://example.myshopify.com/admin/api/v1/products.json?page_info=abc123".to_string()),
                prev_page: None,
                total_count: Some(1),
                rate_limit: Some(RateLimitInfo {
                    bucket: "Shopify API".to_string(),
                    remaining: 39,
                    limit: 40,
                    reset_at: 0,
                }),
            };

            Ok((json_response, metadata))
        }
    }

    /// Apply conditions to query parameters for query pushdown
    pub fn apply_conditions(
        &self,
        endpoint: &str,
        conditions: &[(String, String, String)], // (column, operator, value)
    ) -> HashMap<String, String> {
        let mut params = HashMap::new();

        for (column, operator, value) in conditions {
            match endpoint {
                "products" => {
                    // Product filtering
                    if column == "id" && operator == "eq" {
                        params.insert("ids".to_string(), value.clone());
                    } else if column == "title" && operator == "eq" {
                        params.insert("title".to_string(), value.clone());
                    } else if column == "product_type" && operator == "eq" {
                        params.insert("product_type".to_string(), value.clone());
                    } else if column == "vendor" && operator == "eq" {
                        params.insert("vendor".to_string(), value.clone());
                    } else if column == "status" && operator == "eq" {
                        params.insert("status".to_string(), value.clone());
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
                "customers" => {
                    // Customer filtering
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
                "orders" => {
                    // Order filtering
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
                    } else if column == "processed_at_min" && operator == "ge" {
                        params.insert("processed_at_min".to_string(), value.clone());
                    } else if column == "processed_at_max" && operator == "le" {
                        params.insert("processed_at_max".to_string(), value.clone());
                    }
                }
                "inventory_items" => {
                    // Inventory filtering
                    if column == "id" && operator == "eq" {
                        params.insert("ids".to_string(), value.clone());
                    } else if column == "inventory_item_ids" && operator == "eq" {
                        params.insert("inventory_item_ids".to_string(), value.clone());
                    } else if column == "sku" && operator == "eq" {
                        params.insert("sku".to_string(), value.clone());
                    }
                }
                "inventory_levels" => {
                    // Inventory level filtering
                    if column == "inventory_item_id" && operator == "eq" {
                        params.insert("inventory_item_ids".to_string(), value.clone());
                    } else if column == "location_id" && operator == "eq" {
                        params.insert("location_ids".to_string(), value.clone());
                    }
                }
                "metafields" => {
                    // Metafield filtering
                    if column == "owner_id" && operator == "eq" {
                        params.insert("owner_id".to_string(), value.clone());
                    } else if column == "owner_resource" && operator == "eq" {
                        params.insert("owner_resource".to_string(), value.clone());
                    } else if column == "namespace" && operator == "eq" {
                        params.insert("namespace".to_string(), value.clone());
                    } else if column == "key" && operator == "eq" {
                        params.insert("key".to_string(), value.clone());
                    }
                }
                _ => {
                    // Default: no specific filtering for other endpoints
                }
            }
        }

        params
    }

    /// Handle pagination by following the "next" link
    pub async fn paginate<T: for<'de> Deserialize<'de>>(
        &mut self,
        mut url: String,
    ) -> Result<Vec<T>, String>
    where
        T: Clone,
    {
        let mut all_results = Vec::new();
        let mut next_page_url = Some(url);

        // Follow pagination links until there are no more pages
        while let Some(page_url) = next_page_url {
            // Extract endpoint and parameters from the URL
            let (endpoint, params) = if page_url.contains('?') {
                let parts: Vec<&str> = page_url.split('?').collect();
                let endpoint = parts[0].to_string();
                let params_str = parts[1];
                
                let mut params_map = HashMap::new();
                for param in params_str.split('&') {
                    if let Some(index) = param.find('=') {
                        let (key, value) = param.split_at(index);
                        params_map.insert(key.to_string(), value[1..].to_string());
                    }
                }
                
                (endpoint, Some(params_map))
            } else {
                (page_url, None)
            };

            // Make the request for this page
            let (response, metadata) = self.get(&endpoint, params).await?;
            
            // Parse the response based on expected type
            let page_results: Vec<T> = match serde_json::from_value(response.clone()) {
                Ok(parsed) => parsed,
                Err(e) => return Err(format!("Failed to parse paginated response: {}", e)),
            };
            
            // Add this page's results to the accumulated results
            all_results.extend(page_results);
            
            // Update next page URL for the next iteration
            next_page_url = metadata.next_page;
        }
        
        Ok(all_results)
    }

    /// Get store information
    pub async fn get_shop(&mut self) -> Result<Shop, String> {
        let (response, _) = self.get("shop.json", None).await?;
        
        let shop_response: ShopResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse shop response: {}", e))?;
            
        Ok(shop_response.shop)
    }

    /// Get products with optional filtering conditions
    pub async fn get_products(
        &mut self,
        conditions: Option<&[(String, String, String)]>,
    ) -> Result<Vec<Product>, String> {
        // Apply query conditions if provided
        let params = match conditions {
            Some(conds) => self.apply_conditions("products", conds),
            None => HashMap::new(),
        };
        
        let (response, metadata) = self.get("products.json", Some(params)).await?;
        
        let products_response: ProductsResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse products response: {}", e))?;
            
        // Check if we need to handle pagination
        if metadata.next_page.is_some() {
            // Follow pagination links to get all products
            self.paginate::<Product>(format!("{}/products.json", self.base_url)).await
        } else {
            Ok(products_response.products)
        }
    }

    /// Get a specific product by ID
    pub async fn get_product(&mut self, product_id: i64) -> Result<Product, String> {
        let (response, _) = self.get(&format!("products/{}.json", product_id), None).await?;
        
        let product_response: ProductResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse product response: {}", e))?;
            
        Ok(product_response.product)
    }

    /// Get custom collections with optional filtering
    pub async fn get_custom_collections(
        &mut self,
        conditions: Option<&[(String, String, String)]>,
    ) -> Result<Vec<CustomCollection>, String> {
        // Apply query conditions if provided
        let params = match conditions {
            Some(conds) => self.apply_conditions("custom_collections", conds),
            None => HashMap::new(),
        };
        
        let (response, metadata) = self.get("custom_collections.json", Some(params)).await?;
        
        let collections_response: CustomCollectionsResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse custom collections response: {}", e))?;
            
        // Check if we need to handle pagination
        if metadata.next_page.is_some() {
            // Follow pagination links to get all collections
            self.paginate::<CustomCollection>(format!("{}/custom_collections.json", self.base_url)).await
        } else {
            Ok(collections_response.custom_collections)
        }
    }

    /// Get smart collections with optional filtering
    pub async fn get_smart_collections(
        &mut self,
        conditions: Option<&[(String, String, String)]>,
    ) -> Result<Vec<SmartCollection>, String> {
        // Apply query conditions if provided
        let params = match conditions {
            Some(conds) => self.apply_conditions("smart_collections", conds),
            None => HashMap::new(),
        };
        
        let (response, metadata) = self.get("smart_collections.json", Some(params)).await?;
        
        let collections_response: SmartCollectionsResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse smart collections response: {}", e))?;
            
        // Check if we need to handle pagination
        if metadata.next_page.is_some() {
            // Follow pagination links to get all collections
            self.paginate::<SmartCollection>(format!("{}/smart_collections.json", self.base_url)).await
        } else {
            Ok(collections_response.smart_collections)
        }
    }

    /// Get customers with optional filtering
    pub async fn get_customers(
        &mut self,
        conditions: Option<&[(String, String, String)]>,
    ) -> Result<Vec<Customer>, String> {
        // Apply query conditions if provided
        let params = match conditions {
            Some(conds) => self.apply_conditions("customers", conds),
            None => HashMap::new(),
        };
        
        let (response, metadata) = self.get("customers.json", Some(params)).await?;
        
        let customers_response: CustomersResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse customers response: {}", e))?;
            
        // Check if we need to handle pagination
        if metadata.next_page.is_some() {
            // Follow pagination links to get all customers
            self.paginate::<Customer>(format!("{}/customers.json", self.base_url)).await
        } else {
            Ok(customers_response.customers)
        }
    }

    /// Get orders with optional filtering
    pub async fn get_orders(
        &mut self,
        conditions: Option<&[(String, String, String)]>,
    ) -> Result<Vec<Order>, String> {
        // Apply query conditions if provided
        let params = match conditions {
            Some(conds) => self.apply_conditions("orders", conds),
            None => HashMap::new(),
        };
        
        let (response, metadata) = self.get("orders.json", Some(params)).await?;
        
        let orders_response: OrdersResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse orders response: {}", e))?;
            
        // Check if we need to handle pagination
        if metadata.next_page.is_some() {
            // Follow pagination links to get all orders
            self.paginate::<Order>(format!("{}/orders.json", self.base_url)).await
        } else {
            Ok(orders_response.orders)
        }
    }

    /// Get inventory items with optional filtering
    pub async fn get_inventory_items(
        &mut self,
        conditions: Option<&[(String, String, String)]>,
    ) -> Result<Vec<InventoryItem>, String> {
        // Apply query conditions if provided
        let params = match conditions {
            Some(conds) => self.apply_conditions("inventory_items", conds),
            None => HashMap::new(),
        };
        
        let (response, metadata) = self.get("inventory_items.json", Some(params)).await?;
        
        let inventory_response: InventoryItemsResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse inventory items response: {}", e))?;
            
        // Check if we need to handle pagination
        if metadata.next_page.is_some() {
            // Follow pagination links to get all inventory items
            self.paginate::<InventoryItem>(format!("{}/inventory_items.json", self.base_url)).await
        } else {
            Ok(inventory_response.inventory_items)
        }
    }

    /// Get inventory levels with optional filtering
    pub async fn get_inventory_levels(
        &mut self,
        conditions: Option<&[(String, String, String)]>,
    ) -> Result<Vec<InventoryLevel>, String> {
        // Apply query conditions if provided
        let params = match conditions {
            Some(conds) => self.apply_conditions("inventory_levels", conds),
            None => HashMap::new(),
        };
        
        let (response, metadata) = self.get("inventory_levels.json", Some(params)).await?;
        
        let inventory_levels_response: InventoryLevelsResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse inventory levels response: {}", e))?;
            
        // Check if we need to handle pagination
        if metadata.next_page.is_some() {
            // Follow pagination links to get all inventory levels
            self.paginate::<InventoryLevel>(format!("{}/inventory_levels.json", self.base_url)).await
        } else {
            Ok(inventory_levels_response.inventory_levels)
        }
    }

    /// Get locations
    pub async fn get_locations(&mut self) -> Result<Vec<Location>, String> {
        let (response, metadata) = self.get("locations.json", None).await?;
        
        let locations_response: LocationsResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse locations response: {}", e))?;
            
        // Check if we need to handle pagination
        if metadata.next_page.is_some() {
            // Follow pagination links to get all locations
            self.paginate::<Location>(format!("{}/locations.json", self.base_url)).await
        } else {
            Ok(locations_response.locations)
        }
    }

    /// Get metafields for a specific resource
    pub async fn get_metafields(
        &mut self,
        resource_type: &str,
        resource_id: Option<i64>,
        conditions: Option<&[(String, String, String)]>,
    ) -> Result<Vec<Metafield>, String> {
        // Build the endpoint based on resource type and ID
        let endpoint = match resource_id {
            Some(id) => format!("{}/{}/metafields.json", resource_type, id),
            None => format!("metafields.json"),
        };
        
        // Apply query conditions if provided
        let params = match conditions {
            Some(conds) => self.apply_conditions("metafields", conds),
            None => HashMap::new(),
        };
        
        let (response, metadata) = self.get(&endpoint, Some(params)).await?;
        
        let metafields_response: MetafieldsResponse = serde_json::from_value(response)
            .map_err(|e| format!("Failed to parse metafields response: {}", e))?;
            
        // Check if we need to handle pagination
        if metadata.next_page.is_some() {
            // Follow pagination links to get all metafields
            self.paginate::<Metafield>(format!("{}/{}", self.base_url, endpoint)).await
        } else {
            Ok(metafields_response.metafields)
        }
    }
}

/// Parse Link header value to extract pagination URLs
fn parse_link_header(link_header: &str) -> HashMap<String, String> {
    let mut links = HashMap::new();
    
    for link in link_header.split(',') {
        let parts: Vec<&str> = link.split(';').collect();
        if parts.len() >= 2 {
            let url = parts[0]
                .trim()
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_string();
                
            let rel = parts[1]
                .trim()
                .trim_start_matches("rel=")
                .trim_matches('"')
                .to_string();
                
            links.insert(rel, url);
        }
    }
    
    links
}

// Existing functions and test cases for compatibility
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

    #[test]
    fn test_parse_link_header() {
        let link_header = r#"<https://test-store.myshopify.com/admin/api/2023-07/products.json?page_info=eyJsYXN0X2lkIjozMjQ4MzMwMzQ1NjN9>; rel="next", <https://test-store.myshopify.com/admin/api/2023-07/products.json?page_info=eyJmaXJzdF9pZCI6MjY5ODc3NjczNn0=>; rel="previous""#;
        
        let links = parse_link_header(link_header);
        
        assert_eq!(links.get("next"), Some(&"https://test-store.myshopify.com/admin/api/2023-07/products.json?page_info=eyJsYXN0X2lkIjozMjQ4MzMwMzQ1NjN9".to_string()));
        assert_eq!(links.get("previous"), Some(&"https://test-store.myshopify.com/admin/api/2023-07/products.json?page_info=eyJmaXJzdF9pZCI6MjY5ODc3NjczNn0=".to_string()));
    }

    #[test]
    fn test_shopify_client_apply_conditions() {
        let config = ShopifyConfig {
            shop_domain: "test-store.myshopify.com".to_string(),
            api_version: "2023-07".to_string(),
            access_token: "fake_token".to_string(),
            api_key: None,
            api_secret: None,
            rate_limit: 2.0,
        };
        
        let client = ShopifyClient::new(config);
        
        let conditions = vec![
            ("title".to_string(), "eq".to_string(), "Test Product".to_string()),
            ("vendor".to_string(), "eq".to_string(), "Test Vendor".to_string()),
            ("created_at_min".to_string(), "ge".to_string(), "2023-01-01T00:00:00Z".to_string()),
        ];

        let params = client.apply_conditions("products", &conditions);

        assert_eq!(params.get("title"), Some(&"Test Product".to_string()));
        assert_eq!(params.get("vendor"), Some(&"Test Vendor".to_string()));
        assert_eq!(params.get("created_at_min"), Some(&"2023-01-01T00:00:00Z".to_string()));
    }
}