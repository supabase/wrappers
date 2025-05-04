//! Shopify data models
//!
//! This module defines the data structures for Shopify resources.

use serde::Deserialize;

/// A Shopify product with variants, options, and images
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

/// A variant of a Shopify product
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

/// An option for a Shopify product
#[derive(Debug, Deserialize, Clone)]
pub struct ProductOption {
    pub id: i64,
    pub product_id: i64,
    pub name: String,
    pub position: i32,
    pub values: Vec<String>,
}

/// An image of a Shopify product
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

/// A Shopify custom collection
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

/// A Shopify smart collection
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

/// An image for a Shopify collection
#[derive(Debug, Deserialize, Clone)]
pub struct CollectionImage {
    pub created_at: String,
    pub alt: Option<String>,
    pub width: i32,
    pub height: i32,
    pub src: String,
}

/// A rule for a Shopify smart collection
#[derive(Debug, Deserialize, Clone)]
pub struct CollectionRule {
    pub column: String,
    pub relation: String,
    pub condition: String,
}

/// A Shopify customer
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

/// An address for a Shopify customer
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

/// A Shopify order
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

/// A line item in a Shopify order
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

/// A property of a line item
#[derive(Debug, Deserialize, Clone)]
pub struct LineItemProperty {
    pub name: String,
    pub value: String,
}

/// A discount application
#[derive(Debug, Deserialize, Clone)]
pub struct DiscountApplication {
    pub target_type: String,
    pub r#type: String,
    pub value: String,
    pub value_type: String,
    pub allocation_method: String,
    pub target_selection: String,
    pub code: Option<String>,
}

/// A discount code
#[derive(Debug, Deserialize, Clone)]
pub struct DiscountCode {
    pub code: String,
    pub amount: String,
    pub r#type: String,
}

/// A note attribute
#[derive(Debug, Deserialize, Clone)]
pub struct NoteAttribute {
    pub name: String,
    pub value: String,
}

/// A tax line
#[derive(Debug, Deserialize, Clone)]
pub struct TaxLine {
    pub price: String,
    pub rate: f64,
    pub title: String,
    pub price_set: PriceSet,
}

/// A price set
#[derive(Debug, Deserialize, Clone)]
pub struct PriceSet {
    pub shop_money: Money,
    pub presentment_money: Money,
}

/// A money value
#[derive(Debug, Deserialize, Clone)]
pub struct Money {
    pub amount: String,
    pub currency_code: String,
}

/// A shipping line
#[derive(Debug, Deserialize, Clone)]
pub struct ShippingLine {
    pub id: i64,
    pub title: String,
    pub price: String,
    pub code: Option<String>,
    pub source: String,
    pub phone: Option<String>,
    pub requested_fulfillment_service_id: Option<String>,
    pub delivery_category: Option<String>,
    pub carrier_identifier: Option<String>,
    pub discounted_price: String,
    pub price_set: PriceSet,
    pub discounted_price_set: PriceSet,
    pub discount_allocations: Vec<DiscountAllocation>,
    pub tax_lines: Vec<TaxLine>,
}

/// A discount allocation
#[derive(Debug, Deserialize, Clone)]
pub struct DiscountAllocation {
    pub amount: String,
    pub discount_application_index: i32,
    pub amount_set: PriceSet,
}

/// A duty
#[derive(Debug, Deserialize, Clone)]
pub struct Duty {
    pub id: i64,
    pub harmonized_system_code: Option<String>,
    pub country_code_of_origin: Option<String>,
    pub shop_money: Money,
    pub presentment_money: Money,
    pub tax_lines: Vec<TaxLine>,
    pub admin_graphql_api_id: String,
}

/// An address
#[derive(Debug, Deserialize, Clone)]
pub struct Address {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
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
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub company: Option<String>,
}

/// A fulfillment
#[derive(Debug, Deserialize, Clone)]
pub struct Fulfillment {
    pub id: i64,
    pub order_id: i64,
    pub status: String,
    pub created_at: String,
    pub service: String,
    pub updated_at: String,
    pub tracking_company: Option<String>,
    pub shipment_status: Option<String>,
    pub tracking_number: Option<String>,
    pub tracking_numbers: Vec<String>,
    pub tracking_url: Option<String>,
    pub tracking_urls: Vec<String>,
    pub receipt: Option<Receipt>,
    pub line_items: Vec<LineItem>,
}

/// A receipt
#[derive(Debug, Deserialize, Clone)]
pub struct Receipt {
    pub testcase: bool,
    pub authorization: String,
}

/// A refund
#[derive(Debug, Deserialize, Clone)]
pub struct Refund {
    pub id: i64,
    pub order_id: i64,
    pub created_at: String,
    pub note: Option<String>,
    pub user_id: Option<i64>,
    pub processed_at: String,
    pub refund_line_items: Vec<RefundLineItem>,
    pub transactions: Vec<Transaction>,
    pub order_adjustments: Vec<OrderAdjustment>,
}

/// A refund line item
#[derive(Debug, Deserialize, Clone)]
pub struct RefundLineItem {
    pub id: i64,
    pub quantity: i32,
    pub line_item_id: i64,
    pub location_id: Option<i64>,
    pub restock_type: Option<String>,
    pub subtotal: f64,
    pub total_tax: f64,
    pub subtotal_set: PriceSet,
    pub total_tax_set: PriceSet,
    pub line_item: LineItem,
}

/// A transaction
#[derive(Debug, Deserialize, Clone)]
pub struct Transaction {
    pub id: i64,
    pub order_id: i64,
    pub kind: String,
    pub gateway: String,
    pub status: String,
    pub message: Option<String>,
    pub created_at: String,
    pub test: bool,
    pub authorization: Option<String>,
    pub location_id: Option<i64>,
    pub user_id: Option<i64>,
    pub parent_id: Option<i64>,
    pub processed_at: String,
    pub device_id: Option<i64>,
    pub error_code: Option<String>,
    pub source_name: String,
    pub receipt: Option<Receipt>,
    pub amount: String,
    pub currency: String,
    pub payment_id: Option<String>,
}

/// An order adjustment
#[derive(Debug, Deserialize, Clone)]
pub struct OrderAdjustment {
    pub id: i64,
    pub order_id: i64,
    pub refund_id: i64,
    pub amount: String,
    pub tax_amount: String,
    pub kind: String,
    pub reason: String,
    pub amount_set: PriceSet,
    pub tax_amount_set: PriceSet,
}

/// A Shopify inventory item
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

/// A country harmonized system code
#[derive(Debug, Deserialize, Clone)]
pub struct CountryHarmonizedSystemCode {
    pub harmonized_system_code: String,
    pub country_code: String,
}

/// A Shopify inventory level
#[derive(Debug, Deserialize, Clone)]
pub struct InventoryLevel {
    pub inventory_item_id: i64,
    pub location_id: i64,
    pub available: i32,
    pub updated_at: String,
}

/// A Shopify location
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

/// A Shopify shop
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

/// A Shopify metafield
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