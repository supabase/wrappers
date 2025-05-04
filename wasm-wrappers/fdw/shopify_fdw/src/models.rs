use serde::{Deserialize, Serialize};

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

/// Products response
#[derive(Debug, Deserialize)]
pub struct ProductsResponse {
    pub products: Vec<Product>,
}

/// Single product response
#[derive(Debug, Deserialize)]
pub struct ProductResponse {
    pub product: Product,
}

/// Product resource model
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

/// Product variant model
#[derive(Debug, Deserialize, Clone)]
pub struct ProductVariant {
    pub id: i64,
    pub product_id: i64,
    pub title: String,
    pub price: String,
    pub sku: Option<String>,
    pub position: i32,
    pub inventory_policy: Option<String>,
    pub compare_at_price: Option<String>,
    pub fulfillment_service: Option<String>,
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

/// Product option model
#[derive(Debug, Deserialize, Clone)]
pub struct ProductOption {
    pub id: i64,
    pub product_id: i64,
    pub name: String,
    pub position: i32,
    pub values: Vec<String>,
}

/// Product image model
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

/// Collection responses
#[derive(Debug, Deserialize)]
pub struct CustomCollectionsResponse {
    pub custom_collections: Vec<CustomCollection>,
}

#[derive(Debug, Deserialize)]
pub struct SmartCollectionsResponse {
    pub smart_collections: Vec<SmartCollection>,
}

/// Custom Collection model
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

/// Smart Collection model
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

/// Customer responses
#[derive(Debug, Deserialize)]
pub struct CustomersResponse {
    pub customers: Vec<Customer>,
}

#[derive(Debug, Deserialize)]
pub struct CustomerResponse {
    pub customer: Customer,
}

/// Customer model
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

/// Customer address model
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

/// Order responses
#[derive(Debug, Deserialize)]
pub struct OrdersResponse {
    pub orders: Vec<Order>,
}

#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    pub order: Order,
}

/// Order model
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

#[derive(Debug, Deserialize, Clone)]
pub struct LineItemProperty {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PriceSet {
    pub shop_money: Money,
    pub presentment_money: Money,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Money {
    pub amount: String,
    pub currency_code: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DiscountApplication {
    pub target_type: String,
    pub type_field: String,
    pub value: String,
    pub value_type: String,
    pub allocation_method: String,
    pub target_selection: String,
    pub code: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DiscountCode {
    pub code: String,
    pub amount: String,
    pub type_field: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NoteAttribute {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TaxLine {
    pub price: String,
    pub rate: f64,
    pub title: String,
    pub price_set: PriceSet,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ShippingLine {
    pub id: i64,
    pub title: String,
    pub price: String,
    pub code: String,
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

#[derive(Debug, Deserialize, Clone)]
pub struct DiscountAllocation {
    pub amount: String,
    pub discount_application_index: i32,
    pub amount_set: PriceSet,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Duty {
    pub id: i64,
    pub harmonized_system_code: String,
    pub country_code_of_origin: String,
    pub shop_money: Money,
    pub presentment_money: Money,
    pub tax_lines: Vec<TaxLine>,
    pub admin_graphql_api_id: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Address {
    pub first_name: Option<String>,
    pub address1: Option<String>,
    pub phone: Option<String>,
    pub city: Option<String>,
    pub zip: Option<String>,
    pub province: Option<String>,
    pub country: Option<String>,
    pub last_name: Option<String>,
    pub address2: Option<String>,
    pub company: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub name: Option<String>,
    pub country_code: Option<String>,
    pub province_code: Option<String>,
}

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

#[derive(Debug, Deserialize, Clone)]
pub struct Receipt {
    pub testcase: bool,
    pub authorization: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Refund {
    pub id: i64,
    pub order_id: i64,
    pub created_at: String,
    pub note: Option<String>,
    pub user_id: Option<i64>,
    pub processed_at: String,
    pub restock: bool,
    pub admin_graphql_api_id: String,
    pub refund_line_items: Vec<RefundLineItem>,
    pub transactions: Vec<Transaction>,
    pub order_adjustments: Vec<OrderAdjustment>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RefundLineItem {
    pub id: i64,
    pub quantity: i32,
    pub line_item_id: i64,
    pub location_id: Option<i64>,
    pub restock_type: String,
    pub subtotal: f64,
    pub total_tax: f64,
    pub subtotal_set: PriceSet,
    pub total_tax_set: PriceSet,
    pub line_item: LineItem,
}

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
    pub admin_graphql_api_id: String,
}

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

/// Inventory responses
#[derive(Debug, Deserialize)]
pub struct InventoryItemsResponse {
    pub inventory_items: Vec<InventoryItem>,
}

#[derive(Debug, Deserialize)]
pub struct InventoryLevelsResponse {
    pub inventory_levels: Vec<InventoryLevel>,
}

/// Inventory Item model
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
pub struct CountryHarmonizedSystemCode {
    pub harmonized_system_code: String,
    pub country_code: String,
}

/// Inventory Level model
#[derive(Debug, Deserialize, Clone)]
pub struct InventoryLevel {
    pub inventory_item_id: i64,
    pub location_id: i64,
    pub available: i32,
    pub updated_at: String,
}

/// Location response
#[derive(Debug, Deserialize)]
pub struct LocationsResponse {
    pub locations: Vec<Location>,
}

/// Location model
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

/// Shop response
#[derive(Debug, Deserialize)]
pub struct ShopResponse {
    pub shop: Shop,
}

/// Shop model
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

/// Metafields response
#[derive(Debug, Deserialize)]
pub struct MetafieldsResponse {
    pub metafields: Vec<Metafield>,
}

/// Metafield model
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

// Pagination response fields
#[derive(Debug, Deserialize)]
pub struct ResponseLinks {
    pub previous: Option<String>,
    pub next: Option<String>,
}

// Helper methods for row conversion
impl Product {
    pub fn to_row_vec(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.to_string()),
            Some(self.title.clone()),
            self.body_html.clone(),
            self.vendor.clone(),
            self.product_type.clone(),
            Some(self.created_at.clone()),
            Some(self.updated_at.clone()),
            self.published_at.clone(),
            Some(self.status.clone()),
            self.tags.clone(),
        ]
    }
}

impl ProductVariant {
    pub fn to_row_vec(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.to_string()),
            Some(self.product_id.to_string()),
            Some(self.title.clone()),
            Some(self.price.clone()),
            self.sku.clone(),
            Some(self.position.to_string()),
            self.inventory_policy.clone(),
            self.compare_at_price.clone(),
            self.fulfillment_service.clone(),
            self.inventory_management.clone(),
            self.option1.clone(),
            self.option2.clone(),
            self.option3.clone(),
            Some(self.created_at.clone()),
            Some(self.updated_at.clone()),
            Some(self.taxable.to_string()),
            self.barcode.clone(),
            Some(self.grams.to_string()),
            self.image_id.map(|id| id.to_string()),
            Some(self.weight.to_string()),
            Some(self.weight_unit.clone()),
            Some(self.inventory_item_id.to_string()),
            Some(self.inventory_quantity.to_string()),
            Some(self.old_inventory_quantity.to_string()),
            Some(self.requires_shipping.to_string()),
        ]
    }
}

impl Customer {
    pub fn to_row_vec(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.to_string()),
            self.email.clone(),
            Some(self.accepts_marketing.to_string()),
            Some(self.created_at.clone()),
            Some(self.updated_at.clone()),
            self.first_name.clone(),
            self.last_name.clone(),
            Some(self.orders_count.to_string()),
            Some(self.state.clone()),
            Some(self.total_spent.clone()),
            self.last_order_id.map(|id| id.to_string()),
            self.note.clone(),
            Some(self.verified_email.to_string()),
            Some(self.tax_exempt.to_string()),
            self.phone.clone(),
            Some(self.tags.clone()),
            self.last_order_name.clone(),
            Some(self.currency.clone()),
        ]
    }
}

impl Order {
    pub fn to_row_vec(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.to_string()),
            self.email.clone(),
            self.closed_at.clone(),
            Some(self.created_at.clone()),
            Some(self.updated_at.clone()),
            Some(self.number.to_string()),
            self.note.clone(),
            Some(self.token.clone()),
            self.gateway.clone(),
            Some(self.test.to_string()),
            Some(self.total_price.clone()),
            Some(self.subtotal_price.clone()),
            Some(self.total_weight.to_string()),
            Some(self.total_tax.clone()),
            Some(self.taxes_included.to_string()),
            Some(self.currency.clone()),
            Some(self.financial_status.clone()),
            Some(self.confirmed.to_string()),
            Some(self.total_discounts.clone()),
            Some(self.total_line_items_price.clone()),
            self.cart_token.clone(),
            Some(self.buyer_accepts_marketing.to_string()),
            Some(self.name.clone()),
            self.referring_site.clone(),
            self.landing_site.clone(),
            self.cancelled_at.clone(),
            self.cancel_reason.clone(),
            Some(self.processed_at.clone()),
            self.customer_id.map(|id| id.to_string()),
            self.user_id.map(|id| id.to_string()),
            self.location_id.map(|id| id.to_string()),
            self.source_identifier.clone(),
            self.source_url.clone(),
            self.device_id.map(|id| id.to_string()),
            self.phone.clone(),
            self.customer_locale.clone(),
            self.app_id.map(|id| id.to_string()),
            self.browser_ip.clone(),
            self.landing_site_ref.clone(),
            Some(self.order_number.clone()),
            Some(self.processing_method.clone()),
            self.checkout_id.map(|id| id.to_string()),
            Some(self.source_name.clone()),
            self.fulfillment_status.clone(),
            Some(self.tags.clone()),
            self.contact_email.clone(),
            Some(self.order_status_url.clone()),
            Some(self.presentment_currency.clone()),
        ]
    }
}

impl InventoryItem {
    pub fn to_row_vec(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.to_string()),
            self.sku.clone(),
            Some(self.created_at.clone()),
            Some(self.updated_at.clone()),
            Some(self.requires_shipping.to_string()),
            self.cost.clone(),
            self.country_code_of_origin.clone(),
            self.province_code_of_origin.clone(),
            self.harmonized_system_code.clone(),
            Some(self.tracked.to_string()),
        ]
    }
}

impl InventoryLevel {
    pub fn to_row_vec(&self) -> Vec<Option<String>> {
        vec![
            Some(self.inventory_item_id.to_string()),
            Some(self.location_id.to_string()),
            Some(self.available.to_string()),
            Some(self.updated_at.clone()),
        ]
    }
}

impl Shop {
    pub fn to_row_vec(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.to_string()),
            Some(self.name.clone()),
            Some(self.email.clone()),
            Some(self.domain.clone()),
            self.province.clone(),
            Some(self.country.clone()),
            self.address1.clone(),
            self.zip.clone(),
            self.city.clone(),
            self.source.clone(),
            self.phone.clone(),
            self.latitude.map(|lat| lat.to_string()),
            self.longitude.map(|lng| lng.to_string()),
            Some(self.primary_locale.clone()),
            self.address2.clone(),
            Some(self.created_at.clone()),
            Some(self.updated_at.clone()),
            Some(self.country_code.clone()),
            Some(self.country_name.clone()),
            Some(self.currency.clone()),
            self.customer_email.clone(),
            Some(self.timezone.clone()),
            Some(self.iana_timezone.clone()),
            Some(self.shop_owner.clone()),
            Some(self.money_format.clone()),
            Some(self.money_with_currency_format.clone()),
            Some(self.weight_unit.clone()),
            self.province_code.clone(),
            self.taxes_included.map(|tax| tax.to_string()),
            self.tax_shipping.map(|tax| tax.to_string()),
            Some(self.county_taxes.to_string()),
            Some(self.plan_display_name.clone()),
            Some(self.plan_name.clone()),
            Some(self.has_discounts.to_string()),
            Some(self.has_gift_cards.to_string()),
            Some(self.myshopify_domain.clone()),
            self.google_apps_domain.clone(),
            self.google_apps_login_enabled.map(|enabled| enabled.to_string()),
            Some(self.money_in_emails_format.clone()),
            Some(self.money_with_currency_in_emails_format.clone()),
            Some(self.eligible_for_payments.to_string()),
            Some(self.requires_extra_payments_agreement.to_string()),
            Some(self.password_enabled.to_string()),
            Some(self.has_storefront.to_string()),
            Some(self.eligible_for_card_reader_giveaway.to_string()),
            Some(self.finances.to_string()),
            Some(self.primary_location_id.to_string()),
            Some(self.checkout_api_supported.to_string()),
            Some(self.multi_location_enabled.to_string()),
            Some(self.setup_required.to_string()),
            Some(self.pre_launch_enabled.to_string()),
        ]
    }
}

impl Metafield {
    pub fn to_row_vec(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.to_string()),
            Some(self.namespace.clone()),
            Some(self.key.clone()),
            Some(self.value.clone()),
            self.description.clone(),
            Some(self.owner_id.to_string()),
            Some(self.created_at.clone()),
            Some(self.updated_at.clone()),
            Some(self.owner_resource.clone()),
            Some(self.value_type.clone()),
        ]
    }
}