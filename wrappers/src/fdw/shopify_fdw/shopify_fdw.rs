use crate::stats;
use chrono::DateTime;
use pgrx::{JsonB, datum::datetime_support::to_timestamp, pg_sys};
use reqwest::{self, Url, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use serde_json::{Map as JsonMap, Number, Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use supabase_wrappers::prelude::*;

use super::{ShopifyFdwError, ShopifyFdwResult};

const DEFAULT_MAX_RESPONSE_SIZE: usize = 10 * 1024 * 1024;
const DEFAULT_API_VERSION: &str = "2025-07";

#[derive(Clone)]
struct NestedConfig {
    parent_col: &'static str,
    parent_singular: &'static str,
    child_field: &'static str,
    child_is_connection: bool,
    parent_gid_type: &'static str,
}

#[derive(Clone)]
struct MutationConfig {
    create: &'static str,
    create_field: &'static str,
    create_id_path: &'static str,
    update: &'static str,
    update_field: &'static str,
    update_id_path: &'static str,
    update_id_in_input: bool,
    delete: &'static str,
    delete_field: &'static str,
    delete_id_path: &'static str,
}

#[derive(Clone, Default)]
struct ShopifyTableConfig {
    list_field: &'static str,
    singular_field: &'static str,
    gid_type: &'static str,
    page_size: i64,
    nested: Option<NestedConfig>,
    is_singleton: bool,
    id_required: bool,
    cols: Vec<(&'static str, &'static str, &'static str)>,
    mutations: Option<MutationConfig>,
    // Pre-built query strings — computed once at config-creation time.
    list_query: String,
    singular_query: String,
    nested_query: String,
}

type TableConfig = HashMap<&'static str, ShopifyTableConfig>;

fn add_queries(mut cfg: ShopifyTableConfig) -> ShopifyTableConfig {
    cfg.list_query = if !cfg.list_field.is_empty() {
        build_list_query(cfg.list_field, &cfg.cols, cfg.page_size)
    } else {
        String::new()
    };
    cfg.singular_query = if cfg.is_singleton {
        build_singleton_query(cfg.singular_field, &cfg.cols)
    } else if !cfg.singular_field.is_empty() {
        build_singular_query(cfg.singular_field, &cfg.cols)
    } else {
        String::new()
    };
    cfg.nested_query = if let Some(ref n) = cfg.nested {
        build_nested_query(
            n.parent_singular,
            n.child_field,
            n.child_is_connection,
            &cfg.cols,
        )
    } else {
        String::new()
    };
    cfg
}

#[allow(clippy::too_many_arguments)]
fn mc(
    create: &'static str,
    create_field: &'static str,
    create_id_path: &'static str,
    update: &'static str,
    update_field: &'static str,
    update_id_path: &'static str,
    update_id_in_input: bool,
    delete: &'static str,
    delete_field: &'static str,
    delete_id_path: &'static str,
) -> Option<MutationConfig> {
    Some(MutationConfig {
        create,
        create_field,
        create_id_path,
        update,
        update_field,
        update_id_path,
        update_id_in_input,
        delete,
        delete_field,
        delete_id_path,
    })
}

fn nc(
    parent_col: &'static str,
    parent_singular: &'static str,
    child_field: &'static str,
    child_is_connection: bool,
    parent_gid_type: &'static str,
) -> Option<NestedConfig> {
    Some(NestedConfig {
        parent_col,
        parent_singular,
        child_field,
        child_is_connection,
        parent_gid_type,
    })
}

fn create_table_config() -> TableConfig {
    HashMap::from([
        (
            "products",
            ShopifyTableConfig {
                list_field: "products",
                singular_field: "product",
                gid_type: "Product",
                page_size: 250,
                nested: None,
                is_singleton: false,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("title", "title", "text"),
                    ("handle", "handle", "text"),
                    ("status", "status", "text"),
                    ("vendor", "vendor", "text"),
                    ("product_type", "productType", "text"),
                    ("created_at", "createdAt", "timestamp"),
                    ("updated_at", "updatedAt", "timestamp"),
                ],
                mutations: mc(
                    PRODUCT_CREATE,
                    "productCreate",
                    "/product/id",
                    PRODUCT_UPDATE,
                    "productUpdate",
                    "/product/id",
                    true,
                    PRODUCT_DELETE,
                    "productDelete",
                    "/deletedProductId",
                ),
                ..Default::default()
            },
        ),
        (
            "customers",
            ShopifyTableConfig {
                list_field: "customers",
                singular_field: "customer",
                gid_type: "Customer",
                page_size: 250,
                nested: None,
                is_singleton: false,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("first_name", "firstName", "text"),
                    ("last_name", "lastName", "text"),
                    ("email", "email", "text"),
                    ("phone", "phone", "text"),
                    ("state", "state", "text"),
                    ("created_at", "createdAt", "timestamp"),
                    ("updated_at", "updatedAt", "timestamp"),
                ],
                mutations: mc(
                    CUSTOMER_CREATE,
                    "customerCreate",
                    "/customer/id",
                    CUSTOMER_UPDATE,
                    "customerUpdate",
                    "/customer/id",
                    true,
                    CUSTOMER_DELETE,
                    "customerDelete",
                    "/deletedCustomerId",
                ),
                ..Default::default()
            },
        ),
        (
            "orders",
            ShopifyTableConfig {
                list_field: "orders",
                singular_field: "order",
                gid_type: "Order",
                page_size: 250,
                nested: None,
                is_singleton: false,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("name", "name", "text"),
                    ("email", "email", "text"),
                    ("phone", "phone", "text"),
                    ("financial_status", "displayFinancialStatus", "text"),
                    ("fulfillment_status", "displayFulfillmentStatus", "text"),
                    ("note", "note", "text"),
                    ("test", "test", "bool"),
                    ("cancelled_at", "cancelledAt", "timestamp"),
                    ("closed_at", "closedAt", "timestamp"),
                    ("processed_at", "processedAt", "timestamp"),
                    ("created_at", "createdAt", "timestamp"),
                    ("updated_at", "updatedAt", "timestamp"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "locations",
            ShopifyTableConfig {
                list_field: "locations",
                singular_field: "location",
                gid_type: "Location",
                page_size: 250,
                nested: None,
                is_singleton: false,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("name", "name", "text"),
                    ("is_active", "isActive", "bool"),
                    ("created_at", "createdAt", "timestamp"),
                    ("updated_at", "updatedAt", "timestamp"),
                    ("deactivated_at", "deactivatedAt", "timestamp"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "draft_orders",
            ShopifyTableConfig {
                list_field: "draftOrders",
                singular_field: "draftOrder",
                gid_type: "DraftOrder",
                page_size: 250,
                nested: None,
                is_singleton: false,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("name", "name", "text"),
                    ("status", "status", "text"),
                    ("email", "email", "text"),
                    ("note", "note", "text"),
                    ("created_at", "createdAt", "timestamp"),
                    ("updated_at", "updatedAt", "timestamp"),
                    ("completed_at", "completedAt", "timestamp"),
                ],
                mutations: mc(
                    DRAFT_ORDER_CREATE,
                    "draftOrderCreate",
                    "/draftOrder/id",
                    DRAFT_ORDER_UPDATE,
                    "draftOrderUpdate",
                    "/draftOrder/id",
                    false,
                    DRAFT_ORDER_DELETE,
                    "draftOrderDelete",
                    "/deletedId",
                ),
                ..Default::default()
            },
        ),
        (
            "collections",
            ShopifyTableConfig {
                list_field: "collections",
                singular_field: "collection",
                gid_type: "Collection",
                page_size: 250,
                nested: None,
                is_singleton: false,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("title", "title", "text"),
                    ("handle", "handle", "text"),
                    ("description", "description", "text"),
                    ("sort_order", "sortOrder", "text"),
                    ("updated_at", "updatedAt", "timestamp"),
                ],
                mutations: mc(
                    COLLECTION_CREATE,
                    "collectionCreate",
                    "/collection/id",
                    COLLECTION_UPDATE,
                    "collectionUpdate",
                    "/collection/id",
                    true,
                    COLLECTION_DELETE,
                    "collectionDelete",
                    "/deletedCollectionId",
                ),
                ..Default::default()
            },
        ),
        (
            "product_variants",
            ShopifyTableConfig {
                list_field: "productVariants",
                singular_field: "productVariant",
                gid_type: "ProductVariant",
                page_size: 250,
                nested: None,
                is_singleton: false,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("title", "title", "text"),
                    ("sku", "sku", "text"),
                    ("barcode", "barcode", "text"),
                    ("price", "price", "text"),
                    ("compare_at_price", "compareAtPrice", "text"),
                    ("inventory_quantity", "inventoryQuantity", "bigint"),
                    ("taxable", "taxable", "bool"),
                    ("created_at", "createdAt", "timestamp"),
                    ("updated_at", "updatedAt", "timestamp"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "fulfillment_orders",
            ShopifyTableConfig {
                list_field: "fulfillmentOrders",
                singular_field: "fulfillmentOrder",
                gid_type: "FulfillmentOrder",
                page_size: 250,
                nested: None,
                is_singleton: false,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("status", "status", "text"),
                    ("request_status", "requestStatus", "text"),
                    ("created_at", "createdAt", "timestamp"),
                    ("updated_at", "updatedAt", "timestamp"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "fulfillments",
            ShopifyTableConfig {
                list_field: "",
                singular_field: "fulfillment",
                gid_type: "Fulfillment",
                page_size: 250,
                nested: nc("order_id", "order", "fulfillments", true, "Order"),
                is_singleton: false,
                id_required: false,
                cols: vec![
                    // gql_field "" = synthetic column injected from parent qual
                    ("order_id", "", "text"),
                    ("id", "id", "text"),
                    ("status", "status", "text"),
                    ("created_at", "createdAt", "timestamp"),
                    ("updated_at", "updatedAt", "timestamp"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "apps",
            ShopifyTableConfig {
                list_field: "",
                singular_field: "app",
                gid_type: "",
                page_size: 0,
                nested: None,
                is_singleton: true,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("title", "title", "text"),
                    ("handle", "handle", "text"),
                    ("description", "description", "text"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "business_entities",
            ShopifyTableConfig {
                list_field: "businessEntities",
                singular_field: "",
                gid_type: "BusinessEntity",
                page_size: 250,
                nested: None,
                is_singleton: false,
                id_required: false,
                cols: vec![("id", "id", "text"), ("name", "name", "text")],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "customer_payment_methods",
            ShopifyTableConfig {
                list_field: "",
                singular_field: "customerPaymentMethod",
                gid_type: "CustomerPaymentMethod",
                page_size: 0,
                nested: None,
                is_singleton: false,
                id_required: true,
                cols: vec![
                    ("id", "id", "text"),
                    ("is_revocable", "isRevocable", "bool"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "store_credit_accounts",
            ShopifyTableConfig {
                list_field: "",
                singular_field: "storeCreditAccount",
                gid_type: "StoreCreditAccount",
                page_size: 0,
                nested: None,
                is_singleton: false,
                id_required: true,
                cols: vec![("id", "id", "text")],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "refunds",
            ShopifyTableConfig {
                list_field: "",
                singular_field: "",
                gid_type: "Refund",
                page_size: 0,
                nested: nc("order_id", "order", "refunds", false, "Order"),
                is_singleton: false,
                id_required: false,
                cols: vec![
                    // gql_field "" = synthetic column injected from parent qual
                    ("order_id", "", "text"),
                    ("id", "id", "text"),
                    ("created_at", "createdAt", "timestamp"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "returns",
            ShopifyTableConfig {
                list_field: "",
                singular_field: "return",
                gid_type: "Return",
                page_size: 0,
                nested: None,
                is_singleton: false,
                id_required: true,
                cols: vec![
                    ("id", "id", "text"),
                    ("status", "status", "text"),
                    ("created_at", "createdAt", "timestamp"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "inventory_levels",
            ShopifyTableConfig {
                list_field: "",
                singular_field: "inventoryLevel",
                gid_type: "InventoryLevel",
                page_size: 0,
                nested: None,
                is_singleton: false,
                id_required: true,
                cols: vec![
                    ("id", "id", "text"),
                    ("available", "available", "bigint"),
                    ("updated_at", "updatedAt", "timestamp"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
        (
            "shop",
            ShopifyTableConfig {
                list_field: "",
                singular_field: "shop",
                gid_type: "",
                page_size: 0,
                nested: None,
                is_singleton: true,
                id_required: false,
                cols: vec![
                    ("id", "id", "text"),
                    ("name", "name", "text"),
                    ("email", "email", "text"),
                    ("currency_code", "currencyCode", "text"),
                    ("description", "description", "text"),
                ],
                mutations: None,
                ..Default::default()
            },
        ),
    ])
    .into_iter()
    .map(|(k, v)| (k, add_queries(v)))
    .collect()
}

static TABLE_CONFIG: LazyLock<TableConfig> = LazyLock::new(create_table_config);

// ─── Mutation query constants ────────────────────────────────────────────────

const PRODUCT_CREATE: &str = r#"
mutation productCreate($input: ProductInput!) {
  productCreate(input: $input) {
    product { id }
    userErrors { field message }
  }
}"#;

const PRODUCT_UPDATE: &str = r#"
mutation productUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id }
    userErrors { field message }
  }
}"#;

const PRODUCT_DELETE: &str = r#"
mutation productDelete($input: ProductDeleteInput!) {
  productDelete(input: $input) {
    deletedProductId
    userErrors { field message }
  }
}"#;

const CUSTOMER_CREATE: &str = r#"
mutation customerCreate($input: CustomerInput!) {
  customerCreate(input: $input) {
    customer { id }
    userErrors { field message }
  }
}"#;

const CUSTOMER_UPDATE: &str = r#"
mutation customerUpdate($input: CustomerInput!) {
  customerUpdate(input: $input) {
    customer { id }
    userErrors { field message }
  }
}"#;

const CUSTOMER_DELETE: &str = r#"
mutation customerDelete($input: CustomerDeleteInput!) {
  customerDelete(input: $input) {
    deletedCustomerId
    userErrors { field message }
  }
}"#;

const DRAFT_ORDER_CREATE: &str = r#"
mutation draftOrderCreate($input: DraftOrderInput!) {
  draftOrderCreate(input: $input) {
    draftOrder { id }
    userErrors { field message }
  }
}"#;

const DRAFT_ORDER_UPDATE: &str = r#"
mutation draftOrderUpdate($id: ID!, $input: DraftOrderInput!) {
  draftOrderUpdate(id: $id, input: $input) {
    draftOrder { id }
    userErrors { field message }
  }
}"#;

const DRAFT_ORDER_DELETE: &str = r#"
mutation draftOrderDelete($input: DraftOrderDeleteInput!) {
  draftOrderDelete(input: $input) {
    deletedId
    userErrors { field message }
  }
}"#;

const COLLECTION_CREATE: &str = r#"
mutation collectionCreate($input: CollectionInput!) {
  collectionCreate(input: $input) {
    collection { id }
    userErrors { field message }
  }
}"#;

const COLLECTION_UPDATE: &str = r#"
mutation collectionUpdate($input: CollectionInput!) {
  collectionUpdate(input: $input) {
    collection { id }
    userErrors { field message }
  }
}"#;

const COLLECTION_DELETE: &str = r#"
mutation collectionDelete($input: CollectionDeleteInput!) {
  collectionDelete(input: $input) {
    deletedCollectionId
    userErrors { field message }
  }
}"#;

// ─── Helpers ────────────────────────────────────────────────────────────────

fn normalize_gid(gid_type: &str, id: &str) -> String {
    if id.starts_with("gid://") {
        id.to_string()
    } else {
        format!("gid://shopify/{gid_type}/{id}")
    }
}

fn parse_timestamp_cell(s: &str) -> Option<Cell> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| Cell::Timestamp(to_timestamp(dt.timestamp() as f64).to_utc()))
}

fn cell_from_json(value: &JsonValue, pg_type: &str) -> Option<Cell> {
    match pg_type {
        "bool" => value.as_bool().map(Cell::Bool),
        "bigint" => value.as_i64().map(Cell::I64),
        "text" => value.as_str().map(|s| Cell::String(s.to_owned())),
        "timestamp" => value.as_str().and_then(parse_timestamp_cell),
        _ => None,
    }
}

fn gql_fields(cols: &[(&str, &str, &str)]) -> String {
    cols.iter()
        .filter(|(_, gql, _)| !gql.is_empty())
        .map(|(_, gql, _)| *gql)
        .collect::<Vec<_>>()
        .join("\n        ")
}

fn build_list_query(list_field: &str, cols: &[(&str, &str, &str)], page_size: i64) -> String {
    let fields = gql_fields(cols);
    format!(
        r#"query($cursor: String) {{
  {list_field}(first: {page_size}, after: $cursor) {{
    nodes {{
        {fields}
    }}
    pageInfo {{ hasNextPage endCursor }}
  }}
}}"#
    )
}

fn build_singular_query(singular_field: &str, cols: &[(&str, &str, &str)]) -> String {
    let fields = gql_fields(cols);
    format!(
        r#"query($id: ID!) {{
  {singular_field}(id: $id) {{
    {fields}
  }}
}}"#
    )
}

fn build_singleton_query(field: &str, cols: &[(&str, &str, &str)]) -> String {
    let fields = gql_fields(cols);
    format!(
        r#"query {{
  {field} {{
    {fields}
  }}
}}"#
    )
}

fn build_nested_query(
    parent_singular: &str,
    child_field: &str,
    child_is_connection: bool,
    cols: &[(&str, &str, &str)],
) -> String {
    let fields = gql_fields(cols);
    if child_is_connection {
        format!(
            r#"query($parentId: ID!) {{
  {parent_singular}(id: $parentId) {{
    {child_field}(first: 250) {{
      nodes {{
        {fields}
      }}
    }}
  }}
}}"#
        )
    } else {
        format!(
            r#"query($parentId: ID!) {{
  {parent_singular}(id: $parentId) {{
    {child_field} {{
      {fields}
    }}
  }}
}}"#
        )
    }
}

/// Parse a GraphQL response body, checking for errors. Returns the `data` value.
fn parse_graphql_response(body: &str, max_size: usize) -> ShopifyFdwResult<JsonValue> {
    if body.len() > max_size {
        return Err(ShopifyFdwError::ResponseTooLarge(body.len(), max_size));
    }
    let value: JsonValue = serde_json::from_str(body)?;

    // Check top-level GraphQL errors
    if let Some(first) = value
        .get("errors")
        .and_then(|e| e.as_array())
        .and_then(|a| a.first())
    {
        let code = first
            .pointer("/extensions/code")
            .and_then(|c| c.as_str())
            .unwrap_or("");
        if code == "THROTTLED" || code == "MAX_COST_EXCEEDED" {
            let restore_rate = value
                .pointer("/extensions/cost/throttleStatus/restoreRate")
                .and_then(|v| v.as_f64())
                .map(|r| format!(" (restoreRate: {r} points/s)"))
                .unwrap_or_default();
            return Err(ShopifyFdwError::Throttled(restore_rate));
        }
        let msg = first
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown GraphQL error");
        return Err(ShopifyFdwError::GraphQLError(msg.to_string()));
    }

    // Also check extensions.cost.throttleStatus
    if value
        .pointer("/extensions/cost/throttleStatus")
        .and_then(|s| s.as_str())
        == Some("THROTTLED")
    {
        return Err(ShopifyFdwError::Throttled(String::new()));
    }

    value
        .get("data")
        .cloned()
        .ok_or(ShopifyFdwError::InvalidResponse)
}

/// Convert a JSON node object to a Row using the column config.
fn node_to_row(
    node: &JsonValue,
    cols: &[(&str, &str, &str)],
    tgt_cols: &[Column],
) -> ShopifyFdwResult<Row> {
    let mut row = Row::new();
    for tgt_col in tgt_cols {
        if let Some((pg_col, gql_field, pg_type)) =
            cols.iter().find(|(pg, _, _)| *pg == tgt_col.name)
        {
            // Empty gql_field = synthetic column; caller must inject the value separately.
            if gql_field.is_empty() {
                continue;
            }
            let cell = node
                .get(*gql_field)
                .and_then(|v| cell_from_json(v, pg_type));
            row.push(pg_col, cell);
        } else if tgt_col.name == "attrs" {
            let attrs = serde_json::from_str(&node.to_string())?;
            row.push("attrs", Some(Cell::Json(JsonB(attrs))));
        }
    }
    Ok(row)
}

/// Parse a list response into rows plus pagination info.
fn parse_list_response(
    data: &JsonValue,
    list_field: &str,
    cols: &[(&str, &str, &str)],
    tgt_cols: &[Column],
) -> ShopifyFdwResult<(Vec<Row>, Option<String>, bool)> {
    let connection = data
        .get(list_field)
        .ok_or(ShopifyFdwError::InvalidResponse)?;
    let nodes = connection
        .get("nodes")
        .and_then(|n| n.as_array())
        .ok_or(ShopifyFdwError::InvalidResponse)?;

    let mut rows = Vec::new();
    for node in nodes {
        rows.push(node_to_row(node, cols, tgt_cols)?);
    }

    let has_more = connection
        .pointer("/pageInfo/hasNextPage")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let cursor = connection
        .pointer("/pageInfo/endCursor")
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok((rows, cursor, has_more))
}

/// Parse a singular response (data.<field> is a single object).
fn parse_singular_response(
    data: &JsonValue,
    singular_field: &str,
    cols: &[(&str, &str, &str)],
    tgt_cols: &[Column],
) -> ShopifyFdwResult<Vec<Row>> {
    match data.get(singular_field) {
        None | Some(JsonValue::Null) => Ok(vec![]),
        Some(node) => Ok(vec![node_to_row(node, cols, tgt_cols)?]),
    }
}

/// Parse a nested list response: data.<parent>.<child> = nodes[] or direct [].
fn parse_nested_response(
    data: &JsonValue,
    parent_field: &str,
    child_field: &str,
    child_is_connection: bool,
    cols: &[(&str, &str, &str)],
    tgt_cols: &[Column],
) -> ShopifyFdwResult<Vec<Row>> {
    let parent = data
        .get(parent_field)
        .ok_or(ShopifyFdwError::InvalidResponse)?;
    let empty: &[JsonValue] = &[];
    let items: &[JsonValue] = if child_is_connection {
        parent
            .get(child_field)
            .and_then(|c| c.get("nodes"))
            .and_then(|n| n.as_array())
            .map(Vec::as_slice)
            .unwrap_or(empty)
    } else {
        parent
            .get(child_field)
            .and_then(|c| c.as_array())
            .map(Vec::as_slice)
            .unwrap_or(empty)
    };
    items
        .iter()
        .map(|node| node_to_row(node, cols, tgt_cols))
        .collect()
}

/// Convert a Row to a JSON input map (camelCase keys from gql_field_name).
fn row_to_gql_input(
    row: &Row,
    cols: &[(&str, &str, &str)],
) -> ShopifyFdwResult<JsonMap<String, JsonValue>> {
    let mut map = JsonMap::new();
    for (col_name, cell) in row.iter() {
        let Some(cell) = cell else { continue };
        if col_name == "attrs"
            && let Cell::Json(v) = cell
        {
            if let Some(m) = v.0.clone().as_object_mut() {
                map.append(m);
            }
            continue;
        }
        let gql_key = cols
            .iter()
            .find(|(pg, _, _)| *pg == col_name)
            .map(|(_, gql, _)| *gql)
            .unwrap_or(col_name);
        let value = match cell {
            Cell::Bool(v) => JsonValue::Bool(*v),
            Cell::I64(v) => JsonValue::Number(Number::from(*v)),
            Cell::String(v) => JsonValue::String(v.clone()),
            _ => return Err(ShopifyFdwError::UnsupportedColumnType(col_name.to_string())),
        };
        map.insert(gql_key.to_string(), value);
    }
    Ok(map)
}

fn check_user_errors(
    data: &JsonValue,
    mutation_field: &str,
    id_path: &str,
) -> ShopifyFdwResult<Option<String>> {
    let Some(result) = data.get(mutation_field) else {
        return Ok(None);
    };
    if let Some(errors) = result.get("userErrors").and_then(|e| e.as_array())
        && !errors.is_empty()
    {
        let msg = errors
            .iter()
            .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(ShopifyFdwError::MutationError(msg));
    }
    Ok(result
        .pointer(id_path)
        .and_then(|v| v.as_str())
        .map(String::from))
}

// ─── Stats helpers ────────────────────────────────────────────────────────────

#[inline]
fn get_stats_metadata() -> JsonB {
    stats::get_metadata(ShopifyFdw::FDW_NAME).unwrap_or(JsonB(json!({
        "request_cnt": 0i64,
    })))
}

#[inline]
fn set_stats_metadata(stats_metadata: JsonB) {
    stats::set_metadata(ShopifyFdw::FDW_NAME, Some(stats_metadata));
}

#[inline]
fn inc_stats_request_cnt(stats_metadata: &mut JsonB) -> ShopifyFdwResult<()> {
    if let Some(v) = stats_metadata.0.get_mut("request_cnt") {
        *v = (v.as_i64().ok_or_else(|| {
            ShopifyFdwError::InvalidStats("`request_cnt` is not a number".into())
        })? + 1)
            .into();
    }
    Ok(())
}

// ─── HTTP client ──────────────────────────────────────────────────────────────

fn create_client(access_token: &str) -> ShopifyFdwResult<ClientWithMiddleware> {
    let mut headers = header::HeaderMap::new();
    let mut token_value = header::HeaderValue::from_str(access_token)?;
    token_value.set_sensitive(true);
    headers.insert("X-Shopify-Access-Token", token_value);
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    Ok(ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build())
}

// ─── FDW struct ───────────────────────────────────────────────────────────────

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/shopify_fdw",
    error_type = "ShopifyFdwError"
)]
pub(crate) struct ShopifyFdw {
    rt: Runtime,
    endpoint: Url,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
    obj: String,
    rowid_col: String,
    iter_idx: usize,
    table_config: &'static TableConfig,
    max_response_size: usize,
}

impl ShopifyFdw {
    const FDW_NAME: &'static str = "ShopifyFdw";

    fn make_gql_request(
        &self,
        client: &ClientWithMiddleware,
        query: &str,
        variables: JsonValue,
        stats_metadata: &mut JsonB,
    ) -> ShopifyFdwResult<JsonValue> {
        let payload = json!({ "query": query, "variables": variables });
        inc_stats_request_cnt(stats_metadata)?;
        let max_size = self.max_response_size;
        let resp = self
            .rt
            .block_on(client.post(self.endpoint.clone()).json(&payload).send())?;
        let content_len = resp.content_length().unwrap_or(0);
        stats::inc_stats(Self::FDW_NAME, stats::Metric::BytesIn, content_len as i64);
        if content_len > max_size as u64 {
            return Err(ShopifyFdwError::ResponseTooLarge(
                content_len as usize,
                max_size,
            ));
        }
        let body = resp
            .error_for_status()
            .and_then(|r| self.rt.block_on(r.text()))
            .map_err(reqwest_middleware::Error::from)?;
        parse_graphql_response(&body, max_size)
    }

    fn get_table_config(&self, obj: &str) -> ShopifyFdwResult<&'static ShopifyTableConfig> {
        self.table_config
            .get(obj)
            .ok_or_else(|| ShopifyFdwError::ObjectNotImplemented(obj.to_string()))
    }

    fn get_id_qual(quals: &[Qual]) -> Option<&str> {
        quals.iter().find_map(|q| {
            if q.field == "id"
                && q.operator == "="
                && !q.use_or
                && let Value::Cell(Cell::String(id)) = &q.value
            {
                return Some(id.as_str());
            }
            None
        })
    }

    fn get_string_qual<'a>(quals: &'a [Qual], field: &str) -> Option<&'a str> {
        quals.iter().find_map(|q| {
            if q.field == field
                && q.operator == "="
                && !q.use_or
                && let Value::Cell(Cell::String(s)) = &q.value
            {
                return Some(s.as_str());
            }
            None
        })
    }
}

impl ForeignDataWrapper<ShopifyFdwError> for ShopifyFdw {
    fn new(server: ForeignServer) -> ShopifyFdwResult<Self> {
        let shop = server
            .options
            .get("shop")
            .map(|s| s.to_owned())
            .unwrap_or_default();
        let api_version = server
            .options
            .get("api_version")
            .map(|s| s.as_str())
            .unwrap_or(DEFAULT_API_VERSION);

        // If api_url is provided, use it directly; otherwise build from shop + version.
        let endpoint = if let Some(url) = server.options.get("api_url") {
            let url = if url.ends_with('/') {
                url.clone()
            } else {
                format!("{url}/")
            };
            format!("{url}admin/api/{api_version}/graphql.json")
        } else {
            format!("https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json")
        };

        let client = match server.options.get("access_token") {
            Some(token) => Some(create_client(token)),
            None => server
                .options
                .get("access_token_id")
                .and_then(|id| get_vault_secret(id))
                .or_else(|| {
                    server
                        .options
                        .get("access_token_name")
                        .and_then(|name| get_vault_secret_by_name(name))
                })
                .map(|token| create_client(&token))
                .or_else(|| {
                    report_error(
                        pgrx::PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        "one of access_token, access_token_id, or access_token_name is required",
                    );
                    None
                }),
        }
        .transpose()?;

        let parsed_endpoint = Url::parse(&endpoint)?;
        let is_loopback = parsed_endpoint
            .host_str()
            .map(|h| h == "localhost" || h == "127.0.0.1" || h == "::1")
            .unwrap_or(false);
        if parsed_endpoint.scheme() != "https" && !is_loopback {
            return Err(ShopifyFdwError::OptionsError(
                OptionsError::OptionParsingError {
                    option_name: "api_url".to_string(),
                    type_name: "https URL",
                },
            ));
        }

        let max_response_size = server
            .options
            .get("max_response_size")
            .map(|s| {
                s.parse::<usize>().map_err(|_| {
                    ShopifyFdwError::OptionsError(OptionsError::OptionParsingError {
                        option_name: "max_response_size".to_string(),
                        type_name: "usize",
                    })
                })
            })
            .transpose()?
            .unwrap_or(DEFAULT_MAX_RESPONSE_SIZE);

        if max_response_size == 0 || max_response_size > 100 * 1024 * 1024 {
            return Err(ShopifyFdwError::OptionsError(
                OptionsError::OptionParsingError {
                    option_name: "max_response_size".to_string(),
                    type_name: "integer between 1 and 104857600",
                },
            ));
        }

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(ShopifyFdw {
            rt: create_async_runtime()?,
            endpoint: parsed_endpoint,
            client,
            scan_result: None,
            obj: String::default(),
            rowid_col: String::default(),
            iter_idx: 0,
            table_config: &TABLE_CONFIG,
            max_response_size,
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> ShopifyFdwResult<()> {
        let obj = require_option("object", options)?;
        self.iter_idx = 0;

        let Some(client) = &self.client else {
            return Ok(());
        };
        let cfg = self.get_table_config(obj)?;
        let mut result = Vec::new();
        let mut stats_metadata = get_stats_metadata();

        // ── singleton: shop, app ──────────────────────────────────────────────
        if cfg.is_singleton {
            let data =
                self.make_gql_request(client, &cfg.singular_query, json!({}), &mut stats_metadata)?;
            let rows = parse_singular_response(&data, cfg.singular_field, &cfg.cols, columns)?;
            result.extend(rows);

        // ── nested table (fulfillments, refunds) ──────────────────────────────
        } else if let Some(ref n) = cfg.nested {
            match Self::get_string_qual(quals, n.parent_col) {
                None => {
                    pgrx::notice!(
                        "table '{}' requires WHERE {} = '...' qual",
                        obj,
                        n.parent_col
                    );
                }
                Some(parent_id) => {
                    let gid = normalize_gid(n.parent_gid_type, parent_id);
                    let data = self.make_gql_request(
                        client,
                        &cfg.nested_query,
                        json!({ "parentId": gid }),
                        &mut stats_metadata,
                    )?;
                    let mut rows = parse_nested_response(
                        &data,
                        n.parent_singular,
                        n.child_field,
                        n.child_is_connection,
                        &cfg.cols,
                        columns,
                    )?;
                    if columns.iter().any(|c| c.name == n.parent_col) {
                        for row in &mut rows {
                            row.push(n.parent_col, Some(Cell::String(gid.clone())));
                        }
                    }
                    result.extend(rows);
                }
            }

        // ── id-required or id= optimisation → singular fetch ──────────────────
        } else if cfg.id_required || Self::get_id_qual(quals).is_some() {
            let id = if cfg.id_required {
                match Self::get_id_qual(quals) {
                    None => {
                        pgrx::notice!("table '{}' requires WHERE id = '...' qual", obj);
                        return Ok(());
                    }
                    Some(id) => id,
                }
            } else {
                Self::get_id_qual(quals).unwrap()
            };
            if !cfg.singular_field.is_empty() {
                let gid = if cfg.gid_type.is_empty() {
                    id.to_string()
                } else {
                    normalize_gid(cfg.gid_type, id)
                };
                let data = self.make_gql_request(
                    client,
                    &cfg.singular_query,
                    json!({ "id": gid }),
                    &mut stats_metadata,
                )?;
                let rows = parse_singular_response(&data, cfg.singular_field, &cfg.cols, columns)?;
                result.extend(rows);
            }

        // ── standard list with cursor pagination ──────────────────────────────
        } else {
            let max_rows = limit.as_ref().map(|l| l.offset + l.count);
            let mut cursor: Option<String> = None;
            loop {
                let vars = json!({ "cursor": cursor });
                let data =
                    self.make_gql_request(client, &cfg.list_query, vars, &mut stats_metadata)?;
                let (rows, next_cursor, has_more) =
                    parse_list_response(&data, cfg.list_field, &cfg.cols, columns)?;
                let done = !has_more || next_cursor.is_none();
                result.extend(rows);
                if done {
                    break;
                }
                if let Some(max) = max_rows
                    && result.len() as i64 >= max
                {
                    break;
                }
                cursor = next_cursor;
            }
        }

        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, result.len() as i64);
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, result.len() as i64);
        set_stats_metadata(stats_metadata);

        self.scan_result = Some(result);
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> ShopifyFdwResult<Option<()>> {
        if let Some(ref mut result) = self.scan_result
            && self.iter_idx < result.len()
        {
            row.replace_with(result[self.iter_idx].clone());
            self.iter_idx += 1;
            return Ok(Some(()));
        }
        Ok(None)
    }

    fn re_scan(&mut self) -> ShopifyFdwResult<()> {
        self.iter_idx = 0;
        Ok(())
    }

    fn end_scan(&mut self) -> ShopifyFdwResult<()> {
        self.scan_result.take();
        Ok(())
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> ShopifyFdwResult<()> {
        self.obj = require_option("object", options)?.to_string();
        self.rowid_col = require_option("rowid_column", options)?.to_string();
        Ok(())
    }

    fn insert(&mut self, src: &Row) -> ShopifyFdwResult<()> {
        let Some(ref client) = self.client else {
            return Ok(());
        };
        let cfg = self.get_table_config(&self.obj)?;
        let Some(ref m) = cfg.mutations else {
            return Err(ShopifyFdwError::MutationError(format!(
                "object '{}' does not support INSERT",
                self.obj
            )));
        };
        let input = row_to_gql_input(src, &cfg.cols)?;
        let mut stats_metadata = get_stats_metadata();
        let data = self.make_gql_request(
            client,
            m.create,
            json!({ "input": input }),
            &mut stats_metadata,
        )?;
        if let Some(id) = check_user_errors(&data, m.create_field, m.create_id_path)? {
            report_info(&format!("inserted {} {}", self.obj, id));
        }
        set_stats_metadata(stats_metadata);
        Ok(())
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> ShopifyFdwResult<()> {
        let Some(ref client) = self.client else {
            return Ok(());
        };
        let cfg = self.get_table_config(&self.obj)?;
        let Some(ref m) = cfg.mutations else {
            return Err(ShopifyFdwError::MutationError(format!(
                "object '{}' does not support UPDATE",
                self.obj
            )));
        };
        let Cell::String(rowid_str) = rowid else {
            return Ok(());
        };
        let gid = if cfg.gid_type.is_empty() {
            rowid_str.clone()
        } else {
            normalize_gid(cfg.gid_type, rowid_str)
        };
        let mut input = row_to_gql_input(new_row, &cfg.cols)?;
        let variables = if m.update_id_in_input {
            input.insert("id".to_string(), JsonValue::String(gid));
            json!({ "input": input })
        } else {
            json!({ "id": gid, "input": input })
        };
        let mut stats_metadata = get_stats_metadata();
        let data = self.make_gql_request(client, m.update, variables, &mut stats_metadata)?;
        if let Some(id) = check_user_errors(&data, m.update_field, m.update_id_path)? {
            report_info(&format!("updated {} {}", self.obj, id));
        }
        set_stats_metadata(stats_metadata);
        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> ShopifyFdwResult<()> {
        let Some(ref client) = self.client else {
            return Ok(());
        };
        let cfg = self.get_table_config(&self.obj)?;
        let Some(ref m) = cfg.mutations else {
            return Err(ShopifyFdwError::MutationError(format!(
                "object '{}' does not support DELETE",
                self.obj
            )));
        };
        let Cell::String(rowid_str) = rowid else {
            return Ok(());
        };
        let gid = if cfg.gid_type.is_empty() {
            rowid_str.clone()
        } else {
            normalize_gid(cfg.gid_type, rowid_str)
        };
        let mut stats_metadata = get_stats_metadata();
        let data = self.make_gql_request(
            client,
            m.delete,
            json!({ "input": { "id": gid } }),
            &mut stats_metadata,
        )?;
        if let Some(id) = check_user_errors(&data, m.delete_field, m.delete_id_path)? {
            report_info(&format!("deleted {} {}", self.obj, id));
        }
        set_stats_metadata(stats_metadata);
        Ok(())
    }

    fn import_foreign_schema(
        &mut self,
        stmt: ImportForeignSchemaStmt,
    ) -> ShopifyFdwResult<Vec<String>> {
        // Build a reverse map: table_name → (table_name, cols, writable) from table_config
        let all_tables: HashSet<&str> = self.table_config.keys().copied().collect();
        let table_list: HashSet<&str> = stmt.table_list.iter().map(|t| t.as_str()).collect();
        let selected: HashSet<&str> = match stmt.list_type {
            ImportSchemaType::FdwImportSchemaAll => all_tables,
            ImportSchemaType::FdwImportSchemaLimitTo => {
                all_tables.intersection(&table_list).copied().collect()
            }
            ImportSchemaType::FdwImportSchemaExcept => {
                all_tables.difference(&table_list).copied().collect()
            }
        };
        let ret = selected
            .iter()
            .filter_map(|tbl| {
                self.table_config.get(*tbl).map(|cfg| {
                    let cols = cfg
                        .cols
                        .iter()
                        .map(|(pg_col, _, pg_type)| format!("{pg_col} {pg_type}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let rowid_option = if cfg.mutations.is_some() {
                        ", rowid_column 'id'"
                    } else {
                        ""
                    };
                    format!(
                        "create foreign table if not exists {tbl} ({cols}, attrs jsonb) \
                         server {server} options (object '{tbl}'{rowid_option})",
                        server = stmt.server_name,
                    )
                })
            })
            .collect();
        Ok(ret)
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> ShopifyFdwResult<()> {
        if let Some(oid) = catalog
            && oid == FOREIGN_TABLE_RELATION_ID
        {
            check_options_contain(&options, "object")?;
        }
        Ok(())
    }
}
