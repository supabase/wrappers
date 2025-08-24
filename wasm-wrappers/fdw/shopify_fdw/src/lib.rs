#[allow(warnings)]
mod bindings;
mod field_maps;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{
            Cell, Column, Context, FdwError, FdwResult, ImportForeignSchemaStmt, OptionsType, Row,
            TypeOid, Value,
        },
        utils,
    },
};
use field_maps::get_field_map;

#[derive(Debug, Default)]
struct ShopifyFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    object: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
    src_cursor: Option<String>,
}

static mut INSTANCE: *mut ShopifyFdw = std::ptr::null_mut::<ShopifyFdw>();
static FDW_NAME: &str = "ShopifyFdw";

// max number of rows returned per request
static BATCH_SIZE: usize = 250;

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

    fn object_singular(&self) -> &str {
        match self.object.as_str() {
            "products" => "product",
            "customers" => "customer",
            "orders" => "order",
            "locations" => "location",
            "draftOrders" => "draftOrder",
            "collections" => "collection",
            "productVariants" => "productVariant",
            "fulfillmentOrders" => "fulfillmentOrder",
            _ => self.object.as_str(),
        }
    }

    // convert Shopify response data field to a cell
    fn src_field_to_cell(
        &self,
        src_row: &JsonValue,
        tgt_col: &Column,
    ) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // put all properties into 'attrs' JSON column
        if &tgt_col_name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        let src = src_row
            .as_object()
            .and_then(|v| v.get(&tgt_col_name))
            .ok_or(format!("source column '{}' not found", tgt_col_name))?;

        // column type mapping
        let cell = match tgt_col.type_oid() {
            TypeOid::Bool => src.as_bool().map(Cell::Bool),
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            TypeOid::I64 => src
                .as_i64()
                .or_else(|| src.as_str().map(|v| v.parse::<i64>().unwrap_or_default()))
                .map(Cell::I64),
            TypeOid::Numeric => src.as_f64().map(Cell::Numeric),
            TypeOid::String => src.as_str().map(|v| Cell::String(v.to_owned())),
            TypeOid::Date => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(s)?;
                    Some(Cell::Date(ts / 1_000_000))
                } else {
                    None
                }
            }
            TypeOid::Timestamp => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(s)?;
                    Some(Cell::Timestamp(ts))
                } else {
                    None
                }
            }
            TypeOid::Timestamptz => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(s)?;
                    Some(Cell::Timestamptz(ts))
                } else {
                    None
                }
            }
            TypeOid::Json => {
                if src.is_null() {
                    None
                } else {
                    Some(Cell::Json(src.to_string()))
                }
            }
            _ => {
                return Err(format!(
                    "target column '{}' type is not supported",
                    tgt_col_name
                ));
            }
        };

        Ok(cell)
    }

    // create a request instance
    fn create_request(&self, ctx: &Context) -> Result<http::Request, FdwError> {
        // make cursor
        let cursor = if let Some(ref cursor) = self.src_cursor {
            format!("\\\"{cursor}\\\"")
        } else {
            "null".to_string()
        };

        // make fields
        let field_map = get_field_map(&self.object);
        let mut fragments: Vec<String> = Vec::new();
        let tgt_cols = ctx
            .get_columns()
            .iter()
            .filter(|c| c.name() != "attrs")
            .map(|c| {
                let col_name = c.name();
                let fragment = field_map
                    .get(&col_name)
                    .map(|v| {
                        fragments.extend(v.2.clone());
                        v.1.clone()
                    })
                    .unwrap_or_default();
                format!("{} {}", col_name, fragment)
            })
            .collect::<Vec<_>>()
            .join(" ");

        // make framents
        fragments.sort();
        fragments.dedup();
        let fragments = fragments.join(" ");

        let quals = ctx.get_quals();

        // get id filter from quals
        let id_filter = quals.iter().find_map(|qual| {
            if qual.operator().as_str() == "=" && !qual.use_or() && qual.field().as_str() == "id" {
                if let Value::Cell(Cell::String(id)) = qual.value() {
                    return Some(id.clone());
                }
            }
            None
        });
        let id_filter = match self.object.as_str() {
            "app" | "businessEntities" | "shop" => Some(String::new()),
            "customerPaymentMethod"
            | "storeCreditAccount"
            | "fulfillment"
            | "refund"
            | "return"
            | "inventoryLevel" => {
                if id_filter.is_none() {
                    return Err(
                        "column 'id' is not specified with an value in query condition".to_string(),
                    );
                }
                id_filter
            }
            _ => id_filter,
        };

        // combine all to make the final query
        let query = if let Some(id_filter) = id_filter {
            if id_filter.is_empty() {
                format!(
                    r#"{{
                    "query": "
                        {fragments}
                        query QueryObject {{
                            {} {{
                                {tgt_cols}
                            }} 
                        }}
                    "
                }}"#,
                    self.object
                )
            } else {
                format!(
                    r#"{{
                    "query": "
                        {fragments}
                        query QueryObject($input: ID!) {{
                            {}(id: $input) {{
                                {tgt_cols}
                            }} 
                        }}
                    ",
                    "variables": {{
                        "input": "{id_filter}"
                    }}
                }}"#,
                    self.object_singular()
                )
            }
        } else {
            format!(
                r#"{{
                "query": "
                    {fragments}
                    query {{
                        {}(first: {BATCH_SIZE}, after: {cursor}) {{
                            nodes {{ {tgt_cols} }}
                            pageInfo {{ hasNextPage endCursor }}
                        }} 
                    }}
                "
            }}"#,
                self.object
            )
        };
        let body = query.split_ascii_whitespace().collect::<Vec<_>>().join(" ");

        Ok(http::Request {
            method: http::Method::Post,
            url: self.base_url.clone(),
            headers: self.headers.clone(),
            body,
        })
    }

    // fetch source data rows from Shopify API
    fn fetch_source_data(&mut self, ctx: &Context) -> FdwResult {
        self.src_rows.clear();
        self.src_idx = 0;

        // make request to remote endpoint
        let req = self.create_request(ctx)?;
        let resp = http::post(&req)?;
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

        // transform response to json
        let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        // return when there are error messages in response
        if let Some(msg) = resp_json
            .pointer("/errors/0/message")
            .and_then(|v| v.as_str())
        {
            return Err(msg.to_owned());
        }

        let resp_data = resp_json
            .pointer(&format!("/data/{}/nodes", self.object))
            .or_else(|| resp_json.pointer(&format!("/data/{}", self.object)))
            .or_else(|| resp_json.pointer(&format!("/data/{}", self.object_singular())))
            .and_then(|v| {
                if v.is_array() {
                    v.as_array().cloned()
                } else {
                    Some(vec![v.clone()])
                }
            })
            .ok_or("cannot get query result data")?;
        self.src_rows.extend(resp_data);

        // save pagination next cursor if any
        let has_next_page = resp_json
            .pointer(&format!("/data/{}/pageInfo/hasNextPage", self.object))
            .and_then(|v| v.as_bool())
            .unwrap_or_default();
        self.src_cursor = if has_next_page {
            resp_json
                .pointer(&format!("/data/{}/pageInfo/endCursor", self.object))
                .and_then(|v| v.as_str())
                .map(|v| v.to_owned())
        } else {
            None
        };

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

        Ok(())
    }
}

impl Guest for ShopifyFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        let opts = ctx.get_options(&OptionsType::Server);
        let shop = opts.require("shop")?;

        // make up base url from server options
        this.base_url = format!(
            "https://{}.myshopify.com/admin/api/2025-07/graphql.json",
            shop
        );
        this.base_url = opts.require_or("api_url", &this.base_url);

        // retrieve api access token
        let token = match opts.get("access_token") {
            Some(token) => token,
            None => {
                let token_id = opts.require("access_token_id")?;
                utils::get_vault_secret(&token_id).unwrap_or_default()
            }
        };

        // Shopify api authentication
        // ref: https://shopify.dev/docs/api/admin-graphql/latest#authentication
        this.headers
            .push(("user-agent".to_owned(), "Wrappers Shopify FDW".to_string()));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("x-shopify-access-token".to_owned(), token));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);
        this.object = opts.require("object")?;
        this.fetch_source_data(ctx)
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // if all rows in local batch buffer are consumed
        while this.src_idx >= this.src_rows.len() {
            let consumed_cnt = this.src_rows.len();

            // local batch buffer isn't fully filled or no next page cursor,
            // means no more source records on remote, stop the iteration scan
            if consumed_cnt < BATCH_SIZE || this.src_cursor.is_none() {
                return Ok(None);
            }

            // otherwise, make a new request for the next page
            this.fetch_source_data(ctx)?;

            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, consumed_cnt as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, consumed_cnt as i64);
        }

        // convert source row to Postgres row
        let src_row = &this.src_rows[this.src_idx];
        for tgt_col in ctx.get_columns() {
            let cell = this.src_field_to_cell(src_row, &tgt_col)?;
            row.push(cell.as_ref());
        }
        this.src_idx += 1;

        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.fetch_source_data(ctx)
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("modify on foreign table is not supported".to_owned())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Ok(())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        let mut ret = Vec::new();
        let objects = [
            "app",
            "businessEntities",
            "collections",
            "customerPaymentMethod",
            "customers",
            "draftOrders",
            "fulfillment",
            "fulfillmentOrders",
            "inventoryLevel",
            "locations",
            "orders",
            "productVariants",
            "products",
            "refund",
            "return",
            "shop",
            "storeCreditAccount",
        ];

        for object in objects {
            let field_map = get_field_map(object);
            let mut cols: Vec<String> = field_map
                .iter()
                .map(|(col_name, (col_type, _, _))| format!(r#""{}" {}"#, col_name, col_type))
                .collect();
            cols.sort();
            let sql = format!(
                r#"create foreign table if not exists {} ({}, attrs jsonb)
                server {} options (
                    object '{}'
                )"#,
                object,
                cols.join(","),
                stmt.server_name,
                object,
            );
            ret.push(sql);
        }

        Ok(ret)
    }
}

bindings::export!(ShopifyFdw with_types_in bindings);
