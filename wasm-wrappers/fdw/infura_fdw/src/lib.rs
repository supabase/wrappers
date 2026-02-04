//! Infura FDW - Foreign Data Wrapper for blockchain data via Infura JSON-RPC API
//!
//! Supports querying Ethereum, Polygon, and other EVM-compatible chains.

#[allow(warnings)]
mod bindings;

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
use serde_json::Value as JsonValue;

#[derive(Debug, Default, Clone)]
enum Resource {
    #[default]
    Blocks,
    Transactions,
    Balances,
    Logs,
    ChainInfo,
}

impl Resource {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "blocks" => Resource::Blocks,
            "transactions" => Resource::Transactions,
            "balances" => Resource::Balances,
            "logs" => Resource::Logs,
            "chain_info" | "chaininfo" => Resource::ChainInfo,
            _ => Resource::Blocks,
        }
    }
}

#[derive(Debug, Default)]
struct InfuraFdw {
    base_url: String,
    api_key: String,
    network: String,
    headers: Vec<(String, String)>,
    resource: Resource,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
    // Query pushdown filters
    block_number: Option<String>,
    tx_hash: Option<String>,
    address: Option<String>,
    block_hash: Option<String>,
}

static mut INSTANCE: *mut InfuraFdw = std::ptr::null_mut::<InfuraFdw>();
static FDW_NAME: &str = "InfuraFdw";

impl InfuraFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    fn default_url(&self) -> String {
        format!("https://{}.infura.io/v3/{}", self.network, self.api_key)
    }

    fn make_json_rpc_request(
        &self,
        method: &str,
        params: JsonValue,
    ) -> Result<JsonValue, FdwError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });

        let req = http::Request {
            method: http::Method::Post,
            url: self.base_url.clone(),
            headers: self.headers.clone(),
            body: body.to_string(),
        };

        // Retry logic with exponential backoff for rate limiting
        const MAX_RETRIES: usize = 5;
        let mut retry_count = 0;

        loop {
            let resp = http::post(&req)?;

            // Handle rate limiting with retry
            if resp.status_code == 429 {
                retry_count += 1;
                if retry_count >= MAX_RETRIES {
                    return Err("Infura API rate limit exceeded after max retries".to_string());
                }
                // Exponential backoff: 2s, 4s, 8s, 16s, 32s
                let delay_ms = 2000 * (1 << (retry_count - 1));
                time::sleep(delay_ms);
                continue;
            }

            if resp.status_code != 200 {
                return Err(format!(
                    "Infura API error: status {}, body: {}",
                    resp.status_code, resp.body
                ));
            }

            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

            let json: JsonValue = serde_json::from_str(&resp.body)
                .map_err(|e| format!("Failed to parse response: {e}"))?;

            if let Some(error) = json.get("error") {
                return Err(format!("JSON-RPC error: {error}"));
            }

            return Ok(json);
        }
    }

    fn fetch_blocks(&mut self) -> FdwResult {
        let block_param = self
            .block_number
            .clone()
            .unwrap_or_else(|| "latest".to_string());
        let params = serde_json::json!([block_param, true]);

        let json = self.make_json_rpc_request("eth_getBlockByNumber", params)?;

        if let Some(result) = json.get("result") {
            if !result.is_null() {
                self.src_rows.push(result.clone());
            }
        }

        stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, self.src_rows.len() as i64);
        Ok(())
    }

    fn fetch_transactions(&mut self) -> FdwResult {
        let tx_hash = self
            .tx_hash
            .clone()
            .ok_or("Transaction hash required. Use WHERE hash = '0x...' to query transactions")?;

        let params = serde_json::json!([tx_hash]);
        let json = self.make_json_rpc_request("eth_getTransactionByHash", params)?;

        if let Some(result) = json.get("result") {
            if !result.is_null() {
                self.src_rows.push(result.clone());
            }
        }

        stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, self.src_rows.len() as i64);
        Ok(())
    }

    fn fetch_balances(&mut self) -> FdwResult {
        let address = self
            .address
            .clone()
            .ok_or("Address required. Use WHERE address = '0x...' to query balances")?;

        let block_param = self
            .block_number
            .clone()
            .unwrap_or_else(|| "latest".to_string());
        let params = serde_json::json!([address, block_param]);

        let json = self.make_json_rpc_request("eth_getBalance", params)?;

        if let Some(result) = json.get("result") {
            let balance_row = serde_json::json!({
                "address": address,
                "balance": result,
                "block": block_param
            });
            self.src_rows.push(balance_row);
        }

        stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, self.src_rows.len() as i64);
        Ok(())
    }

    fn fetch_logs(&mut self) -> FdwResult {
        let mut filter = serde_json::Map::new();

        if let Some(addr) = &self.address {
            filter.insert("address".to_string(), serde_json::json!(addr));
        }

        if let Some(block_hash) = &self.block_hash {
            filter.insert("blockHash".to_string(), serde_json::json!(block_hash));
        }

        let params = serde_json::json!([filter]);
        let json = self.make_json_rpc_request("eth_getLogs", params)?;

        if let Some(result) = json.get("result") {
            if let Some(logs) = result.as_array() {
                for log in logs {
                    self.src_rows.push(log.clone());
                }
            }
        }

        stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, self.src_rows.len() as i64);
        Ok(())
    }

    fn fetch_chain_info(&mut self) -> FdwResult {
        // Get chain ID
        let json1 = self.make_json_rpc_request("eth_chainId", serde_json::json!([]))?;
        let chain_id = json1.get("result").cloned().unwrap_or(JsonValue::Null);

        // Get block number
        let json2 = self.make_json_rpc_request("eth_blockNumber", serde_json::json!([]))?;
        let block_number = json2.get("result").cloned().unwrap_or(JsonValue::Null);

        // Get gas price
        let json3 = self.make_json_rpc_request("eth_gasPrice", serde_json::json!([]))?;
        let gas_price = json3.get("result").cloned().unwrap_or(JsonValue::Null);

        let chain_info = serde_json::json!({
            "network": self.network,
            "chain_id": chain_id,
            "block_number": block_number,
            "gas_price": gas_price
        });

        self.src_rows.push(chain_info);
        stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, 1);
        Ok(())
    }

    /// Parse hex string to f64 (for PostgreSQL numeric type)
    fn hex_to_numeric(hex: &str) -> Option<f64> {
        let hex = hex.strip_prefix("0x").unwrap_or(hex);
        if hex.is_empty() {
            return Some(0.0);
        }
        u64::from_str_radix(hex, 16).map(|n| n as f64).ok()
    }

    /// Convert hex balance string to ETH string representation
    fn hex_balance_to_eth_string(hex: &str) -> Option<String> {
        let hex = hex.strip_prefix("0x").unwrap_or(hex);
        if hex.is_empty() {
            return Some("0".to_string());
        }

        // Parse hex string to u128 (Wei amount)
        let wei = match u128::from_str_radix(hex, 16) {
            Ok(val) => val,
            Err(_) => return None,
        };

        // 1 ETH = 10^18 Wei
        const WEI_PER_ETH: u128 = 1_000_000_000_000_000_000;

        // Calculate integer and fractional parts
        let eth_whole = wei / WEI_PER_ETH;
        let wei_remainder = wei % WEI_PER_ETH;

        // Format fractional part with 18 decimal places, removing trailing zeros
        let fractional = format!("{wei_remainder:018}");
        let fractional_trimmed = fractional.trim_end_matches('0');

        // Build result string
        let result = if fractional_trimmed.is_empty() {
            format!("{eth_whole}")
        } else {
            format!("{eth_whole}.{fractional_trimmed}")
        };

        Some(result)
    }

    /// Parse hex timestamp to timestamp (unix timestamp in microseconds)
    fn hex_to_timestamp(hex: &str) -> Option<i64> {
        let hex = hex.strip_prefix("0x").unwrap_or(hex);
        i64::from_str_radix(hex, 16).map(|n| n * 1_000_000).ok()
    }

    fn src_to_cell(&self, src_row: &JsonValue, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // Handle attrs column - return full JSON
        if tgt_col_name == "attrs" || tgt_col_name == "_attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        match self.resource {
            Resource::Blocks => self.block_to_cell(src_row, &tgt_col_name, tgt_col.type_oid()),
            Resource::Transactions => {
                self.transaction_to_cell(src_row, &tgt_col_name, tgt_col.type_oid())
            }
            Resource::Balances => self.balance_to_cell(src_row, &tgt_col_name, tgt_col.type_oid()),
            Resource::Logs => self.log_to_cell(src_row, &tgt_col_name, tgt_col.type_oid()),
            Resource::ChainInfo => {
                self.chain_info_to_cell(src_row, &tgt_col_name, tgt_col.type_oid())
            }
        }
    }

    fn block_to_cell(
        &self,
        src_row: &JsonValue,
        col_name: &str,
        _type_oid: TypeOid,
    ) -> Result<Option<Cell>, FdwError> {
        let cell = match col_name {
            "number" => src_row
                .get("number")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "hash" => src_row
                .get("hash")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "parent_hash" => src_row
                .get("parentHash")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "timestamp" => src_row
                .get("timestamp")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_timestamp)
                .map(Cell::Timestamp),
            "miner" => src_row
                .get("miner")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "gas_used" => src_row
                .get("gasUsed")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "gas_limit" => src_row
                .get("gasLimit")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "transaction_count" => src_row
                .get("transactions")
                .and_then(|v| v.as_array())
                .map(|arr| Cell::I64(arr.len() as i64)),
            "base_fee_per_gas" => src_row
                .get("baseFeePerGas")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            _ => None,
        };
        Ok(cell)
    }

    fn transaction_to_cell(
        &self,
        src_row: &JsonValue,
        col_name: &str,
        _type_oid: TypeOid,
    ) -> Result<Option<Cell>, FdwError> {
        let cell = match col_name {
            "hash" => src_row
                .get("hash")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "block_number" => src_row
                .get("blockNumber")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "block_hash" => src_row
                .get("blockHash")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "from" | "from_address" => src_row
                .get("from")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "to" | "to_address" => src_row
                .get("to")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "value" => src_row
                .get("value")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "gas" => src_row
                .get("gas")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "gas_price" => src_row
                .get("gasPrice")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "nonce" => src_row
                .get("nonce")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "input" | "data" => src_row
                .get("input")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "transaction_index" => src_row
                .get("transactionIndex")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            _ => None,
        };
        Ok(cell)
    }

    fn balance_to_cell(
        &self,
        src_row: &JsonValue,
        col_name: &str,
        _type_oid: TypeOid,
    ) -> Result<Option<Cell>, FdwError> {
        let cell = match col_name {
            "address" => src_row
                .get("address")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "balance" => src_row
                .get("balance")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_balance_to_eth_string)
                .map(Cell::String),
            "block" => src_row
                .get("block")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            _ => None,
        };
        Ok(cell)
    }

    fn log_to_cell(
        &self,
        src_row: &JsonValue,
        col_name: &str,
        _type_oid: TypeOid,
    ) -> Result<Option<Cell>, FdwError> {
        let cell = match col_name {
            "address" => src_row
                .get("address")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "block_number" => src_row
                .get("blockNumber")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "block_hash" => src_row
                .get("blockHash")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "transaction_hash" => src_row
                .get("transactionHash")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "transaction_index" => src_row
                .get("transactionIndex")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "log_index" => src_row
                .get("logIndex")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "data" => src_row
                .get("data")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "topics" => src_row.get("topics").map(|v| Cell::Json(v.to_string())),
            "removed" => src_row
                .get("removed")
                .and_then(|v| v.as_bool())
                .map(Cell::Bool),
            _ => None,
        };
        Ok(cell)
    }

    fn chain_info_to_cell(
        &self,
        src_row: &JsonValue,
        col_name: &str,
        _type_oid: TypeOid,
    ) -> Result<Option<Cell>, FdwError> {
        let cell = match col_name {
            "network" => src_row
                .get("network")
                .and_then(|v| v.as_str())
                .map(|s| Cell::String(s.to_string())),
            "chain_id" => src_row
                .get("chain_id")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "block_number" => src_row
                .get("block_number")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            "gas_price" => src_row
                .get("gas_price")
                .and_then(|v| v.as_str())
                .and_then(Self::hex_to_numeric)
                .map(Cell::Numeric),
            _ => None,
        };
        Ok(cell)
    }
}

impl Guest for InfuraFdw {
    fn host_version_requirement() -> String {
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        let opts = ctx.get_options(&OptionsType::Server);

        // Get network (default to mainnet)
        this.network = opts.require_or("network", "mainnet");

        // Get API key from vault or options
        this.api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id).unwrap_or_default()
            }
        };

        if this.api_key.is_empty() {
            return Err("Infura API key is empty".to_string());
        }

        // Set up headers
        this.headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("user-agent".to_string(), "Wrappers Infura FDW".to_string()),
        ];

        // Get base URL - use api_url option if provided (for testing), otherwise build default
        this.base_url = match opts.get("api_url") {
            Some(url) => url,
            None => this.default_url(),
        };

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        this.src_idx = 0;

        // Reset filters
        this.block_number = None;
        this.tx_hash = None;
        this.address = None;
        this.block_hash = None;

        // Get resource type from table options
        let opts = ctx.get_options(&OptionsType::Table);
        let resource_str = opts.require_or("resource", "blocks");
        this.resource = Resource::from_str(&resource_str);

        // Process query pushdown quals
        for qual in ctx.get_quals() {
            let field = qual.field().to_lowercase();
            let value = match qual.value() {
                Value::Cell(Cell::String(s)) => s.clone(),
                Value::Cell(Cell::I64(n)) => format!("0x{n:x}"),
                Value::Cell(Cell::Numeric(n)) => format!("0x{:x}", n as u64),
                _ => continue,
            };

            match field.as_str() {
                "number" | "block_number" => {
                    this.block_number = Some(value);
                }
                "hash" | "tx_hash" | "transaction_hash" => {
                    this.tx_hash = Some(value);
                }
                "address" => {
                    this.address = Some(value);
                }
                "block_hash" => {
                    this.block_hash = Some(value);
                }
                _ => {}
            }
        }

        // Fetch data based on resource type
        match this.resource.clone() {
            Resource::Blocks => this.fetch_blocks()?,
            Resource::Transactions => this.fetch_transactions()?,
            Resource::Balances => this.fetch_balances()?,
            Resource::Logs => this.fetch_logs()?,
            Resource::ChainInfo => this.fetch_chain_info()?,
        }

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        if this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.src_rows.len() as i64);
            return Ok(None);
        }

        let src_row = &this.src_rows[this.src_idx];

        for tgt_col in ctx.get_columns() {
            let cell = this.src_to_cell(src_row, &tgt_col)?;
            row.push(cell.as_ref());
        }

        this.src_idx += 1;
        Ok(Some(0))
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_idx = 0;
        Ok(())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("Infura FDW is read-only. Blockchain data cannot be modified.".to_string())
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
        let ret = vec![
            format!(
                r#"create foreign table if not exists eth_blocks (
                    number numeric,
                    hash text,
                    parent_hash text,
                    timestamp timestamp,
                    miner text,
                    gas_used numeric,
                    gas_limit numeric,
                    transaction_count bigint,
                    base_fee_per_gas numeric,
                    attrs jsonb
                )
                server {} options (
                    resource 'blocks'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists eth_transactions (
                    hash text,
                    block_number numeric,
                    block_hash text,
                    from_address text,
                    to_address text,
                    value numeric,
                    gas numeric,
                    gas_price numeric,
                    nonce numeric,
                    input text,
                    transaction_index numeric,
                    attrs jsonb
                )
                server {} options (
                    resource 'transactions'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists eth_balances (
                    address text,
                    balance text,
                    block text
                )
                server {} options (
                    resource 'balances'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists eth_logs (
                    address text,
                    block_number numeric,
                    block_hash text,
                    transaction_hash text,
                    transaction_index numeric,
                    log_index numeric,
                    data text,
                    topics jsonb,
                    removed boolean,
                    attrs jsonb
                )
                server {} options (
                    resource 'logs'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists eth_chain_info (
                    network text,
                    chain_id numeric,
                    block_number numeric,
                    gas_price numeric
                )
                server {} options (
                    resource 'chain_info'
                )"#,
                stmt.server_name,
            ),
        ];
        Ok(ret)
    }
}

bindings::export!(InfuraFdw with_types_in bindings);
