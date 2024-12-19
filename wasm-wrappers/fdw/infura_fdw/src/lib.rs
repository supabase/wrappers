#[cfg(test)]
pub use self::InfuraFdw;

use serde_json::{json, Value as JsonValue};
use wit_bindgen_rt::*;

struct InfuraFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    project_id: String,
    table: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

impl InfuraFdw {
    // Convert hex string to decimal
    fn hex_to_decimal(hex: &str) -> Result<i64, String> {
        i64::from_str_radix(hex.trim_start_matches("0x"), 16)
            .map_err(|e| format!("Failed to convert hex to decimal: {}", e))
    }

    // Create JSON-RPC request
    fn create_request(&self, method: &str, params: Vec<String>) -> Result<http::Request, String> {
        let url = format!("{}/{}", self.base_url, self.project_id);
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });

        let mut headers = self.headers.clone();
        headers.push(("Content-Type".to_string(), "application/json".to_string()));

        Ok(http::Request {
            method: http::Method::Post,
            url,
            headers,
            body: body.to_string(),
        })
    }

    // Create new instance
    fn new(base_url: String, project_id: String) -> Self {
        Self {
            base_url,
            headers: Vec::new(),
            project_id,
            table: String::new(),
            src_rows: Vec::new(),
            src_idx: 0,
        }
    }

    // Process API response
    fn process_response(&mut self, response: &str) -> Result<(), String> {
        let value: JsonValue = serde_json::from_str(response)
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        if let Some(error) = value.get("error") {
            return Err(format!("API error: {}", error));
        }

        if let Some(result) = value.get("result") {
            self.src_rows = vec![result.clone()];
            self.src_idx = 0;
            Ok(())
        } else {
            Err("No result in response".to_string())
        }
    }
}

wit_bindgen::generate!({
    path: "../../wit",
    world: "wrappers",
});

struct Component;

impl wrappers::Wrappers for Component {
    fn create_fdw() -> Box<dyn wrappers::Fdw> {
        Box::new(InfuraFdw::new(
            String::from("https://mainnet.infura.io/v3"),
            String::new(), // project_id will be set from options
        ))
    }
}

impl wrappers::Fdw for InfuraFdw {
    fn begin_scan(
        &mut self,
        quals: Vec<wrappers::Qual>,
        attrs: Vec<wrappers::Attr>,
        table_name: String,
    ) -> Result<(), String> {
        self.table = table_name;

        let method = match self.table.as_str() {
            "eth_getBlockByNumber" => "eth_getBlockByNumber",
            "eth_blockNumber" => "eth_blockNumber",
            _ => return Err(format!("Unsupported table: {}", self.table)),
        };

        let request = self.create_request(method, vec![])?;
        let response = http::send_request(&request)
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if response.status_code != 200 {
            return Err(format!("HTTP error: {}", response.status_code));
        }

        self.process_response(&response.body)
    }

    fn iter_scan(&mut self) -> Result<Option<Vec<wrappers::Datum>>, String> {
        if self.src_idx >= self.src_rows.len() {
            return Ok(None);
        }

        let row = &self.src_rows[self.src_idx];
        self.src_idx += 1;

        let mut data = Vec::new();
        match self.table.as_str() {
            "eth_blockNumber" => {
                if let Some(block_num) = row.as_str() {
                    data.push(wrappers::Datum::I64(Self::hex_to_decimal(block_num)?));
                }
            }
            "eth_getBlockByNumber" => {
                if let Some(obj) = row.as_object() {
                    // Convert block data to appropriate types
                    if let Some(num) = obj.get("number").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::I64(Self::hex_to_decimal(num)?));
                    }
                    if let Some(hash) = obj.get("hash").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::String(hash.to_string()));
                    }
                    if let Some(parent) = obj.get("parentHash").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::String(parent.to_string()));
                    }
                    if let Some(nonce) = obj.get("nonce").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::String(nonce.to_string()));
                    }
                    if let Some(miner) = obj.get("miner").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::String(miner.to_string()));
                    }
                    if let Some(difficulty) = obj.get("difficulty").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::I64(Self::hex_to_decimal(difficulty)?));
                    }
                    if let Some(total) = obj.get("totalDifficulty").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::I64(Self::hex_to_decimal(total)?));
                    }
                    if let Some(size) = obj.get("size").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::I64(Self::hex_to_decimal(size)?));
                    }
                    if let Some(gas_limit) = obj.get("gasLimit").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::I64(Self::hex_to_decimal(gas_limit)?));
                    }
                    if let Some(gas_used) = obj.get("gasUsed").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::I64(Self::hex_to_decimal(gas_used)?));
                    }
                    if let Some(timestamp) = obj.get("timestamp").and_then(|v| v.as_str()) {
                        data.push(wrappers::Datum::I64(Self::hex_to_decimal(timestamp)?));
                    }
                }
            }
            _ => return Err(format!("Unsupported table: {}", self.table)),
        }

        Ok(Some(data))
    }

    fn end_scan(&mut self) -> Result<(), String> {
        self.src_rows.clear();
        self.src_idx = 0;
        Ok(())
    }

    fn validator(&self, options: Vec<(String, Option<String>)>) -> Result<(), String> {
        for (key, value) in options {
            match key.as_str() {
                "project_id" => {
                    if value.is_none() {
                        return Err("project_id is required".to_string());
                    }
                    self.project_id = value.unwrap();
                }
                _ => return Err(format!("Unknown option: {}", key)),
            }
        }
        Ok(())
    }
}
