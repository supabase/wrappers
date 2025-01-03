use serde_json::json;

// Import the crate being tested
use infura_fdw::*;

#[test]
fn test_hex_to_decimal() {
    let fdw = InfuraFdw::new(
        String::from("https://mainnet.infura.io/v3"),
        String::from("test-project-id"),
    );

    assert_eq!(fdw.hex_to_decimal("0x1234").unwrap(), 4660);
    assert_eq!(fdw.hex_to_decimal("0xabcd").unwrap(), 43981);
    assert_eq!(fdw.hex_to_decimal("0x0").unwrap(), 0);
    assert!(fdw.hex_to_decimal("invalid").is_err());
}

#[test]
fn test_create_request() {
    let fdw = InfuraFdw::new(
        String::from("https://mainnet.infura.io/v3"),
        String::from("test-project-id"),
    );

    // Test eth_blockNumber request
    let req = fdw.create_request("eth_blockNumber", vec![]).unwrap();
    assert_eq!(req.method, http::Method::Post);
    assert_eq!(req.url, "https://mainnet.infura.io/v3/test-project-id");

    let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["method"], "eth_blockNumber");
    assert!(body["params"].as_array().unwrap().is_empty());

    // Test eth_getBlockByNumber request
    let req = fdw.create_request("eth_getBlockByNumber", vec!["0x1234".to_string(), "true".to_string()]).unwrap();
    assert_eq!(req.method, http::Method::Post);

    let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
    assert_eq!(body["method"], "eth_getBlockByNumber");
    assert_eq!(body["params"][0], "0x1234");
    assert_eq!(body["params"][1], "true");
}

#[test]
fn test_process_response() {
    let mut fdw = InfuraFdw::new(
        String::from("https://mainnet.infura.io/v3"),
        String::from("test-project-id"),
    );

    // Test block number response
    let response = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": "0x1234567"
    }).to_string();

    assert!(fdw.process_response(&response).is_ok());
    assert_eq!(fdw.src_rows.len(), 1);
    assert_eq!(fdw.src_rows[0].as_str().unwrap(), "0x1234567");

    // Test error response
    let error_response = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": -32601,
            "message": "Method not found"
        }
    }).to_string();

    assert!(fdw.process_response(&error_response).is_err());
}
