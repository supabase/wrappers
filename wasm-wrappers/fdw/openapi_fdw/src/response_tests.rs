use super::*;
use crate::config::ServerConfig;
use crate::pagination::PaginationToken;

// --- json_to_rows tests ---

#[test]
fn test_json_to_rows_array() {
    let data = serde_json::json!([
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "charlie"}
    ]);
    let rows = OpenApiFdw::json_to_rows(data).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[2]["name"], "charlie");
}

#[test]
fn test_json_to_rows_single_object() {
    let data = serde_json::json!({"id": 1, "name": "alice"});
    let rows = OpenApiFdw::json_to_rows(data).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], "alice");
}

#[test]
fn test_json_to_rows_empty_array() {
    let data = serde_json::json!([]);
    let rows = OpenApiFdw::json_to_rows(data).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn test_json_to_rows_rejects_primitive() {
    let data = serde_json::json!("just a string");
    let err = OpenApiFdw::json_to_rows(data).unwrap_err();
    assert!(
        err.contains("string"),
        "Error should mention the type: {err}"
    );
}

// --- extract_data tests ---

fn fdw_with_response_path(path: Option<&str>) -> OpenApiFdw {
    OpenApiFdw {
        response_path: path.map(String::from),
        ..Default::default()
    }
}

#[test]
fn test_extract_data_with_response_path() {
    let fdw = fdw_with_response_path(Some("/features"));
    let mut resp = serde_json::json!({
        "type": "FeatureCollection",
        "features": [
            {"properties": {"id": "a"}},
            {"properties": {"id": "b"}}
        ]
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    // Original is taken, not cloned
    assert!(resp["features"].is_null());
}

#[test]
fn test_extract_data_with_nested_response_path() {
    let fdw = fdw_with_response_path(Some("/result/data"));
    let mut resp = serde_json::json!({
        "result": {
            "data": [{"id": 1}, {"id": 2}, {"id": 3}]
        }
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_extract_data_response_path_fallback_single_object() {
    // response_path /features fails (single object, not FeatureCollection),
    // falls back to single object auto-detection
    let fdw = fdw_with_response_path(Some("/features"));
    let mut resp = serde_json::json!({
        "@type": "wx:ObservationStation",
        "stationIdentifier": "KDEN",
        "name": "Denver International Airport"
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["stationIdentifier"], "KDEN");
}

#[test]
fn test_extract_data_response_path_fallback_with_wrapper_key() {
    // response_path /features fails, falls back to auto-detect "data" wrapper key
    let fdw = fdw_with_response_path(Some("/features"));
    let mut resp = serde_json::json!({
        "data": [{"id": 1}, {"id": 2}],
        "total": 2
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 1);
}

#[test]
fn test_extract_data_direct_array() {
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!([{"id": 1}, {"id": 2}]);
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_extract_data_auto_detect_data_key() {
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "data": [{"id": 1}, {"id": 2}],
        "meta": {"total": 2}
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(resp["data"].is_null());
}

#[test]
fn test_extract_data_auto_detect_results_key() {
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "results": [{"id": "x"}],
        "count": 1
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "x");
}

#[test]
fn test_extract_data_auto_detect_features_key() {
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"name": "A"}},
            {"type": "Feature", "properties": {"name": "B"}}
        ]
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_extract_data_single_object_fallback() {
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "id": "abc",
        "name": "singleton"
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "abc");
}

#[test]
fn test_extract_data_ownership_no_clone() {
    // Verify that extract_data takes ownership rather than cloning:
    // after extraction, the original data should be replaced with null
    let fdw = fdw_with_response_path(Some("/items"));
    let mut resp = serde_json::json!({
        "items": [
            {"id": 1, "payload": "x".repeat(1000)},
            {"id": 2, "payload": "y".repeat(1000)}
        ]
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["payload"].as_str().unwrap().len(), 1000);
    // The original value was taken, not cloned
    assert!(resp.pointer("/items").unwrap().is_null());
}

#[test]
fn test_extract_data_auto_detect_records_key() {
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "records": [{"id": 1}, {"id": 2}],
        "total": 2
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_extract_data_auto_detect_entries_key() {
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "entries": [{"id": "a"}, {"id": "b"}]
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_extract_data_auto_detect_items_key() {
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "items": [{"id": 1}],
        "next_page": null
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_extract_data_priority_order() {
    // When response has both "data" and "results", "data" wins (checked first)
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "data": [{"id": 1}],
        "results": [{"id": 2}, {"id": 3}]
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], 1);
}

#[test]
fn test_extract_data_non_array_wrapper_becomes_single_row() {
    // If a wrapper key contains an object (not array), treat as single row
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "data": {"id": "single", "name": "test"}
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "single");
}

#[test]
fn test_extract_data_auto_detect_at_graph_key() {
    // JSON-LD @graph wrapper (NWS API with Accept: application/ld+json)
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "@context": {"@version": "1.1", "wx": "https://api.weather.gov/ontology#"},
        "@graph": [
            {"@id": "urn:alert:1", "@type": "wx:Alert", "headline": "Storm warning"},
            {"@id": "urn:alert:2", "@type": "wx:Alert", "headline": "Heat advisory"}
        ]
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["headline"], "Storm warning");
}

#[test]
fn test_extract_data_response_path_to_single_object() {
    // response_path pointing to a single object (not array) → wrapped as single row
    let fdw = fdw_with_response_path(Some("/user"));
    let mut resp = serde_json::json!({
        "user": {"id": 1, "name": "alice"},
        "meta": {"request_id": "abc"}
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["name"], "alice");
}

#[test]
fn test_extract_data_deeply_nested_response_path() {
    // Three-level deep response path
    let fdw = fdw_with_response_path(Some("/response/body/items"));
    let mut resp = serde_json::json!({
        "response": {
            "body": {
                "items": [{"id": 1}, {"id": 2}]
            }
        }
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_extract_data_empty_object_is_single_row() {
    // Empty object {} treated as single row
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({});
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 1);
}

// --- Pagination tests ---

fn make_fdw_for_pagination(cursor_path: &str) -> OpenApiFdw {
    OpenApiFdw {
        cursor_path: cursor_path.to_string(),
        config: ServerConfig {
            cursor_param: "after".to_string(),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn test_handle_pagination_cursor_path_token() {
    let mut fdw = make_fdw_for_pagination("/cursor");
    let resp = serde_json::json!({"cursor": "abc123", "data": []});
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Cursor("abc123".to_string()))
    );
}

#[test]
fn test_handle_pagination_cursor_path_full_url() {
    let mut fdw = make_fdw_for_pagination("/pagination/next");
    let resp = serde_json::json!({
        "pagination": {"next": "https://api.example.com/items?cursor=xyz"},
        "data": []
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "https://api.example.com/items?cursor=xyz".to_string()
        ))
    );
}

#[test]
fn test_handle_pagination_cursor_path_http_url() {
    let mut fdw = make_fdw_for_pagination("/next");
    let resp = serde_json::json!({"next": "http://api.example.com/page2"});
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "http://api.example.com/page2".to_string()
        ))
    );
}

#[test]
fn test_handle_pagination_cursor_path_missing() {
    let mut fdw = make_fdw_for_pagination("/cursor");
    let resp = serde_json::json!({"data": []});
    fdw.handle_pagination(&resp);
    assert!(fdw.pagination.next.is_none());
}

#[test]
fn test_handle_pagination_auto_detect_next_url() {
    let mut fdw = make_fdw_for_pagination(""); // no cursor_path configured
    let resp = serde_json::json!({
        "pagination": {"next": "https://api.example.com/items?page=2"},
        "data": []
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "https://api.example.com/items?page=2".to_string()
        ))
    );
}

#[test]
fn test_handle_pagination_auto_detect_links_next() {
    // HAL-style: /links/next
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "links": {"next": "https://api.example.com/page2"},
        "data": []
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "https://api.example.com/page2".to_string()
        ))
    );
}

#[test]
fn test_handle_pagination_auto_detect_has_more_with_cursor() {
    // Stripe-style: has_more + next_cursor
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "has_more": true,
        "next_cursor": "cursor_xyz",
        "data": [{"id": 1}]
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Cursor("cursor_xyz".to_string()))
    );
}

#[test]
fn test_handle_pagination_has_more_false_stops() {
    // has_more: false should NOT set any pagination
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "has_more": false,
        "next_cursor": "stale_cursor",
        "data": [{"id": 1}]
    });
    fdw.handle_pagination(&resp);
    assert!(fdw.pagination.next.is_none());
}

#[test]
fn test_handle_pagination_auto_detect_meta_pagination() {
    // Nested meta.pagination.next pattern
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "meta": {
            "pagination": {
                "next": "https://api.example.com/items?page=3"
            }
        },
        "data": []
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "https://api.example.com/items?page=3".to_string()
        ))
    );
}

#[test]
fn test_handle_pagination_empty_string_next_url_stops() {
    // Empty string cursor_path value should be treated as "no more pages"
    let mut fdw = make_fdw_for_pagination("/next");
    let resp = serde_json::json!({"next": "", "data": []});
    fdw.handle_pagination(&resp);
    assert!(fdw.pagination.next.is_none());
}

#[test]
fn test_handle_pagination_null_cursor_stops() {
    // Null cursor should mean end of pagination
    let mut fdw = make_fdw_for_pagination("/cursor");
    let resp = serde_json::json!({"cursor": null, "data": []});
    fdw.handle_pagination(&resp);
    assert!(fdw.pagination.next.is_none());
}

#[test]
fn test_handle_pagination_array_response_no_autodetect() {
    // Auto-detection should not run on array responses
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!([{"id": 1}, {"id": 2}]);
    fdw.handle_pagination(&resp);
    assert!(fdw.pagination.next.is_none());
}

#[test]
fn test_meta_pagination_has_more_nested() {
    // Paginated APIs: /meta/pagination/has_more + /meta/pagination/next_cursor
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "meta": {
            "pagination": {
                "has_more": true,
                "next_cursor": "cursor_abc123"
            }
        },
        "data": [{"id": 1}, {"id": 2}]
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Cursor("cursor_abc123".to_string()))
    );
}

#[test]
fn test_handle_pagination_has_more_true_but_no_cursor() {
    // has_more: true but no cursor path found — should NOT paginate (avoid infinite loop)
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "has_more": true,
        "data": [{"id": 1}]
    });
    fdw.handle_pagination(&resp);
    assert!(fdw.pagination.next.is_none());
}

#[test]
fn test_handle_pagination_next_url_direct_key() {
    // Auto-detect: /next_url key directly
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "next_url": "https://api.example.com/items?page=3",
        "data": [{"id": 1}]
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "https://api.example.com/items?page=3".to_string()
        ))
    );
}

#[test]
fn test_handle_pagination_pagination_next_url() {
    // Auto-detect: /pagination/next_url key
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "pagination": {
            "next_url": "https://api.example.com/page/2"
        },
        "data": []
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "https://api.example.com/page/2".to_string()
        ))
    );
}

#[test]
fn test_handle_pagination_next_direct() {
    // Auto-detect: /next key directly (not nested)
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "next": "https://api.example.com/items?cursor=xyz",
        "data": [{"id": 1}]
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "https://api.example.com/items?cursor=xyz".to_string()
        ))
    );
}

#[test]
fn test_handle_pagination_cursor_path_integer_value() {
    // Cursor path resolves to an integer — should be treated as non-string, ignored
    let mut fdw = make_fdw_for_pagination("/cursor");
    let resp = serde_json::json!({"cursor": 12345, "data": []});
    fdw.handle_pagination(&resp);
    // extract_non_empty_string returns None for non-string values
    assert!(fdw.pagination.next.is_none());
}

#[test]
fn test_handle_pagination_pagination_has_more_with_cursor() {
    // Auto-detect: /pagination/has_more + /pagination/next_cursor
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "pagination": {
            "has_more": true,
            "next_cursor": "pg_cursor_99"
        },
        "data": [{"id": 1}]
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Cursor("pg_cursor_99".to_string()))
    );
}

#[test]
fn test_handle_pagination_meta_next_url() {
    // Auto-detect: /meta/pagination/next_url (not /next)
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "meta": {
            "pagination": {
                "next_url": "https://api.example.com/page/4"
            }
        },
        "data": []
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "https://api.example.com/page/4".to_string()
        ))
    );
}

// --- Real-world API pattern tests ---

#[test]
fn test_stripe_list_response() {
    // Stripe pattern: {object:"list", data:[...], has_more:true}
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "object": "list",
        "data": [
            {"id": "ch_1", "amount": 2000, "currency": "usd"},
            {"id": "ch_2", "amount": 5000, "currency": "eur"}
        ],
        "has_more": true,
        "url": "/v1/charges"
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], "ch_1");
    assert_eq!(rows[1]["amount"], 5000);
    // data was taken (ownership), not cloned
    assert!(resp["data"].is_null());
}

#[test]
fn test_github_direct_array() {
    // GitHub pattern: direct array response + no auto-pagination
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!([
        {"id": 1, "login": "octocat", "type": "User"},
        {"id": 2, "login": "hubot", "type": "Bot"}
    ]);
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["login"], "octocat");

    // Array responses should not trigger auto-pagination
    let mut pagination_fdw = make_fdw_for_pagination("");
    let array_resp = serde_json::json!([{"id": 1}, {"id": 2}]);
    pagination_fdw.handle_pagination(&array_resp);
    assert!(pagination_fdw.pagination.next.is_none());
}

#[test]
fn test_hal_links_next_href_pagination() {
    // HAL pattern: _links/next/href pagination path
    let mut fdw = make_fdw_for_pagination("");
    let resp = serde_json::json!({
        "_embedded": {"items": [{"id": 1}]},
        "_links": {
            "self": {"href": "https://api.example.com/items?page=1"},
            "next": {"href": "https://api.example.com/items?page=2"}
        }
    });
    fdw.handle_pagination(&resp);
    assert_eq!(
        fdw.pagination.next,
        Some(PaginationToken::Url(
            "https://api.example.com/items?page=2".to_string()
        ))
    );
}

#[test]
fn test_kubernetes_list_response() {
    // Kubernetes pattern: {kind, apiVersion, metadata, items:[...]}
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "kind": "PodList",
        "apiVersion": "v1",
        "metadata": {"resourceVersion": "1234"},
        "items": [
            {"metadata": {"name": "pod-1"}, "status": {"phase": "Running"}},
            {"metadata": {"name": "pod-2"}, "status": {"phase": "Pending"}}
        ]
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["metadata"]["name"], "pod-1");
}

#[test]
fn test_elasticsearch_hits_response() {
    // Elasticsearch pattern: response_path must be used for non-standard wrapper
    let fdw = fdw_with_response_path(Some("/hits/hits"));
    let mut resp = serde_json::json!({
        "took": 5,
        "hits": {
            "total": {"value": 2},
            "hits": [
                {"_id": "1", "_source": {"title": "Doc 1"}},
                {"_id": "2", "_source": {"title": "Doc 2"}}
            ]
        }
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["_id"], "1");
}

#[test]
fn test_graphql_style_response() {
    // GraphQL-style: {data: {users: [...]}} — needs response_path
    let fdw = fdw_with_response_path(Some("/data/users"));
    let mut resp = serde_json::json!({
        "data": {
            "users": [
                {"id": "1", "name": "Alice"},
                {"id": "2", "name": "Bob"}
            ]
        }
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["name"], "Alice");
}

#[test]
fn test_jsonapi_style_response() {
    // JSON:API pattern: {data: [{type, id, attributes}], meta}
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!({
        "data": [
            {"type": "articles", "id": "1", "attributes": {"title": "JSON:API"}},
            {"type": "articles", "id": "2", "attributes": {"title": "REST"}}
        ],
        "meta": {"total-pages": 1}
    });
    let rows = fdw.extract_data(&mut resp).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], "1");
}

// --- Error message context tests ---

#[test]
fn test_extract_data_error_shows_type_for_non_extractable() {
    let fdw = fdw_with_response_path(None);
    let mut resp = serde_json::json!(42);
    let err = fdw.extract_data(&mut resp).unwrap_err();
    assert!(
        err.contains("number"),
        "Error should mention JSON type: {err}"
    );
    assert!(
        err.contains("response_path"),
        "Error should suggest response_path: {err}"
    );
}

#[test]
fn test_json_to_rows_error_shows_type() {
    let err = OpenApiFdw::json_to_rows(serde_json::json!(null)).unwrap_err();
    assert!(err.contains("null"), "Error should show type: {err}");

    let err = OpenApiFdw::json_to_rows(serde_json::json!(true)).unwrap_err();
    assert!(err.contains("boolean"), "Error should show type: {err}");

    let err = OpenApiFdw::json_to_rows(serde_json::json!(42)).unwrap_err();
    assert!(err.contains("number"), "Error should show type: {err}");
}
