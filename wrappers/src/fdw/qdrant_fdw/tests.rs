#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;
    use reqwest::blocking::Client;
    use serde_json::json;

    #[pg_test]
    fn qdrant_smoketest() {
        const COLLECTION_NAME: &str = "test_collection";
        create_collection(COLLECTION_NAME);
        insert_points(COLLECTION_NAME);
        Spi::connect(|mut c| {
            c.update(
                r#"create foreign data wrapper qdrant_wrapper
                         handler qdrant_fdw_handler validator qdrant_fdw_validator"#,
                None,
                None,
            )
            .expect("Failed to create foreign data wrapper");

            c.update(
                r#"create server qdrant_server
                         foreign data wrapper qdrant_wrapper
                         options (
                            api_url 'http://localhost:6333',
                            api_key ''
                         )"#,
                None,
                None,
            )
            .expect("failed to create qdrant server");

            c.update(
                r#"
                  create foreign table qdrant_table (
                    id bigint,
                    payload jsonb,
                    vector real[]
                  )
                  server qdrant_server
                  options (
                    collection_name 'test_collection'
                  )
                 "#,
                None,
                None,
            )
            .expect("failed to create foreign table");

            let results = c
                .select("select * from qdrant_table", None, None)
                .expect("failed to run select query")
                .filter_map(|r| {
                    Some((
                        r.get_by_name::<i64, _>("id")
                            .expect("failed to get `id` field")
                            .expect("`id` field is missing"),
                        r.get_by_name::<Vec<f32>, _>("vector")
                            .expect("failed to get `vector` field")
                            .expect("`vector` field is missing"),
                    ))
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    (1, vec![0.123, 0.456, 0.789]),
                    (2, vec![0.456, 0.789, 0.123]),
                    (3, vec![0.789, 0.123, 0.456])
                ]
            );
        });
        delete_collection(COLLECTION_NAME);
    }

    fn create_collection(collection_name: &str) {
        let client = Client::new();
        let res = client
            .put(format!(
                "http://localhost:6333/collections/{collection_name}"
            ))
            .json(&json!({"vectors": { "size": 3, "distance": "Dot" }}))
            .send()
            .expect("failed to send create collection request");
        if !res.status().is_success() {
            panic!("failed to create collection: {res:?}");
        }
    }

    fn insert_points(collection_name: &str) {
        let client = Client::new();
        let res = client
            .put(format!(
                "http://localhost:6333/collections/{collection_name}/points"
            ))
            .json(&json!({
                "batch": {
                    "ids": [1, 2, 3],
                    "payloads": [
                        {"color": "red"},
                        {"color": "green"},
                        {"color": "blue"}
                    ],
                    "vectors": [
                        [0.123, 0.456, 0.789],
                        [0.456, 0.789, 0.123],
                        [0.789, 0.123, 0.456]
                    ]
                }
            }))
            .send()
            .expect("failed to send insert points request");
        if !res.status().is_success() {
            panic!("failed to insert points: {res:?}");
        }
    }

    fn delete_collection(collection_name: &str) {
        let client = Client::new();
        let res = client
            .delete(format!(
                "http://localhost:6333/collections/{collection_name}"
            ))
            .send()
            .expect("failed to send delete collection request");
        if !res.status().is_success() {
            panic!("failed to delete collection: {res:?}");
        }
    }
}
