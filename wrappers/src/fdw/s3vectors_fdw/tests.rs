#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn s3vectors_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER s3_vectors_wrapper
                     HANDLER s3_vectors_fdw_handler VALIDATOR s3_vectors_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER s3_vectors_server
                     FOREIGN DATA WRAPPER s3_vectors_wrapper
                     OPTIONS (
                       aws_access_key_id 'test',
                       aws_secret_access_key 'test',
                       endpoint_url 'http://localhost:4444'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS s3_vectors"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA "s3_vectors" FROM SERVER s3_vectors_server INTO s3_vectors
                   OPTIONS (
                     bucket_name 'my-vector-bucket'
                   )
                "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM s3_vectors.my_vector_index ORDER BY key",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("key").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["test-key1", "test-key2", "test-key3"]);

            let results = c
                .select(
                    r#"SELECT s3vec_distance(data) as distance, *
                    FROM s3_vectors.my_vector_index
                    WHERE data <==> '[1.2, -0.8, 2.9]'
                      AND metadata <==> '{"model": {"$eq": "test"}}'
                    ORDER BY 1"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("key").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["test-key2", "test-key1"]);

            c.update(
                r#"INSERT INTO s3_vectors.my_vector_index (key, data, metadata)
                    VALUES (
                        'test1',
                        '[1.0, 2.0, 1.0]',
                        '{"model": "test1", "dimensions": 3}'::jsonb
                    )"#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"DELETE FROM s3_vectors.my_vector_index WHERE key = 'test-key1'"#,
                None,
                &[],
            )
            .unwrap();
        });
    }
}
