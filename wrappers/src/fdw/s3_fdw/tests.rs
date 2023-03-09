#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use pgx::prelude::*;

    #[pg_test]
    fn s3_smoketest() {
        Spi::execute(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER s3_wrapper
                     HANDLER s3_fdw_handler VALIDATOR s3_fdw_validator"#,
                None,
                None,
            );
            c.update(
                r#"CREATE SERVER s3_server
                     FOREIGN DATA WRAPPER s3_wrapper
                     OPTIONS (
                       aws_access_key_id 'test',
                       aws_secret_access_key 'test',
                       aws_region 'us-east-1',
                       is_mock 'true'
                     )"#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_csv (
                  name text,
                  sex text,
                  age text,
                  height text,
                  weight text
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://test/test_data.csv',
                    format 'csv',
                    has_header 'true'
                  )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_csv_gz (
                  name text,
                  sex text,
                  age text,
                  height text,
                  weight text
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://test/test_data.csv.gz',
                    format 'csv',
                    has_header 'true',
                    compress 'gzip'
                  )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_jsonl (
                  name text,
                  sex text,
                  age text,
                  height text,
                  weight text
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://test/test_data.jsonl',
                    format 'jsonl'
                  )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_jsonl_bz (
                  name text,
                  sex text,
                  age text,
                  height text,
                  weight text
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://test/test_data.jsonl.bz2',
                    format 'jsonl',
                    compress 'bzip2'
                  )
             "#,
                None,
                None,
            );

            let check_test_table = |table| {
                let sql = format!("SELECT * FROM {} ORDER BY name LIMIT 1", table);
                let results = c
                    .select(&sql, None, None)
                    .filter_map(|r| {
                        r.by_name("name")
                            .ok()
                            .and_then(|v| v.value::<&str>())
                            .zip(r.by_name("age").ok().and_then(|v| v.value::<&str>()))
                            .zip(r.by_name("height").ok().and_then(|v| v.value::<&str>()))
                    })
                    .collect::<Vec<_>>();
                assert_eq!(results, vec![(("Alex", "41"), "74")]);
            };

            check_test_table("s3_test_table_csv");
            check_test_table("s3_test_table_csv_gz");
            check_test_table("s3_test_table_jsonl");
            check_test_table("s3_test_table_jsonl_bz");
        });
    }
}
