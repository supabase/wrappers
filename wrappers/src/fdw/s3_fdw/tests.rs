#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn s3_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER s3_wrapper
                     HANDLER s3_fdw_handler VALIDATOR s3_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
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
            )
            .unwrap();

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
            )
            .unwrap();

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
            )
            .unwrap();

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
            )
            .unwrap();

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
            )
            .unwrap();

            let check_test_table = |table| {
                let sql = format!("SELECT * FROM {} ORDER BY name LIMIT 1", table);
                let results = c
                    .select(&sql, None, None)
                    .unwrap()
                    .filter_map(|r| {
                        r.get_by_name::<&str, _>("name")
                            .unwrap()
                            .zip(r.get_by_name::<&str, _>("age").unwrap())
                            .zip(r.get_by_name::<&str, _>("height").unwrap())
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
