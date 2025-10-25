#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    use serde_json::json;
    use std::str::FromStr;

    #[pg_test]
    fn duckdb_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER duckdb_wrapper
                     HANDLER duckdb_fdw_handler VALIDATOR duckdb_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER duckdb_server_s3
                     FOREIGN DATA WRAPPER duckdb_wrapper
                     OPTIONS (
                       type 's3',
                       key_id 'admin',
                       secret 'password',
                       region 'us-east-1',
                       endpoint 'localhost:8000',
                       url_style 'path',
                       use_ssl 'false'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER duckdb_server_iceberg
                     FOREIGN DATA WRAPPER duckdb_wrapper
                     OPTIONS (
                       type 'iceberg',
                       key_id 'admin',
                       secret 'password',
                       region 'us-east-1',
                       endpoint 'localhost:8000',
                       url_style 'path',
                       use_ssl 'false',
                       token 'test',
                       warehouse 'warehouse',
                       catalog_uri 'localhost:8181'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER duckdb_server_motherduck
                    FOREIGN DATA WRAPPER duckdb_wrapper
                    OPTIONS (
                        type 'md',
                        database 'my_db',
                        motherduck_token 'my_token'
                    )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS duckdb"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA s3 FROM SERVER duckdb_server_s3 INTO duckdb
                    OPTIONS (
                        tables '
                            s3://warehouse/test_data.csv,
                            s3://warehouse/test_data.parquet
                        ',
                        strict 'true'
                    )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA "docs_example" FROM SERVER duckdb_server_iceberg INTO duckdb"#,
                None,
                &[],
            )
            .unwrap();
            let results = c
                .select(
                    "SELECT * FROM duckdb.s3_0_test_data order by name",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Alex", "Bert", "Carl"]);

            let results = c
                .select(
                    "SELECT * FROM duckdb.s3_1_test_data order by id limit 3",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<pgrx::datum::Timestamp, _>("timestamp_col")
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    pgrx::datum::Timestamp::from_str("2009-01-01 00:00:00").unwrap(),
                    pgrx::datum::Timestamp::from_str("2009-01-01 00:01:00").unwrap(),
                    pgrx::datum::Timestamp::from_str("2009-02-01 00:00:00").unwrap(),
                ]
            );

            let results = c
                .select(
                    "SELECT datetime,symbol,bid,ask,details,amt,dt,tstz,bin,bcol,list,icol,map,lcol
                     FROM duckdb.iceberg_docs_example_bids
                     WHERE symbol in ('APL', 'MCS')
                     order by symbol",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL", "MCS"]);

            let results = c
                .select(
                    "SELECT *
                     FROM duckdb.iceberg_docs_example_bids
                     WHERE symbol = 'APL'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::JsonB, _>("details").unwrap())
                .map(|v| v.0.clone())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![json!({
                    "created_by": "alice",
                    "balance": 222.33,
                    "count": 42,
                    "valid": true
                })]
            );
        });
    }

    #[pg_test]
    #[should_panic]
    fn duckdb_local_file_read() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER duckdb_wrapper
                     HANDLER duckdb_fdw_handler VALIDATOR duckdb_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER duckdb_server_s3
                     FOREIGN DATA WRAPPER duckdb_wrapper
                     OPTIONS (
                       type 's3',
                       key_id 'admin',
                       secret 'password',
                       region 'us-east-1',
                       endpoint 'localhost:8000',
                       url_style 'path',
                       use_ssl 'false'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FOREIGN TABLE duckdb.passwd (a text)
                   SERVER duckdb_server_s3
                   OPTIONS (
                     table 'read_csv(''/etc/passwd'', sep = '':'')'
                   )"#,
                None,
                &[],
            )
            .unwrap();
            let _results = c.select("SELECT * FROM duckdb.passwd", None, &[]);
        });
    }
}
