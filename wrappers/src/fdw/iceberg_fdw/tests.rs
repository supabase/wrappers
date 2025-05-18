#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    use serde_json::json;

    #[pg_test]
    fn iceberg_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER iceberg_wrapper
                     HANDLER iceberg_fdw_handler VALIDATOR iceberg_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER iceberg_server
                     FOREIGN DATA WRAPPER iceberg_wrapper
                     OPTIONS (
                       aws_access_key_id 'admin',
                       aws_secret_access_key 'password',
                       aws_region 'us-east-1',
                       catalog_uri 'http://localhost:8181',
                       s3_endpoint_url 'http://localhost:8000'
                     )"#,
                None,
                None,
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS iceberg"#, None, None)
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA "docs_example" FROM SERVER iceberg_server INTO iceberg"#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM iceberg.bids order by symbol", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL", "MCS"]);

            let results = c
                .select(
                    "SELECT * FROM iceberg.bids WHERE symbol in ('APL', 'XXX')",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL"]);

            let results = c
                .select(
                    "SELECT icol FROM iceberg.bids WHERE icol = 1234",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i32, _>("icol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![1234]);

            let results = c
                .select(
                    "SELECT lcol FROM iceberg.bids WHERE lcol >= 5678",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("lcol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![5678]);

            let results = c
                .select("SELECT symbol FROM iceberg.bids WHERE bcol", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL"]);

            let results = c
                .select(
                    "SELECT symbol FROM iceberg.bids WHERE bcol is true",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL"]);

            let results = c
                .select(
                    "SELECT symbol FROM iceberg.bids WHERE amt is null and ask = 11.22",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["MCS"]);

            let results = c
                .select(
                    "SELECT dt FROM iceberg.bids WHERE dt = date '2025-05-16'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::Date, _>("dt").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![pgrx::datum::Date::new(2025, 5, 16).unwrap()]);

            let results = c
                .select(
                    "SELECT tstz FROM iceberg.bids WHERE symbol = 'APL'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<pgrx::datum::TimestampWithTimeZone, _>("tstz")
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![pgrx::datum::TimestampWithTimeZone::new(2025, 5, 16, 12, 34, 56.0).unwrap()]
            );

            let results = c
                .select(
                    "SELECT bin FROM iceberg.bids WHERE symbol = 'MCS'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&[u8], _>("bin").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![&[67u8; 16]]);

            let results = c
                .select(
                    "SELECT uid FROM iceberg.bids WHERE symbol = 'MCS'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::Uuid, _>("uid").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![pgrx::datum::Uuid::from_bytes([66u8; 16])]);

            let results = c
                .select(
                    "SELECT details FROM iceberg.bids WHERE symbol = 'APL'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::JsonB, _>("details").unwrap())
                .map(|v| v.0.clone())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![json!({ "created_by": "alice" })]);

            let results = c
                .select(
                    "SELECT list FROM iceberg.bids WHERE symbol = 'MCS'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::JsonB, _>("list").unwrap())
                .map(|v| v.0.clone())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![json!(["xx", "yy"])]);

            let results = c
                .select(
                    "SELECT map FROM iceberg.bids WHERE symbol = 'APL'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::JsonB, _>("map").unwrap())
                .map(|v| v.0.clone())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![json!({"nn": "qq", "nn2": "pp"})]);
        });
    }
}
