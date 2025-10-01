#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    use serde_json::json;
    use std::str::FromStr;

    #[pg_test]
    fn iceberg_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER iceberg_wrapper
                     HANDLER iceberg_fdw_handler VALIDATOR iceberg_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER iceberg_server
                     FOREIGN DATA WRAPPER iceberg_wrapper
                     OPTIONS (
                       aws_access_key_id 'admin',
                       aws_secret_access_key 'password',
                       catalog_uri 'http://localhost:8181',
                       warehouse 'warehouse',
                       "s3.endpoint" 'http://localhost:8000'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS iceberg"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA "docs_example" FROM SERVER iceberg_server INTO iceberg"#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM iceberg.bids order by symbol", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL", "MCS"]);

            let results = c
                .select(
                    "SELECT * FROM iceberg.bids WHERE symbol in ('APL', 'XXX')",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL"]);

            let results = c
                .select("SELECT icol FROM iceberg.bids WHERE icol = 1234", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i32, _>("icol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![1234]);

            let results = c
                .select(
                    "SELECT lcol FROM iceberg.bids WHERE lcol >= 5678",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("lcol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![5678]);

            let results = c
                .select("SELECT symbol FROM iceberg.bids WHERE bcol", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL"]);

            let results = c
                .select(
                    "SELECT symbol FROM iceberg.bids WHERE bcol is true",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL"]);

            let results = c
                .select(
                    "SELECT symbol FROM iceberg.bids WHERE amt is null and ask = 11.22",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["MCS"]);

            let results = c
                .select(
                    "SELECT dt FROM iceberg.bids WHERE dt = date '2025-05-16'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::Date, _>("dt").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![pgrx::datum::Date::new(2025, 5, 16).unwrap()]);

            let results = c
                .select(
                    "SELECT tstz FROM iceberg.bids WHERE symbol like 'APL%'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<pgrx::datum::TimestampWithTimeZone, _>("tstz")
                        .unwrap()
                        .map(|t| t.to_utc())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    pgrx::datum::TimestampWithTimeZone::from_str("2025-05-16T12:34:56+08:00")
                        .unwrap()
                        .to_utc()
                ]
            );

            let results = c
                .select(
                    "SELECT bin FROM iceberg.bids WHERE symbol = 'MCS'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&[u8], _>("bin").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![&[67u8; 16]]);

            let results = c
                .select(
                    "SELECT uid FROM iceberg.bids WHERE symbol = 'MCS'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::Uuid, _>("uid").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![pgrx::datum::Uuid::from_bytes([66u8; 16])]);

            let results = c
                .select(
                    "SELECT details FROM iceberg.bids WHERE symbol = 'APL'",
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

            let results = c
                .select(
                    "SELECT list FROM iceberg.bids WHERE symbol = 'MCS'",
                    None,
                    &[],
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
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::JsonB, _>("map").unwrap())
                .map(|v| v.0.clone())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![json!({"nn": "qq", "nn2": "pp"})]);

            // test pushdown
            let _results = c
                .select(
                    "SELECT * FROM iceberg.bids
                    WHERE symbol not like 'APL%'
                      AND bid = 12.34 AND ask = 56.78
                      AND icol = 1234 AND lcol = 5678
                      AND datetime = '2025-09-15 11:22:33'
                      AND tstz = '2025-09-15 11:22:33+00:00'
                      AND tcol = '12:34:56.123'
                      AND bcol
                    ",
                    None,
                    &[],
                )
                .unwrap();

            // test data insertion with partitioning
            c.update(
                r#"INSERT INTO iceberg.bids(
                      datetime, symbol, bid, ask, amt, dt, tstz, list, map,
                      details, uid, lcol,
                      bin,
                      map2, map3, map4, map5, map6,
                      list2, list3, list4, list5, list6,
                      pat_col_year, pat_col_month, pat_col_hour,
                      pat_bcol, pat_icol, pat_lcol, pat_tcol
                   ) VALUES
                   ('2025-09-15 11:22:33', 'GOOG', 123.45, 1.23456, 123.345,
                     '2025-09-15', '2025-09-15 11:22:33+00:00',
                     '["row1a", "row1b"]',
                     '{"key1a": "value1a", "key1b": "value1b"}',
                     '{"created_by": "foo", "balance": 888.99, "balance2": 33.44, "count": 33, "count2": 44, "valid": false}',
                     null,
                     123456789,
                     E'\\xDEADBEEF',
                     null, null, null, null, null,
                     null, null, null, null, null,
                     '2025-09-15 11:22:33+00:00', '2025-09-15', '2025-09-15 11:22:33',
                     true, 123, 345, '2025-09-15 11:22:33'
                   ),
                   ('2025-09-16 11:22:33', 'META', 123.45, 1.23456, 123.345,
                     '2025-09-16', '2025-09-16 11:22:33+00:00',
                     null,
                     null,
                     null,
                     null,
                     123456789,
                     E'\\xDEADBEEF',
                     '{"aa": 222.33}', '{"aa": null, "bb": true}', '{"aa": 123}', '{"aa": 345}', '{"aa": 12.34}',
                     '[12, null, 56]', '[12.34, 56.78]', '[true, false]', '[1, 2]', '[123.45, 0.0]',
                     '2025-09-16 11:22:33+00:00', '2025-09-16', '2025-09-16 11:22:33',
                     null, null, null, null
                   )
                "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM iceberg.bids WHERE symbol = 'GOOG'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["GOOG"]);
            let results = c
                .select(
                    "SELECT details FROM iceberg.bids WHERE symbol = 'GOOG'",
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
                    "created_by": "foo",
                    "balance": 888.99,
                    "balance2": 33.44,
                    "count": 33,
                    "count2": 44,
                    "valid": false,
                })]
            );

            // test data insertion without partitioning
            c.update(
                r#"INSERT INTO iceberg.asks(
                      datetime, symbol, ask
                   ) VALUES
                   ('2025-09-15 11:22:33', 'APL', 123.45)
                "#,
                None,
                &[],
            )
            .unwrap();
        });
    }
}
