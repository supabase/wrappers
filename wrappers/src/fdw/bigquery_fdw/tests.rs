#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;

    #[pg_test]
    fn bigquery_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER bigquery_wrapper
                         HANDLER big_query_fdw_handler VALIDATOR big_query_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_bigquery_server
                         FOREIGN DATA WRAPPER bigquery_wrapper
                         OPTIONS (
                           project_id 'test_project',
                           dataset_id 'test_dataset',
                           api_endpoint 'http://localhost:9111',
                           mock_auth 'true'
                         )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_table (
                    id bigint,
                    name text,
                    num numeric,
                    is_active boolean,
                    col_int8 "char",
                    col_int16 smallint,
                    col_int32 integer,
                    col_float32 real,
                    col_float64 double precision,
                    attrs jsonb,
                    signup_dt date,
                    created_at timestamp
                  )
                  SERVER my_bigquery_server
                  OPTIONS (
                    table 'test_table',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_table_with_subquery (
                    id bigint,
                    name text
                  )
                  SERVER my_bigquery_server
                  OPTIONS (
                    table '(select id, upper(name) as name from `test_project.test_dataset.test_table`)'
                  )
             "#,
                None,
                &[],
            ).unwrap();

            /*
             The tables below come from the code in docker-compose.yml that looks like this:

             ```
             volumes:
                   - ${PWD}/dockerfiles/bigquery/data.yaml:/app/data.yaml
             ```
            */

            let results = c
                .select("SELECT * FROM test_table", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["foo", "bar"]);

            let results = c
                .select(
                    "SELECT name FROM test_table ORDER BY id DESC, name LIMIT 1",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["bar"]);

            let results = c
                .select("SELECT name FROM test_table_with_subquery", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["FOO", "BAR"]);

            let results = c
                .select("SELECT num::text FROM test_table ORDER BY num", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("num").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["0.123", "1234.56789"]);

            c.update(
                "INSERT INTO test_table (id, name, num, is_active, col_int8, col_int16, col_int32, col_float32, col_float64, attrs, signup_dt, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::date, $12::timestamp)",
                None,
                &[
                    42.into(),
                    "baz".into(),
                    9.876_f64.into(),
                    true.into(),
                    8_i8.into(),
                    16_i16.into(),
                    32_i32.into(),
                    1.23_f32.into(),
                    4.56_f64.into(),
                    r#"{"source":"test","active":true}"#.into(),
                    "2024-01-02".into(),
                    "2024-01-02 03:04:05".into(),
                ],
            )
            .unwrap();

            let results = c
                .select("SELECT name FROM test_table", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo", "bar", "baz"]);

            c.update(
                "UPDATE test_table SET name = $1 WHERE id = $2",
                None,
                &["qux".into(), 42.into()],
            )
            .unwrap();

            let results = c
                .select("SELECT name FROM test_table ORDER BY id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo", "bar", "qux"]);

            c.update("DELETE FROM test_table WHERE id = $1", None, &[42.into()])
                .unwrap();

            let results = c
                .select("SELECT name FROM test_table ORDER BY id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo", "bar"]);
        });
    }

    #[pg_test]
    #[should_panic]
    fn bigquery_unsupported_type() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER bigquery_wrapper
                         HANDLER big_query_fdw_handler VALIDATOR big_query_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_bigquery_server
                         FOREIGN DATA WRAPPER bigquery_wrapper
                         OPTIONS (
                           project_id 'test_project',
                           dataset_id 'test_dataset',
                           api_endpoint 'http://localhost:9111',
                           mock_auth 'true'
                         )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_unsupported_type (
                    id bigint,
                    time_col time
                  )
                  SERVER my_bigquery_server
                  OPTIONS (
                    table 'test_unsupported_type',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let _results = c.update(
                "INSERT INTO test_unsupported_type (id, time_col) VALUES (42, '12:30:00.45')",
                None,
                &[],
            );
        });
    }

    /// Verify aggregate queries are pushed down to BigQuery.
    ///
    /// Reads against the `agg_test` table seeded by
    /// `wrappers/dockerfiles/bigquery/data.yaml`:
    ///   id=1, name='alice', amount=10
    ///   id=1, name='alice', amount=20  (duplicate name, same id)
    ///   id=2, name='bob',   amount=30
    ///   id=2, name='carol', amount=40
    #[pg_test]
    fn bigquery_aggregate_pushdown_test() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER bigquery_wrapper
                         HANDLER big_query_fdw_handler VALIDATOR big_query_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_bigquery_server
                         FOREIGN DATA WRAPPER bigquery_wrapper
                         OPTIONS (
                           project_id 'test_project',
                           dataset_id 'test_dataset',
                           api_endpoint 'http://localhost:9111',
                           mock_auth 'true'
                         )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE agg_test (
                    id     bigint,
                    name   text,
                    amount numeric
                  )
                  SERVER my_bigquery_server
                  OPTIONS (
                    table 'agg_test'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE agg_test_sub (
                    id     bigint,
                    name   text,
                    amount numeric
                  )
                  SERVER my_bigquery_server
                  OPTIONS (
                    table '(select id, name, amount from `test_project.test_dataset.agg_test` where id < 3)'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            // Helper macros: assert that EXPLAIN does (or does not) contain a
            // local Aggregate node. Plan-node lines always include "(cost=...)";
            // FDW EXPLAIN extras (e.g. "Wrappers: aggregates = [...]") do not,
            // so combining both substrings avoids false positives from the FDW's
            // own debug output.
            macro_rules! assert_pushed_down {
                ($c:expr, $sql:expr) => {{
                    let explain = format!("EXPLAIN {}", $sql);
                    let plan: Vec<String> = $c
                        .select(&explain, None, &[])
                        .unwrap()
                        .filter_map(|r| r.get::<&str>(1).unwrap().map(|s| s.to_string()))
                        .collect();
                    assert!(
                        !plan
                            .iter()
                            .any(|l| l.contains("Aggregate") && l.contains("(cost=")),
                        "Expected pushdown for [{}] but plan shows local aggregation: {:?}",
                        $sql,
                        plan
                    );
                }};
            }

            macro_rules! assert_not_pushed_down {
                ($c:expr, $sql:expr) => {{
                    let explain = format!("EXPLAIN {}", $sql);
                    let plan: Vec<String> = $c
                        .select(&explain, None, &[])
                        .unwrap()
                        .filter_map(|r| r.get::<&str>(1).unwrap().map(|s| s.to_string()))
                        .collect();
                    assert!(
                        plan.iter()
                            .any(|l| l.contains("Aggregate") && l.contains("(cost=")),
                        "Expected NO pushdown (local Aggregate) for [{}], plan: {:?}",
                        $sql,
                        plan
                    );
                }};
            }

            // --- COUNT(*) — whole-table scalar aggregate ---
            assert_eq!(
                c.select("SELECT COUNT(*) FROM agg_test", None, &[])
                    .unwrap()
                    .first()
                    .get_one::<i64>()
                    .unwrap()
                    .unwrap(),
                4
            );
            assert_pushed_down!(c, "SELECT COUNT(*) FROM agg_test");

            // --- COUNT(name) GROUP BY id ---
            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id, COUNT(name) AS cnt FROM agg_test GROUP BY id ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let id = r.get_by_name::<i64, _>("id").unwrap().unwrap();
                    let cnt = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (id, cnt)
                })
                .collect();
            rows.sort();
            assert_eq!(rows, vec![(1, 2), (2, 2)]);
            assert_pushed_down!(c, "SELECT id, COUNT(name) AS cnt FROM agg_test GROUP BY id");

            // --- SUM(amount) GROUP BY id ---
            let mut rows: Vec<(i64, pgrx::AnyNumeric)> = c
                .select(
                    "SELECT id, SUM(amount) AS s FROM agg_test GROUP BY id ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let id = r.get_by_name::<i64, _>("id").unwrap().unwrap();
                    let s = r.get_by_name::<pgrx::AnyNumeric, _>("s").unwrap().unwrap();
                    (id, s)
                })
                .collect();
            rows.sort_by_key(|&(id, _)| id);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].0, 1);
            assert_eq!(rows[1].0, 2);
            assert!((f64::try_from(rows[0].1.clone()).unwrap() - 30.0).abs() < 1e-6);
            assert!((f64::try_from(rows[1].1.clone()).unwrap() - 70.0).abs() < 1e-6);
            assert_pushed_down!(c, "SELECT id, SUM(amount) FROM agg_test GROUP BY id");

            // --- AVG(amount) — whole-table ---
            let avg: pgrx::AnyNumeric = c
                .select("SELECT AVG(amount) FROM agg_test", None, &[])
                .unwrap()
                .first()
                .get_one::<pgrx::AnyNumeric>()
                .unwrap()
                .unwrap();
            assert!((f64::try_from(avg).unwrap() - 25.0).abs() < 1e-6);
            assert_pushed_down!(c, "SELECT AVG(amount) FROM agg_test");

            // --- MIN / MAX in the same query ---
            assert_eq!(
                c.select("SELECT MIN(id), MAX(id) FROM agg_test", None, &[])
                    .unwrap()
                    .first()
                    .get_two::<i64, i64>()
                    .unwrap(),
                (Some(1), Some(2))
            );
            assert_pushed_down!(c, "SELECT MIN(id), MAX(id) FROM agg_test");

            // --- COUNT(DISTINCT name) — whole-table (alice, bob, carol = 3) ---
            assert_eq!(
                c.select("SELECT COUNT(DISTINCT name) FROM agg_test", None, &[])
                    .unwrap()
                    .first()
                    .get_one::<i64>()
                    .unwrap()
                    .unwrap(),
                3
            );
            assert_pushed_down!(c, "SELECT COUNT(DISTINCT name) FROM agg_test");

            // --- WHERE + aggregate: the qual is pushed alongside the agg ---
            let s: pgrx::AnyNumeric = c
                .select("SELECT SUM(amount) FROM agg_test WHERE id = 1", None, &[])
                .unwrap()
                .first()
                .get_one::<pgrx::AnyNumeric>()
                .unwrap()
                .unwrap();
            assert!((f64::try_from(s).unwrap() - 30.0).abs() < 1e-6);
            assert_pushed_down!(c, "SELECT SUM(amount) FROM agg_test WHERE id = 1");

            // --- Aggregate over sub-query in `table` option ---
            assert_eq!(
                c.select("SELECT COUNT(*) FROM agg_test_sub", None, &[])
                    .unwrap()
                    .first()
                    .get_one::<i64>()
                    .unwrap()
                    .unwrap(),
                4
            );
            assert_pushed_down!(c, "SELECT COUNT(*) FROM agg_test_sub");

            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id, COUNT(*)::bigint AS cnt FROM agg_test_sub GROUP BY id ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let id = r.get_by_name::<i64, _>("id").unwrap().unwrap();
                    let cnt = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (id, cnt)
                })
                .collect();
            rows.sort();
            assert_eq!(rows, vec![(1, 2), (2, 2)]);
            assert_pushed_down!(c, "SELECT id, COUNT(*) FROM agg_test_sub GROUP BY id");

            // --- Negative cases: planner falls back to local Aggregate ---

            // HAVING clause is not pushed down.
            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id, COUNT(*)::bigint AS cnt FROM agg_test \
                     GROUP BY id HAVING COUNT(*) > 1 ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let id = r.get_by_name::<i64, _>("id").unwrap().unwrap();
                    let cnt = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (id, cnt)
                })
                .collect();
            rows.sort();
            assert_eq!(rows, vec![(1, 2), (2, 2)]);
            assert_not_pushed_down!(
                c,
                "SELECT id, COUNT(*) FROM agg_test GROUP BY id HAVING COUNT(*) > 1"
            );

            // FILTER clause on aggregate is not pushed down.
            let cnt: i64 = c
                .select(
                    "SELECT COUNT(*) FILTER (WHERE id = 1) FROM agg_test",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 2);
            assert_not_pushed_down!(c, "SELECT COUNT(*) FILTER (WHERE id = 1) FROM agg_test");

            // GROUP BY non-column expression is not pushed down.
            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id + 1 AS g, COUNT(*)::bigint AS cnt FROM agg_test \
                     GROUP BY id + 1 ORDER BY g",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let g = r.get_by_name::<i64, _>("g").unwrap().unwrap();
                    let cnt = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (g, cnt)
                })
                .collect();
            rows.sort();
            assert_eq!(rows, vec![(2, 2), (3, 2)]);
            assert_not_pushed_down!(
                c,
                "SELECT id + 1 AS g, COUNT(*) FROM agg_test GROUP BY id + 1"
            );
        });
    }
}
