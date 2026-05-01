#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;
    use supabase_wrappers::prelude::create_async_runtime;
    use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
    use tokio::net::TcpStream;
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    #[pg_test]
    fn mssql_smoketest() {
        let rt = create_async_runtime().expect("failed to create runtime");

        let mut config = Config::new();
        config.host("localhost");
        config.port(1433);
        config.encryption(EncryptionLevel::NotSupported);
        config.authentication(AuthMethod::sql_server("SA", "Password1234_56"));
        config.trust_cert();

        let tcp = rt
            .block_on(TcpStream::connect(config.get_addr()))
            .expect("cannot create TcpStream");
        tcp.set_nodelay(true).expect("TcpStream set nodelay");

        let mut client = rt
            .block_on(Client::connect(config, tcp.compat_write()))
            .expect("client connect");

        rt.block_on(async {
            client
                .execute(
                    "IF OBJECT_ID('users', 'U') IS NOT NULL DROP TABLE users",
                    &[],
                )
                .await?;
            client
                .execute(
                    r#"CREATE TABLE users (
                        id bigint,
                        name varchar(30),
                        amount numeric(18,2),
                        is_admin bit,
                        dt datetime2
                    )"#,
                    &[],
                )
                .await
        })
        .expect("create test table in SQL Server");

        rt.block_on(async {
            client
                .execute(
                    r#"
                    INSERT INTO users(id, name, amount, is_admin, dt) VALUES (42, 'foo', 12.34, 0, '2023-12-28');
                    INSERT INTO users(id, name, amount, is_admin, dt) VALUES (43, 'bar', 0.0, 1, '2023-12-27');
                    INSERT INTO users(id, name, amount, is_admin, dt) VALUES (44, 'baz', 42.0, 0, '2023-12-26');
                    "#,
                    &[],
                )
                .await
        })
        .expect("insert test data");

        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER mssql_wrapper
                         HANDLER mssql_fdw_handler VALIDATOR mssql_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER mssql_server
                         FOREIGN DATA WRAPPER mssql_wrapper
                         OPTIONS (
                           conn_string 'Server=localhost,1433;User=sa;Password=Password1234_56;Database=master;IntegratedSecurity=false;TrustServerCertificate=true;encrypt=DANGER_PLAINTEXT;ApplicationName=wrappers'
                         )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE mssql_users (
                    id bigint,
                    name text,
                    amount numeric(18,2),
                    is_admin boolean,
                    dt timestamp
                  )
                  SERVER mssql_server
                  OPTIONS (
                    table 'users'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE mssql_users_cust_sql (
                    id bigint,
                    name text
                  )
                  SERVER mssql_server
                  OPTIONS (
                    table '(select * from users where id = 42 or id = 43)'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT name, amount FROM mssql_users WHERE id = 42",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("name")
                        .unwrap()
                        .zip(r.get_by_name::<pgrx::Numeric<18, 2>, _>("amount").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![("foo", pgrx::Numeric::try_from(12.34).unwrap())]
            );

            let results = c
                .select("SELECT name FROM mssql_users ORDER BY id DESC", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["baz", "bar", "foo"]);

            let results = c
                .select(
                    "SELECT name FROM mssql_users ORDER BY id LIMIT 2 OFFSET 1",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["bar", "baz"]);

            let results = c
                .select(
                    "SELECT name FROM mssql_users WHERE name like 'ba%' ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["bar", "baz"]);

            let results = c
                .select(
                    "SELECT name FROM mssql_users WHERE name not like 'ba%'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo"]);

            let results = c
                .select(
                    "SELECT name FROM mssql_users_cust_sql ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo", "bar"]);

            let results = c
                .select(
                    "SELECT name FROM mssql_users WHERE is_admin is true",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["bar"]);
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect(|c| {
                c.select("SELECT name FROM mssql_users LIMIT 2 OFFSET 1", None, &[])
                    .is_err()
            })
        });
        assert!(result.is_err());
    }

    /// Verify aggregate queries are pushed down to SQL Server.
    ///
    /// Test data (table `agg_test`):
    ///   id=1, name='alice', amount=10.0
    ///   id=1, name='alice', amount=20.0   (duplicate name)
    ///   id=2, name='bob',   amount=30.0
    ///   id=2, name='carol', amount=40.0
    #[pg_test]
    fn mssql_aggregate_pushdown_test() {
        let rt = create_async_runtime().expect("failed to create runtime");

        let mut config = Config::new();
        config.host("localhost");
        config.port(1433);
        config.encryption(EncryptionLevel::NotSupported);
        config.authentication(AuthMethod::sql_server("SA", "Password1234_56"));
        config.trust_cert();

        let tcp = rt
            .block_on(TcpStream::connect(config.get_addr()))
            .expect("cannot create TcpStream");
        tcp.set_nodelay(true).expect("TcpStream set nodelay");

        let mut client = rt
            .block_on(Client::connect(config, tcp.compat_write()))
            .expect("client connect");

        rt.block_on(async {
            client
                .execute(
                    "IF OBJECT_ID('agg_test', 'U') IS NOT NULL DROP TABLE agg_test",
                    &[],
                )
                .await?;
            client
                .execute(
                    "CREATE TABLE agg_test (
                        id     bigint,
                        name   varchar(30),
                        amount numeric(18,2)
                     )",
                    &[],
                )
                .await?;
            client
                .execute(
                    "INSERT INTO agg_test (id, name, amount) VALUES
                        (1, 'alice', 10.0),
                        (1, 'alice', 20.0),
                        (2, 'bob',   30.0),
                        (2, 'carol', 40.0)",
                    &[],
                )
                .await
        })
        .expect("seed agg_test in SQL Server");

        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER mssql_agg_wrapper
                         HANDLER mssql_fdw_handler VALIDATOR mssql_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER mssql_agg_server
                         FOREIGN DATA WRAPPER mssql_agg_wrapper
                         OPTIONS (
                           conn_string 'Server=localhost,1433;User=sa;Password=Password1234_56;Database=master;IntegratedSecurity=false;TrustServerCertificate=true;encrypt=DANGER_PLAINTEXT;ApplicationName=wrappers'
                         )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FOREIGN TABLE agg_test (
                        id     bigint,
                        name   text,
                        amount numeric(18,2)
                     )
                     SERVER mssql_agg_server
                     OPTIONS (table 'agg_test')"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FOREIGN TABLE agg_sub (
                        id     bigint,
                        name   text,
                        amount numeric(18,2)
                     )
                     SERVER mssql_agg_server
                     OPTIONS (table '(select * from agg_test where id < 3)')"#,
                None,
                &[],
            )
            .unwrap();

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

            // --- COUNT(*) — whole-table scalar aggregate (mapped to count_big)
            let cnt: i64 = c
                .select("SELECT COUNT(*) FROM agg_test", None, &[])
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 4);
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

            // --- SUM(amount) GROUP BY id (numeric → CAST to numeric(38,10)) ---
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
            assert!(
                f64::try_from(rows[0].1.clone()).unwrap() - 30.0 < 1e-6,
                "SUM(amount) for id=1 expected 30, got {:?}",
                rows[0].1
            );
            assert!(
                f64::try_from(rows[1].1.clone()).unwrap() - 70.0 < 1e-6,
                "SUM(amount) for id=2 expected 70, got {:?}",
                rows[1].1
            );
            assert_pushed_down!(c, "SELECT id, SUM(amount) FROM agg_test GROUP BY id");

            // --- AVG(amount) — whole-table ---
            let avg: pgrx::AnyNumeric = c
                .select("SELECT AVG(amount) FROM agg_test", None, &[])
                .unwrap()
                .first()
                .get_one::<pgrx::AnyNumeric>()
                .unwrap()
                .unwrap();
            let avg_f = f64::try_from(avg.clone()).unwrap();
            assert!(
                (avg_f - 25.0).abs() < 1e-6,
                "AVG(amount) expected 25, got {avg_f}"
            );
            assert_pushed_down!(c, "SELECT AVG(amount) FROM agg_test");

            // --- MIN / MAX in the same query (bigint → CAST to bigint) ---
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

            // --- WHERE + aggregate: qual is pushed alongside the aggregate
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
            let cnt: i64 = c
                .select("SELECT COUNT(*) FROM agg_sub", None, &[])
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 4);
            assert_pushed_down!(c, "SELECT COUNT(*) FROM agg_sub");

            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id, COUNT(*)::bigint AS cnt FROM agg_sub GROUP BY id ORDER BY id",
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
            assert_pushed_down!(c, "SELECT id, COUNT(*) FROM agg_sub GROUP BY id");

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

            // FILTER clause is not pushed down.
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
