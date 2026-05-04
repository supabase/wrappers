#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use mysql_async::{Error as MySqlError, Pool, prelude::*};
    use pgrx::prelude::*;
    use supabase_wrappers::prelude::create_async_runtime;

    fn setup_mysql_test_data() {
        let rt = create_async_runtime().expect("failed to create runtime");

        let conn_str = "mysql://root:password@localhost:3306/testdb";
        let pool = Pool::new(conn_str);
        let mut conn = rt
            .block_on(async {
                let conn = pool.get_conn().await?;
                Ok::<_, MySqlError>(conn)
            })
            .expect("failed to connect to mysql");

        let mut exec_mysql_query = |sql: &str| {
            rt.block_on(async {
                conn.query_drop(sql).await?;
                Ok::<_, MySqlError>(())
            })
            .expect("failed to execute query");
        };
        exec_mysql_query("CREATE DATABASE IF NOT EXISTS testdb");
        exec_mysql_query("DROP TABLE IF EXISTS testdb.users");
        exec_mysql_query(
            r#"CREATE TABLE testdb.users (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(200) NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                age INT,
                score SMALLINT,
                rating FLOAT,
                salary DOUBLE,
                balance DECIMAL(12,2),
                birth_date DATE,
                metadata JSON,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )"#,
        );
        exec_mysql_query("TRUNCATE TABLE testdb.users");
        exec_mysql_query(
            r#"INSERT INTO testdb.users (name, email, active, age, score, rating, salary, balance, birth_date, metadata, created_at) VALUES
                ('Alice', 'alice@example.com', TRUE, 30, 85, 4.5, 75000.50, 120.50, '1995-03-15', '{\"role\": \"admin\", \"level\": 5}', '2026-03-01 10:00:00'),
                ('Bob', 'bob@example.com', FALSE, 41, 72, 3.5, 52000.75, 88.25, '1984-07-22', '{\"role\": \"user\", \"level\": 2}', '2026-03-02 11:30:00'),
                ('Carol', 'carol@example.com', TRUE, 27, 91, 5.0, 98000.00, 999.99, '1998-11-30', '{\"role\": \"editor\", \"level\": 3}', '2026-03-03 09:15:00'),
                ('Dave', 'dave@example.com', TRUE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-03-04 14:45:00')"#,
        );
    }

    fn setup_mysql_agg_test_data() {
        let rt = create_async_runtime().expect("failed to create runtime");
        let pool = Pool::new("mysql://root:password@localhost:3306/testdb");
        let mut conn = rt
            .block_on(async {
                let conn = pool.get_conn().await?;
                Ok::<_, MySqlError>(conn)
            })
            .expect("failed to connect to mysql");

        let mut exec = |sql: &str| {
            rt.block_on(async {
                conn.query_drop(sql).await?;
                Ok::<_, MySqlError>(())
            })
            .expect("failed to execute query");
        };

        exec("CREATE DATABASE IF NOT EXISTS testdb");
        exec("DROP TABLE IF EXISTS testdb.orders");
        exec(
            "CREATE TABLE testdb.orders (
                id      BIGINT PRIMARY KEY AUTO_INCREMENT,
                name    VARCHAR(100) NOT NULL,
                amount  DECIMAL(12,2) NOT NULL,
                status  VARCHAR(50) NOT NULL
            )",
        );
        exec(
            "INSERT INTO testdb.orders (name, amount, status) VALUES
                ('Alice', 100.00, 'active'),
                ('Bob',    50.00, 'active'),
                ('Carol', 200.00, 'inactive'),
                ('Dave',   75.00, 'active'),
                ('Eve',   150.00, 'inactive')",
        );
    }

    #[pg_test]
    fn mysql_smoketest() {
        setup_mysql_test_data();

        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER mysql_wrapper
                    HANDLER mysql_fdw_handler VALIDATOR mysql_fdw_validator"#,
                None,
                &[],
            )
            .expect("failed to create foreign data wrapper");
            c.update(
                r#"CREATE SERVER mysql_server
                     FOREIGN DATA WRAPPER mysql_wrapper
                     OPTIONS (
                       conn_string 'mysql://root:password@localhost:3306/testdb'
                     )"#,
                None,
                &[],
            )
            .expect("failed to create server");
            c.update(r#"CREATE SCHEMA IF NOT EXISTS mysql"#, None, &[])
                .expect("failed to create schema");
            c.update(
                r#"IMPORT FOREIGN SCHEMA "testdb" FROM SERVER mysql_server INTO mysql
                    OPTIONS (strict 'true')"#,
                None,
                &[],
            )
            .expect("failed to import foreign schema");
            c.update(
                r#"IMPORT FOREIGN SCHEMA "testdb" LIMIT TO (users) FROM SERVER mysql_server INTO mysql
                    OPTIONS (strict 'true')"#,
                None,
                &[],
            )
            .expect("failed to import foreign schema");
            c.update(
                r#"IMPORT FOREIGN SCHEMA "testdb" EXCEPT (users) FROM SERVER mysql_server INTO mysql"#,
                None,
                &[],
            )
            .expect("failed to import foreign schema");
            c.update(
                r#"
                CREATE FOREIGN TABLE mysql.users2 (
                    id bigint,
                    name text,
                    email text,
                    active boolean,
                    age integer,
                    balance numeric,
                    created_at timestamp
                )
                SERVER mysql_server
                OPTIONS (
                    table '(select id,name,email,active,age,balance,created_at from testdb.users)',
                    rowid_column 'id'
                );
            "#,
                None,
                &[],
            )
            .expect("failed to create foreign table with custom query");

            let results = c
                .select("SELECT * FROM mysql.users order by id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Alice", "Bob", "Carol", "Dave"]);

            let results = c
                .select("SELECT * FROM mysql.users2 order by id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Alice", "Bob", "Carol", "Dave"]);

            c.update(
                r#"INSERT INTO mysql.users
                    (name, email, active, age, score, rating, salary, balance, birth_date, metadata, created_at)
                    VALUES ('Eve', 'eve@example.com', true, 35, 88, 4.0, 60000.00, 42.75, '1990-06-10', '{"role": "guest", "level": 1}', '2026-03-05 08:00:00')"#,
                None,
                &[],
            )
            .expect("failed to insert row through mysql fdw");

            let inserted_email = c
                .select(
                    r#"SELECT email FROM mysql.users WHERE name = 'Eve'"#,
                    None,
                    &[],
                )
                .expect("failed to select inserted row")
                .next()
                .and_then(|r| r.get_by_name::<&str, _>("email").ok().flatten())
                .map(str::to_string);
            assert_eq!(inserted_email.as_deref(), Some("eve@example.com"));

            c.update(
                r#"UPDATE mysql.users
                    SET balance = 150.00, active = false
                    WHERE id = 1"#,
                None,
                &[],
            )
            .expect("failed to update row through mysql fdw");

            let updated = c
                .select(
                    r#"SELECT active, balance FROM mysql.users WHERE id = 1"#,
                    None,
                    &[],
                )
                .expect("failed to select updated row")
                .next()
                .map(|r| {
                    let active = r
                        .get_by_name::<bool, _>("active")
                        .expect("failed to decode active")
                        .expect("active is null");
                    let balance = r
                        .get_by_name::<pgrx::AnyNumeric, _>("balance")
                        .expect("failed to decode balance")
                        .expect("balance is null")
                        .to_string();
                    (active, balance)
                });
            assert_eq!(updated, Some((false, "150".to_string())));

            // Verify SMALLINT (score) round-trips correctly
            let score = c
                .select(r#"SELECT score FROM mysql.users WHERE id = 1"#, None, &[])
                .expect("failed to select score")
                .next()
                .and_then(|r| r.get_by_name::<i16, _>("score").ok().flatten());
            assert_eq!(score, Some(85i16));

            // Verify DATE (birth_date) round-trips correctly
            let birth_date = c
                .select(
                    r#"SELECT birth_date::text AS bd FROM mysql.users WHERE id = 1"#,
                    None,
                    &[],
                )
                .expect("failed to select birth_date")
                .next()
                .and_then(|r| r.get_by_name::<&str, _>("bd").ok().flatten())
                .map(str::to_string);
            assert_eq!(birth_date.as_deref(), Some("1995-03-15"));

            // Verify DOUBLE (salary) round-trips correctly
            let salary = c
                .select(r#"SELECT salary FROM mysql.users WHERE id = 3"#, None, &[])
                .expect("failed to select salary")
                .next()
                .and_then(|r| r.get_by_name::<f64, _>("salary").ok().flatten());
            assert_eq!(salary, Some(98000.0f64));

            // Verify FLOAT (rating) round-trips as real
            let rating = c
                .select(r#"SELECT rating FROM mysql.users WHERE id = 3"#, None, &[])
                .expect("failed to select rating")
                .next()
                .and_then(|r| r.get_by_name::<f32, _>("rating").ok().flatten());
            assert_eq!(rating, Some(5.0f32));

            // Verify JSON (metadata) is mapped to jsonb and round-trips correctly
            let metadata = c
                .select(
                    r#"SELECT metadata->>'role' AS role FROM mysql.users WHERE id = 1"#,
                    None,
                    &[],
                )
                .expect("failed to select metadata")
                .next()
                .and_then(|r| r.get_by_name::<&str, _>("role").ok().flatten())
                .map(str::to_string);
            assert_eq!(metadata.as_deref(), Some("admin"));

            // Verify NULL JSON round-trips as NULL
            let null_metadata = c
                .select(
                    r#"SELECT metadata FROM mysql.users WHERE id = 4"#,
                    None,
                    &[],
                )
                .expect("failed to select null metadata")
                .next()
                .and_then(|r| r.get_by_name::<pgrx::datum::JsonB, _>("metadata").ok())
                .expect("row missing");
            assert!(null_metadata.is_none());

            c.update(r#"DELETE FROM mysql.users WHERE id = 2"#, None, &[])
                .expect("failed to delete row through mysql fdw");

            let deleted_cnt = c
                .select(
                    r#"SELECT count(*) AS cnt FROM mysql.users WHERE id = 2"#,
                    None,
                    &[],
                )
                .expect("failed to count deleted rows")
                .next()
                .and_then(|r| r.get_by_name::<i64, _>("cnt").ok().flatten())
                .expect("count row missing");
            assert_eq!(deleted_cnt, 0);
        });
    }

    #[pg_test]
    fn mysql_aggregate_pushdown_test() {
        setup_mysql_agg_test_data();

        Spi::connect_mut(|c| {
            c.update(
                "CREATE FOREIGN DATA WRAPPER mysql_agg_wrapper
                 HANDLER mysql_fdw_handler VALIDATOR mysql_fdw_validator",
                None,
                &[],
            )
            .expect("failed to create FDW");
            c.update(
                "CREATE SERVER mysql_agg_server FOREIGN DATA WRAPPER mysql_agg_wrapper
                 OPTIONS (conn_string 'mysql://root:password@localhost:3306/testdb')",
                None,
                &[],
            )
            .expect("failed to create server");
            c.update("CREATE SCHEMA IF NOT EXISTS mysql_agg", None, &[])
                .expect("failed to create schema");
            c.update(
                "CREATE FOREIGN TABLE mysql_agg.orders (
                    id     bigint,
                    name   text,
                    amount numeric,
                    status text
                 )
                 SERVER mysql_agg_server
                 OPTIONS (table 'orders')",
                None,
                &[],
            )
            .expect("failed to create foreign table");
            // Subquery foreign table — exercises the starts_with('(') branch in
            // deparse_aggregate, which passes the subquery through unquoted.
            c.update(
                "CREATE FOREIGN TABLE mysql_agg.orders_sub (
                    amount numeric,
                    status text
                 )
                 SERVER mysql_agg_server
                 OPTIONS (table '(select amount, status from testdb.orders)')",
                None,
                &[],
            )
            .expect("failed to create subquery foreign table");

            // Helper: collect EXPLAIN lines and assert no local Aggregate node.
            // A pushed-down aggregate is a bare ForeignScan; a non-pushed aggregate
            // has a HashAggregate/GroupAggregate node above ForeignScan.
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
                        "Expected pushdown for [{}] but plan shows local aggregation:\n{:?}",
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
                        "Expected NO pushdown (local Aggregate) for [{}], plan:\n{:?}",
                        $sql,
                        plan
                    );
                }};
            }

            // --- COUNT(*) whole-table ---
            let cnt: i64 = c
                .select("SELECT COUNT(*) FROM mysql_agg.orders", None, &[])
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 5);
            assert_pushed_down!(c, "SELECT COUNT(*) FROM mysql_agg.orders");

            // --- COUNT(id) ---
            let cnt: i64 = c
                .select("SELECT COUNT(id) FROM mysql_agg.orders", None, &[])
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 5);
            assert_pushed_down!(c, "SELECT COUNT(id) FROM mysql_agg.orders");

            // --- COUNT(DISTINCT status) — 'active' and 'inactive' = 2 ---
            let cnt: i64 = c
                .select(
                    "SELECT COUNT(DISTINCT status) FROM mysql_agg.orders",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 2);
            assert_pushed_down!(c, "SELECT COUNT(DISTINCT status) FROM mysql_agg.orders");

            // --- SUM whole-table ---
            // amounts: 100, 50, 200, 75, 150 → sum=575
            let s: f64 = c
                .select("SELECT SUM(amount) FROM mysql_agg.orders", None, &[])
                .unwrap()
                .first()
                .get_one::<pgrx::AnyNumeric>()
                .unwrap()
                .unwrap()
                .to_string()
                .parse()
                .unwrap();
            assert!((s - 575.0).abs() < 0.01, "SUM expected 575.0, got {s}");
            assert_pushed_down!(c, "SELECT SUM(amount) FROM mysql_agg.orders");

            // --- AVG whole-table → 575/5 = 115.0 ---
            let avg: f64 = c
                .select("SELECT AVG(amount) FROM mysql_agg.orders", None, &[])
                .unwrap()
                .first()
                .get_one::<pgrx::AnyNumeric>()
                .unwrap()
                .unwrap()
                .to_string()
                .parse()
                .unwrap();
            assert!((avg - 115.0).abs() < 0.01, "AVG expected 115.0, got {avg}");
            assert_pushed_down!(c, "SELECT AVG(amount) FROM mysql_agg.orders");

            // --- MIN whole-table → 50 ---
            let mn: f64 = c
                .select("SELECT MIN(amount) FROM mysql_agg.orders", None, &[])
                .unwrap()
                .first()
                .get_one::<pgrx::AnyNumeric>()
                .unwrap()
                .unwrap()
                .to_string()
                .parse()
                .unwrap();
            assert!((mn - 50.0).abs() < 0.01, "MIN expected 50.0, got {mn}");
            assert_pushed_down!(c, "SELECT MIN(amount) FROM mysql_agg.orders");

            // --- MAX whole-table → 200 ---
            let mx: f64 = c
                .select("SELECT MAX(amount) FROM mysql_agg.orders", None, &[])
                .unwrap()
                .first()
                .get_one::<pgrx::AnyNumeric>()
                .unwrap()
                .unwrap()
                .to_string()
                .parse()
                .unwrap();
            assert!((mx - 200.0).abs() < 0.01, "MAX expected 200.0, got {mx}");
            assert_pushed_down!(c, "SELECT MAX(amount) FROM mysql_agg.orders");

            // --- GROUP BY status, COUNT(*) ---
            // active: Alice(100), Bob(50), Dave(75)   → count=3
            // inactive: Carol(200), Eve(150)           → count=2
            let mut rows: Vec<(String, i64)> = c
                .select(
                    "SELECT status, COUNT(*) AS cnt FROM mysql_agg.orders
                     GROUP BY status ORDER BY status",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let s = r
                        .get_by_name::<&str, _>("status")
                        .unwrap()
                        .unwrap()
                        .to_string();
                    let n = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (s, n)
                })
                .collect();
            rows.sort();
            assert_eq!(
                rows,
                vec![("active".to_string(), 3), ("inactive".to_string(), 2)]
            );
            assert_pushed_down!(
                c,
                "SELECT status, COUNT(*) FROM mysql_agg.orders GROUP BY status"
            );

            // --- WHERE + GROUP BY ---
            // amount > 75: Alice(100, active), Carol(200, inactive), Eve(150, inactive)
            // active SUM = 100; inactive SUM = 350
            let mut rows: Vec<(String, f64)> = c
                .select(
                    "SELECT status, SUM(amount) AS s FROM mysql_agg.orders
                     WHERE amount > 75 GROUP BY status ORDER BY status",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let st = r
                        .get_by_name::<&str, _>("status")
                        .unwrap()
                        .unwrap()
                        .to_string();
                    let s: f64 = r
                        .get_by_name::<pgrx::AnyNumeric, _>("s")
                        .unwrap()
                        .unwrap()
                        .to_string()
                        .parse()
                        .unwrap();
                    (st, s)
                })
                .collect();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].0, "active");
            assert!(
                (rows[0].1 - 100.0).abs() < 0.01,
                "active SUM expected 100.0, got {}",
                rows[0].1
            );
            assert_eq!(rows[1].0, "inactive");
            assert!(
                (rows[1].1 - 350.0).abs() < 0.01,
                "inactive SUM expected 350.0, got {}",
                rows[1].1
            );
            assert_pushed_down!(
                c,
                "SELECT status, SUM(amount) FROM mysql_agg.orders WHERE amount > 75 GROUP BY status"
            );

            // --- Negative: HAVING — must NOT push down ---
            // active count=3, inactive count=2 → HAVING COUNT(*) > 2 → only 'active'
            let rows: Vec<(String, i64)> = c
                .select(
                    "SELECT status, COUNT(*)::bigint AS cnt FROM mysql_agg.orders
                     GROUP BY status HAVING COUNT(*) > 2 ORDER BY status",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let st = r
                        .get_by_name::<&str, _>("status")
                        .unwrap()
                        .unwrap()
                        .to_string();
                    let n = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (st, n)
                })
                .collect();
            assert_eq!(rows, vec![("active".to_string(), 3)]);
            assert_not_pushed_down!(
                c,
                "SELECT status, COUNT(*) FROM mysql_agg.orders GROUP BY status HAVING COUNT(*) > 2"
            );

            // --- Negative: unsupported aggregate (STDDEV) ---
            // amounts: 100, 50, 200, 75, 150
            // mean=115, sum-of-sq-deviations=14500, sample stddev=sqrt(14500/4)≈60.21
            let stddev: f64 = c
                .select("SELECT STDDEV(amount) FROM mysql_agg.orders", None, &[])
                .unwrap()
                .first()
                .get_one::<pgrx::AnyNumeric>()
                .unwrap()
                .unwrap()
                .to_string()
                .parse()
                .unwrap();
            assert!(
                (stddev - 60.21).abs() < 0.1,
                "expected STDDEV ≈ 60.21, got {stddev}"
            );
            assert_not_pushed_down!(c, "SELECT STDDEV(amount) FROM mysql_agg.orders");

            // --- IN clause: array-valued qual must be included in the pushed-down WHERE ---
            // id IN (1,2,3) → Alice(100,active), Bob(50,active), Carol(200,inactive)
            // deparse_aggregate renders use_or=true array quals as `id`=1 or `id`=2 or `id`=3
            let cnt: i64 = c
                .select(
                    "SELECT COUNT(*) FROM mysql_agg.orders WHERE id IN (1, 2, 3)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 3, "COUNT(*) WHERE id IN (1,2,3) expected 3, got {cnt}");
            assert_pushed_down!(
                c,
                "SELECT COUNT(*) FROM mysql_agg.orders WHERE id IN (1, 2, 3)"
            );

            // SUM over the same IN filter: 100+50+200 = 350
            let s: f64 = c
                .select(
                    "SELECT SUM(amount) FROM mysql_agg.orders WHERE id IN (1, 2, 3)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<pgrx::AnyNumeric>()
                .unwrap()
                .unwrap()
                .to_string()
                .parse()
                .unwrap();
            assert!(
                (s - 350.0).abs() < 0.01,
                "SUM WHERE id IN (1,2,3) expected 350.0, got {s}"
            );
            assert_pushed_down!(
                c,
                "SELECT SUM(amount) FROM mysql_agg.orders WHERE id IN (1, 2, 3)"
            );

            // --- Subquery table: verify aggregate pushdown through starts_with('(') branch ---
            // COUNT(*) over the subquery — same 5 rows
            let cnt: i64 = c
                .select("SELECT COUNT(*) FROM mysql_agg.orders_sub", None, &[])
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 5);
            assert_pushed_down!(c, "SELECT COUNT(*) FROM mysql_agg.orders_sub");

            // GROUP BY SUM over the subquery
            // active: 100+50+75=225, inactive: 200+150=350
            let mut rows: Vec<(String, f64)> = c
                .select(
                    "SELECT status, SUM(amount) AS s FROM mysql_agg.orders_sub
                     GROUP BY status ORDER BY status",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let st = r
                        .get_by_name::<&str, _>("status")
                        .unwrap()
                        .unwrap()
                        .to_string();
                    let s: f64 = r
                        .get_by_name::<pgrx::AnyNumeric, _>("s")
                        .unwrap()
                        .unwrap()
                        .to_string()
                        .parse()
                        .unwrap();
                    (st, s)
                })
                .collect();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].0, "active");
            assert!(
                (rows[0].1 - 225.0).abs() < 0.01,
                "active SUM expected 225.0, got {}",
                rows[0].1
            );
            assert_eq!(rows[1].0, "inactive");
            assert!(
                (rows[1].1 - 350.0).abs() < 0.01,
                "inactive SUM expected 350.0, got {}",
                rows[1].1
            );
            assert_pushed_down!(
                c,
                "SELECT status, SUM(amount) FROM mysql_agg.orders_sub GROUP BY status"
            );
        });
    }
}
