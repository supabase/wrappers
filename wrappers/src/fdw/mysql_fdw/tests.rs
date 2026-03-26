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
        exec_mysql_query(
            r#"CREATE TABLE IF NOT EXISTS testdb.users (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(200) NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                age INT,
                balance DECIMAL(12,2),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )"#,
        );
        exec_mysql_query("TRUNCATE TABLE testdb.users");
        exec_mysql_query(
            r#"INSERT INTO testdb.users (name, email, active, age, balance, created_at) VALUES
                ('Alice', 'alice@example.com', TRUE, 30, 120.50, '2026-03-01 10:00:00'),
                ('Bob', 'bob@example.com', FALSE, 41, 88.25, '2026-03-02 11:30:00'),
                ('Carol', 'carol@example.com', TRUE, 27, 999.99, '2026-03-03 09:15:00'),
                ('Dave', 'dave@example.com', TRUE, NULL, NULL, '2026-03-04 14:45:00')"#,
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
                r#"IMPORT FOREIGN SCHEMA "testdb" FROM SERVER mysql_server INTO mysql"#,
                None,
                &[],
            )
            .expect("failed to import foreign schema");

            let results = c
                .select("SELECT * FROM mysql.users order by id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Alice", "Bob", "Carol", "Dave"]);

            c.update(
                r#"INSERT INTO mysql.users
                    (name, email, active, age, balance, created_at)
                    VALUES ('Eve', 'eve@example.com', true, 35, 42.75, '2026-03-05 08:00:00')"#,
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
}
