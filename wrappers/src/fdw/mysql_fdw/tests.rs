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
        exec_mysql_query(
            r#"
            CREATE DATABASE IF NOT EXISTS testdb;
            USE testdb;

            DROP TABLE IF EXISTS users;
            CREATE TABLE users (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(200) NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                age INT,
                balance DECIMAL(12,2),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            INSERT INTO users (name, email, active, age, balance, created_at) VALUES
                ('Alice', 'alice@example.com', TRUE, 30, 120.50, '2026-03-01 10:00:00'),
                ('Bob', 'bob@example.com', FALSE, 41, 88.25, '2026-03-02 11:30:00'),
                ('Carol', 'carol@example.com', TRUE, 27, 999.99, '2026-03-03 09:15:00'),
                ('Dave', 'dave@example.com', TRUE, NULL, NULL, '2026-03-04 14:45:00');
        "#,
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
                r#"
                CREATE FOREIGN TABLE mysql.users (
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
                    table 'users',
                    rowid_column 'id'
                );
            "#,
                None,
                &[],
            )
            .expect("failed to create foreign table");

            let results = c
                .select("SELECT * FROM mysql.users order by id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Alice", "Bob", "Carol", "Dave"]);
        });
    }
}
