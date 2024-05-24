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
                    r#"CREATE TABLE users (
                        id bigint,
                        name varchar(30),
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
                    INSERT INTO users(id, name, dt) VALUES (42, 'foo', '2023-12-28');
                    INSERT INTO users(id, name, dt) VALUES (43, 'bar', '2023-12-27');
                    INSERT INTO users(id, name, dt) VALUES (44, 'baz', '2023-12-26');
                    "#,
                    &[],
                )
                .await
        })
        .expect("insert test data");

        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER mssql_wrapper
                         HANDLER mssql_fdw_handler VALIDATOR mssql_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER mssql_server
                         FOREIGN DATA WRAPPER mssql_wrapper
                         OPTIONS (
                           conn_string 'Server=localhost,1433;User=sa;Password=Password1234_56;Database=master;IntegratedSecurity=false;TrustServerCertificate=true;encrypt=DANGER_PLAINTEXT;ApplicationName=wrappers'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE mssql_users (
                    id bigint,
                    name text,
                    dt timestamp
                  )
                  SERVER mssql_server
                  OPTIONS (
                    table 'users'
                  )
             "#,
                None,
                None,
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
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT name FROM mssql_users WHERE id = 42", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo"]);

            let results = c
                .select("SELECT name FROM mssql_users ORDER BY id DESC", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["baz", "bar", "foo"]);

            let results = c
                .select(
                    "SELECT name FROM mssql_users ORDER BY id LIMIT 2 OFFSET 1",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["bar", "baz"]);

            let results = c
                .select(
                    "SELECT name FROM mssql_users WHERE name like 'ba%' ORDER BY id",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["bar", "baz"]);

            let results = c
                .select(
                    "SELECT name FROM mssql_users WHERE name not like 'ba%'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo"]);

            let results = c
                .select(
                    "SELECT name FROM mssql_users_cust_sql ORDER BY id",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo", "bar"]);
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect(|c| {
                c.select("SELECT name FROM mssql_users LIMIT 2 OFFSET 1", None, None)
                    .is_err()
            })
        });
        assert!(result.is_err());
    }
}
