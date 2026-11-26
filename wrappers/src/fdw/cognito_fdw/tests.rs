#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::unresponsive_server::UnresponsiveServer;
    use pgrx::pg_test;
    use pgrx::prelude::*;

    #[pg_test]
    fn cognito_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"create foreign data wrapper cognito_wrapper
                         handler cognito_fdw_handler validator cognito_fdw_validator"#,
                None,
                &[],
            )
            .expect("Failed to create foreign data wrapper");
            c.update(
                r#"CREATE SERVER cognito_server
                         FOREIGN DATA WRAPPER cognito_wrapper
                         OPTIONS (
                            aws_access_key_id 'mysecretaccesskey',
                            aws_secret_access_key 'apiKey',
                            endpoint_url 'http://localhost:9229/',
                            user_pool_id 'local_6QNVVZIN',
                            region 'ap-southeast-1'
                         )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS cognito"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA cognito FROM SERVER cognito_server INTO cognito"#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM cognito.users WHERE email = 'test1'",
                    None,
                    &[],
                )
                .expect("One record for the query")
                .collect::<Vec<_>>();
            assert_eq!(results.len(), 1);
        });
    }

    /// Start an unresponsive server and create a foreign table.
    fn setup_unresponsive_server() -> UnresponsiveServer {
        let server = UnresponsiveServer::start();
        let port = server.port;

        Spi::connect_mut(|connection| {
            connection
                .update(
                    r#"CREATE FOREIGN DATA WRAPPER cognito_wrapper
                         HANDLER cognito_fdw_handler VALIDATOR cognito_fdw_validator"#,
                    None,
                    &[],
                )
                .unwrap();

            connection
                .update(
                    &format!(
                        r#"CREATE SERVER cognito_server
                         FOREIGN DATA WRAPPER cognito_wrapper
                         OPTIONS (
                            aws_access_key_id 'mysecretaccesskey',
                            aws_secret_access_key 'apiKey',
                            endpoint_url 'http://127.0.0.1:{port}/',
                            user_pool_id 'local_6QNVVZIN',
                            region 'ap-southeast-1'
                         )"#
                    ),
                    None,
                    &[],
                )
                .unwrap();

            connection
                .update(
                    r#"
                  CREATE FOREIGN TABLE cognito_table (username text)
                  SERVER cognito_server
                  OPTIONS (
                    object 'users'
                  )
             "#,
                    None,
                    &[],
                )
                .unwrap();
        });

        server
    }

    #[pg_test]
    #[ignore = "backend gets stuck due to blocking I/O"]
    fn cognito_backend_cancellable() {
        let server = setup_unresponsive_server();
        server.assert_cancellable("SELECT * from cognito_table");
    }

    #[pg_test(error = "terminating connection due to administrator command")]
    #[ignore = "backend gets stuck due to blocking I/O"]
    fn cognito_backend_terminable() {
        let server = setup_unresponsive_server();
        server.assert_terminable("SELECT * from cognito_table");
    }
}
