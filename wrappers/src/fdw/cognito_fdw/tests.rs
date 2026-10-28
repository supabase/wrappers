#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
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

    // The backend must still respond to query cancel, statement_timeout and
    // pg_terminate_backend() while waiting on a remote which never answers
    #[pg_test]
    fn cognito_cancellable_while_remote_unresponsive() {
        let server = crate::test_utils::UnresponsiveServer::start();
        let port = server.port;
        Spi::run(
            r#"CREATE FOREIGN DATA WRAPPER cognito_unresponsive_wrapper
                 HANDLER cognito_fdw_handler VALIDATOR cognito_fdw_validator"#,
        )
        .unwrap();
        Spi::run(&format!(
            r#"CREATE SERVER cognito_unresponsive_server
                 FOREIGN DATA WRAPPER cognito_unresponsive_wrapper
                 OPTIONS (
                   aws_access_key_id 'mysecretaccesskey',
                   aws_secret_access_key 'apiKey',
                   endpoint_url 'http://127.0.0.1:{port}/',
                   user_pool_id 'local_6QNVVZIN',
                   region 'ap-southeast-1'
                 )"#
        ))
        .unwrap();
        Spi::run(
            r#"CREATE FOREIGN TABLE cognito_unresponsive_table (username text)
                 SERVER cognito_unresponsive_server
                 OPTIONS (object 'users')"#,
        )
        .unwrap();

        server.assert_cancellable("SELECT * FROM cognito_unresponsive_table");
    }
}
