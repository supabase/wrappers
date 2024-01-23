#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;

    #[pg_test]
    fn cognito_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"create foreign data wrapper cognito_wrapper
                         handler cognito_fdw_handler validator cognito_fdw_validator"#,
                None,
                None,
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
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE cognito_view (
                    email text,
                    username text
                  )
                  SERVER cognito_server
                  options (
                    object 'users'
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM cognito_view WHERE email = 'test1'",
                    None,
                    None,
                )
                .expect("One record for the query")
                .collect::<Vec<_>>();
            assert_eq!(results.len(), 1);
        });
    }
}
