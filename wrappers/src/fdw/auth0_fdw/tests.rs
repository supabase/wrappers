#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;

    #[pg_test]
    fn auth0_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"create foreign data wrapper auth0_wrapper
                         handler auth0_fdw_handler validator auth0_fdw_validator"#,
                None,
                None,
            )
            .expect("Failed to create foreign data wrapper");
            c.update(
                r#"CREATE SERVER auth0_server
                         FOREIGN DATA WRAPPER auth0_wrapper
                         OPTIONS (
                            api_url 'http://localhost:3796',
                            api_key 'apiKey'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE auth0_view (
                    string_field text
                  )
                  SERVER auth0_server
             "#,
                None,
                None,
            )
            .unwrap();
        });
    }
}
