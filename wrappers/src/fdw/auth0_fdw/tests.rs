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
                            url 'http://localhost:3796',
                            api_key 'apiKey'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE auth0_view (
                    created_at text,
                    email text,
                    email_verified bool,
                    identities jsonb
                  )
                  SERVER auth0_server
                  options (
                    object 'users'
                  )
             "#,
                None,
                None,
            )
            .unwrap();
            /*
             The table data below comes from the code in wrappers/dockerfiles/auth0/server.py
            */
            let results = c
                .select(
                    "SELECT * FROM auth0_view WHERE email = 'example@gmail.com'",
                    None,
                    None,
                )
                .expect("One record for the query")
                .collect::<Vec<_>>();
            assert_eq!(results.len(), 1);
        });
    }
}
