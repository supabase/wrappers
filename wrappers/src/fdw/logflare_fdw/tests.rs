#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn logflare_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER logflare_wrapper
                         HANDLER logflare_fdw_handler VALIDATOR logflare_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER logflare_server
                         FOREIGN DATA WRAPPER logflare_wrapper
                         OPTIONS (
                            api_url 'http://localhost:4343/v1/endpoint',
                            api_key 'apiKey'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE logflare_table (
                    id text,
                    timestamp bigint,
                    event_message text,
                    _result text
                  )
                  SERVER logflare_server
                  OPTIONS (
                    endpoint '3d48cbfe-5d65-494f-be06-910479aed6c1'
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM logflare_table",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["84e1ed2a-3627-4d70-b311-c0e7c0bed313", "f45121ea-1738-46c9-a506-9ee52ff8220f"]);
        });
    }
}

