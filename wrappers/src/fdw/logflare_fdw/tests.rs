#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn logflare_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER logflare_wrapper
                         HANDLER logflare_fdw_handler VALIDATOR logflare_fdw_validator"#,
                None,
                &[],
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
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE logflare_table (
                    id text,
                    timestamp bigint,
                    event_message text,
                    bool_field bool,
                    char_field "char",
                    int2_field smallint,
                    int4_field integer,
                    int8_field bigint,
                    float4_field real,
                    float8_field double precision,
                    numeric_field numeric,
                    date_field date,
                    ts_field timestamp,
                    tstz_field timestamptz,
                    _param_id text,
                    _param_date_field date,
                    _param_ts_field timestamp,
                    _param_tstz_field timestamptz,
                    _result text
                  )
                  SERVER logflare_server
                  OPTIONS (
                    endpoint '3d48cbfe-5d65-494f-be06-910479aed6c1'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM logflare_table", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![
                    "84e1ed2a-3627-4d70-b311-c0e7c0bed313",
                    "f45121ea-1738-46c9-a506-9ee52ff8220f"
                ]
            );

            let results = c
                .select(
                    "
                    SELECT * FROM logflare_table
                    WHERE _param_id = '84e1ed2a-3627-4d70-b311-c0e7c0bed313'
                    AND _param_date_field = '2023-07-19'::date
                    AND _param_ts_field = '2023-07-19T06:39:15'::timestamp
                    AND _param_tstz_field = '2023-07-19T06:39:15.000Z'::timestamptz
                ",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["84e1ed2a-3627-4d70-b311-c0e7c0bed313"]);
        });
    }

    // The backend must still respond to query cancel, statement_timeout and
    // pg_terminate_backend() while waiting on a remote which never answers
    #[pg_test]
    fn logflare_cancellable_while_remote_unresponsive() {
        let server = crate::test_utils::UnresponsiveServer::start();
        let port = server.port;
        Spi::run(
            r#"CREATE FOREIGN DATA WRAPPER logflare_unresponsive_wrapper
                 HANDLER logflare_fdw_handler VALIDATOR logflare_fdw_validator"#,
        )
        .unwrap();
        Spi::run(&format!(
            r#"CREATE SERVER logflare_unresponsive_server
                 FOREIGN DATA WRAPPER logflare_unresponsive_wrapper
                 OPTIONS (
                   api_url 'http://127.0.0.1:{port}/v1/endpoint',
                   api_key 'apiKey'
                 )"#
        ))
        .unwrap();
        Spi::run(
            r#"CREATE FOREIGN TABLE logflare_unresponsive_table (id text)
                 SERVER logflare_unresponsive_server
                 OPTIONS (endpoint '3d48cbfe-5d65-494f-be06-910479aed6c1')"#,
        )
        .unwrap();

        server.assert_cancellable("SELECT * FROM logflare_unresponsive_table");
    }
}
