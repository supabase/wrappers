#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use pgx::pg_test;
    use pgx::prelude::*;

    #[pg_test]
    fn bigquery_smoketest() {
        Spi::execute(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER bigquery_wrapper
                         HANDLER big_query_fdw_handler VALIDATOR big_query_fdw_validator"#,
                None,
                None,
            );
            c.update(
                r#"CREATE SERVER my_bigquery_server
                         FOREIGN DATA WRAPPER bigquery_wrapper
                         OPTIONS (
                           project_id 'test_project',
                           dataset_id 'test_dataset',
                           api_endpoint 'http://localhost:9111',
                           mock_auth 'true'
                         )"#,
                None,
                None,
            );
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_table (
                    id bigint,
                    name text
                  )
                  SERVER my_bigquery_server
                  OPTIONS (
                    table 'test_table',
                    rowid_column 'id'
                  )
             "#,
                None,
                None,
            );

            /*
             The tables below come from the code in docker-compose.yml that looks like this:

             ```
             volumes:
                   - ${PWD}/dockerfiles/bigquery/data.yaml:/app/data.yaml
             ```
            */

            let results = c
                .select("SELECT name FROM test_table", None, None)
                .filter_map(|r| r.by_name("name").ok().and_then(|v| v.value::<&str>()))
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["foo", "bar"]);

            // DISABLED: error: [FIXME]
            // insert failed: Request error (error: error decoding response body: missing field `status` at line 1 column 436)

            /*
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                Some(vec![(
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    "baz".into_datum(),
                )]),
            );

            let results = c
                .select("SELECT name FROM test_table", None, None)
                .filter_map(|r| r.by_name("name").ok().and_then(|v| v.value::<&str>()))
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["foo", "bar", "baz"]);
             */
        });
    }
}
