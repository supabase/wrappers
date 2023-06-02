#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;

    #[pg_test]
    fn bigquery_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER bigquery_wrapper
                         HANDLER big_query_fdw_handler VALIDATOR big_query_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
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
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_table (
                    id bigint,
                    name text,
                    num numeric
                  )
                  SERVER my_bigquery_server
                  OPTIONS (
                    table 'test_table',
                    rowid_column 'id'
                  )
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_table_with_subquery (
                    id bigint,
                    name text
                  )
                  SERVER my_bigquery_server
                  OPTIONS (
                    table '(select id, upper(name) as name from `test_project.test_dataset.test_table`)'
                  )
             "#,
                None,
                None,
            ).unwrap();

            /*
             The tables below come from the code in docker-compose.yml that looks like this:

             ```
             volumes:
                   - ${PWD}/dockerfiles/bigquery/data.yaml:/app/data.yaml
             ```
            */

            let results = c
                .select("SELECT name FROM test_table", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["foo", "bar"]);

            let results = c
                .select(
                    "SELECT name FROM test_table ORDER BY id DESC, name LIMIT 1",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["bar"]);

            let results = c
                .select("SELECT name FROM test_table_with_subquery", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["FOO", "BAR"]);

            let results = c
                .select("SELECT num::text FROM test_table ORDER BY num", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("num").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["0.123", "1234.56789"]);

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
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["foo", "bar", "baz"]);
             */
        });
    }
}
