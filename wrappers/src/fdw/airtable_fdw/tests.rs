#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;

    #[pg_test]
    fn airtable_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER airtable_wrapper
                         HANDLER airtable_fdw_handler VALIDATOR airtable_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER airtable_server
                         FOREIGN DATA WRAPPER airtable_wrapper
                         OPTIONS (
                            api_url 'http://localhost:8086',
                            api_key 'apiKey'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE airtable_table (
                    field1 numeric,
                    field2 text,
                    field3 timestamp
                  )
                  SERVER airtable_server
                  OPTIONS (
                    base_id 'baseID',
                    table_id 'table-foo'
                  )
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE airtable_view (
                    field1 numeric,
                    field2 text,
                    field3 timestamp
                  )
                  SERVER airtable_server
                  OPTIONS (
                    base_id 'baseID',
                    table_id 'table-foo',
                    view_id 'view-bar'
                  )
             "#,
                None,
                None,
            )
            .unwrap();


            /*
             The table data below comes from the code in wrappers/dockerfiles/airtable/server.py
            */

            let results = c
                .select("SELECT field2 FROM airtable_table WHERE field = 1", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("field2").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["two"]);

            let results = c
                .select("SELECT field2 FROM airtable_view", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("field2").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["three"]);
        });
    }
}
