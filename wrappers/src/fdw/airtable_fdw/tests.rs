#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::{prelude::*, JsonB};

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
                    numeric_field numeric,
                    string_field text,
                    timestamp_field timestamp,
                    strings_array_field jsonb,
                    object_field jsonb
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
                    numeric_field numeric,
                    string_field text,
                    timestamp_field timestamp,
                    strings_array_field jsonb,
                    object_field jsonb
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
                .select(
                    "SELECT string_field FROM airtable_table WHERE numeric_field = 1",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("string_field").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["two"]);

            let results = c
                .select(
                    "SELECT strings_array_field FROM airtable_table WHERE numeric_field = 1",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<JsonB, _>("strings_array_field")
                        .expect("strings_array_field is missing")
                        .and_then(|v| serde_json::from_value::<Vec<String>>(v.0.to_owned()).ok())
                })
                .collect::<Vec<_>>();

            assert_eq!(results, vec![vec!["foo", "bar"]]);

            #[derive(serde::Deserialize)]
            struct Foo {
                foo: String,
            }

            let results = c
                .select(
                    "SELECT object_field FROM airtable_table WHERE numeric_field = 1",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<JsonB, _>("object_field")
                        .expect("object_field is missing")
                        .and_then(|v| serde_json::from_value::<Foo>(v.0.to_owned()).ok())
                })
                .collect::<Vec<_>>();
            assert_eq!(results[0].foo, "bar");

            let results = c
                .select("SELECT string_field FROM airtable_view", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("string_field").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["three"]);
        });
    }
}
