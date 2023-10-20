#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
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
                    bool_field bool,
                    numeric_field numeric,
                    string_field text,
                    timestamp_field timestamp,
                    object_field jsonb,
                    strings_array_field jsonb,
                    numerics_array_field jsonb,
                    bools_array_field jsonb,
                    objects_array_field jsonb
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
                    string_field text
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

            #[derive(serde::Deserialize)]
            struct Foo {
                foo: String,
            }

            /*
             The table data below comes from the code in wrappers/dockerfiles/airtable/server.py
            */
            let results = c
                .select(
                    "SELECT bool_field FROM airtable_table WHERE bool_field = False",
                    None,
                    None,
                )
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<bool, _>("bool_field")
                        .expect("bool_field is missing")
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![false]);

            let results = c
                .select("SELECT string_field FROM airtable_table", None, None)
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("string_field")
                        .expect("string_field is missing")
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["two", "three"]);

            let results = c
                .select("SELECT numeric_field FROM airtable_table", None, None)
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::AnyNumeric, _>("numeric_field")
                        .expect("numeric_field is missing")
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![pgrx::AnyNumeric::from(1), pgrx::AnyNumeric::from(2)]
            );

            let results = c
                .select("SELECT timestamp_field FROM airtable_table", None, None)
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::Timestamp, _>("timestamp_field")
                        .expect("timestamp_field is missing")
                        .map(|v| v.to_iso_string())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["2023-07-19T06:39:15", "2023-07-20T06:39:15"]);

            let results = c
                .select("SELECT object_field FROM airtable_table", None, None)
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::JsonB, _>("object_field")
                        .expect("object_field is missing")
                        .and_then(|v| serde_json::from_value::<Foo>(v.0.to_owned()).ok())
                        .map(|v| v.foo)
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["bar", "baz"]);

            let results = c
                .select("SELECT strings_array_field FROM airtable_table", None, None)
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::JsonB, _>("strings_array_field")
                        .expect("strings_array_field is missing")
                        .and_then(|v| serde_json::from_value::<Vec<String>>(v.0.to_owned()).ok())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![vec!["foo", "bar"], vec!["baz", "qux"]]);

            let results = c
                .select(
                    "SELECT numerics_array_field FROM airtable_table",
                    None,
                    None,
                )
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::JsonB, _>("numerics_array_field")
                        .expect("numerics_array_field is missing")
                        .and_then(|v| serde_json::from_value::<Vec<u32>>(v.0.to_owned()).ok())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![vec![1, 2], vec![3, 4]]);

            let results = c
                .select("SELECT bools_array_field FROM airtable_table", None, None)
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::JsonB, _>("bools_array_field")
                        .expect("bools_array_field is missing")
                        .and_then(|v| serde_json::from_value::<Vec<bool>>(v.0.to_owned()).ok())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![vec![false], vec![true, false, true]]);

            let results = c
                .select("SELECT objects_array_field FROM airtable_table", None, None)
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::JsonB, _>("objects_array_field")
                        .expect("objects_array_field is missing")
                        .and_then(|v| serde_json::from_value::<Vec<Foo>>(v.0.to_owned()).ok())
                        .map(|v| v.into_iter().map(|f| f.foo).collect::<Vec<_>>())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![vec!["bar", "baz"], vec!["qux"]]);

            let results = c
                .select("SELECT string_field FROM airtable_view", None, None)
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("string_field")
                        .expect("string_field is missing")
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["three"]);
        });
    }
}
