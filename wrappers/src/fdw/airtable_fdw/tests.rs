#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn airtable_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER airtable_wrapper
                         HANDLER airtable_fdw_handler VALIDATOR airtable_fdw_validator"#,
                None,
                &[],
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
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE airtable_table (
                    "BOOL_FIELD" bool,
                    numeric_field numeric,
                    string_field text,
                    timestamp_field timestamp,
                    object_field jsonb,
                    strings_array_field jsonb,
                    numerics_array_field jsonb,
                    bools_array_field jsonb,
                    objects_array_field jsonb,
                    char_field "char",
                    int2_field smallint,
                    int4_field integer,
                    int8_field bigint,
                    float4_field real,
                    float8_field double precision,
                    date_field date,
                    timestamptz_field timestamptz
                  )
                  SERVER airtable_server
                  OPTIONS (
                    base_id 'baseID',
                    table_id 'table-foo'
                  )
             "#,
                None,
                &[],
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
                &[],
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
                    r#"SELECT "BOOL_FIELD" FROM airtable_table WHERE "BOOL_FIELD" = False"#,
                    None,
                    &[],
                )
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<bool, _>("BOOL_FIELD")
                        .expect("BOOL_FIELD is missing")
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![false]);

            let results = c
                .select("SELECT string_field FROM airtable_table", None, &[])
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("string_field")
                        .expect("string_field is missing")
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["two", "three"]);

            let results = c
                .select("SELECT numeric_field FROM airtable_table", None, &[])
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
                .select("SELECT timestamp_field FROM airtable_table", None, &[])
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::prelude::Timestamp, _>("timestamp_field")
                        .expect("timestamp_field is missing")
                        .map(|v| v.to_iso_string())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["2023-07-19T06:39:15", "2023-07-20T06:39:15"]);

            let results = c
                .select("SELECT object_field FROM airtable_table", None, &[])
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
                .select("SELECT strings_array_field FROM airtable_table", None, &[])
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::JsonB, _>("strings_array_field")
                        .expect("strings_array_field is missing")
                        .and_then(|v| serde_json::from_value::<Vec<String>>(v.0.to_owned()).ok())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![vec!["foo", "bar"], vec!["baz", "qux"]]);

            let results = c
                .select("SELECT numerics_array_field FROM airtable_table", None, &[])
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::JsonB, _>("numerics_array_field")
                        .expect("numerics_array_field is missing")
                        .and_then(|v| serde_json::from_value::<Vec<u32>>(v.0.to_owned()).ok())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![vec![1, 2], vec![3, 4]]);

            let results = c
                .select("SELECT bools_array_field FROM airtable_table", None, &[])
                .expect("No results for a given query")
                .filter_map(|r| {
                    r.get_by_name::<pgrx::JsonB, _>("bools_array_field")
                        .expect("bools_array_field is missing")
                        .and_then(|v| serde_json::from_value::<Vec<bool>>(v.0.to_owned()).ok())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![vec![false], vec![true, false, true]]);

            let results = c
                .select("SELECT objects_array_field FROM airtable_table", None, &[])
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
                .select("SELECT * FROM airtable_table", None, &[])
                .expect("No results for a given query")
                .filter_map(|r| {
                    // Ensure additional column types can be deserialized from SELECT *
                    let string = r
                        .get_by_name::<&str, _>("string_field")
                        .expect("string_field is missing")?;
                    let _int4 = r
                        .get_by_name::<i32, _>("int4_field")
                        .expect("int4_field is missing")?;
                    let _float4 = r
                        .get_by_name::<f32, _>("float4_field")
                        .expect("float4_field is missing")?;
                    let _date = r
                        .get_by_name::<pgrx::datum::Date, _>("date_field")
                        .expect("date_field is missing")?;
                    let _timestamptz = r
                        .get_by_name::<pgrx::datum::TimestampWithTimeZone, _>("timestamptz_field")
                        .expect("timestamptz_field is missing")?;
                    Some(string)
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["two", "three"]);

            let results = c
                .select("SELECT string_field FROM airtable_view", None, &[])
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
