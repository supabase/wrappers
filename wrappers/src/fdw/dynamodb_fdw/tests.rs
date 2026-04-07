#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;

    #[pg_test]
    fn dynamodb_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER dynamodb_wrapper
                   HANDLER dynamo_db_fdw_handler VALIDATOR dynamo_db_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"CREATE SERVER dynamodb_server
                   FOREIGN DATA WRAPPER dynamodb_wrapper
                   OPTIONS (
                     aws_access_key_id 'test',
                     aws_secret_access_key 'test',
                     region 'us-east-1',
                     endpoint_url 'http://localhost:8008'
                   )"#,
                None,
                &[],
            )
            .unwrap();

            // Create foreign tables pointing to pre-seeded DynamoDB Local tables
            c.update(
                r#"CREATE FOREIGN TABLE dynamodb_users (
                     id text,
                     name text,
                     age bigint,
                     active boolean,
                     attributes jsonb
                   )
                   SERVER dynamodb_server
                   OPTIONS (table 'wrappers_test_users', rowid_column 'id')"#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"CREATE FOREIGN TABLE dynamodb_write_test (
                     id text,
                     value text
                   )
                   SERVER dynamodb_server
                   OPTIONS (table 'wrappers_write_test', rowid_column 'id')"#,
                None,
                &[],
            )
            .unwrap();

            // Test full scan
            let results = c
                .select("SELECT * FROM dynamodb_users ORDER BY id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["user1", "user2", "user3"]);

            // Test partition key pushdown (uses Query API)
            let results = c
                .select(
                    "SELECT id, name FROM dynamodb_users WHERE id = 'user1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Alice"]);

            // Test typed column (bigint)
            let ages = c
                .select(
                    "SELECT age FROM dynamodb_users WHERE id = 'user2'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("age").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(ages, vec![30]);

            // Test INSERT
            c.update(
                r#"INSERT INTO dynamodb_write_test (id, value) VALUES ('write1', 'hello')"#,
                None,
                &[],
            )
            .unwrap();

            let inserted = c
                .select(
                    "SELECT value FROM dynamodb_write_test WHERE id = 'write1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("value").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(inserted, vec!["hello"]);

            // Test UPDATE
            c.update(
                r#"UPDATE dynamodb_write_test SET value = 'updated' WHERE id = 'write1'"#,
                None,
                &[],
            )
            .unwrap();

            let updated = c
                .select(
                    "SELECT value FROM dynamodb_write_test WHERE id = 'write1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("value").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(updated, vec!["updated"]);

            // Test DELETE
            c.update(
                "DELETE FROM dynamodb_write_test WHERE id = 'write1'",
                None,
                &[],
            )
            .unwrap();

            let deleted = c
                .select(
                    "SELECT value FROM dynamodb_write_test WHERE id = 'write1'",
                    None,
                    &[],
                )
                .unwrap()
                .collect::<Vec<_>>();
            assert!(deleted.is_empty());

            // Test IMPORT FOREIGN SCHEMA (basic — schema tests are in dynamodb_import_schema_test)
            c.update("CREATE SCHEMA IF NOT EXISTS dynamodb_import", None, &[])
                .unwrap();
            c.update(
                "IMPORT FOREIGN SCHEMA dynamodb FROM SERVER dynamodb_server INTO dynamodb_import",
                None,
                &[],
            )
            .unwrap();
        });
    }

    /// Covers every branch of attr_to_cell (read path) and cell_to_attr (write path) in conv.rs,
    /// as well as the attr_to_json helper used for compound/set types.
    #[pg_test]
    fn dynamodb_type_mapping_test() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER dynamodb_wrapper
                   HANDLER dynamo_db_fdw_handler VALIDATOR dynamo_db_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER dynamodb_server
                   FOREIGN DATA WRAPPER dynamodb_wrapper
                   OPTIONS (
                     aws_access_key_id 'test',
                     aws_secret_access_key 'test',
                     region 'us-east-1',
                     endpoint_url 'http://localhost:8008'
                   )"#,
                None,
                &[],
            )
            .unwrap();

            // ----------------------------------------------------------------
            // Read-path: attr_to_cell for every DynamoDB attribute type
            // ----------------------------------------------------------------
            c.update(
                r#"CREATE FOREIGN TABLE ddb_types (
                     id          text,
                     col_bool    boolean,
                     col_bool_txt text,
                     col_null    text,
                     col_date    date,
                     col_ts      timestamp,
                     col_tstz    timestamptz,
                     col_num_txt text,
                     col_si      smallint,
                     col_int     integer,
                     col_bigint  bigint,
                     col_real    real,
                     col_double  double precision,
                     col_numeric numeric,
                     col_bytes   bytea,
                     col_list    jsonb,
                     col_sset    jsonb,
                     col_nset    jsonb,
                     col_map     jsonb,
                     col_json_s  jsonb
                   )
                   SERVER dynamodb_server
                   OPTIONS (table 'wrappers_types_test', rowid_column 'id')"#,
                None,
                &[],
            )
            .unwrap();

            // Bool → boolean
            let col_bool = c
                .select(
                    "SELECT col_bool FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<bool, _>("col_bool").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_bool, vec![true]);

            // Bool → text  (covers Bool→TEXTOID branch)
            let col_bool_txt = c
                .select(
                    "SELECT col_bool_txt FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("col_bool_txt").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_bool_txt, vec!["false"]);

            // Null(true) → SQL NULL  (covers early-return Null branch)
            let col_null = c
                .select(
                    "SELECT col_null FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("col_null").unwrap())
                .collect::<Vec<_>>();
            assert!(col_null.is_empty());

            // N → text fallback  (covers N→_ default branch)
            let col_num_txt = c
                .select(
                    "SELECT col_num_txt FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("col_num_txt").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_num_txt, vec!["42"]);

            // N → smallint  (covers N→INT2OID)
            let col_si = c
                .select("SELECT col_si FROM ddb_types WHERE id = 'item1'", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i16, _>("col_si").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_si, vec![42i16]);

            // N → integer  (covers N→INT4OID)
            let col_int = c
                .select(
                    "SELECT col_int FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i32, _>("col_int").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_int, vec![1000i32]);

            // N → bigint  (covers N→INT8OID)
            let col_bigint = c
                .select(
                    "SELECT col_bigint FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("col_bigint").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_bigint, vec![9999999i64]);

            // N → real  (covers N→FLOAT4OID)
            let col_real = c
                .select(
                    "SELECT col_real FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<f32, _>("col_real").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_real, vec![2.5f32]);

            // N → double precision  (covers N→FLOAT8OID)
            let col_double = c
                .select(
                    "SELECT col_double FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<f64, _>("col_double").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_double, vec![2.5f64]);

            // N → numeric  (covers N→NUMERICOID; compare as text to avoid float imprecision)
            let col_numeric = c
                .select(
                    "SELECT col_numeric::text FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("col_numeric").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_numeric, vec!["42"]);

            // S → date  (covers S→DATEOID)
            let col_date = c
                .select(
                    "SELECT col_date::text FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("col_date").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_date, vec!["2024-01-15"]);

            // S → timestamp  (covers S→TIMESTAMPOID)
            let col_ts = c
                .select(
                    "SELECT col_ts::text FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("col_ts").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_ts, vec!["2024-01-15 10:30:00"]);

            // S → timestamptz  (covers S→TIMESTAMPTZOID; just verify non-null)
            let col_tstz_ok = c
                .select(
                    "SELECT col_tstz IS NOT NULL AS ok FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<bool, _>("ok").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_tstz_ok, vec![true]);

            // S → jsonb  (covers S→JSONBOID; string parsed as JSON)
            let col_json_s = c
                .select(
                    "SELECT col_json_s->>'key' AS k FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("k").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_json_s, vec!["value"]);

            // B → bytea  (covers B→BYTEAOID; "aGVsbG8=" = base64("hello"))
            let col_bytes = c
                .select(
                    "SELECT convert_from(col_bytes, 'UTF8') AS s FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("s").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_bytes, vec!["hello"]);

            // L → jsonb, covers attr_to_json for S, N (int), N (float "3.5"), Bool, Null
            // col_list = ["a", 1, 3.5, true, null]
            let col_list_len = c
                .select(
                    "SELECT jsonb_array_length(col_list) AS len FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i32, _>("len").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_list_len, vec![5i32]);

            // The float element ("3.5") and null element are in the list
            let col_list_float = c
                .select(
                    "SELECT (col_list->>2)::float8 AS f FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<f64, _>("f").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_list_float, vec![3.5f64]);

            // col_list->4 is JSON null; ->> coerces it to SQL NULL
            let col_list_null_ok = c
                .select(
                    "SELECT (col_list->>4) IS NULL AS ok FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<bool, _>("ok").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_list_null_ok, vec![true]);

            // SS → jsonb  (covers Ss→jsonb via attr_to_cell compound branch)
            let col_sset_len = c
                .select(
                    "SELECT jsonb_array_length(col_sset) AS len FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i32, _>("len").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_sset_len, vec![2i32]);

            // NS → jsonb  (covers Ns→jsonb; NS=["10","3.5"] exercises int and float paths in attr_to_json)
            let col_nset_len = c
                .select(
                    "SELECT jsonb_array_length(col_nset) AS len FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i32, _>("len").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_nset_len, vec![2i32]);

            // NS is unordered — use containment to verify the float element is present
            let col_nset_has_float = c
                .select(
                    "SELECT (col_nset @> '[3.5]'::jsonb) AS ok FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<bool, _>("ok").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_nset_has_float, vec![true]);

            // M → jsonb  (covers M→jsonb; map has N and B entries — B exercises attr_to_json B hex path)
            let col_map_x = c
                .select(
                    "SELECT (col_map->>'x')::integer AS x FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i32, _>("x").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_map_x, vec![1i32]);

            // The "data" key in the map is a B attribute; attr_to_json hex-encodes it
            let col_map_data = c
                .select(
                    "SELECT col_map->>'data' AS d FROM ddb_types WHERE id = 'item1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("d").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(col_map_data, vec!["68656c6c6f"]); // hex("hello")

            // ----------------------------------------------------------------
            // Write-path: cell_to_attr roundtrip for every Cell variant
            // ----------------------------------------------------------------
            c.update(
                r#"CREATE FOREIGN TABLE ddb_write_typed (
                     id       text,
                     w_bool   boolean,
                     w_si     smallint,
                     w_int    integer,
                     w_bigint bigint,
                     w_real   real,
                     w_double double precision,
                     w_num    numeric,
                     w_date   date,
                     w_ts     timestamp,
                     w_tstz   timestamptz,
                     w_json   jsonb,
                     w_bytes  bytea
                   )
                   SERVER dynamodb_server
                   OPTIONS (table 'wrappers_types_write_test', rowid_column 'id')"#,
                None,
                &[],
            )
            .unwrap();

            // Insert a row exercising every typed Cell variant
            c.update(
                "INSERT INTO ddb_write_typed \
                   (id, w_bool, w_si, w_int, w_bigint, w_real, w_double, w_num, \
                    w_date, w_ts, w_tstz, w_json, w_bytes) \
                 VALUES \
                   ('wtype1', true, 7::smallint, 500, 1234567, 2.5::real, 2.5, 42::numeric, \
                    '2025-06-01'::date, '2025-06-01 12:00:00'::timestamp, \
                    '2025-06-01 12:00:00+00'::timestamptz, \
                    '{\"a\":1}'::jsonb, '\\x68656c6c6f'::bytea)",
                None,
                &[],
            )
            .unwrap();

            // bool roundtrip  (covers cell_to_attr Bool)
            let w_bool = c
                .select(
                    "SELECT w_bool FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<bool, _>("w_bool").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_bool, vec![true]);

            // smallint roundtrip  (covers cell_to_attr I16)
            let w_si = c
                .select(
                    "SELECT w_si FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i16, _>("w_si").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_si, vec![7i16]);

            // integer roundtrip  (covers cell_to_attr I32)
            let w_int = c
                .select(
                    "SELECT w_int FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i32, _>("w_int").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_int, vec![500i32]);

            // bigint roundtrip  (covers cell_to_attr I64)
            let w_bigint = c
                .select(
                    "SELECT w_bigint FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("w_bigint").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_bigint, vec![1234567i64]);

            // real roundtrip  (covers cell_to_attr F32)
            let w_real = c
                .select(
                    "SELECT w_real FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<f32, _>("w_real").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_real, vec![2.5f32]);

            // double precision roundtrip  (covers cell_to_attr F64)
            let w_double = c
                .select(
                    "SELECT w_double FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<f64, _>("w_double").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_double, vec![2.5f64]);

            // numeric roundtrip  (covers cell_to_attr Numeric)
            let w_num = c
                .select(
                    "SELECT w_num::text FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("w_num").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_num, vec!["42"]);

            // date roundtrip  (covers cell_to_attr Date)
            let w_date = c
                .select(
                    "SELECT w_date::text FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("w_date").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_date, vec!["2025-06-01"]);

            // timestamp roundtrip  (covers cell_to_attr Timestamp)
            let w_ts = c
                .select(
                    "SELECT w_ts::text FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("w_ts").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_ts, vec!["2025-06-01 12:00:00"]);

            // timestamptz roundtrip  (covers cell_to_attr Timestamptz; exact text is tz-dependent)
            let w_tstz_ok = c
                .select(
                    "SELECT w_tstz IS NOT NULL AS ok FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<bool, _>("ok").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_tstz_ok, vec![true]);

            // jsonb roundtrip  (covers cell_to_attr Json; stored as DynamoDB S, read back parsed)
            let w_json = c
                .select(
                    "SELECT w_json->>'a' AS a FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("a").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_json, vec!["1"]);

            // bytea roundtrip  (covers cell_to_attr Bytea; stored as DynamoDB B)
            let w_bytes = c
                .select(
                    "SELECT convert_from(w_bytes, 'UTF8') AS s FROM ddb_write_typed WHERE id = 'wtype1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("s").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(w_bytes, vec!["hello"]);

            // NULL roundtrip  (covers cell_to_attr None → Null(true); read back as SQL NULL)
            c.update(
                "INSERT INTO ddb_write_typed (id, w_bool) VALUES ('wtype_null', NULL)",
                None,
                &[],
            )
            .unwrap();
            let w_null = c
                .select(
                    "SELECT w_bool FROM ddb_write_typed WHERE id = 'wtype_null'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<bool, _>("w_bool").unwrap())
                .collect::<Vec<_>>();
            assert!(w_null.is_empty());

            // Cleanup DynamoDB rows (external service, not rolled back with the transaction)
            c.update("DELETE FROM ddb_write_typed WHERE id = 'wtype1'", None, &[])
                .unwrap();
            c.update(
                "DELETE FROM ddb_write_typed WHERE id = 'wtype_null'",
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test]
    fn dynamodb_import_schema_test() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER dynamodb_wrapper
                   HANDLER dynamo_db_fdw_handler VALIDATOR dynamo_db_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER dynamodb_server
                   FOREIGN DATA WRAPPER dynamodb_wrapper
                   OPTIONS (
                     aws_access_key_id 'test',
                     aws_secret_access_key 'test',
                     region 'us-east-1',
                     endpoint_url 'http://localhost:8008'
                   )"#,
                None,
                &[],
            )
            .unwrap();

            // ----------------------------------------------------------------
            // Test 1: full IMPORT creates a table for every DynamoDB table
            // ----------------------------------------------------------------
            c.update("CREATE SCHEMA IF NOT EXISTS ddb_full", None, &[])
                .unwrap();
            c.update(
                "IMPORT FOREIGN SCHEMA dynamodb FROM SERVER dynamodb_server INTO ddb_full",
                None,
                &[],
            )
            .unwrap();

            let tables = c
                .select(
                    "SELECT table_name::text FROM information_schema.tables \
                     WHERE table_schema = 'ddb_full' ORDER BY table_name",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("table_name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                tables,
                vec![
                    "wrappers_test_users",
                    "wrappers_types_test",
                    "wrappers_types_write_test",
                    "wrappers_write_test",
                ]
            );

            // ----------------------------------------------------------------
            // Test 2: generated DDL has exactly (partition_key text, _attrs jsonb)
            // ----------------------------------------------------------------
            let cols = c
                .select(
                    "SELECT column_name::text, data_type::text \
                     FROM information_schema.columns \
                     WHERE table_schema = 'ddb_full' \
                       AND table_name = 'wrappers_test_users' \
                     ORDER BY ordinal_position",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    (
                        r.get_by_name::<String, _>("column_name")
                            .unwrap()
                            .unwrap_or_default(),
                        r.get_by_name::<String, _>("data_type")
                            .unwrap()
                            .unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                cols,
                vec![
                    ("id".to_string(), "text".to_string()),
                    ("_attrs".to_string(), "jsonb".to_string()),
                ]
            );

            // ----------------------------------------------------------------
            // Test 3: catch-all _attrs column contains all non-key fields
            //
            // user1 was seeded with: name, age, active, and a nested Map
            // called "tags". All of these should appear under the catch-all
            // `_attrs` jsonb column because none of them are declared as their
            // own column in the imported table.
            // ----------------------------------------------------------------

            // Scalar string field
            let name = c
                .select(
                    "SELECT _attrs->>'name' AS name \
                     FROM ddb_full.wrappers_test_users WHERE id = 'user1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(name, vec!["Alice"]);

            // Scalar number field (DynamoDB N → JSON number)
            let age = c
                .select(
                    "SELECT (_attrs->>'age')::bigint AS age \
                     FROM ddb_full.wrappers_test_users WHERE id = 'user2'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("age").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(age, vec![30]);

            // Boolean field (DynamoDB BOOL → JSON bool)
            let active = c
                .select(
                    "SELECT (_attrs->>'active')::boolean AS active \
                     FROM ddb_full.wrappers_test_users WHERE id = 'user2'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<bool, _>("active").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(active, vec![false]);

            // Nested Map field — user1 has a DynamoDB Map attribute called
            // "tags". It should appear nested inside the catch-all.
            let role = c
                .select(
                    "SELECT _attrs->'tags'->>'role' AS role \
                     FROM ddb_full.wrappers_test_users WHERE id = 'user1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("role").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(role, vec!["admin"]);

            // user2 has no nested map, so that path is NULL
            let no_nested = c
                .select(
                    "SELECT _attrs->'tags' AS nested \
                     FROM ddb_full.wrappers_test_users WHERE id = 'user2'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::JsonB, _>("nested").unwrap())
                .collect::<Vec<_>>();
            assert!(no_nested.is_empty());

            // Full scan through imported table returns all three seeded rows
            let ids = c
                .select(
                    "SELECT id FROM ddb_full.wrappers_test_users ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(ids, vec!["user1", "user2", "user3"]);

            // ----------------------------------------------------------------
            // Test 4: LIMIT TO imports only the named tables
            // ----------------------------------------------------------------
            c.update("CREATE SCHEMA IF NOT EXISTS ddb_limited", None, &[])
                .unwrap();
            c.update(
                "IMPORT FOREIGN SCHEMA dynamodb \
                 LIMIT TO (wrappers_test_users) \
                 FROM SERVER dynamodb_server INTO ddb_limited",
                None,
                &[],
            )
            .unwrap();

            let limited = c
                .select(
                    "SELECT table_name::text FROM information_schema.tables \
                     WHERE table_schema = 'ddb_limited' ORDER BY table_name",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("table_name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(limited, vec!["wrappers_test_users"]);

            // ----------------------------------------------------------------
            // Test 5: EXCEPT excludes the named tables
            // ----------------------------------------------------------------
            c.update("CREATE SCHEMA IF NOT EXISTS ddb_except", None, &[])
                .unwrap();
            c.update(
                "IMPORT FOREIGN SCHEMA dynamodb \
                 EXCEPT (wrappers_write_test) \
                 FROM SERVER dynamodb_server INTO ddb_except",
                None,
                &[],
            )
            .unwrap();

            let excepted = c
                .select(
                    "SELECT table_name::text FROM information_schema.tables \
                     WHERE table_schema = 'ddb_except' ORDER BY table_name",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<String, _>("table_name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                excepted,
                vec![
                    "wrappers_test_users",
                    "wrappers_types_test",
                    "wrappers_types_write_test",
                ]
            );
        });
    }
}
