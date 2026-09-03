//! Runtime tests for `supabase-wrappers` core types and framework behavior that need a
//! live Postgres backend (e.g. `Cell::into_datum()`/`from_datum()` round trips, or the
//! scan callback lifecycle under a cached plan). These can't run as plain `cargo test`
//! in the `supabase-wrappers` crate itself since it isn't a pgrx extension, so they run
//! here instead, against the real Postgres backend `cargo pgrx test` spins up.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_sys::panic::ErrorReport;
    use pgrx::prelude::*;
    use std::collections::HashMap;
    use supabase_wrappers::prelude::*;
    use supabase_wrappers::qual::form_array_from_datum;

    fn assert_cell_clone(cell: Cell) {
        let cell_clone = cell.clone();

        match (cell, cell_clone) {
            (Cell::Bool(left), Cell::Bool(right)) => assert_eq!(left, right),
            (Cell::I8(left), Cell::I8(right)) => assert_eq!(left, right),
            (Cell::I16(left), Cell::I16(right)) => assert_eq!(left, right),
            (Cell::F32(left), Cell::F32(right)) => assert_eq!(left, right),
            (Cell::I32(left), Cell::I32(right)) => assert_eq!(left, right),
            (Cell::F64(left), Cell::F64(right)) => assert_eq!(left, right),
            (Cell::I64(left), Cell::I64(right)) => assert_eq!(left, right),
            (Cell::String(left), Cell::String(right)) => assert_eq!(left, right),
            (Cell::BoolArray(left), Cell::BoolArray(right)) => assert_eq!(left, right),
            (Cell::I16Array(left), Cell::I16Array(right)) => assert_eq!(left, right),
            (Cell::I32Array(left), Cell::I32Array(right)) => assert_eq!(left, right),
            (Cell::I64Array(left), Cell::I64Array(right)) => assert_eq!(left, right),
            (Cell::F32Array(left), Cell::F32Array(right)) => assert_eq!(left, right),
            (Cell::F64Array(left), Cell::F64Array(right)) => assert_eq!(left, right),
            (Cell::StringArray(left), Cell::StringArray(right)) => assert_eq!(left, right),
            (left, right) => panic!("cell clone variant mismatch: left={left:?}, right={right:?}",),
        }
    }

    // ==========================================================================
    // Tests for Cell
    // ==========================================================================
    #[pg_test]
    fn test_cell_clone() {
        let cell = Cell::String("hello".to_string());
        assert_cell_clone(cell);
    }

    #[pg_test]
    fn test_cell_clone_primitives() {
        let cases = vec![
            Cell::Bool(true),
            Cell::I8(-8),
            Cell::I16(-16),
            Cell::F32(123.456f32),
            Cell::I32(32),
            Cell::F64(654.321f64),
            Cell::I64(64),
            Cell::String("supabase".to_string()),
        ];

        for cell in cases {
            assert_cell_clone(cell);
        }
    }

    #[pg_test]
    fn test_cell_clone_array_variants() {
        let cases = vec![
            Cell::BoolArray(vec![Some(true), None, Some(false)]),
            Cell::I16Array(vec![Some(-1), None, Some(2)]),
            Cell::I32Array(vec![Some(-10), None, Some(20)]),
            Cell::I64Array(vec![Some(-100), None, Some(200)]),
            Cell::F32Array(vec![Some(1.5), None, Some(2.5)]),
            Cell::F64Array(vec![Some(10.5), None, Some(20.5)]),
            Cell::StringArray(vec![Some("a".to_string()), None, Some("b".to_string())]),
        ];

        for cell in cases {
            let cell_clone = cell.clone();
            assert_cell_clone(cell);
            assert!(cell_clone.is_array());
        }
    }

    #[pg_test]
    fn test_cell_clone_deep_copy_for_owned_types() {
        let mut string_cell = Cell::String("hello".to_string());
        let string_cell_clone = string_cell.clone();
        if let Cell::String(value) = &mut string_cell {
            value.push_str(" world");
        }
        match string_cell_clone {
            Cell::String(value) => assert_eq!(value, "hello"),
            other => panic!("expected Cell::String clone, got {other:?}"),
        }
        match string_cell {
            Cell::String(value) => assert_eq!(value, "hello world"),
            other => panic!("expected mutated Cell::String, got {other:?}"),
        }

        let mut string_array_cell =
            Cell::StringArray(vec![Some("foo".to_string()), None, Some("bar".to_string())]);
        let string_array_cell_clone = string_array_cell.clone();
        if let Cell::StringArray(values) = &mut string_array_cell {
            values[0] = Some("baz".to_string());
        }
        match string_array_cell_clone {
            Cell::StringArray(values) => {
                assert_eq!(
                    values,
                    vec![Some("foo".to_string()), None, Some("bar".to_string())]
                )
            }
            other => panic!("expected Cell::StringArray clone, got {other:?}"),
        }
        match string_array_cell {
            Cell::StringArray(values) => {
                assert_eq!(
                    values,
                    vec![Some("baz".to_string()), None, Some("bar".to_string())]
                )
            }
            other => panic!("expected mutated Cell::StringArray, got {other:?}"),
        }
    }

    #[pg_test]
    fn test_cell_display_primitives_and_string() {
        assert_eq!(format!("{}", Cell::Bool(true)), "true");
        assert_eq!(format!("{}", Cell::I8(-8)), "-8");
        assert_eq!(format!("{}", Cell::I16(16)), "16");
        assert_eq!(format!("{}", Cell::I32(32)), "32");
        assert_eq!(format!("{}", Cell::I64(64)), "64");
        assert_eq!(format!("{}", Cell::F32(3.5)), "3.5");
        assert_eq!(format!("{}", Cell::F64(7.25)), "7.25");
        assert_eq!(format!("{}", Cell::String("hello".to_string())), "'hello'");
    }

    #[pg_test]
    fn test_cell_display_arrays_with_nulls() {
        assert_eq!(
            format!("{}", Cell::BoolArray(vec![Some(true), None, Some(false)])),
            "[true,null,false]"
        );
        assert_eq!(
            format!("{}", Cell::I32Array(vec![Some(1), None, Some(3)])),
            "[1,null,3]"
        );
        assert_eq!(
            format!(
                "{}",
                Cell::StringArray(vec![Some("foo".to_string()), None, Some("bar".to_string())])
            ),
            "[foo,null,bar]"
        );
    }

    #[pg_test]
    fn test_cell_display_empty_arrays() {
        assert_eq!(format!("{}", Cell::BoolArray(vec![])), "[]");
        assert_eq!(format!("{}", Cell::I16Array(vec![])), "[]");
        assert_eq!(format!("{}", Cell::I32Array(vec![])), "[]");
        assert_eq!(format!("{}", Cell::I64Array(vec![])), "[]");
        assert_eq!(format!("{}", Cell::F32Array(vec![])), "[]");
        assert_eq!(format!("{}", Cell::F64Array(vec![])), "[]");
        assert_eq!(format!("{}", Cell::StringArray(vec![])), "[]");
    }

    #[pg_test]
    fn test_cell_into_datum_scalars_round_trip() {
        let bool_datum = Cell::Bool(true).into_datum().expect("bool should convert");
        let bool_value =
            unsafe { bool::from_datum(bool_datum, false) }.expect("bool should decode");
        assert!(bool_value);

        let i32_datum = Cell::I32(42).into_datum().expect("i32 should convert");
        let i32_value = unsafe { i32::from_datum(i32_datum, false) }.expect("i32 should decode");
        assert_eq!(i32_value, 42);

        let f64_datum = Cell::F64(12.5).into_datum().expect("f64 should convert");
        let f64_value = unsafe { f64::from_datum(f64_datum, false) }.expect("f64 should decode");
        assert_eq!(f64_value, 12.5);

        let string_datum = Cell::String("hello".to_string())
            .into_datum()
            .expect("string should convert");
        let string_value =
            unsafe { String::from_datum(string_datum, false) }.expect("string should decode");
        assert_eq!(string_value, "hello");
    }

    #[pg_test]
    fn test_cell_into_datum_arrays_round_trip() {
        let bool_array_datum = Cell::BoolArray(vec![Some(true), None, Some(false)])
            .into_datum()
            .expect("bool array should convert");
        let bool_array_value = unsafe { Vec::<Option<bool>>::from_datum(bool_array_datum, false) }
            .expect("bool array should decode");
        assert_eq!(bool_array_value, vec![Some(true), None, Some(false)]);

        let i64_array_datum = Cell::I64Array(vec![Some(1), None, Some(3)])
            .into_datum()
            .expect("i64 array should convert");
        let i64_array_value = unsafe { Vec::<Option<i64>>::from_datum(i64_array_datum, false) }
            .expect("i64 array should decode");
        assert_eq!(i64_array_value, vec![Some(1), None, Some(3)]);

        let string_array_datum =
            Cell::StringArray(vec![Some("foo".to_string()), None, Some("bar".to_string())])
                .into_datum()
                .expect("string array should convert");
        let string_array_value =
            unsafe { Vec::<Option<String>>::from_datum(string_array_datum, false) }
                .expect("string array should decode");
        assert_eq!(
            string_array_value,
            vec![Some("foo".to_string()), None, Some("bar".to_string())]
        );
    }

    // ==========================================================================
    // Tests for form_array_from_datum
    // ==========================================================================
    #[pg_test]
    fn test_form_array_from_datum_int4_array() {
        let values = vec![1_i32, 2_i32, 3_i32];
        let datum = values
            .into_datum()
            .expect("int4 array datum should be created");

        let result = unsafe { form_array_from_datum(datum, false, pg_sys::INT4ARRAYOID) };
        let result = result.expect("int4 array should be parsed");

        assert_eq!(result.len(), 3);
        assert!(matches!(result[0], Cell::I32(1)));
        assert!(matches!(result[1], Cell::I32(2)));
        assert!(matches!(result[2], Cell::I32(3)));
    }

    #[pg_test]
    fn test_form_array_from_datum_null_datum_returns_none() {
        let result = unsafe { form_array_from_datum(0.into(), true, pg_sys::INT4ARRAYOID) };
        assert!(result.is_none());
    }

    #[pg_test]
    fn test_form_array_from_datum_unsupported_oid_returns_none() {
        let values = vec![1_i32, 2_i32];
        let datum = values
            .into_datum()
            .expect("int4 array datum should be created");

        let result = unsafe { form_array_from_datum(datum, false, pg_sys::UUIDARRAYOID) };
        assert!(result.is_none());
    }

    // ==========================================================================
    // Regression test: cached-plan re-execution must not crash the backend
    // ==========================================================================

    // `get_rel_size` is a planning-time-only trait hook and takes no `self` (planning
    // never constructs an FDW instance), so it can't use instance-local state to detect
    // repeat calls. Use a static counter instead to assert it's never re-run once the
    // plan is cached.
    static PLANNING_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    // Likewise, `new()` should now only run once per `EXECUTE` of the cached plan (never
    // during planning), so this counter should end up equal to the number of executions.
    static NEW_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    #[wrappers_fdw(
        version = "0.1.0",
        author = "Supabase",
        website = "https://github.com/supabase/wrappers",
        error_type = "CacheTestFdwError"
    )]
    struct CacheTestFdw {
        iter_done: bool,
        tgt_cols: Vec<Column>,
    }

    enum CacheTestFdwError {}

    impl From<CacheTestFdwError> for ErrorReport {
        fn from(_value: CacheTestFdwError) -> Self {
            ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, "", "")
        }
    }

    impl ForeignDataWrapper<CacheTestFdwError> for CacheTestFdw {
        fn new(_server: ForeignServer) -> Result<Self, CacheTestFdwError> {
            NEW_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Self {
                iter_done: false,
                tgt_cols: Vec::new(),
            })
        }

        // This method is called during the planning phase, we use it to
        // assert that plan is being cached by checking that this is only
        // ever called once.
        fn get_rel_size(
            _quals: &[Qual],
            _columns: &[Column],
            _sorts: &[Sort],
            _limit: &Option<Limit>,
            _options: &HashMap<String, String>,
        ) -> Result<(i64, i32), CacheTestFdwError> {
            PLANNING_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok((0, 0))
        }

        fn begin_scan(
            &mut self,
            _quals: &[Qual],
            columns: &[Column],
            _sorts: &[Sort],
            _limit: &Option<Limit>,
            _options: &HashMap<String, String>,
        ) -> Result<(), CacheTestFdwError> {
            self.iter_done = false;
            self.tgt_cols = columns.to_vec();
            Ok(())
        }

        fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, CacheTestFdwError> {
            if self.iter_done {
                return Ok(None);
            }
            self.iter_done = true;
            for col in &self.tgt_cols {
                if col.name == "id" {
                    row.push("id", Some(Cell::I64(1)));
                }
            }
            Ok(Some(()))
        }

        fn end_scan(&mut self) -> Result<(), CacheTestFdwError> {
            Ok(())
        }
    }

    #[pg_test]
    fn cached_plan_repeated_execution_does_not_crash() {
        Spi::connect_mut(|c| {
            c.update(
                r#"create foreign data wrapper cache_test_wrapper
                   handler cache_test_fdw_handler validator cache_test_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"create server cache_test_server foreign data wrapper cache_test_wrapper"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"create foreign table cache_test_table (id bigint) server cache_test_server"#,
                None,
                &[],
            )
            .unwrap();

            // Use a prepared statement to force plan caching
            c.update(
                "prepare cache_test_q as select count(*) from cache_test_table",
                None,
                &[],
            )
            .unwrap();

            // Run the cached plan multiple times
            for _ in 0..3 {
                let count = c
                    .select("execute cache_test_q", None, &[])
                    .unwrap()
                    .first()
                    .get_one::<i64>()
                    .unwrap();
                assert_eq!(count, Some(1));
            }

            c.update("deallocate cache_test_q", None, &[]).unwrap();

            assert_eq!(
                PLANNING_CALLS.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "expected get_rel_size to run exactly once for the whole cached plan"
            );
            assert_eq!(
                NEW_CALLS.load(std::sync::atomic::Ordering::SeqCst),
                3,
                "expected new() to run exactly once per execution, never during planning"
            );
        });
    }
}
