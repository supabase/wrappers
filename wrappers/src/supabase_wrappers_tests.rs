//! Runtime tests for `supabase-wrappers` core types that need a live Postgres backend
//! (e.g. `Cell::into_datum()`/`from_datum()` round trips). These can't run as plain
//! `cargo test` in the `supabase-wrappers` crate itself since it isn't a pgrx extension,
//! so they run here instead, against the real Postgres backend `cargo pgrx test` spins up.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
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
}
