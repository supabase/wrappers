#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::JsonB;
    use pgrx::pg_test;
    use pgrx::prelude::*;

    fn create_minio_e2e_server() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_e2e_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_e2e_server
                     FOREIGN DATA WRAPPER zarr_e2e_wrapper
                     OPTIONS (
                       store_url 's3://warehouse/zarr/e2e.zarr',
                       aws_access_key_id 'admin',
                       aws_secret_access_key 'password',
                       aws_region 'us-east-1',
                       endpoint_url 'http://localhost:8000',
                       path_style_url 'true'
                     )"#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    fn create_minio_e2e_table(table: &str, array_group: &str, value_type: &str) {
        create_minio_e2e_table_with_cf(table, array_group, value_type, false);
    }

    fn create_minio_e2e_table_with_cf(
        table: &str,
        array_group: &str,
        value_type: &str,
        decode_cf: bool,
    ) {
        let decode_option = if decode_cf {
            ",\n                         decode_cf 'true'"
        } else {
            ""
        };
        create_minio_e2e_table_with_options(
            table,
            array_group,
            value_type,
            &format!(
                ",\n                         time_unit 'seconds',\n                         time_origin 'unix'{decode_option}"
            ),
        );
    }

    fn create_minio_e2e_table_with_options(
        table: &str,
        array_group: &str,
        value_type: &str,
        options: &str,
    ) {
        create_minio_e2e_server();
        Spi::connect_mut(|c| {
            c.update(
                &format!(
                    r#"CREATE FOREIGN TABLE {table} (
                         x double precision,
                         y double precision,
                         time timestamp with time zone,
                         value {value_type}
                       )
                       SERVER zarr_e2e_server
                       OPTIONS (
                         array_group '{array_group}'{options}
                       )"#
                ),
                None,
                &[],
            )
            .unwrap();
        });
    }

    fn create_minio_generic4d_table(table: &str, band_type: &str) {
        create_minio_e2e_server();
        Spi::connect_mut(|c| {
            c.update(
                &format!(
                    r#"CREATE FOREIGN TABLE {table} (
                         forecast_time timestamp with time zone,
                         level double precision,
                         band {band_type},
                         channel double precision,
                         measurement real
                       )
                       SERVER zarr_e2e_server
                       OPTIONS (
                         array_group 'nested/generic4d',
                         time_from_attrs 'true'
                       )"#
                ),
                None,
                &[],
            )
            .unwrap();
        });
    }

    fn explain_lines(sql: &str) -> Vec<String> {
        Spi::connect(|c| {
            c.select(&format!("EXPLAIN {sql}"), None, &[])
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect()
        })
    }

    fn assert_aggregate_pushed_down(sql: &str) {
        let plan = explain_lines(sql);
        assert!(
            plan.iter().any(|line| line.contains("Foreign Scan")),
            "expected a Foreign Scan in plan: {plan:?}"
        );
        assert!(
            !plan
                .iter()
                .any(|line| line.contains("Aggregate") && line.contains("(cost=")),
            "expected no local Aggregate plan node: {plan:?}"
        );
        assert!(
            plan.iter()
                .any(|line| line.contains("Wrappers") && line.contains("aggregates =")),
            "expected aggregate details on the Foreign Scan: {plan:?}"
        );
    }

    fn assert_aggregate_falls_back(sql: &str) {
        let plan = explain_lines(sql);
        assert!(
            plan.iter()
                .any(|line| line.contains("Aggregate") && line.contains("(cost=")),
            "expected a local Aggregate plan node: {plan:?}"
        );
    }

    // DDL-only smoke test. The MinIO-backed cases below cover actual scans
    // against the fixture seeded by .ci/docker-compose-native.yaml.
    #[pg_test]
    fn zarr_ddl_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_test_server
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (
                       store_url 's3://zarr-test/sentinel2/2025.zarr',
                       aws_access_key_id 'test-key',
                       aws_secret_access_key 'test-secret',
                       aws_region 'us-east-1'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                CREATE FOREIGN TABLE zarr_test_cells (
                    x      double precision,
                    y      double precision,
                    time   timestamptz,
                    b04    real
                )
                SERVER zarr_test_server
                OPTIONS (
                    array_group 'reflectance',
                    time_unit  'seconds',
                    time_origin 'unix'
                )
             "#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    // Building a server without a store_url must be rejected by the validator.
    #[pg_test(error = "required option `store_url` is not specified")]
    fn zarr_validator_requires_store_url() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_bad_server
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (
                       endpoint_url 'http://localhost:9000'
                     )"#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    // Invalid time_unit values must be rejected at CREATE FOREIGN TABLE time.
    #[pg_test(
        error = "invalid value for option 'fortnights': must be one of: seconds, milliseconds, microseconds, nanoseconds, minutes, hours, days"
    )]
    fn zarr_validator_rejects_bad_time_unit() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_test_server
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (store_url 's3://zarr-test/x.zarr')"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                CREATE FOREIGN TABLE zarr_bad_time (
                    x double precision,
                    y double precision,
                    b04 real
                )
                SERVER zarr_test_server
                OPTIONS (time_unit 'fortnights')
             "#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test(error = "invalid value for option 'decode_cf': must be 'true' or 'false'")]
    fn zarr_validator_rejects_bad_cf_decode_boolean() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_cf_server
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (store_url 's3://zarr-test/x.zarr')"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FOREIGN TABLE zarr_bad_cf_option (
                       x double precision,
                       y double precision,
                       value double precision
                     )
                     SERVER zarr_cf_server
                     OPTIONS (array_group 'value', decode_cf 'yes')"#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test(error = "invalid value for option 'time_from_attrs': must be 'true' or 'false'")]
    fn zarr_validator_rejects_bad_time_from_attrs_boolean() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_time_attrs_server
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (store_url 's3://zarr-test/x.zarr')"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FOREIGN TABLE zarr_bad_time_attrs_option (
                       x double precision,
                       y double precision,
                       time timestamptz,
                       value real
                     )
                     SERVER zarr_time_attrs_server
                     OPTIONS (array_group 'value', time_from_attrs 'yes')"#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test(
        error = "invalid value for option 'time_from_attrs': cannot be combined with 'time_unit' or 'time_origin'"
    )]
    fn zarr_validator_rejects_time_from_attrs_with_manual_time_options() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_time_attrs_conflict_server
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (store_url 's3://zarr-test/x.zarr')"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FOREIGN TABLE zarr_bad_time_attrs_conflict (
                       x double precision,
                       y double precision,
                       time timestamptz,
                       value real
                     )
                     SERVER zarr_time_attrs_conflict_server
                     OPTIONS (
                       array_group 'value',
                       time_from_attrs 'true',
                       time_unit 'seconds'
                     )"#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test(error = "invalid value for option 'path_style_url': must be 'true' or 'false'")]
    fn zarr_validator_rejects_bad_path_style_boolean() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_bad_path_style
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (
                       store_url 's3://zarr-test/x.zarr',
                       path_style_url 'yes'
                     )"#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test(error = "required option `aws_secret_access_key` is not specified")]
    fn zarr_validator_rejects_partial_direct_credentials() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_partial_credentials
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (
                       store_url 's3://zarr-test/x.zarr',
                       aws_access_key_id 'test-key'
                     )"#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test(
        error = "invalid authentication options: anonymous authentication cannot be combined with explicit credentials"
    )]
    fn zarr_validator_rejects_conflicting_authentication() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_conflicting_auth
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (
                       store_url 's3://zarr-test/x.zarr',
                       anonymous 'true',
                       aws_access_key_id 'test-key',
                       aws_secret_access_key 'test-secret'
                     )"#,
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test]
    fn zarr_minio_raw_scan_e2e() {
        create_minio_e2e_table("zarr_e2e_raw", "nested/raw", "real");

        Spi::connect_mut(|c| {
            let summary = c
                .select(
                    r#"SELECT count(value) AS row_count,
                              sum(value)::double precision AS value_sum,
                              min(value)::double precision AS value_min,
                              max(value)::double precision AS value_max
                       FROM zarr_e2e_raw"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                summary.get_by_name::<i64, _>("row_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_sum").unwrap().unwrap(),
                3574.0
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_min").unwrap().unwrap(),
                -7.5
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_max").unwrap().unwrap(),
                143.0
            );

            let fill_count = c
                .select(
                    "SELECT count(*) AS fill_count FROM zarr_e2e_raw WHERE value = -7.5",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<i64, _>("fill_count")
                .unwrap()
                .unwrap();
            assert_eq!(fill_count, 8);

            // Only `value` is projected and only `x` is restricted: `y` must
            // remain internal scan state rather than a required SQL column.
            let x_only = c
                .select(
                    r#"SELECT count(value) AS row_count,
                              sum(value)::double precision AS value_sum
                       FROM zarr_e2e_raw
                       WHERE x = 120"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                x_only.get_by_name::<i64, _>("row_count").unwrap().unwrap(),
                10
            );
            assert_eq!(
                x_only.get_by_name::<f64, _>("value_sum").unwrap().unwrap(),
                720.0
            );

            let boundary = c
                .select(
                    r#"SELECT value
                       FROM zarr_e2e_raw
                       WHERE time = '1970-01-01 01:00:00+00'::timestamptz
                         AND y = 50
                         AND x = 150"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get_by_name::<f32, _>("value").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(boundary, vec![-7.5]);
        });
    }

    #[pg_test]
    fn zarr_minio_lazy_chunk_cursor_starts_large_selection() {
        create_minio_e2e_server();
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN TABLE zarr_e2e_lazy1m (value real)
                   SERVER zarr_e2e_server
                   OPTIONS (array_group 'nested/lazy1m')"#,
                None,
                &[],
            )
            .unwrap();

            let values = c
                .select("SELECT value FROM zarr_e2e_lazy1m LIMIT 1", None, &[])
                .unwrap()
                .filter_map(|row| row.get_by_name::<f32, _>("value").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(values, vec![42.0]);
        });
    }

    #[pg_test]
    fn zarr_minio_execution_metrics_are_explained() {
        create_minio_e2e_table("zarr_e2e_explain_runtime", "nested/raw", "real");
        Spi::connect(|c| {
            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT count(*) AS cells, sum(value) AS total
                         FROM zarr_e2e_explain_runtime"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Chunks Total: 4"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Selected: 4"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Requested: 4"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Present: 3"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Missing: 1"), "plan: {plan:?}");
            assert!(has("Zarr Data GET Calls: 4"), "plan: {plan:?}");
            assert!(has("Zarr Cache Misses: 4"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 288 bytes"), "plan: {plan:?}");
            assert!(has("Zarr Data Decoded Bytes: 384 bytes"), "plan: {plan:?}");
            assert!(
                has("Zarr Fill Bytes Synthesized: 96 bytes"),
                "plan: {plan:?}"
            );
            assert!(has("Zarr Logical Cells Examined: 60"), "plan: {plan:?}");
            assert!(has("Zarr Logical Cells Matched: 60"), "plan: {plan:?}");
            assert!(has("Zarr Tuples Emitted: 1"), "plan: {plan:?}");
            assert!(has("Zarr Max Concurrent Reads: 4"), "plan: {plan:?}");
            assert!(has("Zarr Chunk-Stat Pruning: disabled"), "plan: {plan:?}");
        });
    }

    #[pg_test]
    fn zarr_minio_rescan_reuses_compressed_chunk_cache() {
        create_minio_e2e_table("zarr_e2e_cache_rescan", "nested/raw", "real");
        Spi::connect(|c| {
            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT ordinal,
                              (SELECT count(*)
                                 FROM zarr_e2e_cache_rescan
                                WHERE x > threshold) AS selected
                         FROM (VALUES (1, 120.0::double precision),
                                      (2, NULL::double precision),
                                      (3, 140.0::double precision)) AS limits(ordinal, threshold)
                        ORDER BY ordinal"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Data GET Calls: 4"), "plan: {plan:?}");
            assert!(has("Zarr Cache Misses: 4"), "plan: {plan:?}");
            assert!(has("Zarr Cache Hits: 6"), "plan: {plan:?}");
        });
    }

    #[pg_test]
    fn zarr_minio_scalar_aggregate_pushdown_e2e() {
        create_minio_e2e_table("zarr_e2e_aggregate", "nested/raw", "real");

        let whole_array_sql = r#"SELECT count(*) AS total_count,
                                        count(value) AS value_count,
                                        sum(value) AS value_sum,
                                        avg(value) AS value_avg,
                                        min(value) AS value_min,
                                        max(value) AS value_max
                                   FROM zarr_e2e_aggregate"#;
        assert_aggregate_pushed_down(whole_array_sql);
        assert_aggregate_pushed_down("SELECT count(*) FROM zarr_e2e_aggregate WHERE 120 < x");
        for operator in ["<", "<=", "=", ">", ">="] {
            assert_aggregate_pushed_down(&format!(
                "SELECT count(*) FROM zarr_e2e_aggregate WHERE x {operator} 'NaN'::double precision"
            ));
        }
        assert_aggregate_pushed_down(
            "SELECT count(*) FROM zarr_e2e_aggregate WHERE x IN ('NaN'::double precision, 110)",
        );

        Spi::connect_mut(|c| {
            let whole = c
                .select(whole_array_sql, None, &[])
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                whole.get_by_name::<i64, _>("total_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                whole.get_by_name::<i64, _>("value_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                whole.get_by_name::<f32, _>("value_sum").unwrap().unwrap(),
                3574.0
            );
            assert!(
                (whole.get_by_name::<f64, _>("value_avg").unwrap().unwrap()
                    - 59.566_666_666_666_67)
                    .abs()
                    < 1e-12
            );
            assert_eq!(
                whole.get_by_name::<f32, _>("value_min").unwrap().unwrap(),
                -7.5
            );
            assert_eq!(
                whole.get_by_name::<f32, _>("value_max").unwrap().unwrap(),
                143.0
            );

            let strict = c
                .select(
                    r#"SELECT count(*) AS total_count,
                              count(value) AS value_count,
                              sum(value) AS value_sum,
                              avg(value) AS value_avg,
                              min(value) AS value_min,
                              max(value) AS value_max
                         FROM zarr_e2e_aggregate
                        WHERE x > 120"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                strict
                    .get_by_name::<i64, _>("total_count")
                    .unwrap()
                    .unwrap(),
                30
            );
            assert_eq!(
                strict
                    .get_by_name::<i64, _>("value_count")
                    .unwrap()
                    .unwrap(),
                30
            );
            assert_eq!(
                strict.get_by_name::<f32, _>("value_sum").unwrap().unwrap(),
                1444.0
            );
            assert!(
                (strict.get_by_name::<f64, _>("value_avg").unwrap().unwrap()
                    - 48.133_333_333_333_33)
                    .abs()
                    < 1e-12
            );
            assert_eq!(
                strict.get_by_name::<f32, _>("value_min").unwrap().unwrap(),
                -7.5
            );
            assert_eq!(
                strict.get_by_name::<f32, _>("value_max").unwrap().unwrap(),
                143.0
            );

            let reversed_operand = c
                .select(
                    "SELECT count(*) FROM zarr_e2e_aggregate WHERE 120 < x",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<i64>(1)
                .unwrap()
                .unwrap();
            assert_eq!(reversed_operand, 30);

            let coordinate_aggregates = c
                .select(
                    r#"SELECT count(x) AS x_count,
                              sum(x) AS x_sum,
                              avg(x) AS x_avg,
                              min(x) AS x_min,
                              max(x) AS x_max
                         FROM zarr_e2e_aggregate"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                coordinate_aggregates
                    .get_by_name::<i64, _>("x_count")
                    .unwrap()
                    .unwrap(),
                60
            );
            assert_eq!(
                coordinate_aggregates
                    .get_by_name::<f64, _>("x_sum")
                    .unwrap()
                    .unwrap(),
                7500.0
            );
            assert_eq!(
                coordinate_aggregates
                    .get_by_name::<f64, _>("x_avg")
                    .unwrap()
                    .unwrap(),
                125.0
            );
            assert_eq!(
                coordinate_aggregates
                    .get_by_name::<f64, _>("x_min")
                    .unwrap()
                    .unwrap(),
                100.0
            );
            assert_eq!(
                coordinate_aggregates
                    .get_by_name::<f64, _>("x_max")
                    .unwrap()
                    .unwrap(),
                150.0
            );

            let sparse = c
                .select(
                    r#"SELECT count(*) AS total_count,
                              count(value) AS value_count,
                              sum(value) AS value_sum,
                              avg(value) AS value_avg,
                              min(value) AS value_min,
                              max(value) AS value_max
                         FROM zarr_e2e_aggregate
                        WHERE y >= 40 AND x >= 140"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                sparse
                    .get_by_name::<i64, _>("total_count")
                    .unwrap()
                    .unwrap(),
                8
            );
            assert_eq!(
                sparse
                    .get_by_name::<i64, _>("value_count")
                    .unwrap()
                    .unwrap(),
                8
            );
            assert_eq!(
                sparse.get_by_name::<f32, _>("value_sum").unwrap().unwrap(),
                -60.0
            );
            assert_eq!(
                sparse.get_by_name::<f64, _>("value_avg").unwrap().unwrap(),
                -7.5
            );
            assert_eq!(
                sparse.get_by_name::<f32, _>("value_min").unwrap().unwrap(),
                -7.5
            );
            assert_eq!(
                sparse.get_by_name::<f32, _>("value_max").unwrap().unwrap(),
                -7.5
            );

            let membership = c
                .select(
                    r#"SELECT count(*) AS total_count,
                              sum(value) AS value_sum,
                              min(value) AS value_min,
                              max(value) AS value_max
                         FROM zarr_e2e_aggregate
                        WHERE x IN (110, 150)"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                membership
                    .get_by_name::<i64, _>("total_count")
                    .unwrap()
                    .unwrap(),
                20
            );
            assert_eq!(
                membership
                    .get_by_name::<f32, _>("value_sum")
                    .unwrap()
                    .unwrap(),
                1070.0
            );
            assert_eq!(
                membership
                    .get_by_name::<f32, _>("value_min")
                    .unwrap()
                    .unwrap(),
                -7.5
            );
            assert_eq!(
                membership
                    .get_by_name::<f32, _>("value_max")
                    .unwrap()
                    .unwrap(),
                141.0
            );

            let value_qual = c
                .select(
                    r#"SELECT count(*) AS total_count, sum(value) AS value_sum
                         FROM zarr_e2e_aggregate
                        WHERE value = -7.5"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                value_qual
                    .get_by_name::<i64, _>("total_count")
                    .unwrap()
                    .unwrap(),
                8
            );
            assert_eq!(
                value_qual
                    .get_by_name::<f32, _>("value_sum")
                    .unwrap()
                    .unwrap(),
                -60.0
            );

            let empty = c
                .select(
                    r#"SELECT count(*) AS total_count,
                              count(value) AS value_count,
                              sum(value) AS value_sum,
                              avg(value) AS value_avg,
                              min(value) AS value_min,
                              max(value) AS value_max
                         FROM zarr_e2e_aggregate
                        WHERE x > 999"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                empty.get_by_name::<i64, _>("total_count").unwrap().unwrap(),
                0
            );
            assert_eq!(
                empty.get_by_name::<i64, _>("value_count").unwrap().unwrap(),
                0
            );
            assert_eq!(empty.get_by_name::<f32, _>("value_sum").unwrap(), None);
            assert_eq!(empty.get_by_name::<f64, _>("value_avg").unwrap(), None);
            assert_eq!(empty.get_by_name::<f32, _>("value_min").unwrap(), None);
            assert_eq!(empty.get_by_name::<f32, _>("value_max").unwrap(), None);

            for (operator, expected) in [("<", 60), ("<=", 60), ("=", 0), (">", 0), (">=", 0)] {
                let count = c
                    .select(
                        &format!(
                            "SELECT count(*) FROM zarr_e2e_aggregate WHERE x {operator} 'NaN'::double precision"
                        ),
                        None,
                        &[],
                    )
                    .unwrap()
                    .next()
                    .unwrap()
                    .get::<i64>(1)
                    .unwrap()
                    .unwrap();
                assert_eq!(count, expected, "unexpected result for x {operator} NaN");
            }
            assert_eq!(
                c.select(
                    "SELECT count(*) FROM zarr_e2e_aggregate WHERE x IN ('NaN'::double precision, 110)",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<i64>(1)
                .unwrap(),
                Some(10)
            );

            let rescans = c
                .select(
                    r#"SELECT ordinal, threshold,
                              (SELECT count(*)
                                 FROM zarr_e2e_aggregate
                                WHERE x > threshold) AS selected
                         FROM (VALUES (1, 120.0::double precision),
                                      (2, NULL::double precision),
                                      (3, 140.0::double precision)) AS limits(ordinal, threshold)
                        ORDER BY ordinal"#,
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| {
                    (
                        row.get_by_name::<f64, _>("threshold").unwrap(),
                        row.get_by_name::<i64, _>("selected").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                rescans,
                vec![(Some(120.0), 30), (None, 0), (Some(140.0), 10)]
            );

            c.update("SET LOCAL plan_cache_mode = force_generic_plan", None, &[])
                .unwrap();
            c.update(
                "PREPARE zarr_aggregate_threshold(double precision) AS SELECT count(*) FROM zarr_e2e_aggregate WHERE x > $1",
                None,
                &[],
            )
            .unwrap();
            for (argument, expected) in [("120", 30), ("NULL", 0), ("140", 10)] {
                let count = c
                    .select(
                        &format!("EXECUTE zarr_aggregate_threshold({argument})"),
                        None,
                        &[],
                    )
                    .unwrap()
                    .next()
                    .unwrap()
                    .get::<i64>(1)
                    .unwrap()
                    .unwrap();
                assert_eq!(count, expected, "unexpected prepared result for {argument}");
            }
            c.update("DEALLOCATE zarr_aggregate_threshold", None, &[])
                .unwrap();
        });
    }

    #[pg_test]
    fn zarr_scalar_aggregate_unsupported_shapes_fall_back() {
        create_minio_e2e_table("zarr_e2e_aggregate_fallback", "nested/raw", "real");
        Spi::connect_mut(|c| {
            c.update("CREATE SCHEMA zarr_aggregate_custom", None, &[])
                .unwrap();
            c.update(
                r#"CREATE FUNCTION zarr_aggregate_custom.add_hundred(real, real)
                     RETURNS real
                     LANGUAGE sql IMMUTABLE STRICT
                     AS 'SELECT $1 + 100::real'"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE AGGREGATE zarr_aggregate_custom.sum(real) (
                     SFUNC = zarr_aggregate_custom.add_hundred,
                     STYPE = real,
                     INITCOND = '0'
                   )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FUNCTION zarr_aggregate_custom.always_true(double precision, double precision)
                     RETURNS boolean
                     LANGUAGE plpgsql IMMUTABLE
                     AS $$ BEGIN RETURN true; END $$"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE OPERATOR zarr_aggregate_custom.= (
                     LEFTARG = double precision,
                     RIGHTARG = double precision,
                     FUNCTION = zarr_aggregate_custom.always_true
                   )"#,
                None,
                &[],
            )
            .unwrap();
        });

        let cases = [
            "SELECT count(DISTINCT value) FROM zarr_e2e_aggregate_fallback",
            "SELECT sum(value + 1) FROM zarr_e2e_aggregate_fallback",
            "SELECT count(*) FILTER (WHERE value = -7.5) FROM zarr_e2e_aggregate_fallback",
            "SELECT sum(value ORDER BY x) FROM zarr_e2e_aggregate_fallback",
            "SELECT count(*) FROM zarr_e2e_aggregate_fallback WHERE value + 1 > 0",
            "SELECT time, count(*) FROM zarr_e2e_aggregate_fallback GROUP BY time",
            "SELECT count(*), 1 FROM zarr_e2e_aggregate_fallback",
            "SELECT count(*) FROM zarr_e2e_aggregate_fallback HAVING count(*) > 0",
            "SELECT zarr_aggregate_custom.sum(value) FROM zarr_e2e_aggregate_fallback",
            "SELECT count(*) FROM zarr_e2e_aggregate_fallback WHERE x OPERATOR(zarr_aggregate_custom.=) 999::double precision",
            "SELECT count(*) FROM zarr_e2e_aggregate_fallback GROUP BY GROUPING SETS ((), ())",
        ];
        for sql in cases {
            assert_aggregate_falls_back(sql);
        }

        Spi::connect(|c| {
            assert_eq!(
                c.select(
                    "SELECT count(DISTINCT value) FROM zarr_e2e_aggregate_fallback",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<i64>(1)
                .unwrap(),
                Some(53)
            );
            assert_eq!(
                c.select(
                    "SELECT sum(value + 1) FROM zarr_e2e_aggregate_fallback",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<f64>(1)
                .unwrap(),
                Some(3634.0)
            );
            assert_eq!(
                c.select(
                    "SELECT count(*) FILTER (WHERE value = -7.5) FROM zarr_e2e_aggregate_fallback",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<i64>(1)
                .unwrap(),
                Some(8)
            );
            assert_eq!(
                c.select(
                    "SELECT count(*) FROM zarr_e2e_aggregate_fallback WHERE value + 1 > 0",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<i64>(1)
                .unwrap(),
                Some(52)
            );
            assert_eq!(
                c.select(
                    "SELECT zarr_aggregate_custom.sum(value) FROM zarr_e2e_aggregate_fallback",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<f32>(1)
                .unwrap(),
                Some(6000.0)
            );
            assert_eq!(
                c.select(
                    "SELECT count(*) FROM zarr_e2e_aggregate_fallback WHERE x OPERATOR(zarr_aggregate_custom.=) 999::double precision",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<i64>(1)
                .unwrap(),
                Some(60)
            );
            let grouping_sets = c
                .select(
                    "SELECT count(*) FROM zarr_e2e_aggregate_fallback GROUP BY GROUPING SETS ((), ())",
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| row.get::<i64>(1).unwrap().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(grouping_sets, vec![60, 60]);
        });
    }

    #[pg_test]
    fn zarr_minio_blosc_scan_e2e() {
        create_minio_e2e_table("zarr_e2e_blosc", "nested/blosc", "real");

        assert_aggregate_pushed_down(
            r#"SELECT count(*) AS total_count,
                      sum(value) AS value_sum,
                      avg(value) AS value_avg,
                      min(value) AS value_min,
                      max(value) AS value_max
                 FROM zarr_e2e_blosc
                WHERE time = '1970-01-01 01:00:00+00'::timestamptz
                  AND y >= 40
                  AND x BETWEEN 130 AND 150"#,
        );

        Spi::connect_mut(|c| {
            let summary = c
                .select(
                    r#"SELECT count(value) AS row_count,
                              sum(value)::double precision AS value_sum,
                              avg(value) AS value_avg,
                              min(value) AS value_min,
                              max(value) AS value_max
                       FROM zarr_e2e_blosc
                       WHERE time = '1970-01-01 01:00:00+00'::timestamptz
                         AND y >= 40
                         AND x BETWEEN 130 AND 150"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                summary.get_by_name::<i64, _>("row_count").unwrap().unwrap(),
                6
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_sum").unwrap().unwrap(),
                834.0
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_avg").unwrap().unwrap(),
                139.0
            );
            assert_eq!(
                summary.get_by_name::<f32, _>("value_min").unwrap().unwrap(),
                133.0
            );
            assert_eq!(
                summary.get_by_name::<f32, _>("value_max").unwrap().unwrap(),
                145.0
            );

            let boundary = c
                .select(
                    r#"SELECT value
                       FROM zarr_e2e_blosc
                       WHERE time = '1970-01-01 01:00:00+00'::timestamptz
                         AND y = 50
                         AND x = 150"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get_by_name::<f32, _>("value").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(boundary, vec![145.0]);
        });
    }

    #[pg_test(
        error = "column 'value' has incompatible PostgreSQL type OID 701; expected real (OID 700)"
    )]
    fn zarr_minio_rejects_wrong_value_type() {
        create_minio_e2e_table("zarr_e2e_bad_value_type", "nested/raw", "double precision");

        Spi::connect_mut(|c| {
            c.select("SELECT value FROM zarr_e2e_bad_value_type", None, &[])
                .unwrap();
        });
    }

    #[pg_test(
        error = "column 'value' has incompatible PostgreSQL type OID 700; expected double precision (OID 701)"
    )]
    fn zarr_minio_cf_decode_rejects_non_float8_value_type() {
        create_minio_e2e_table_with_cf("zarr_e2e_bad_cf_type", "nested/raw", "real", true);

        Spi::connect_mut(|c| {
            c.select("SELECT value FROM zarr_e2e_bad_cf_type", None, &[])
                .unwrap();
        });
    }

    #[pg_test]
    fn zarr_minio_cf_value_decode_e2e() {
        create_minio_e2e_table_with_cf(
            "zarr_e2e_cf_decoded",
            "nested/raw",
            "double precision",
            true,
        );

        assert_aggregate_pushed_down(
            r#"SELECT count(*) AS total_count,
                      count(value) AS valid_count,
                      sum(value) AS value_sum,
                      avg(value) AS value_avg,
                      min(value) AS value_min,
                      max(value) AS value_max
                 FROM zarr_e2e_cf_decoded"#,
        );

        Spi::connect(|c| {
            let summary = c
                .select(
                    r#"SELECT count(*) AS total_count,
                              count(value) AS valid_count,
                              sum(value) AS value_sum,
                              avg(value) AS value_avg,
                              min(value) AS value_min,
                              max(value) AS value_max
                         FROM zarr_e2e_cf_decoded"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                summary
                    .get_by_name::<i64, _>("total_count")
                    .unwrap()
                    .unwrap(),
                60
            );
            assert_eq!(
                summary
                    .get_by_name::<i64, _>("valid_count")
                    .unwrap()
                    .unwrap(),
                48
            );
            let value_sum = summary.get_by_name::<f64, _>("value_sum").unwrap().unwrap();
            let value_avg = summary.get_by_name::<f64, _>("value_avg").unwrap().unwrap();
            let value_min = summary.get_by_name::<f64, _>("value_min").unwrap().unwrap();
            let value_max = summary.get_by_name::<f64, _>("value_max").unwrap().unwrap();
            assert!((value_sum - 13_142.86).abs() < 1e-8);
            assert!((value_avg - 273.809_583_333_333_36).abs() < 1e-10);
            assert!((value_min - 273.15).abs() < 1e-10);
            assert!((value_max - 274.55).abs() < 1e-10);

            let sparse = c
                .select(
                    r#"SELECT count(*) AS total_count,
                              count(value) AS valid_count,
                              sum(value) AS value_sum,
                              avg(value) AS value_avg,
                              min(value) AS value_min,
                              max(value) AS value_max
                         FROM zarr_e2e_cf_decoded
                        WHERE y >= 40 AND x >= 140"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                sparse
                    .get_by_name::<i64, _>("total_count")
                    .unwrap()
                    .unwrap(),
                8
            );
            assert_eq!(
                sparse
                    .get_by_name::<i64, _>("valid_count")
                    .unwrap()
                    .unwrap(),
                0
            );
            assert_eq!(sparse.get_by_name::<f64, _>("value_sum").unwrap(), None);
            assert_eq!(sparse.get_by_name::<f64, _>("value_avg").unwrap(), None);
            assert_eq!(sparse.get_by_name::<f64, _>("value_min").unwrap(), None);
            assert_eq!(sparse.get_by_name::<f64, _>("value_max").unwrap(), None);

            let decoded_nulls = c
                .select(
                    "SELECT count(*) FROM zarr_e2e_cf_decoded WHERE value IS NULL",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<i64>(1)
                .unwrap()
                .unwrap();
            assert_eq!(decoded_nulls, 12);

            for predicate in [
                "time = '1970-01-01 00:00:00+00'::timestamptz AND y = 50 AND x = 120",
                "time = '1970-01-01 01:00:00+00'::timestamptz AND y = 50 AND x = 110",
                "time = '1970-01-01 01:00:00+00'::timestamptz AND y = 50 AND x = 150",
            ] {
                let sql = format!("SELECT value FROM zarr_e2e_cf_decoded WHERE {predicate}");
                let value = c
                    .select(&sql, None, &[])
                    .unwrap()
                    .next()
                    .unwrap()
                    .get_by_name::<f64, _>("value")
                    .unwrap();
                assert_eq!(value, None);
            }

            let valid_boundary = c
                .select(
                    r#"SELECT value
                         FROM zarr_e2e_cf_decoded
                        WHERE time = '1970-01-01 01:00:00+00'::timestamptz
                          AND y = 50
                          AND x = 100"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<f64, _>("value")
                .unwrap()
                .unwrap();
            assert!((valid_boundary - 274.55).abs() < 1e-10);
        });
    }

    #[pg_test]
    fn zarr_minio_time_from_attrs_e2e() {
        create_minio_e2e_table_with_options(
            "zarr_e2e_time_from_attrs",
            "nested/raw",
            "real",
            ",\n                         time_from_attrs 'true'",
        );

        Spi::connect(|c| {
            let times = c
                .select(
                    r#"SELECT string_agg(ts, ',' ORDER BY ts) AS times
                         FROM (
                           SELECT DISTINCT to_char(
                                    time AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD HH24:MI:SS.MS'
                                  ) AS ts
                             FROM zarr_e2e_time_from_attrs
                         ) t"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<String, _>("times")
                .unwrap()
                .unwrap();
            assert_eq!(times, "1970-01-01 00:00:00.000,1970-01-01 00:00:03.600");

            let selected = c
                .select(
                    r#"SELECT value
                         FROM zarr_e2e_time_from_attrs
                        WHERE time = '1970-01-01 00:00:03.6+00'::timestamptz
                          AND y = 50
                          AND x = 100"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get_by_name::<f32, _>("value").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(selected, vec![140.0]);
        });
    }

    #[pg_test]
    fn zarr_minio_generic_dimensions_scan_e2e() {
        create_minio_generic4d_table("zarr_e2e_generic4d", "double precision");

        let generic_aggregate_sql = r#"SELECT count(*) AS total_count,
                      sum(measurement) AS value_sum,
                      avg(measurement) AS value_avg,
                      min(measurement) AS value_min,
                      max(measurement) AS value_max
                 FROM zarr_e2e_generic4d
                WHERE forecast_time = '1970-01-01 00:00:03.6+00'::timestamptz
                  AND level >= 40
                  AND band > 120
                  AND channel = 7"#;
        assert_aggregate_pushed_down(generic_aggregate_sql);

        Spi::connect(|c| {
            // No dimension is projected or restricted. The executor must still
            // return the complete logical value array without requiring any
            // coordinate chunk values.
            let summary = c
                .select(
                    r#"SELECT count(measurement) AS row_count,
                              sum(measurement)::double precision AS value_sum,
                              min(measurement)::double precision AS value_min,
                              max(measurement)::double precision AS value_max
                         FROM zarr_e2e_generic4d"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                summary.get_by_name::<i64, _>("row_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_sum").unwrap().unwrap(),
                3574.0
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_min").unwrap().unwrap(),
                -7.5
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_max").unwrap().unwrap(),
                143.0
            );

            let fill_count = c
                .select(
                    "SELECT count(*) AS fill_count FROM zarr_e2e_generic4d WHERE measurement = -7.5",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<i64, _>("fill_count")
                .unwrap()
                .unwrap();
            assert_eq!(fill_count, 8);

            let boundary = c
                .select(
                    r#"SELECT to_char(
                                forecast_time AT TIME ZONE 'UTC',
                                'YYYY-MM-DD HH24:MI:SS.MS'
                              ) AS forecast_time,
                              level,
                              band,
                              channel,
                              measurement
                         FROM zarr_e2e_generic4d
                        WHERE forecast_time = '1970-01-01 00:00:03.6+00'::timestamptz
                          AND level = 50
                          AND band = 130
                          AND channel = 7"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                boundary
                    .get_by_name::<String, _>("forecast_time")
                    .unwrap()
                    .unwrap(),
                "1970-01-01 00:00:03.600"
            );
            assert_eq!(
                boundary.get_by_name::<f64, _>("level").unwrap().unwrap(),
                50.0
            );
            assert_eq!(
                boundary.get_by_name::<f64, _>("band").unwrap().unwrap(),
                130.0
            );
            assert_eq!(
                boundary.get_by_name::<f64, _>("channel").unwrap().unwrap(),
                7.0
            );
            assert_eq!(
                boundary
                    .get_by_name::<f32, _>("measurement")
                    .unwrap()
                    .unwrap(),
                143.0
            );

            let missing_boundary = c
                .select(
                    r#"SELECT measurement
                         FROM zarr_e2e_generic4d
                        WHERE forecast_time = '1970-01-01 00:00:03.6+00'::timestamptz
                          AND level = 50
                          AND band = 150
                          AND channel = 7"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<f32, _>("measurement")
                .unwrap()
                .unwrap();
            assert_eq!(missing_boundary, -7.5);

            let aggregate = c
                .select(generic_aggregate_sql, None, &[])
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                aggregate
                    .get_by_name::<i64, _>("total_count")
                    .unwrap()
                    .unwrap(),
                6
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f32, _>("value_sum")
                    .unwrap()
                    .unwrap(),
                246.0
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f64, _>("value_avg")
                    .unwrap()
                    .unwrap(),
                41.0
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f32, _>("value_min")
                    .unwrap()
                    .unwrap(),
                -7.5
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f32, _>("value_max")
                    .unwrap()
                    .unwrap(),
                143.0
            );
        });
    }

    #[pg_test(
        error = "column 'band' has incompatible PostgreSQL type OID 23; expected double precision (OID 701)"
    )]
    fn zarr_minio_generic_dimension_rejects_wrong_type() {
        create_minio_generic4d_table("zarr_e2e_bad_generic_dimension", "integer");

        Spi::connect(|c| {
            c.select(
                "SELECT measurement FROM zarr_e2e_bad_generic_dimension WHERE band = 130",
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test]
    fn zarr_inspect_minio_metadata_e2e() {
        create_minio_e2e_server();

        Spi::connect(|c| {
            let paths = c
                .select(
                    "SELECT path FROM zarr_inspect('zarr_e2e_server') ORDER BY path",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get_by_name::<String, _>("path").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                paths,
                vec![
                    "/",
                    "nested",
                    "nested/band",
                    "nested/blosc",
                    "nested/channel",
                    "nested/forecast_time",
                    "nested/generic4d",
                    "nested/lazy1m",
                    "nested/level",
                    "nested/raw",
                    "nested/sample",
                    "nested/spatial_ref",
                    "nested/time",
                    "nested/x",
                    "nested/y",
                ]
            );

            let root = c
                .select(
                    "SELECT kind, attributes FROM zarr_inspect('zarr_e2e_server') WHERE path = '/'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                root.get_by_name::<String, _>("kind").unwrap().unwrap(),
                "group"
            );
            assert_eq!(
                root.get_by_name::<JsonB, _>("attributes")
                    .unwrap()
                    .unwrap()
                    .0["title"],
                "Deterministic Zarr inspection fixture"
            );

            let group = c
                .select(
                    "SELECT crs FROM zarr_inspect('zarr_e2e_server') WHERE path = 'nested'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                group.get_by_name::<JsonB, _>("crs").unwrap().unwrap().0["properties"]["name"],
                "EPSG:3857"
            );

            let raw = c
                .select(
                    r#"SELECT kind, group_path, variable, shape, dimensions, dtype,
                              chunks, codecs, units, fill_value, scale_factor,
                              add_offset, crs, attributes, warnings
                         FROM zarr_inspect('zarr_e2e_server')
                        WHERE path = 'nested/raw'"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                raw.get_by_name::<String, _>("kind").unwrap().unwrap(),
                "array"
            );
            assert_eq!(
                raw.get_by_name::<String, _>("group_path").unwrap().unwrap(),
                "nested"
            );
            assert_eq!(
                raw.get_by_name::<String, _>("variable").unwrap().unwrap(),
                "raw"
            );
            assert_eq!(
                raw.get_by_name::<JsonB, _>("shape").unwrap().unwrap().0,
                serde_json::json!([2, 5, 6])
            );
            assert_eq!(
                raw.get_by_name::<Vec<String>, _>("dimensions")
                    .unwrap()
                    .unwrap(),
                vec!["time", "y", "x"]
            );
            assert_eq!(
                raw.get_by_name::<String, _>("dtype").unwrap().unwrap(),
                "<f4"
            );
            assert_eq!(
                raw.get_by_name::<JsonB, _>("chunks").unwrap().unwrap().0,
                serde_json::json!([2, 3, 4])
            );
            assert!(
                raw.get_by_name::<JsonB, _>("codecs").unwrap().unwrap().0["compressor"].is_null()
            );
            assert_eq!(raw.get_by_name::<String, _>("units").unwrap().unwrap(), "K");
            assert_eq!(
                raw.get_by_name::<JsonB, _>("fill_value")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!(-7.5)
            );
            assert_eq!(
                raw.get_by_name::<f64, _>("scale_factor").unwrap().unwrap(),
                0.01
            );
            assert_eq!(
                raw.get_by_name::<f64, _>("add_offset").unwrap().unwrap(),
                273.15
            );
            assert_eq!(
                raw.get_by_name::<JsonB, _>("crs").unwrap().unwrap().0,
                serde_json::json!("PROJCRS[\"WGS 84 / Pseudo-Mercator\"]")
            );
            assert_eq!(
                raw.get_by_name::<JsonB, _>("attributes")
                    .unwrap()
                    .unwrap()
                    .0["grid_mapping"],
                serde_json::json!("spatial_ref")
            );
            assert_eq!(
                raw.get_by_name::<Vec<String>, _>("warnings")
                    .unwrap()
                    .unwrap(),
                Vec::<String>::new()
            );

            let blosc = c
                .select(
                    "SELECT codecs FROM zarr_inspect('zarr_e2e_server') WHERE path = 'nested/blosc'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                blosc.get_by_name::<JsonB, _>("codecs").unwrap().unwrap().0["compressor"]["id"],
                "blosc"
            );

            let spatial_ref = c
                .select(
                    r#"SELECT kind, group_path, variable, dimensions, dtype,
                              crs, attributes, warnings
                         FROM zarr_inspect('zarr_e2e_server')
                        WHERE path = 'nested/spatial_ref'"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                spatial_ref
                    .get_by_name::<String, _>("kind")
                    .unwrap()
                    .unwrap(),
                "array"
            );
            assert_eq!(
                spatial_ref
                    .get_by_name::<String, _>("group_path")
                    .unwrap()
                    .unwrap(),
                "nested"
            );
            assert_eq!(
                spatial_ref
                    .get_by_name::<String, _>("variable")
                    .unwrap()
                    .unwrap(),
                "spatial_ref"
            );
            assert_eq!(
                spatial_ref
                    .get_by_name::<Vec<String>, _>("dimensions")
                    .unwrap()
                    .unwrap(),
                Vec::<String>::new()
            );
            assert_eq!(
                spatial_ref
                    .get_by_name::<String, _>("dtype")
                    .unwrap()
                    .unwrap(),
                "|i1"
            );
            let spatial_ref_crs = spatial_ref
                .get_by_name::<JsonB, _>("crs")
                .unwrap()
                .unwrap()
                .0;
            assert_eq!(
                spatial_ref_crs,
                serde_json::json!("PROJCRS[\"WGS 84 / Pseudo-Mercator\"]")
            );
            let spatial_ref_attrs = spatial_ref
                .get_by_name::<JsonB, _>("attributes")
                .unwrap()
                .unwrap()
                .0;
            assert_eq!(
                spatial_ref_attrs["grid_mapping_name"],
                serde_json::json!("mercator")
            );
            assert_eq!(
                spatial_ref_attrs["epsg_code"],
                serde_json::json!("EPSG:3857")
            );
            assert_eq!(
                spatial_ref_attrs["crs_wkt"],
                serde_json::json!("PROJCRS[\"WGS 84 / Pseudo-Mercator\"]")
            );
            assert_eq!(
                spatial_ref_attrs["GeoTransform"],
                serde_json::json!("100 10 0 50 0 -10")
            );
            assert_eq!(
                spatial_ref
                    .get_by_name::<Vec<String>, _>("warnings")
                    .unwrap()
                    .unwrap(),
                Vec::<String>::new()
            );

            let time = c
                .select(
                    r#"SELECT dimensions, units, calendar
                         FROM zarr_inspect('zarr_e2e_server')
                        WHERE path = 'nested/time'"#,
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                time.get_by_name::<Vec<String>, _>("dimensions")
                    .unwrap()
                    .unwrap(),
                vec!["time"]
            );
            assert_eq!(
                time.get_by_name::<String, _>("units").unwrap().unwrap(),
                "milliseconds since 1970-01-01 00:00:00"
            );
            assert_eq!(
                time.get_by_name::<String, _>("calendar").unwrap().unwrap(),
                "proleptic_gregorian"
            );

            let projected_axes = c
                .select(
                    r#"SELECT path, attributes
                         FROM zarr_inspect('zarr_e2e_server')
                        WHERE path IN ('nested/x', 'nested/y')
                        ORDER BY path"#,
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| {
                    (
                        row.get_by_name::<String, _>("path").unwrap().unwrap(),
                        row.get_by_name::<JsonB, _>("attributes")
                            .unwrap()
                            .unwrap()
                            .0,
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(projected_axes.len(), 2);
            assert_eq!(projected_axes[0].0, "nested/x");
            assert_eq!(projected_axes[0].1["axis"], serde_json::json!("X"));
            assert_eq!(
                projected_axes[0].1["standard_name"],
                serde_json::json!("projection_x_coordinate")
            );
            assert_eq!(projected_axes[0].1["units"], serde_json::json!("m"));
            assert_eq!(projected_axes[1].0, "nested/y");
            assert_eq!(projected_axes[1].1["axis"], serde_json::json!("Y"));
            assert_eq!(
                projected_axes[1].1["standard_name"],
                serde_json::json!("projection_y_coordinate")
            );
            assert_eq!(projected_axes[1].1["units"], serde_json::json!("m"));
        });
    }

    #[pg_test(error = "foreign server 'missing_zarr_server' does not exist or is not accessible")]
    fn zarr_inspect_rejects_missing_server() {
        Spi::run("SELECT * FROM zarr_inspect('missing_zarr_server')").unwrap();
    }

    #[pg_test(error = "foreign server 'zarr_private_server' does not exist or is not accessible")]
    fn zarr_inspect_requires_server_usage() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_private_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_private_server
                     FOREIGN DATA WRAPPER zarr_private_wrapper
                     OPTIONS (
                       store_url 's3://warehouse/zarr/e2e.zarr',
                       anonymous 'true'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update("CREATE ROLE zarr_inspect_no_usage", None, &[])
                .unwrap();
            c.update("SET ROLE zarr_inspect_no_usage", None, &[])
                .unwrap();
            c.select(
                "SELECT * FROM zarr_inspect('zarr_private_server')",
                None,
                &[],
            )
            .unwrap();
        });
    }

    #[pg_test]
    fn zarr_explain_uses_network_free_positive_estimate() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_plan_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_plan_server
                     FOREIGN DATA WRAPPER zarr_plan_wrapper
                     OPTIONS (
                       store_url 's3://zarr-test/does-not-exist.zarr',
                       anonymous 'true'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FOREIGN TABLE zarr_plan_table (
                       x double precision,
                       y double precision,
                       value real
                     )
                     SERVER zarr_plan_server
                     OPTIONS (array_group 'value')"#,
                None,
                &[],
            )
            .unwrap();

            let plan = c
                .select("EXPLAIN SELECT value FROM zarr_plan_table", None, &[])
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            assert!(
                plan.iter()
                    .any(|line| line.contains("rows=1000000 width=4")),
                "expected a positive network-free Zarr estimate, got {plan:?}"
            );
        });
    }
}
