#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;
    use pgrx::JsonB;

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

    fn create_minio_v3_e2e_server() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_v3_e2e_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_v3_e2e_server
                     FOREIGN DATA WRAPPER zarr_v3_e2e_wrapper
                     OPTIONS (
                       store_url 's3://warehouse/zarr/e2e-v3.zarr',
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

    fn create_minio_v3_e2e_table(table: &str, array_group: &str, decode_cf: bool) {
        create_minio_v3_e2e_server();
        create_minio_v3_e2e_table_on_server(table, array_group, decode_cf);
    }

    fn create_minio_v3_e2e_table_on_server(table: &str, array_group: &str, decode_cf: bool) {
        let decode_option = if decode_cf {
            ",\n                         decode_cf 'true'"
        } else {
            ""
        };
        Spi::connect_mut(|c| {
            c.update(
                &format!(
                    r#"CREATE FOREIGN TABLE {table} (
                         time timestamp with time zone,
                         y double precision,
                         x double precision,
                         value {value_type}
                       )
                       SERVER zarr_v3_e2e_server
                       OPTIONS (
                         array_group '{array_group}',
                         time_from_attrs 'true'{decode_option}
                       )"#,
                    value_type = if decode_cf {
                        "double precision"
                    } else {
                        "real"
                    },
                ),
                None,
                &[],
            )
            .unwrap();
        });
    }

    fn create_minio_ome_v3_e2e_server() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_ome_v3_e2e_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_ome_v3_e2e_server
                     FOREIGN DATA WRAPPER zarr_ome_v3_e2e_wrapper
                     OPTIONS (
                       store_url 's3://warehouse/zarr/e2e-ome-v3.zarr',
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

    fn create_minio_ome_v3_e2e_table(table: &str, level: usize) {
        create_minio_ome_v3_e2e_server();
        create_minio_ome_v3_e2e_table_on_server(table, level);
    }

    fn create_minio_ome_v3_e2e_table_on_server(table: &str, level: usize) {
        Spi::connect_mut(|c| {
            c.update(
                &format!(
                    r#"CREATE FOREIGN TABLE {table} (
                         y double precision,
                         x double precision,
                         value real
                       )
                       SERVER zarr_ome_v3_e2e_server
                       OPTIONS (
                         multiscale_group 'image',
                         multiscale_index '0',
                         multiscale_level '{level}'
                       )"#
                ),
                None,
                &[],
            )
            .unwrap();
        });
    }

    fn capture_query_error(statement: &str) -> String {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE OR REPLACE FUNCTION pg_temp.capture_zarr_error(statement text)
                     RETURNS text
                     LANGUAGE plpgsql
                     AS $function$
                     BEGIN
                       EXECUTE statement;
                       RETURN NULL;
                     EXCEPTION WHEN OTHERS THEN
                       RETURN SQLERRM;
                     END
                     $function$"#,
                None,
                &[],
            )
            .unwrap();
            c.select(
                "SELECT pg_temp.capture_zarr_error($1) AS message",
                None,
                &[statement.into()],
            )
            .unwrap()
            .next()
            .unwrap()
            .get_by_name::<String, _>("message")
            .unwrap()
            .expect("query must fail")
        })
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

    fn create_minio_spatial2d_table(table: &str) {
        create_minio_e2e_server();
        Spi::connect_mut(|c| {
            c.update(
                &format!(
                    r#"CREATE FOREIGN TABLE {table} (
                         y double precision,
                         x double precision,
                         value real
                       )
                       SERVER zarr_e2e_server
                       OPTIONS (array_group 'nested/spatial2d')"#
                ),
                None,
                &[],
            )
            .unwrap();
        });
    }

    fn create_minio_spatial2d_coordinate_only_table(table: &str) {
        create_minio_e2e_server();
        Spi::connect_mut(|c| {
            c.update(
                &format!(
                    r#"CREATE FOREIGN TABLE {table} (
                         y double precision,
                         x double precision
                       )
                       SERVER zarr_e2e_server
                       OPTIONS (array_group 'nested/spatial2d')"#
                ),
                None,
                &[],
            )
            .unwrap();
        });
    }

    fn create_minio_spatial_time_table(table: &str, decode_cf: bool) {
        let (value_type, decode_option) = if decode_cf {
            (
                "double precision",
                ",\n                         decode_cf 'true'",
            )
        } else {
            ("real", "")
        };
        create_minio_e2e_table_with_options(
            table,
            "nested/raw",
            value_type,
            &format!(",\n                         time_from_attrs 'true'{decode_option}"),
        );
    }

    fn install_postgis_in_test_schema() {
        Spi::run("CREATE SCHEMA zarr_gis; CREATE EXTENSION postgis WITH SCHEMA zarr_gis").unwrap();
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

    fn assert_v3_zstd_cf_aggregate(table: &str) {
        let sql = format!(
            r#"SELECT count(*) AS total_count,
                      count(value) AS value_count,
                      sum(value) AS value_sum,
                      avg(value) AS value_avg,
                      min(value) AS value_min,
                      max(value) AS value_max
                 FROM {table}"#
        );
        assert_aggregate_pushed_down(&sql);

        Spi::connect(|c| {
            let row = c.select(&sql, None, &[]).unwrap().next().unwrap();
            assert_eq!(
                row.get_by_name::<i64, _>("total_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                row.get_by_name::<i64, _>("value_count").unwrap().unwrap(),
                48
            );
            assert!(
                (row.get_by_name::<f64, _>("value_sum").unwrap().unwrap() - 13_142.86).abs() < 1e-8
            );
            assert!(
                (row.get_by_name::<f64, _>("value_avg").unwrap().unwrap() - 273.809_583_333_333_36)
                    .abs()
                    < 1e-10
            );
            assert!(
                (row.get_by_name::<f64, _>("value_min").unwrap().unwrap() - 273.15).abs() < 1e-10
            );
            assert!(
                (row.get_by_name::<f64, _>("value_max").unwrap().unwrap() - 274.55).abs() < 1e-10
            );
        });
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
    fn zarr_postgis_point_sample_exact_and_transformed_nearest() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_table("zarr_e2e_spatial2d");

        Spi::connect(|c| {
            c.select(
                "SELECT pg_catalog.set_config('search_path', 'zarr_gis, public, pg_catalog', true)",
                Some(1),
                &[],
            )
            .unwrap()
            .next()
            .unwrap();
            let exact = c
                .select(
                    r#"SELECT sample.*
                         FROM zarr_sample(
                           'zarr_e2e_spatial2d',
                           zarr_gis.ST_AsEWKB(
                             zarr_gis.ST_SetSRID(zarr_gis.ST_Point(110, 20), 3857)
                           ),
                           'exact'
                         ) AS sample"#,
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(exact.get_by_name::<f64, _>("x").unwrap(), Some(110.0));
            assert_eq!(exact.get_by_name::<f64, _>("y").unwrap(), Some(20.0));
            assert_eq!(exact.get_by_name::<f64, _>("value").unwrap(), Some(42.0));
            assert_eq!(exact.get_by_name::<i64, _>("x_index").unwrap(), Some(1));
            assert_eq!(exact.get_by_name::<i64, _>("y_index").unwrap(), Some(1));
            assert_eq!(
                exact.get_by_name::<f64, _>("coordinate_distance").unwrap(),
                Some(0.0)
            );
            assert_eq!(exact.get_by_name::<i32, _>("srid").unwrap(), Some(3857));

            let transformed = c
                .select(
                    r#"SELECT sample.*
                         FROM zarr_sample(
                           'zarr_e2e_spatial2d',
                           zarr_gis.ST_AsEWKB(
                             zarr_gis.ST_SetSRID(
                               zarr_gis.ST_Point(
                                 0.000988146812531,
                                 0.000179663056824
                               ),
                               4326
                             )
                           ),
                           'nearest'
                         ) AS sample"#,
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(transformed.get_by_name::<f64, _>("x").unwrap(), Some(110.0));
            assert_eq!(transformed.get_by_name::<f64, _>("y").unwrap(), Some(20.0));
            assert_eq!(
                transformed.get_by_name::<f64, _>("value").unwrap(),
                Some(42.0)
            );

            let misses = c
                .select(
                    r#"SELECT count(*)::bigint AS count
                         FROM zarr_sample(
                           'zarr_e2e_spatial2d',
                           zarr_gis.ST_AsEWKB(
                             zarr_gis.ST_SetSRID(zarr_gis.ST_Point(111, 20), 3857)
                           ),
                           'exact'
                         )"#,
                    Some(1),
                    &[],
                )
                .unwrap()
                .first()
                .get_by_name::<i64, _>("count")
                .unwrap();
            assert_eq!(misses, Some(0));
        });
    }

    #[pg_test(
        error = "invalid PostGIS geometry: point sampling method must be 'exact' or 'nearest', got 'bilinear'"
    )]
    fn zarr_postgis_point_sample_rejects_unknown_method() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_table("zarr_e2e_spatial2d_bad_method");
        Spi::run(
            r#"SELECT *
                 FROM zarr_sample(
                   'zarr_e2e_spatial2d_bad_method',
                   zarr_gis.ST_AsEWKB(
                     zarr_gis.ST_SetSRID(zarr_gis.ST_Point(110, 20), 3857)
                   ),
                   'bilinear'
                 )"#,
        )
        .unwrap();
    }

    #[pg_test(
        error = "zarr array metadata missing or invalid: spatial operations require exactly one value column"
    )]
    fn zarr_postgis_point_exact_miss_still_validates_value_column() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_coordinate_only_table("zarr_e2e_spatial2d_point_no_value");
        Spi::run(
            r#"SELECT *
                 FROM zarr_sample(
                   'zarr_e2e_spatial2d_point_no_value',
                   zarr_gis.ST_AsEWKB(
                     zarr_gis.ST_SetSRID(zarr_gis.ST_Point(111, 20), 3857)
                   ),
                   'exact'
                 )"#,
        )
        .unwrap();
    }

    #[pg_test]
    fn zarr_postgis_cells_include_boundary_and_transform_region_crs() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_table("zarr_e2e_spatial2d_cells");

        let rows = Spi::connect(|c| {
            c.select(
                "SELECT pg_catalog.set_config('search_path', 'zarr_gis, public, pg_catalog', true)",
                Some(1),
                &[],
            )
            .unwrap()
            .next()
            .unwrap();
            c.select(
                r#"WITH regions(label, region_ewkb) AS (
                       VALUES
                         (
                           'boundary',
                           zarr_gis.ST_AsEWKB(
                             zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
                           )
                         ),
                         (
                           'transformed',
                           zarr_gis.ST_AsEWKB(
                             zarr_gis.ST_MakeEnvelope(
                               0.000943231048325,
                               0.000134747292628,
                               0.001212725633561,
                               0.000404241877857,
                               4326
                             )
                           )
                         )
                     )
                     SELECT regions.label, cells.*
                       FROM regions
                       CROSS JOIN LATERAL zarr_cells(
                         'zarr_e2e_spatial2d_cells',
                         regions.region_ewkb
                       ) AS cells
                      ORDER BY regions.label, cells.y_index, cells.x_index"#,
                None,
                &[],
            )
            .unwrap()
            .map(|row| {
                (
                    row.get_by_name::<String, _>("label").unwrap().unwrap(),
                    row.get_by_name::<f64, _>("x").unwrap().unwrap(),
                    row.get_by_name::<f64, _>("y").unwrap().unwrap(),
                    row.get_by_name::<f64, _>("value").unwrap(),
                    row.get_by_name::<i64, _>("x_index").unwrap().unwrap(),
                    row.get_by_name::<i64, _>("y_index").unwrap().unwrap(),
                    row.get_by_name::<i32, _>("srid").unwrap().unwrap(),
                )
            })
            .collect::<Vec<_>>()
        });

        let mut expected = Vec::new();
        for label in ["boundary", "transformed"] {
            for y_index in 1_i64..=3 {
                for x_index in 1_i64..=3 {
                    expected.push((
                        label.to_string(),
                        100.0 + 10.0 * x_index as f64,
                        10.0 + 10.0 * y_index as f64,
                        Some(42.0),
                        x_index,
                        y_index,
                        3857,
                    ));
                }
            }
        }
        assert_eq!(rows, expected);
    }

    #[pg_test]
    fn zarr_postgis_zonal_stats_preserve_fill_value_semantics() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_table("zarr_e2e_spatial2d_zonal");

        Spi::connect(|c| {
            c.select(
                "SELECT pg_catalog.set_config('search_path', 'zarr_gis, public, pg_catalog', true)",
                Some(1),
                &[],
            )
            .unwrap()
            .next()
            .unwrap();
            let stats = c
                .select(
                    r#"SELECT stats.*
                         FROM zarr_zonal_stats(
                           'zarr_e2e_spatial2d_zonal',
                           zarr_gis.ST_AsEWKB(
                             zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
                           )
                         ) AS stats"#,
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(stats.get_by_name::<i64, _>("count").unwrap(), Some(9));
            assert_eq!(stats.get_by_name::<i64, _>("valid_count").unwrap(), Some(9));
            assert_eq!(stats.get_by_name::<f64, _>("min").unwrap(), Some(42.0));
            assert_eq!(stats.get_by_name::<f64, _>("max").unwrap(), Some(42.0));
            assert_eq!(stats.get_by_name::<f64, _>("sum").unwrap(), Some(378.0));
            assert_eq!(stats.get_by_name::<f64, _>("avg").unwrap(), Some(42.0));
            assert_eq!(stats.get_by_name::<i32, _>("srid").unwrap(), Some(3857));
        });
    }

    #[pg_test]
    fn zarr_postgis_cells_reject_non_polygon_geometry() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_table("zarr_e2e_spatial2d_bad_region");
        Spi::run(
            "SELECT pg_catalog.set_config('search_path', 'zarr_gis, public, pg_catalog', true)",
        )
        .unwrap();

        let result = std::panic::catch_unwind(|| {
            Spi::run(
                r#"SELECT *
                     FROM zarr_cells(
                       'zarr_e2e_spatial2d_bad_region',
                       zarr_gis.ST_AsEWKB(
                         zarr_gis.ST_SetSRID(zarr_gis.ST_Point(110, 20), 3857)
                       )
                     )"#,
            )
        });
        let failed = match result {
            Ok(result) => result.is_err(),
            Err(_) => true,
        };
        assert!(failed, "expected zarr_cells to reject a Point region");
    }

    #[pg_test(
        error = "zarr array metadata missing or invalid: spatial operations require exactly one value column"
    )]
    fn zarr_postgis_nonoverlap_still_validates_value_column() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_coordinate_only_table("zarr_e2e_spatial2d_region_no_value");
        Spi::run(
            r#"SELECT *
                 FROM zarr_cells(
                   'zarr_e2e_spatial2d_region_no_value',
                   zarr_gis.ST_AsEWKB(
                     zarr_gis.ST_MakeEnvelope(1000, 1000, 1010, 1010, 3857)
                   )
                 )"#,
        )
        .unwrap();
    }

    #[pg_test(
        error = "invalid CRS metadata for zarr array 'nested/spatial2d': EPSG:999999 is not present in the installed PostGIS spatial_ref_sys"
    )]
    fn zarr_postgis_cells_reject_unknown_source_srid() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_table("zarr_e2e_spatial2d_unknown_srid");
        Spi::run(
            r#"SELECT *
                 FROM zarr_cells(
                   'zarr_e2e_spatial2d_unknown_srid',
                   zarr_gis.ST_AsEWKB(
                     zarr_gis.ST_SetSRID(
                       zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857),
                       999999
                     )
                   )
                 )"#,
        )
        .unwrap();
    }

    #[pg_test(
        error = "invalid PostGIS geometry: PostGIS could not parse or transform the supplied polygon"
    )]
    fn zarr_postgis_cells_normalize_malformed_ewkb_error() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_table("zarr_e2e_spatial2d_malformed_ewkb");
        Spi::run(
            r#"SELECT *
                 FROM zarr_cells(
                   'zarr_e2e_spatial2d_malformed_ewkb',
                   '\x0102'::bytea
                 )"#,
        )
        .unwrap();
    }

    #[pg_test(
        error = "PostGIS is unavailable for zarr spatial operations: the postgis extension is not installed"
    )]
    fn zarr_postgis_cells_fail_cleanly_without_postgis() {
        create_minio_spatial2d_table("zarr_e2e_spatial2d_no_postgis");
        Spi::run(
            r#"SELECT *
                 FROM zarr_cells(
                   'zarr_e2e_spatial2d_no_postgis',
                   '\x'::bytea
                 )"#,
        )
        .unwrap();
    }

    #[pg_test(
        error = "zarr array metadata missing or invalid: foreign table 'zarr_e2e_spatial2d_private' does not exist or is not accessible"
    )]
    fn zarr_postgis_cells_enforce_foreign_table_privileges() {
        install_postgis_in_test_schema();
        create_minio_spatial2d_table("zarr_e2e_spatial2d_private");
        Spi::run(
            r#"CREATE ROLE zarr_spatial_no_access;
               GRANT USAGE ON SCHEMA zarr_gis TO zarr_spatial_no_access;
               SET ROLE zarr_spatial_no_access;
               SELECT *
                 FROM zarr_cells(
                   'zarr_e2e_spatial2d_private',
                   zarr_gis.ST_AsEWKB(
                     zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
                   )
                 )"#,
        )
        .unwrap();
    }

    #[pg_test]
    fn zarr_postgis_cells_by_time_honor_half_open_bounds() {
        install_postgis_in_test_schema();
        create_minio_spatial_time_table("zarr_e2e_spatial_time_cells", false);

        let rows = Spi::connect(|c| {
            c.select(
                r#"SELECT extract(epoch FROM cells.time)::double precision AS epoch,
                          cells.time_index,
                          pg_catalog.count(*)::bigint AS count,
                          pg_catalog.min(cells.value) AS min,
                          pg_catalog.max(cells.value) AS max,
                          pg_catalog.sum(cells.value)::double precision AS sum
                     FROM zarr_cells_by_time(
                       'zarr_e2e_spatial_time_cells',
                       zarr_gis.ST_AsEWKB(
                         zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
                       ),
                       TIMESTAMPTZ '1970-01-01 00:00:00+00',
                       TIMESTAMPTZ '1970-01-01 00:00:03.600001+00'
                     ) AS cells
                    GROUP BY cells.time, cells.time_index
                    ORDER BY cells.time_index"#,
                None,
                &[],
            )
            .unwrap()
            .map(|row| {
                (
                    row.get_by_name::<f64, _>("epoch").unwrap().unwrap(),
                    row.get_by_name::<i64, _>("time_index").unwrap().unwrap(),
                    row.get_by_name::<i64, _>("count").unwrap().unwrap(),
                    row.get_by_name::<f64, _>("min").unwrap().unwrap(),
                    row.get_by_name::<f64, _>("max").unwrap().unwrap(),
                    row.get_by_name::<f64, _>("sum").unwrap().unwrap(),
                )
            })
            .collect::<Vec<_>>()
        });

        assert_eq!(
            rows,
            vec![
                (0.0, 0, 9, 11.0, 33.0, 198.0),
                (3.6, 1, 9, 111.0, 133.0, 1098.0)
            ]
        );

        let first_slice_count = Spi::get_one::<i64>(
            r#"SELECT pg_catalog.count(*)::bigint
                 FROM zarr_cells_by_time(
                   'zarr_e2e_spatial_time_cells',
                   zarr_gis.ST_AsEWKB(
                     zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
                   ),
                   TIMESTAMPTZ '1970-01-01 00:00:00+00',
                   TIMESTAMPTZ '1970-01-01 00:00:03.6+00'
                 )"#,
        )
        .unwrap();
        assert_eq!(first_slice_count, Some(9));
    }

    #[pg_test]
    fn zarr_postgis_zonal_stats_by_time_preserve_scientific_semantics() {
        install_postgis_in_test_schema();
        create_minio_spatial_time_table("zarr_e2e_spatial_time_zonal", false);

        let rows = Spi::connect(|c| {
            c.select(
                r#"SELECT extract(epoch FROM stats.time)::double precision AS epoch,
                          stats.time_index,
                          stats.count,
                          stats.valid_count,
                          stats.min,
                          stats.max,
                          stats.sum,
                          stats.avg,
                          stats.srid
                     FROM zarr_zonal_stats_by_time(
                       'zarr_e2e_spatial_time_zonal',
                       zarr_gis.ST_AsEWKB(
                         zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
                       ),
                       TIMESTAMPTZ '1970-01-01 00:00:00+00',
                       TIMESTAMPTZ '1970-01-01 00:00:03.600001+00'
                     ) AS stats
                    ORDER BY stats.time_index"#,
                None,
                &[],
            )
            .unwrap()
            .map(|row| {
                (
                    row.get_by_name::<f64, _>("epoch").unwrap().unwrap(),
                    row.get_by_name::<i64, _>("time_index").unwrap().unwrap(),
                    row.get_by_name::<i64, _>("count").unwrap().unwrap(),
                    row.get_by_name::<i64, _>("valid_count").unwrap().unwrap(),
                    row.get_by_name::<f64, _>("min").unwrap(),
                    row.get_by_name::<f64, _>("max").unwrap(),
                    row.get_by_name::<f64, _>("sum").unwrap(),
                    row.get_by_name::<f64, _>("avg").unwrap(),
                    row.get_by_name::<i32, _>("srid").unwrap().unwrap(),
                )
            })
            .collect::<Vec<_>>()
        });

        assert_eq!(
            rows,
            vec![
                (
                    0.0,
                    0,
                    9,
                    9,
                    Some(11.0),
                    Some(33.0),
                    Some(198.0),
                    Some(22.0),
                    3857,
                ),
                (
                    3.6,
                    1,
                    9,
                    9,
                    Some(111.0),
                    Some(133.0),
                    Some(1098.0),
                    Some(122.0),
                    3857,
                ),
            ]
        );
    }

    #[pg_test]
    fn zarr_postgis_zonal_stats_by_time_keep_decoded_null_slices() {
        install_postgis_in_test_schema();
        create_minio_spatial_time_table("zarr_e2e_spatial_time_decoded", true);

        let rows = Spi::connect(|c| {
            c.select(
                r#"SELECT stats.time_index,
                          stats.count,
                          stats.valid_count,
                          stats.min,
                          stats.max,
                          stats.sum,
                          stats.avg
                     FROM zarr_zonal_stats_by_time(
                       'zarr_e2e_spatial_time_decoded',
                       zarr_gis.ST_AsEWKB(
                         zarr_gis.ST_MakeEnvelope(140, 40, 150, 50, 3857)
                       ),
                       TIMESTAMPTZ '1970-01-01 00:00:00+00',
                       TIMESTAMPTZ '1970-01-01 00:00:03.600001+00'
                     ) AS stats
                    ORDER BY stats.time_index"#,
                None,
                &[],
            )
            .unwrap()
            .map(|row| {
                (
                    row.get_by_name::<i64, _>("time_index").unwrap().unwrap(),
                    row.get_by_name::<i64, _>("count").unwrap().unwrap(),
                    row.get_by_name::<i64, _>("valid_count").unwrap().unwrap(),
                    row.get_by_name::<f64, _>("min").unwrap(),
                    row.get_by_name::<f64, _>("max").unwrap(),
                    row.get_by_name::<f64, _>("sum").unwrap(),
                    row.get_by_name::<f64, _>("avg").unwrap(),
                )
            })
            .collect::<Vec<_>>()
        });

        assert_eq!(
            rows,
            vec![
                (0, 4, 0, None, None, None, None),
                (1, 4, 0, None, None, None, None),
            ]
        );
    }

    #[pg_test]
    fn zarr_postgis_time_range_validation_and_empty_selection() {
        install_postgis_in_test_schema();
        create_minio_spatial_time_table("zarr_e2e_spatial_time_range", false);

        let no_rows = Spi::get_one::<i64>(
            r#"SELECT pg_catalog.count(*)::bigint
                 FROM zarr_zonal_stats_by_time(
                   'zarr_e2e_spatial_time_range',
                   zarr_gis.ST_AsEWKB(
                     zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
                   ),
                   TIMESTAMPTZ '1970-01-01 00:00:10+00',
                   TIMESTAMPTZ '1970-01-01 00:00:20+00'
                 )"#,
        )
        .unwrap();
        assert_eq!(no_rows, Some(0));

        Spi::connect(|c| {
            let empty = c
                .select(
                    r#"SELECT pg_catalog.count(*)::bigint AS rows,
                              pg_catalog.count(*) FILTER (
                                WHERE stats.count = 0
                                  AND stats.valid_count = 0
                                  AND stats.min IS NULL
                                  AND stats.max IS NULL
                                  AND stats.sum IS NULL
                                  AND stats.avg IS NULL
                              )::bigint AS empty_rows
                         FROM zarr_zonal_stats_by_time(
                           'zarr_e2e_spatial_time_range',
                           zarr_gis.ST_AsEWKB(
                             zarr_gis.ST_MakeEnvelope(1000, 1000, 1010, 1010, 3857)
                           ),
                           TIMESTAMPTZ '1970-01-01 00:00:00+00',
                           TIMESTAMPTZ '1970-01-01 00:00:03.600001+00'
                         ) AS stats"#,
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(empty.get_by_name::<i64, _>("rows").unwrap(), Some(2));
            assert_eq!(empty.get_by_name::<i64, _>("empty_rows").unwrap(), Some(2));
        });

        let invalid = std::panic::catch_unwind(|| {
            Spi::run(
                r#"SELECT *
                     FROM zarr_zonal_stats_by_time(
                       'zarr_e2e_spatial_time_range',
                       zarr_gis.ST_AsEWKB(
                         zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
                       ),
                       TIMESTAMPTZ '1970-01-01 00:00:03.6+00',
                       TIMESTAMPTZ '1970-01-01 00:00:03.6+00'
                     )"#,
            )
        });
        let failed = match invalid {
            Ok(result) => result.is_err(),
            Err(_) => true,
        };
        assert!(failed, "expected an empty time range to be rejected");
    }

    #[pg_test(
        error = "PostGIS is unavailable for zarr spatial operations: the postgis extension is not installed"
    )]
    fn zarr_postgis_cells_by_time_fail_cleanly_without_postgis() {
        create_minio_spatial_time_table("zarr_e2e_spatial_time_no_postgis", false);
        Spi::run(
            r#"SELECT *
                 FROM zarr_cells_by_time(
                   'zarr_e2e_spatial_time_no_postgis',
                   '\x'::bytea,
                   TIMESTAMPTZ '1970-01-01 00:00:00+00',
                   TIMESTAMPTZ '1970-01-01 00:00:03.6+00'
                 )"#,
        )
        .unwrap();
    }

    #[pg_test(
        error = "zarr array metadata missing or invalid: foreign table 'zarr_e2e_spatial_time_private' does not exist or is not accessible"
    )]
    fn zarr_postgis_cells_by_time_enforce_foreign_table_privileges() {
        install_postgis_in_test_schema();
        create_minio_spatial_time_table("zarr_e2e_spatial_time_private", false);
        Spi::run(
            r#"CREATE ROLE zarr_spatial_time_no_access;
               GRANT USAGE ON SCHEMA zarr_gis TO zarr_spatial_time_no_access;
               SET ROLE zarr_spatial_time_no_access;
               SELECT *
                 FROM zarr_cells_by_time(
                   'zarr_e2e_spatial_time_private',
                   zarr_gis.ST_AsEWKB(
                     zarr_gis.ST_MakeEnvelope(110, 20, 130, 40, 3857)
                   ),
                   TIMESTAMPTZ '1970-01-01 00:00:00+00',
                   TIMESTAMPTZ '1970-01-01 00:00:03.6+00'
                 )"#,
        )
        .unwrap();
    }

    #[pg_test]
    fn zarr_multiscales_minio_ome_v05_discovery_e2e() {
        create_minio_ome_v3_e2e_server();

        Spi::connect(|c| {
            let rows = c
                .select(
                    r#"SELECT group_path, multiscale_index, multiscale_name,
                              level_index, array_path, axes, shape, chunks,
                              dtype, codecs, scale, translation, supported,
                              warnings
                         FROM zarr_multiscales('zarr_ome_v3_e2e_server')
                        ORDER BY group_path, multiscale_index, level_index"#,
                    None,
                    &[],
                )
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 2);

            let expected = [
                (
                    0_i64,
                    "image/0",
                    serde_json::json!([4, 4]),
                    serde_json::json!([3, 3]),
                    vec![4.0, 12.0],
                    vec![120.0, 260.0],
                ),
                (
                    1_i64,
                    "image/1",
                    serde_json::json!([2, 2]),
                    serde_json::json!([2, 2]),
                    vec![8.0, 24.0],
                    vec![122.0, 266.0],
                ),
            ];
            for (row, (level, array_path, shape, chunks, scale, translation)) in
                rows.iter().zip(expected)
            {
                assert_eq!(
                    row.get_by_name::<String, _>("group_path").unwrap(),
                    Some("image".to_string())
                );
                assert_eq!(
                    row.get_by_name::<i64, _>("multiscale_index").unwrap(),
                    Some(0)
                );
                assert_eq!(
                    row.get_by_name::<String, _>("multiscale_name").unwrap(),
                    Some("mean-pyramid".to_string())
                );
                assert_eq!(
                    row.get_by_name::<i64, _>("level_index").unwrap(),
                    Some(level)
                );
                assert_eq!(
                    row.get_by_name::<String, _>("array_path").unwrap(),
                    Some(array_path.to_string())
                );
                assert_eq!(
                    row.get_by_name::<JsonB, _>("axes").unwrap().unwrap().0,
                    serde_json::json!([
                        {"name": "y", "type": "space", "unit": "micrometer"},
                        {"name": "x", "type": "space", "unit": "micrometer"}
                    ])
                );
                assert_eq!(
                    row.get_by_name::<JsonB, _>("shape").unwrap().unwrap().0,
                    shape
                );
                assert_eq!(
                    row.get_by_name::<JsonB, _>("chunks").unwrap().unwrap().0,
                    chunks
                );
                assert_eq!(
                    row.get_by_name::<String, _>("dtype").unwrap(),
                    Some("float32".to_string())
                );
                assert_eq!(
                    row.get_by_name::<JsonB, _>("codecs").unwrap().unwrap().0,
                    serde_json::json!([{
                        "name": "bytes",
                        "configuration": {"endian": "little"}
                    }])
                );
                assert_eq!(
                    row.get_by_name::<Vec<f64>, _>("scale").unwrap(),
                    Some(scale)
                );
                assert_eq!(
                    row.get_by_name::<Vec<f64>, _>("translation").unwrap(),
                    Some(translation)
                );
                assert_eq!(row.get_by_name::<bool, _>("supported").unwrap(), Some(true));
                assert_eq!(
                    row.get_by_name::<Vec<String>, _>("warnings").unwrap(),
                    Some(Vec::new())
                );
            }

            let paths = c
                .select(
                    "SELECT path FROM zarr_inspect('zarr_ome_v3_e2e_server') ORDER BY path",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get_by_name::<String, _>("path").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(paths, vec!["/", "image", "image/0", "image/1"]);
        });
    }

    #[pg_test]
    fn zarr_minio_ome_v05_explicit_level_scans_e2e() {
        create_minio_ome_v3_e2e_server();
        create_minio_ome_v3_e2e_table_on_server("zarr_ome_v05_level0", 0);
        create_minio_ome_v3_e2e_table_on_server("zarr_ome_v05_level1", 1);

        Spi::connect(|c| {
            let level0 = c
                .select(
                    "SELECT y, x, value FROM zarr_ome_v05_level0 ORDER BY y, x",
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| {
                    (
                        row.get_by_name::<f64, _>("y").unwrap().unwrap(),
                        row.get_by_name::<f64, _>("x").unwrap().unwrap(),
                        row.get_by_name::<f32, _>("value").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                level0,
                vec![
                    (120.0, 260.0, 0.0),
                    (120.0, 272.0, 1.0),
                    (120.0, 284.0, 2.0),
                    (120.0, 296.0, 3.0),
                    (124.0, 260.0, 4.0),
                    (124.0, 272.0, 5.0),
                    (124.0, 284.0, 6.0),
                    (124.0, 296.0, 7.0),
                    (128.0, 260.0, 8.0),
                    (128.0, 272.0, 9.0),
                    (128.0, 284.0, 10.0),
                    (128.0, 296.0, 11.0),
                    (132.0, 260.0, 12.0),
                    (132.0, 272.0, 13.0),
                    (132.0, 284.0, 14.0),
                    (132.0, 296.0, 15.0),
                ]
            );

            let level1 = c
                .select(
                    "SELECT y, x, value FROM zarr_ome_v05_level1 ORDER BY y, x",
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| {
                    (
                        row.get_by_name::<f64, _>("y").unwrap().unwrap(),
                        row.get_by_name::<f64, _>("x").unwrap().unwrap(),
                        row.get_by_name::<f32, _>("value").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                level1,
                vec![
                    (122.0, 266.0, 2.5),
                    (122.0, 290.0, 4.5),
                    (130.0, 266.0, 10.5),
                    (130.0, 290.0, 12.5),
                ]
            );
        });
    }

    #[pg_test]
    fn zarr_minio_ome_v05_affine_pruning_metrics_e2e() {
        create_minio_ome_v3_e2e_table("zarr_ome_v05_pruning", 0);

        Spi::connect(|c| {
            let value = c
                .select(
                    "SELECT value FROM zarr_ome_v05_pruning WHERE y = 132 AND x = 296",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<f32, _>("value")
                .unwrap();
            assert_eq!(value, Some(15.0));

            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_ome_v05_pruning
                        WHERE y = 132 AND x = 296"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Array: image/0"), "plan: {plan:?}");
            assert!(has("Zarr Dimensions: [y, x]"), "plan: {plan:?}");
            assert!(has("Zarr Shape: [4, 4]"), "plan: {plan:?}");
            assert!(has("Zarr Chunk Shape: [3, 3]"), "plan: {plan:?}");
            assert!(has("Zarr OME Group: image"), "plan: {plan:?}");
            assert!(has("Zarr OME Multiscale Index: 0"), "plan: {plan:?}");
            assert!(has("Zarr OME Level Index: 0"), "plan: {plan:?}");
            assert!(
                has("Zarr OME Effective Scale: [4.0, 12.0]"),
                "plan: {plan:?}"
            );
            assert!(
                has("Zarr OME Effective Translation: [120.0, 260.0]"),
                "plan: {plan:?}"
            );
            assert!(has("Zarr Chunks Total: 4"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Selected: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Coordinate-Pruned: 3"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Requested: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Present: 1"), "plan: {plan:?}");
            assert!(has("Zarr Coordinate GET Calls: 0"), "plan: {plan:?}");
            assert!(
                has("Zarr Coordinate Encoded Bytes: 0 bytes"),
                "plan: {plan:?}"
            );
            assert!(has("Zarr Data GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 36 bytes"), "plan: {plan:?}");
            assert!(has("Zarr Data Decoded Bytes: 36 bytes"), "plan: {plan:?}");
        });
    }

    #[pg_test]
    fn zarr_minio_ome_v05_aggregate_pushdown_e2e() {
        create_minio_ome_v3_e2e_server();
        create_minio_ome_v3_e2e_table_on_server("zarr_ome_v05_aggregate0", 0);
        create_minio_ome_v3_e2e_table_on_server("zarr_ome_v05_aggregate1", 1);

        for (table, count, sum, minimum, maximum) in [
            (
                "zarr_ome_v05_aggregate0",
                16_i64,
                120.0_f32,
                0.0_f32,
                15.0_f32,
            ),
            (
                "zarr_ome_v05_aggregate1",
                4_i64,
                30.0_f32,
                2.5_f32,
                12.5_f32,
            ),
        ] {
            let sql = format!(
                r#"SELECT count(*) AS total_count,
                           count(value) AS value_count,
                           sum(value) AS value_sum,
                           avg(value) AS value_avg,
                           min(value) AS value_min,
                           max(value) AS value_max
                      FROM {table}"#
            );
            assert_aggregate_pushed_down(&sql);
            Spi::connect(|c| {
                let row = c.select(&sql, None, &[]).unwrap().next().unwrap();
                assert_eq!(
                    row.get_by_name::<i64, _>("total_count").unwrap(),
                    Some(count)
                );
                assert_eq!(
                    row.get_by_name::<i64, _>("value_count").unwrap(),
                    Some(count)
                );
                assert_eq!(row.get_by_name::<f32, _>("value_sum").unwrap(), Some(sum));
                assert_eq!(row.get_by_name::<f64, _>("value_avg").unwrap(), Some(7.5));
                assert_eq!(
                    row.get_by_name::<f32, _>("value_min").unwrap(),
                    Some(minimum)
                );
                assert_eq!(
                    row.get_by_name::<f32, _>("value_max").unwrap(),
                    Some(maximum)
                );
            });
        }

        for table in ["zarr_ome_v05_aggregate0", "zarr_ome_v05_aggregate1"] {
            Spi::connect(|c| {
                let row = c
                    .select(
                        &format!(
                            r#"SELECT count(*) AS cells,
                                      sum(value) AS value_sum
                                 FROM {table}
                                WHERE y BETWEEN 122 AND 130
                                  AND x BETWEEN 266 AND 290"#
                        ),
                        None,
                        &[],
                    )
                    .unwrap()
                    .next()
                    .unwrap();
                assert_eq!(row.get_by_name::<i64, _>("cells").unwrap(), Some(4));
                assert_eq!(row.get_by_name::<f32, _>("value_sum").unwrap(), Some(30.0));
            });
        }
    }

    #[pg_test]
    fn zarr_minio_ome_v05_selector_rejections_e2e() {
        create_minio_ome_v3_e2e_server();

        let partial = capture_query_error(
            r#"CREATE FOREIGN TABLE zarr_ome_v05_partial (
                 y double precision, x double precision, value real
               ) SERVER zarr_ome_v3_e2e_server
               OPTIONS (multiscale_group 'image')"#,
        );
        assert!(
            partial.contains(
                "multiscale_group, multiscale_index, and multiscale_level must be provided together"
            ),
            "message: {partial}"
        );

        let conflicting = capture_query_error(
            r#"CREATE FOREIGN TABLE zarr_ome_v05_conflicting (
                 y double precision, x double precision, value real
               ) SERVER zarr_ome_v3_e2e_server
               OPTIONS (
                 array_group 'image/0',
                 multiscale_group 'image',
                 multiscale_index '0',
                 multiscale_level '0'
               )"#,
        );
        assert!(
            conflicting
                .contains("array_group cannot be combined with multiscale selection options"),
            "message: {conflicting}"
        );

        create_minio_ome_v3_e2e_table_on_server("zarr_ome_v05_bad_level", 2);
        let bad_level = capture_query_error("SELECT * FROM zarr_ome_v05_bad_level");
        assert!(
            bad_level.contains("multiscale level 2 is outside"),
            "message: {bad_level}"
        );

        Spi::run(
            r#"CREATE FOREIGN TABLE zarr_ome_v05_bad_index (
                 y double precision, x double precision, value real
               ) SERVER zarr_ome_v3_e2e_server
               OPTIONS (
                 multiscale_group 'image',
                 multiscale_index '1',
                 multiscale_level '0'
               )"#,
        )
        .unwrap();
        let bad_index = capture_query_error("SELECT * FROM zarr_ome_v05_bad_index");
        assert!(
            bad_index.contains("multiscale index 1 is outside"),
            "message: {bad_index}"
        );
    }

    #[pg_test]
    fn zarr_minio_v3_default_and_v2_chunk_keys_scan_e2e() {
        create_minio_v3_e2e_server();
        for (table, array_group) in [
            ("zarr_v3_default_keys", "nested/raw_default"),
            ("zarr_v3_v2_keys", "nested/raw_v2keys"),
        ] {
            create_minio_v3_e2e_table_on_server(table, array_group, false);
            Spi::connect(|c| {
                let summary = c
                    .select(
                        &format!(
                            r#"SELECT count(*) AS row_count,
                                      count(value) AS value_count,
                                      sum(value)::double precision AS value_sum,
                                      min(value)::double precision AS value_min,
                                      max(value)::double precision AS value_max
                                 FROM {table}"#
                        ),
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
                    summary
                        .get_by_name::<i64, _>("value_count")
                        .unwrap()
                        .unwrap(),
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

                let boundary = c
                    .select(
                        &format!(
                            r#"SELECT value
                                 FROM {table}
                                WHERE time = '1970-01-01 00:00:03.6+00'::timestamptz
                                  AND y = 50
                                  AND x = 150"#
                        ),
                        None,
                        &[],
                    )
                    .unwrap()
                    .filter_map(|row| row.get_by_name::<f32, _>("value").unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(boundary, vec![-7.5]);
            });
        }
    }

    #[pg_test]
    fn zarr_minio_v3_sharding_start_end_scan_and_sparse_fill_e2e() {
        create_minio_v3_e2e_server();
        for (table, array_group) in [
            ("zarr_v3_shard_end", "nested/shard_end"),
            ("zarr_v3_shard_start", "nested/shard_start"),
        ] {
            create_minio_v3_e2e_table_on_server(table, array_group, false);
            let aggregate_sql = format!(
                r#"SELECT count(*) AS total_count,
                           count(value) AS value_count,
                           sum(value) AS value_sum,
                           avg(value) AS value_avg,
                           min(value) AS value_min,
                           max(value) AS value_max
                      FROM {table}"#
            );
            assert_aggregate_pushed_down(&aggregate_sql);

            Spi::connect(|c| {
                let summary = c.select(&aggregate_sql, None, &[]).unwrap().next().unwrap();
                assert_eq!(
                    summary
                        .get_by_name::<i64, _>("total_count")
                        .unwrap()
                        .unwrap(),
                    60
                );
                assert_eq!(
                    summary
                        .get_by_name::<i64, _>("value_count")
                        .unwrap()
                        .unwrap(),
                    60
                );
                assert_eq!(
                    summary.get_by_name::<f32, _>("value_sum").unwrap().unwrap(),
                    3_574.0
                );
                assert!(
                    (summary.get_by_name::<f64, _>("value_avg").unwrap().unwrap()
                        - 59.566_666_666_666_67)
                        .abs()
                        < 1e-12
                );
                assert_eq!(
                    summary.get_by_name::<f32, _>("value_min").unwrap().unwrap(),
                    -7.5
                );
                assert_eq!(
                    summary.get_by_name::<f32, _>("value_max").unwrap().unwrap(),
                    143.0
                );
                let fill_count = c
                    .select(
                        &format!("SELECT count(*) FROM {table} WHERE value = -7.5"),
                        Some(1),
                        &[],
                    )
                    .unwrap()
                    .next()
                    .unwrap()
                    .get::<i64>(1)
                    .unwrap();
                assert_eq!(fill_count, Some(8));

                // The pinned start-index shard stores these two logical inner
                // chunks out of C-order physically. Correct values prove that
                // the index offsets, rather than payload order, drive reads.
                let morton_order_probe = c
                    .select(
                        &format!(
                            r#"SELECT x, value
                                 FROM {table}
                                WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                                  AND y = 20
                                  AND x BETWEEN 110 AND 130
                                ORDER BY x"#
                        ),
                        None,
                        &[],
                    )
                    .unwrap()
                    .map(|row| {
                        (
                            row.get_by_name::<f64, _>("x").unwrap().unwrap(),
                            row.get_by_name::<f32, _>("value").unwrap().unwrap(),
                        )
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    morton_order_probe,
                    vec![(110.0, 11.0), (120.0, 12.0), (130.0, 13.0)]
                );

                let absent_shard_fill = c
                    .select(
                        &format!(
                            r#"SELECT value
                                 FROM {table}
                                WHERE time = '1970-01-01 00:00:03.6+00'::timestamptz
                                  AND y = 50
                                  AND x = 150"#
                        ),
                        Some(1),
                        &[],
                    )
                    .unwrap()
                    .next()
                    .unwrap()
                    .get_by_name::<f32, _>("value")
                    .unwrap();
                assert_eq!(absent_shard_fill, Some(-7.5));
            });
        }
    }

    #[pg_test]
    fn zarr_minio_v3_sharding_scientific_aggregate_pushdown_e2e() {
        create_minio_v3_e2e_table("zarr_v3_shard_cf", "nested/shard_end", true);
        let sql = r#"SELECT count(*) AS total_count,
                            count(value) AS value_count,
                            sum(value) AS value_sum,
                            avg(value) AS value_avg,
                            min(value) AS value_min,
                            max(value) AS value_max
                       FROM zarr_v3_shard_cf"#;
        assert_aggregate_pushed_down(sql);

        Spi::connect(|c| {
            let row = c.select(sql, None, &[]).unwrap().next().unwrap();
            assert_eq!(
                row.get_by_name::<i64, _>("total_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                row.get_by_name::<i64, _>("value_count").unwrap().unwrap(),
                48
            );
            assert!(
                (row.get_by_name::<f64, _>("value_sum").unwrap().unwrap() - 13_142.86).abs() < 1e-8
            );
            assert!(
                (row.get_by_name::<f64, _>("value_avg").unwrap().unwrap() - 273.809_583_333_333_36)
                    .abs()
                    < 1e-10
            );
            assert!(
                (row.get_by_name::<f64, _>("value_min").unwrap().unwrap() - 273.15).abs() < 1e-10
            );
            assert!(
                (row.get_by_name::<f64, _>("value_max").unwrap().unwrap() - 274.55).abs() < 1e-10
            );
        });
    }

    #[pg_test]
    fn zarr_minio_v3_sharding_missing_inner_sentinel_uses_fill_e2e() {
        create_minio_v3_e2e_server();
        create_minio_v3_e2e_table_on_server(
            "zarr_v3_shard_sentinel_raw",
            "nested/shard_sentinel",
            false,
        );
        create_minio_v3_e2e_table_on_server(
            "zarr_v3_shard_sentinel_cf",
            "nested/shard_sentinel",
            true,
        );

        Spi::connect(|c| {
            let raw = c
                .select(
                    r#"SELECT value
                         FROM zarr_v3_shard_sentinel_raw
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x = 110"#,
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<f32, _>("value")
                .unwrap();
            assert_eq!(raw, Some(-7.5));

            let decoded = c
                .select(
                    r#"SELECT value
                         FROM zarr_v3_shard_sentinel_cf
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x = 110"#,
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<f64, _>("value")
                .unwrap();
            assert_eq!(decoded, None);
        });
    }

    #[pg_test]
    fn zarr_minio_v3_sharding_index_corruption_fails_closed_e2e() {
        create_minio_v3_e2e_server();
        let cases: [(&str, &str, &[&str]); 4] = [
            (
                "zarr_v3_shard_bad_index_crc",
                "nested/shard_bad_index_crc",
                &["shard index codec index 1 ('crc32c')", "checksum mismatch"],
            ),
            (
                "zarr_v3_shard_truncated_index",
                "nested/shard_truncated_index",
                &["expected exactly the final 68 bytes"],
            ),
            (
                "zarr_v3_shard_oob",
                "nested/shard_oob",
                &["inner chunk byte range", "exceeds shard object length"],
            ),
            (
                "zarr_v3_shard_half_sentinel",
                "nested/shard_half_sentinel",
                &[
                    "uses a mixed uint64 missing sentinel",
                    "offset and nbytes must both be 2^64 - 1",
                ],
            ),
        ];

        for (table, array_group, expected_phrases) in cases {
            create_minio_v3_e2e_table_on_server(table, array_group, false);
            let message = capture_query_error(&format!(
                r#"SELECT value
                     FROM {table}
                    WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                      AND y = 20
                      AND x = 110"#
            ));
            assert!(
                message.contains(&format!("{array_group}/c/0/0/0")),
                "message: {message}"
            );
            for phrase in expected_phrases {
                assert!(message.contains(*phrase), "message: {message}");
            }
        }
    }

    #[pg_test]
    fn zarr_minio_v3_sharding_bounded_range_metrics_e2e() {
        create_minio_v3_e2e_server();
        create_minio_v3_e2e_table_on_server("zarr_v3_shard_ranges", "nested/shard_end", false);
        create_minio_v3_e2e_table_on_server(
            "zarr_v3_shard_start_range",
            "nested/shard_start",
            false,
        );

        Spi::connect(|c| {
            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_v3_shard_ranges
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x BETWEEN 110 AND 130"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(
                has("Zarr Storage Layout: sharding_indexed (index: end)"),
                "plan: {plan:?}"
            );
            assert!(has("Zarr Shard Shape: [2, 3, 4]"), "plan: {plan:?}");
            assert!(has("Zarr Chunk Shape: [1, 3, 2]"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index Location: end"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Selected: 2"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Requested: 2"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Present: 2"), "plan: {plan:?}");
            assert!(has("Zarr Data GET Calls: 3"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 116 bytes"), "plan: {plan:?}");
            assert!(has("Zarr Cache Hits: 0"), "plan: {plan:?}");
            assert!(has("Zarr Cache Misses: 2"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Shard Payload GET Calls: 2"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index Cache Hits: 1"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index Cache Misses: 1"), "plan: {plan:?}");
            assert!(
                has("Zarr Shard Index Encoded Bytes: 68 bytes"),
                "plan: {plan:?}"
            );
            assert!(
                has("Zarr Shard Payload Encoded Bytes: 48 bytes"),
                "plan: {plan:?}"
            );

            let start_plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_v3_shard_start_range
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x = 130"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let start_has = |text: &str| start_plan.iter().any(|line| line.contains(text));
            assert!(
                start_has("Zarr Storage Layout: sharding_indexed (index: start)"),
                "plan: {start_plan:?}"
            );
            assert!(
                start_has("Zarr Shard Index Location: start"),
                "plan: {start_plan:?}"
            );
            assert!(start_has("Zarr Data GET Calls: 2"), "plan: {start_plan:?}");
            assert!(
                start_has("Zarr Data Encoded Bytes: 92 bytes"),
                "plan: {start_plan:?}"
            );
            assert!(
                start_has("Zarr Shard Index GET Calls: 1"),
                "plan: {start_plan:?}"
            );
            assert!(
                start_has("Zarr Shard Payload GET Calls: 1"),
                "plan: {start_plan:?}"
            );
            assert!(
                start_has("Zarr Shard Index Encoded Bytes: 68 bytes"),
                "plan: {start_plan:?}"
            );
            assert!(
                start_has("Zarr Shard Payload Encoded Bytes: 24 bytes"),
                "plan: {start_plan:?}"
            );
        });
    }

    #[pg_test]
    fn zarr_minio_v3_sharding_rescan_cache_e2e() {
        create_minio_v3_e2e_table("zarr_v3_shard_rescan", "nested/shard_end", false);

        Spi::connect(|c| {
            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT ordinal,
                              (SELECT count(*)
                                 FROM zarr_v3_shard_rescan
                                WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                                  AND y = 20
                                  AND x BETWEEN 110 AND upper_x) AS selected
                         FROM (VALUES (1, 130.0::double precision),
                                      (2, 130.0::double precision)) AS limits(ordinal, upper_x)
                        ORDER BY ordinal"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Chunks Requested: 4"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Present: 4"), "plan: {plan:?}");
            assert!(has("Zarr Data GET Calls: 3"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 116 bytes"), "plan: {plan:?}");
            assert!(has("Zarr Cache Hits: 2"), "plan: {plan:?}");
            assert!(has("Zarr Cache Misses: 2"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Shard Payload GET Calls: 2"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index Cache Hits: 3"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index Cache Misses: 1"), "plan: {plan:?}");
            assert!(has("Zarr Rescans: 1"), "plan: {plan:?}");
        });
    }

    #[pg_test]
    fn zarr_minio_v3_ordered_codec_pipeline_scan_e2e() {
        create_minio_v3_e2e_table("zarr_v3_pipeline", "nested/pipeline", false);

        Spi::connect(|c| {
            let summary = c
                .select(
                    r#"SELECT count(*) AS total_count,
                              count(value) AS value_count,
                              sum(value)::double precision AS value_sum,
                              avg(value) AS value_avg,
                              min(value)::double precision AS value_min,
                              max(value)::double precision AS value_max,
                              count(*) FILTER (WHERE value = -7.5) AS fill_count
                         FROM zarr_v3_pipeline"#,
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
                    .get_by_name::<i64, _>("value_count")
                    .unwrap()
                    .unwrap(),
                60
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_sum").unwrap().unwrap(),
                3_574.0
            );
            assert!(
                (summary.get_by_name::<f64, _>("value_avg").unwrap().unwrap()
                    - 59.566_666_666_666_67)
                    .abs()
                    < 1e-12
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_min").unwrap().unwrap(),
                -7.5
            );
            assert_eq!(
                summary.get_by_name::<f64, _>("value_max").unwrap().unwrap(),
                143.0
            );
            assert_eq!(
                summary
                    .get_by_name::<i64, _>("fill_count")
                    .unwrap()
                    .unwrap(),
                8
            );

            for (time, y, x, expected) in [
                ("1970-01-01 00:00:00+00", 20, 110, 11.0_f32),
                ("1970-01-01 00:00:00+00", 50, 130, 43.0_f32),
                ("1970-01-01 00:00:03.6+00", 20, 110, 111.0_f32),
                ("1970-01-01 00:00:03.6+00", 30, 150, 125.0_f32),
                ("1970-01-01 00:00:03.6+00", 50, 150, -7.5_f32),
            ] {
                let values = c
                    .select(
                        &format!(
                            "SELECT value FROM zarr_v3_pipeline \
                             WHERE time = '{time}'::timestamptz AND y = {y} AND x = {x}"
                        ),
                        None,
                        &[],
                    )
                    .unwrap()
                    .filter_map(|row| row.get_by_name::<f32, _>("value").unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(values, vec![expected]);
            }

            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_v3_pipeline
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x = 110"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Chunks Selected: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Requested: 1"), "plan: {plan:?}");
            assert!(
                has("Zarr Codec: transpose -> bytes -> gzip -> crc32c"),
                "plan: {plan:?}"
            );
        });
    }

    #[pg_test]
    fn zarr_minio_v3_codec_pipeline_aggregate_pushdown_e2e() {
        create_minio_v3_e2e_table("zarr_v3_pipeline_cf", "nested/pipeline", true);
        let sql = r#"SELECT count(*) AS total_count,
                            count(value) AS value_count,
                            sum(value) AS value_sum,
                            avg(value) AS value_avg,
                            min(value) AS value_min,
                            max(value) AS value_max
                       FROM zarr_v3_pipeline_cf"#;
        assert_aggregate_pushed_down(sql);

        Spi::connect(|c| {
            let row = c.select(sql, None, &[]).unwrap().next().unwrap();
            assert_eq!(
                row.get_by_name::<i64, _>("total_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                row.get_by_name::<i64, _>("value_count").unwrap().unwrap(),
                48
            );
            let value_sum = row.get_by_name::<f64, _>("value_sum").unwrap().unwrap();
            let value_avg = row.get_by_name::<f64, _>("value_avg").unwrap().unwrap();
            let value_min = row.get_by_name::<f64, _>("value_min").unwrap().unwrap();
            let value_max = row.get_by_name::<f64, _>("value_max").unwrap().unwrap();
            assert!((value_sum - 13_142.86).abs() < 1e-8);
            assert!((value_avg - 273.809_583_333_333_36).abs() < 1e-10);
            assert!((value_min - 273.15).abs() < 1e-10);
            assert!((value_max - 274.55).abs() < 1e-10);
        });
    }

    #[pg_test]
    fn zarr_minio_v3_zstd_pipeline_scan_and_aggregate_e2e() {
        create_minio_v3_e2e_server();
        create_minio_v3_e2e_table_on_server("zarr_v3_zstd_pipeline", "nested/zstd_pipeline", false);
        create_minio_v3_e2e_table_on_server(
            "zarr_v3_zstd_pipeline_cf",
            "nested/zstd_pipeline",
            true,
        );
        assert_v3_zstd_cf_aggregate("zarr_v3_zstd_pipeline_cf");

        Spi::connect(|c| {
            let probes = c
                .select(
                    r#"SELECT time, y, x, value
                         FROM zarr_v3_zstd_pipeline
                        WHERE (time, y, x) IN (
                          ('1970-01-01 00:00:00+00'::timestamptz, 20, 110),
                          ('1970-01-01 00:00:00+00'::timestamptz, 50, 130),
                          ('1970-01-01 00:00:03.6+00'::timestamptz, 20, 110),
                          ('1970-01-01 00:00:03.6+00'::timestamptz, 50, 150)
                        )
                        ORDER BY time, y, x"#,
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| row.get_by_name::<f32, _>("value").unwrap().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(probes, vec![11.0, 43.0, 111.0, -7.5]);

            let fill_count = c
                .select(
                    "SELECT count(*) FROM zarr_v3_zstd_pipeline WHERE value = -7.5",
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<i64>(1)
                .unwrap();
            assert_eq!(fill_count, Some(8));

            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_v3_zstd_pipeline
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x = 110"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(
                has("Zarr Codec: transpose -> bytes -> zstd -> crc32c"),
                "plan: {plan:?}"
            );
            assert!(has("Zarr Chunks Selected: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Requested: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Present: 1"), "plan: {plan:?}");
            assert!(has("Zarr Data GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 82 bytes"), "plan: {plan:?}");
            assert!(has("Zarr Data Decoded Bytes: 96 bytes"), "plan: {plan:?}");
        });
    }

    #[pg_test]
    fn zarr_minio_v3_zstd_coordinate_decode_and_pruning_e2e() {
        create_minio_v3_e2e_server();
        Spi::run(
            r#"CREATE FOREIGN TABLE zarr_v3_zstd_coordinate (
                 zstd_x double precision,
                 value real
               )
               SERVER zarr_v3_e2e_server
               OPTIONS (array_group 'nested/zstd_coord_values')"#,
        )
        .unwrap();

        let aggregate_sql = r#"SELECT count(*) AS value_count,
                                      sum(value) AS value_sum,
                                      avg(value) AS value_avg,
                                      min(value) AS value_min,
                                      max(value) AS value_max
                                 FROM zarr_v3_zstd_coordinate
                                WHERE zstd_x BETWEEN 110 AND 140"#;
        assert_aggregate_pushed_down(aggregate_sql);

        Spi::connect(|c| {
            let values = c
                .select(
                    r#"SELECT zstd_x, value
                         FROM zarr_v3_zstd_coordinate
                        WHERE zstd_x BETWEEN 110 AND 140
                        ORDER BY zstd_x"#,
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| {
                    (
                        row.get_by_name::<f64, _>("zstd_x").unwrap().unwrap(),
                        row.get_by_name::<f32, _>("value").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                values,
                vec![(110.0, 1.0), (120.0, 2.0), (130.0, 3.0), (140.0, 4.0)]
            );

            let aggregate = c.select(aggregate_sql, None, &[]).unwrap().next().unwrap();
            assert_eq!(
                aggregate
                    .get_by_name::<i64, _>("value_count")
                    .unwrap()
                    .unwrap(),
                4
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f32, _>("value_sum")
                    .unwrap()
                    .unwrap(),
                10.0
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f64, _>("value_avg")
                    .unwrap()
                    .unwrap(),
                2.5
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f32, _>("value_min")
                    .unwrap()
                    .unwrap(),
                1.0
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f32, _>("value_max")
                    .unwrap()
                    .unwrap(),
                4.0
            );

            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_v3_zstd_coordinate
                        WHERE zstd_x = 150"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Chunks Selected: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Coordinate-Pruned: 1"), "plan: {plan:?}");
            assert!(has("Zarr Coordinate GET Calls: 2"), "plan: {plan:?}");
            assert!(
                has("Zarr Coordinate Encoded Bytes: 70 bytes"),
                "plan: {plan:?}"
            );
            assert!(
                has("Zarr Coordinate Decoded Bytes: 64 bytes"),
                "plan: {plan:?}"
            );
            assert!(has("Zarr Data GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 16 bytes"), "plan: {plan:?}");
        });
    }

    #[pg_test]
    fn zarr_minio_v3_zstd_sharded_inner_e2e() {
        create_minio_v3_e2e_server();
        create_minio_v3_e2e_table_on_server("zarr_v3_shard_zstd", "nested/shard_zstd", false);
        create_minio_v3_e2e_table_on_server("zarr_v3_shard_zstd_cf", "nested/shard_zstd", true);
        assert_v3_zstd_cf_aggregate("zarr_v3_shard_zstd_cf");

        Spi::connect(|c| {
            let values = c
                .select(
                    r#"SELECT x, value
                         FROM zarr_v3_shard_zstd
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x BETWEEN 110 AND 130
                        ORDER BY x"#,
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| {
                    (
                        row.get_by_name::<f64, _>("x").unwrap().unwrap(),
                        row.get_by_name::<f32, _>("value").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(values, vec![(110.0, 11.0), (120.0, 12.0), (130.0, 13.0)]);

            let sparse_fill = c
                .select(
                    r#"SELECT value
                         FROM zarr_v3_shard_zstd
                        WHERE time = '1970-01-01 00:00:03.6+00'::timestamptz
                          AND y = 50
                          AND x = 150"#,
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<f32, _>("value")
                .unwrap();
            assert_eq!(sparse_fill, Some(-7.5));

            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_v3_shard_zstd
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x BETWEEN 110 AND 130"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Codec: bytes -> zstd"), "plan: {plan:?}");
            assert!(
                has("Zarr Storage Layout: sharding_indexed (index: end)"),
                "plan: {plan:?}"
            );
            assert!(has("Zarr Shard Shape: [2, 3, 4]"), "plan: {plan:?}");
            assert!(has("Zarr Chunk Shape: [2, 3, 4]"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Selected: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Requested: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Present: 1"), "plan: {plan:?}");
            assert!(has("Zarr Data GET Calls: 2"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 98 bytes"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Shard Payload GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index Cache Hits: 0"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index Cache Misses: 1"), "plan: {plan:?}");
            assert!(
                has("Zarr Shard Index Encoded Bytes: 20 bytes"),
                "plan: {plan:?}"
            );
            assert!(
                has("Zarr Shard Payload Encoded Bytes: 78 bytes"),
                "plan: {plan:?}"
            );
        });
    }

    #[pg_test]
    fn zarr_minio_v3_zstd_frame_policy_fails_closed_e2e() {
        create_minio_v3_e2e_server();
        Spi::run(
            r#"CREATE FOREIGN TABLE zarr_v3_bad_zstd (
                 failure_case double precision,
                 value real
               )
               SERVER zarr_v3_e2e_server
               OPTIONS (array_group 'nested/zstd_bad/values')"#,
        )
        .unwrap();

        for (failure_case, key, reason) in [
            (
                0,
                "nested/zstd_bad/values/c/0",
                "failed to decode Zstandard frame",
            ),
            (
                1,
                "nested/zstd_bad/values/c/1",
                "Zstandard frame window 16777216 exceeds the 8388608-byte limit",
            ),
            (
                2,
                "nested/zstd_bad/values/c/2",
                "Zstandard dictionaries are not supported",
            ),
        ] {
            let message = capture_query_error(&format!(
                "SELECT value FROM zarr_v3_bad_zstd WHERE failure_case = {failure_case}"
            ));
            assert!(message.contains(key), "message: {message}");
            assert!(
                message.contains("codec index 1 ('zstd')"),
                "message: {message}"
            );
            assert!(message.contains(reason), "message: {message}");
        }
    }

    #[pg_test]
    fn zarr_minio_v3_blosc_direct_scan_and_aggregate_e2e() {
        create_minio_v3_e2e_server();
        create_minio_v3_e2e_table_on_server("zarr_v3_blosc_direct", "nested/blosc_v3", false);
        create_minio_v3_e2e_table_on_server("zarr_v3_blosc_direct_cf", "nested/blosc_v3", true);

        let raw_sql = r#"SELECT count(*) AS total_count,
                                count(value) AS value_count,
                                sum(value)::double precision AS value_sum,
                                avg(value) AS value_avg,
                                min(value)::double precision AS value_min,
                                max(value)::double precision AS value_max
                           FROM zarr_v3_blosc_direct"#;
        let decoded_sql = r#"SELECT count(*) AS total_count,
                                    count(value) AS value_count,
                                    sum(value) AS value_sum,
                                    avg(value) AS value_avg,
                                    min(value) AS value_min,
                                    max(value) AS value_max
                               FROM zarr_v3_blosc_direct_cf"#;
        assert_aggregate_pushed_down(decoded_sql);

        Spi::connect(|c| {
            let raw = c.select(raw_sql, None, &[]).unwrap().next().unwrap();
            assert_eq!(
                raw.get_by_name::<i64, _>("total_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                raw.get_by_name::<i64, _>("value_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                raw.get_by_name::<f64, _>("value_sum").unwrap().unwrap(),
                3_574.0
            );
            assert!(
                (raw.get_by_name::<f64, _>("value_avg").unwrap().unwrap() - 59.566_666_666_666_67)
                    .abs()
                    < 1e-12
            );
            assert_eq!(
                raw.get_by_name::<f64, _>("value_min").unwrap().unwrap(),
                -7.5
            );
            assert_eq!(
                raw.get_by_name::<f64, _>("value_max").unwrap().unwrap(),
                143.0
            );
            let fill_count = c
                .select(
                    "SELECT count(*) FROM zarr_v3_blosc_direct WHERE value = -7.5",
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get::<i64>(1)
                .unwrap();
            assert_eq!(fill_count, Some(8));

            let decoded = c.select(decoded_sql, None, &[]).unwrap().next().unwrap();
            assert_eq!(
                decoded
                    .get_by_name::<i64, _>("total_count")
                    .unwrap()
                    .unwrap(),
                60
            );
            assert_eq!(
                decoded
                    .get_by_name::<i64, _>("value_count")
                    .unwrap()
                    .unwrap(),
                48
            );
            assert!(
                (decoded.get_by_name::<f64, _>("value_sum").unwrap().unwrap() - 13_142.86).abs()
                    < 1e-8
            );
            assert!(
                (decoded.get_by_name::<f64, _>("value_avg").unwrap().unwrap()
                    - 273.809_583_333_333_36)
                    .abs()
                    < 1e-10
            );
            assert!(
                (decoded.get_by_name::<f64, _>("value_min").unwrap().unwrap() - 273.15).abs()
                    < 1e-10
            );
            assert!(
                (decoded.get_by_name::<f64, _>("value_max").unwrap().unwrap() - 274.55).abs()
                    < 1e-10
            );

            let probes = c
                .select(
                    r#"SELECT time, y, x, value
                         FROM zarr_v3_blosc_direct
                        WHERE (time, y, x) IN (
                          ('1970-01-01 00:00:00+00'::timestamptz, 20, 110),
                          ('1970-01-01 00:00:00+00'::timestamptz, 50, 130),
                          ('1970-01-01 00:00:03.6+00'::timestamptz, 20, 110),
                          ('1970-01-01 00:00:03.6+00'::timestamptz, 50, 150)
                        )
                        ORDER BY time, y, x"#,
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| row.get_by_name::<f32, _>("value").unwrap().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(probes, vec![11.0, 43.0, 111.0, -7.5]);

            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_v3_blosc_direct
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x = 110"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Codec: bytes -> blosc"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Selected: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Requested: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Present: 1"), "plan: {plan:?}");
            assert!(has("Zarr Data GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 112 bytes"), "plan: {plan:?}");
            assert!(has("Zarr Data Decoded Bytes: 96 bytes"), "plan: {plan:?}");
        });
    }

    #[pg_test]
    fn zarr_minio_v3_blosc_coordinate_decode_and_pruning_e2e() {
        create_minio_v3_e2e_server();
        Spi::run(
            r#"CREATE FOREIGN TABLE zarr_v3_blosc_coordinate (
                 blosc_x double precision,
                 value real
               )
               SERVER zarr_v3_e2e_server
               OPTIONS (array_group 'nested/blosc_coord_values')"#,
        )
        .unwrap();

        let aggregate_sql = r#"SELECT count(*) AS value_count,
                                      sum(value) AS value_sum,
                                      avg(value) AS value_avg,
                                      min(value) AS value_min,
                                      max(value) AS value_max
                                 FROM zarr_v3_blosc_coordinate
                                WHERE blosc_x BETWEEN 110 AND 140"#;
        assert_aggregate_pushed_down(aggregate_sql);

        Spi::connect(|c| {
            let values = c
                .select(
                    r#"SELECT blosc_x, value
                         FROM zarr_v3_blosc_coordinate
                        WHERE blosc_x BETWEEN 110 AND 140
                        ORDER BY blosc_x"#,
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| {
                    (
                        row.get_by_name::<f64, _>("blosc_x").unwrap().unwrap(),
                        row.get_by_name::<f32, _>("value").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                values,
                vec![(110.0, 1.0), (120.0, 2.0), (130.0, 3.0), (140.0, 4.0)]
            );

            let aggregate = c.select(aggregate_sql, None, &[]).unwrap().next().unwrap();
            assert_eq!(
                aggregate
                    .get_by_name::<i64, _>("value_count")
                    .unwrap()
                    .unwrap(),
                4
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f32, _>("value_sum")
                    .unwrap()
                    .unwrap(),
                10.0
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f64, _>("value_avg")
                    .unwrap()
                    .unwrap(),
                2.5
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f32, _>("value_min")
                    .unwrap()
                    .unwrap(),
                1.0
            );
            assert_eq!(
                aggregate
                    .get_by_name::<f32, _>("value_max")
                    .unwrap()
                    .unwrap(),
                4.0
            );

            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_v3_blosc_coordinate
                        WHERE blosc_x = 150"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Chunks Selected: 1"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Coordinate-Pruned: 1"), "plan: {plan:?}");
            assert!(has("Zarr Coordinate GET Calls: 2"), "plan: {plan:?}");
            assert!(
                has("Zarr Coordinate Encoded Bytes: 96 bytes"),
                "plan: {plan:?}"
            );
            assert!(
                has("Zarr Coordinate Decoded Bytes: 64 bytes"),
                "plan: {plan:?}"
            );
            assert!(has("Zarr Data GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 16 bytes"), "plan: {plan:?}");
        });
    }

    #[pg_test]
    fn zarr_minio_v3_blosc_sharded_inner_e2e() {
        create_minio_v3_e2e_server();
        create_minio_v3_e2e_table_on_server("zarr_v3_shard_blosc", "nested/shard_blosc", false);
        create_minio_v3_e2e_table_on_server("zarr_v3_shard_blosc_cf", "nested/shard_blosc", true);

        let aggregate_sql = r#"SELECT count(*) AS total_count,
                                      count(value) AS value_count,
                                      sum(value) AS value_sum,
                                      avg(value) AS value_avg,
                                      min(value) AS value_min,
                                      max(value) AS value_max
                                 FROM zarr_v3_shard_blosc_cf"#;
        assert_aggregate_pushed_down(aggregate_sql);

        Spi::connect(|c| {
            let values = c
                .select(
                    r#"SELECT x, value
                         FROM zarr_v3_shard_blosc
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x BETWEEN 110 AND 130
                        ORDER BY x"#,
                    None,
                    &[],
                )
                .unwrap()
                .map(|row| {
                    (
                        row.get_by_name::<f64, _>("x").unwrap().unwrap(),
                        row.get_by_name::<f32, _>("value").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(values, vec![(110.0, 11.0), (120.0, 12.0), (130.0, 13.0)]);

            let sparse_fill = c
                .select(
                    r#"SELECT value
                         FROM zarr_v3_shard_blosc
                        WHERE time = '1970-01-01 00:00:03.6+00'::timestamptz
                          AND y = 50
                          AND x = 150"#,
                    Some(1),
                    &[],
                )
                .unwrap()
                .next()
                .unwrap()
                .get_by_name::<f32, _>("value")
                .unwrap();
            assert_eq!(sparse_fill, Some(-7.5));

            let aggregate = c.select(aggregate_sql, None, &[]).unwrap().next().unwrap();
            assert_eq!(
                aggregate
                    .get_by_name::<i64, _>("total_count")
                    .unwrap()
                    .unwrap(),
                60
            );
            assert_eq!(
                aggregate
                    .get_by_name::<i64, _>("value_count")
                    .unwrap()
                    .unwrap(),
                48
            );
            assert!(
                (aggregate
                    .get_by_name::<f64, _>("value_sum")
                    .unwrap()
                    .unwrap()
                    - 13_142.86)
                    .abs()
                    < 1e-8
            );
            assert!(
                (aggregate
                    .get_by_name::<f64, _>("value_avg")
                    .unwrap()
                    .unwrap()
                    - 273.809_583_333_333_36)
                    .abs()
                    < 1e-10
            );
            assert!(
                (aggregate
                    .get_by_name::<f64, _>("value_min")
                    .unwrap()
                    .unwrap()
                    - 273.15)
                    .abs()
                    < 1e-10
            );
            assert!(
                (aggregate
                    .get_by_name::<f64, _>("value_max")
                    .unwrap()
                    .unwrap()
                    - 274.55)
                    .abs()
                    < 1e-10
            );

            let plan = c
                .select(
                    r#"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                       SELECT value
                         FROM zarr_v3_shard_blosc
                        WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                          AND y = 20
                          AND x BETWEEN 110 AND 130"#,
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|row| row.get::<&str>(1).unwrap().map(str::to_string))
                .collect::<Vec<_>>();
            let has = |text: &str| plan.iter().any(|line| line.contains(text));
            assert!(has("Zarr Codec: bytes -> blosc"), "plan: {plan:?}");
            assert!(
                has("Zarr Storage Layout: sharding_indexed (index: end)"),
                "plan: {plan:?}"
            );
            assert!(has("Zarr Shard Shape: [2, 3, 4]"), "plan: {plan:?}");
            assert!(has("Zarr Chunk Shape: [1, 3, 2]"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Selected: 2"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Requested: 2"), "plan: {plan:?}");
            assert!(has("Zarr Chunks Present: 2"), "plan: {plan:?}");
            assert!(has("Zarr Data GET Calls: 3"), "plan: {plan:?}");
            assert!(has("Zarr Data Encoded Bytes: 148 bytes"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index GET Calls: 1"), "plan: {plan:?}");
            assert!(has("Zarr Shard Payload GET Calls: 2"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index Cache Hits: 1"), "plan: {plan:?}");
            assert!(has("Zarr Shard Index Cache Misses: 1"), "plan: {plan:?}");
            assert!(
                has("Zarr Shard Index Encoded Bytes: 68 bytes"),
                "plan: {plan:?}"
            );
            assert!(
                has("Zarr Shard Payload Encoded Bytes: 80 bytes"),
                "plan: {plan:?}"
            );
        });
    }

    #[pg_test]
    fn zarr_minio_v3_blosc_corrupt_chunks_fail_closed_e2e() {
        create_minio_v3_e2e_server();
        create_minio_v3_e2e_table_on_server("zarr_v3_bad_blosc", "nested/bad_blosc", false);

        for (x, key, reason) in [
            (
                110,
                "nested/bad_blosc/c/0/0/0",
                "encoded chunk is shorter than the 16-byte Blosc header",
            ),
            (
                150,
                "nested/bad_blosc/c/0/0/1",
                "Blosc header declares 100 uncompressed bytes, expected exactly 96",
            ),
        ] {
            let message = capture_query_error(&format!(
                r#"SELECT value
                     FROM zarr_v3_bad_blosc
                    WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                      AND y = 20
                      AND x = {x}"#
            ));
            assert!(message.contains(key), "message: {message}");
            assert!(
                message.contains("codec index 1 ('blosc')"),
                "message: {message}"
            );
            assert!(message.contains(reason), "message: {message}");
        }
    }

    #[pg_test]
    fn zarr_minio_v3_crc32c_corruption_fails_closed_e2e() {
        create_minio_v3_e2e_table("zarr_v3_bad_crc", "nested/bad_crc", false);
        let message = capture_query_error(
            r#"SELECT value
                 FROM zarr_v3_bad_crc
                WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                  AND y = 20
                  AND x = 110"#,
        );
        assert!(
            message.contains("nested/bad_crc/c/0/0/0"),
            "message: {message}"
        );
        assert!(
            message.contains("codec index 3 ('crc32c')"),
            "message: {message}"
        );
        assert!(
            message.to_ascii_lowercase().contains("checksum mismatch"),
            "message: {message}"
        );
    }

    #[pg_test]
    fn zarr_minio_v3_truncated_gzip_fails_closed_e2e() {
        create_minio_v3_e2e_table("zarr_v3_bad_gzip", "nested/bad_gzip", false);
        let message = capture_query_error(
            r#"SELECT value
                 FROM zarr_v3_bad_gzip
                WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                  AND y = 20
                  AND x = 110"#,
        );
        assert!(
            message.contains("nested/bad_gzip/c/0/0/0"),
            "message: {message}"
        );
        assert!(
            message.contains("codec index 2 ('gzip')"),
            "message: {message}"
        );
    }

    #[pg_test]
    fn zarr_minio_v3_overexpanding_gzip_fails_closed_e2e() {
        create_minio_v3_e2e_table("zarr_v3_oversize", "nested/oversize", false);
        let message = capture_query_error(
            r#"SELECT value
                 FROM zarr_v3_oversize
                WHERE time = '1970-01-01 00:00:00+00'::timestamptz
                  AND y = 20
                  AND x = 110"#,
        );
        assert!(
            message.contains("nested/oversize/c/0/0/0"),
            "message: {message}"
        );
        assert!(
            message.contains("codec index 2 ('gzip')")
                && message.contains("decoded chunk has more than 96 bytes, expected exactly 96"),
            "message: {message}"
        );
    }

    #[pg_test]
    fn zarr_minio_v3_scalar_aggregate_pushdown_e2e() {
        create_minio_v3_e2e_table("zarr_v3_aggregate", "nested/raw_default", true);
        let sql = r#"SELECT count(*) AS total_count,
                            count(value) AS value_count,
                            sum(value) AS value_sum,
                            avg(value) AS value_avg,
                            min(value) AS value_min,
                            max(value) AS value_max
                       FROM zarr_v3_aggregate"#;
        assert_aggregate_pushed_down(sql);

        Spi::connect(|c| {
            let row = c.select(sql, None, &[]).unwrap().next().unwrap();
            assert_eq!(
                row.get_by_name::<i64, _>("total_count").unwrap().unwrap(),
                60
            );
            assert_eq!(
                row.get_by_name::<i64, _>("value_count").unwrap().unwrap(),
                48
            );
            let value_sum = row.get_by_name::<f64, _>("value_sum").unwrap().unwrap();
            let value_avg = row.get_by_name::<f64, _>("value_avg").unwrap().unwrap();
            let value_min = row.get_by_name::<f64, _>("value_min").unwrap().unwrap();
            let value_max = row.get_by_name::<f64, _>("value_max").unwrap().unwrap();
            assert!((value_sum - 13_142.86).abs() < 1e-8);
            assert!((value_avg - 273.809_583_333_333_36).abs() < 1e-10);
            assert!((value_min - 273.15).abs() < 1e-10);
            assert!((value_max - 274.55).abs() < 1e-10);
        });
    }

    #[pg_test]
    fn zarr_inspect_minio_v3_metadata_e2e() {
        create_minio_v3_e2e_server();

        Spi::connect(|c| {
            let paths = c
                .select(
                    "SELECT path FROM zarr_inspect('zarr_v3_e2e_server') ORDER BY path",
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
                    "nested/bad_blosc",
                    "nested/bad_crc",
                    "nested/bad_gzip",
                    "nested/blosc_coord_values",
                    "nested/blosc_v3",
                    "nested/blosc_x",
                    "nested/oversize",
                    "nested/pipeline",
                    "nested/raw_default",
                    "nested/raw_v2keys",
                    "nested/shard_bad_index_crc",
                    "nested/shard_blosc",
                    "nested/shard_end",
                    "nested/shard_half_sentinel",
                    "nested/shard_oob",
                    "nested/shard_sentinel",
                    "nested/shard_start",
                    "nested/shard_truncated_index",
                    "nested/shard_zstd",
                    "nested/time",
                    "nested/x",
                    "nested/y",
                    "nested/zstd_bad",
                    "nested/zstd_bad/failure_case",
                    "nested/zstd_bad/values",
                    "nested/zstd_coord_values",
                    "nested/zstd_pipeline",
                    "nested/zstd_x",
                ]
            );

            let root = c
                .select(
                    "SELECT kind, zarr_format, attributes, warnings FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = '/'",
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
                root.get_by_name::<i64, _>("zarr_format").unwrap().unwrap(),
                3
            );
            assert_eq!(
                root.get_by_name::<JsonB, _>("attributes")
                    .unwrap()
                    .unwrap()
                    .0["title"],
                serde_json::json!("Deterministic Zarr v3 inspection fixture")
            );
            assert_eq!(
                root.get_by_name::<Vec<String>, _>("warnings")
                    .unwrap()
                    .unwrap(),
                Vec::<String>::new()
            );

            let nested = c
                .select(
                    "SELECT kind, zarr_format, crs, warnings FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                nested.get_by_name::<String, _>("kind").unwrap().unwrap(),
                "group"
            );
            assert_eq!(
                nested
                    .get_by_name::<i64, _>("zarr_format")
                    .unwrap()
                    .unwrap(),
                3
            );
            let nested_crs = nested.get_by_name::<JsonB, _>("crs").unwrap().unwrap().0;
            assert_eq!(
                nested_crs["properties"]["name"],
                serde_json::json!("EPSG:3857")
            );
            assert_eq!(
                nested
                    .get_by_name::<Vec<String>, _>("warnings")
                    .unwrap()
                    .unwrap(),
                Vec::<String>::new()
            );

            let raw = c
                .select(
                    r#"SELECT kind, group_path, variable, zarr_format, shape,
                              dimensions, dtype, chunks, codecs, units,
                              fill_value, scale_factor, add_offset, attributes,
                              warnings
                         FROM zarr_inspect('zarr_v3_e2e_server')
                        WHERE path = 'nested/raw_default'"#,
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
                "raw_default"
            );
            assert_eq!(
                raw.get_by_name::<i64, _>("zarr_format").unwrap().unwrap(),
                3
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
                "float32"
            );
            assert_eq!(
                raw.get_by_name::<JsonB, _>("chunks").unwrap().unwrap().0,
                serde_json::json!([2, 3, 4])
            );
            assert_eq!(
                raw.get_by_name::<JsonB, _>("codecs").unwrap().unwrap().0,
                serde_json::json!([{
                    "name": "bytes",
                    "configuration": {"endian": "little"}
                }])
            );
            assert_eq!(
                raw.get_by_name::<JsonB, _>("fill_value")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!(-7.5)
            );
            assert_eq!(raw.get_by_name::<String, _>("units").unwrap().unwrap(), "K");
            assert_eq!(
                raw.get_by_name::<f64, _>("scale_factor").unwrap().unwrap(),
                0.01
            );
            assert_eq!(
                raw.get_by_name::<f64, _>("add_offset").unwrap().unwrap(),
                273.15
            );
            assert_eq!(
                raw.get_by_name::<JsonB, _>("attributes")
                    .unwrap()
                    .unwrap()
                    .0["missing_value"],
                serde_json::json!([42.0])
            );
            assert_eq!(
                raw.get_by_name::<Vec<String>, _>("warnings")
                    .unwrap()
                    .unwrap(),
                Vec::<String>::new()
            );

            let alternate = c
                .select(
                    "SELECT zarr_format, dimensions, dtype, codecs FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/raw_v2keys'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                alternate
                    .get_by_name::<i64, _>("zarr_format")
                    .unwrap()
                    .unwrap(),
                3
            );
            assert_eq!(
                alternate
                    .get_by_name::<Vec<String>, _>("dimensions")
                    .unwrap()
                    .unwrap(),
                vec!["time", "y", "x"]
            );
            assert_eq!(
                alternate
                    .get_by_name::<String, _>("dtype")
                    .unwrap()
                    .unwrap(),
                "float32"
            );
            assert_eq!(
                alternate
                    .get_by_name::<JsonB, _>("codecs")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([{
                    "name": "bytes",
                    "configuration": {"endian": "little"}
                }])
            );

            let pipeline = c
                .select(
                    "SELECT codecs FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/pipeline'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                pipeline
                    .get_by_name::<JsonB, _>("codecs")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([
                    {"name": "transpose", "configuration": {"order": [2, 1, 0]}},
                    {"name": "bytes", "configuration": {"endian": "little"}},
                    {"name": "gzip", "configuration": {"level": 1}},
                    {"name": "crc32c"}
                ])
            );

            let zstd_pipeline = c
                .select(
                    "SELECT chunks, codecs FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/zstd_pipeline'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                zstd_pipeline
                    .get_by_name::<JsonB, _>("chunks")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([2, 3, 4])
            );
            assert_eq!(
                zstd_pipeline
                    .get_by_name::<JsonB, _>("codecs")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([
                    {"name": "transpose", "configuration": {"order": [2, 1, 0]}},
                    {"name": "bytes", "configuration": {"endian": "little"}},
                    {"name": "zstd", "configuration": {"level": 1, "checksum": true}},
                    {"name": "crc32c"}
                ])
            );

            let zstd_coordinate = c
                .select(
                    "SELECT dimensions, dtype, chunks, codecs FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/zstd_x'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                zstd_coordinate
                    .get_by_name::<Vec<String>, _>("dimensions")
                    .unwrap()
                    .unwrap(),
                vec!["zstd_x"]
            );
            assert_eq!(
                zstd_coordinate
                    .get_by_name::<String, _>("dtype")
                    .unwrap()
                    .unwrap(),
                "float64"
            );
            assert_eq!(
                zstd_coordinate
                    .get_by_name::<JsonB, _>("chunks")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([4])
            );
            assert_eq!(
                zstd_coordinate
                    .get_by_name::<JsonB, _>("codecs")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([
                    {"name": "bytes", "configuration": {"endian": "little"}},
                    {"name": "zstd", "configuration": {"level": 1, "checksum": false}}
                ])
            );

            let blosc = c
                .select(
                    "SELECT chunks, codecs FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/blosc_v3'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                blosc.get_by_name::<JsonB, _>("chunks").unwrap().unwrap().0,
                serde_json::json!([2, 3, 4])
            );
            assert_eq!(
                blosc.get_by_name::<JsonB, _>("codecs").unwrap().unwrap().0,
                serde_json::json!([
                    {"name": "bytes", "configuration": {"endian": "little"}},
                    {
                        "name": "blosc",
                        "configuration": {
                            "typesize": 4,
                            "cname": "lz4",
                            "clevel": 5,
                            "shuffle": "shuffle",
                            "blocksize": 0
                        }
                    }
                ])
            );

            let blosc_coordinate = c
                .select(
                    "SELECT dimensions, dtype, chunks, codecs FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/blosc_x'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                blosc_coordinate
                    .get_by_name::<Vec<String>, _>("dimensions")
                    .unwrap()
                    .unwrap(),
                vec!["blosc_x"]
            );
            assert_eq!(
                blosc_coordinate
                    .get_by_name::<String, _>("dtype")
                    .unwrap()
                    .unwrap(),
                "float64"
            );
            assert_eq!(
                blosc_coordinate
                    .get_by_name::<JsonB, _>("chunks")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([4])
            );
            assert_eq!(
                blosc_coordinate
                    .get_by_name::<JsonB, _>("codecs")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([
                    {"name": "bytes", "configuration": {"endian": "little"}},
                    {
                        "name": "blosc",
                        "configuration": {
                            "typesize": 8,
                            "cname": "lz4",
                            "clevel": 5,
                            "shuffle": "shuffle",
                            "blocksize": 0
                        }
                    }
                ])
            );

            let sharded = c
                .select(
                    "SELECT chunks, codecs FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/shard_end'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                sharded
                    .get_by_name::<JsonB, _>("chunks")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([2, 3, 4])
            );
            assert_eq!(
                sharded
                    .get_by_name::<JsonB, _>("codecs")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([{
                    "name": "sharding_indexed",
                    "configuration": {
                        "chunk_shape": [1, 3, 2],
                        "codecs": [{
                            "name": "bytes",
                            "configuration": {"endian": "little"}
                        }],
                        "index_codecs": [
                            {
                                "name": "bytes",
                                "configuration": {"endian": "little"}
                            },
                            {"name": "crc32c"}
                        ],
                        "index_location": "end"
                    }
                }])
            );

            let sharded_blosc = c
                .select(
                    "SELECT chunks, codecs FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/shard_blosc'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                sharded_blosc
                    .get_by_name::<JsonB, _>("chunks")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([2, 3, 4])
            );
            assert_eq!(
                sharded_blosc
                    .get_by_name::<JsonB, _>("codecs")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([{
                    "name": "sharding_indexed",
                    "configuration": {
                        "chunk_shape": [1, 3, 2],
                        "codecs": [
                            {
                                "name": "bytes",
                                "configuration": {"endian": "little"}
                            },
                            {
                                "name": "blosc",
                                "configuration": {
                                    "typesize": 4,
                                    "cname": "lz4",
                                    "clevel": 5,
                                    "shuffle": "shuffle",
                                    "blocksize": 0
                                }
                            }
                        ],
                        "index_codecs": [
                            {
                                "name": "bytes",
                                "configuration": {"endian": "little"}
                            },
                            {"name": "crc32c"}
                        ],
                        "index_location": "end"
                    }
                }])
            );

            let sharded_zstd = c
                .select(
                    "SELECT chunks, codecs FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/shard_zstd'",
                    None,
                    &[],
                )
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(
                sharded_zstd
                    .get_by_name::<JsonB, _>("chunks")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([2, 3, 4])
            );
            assert_eq!(
                sharded_zstd
                    .get_by_name::<JsonB, _>("codecs")
                    .unwrap()
                    .unwrap()
                    .0,
                serde_json::json!([{
                    "name": "sharding_indexed",
                    "configuration": {
                        "chunk_shape": [2, 3, 4],
                        "codecs": [
                            {
                                "name": "bytes",
                                "configuration": {"endian": "little"}
                            },
                            {
                                "name": "zstd",
                                "configuration": {"level": 1, "checksum": true}
                            }
                        ],
                        "index_codecs": [
                            {
                                "name": "bytes",
                                "configuration": {"endian": "little"}
                            },
                            {"name": "crc32c"}
                        ],
                        "index_location": "end"
                    }
                }])
            );

            let time = c
                .select(
                    "SELECT dimensions, units, calendar FROM zarr_inspect('zarr_v3_e2e_server') WHERE path = 'nested/time'",
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
                    "nested/spatial2d",
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
