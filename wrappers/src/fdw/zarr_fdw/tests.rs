#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;

    fn create_minio_e2e_table(table: &str, array_group: &str, value_type: &str) {
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
                         array_group '{array_group}',
                         time_unit 'seconds',
                         time_origin 'unix'
                       )"#
                ),
                None,
                &[],
            )
            .unwrap();
        });
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
        error = "invalid value for option 'fortnights': must be one of: seconds, milliseconds, nanoseconds, hours, days"
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

    #[pg_test(
        error = "column 'x' has incompatible PostgreSQL type OID 23; expected double precision (OID 701)"
    )]
    fn zarr_scan_rejects_qual_only_coordinate_type_before_s3() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER zarr_wrapper
                     HANDLER zarr_fdw_handler VALIDATOR zarr_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER zarr_type_server
                     FOREIGN DATA WRAPPER zarr_wrapper
                     OPTIONS (
                       store_url 's3://zarr-test/does-not-exist.zarr',
                       anonymous 'true'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FOREIGN TABLE zarr_bad_coordinate_type (
                       x integer,
                       y double precision,
                       value real
                     )
                     SERVER zarr_type_server
                     OPTIONS (array_group 'value')"#,
                None,
                &[],
            )
            .unwrap();
            c.select(
                "SELECT y FROM zarr_bad_coordinate_type WHERE x = 1",
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
    fn zarr_minio_blosc_scan_e2e() {
        create_minio_e2e_table("zarr_e2e_blosc", "nested/blosc", "real");

        Spi::connect_mut(|c| {
            let summary = c
                .select(
                    r#"SELECT count(value) AS row_count,
                              sum(value)::double precision AS value_sum
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
