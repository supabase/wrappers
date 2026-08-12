#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;

    // Smoke test covering the DDL surface only. No scan is executed here:
    // hitting a real Zarr store needs a reachable S3 endpoint and a
    // pre-populated store (see .ci/docker-compose-native.yaml), which the
    // unit test environment does not guarantee.
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
}
