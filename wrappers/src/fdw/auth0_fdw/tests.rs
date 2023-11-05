#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;

    #[pg_test]
    fn auth0_smoketest() {
        const COLLECTION_NAME: &str = "test_collection";
        // create_collection(COLLECTION_NAME);
        Spi::connect(|mut c| {
            c.update(
                r#"create foreign data wrapper auth0_wrapper
                         handler auth0_fdw_handler validator auth0_fdw_validator"#,
                None,
                None,
            )
            .expect("Failed to create foreign data wrapper");
        });
    }
}
