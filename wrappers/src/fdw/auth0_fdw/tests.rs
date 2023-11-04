#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    use pgrx::{pg_test, JsonB};
    use reqwest::blocking::Client;
    use serde_json::json;

    #[pg_test]
    fn auth0_smoketest() {
        const COLLECTION_NAME: &str = "test_collection";
        // create_collection(COLLECTION_NAME);
        Spi::connect(|mut c| {
            c.update(
                r#"create foreign data wrapper qdrant_wrapper
                         handler qdrant_fdw_handler validator qdrant_fdw_validator"#,
                None,
                None,
            )
            .expect("Failed to create foreign data wrapper");
        });
    }
}
