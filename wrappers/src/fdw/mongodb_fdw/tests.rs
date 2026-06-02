#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use bson::{Document, doc};
    use mongodb::Client;
    use pgrx::prelude::*;
    use supabase_wrappers::prelude::create_async_runtime;

    const MONGO_URI: &str = "mongodb://localhost:27017";

    fn setup_mongo_users(rt: &tokio::runtime::Runtime) {
        rt.block_on(async {
            let client = Client::with_uri_str(MONGO_URI).await.unwrap();
            let db = client.database("testdb");
            db.collection::<Document>("users").drop().await.ok();
            let coll = db.collection::<Document>("users");
            coll.insert_many(vec![
                doc! { "_id": "u1", "name": "Alice", "age": 30, "active": true,
                       "tags": ["admin", "ops"], "profile": { "level": 5 } },
                doc! { "_id": "u2", "name": "Bob",   "age": 41, "active": false,
                       "tags": ["user"], "profile": { "level": 2 } },
                doc! { "_id": "u3", "name": "Carol", "age": 27, "active": true,
                       "tags": ["editor"], "profile": { "level": 3 } },
                doc! { "_id": "u4", "name": "Dave",  "active": true },
            ])
            .await
            .unwrap();
        });
    }

    fn create_server_and_table(c: &mut pgrx::spi::SpiClient) {
        c.update(
            r#"CREATE FOREIGN DATA WRAPPER mongodb_wrapper
                HANDLER mongodb_fdw_handler VALIDATOR mongodb_fdw_validator"#,
            None,
            &[],
        )
        .unwrap();
        c.update(
            r#"CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongodb_wrapper
               OPTIONS (conn_string 'mongodb://localhost:27017')"#,
            None,
            &[],
        )
        .unwrap();
        c.update(
            r#"CREATE FOREIGN TABLE users (
                _id text,
                name text,
                age int,
                active bool,
                _doc jsonb
              )
              SERVER mongo_server
              OPTIONS (database 'testdb', collection 'users', rowid_column '_id')"#,
            None,
            &[],
        )
        .unwrap();
    }

    #[pg_test]
    fn mongodb_smoketest() {
        let rt = create_async_runtime().unwrap();
        setup_mongo_users(&rt);

        Spi::connect_mut(|c| {
            create_server_and_table(c);

            let n: Option<i64> = c
                .select("SELECT count(*)::bigint FROM users", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(n, Some(4));

            let name: Option<String> = c
                .select("SELECT name FROM users WHERE _id = 'u1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(name.as_deref(), Some("Alice"));

            // Missing field on u4 → NULL
            let age: Option<i32> = c
                .select("SELECT age FROM users WHERE _id = 'u4'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(age, None);
        });
    }
}
