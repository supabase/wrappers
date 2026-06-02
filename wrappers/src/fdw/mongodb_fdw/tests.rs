#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use bson::{Document, doc};
    use mongodb::Client;
    use pgrx::prelude::*;
    use supabase_wrappers::prelude::create_async_runtime;

    const MONGO_URI: &str = "mongodb://localhost:27017";

    /// Set up a named users-style collection, dropping it first for idempotency.
    /// Each test passes a unique collection name to avoid parallel-run collisions.
    fn setup_mongo_users(rt: &tokio::runtime::Runtime, coll_name: &str) {
        rt.block_on(async {
            let client = Client::with_uri_str(MONGO_URI).await.unwrap();
            let db = client.database("testdb");
            db.collection::<Document>(coll_name).drop().await.ok();
            let coll = db.collection::<Document>(coll_name);
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

    fn create_server_and_table(c: &mut pgrx::spi::SpiClient, coll_name: &str) {
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
            &format!(
                r#"CREATE FOREIGN TABLE users (
                    _id text,
                    name text,
                    age int,
                    active bool,
                    _doc jsonb
                  )
                  SERVER mongo_server
                  OPTIONS (database 'testdb', collection '{coll_name}', rowid_column '_id')"#
            ),
            None,
            &[],
        )
        .unwrap();
    }

    #[pg_test]
    fn mongodb_smoketest() {
        let rt = create_async_runtime().unwrap();
        setup_mongo_users(&rt, "users_smoketest");

        Spi::connect_mut(|c| {
            create_server_and_table(c, "users_smoketest");

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

    #[pg_test]
    fn mongodb_pushdown() {
        let rt = create_async_runtime().unwrap();
        setup_mongo_users(&rt, "users_pushdown");

        Spi::connect_mut(|c| {
            create_server_and_table(c, "users_pushdown");

            // =
            let r: Option<String> = c
                .select("SELECT name FROM users WHERE age = 30", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(r.as_deref(), Some("Alice"));

            // != (excludes u4 because its age is missing -> NULL is excluded by !=)
            let r: Option<i64> = c
                .select(
                    "SELECT count(*)::bigint FROM users WHERE age != 30",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(r, Some(2));

            // < / <= / > / >=
            for (op, expected) in [("<", 1), ("<=", 2), (">", 1), (">=", 2)] {
                let r: Option<i64> = c
                    .select(
                        &format!("SELECT count(*)::bigint FROM users WHERE age {op} 30"),
                        None,
                        &[],
                    )
                    .unwrap()
                    .first()
                    .get(1)
                    .unwrap();
                assert_eq!(r, Some(expected), "operator {op}");
            }

            // IN
            let r: Option<i64> = c
                .select(
                    "SELECT count(*)::bigint FROM users WHERE _id IN ('u1','u3')",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(r, Some(2));

            // IS NULL — u4 has no `age`
            let r: Option<String> = c
                .select("SELECT name FROM users WHERE age IS NULL", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(r.as_deref(), Some("Dave"));

            // IS NOT NULL
            let r: Option<i64> = c
                .select(
                    "SELECT count(*)::bigint FROM users WHERE age IS NOT NULL",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(r, Some(3));

            // ORDER BY + LIMIT
            let r: Option<String> = c
                .select(
                    "SELECT name FROM users WHERE age IS NOT NULL ORDER BY age DESC LIMIT 1",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(r.as_deref(), Some("Bob"));
        });
    }

    #[pg_test]
    fn mongodb_insert() {
        let rt = create_async_runtime().unwrap();
        setup_mongo_users(&rt, "users_insert");

        Spi::connect_mut(|c| {
            create_server_and_table(c, "users_insert");

            c.update(
                "INSERT INTO users (_id, name, age, active) VALUES ('u5', 'Eve', 35, true)",
                None,
                &[],
            )
            .unwrap();

            let r: Option<String> = c
                .select("SELECT name FROM users WHERE _id = 'u5'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(r.as_deref(), Some("Eve"));
        });
    }

    #[pg_test]
    fn mongodb_update() {
        let rt = create_async_runtime().unwrap();
        setup_mongo_users(&rt, "users_update");

        Spi::connect_mut(|c| {
            create_server_and_table(c, "users_update");

            c.update("UPDATE users SET age = 99 WHERE _id = 'u1'", None, &[])
                .unwrap();
            let r: Option<i32> = c
                .select("SELECT age FROM users WHERE _id = 'u1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(r, Some(99));

            // Setting a column to NULL should $unset the field.
            c.update("UPDATE users SET age = NULL WHERE _id = 'u1'", None, &[])
                .unwrap();
            let r: Option<i32> = c
                .select("SELECT age FROM users WHERE _id = 'u1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(r, None);
        });
    }

    #[pg_test]
    fn mongodb_delete() {
        let rt = create_async_runtime().unwrap();
        setup_mongo_users(&rt, "users_delete");

        Spi::connect_mut(|c| {
            create_server_and_table(c, "users_delete");

            c.update("DELETE FROM users WHERE _id = 'u2'", None, &[])
                .unwrap();
            let n: Option<i64> = c
                .select("SELECT count(*)::bigint FROM users", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(n, Some(3));
        });
    }

    #[pg_test]
    fn mongodb_doc_column() {
        let rt = create_async_runtime().unwrap();
        setup_mongo_users(&rt, "users_doc_column");

        Spi::connect_mut(|c| {
            create_server_and_table(c, "users_doc_column");

            let level: Option<i32> = c
                .select(
                    "SELECT (_doc -> 'profile' ->> 'level')::int FROM users WHERE _id = 'u1'",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(level, Some(5));

            let tag0: Option<String> = c
                .select(
                    "SELECT (_doc -> 'tags' ->> 0) FROM users WHERE _id = 'u2'",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(tag0.as_deref(), Some("user"));
        });
    }

    #[pg_test]
    fn mongodb_objectid_roundtrip() {
        let rt = create_async_runtime().unwrap();
        let hex_id = "507f1f77bcf86cd799439011";

        rt.block_on(async {
            let client = Client::with_uri_str(MONGO_URI).await.unwrap();
            let db = client.database("testdb");
            db.collection::<Document>("oid_items").drop().await.ok();
            let coll = db.collection::<Document>("oid_items");
            let oid = bson::oid::ObjectId::parse_str(hex_id).unwrap();
            coll.insert_one(doc! { "_id": oid, "label": "alpha" })
                .await
                .unwrap();
        });

        Spi::connect_mut(|c| {
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
                r#"CREATE FOREIGN TABLE oid_items (_id text, label text)
                   SERVER mongo_server
                   OPTIONS (database 'testdb', collection 'oid_items', rowid_column '_id')"#,
                None,
                &[],
            )
            .unwrap();

            // Read: ObjectId returns as 24-char hex string.
            let id: Option<String> = c
                .select("SELECT _id FROM oid_items LIMIT 1", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(id.as_deref(), Some(hex_id));

            // Qual: filter by hex value should match the ObjectId-typed document.
            let label: Option<String> = c
                .select(
                    &format!("SELECT label FROM oid_items WHERE _id = '{hex_id}'"),
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(label.as_deref(), Some("alpha"));
        });
    }
}
