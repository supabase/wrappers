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

    /// Seed a document with one field per BSON type variant exercised in
    /// `bson_to_cell` and assert every Postgres column reads back correctly.
    #[pg_test]
    fn mongodb_types_read() {
        use bson::spec::BinarySubtype;
        use std::str::FromStr;

        let rt = create_async_runtime().unwrap();

        rt.block_on(async {
            let client = Client::with_uri_str(MONGO_URI).await.unwrap();
            let db = client.database("testdb");
            db.collection::<Document>("types_read").drop().await.ok();
            let coll = db.collection::<Document>("types_read");

            let doc = doc! {
                "c_bool":        true,
                "c_i2":          12345i32,
                "c_i4":          12345i32,
                "c_i8_from_i32": 12345i32,
                "c_i8_from_i64": bson::Bson::Int64(9_000_000_000i64),
                "c_f8":          3.24f64,
                "c_f4":          1.25f64,           // exactly representable as f32
                "c_num_dec":     bson::Bson::Decimal128(bson::Decimal128::from_str("123.456").unwrap()),
                "c_num_dbl":     6.38f64,
                "c_num_i64":     bson::Bson::Int64(42i64),
                "c_str":         "hello",
                "c_oid":         bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap(),
                // 1_700_000_000_000 ms  →  2023-11-14 22:13:20 UTC (whole second)
                "c_dt":          bson::DateTime::from_millis(1_700_000_000_000i64),
                "c_dt_tz":       bson::DateTime::from_millis(1_700_000_000_000i64),
                "c_doc":         { "k": "v", "n": 7i32 },
                "c_arr":         [1i32, 2i32, 3i32],
                "c_bin":         bson::Bson::Binary(bson::Binary {
                                     subtype: BinarySubtype::Generic,
                                     bytes: vec![0x01u8, 0x02u8, 0xffu8],
                                 }),
            };

            coll.insert_one(doc).await.unwrap();
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
                r#"CREATE FOREIGN TABLE types_read (
                     c_bool          bool,
                     c_i2            int2,
                     c_i4            int4,
                     c_i8_from_i32   int8,
                     c_i8_from_i64   int8,
                     c_f8            float8,
                     c_f4            float4,
                     c_num_dec       numeric,
                     c_num_dbl       numeric,
                     c_num_i64       numeric,
                     c_str           text,
                     c_oid           text,
                     c_dt            timestamp,
                     c_dt_tz         timestamptz,
                     c_doc           jsonb,
                     c_arr           jsonb,
                     c_bin           bytea
                   )
                   SERVER mongo_server
                   OPTIONS (database 'testdb', collection 'types_read')"#,
                None,
                &[],
            )
            .unwrap();

            // bool
            let v: Option<bool> = c
                .select("SELECT c_bool FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(true));

            // int2 from BSON Int32
            let v: Option<i16> = c
                .select("SELECT c_i2 FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(12345i16));

            // int4 from BSON Int32
            let v: Option<i32> = c
                .select("SELECT c_i4 FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(12345i32));

            // int8 from BSON Int32
            let v: Option<i64> = c
                .select("SELECT c_i8_from_i32 FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(12345i64));

            // int8 from BSON Int64
            let v: Option<i64> = c
                .select("SELECT c_i8_from_i64 FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(9_000_000_000i64));

            // float8 from BSON Double
            let v: Option<f64> = c
                .select("SELECT c_f8 FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert!((v.unwrap() - 3.24f64).abs() < 1e-10, "c_f8 mismatch: {v:?}");

            // float4 from BSON Double (use a value exact in f32: 1.25)
            let v: Option<f32> = c
                .select("SELECT c_f4 FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(1.25f32));

            // numeric from Decimal128
            let v: Option<String> = c
                .select("SELECT c_num_dec::text FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v.as_deref(), Some("123.456"));

            // numeric from Double
            let v: Option<String> = c
                .select("SELECT c_num_dbl::text FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert!(
                v.as_deref().unwrap().starts_with("6.38"),
                "c_num_dbl unexpected: {v:?}"
            );

            // numeric from Int64
            let v: Option<String> = c
                .select("SELECT c_num_i64::text FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v.as_deref(), Some("42"));

            // text from BSON String
            let v: Option<String> = c
                .select("SELECT c_str FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v.as_deref(), Some("hello"));

            // text from BSON ObjectId
            let v: Option<String> = c
                .select("SELECT c_oid FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v.as_deref(), Some("507f1f77bcf86cd799439011"));

            // timestamp from BSON DateTime
            let v: Option<bool> = c
                .select(
                    "SELECT c_dt = '2023-11-14 22:13:20'::timestamp FROM types_read",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(true), "c_dt mismatch");

            // timestamptz from BSON DateTime
            let v: Option<bool> = c
                .select(
                    "SELECT c_dt_tz = '2023-11-14 22:13:20+00'::timestamptz FROM types_read",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(true), "c_dt_tz mismatch");

            // jsonb from BSON Document
            let v: Option<String> = c
                .select("SELECT c_doc->>'k' FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v.as_deref(), Some("v"));

            let v: Option<i32> = c
                .select("SELECT (c_doc->>'n')::int FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(7));

            // jsonb from BSON Array
            let v: Option<i32> = c
                .select(
                    "SELECT jsonb_array_length(c_arr) FROM types_read",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(3));

            // bytea from BSON Binary — compare as hex
            let v: Option<String> = c
                .select("SELECT encode(c_bin, 'hex') FROM types_read", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v.as_deref(), Some("0102ff"));
        });
    }

    /// Round-trip test: INSERT via SQL (exercises cell_to_bson) then SELECT
    /// back (exercises bson_to_cell) and assert every column matches.
    #[pg_test]
    fn mongodb_types_write() {
        let rt = create_async_runtime().unwrap();

        // Drop the collection for idempotency.
        rt.block_on(async {
            let client = Client::with_uri_str(MONGO_URI).await.unwrap();
            client
                .database("testdb")
                .collection::<Document>("types_write")
                .drop()
                .await
                .ok();
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
                r#"CREATE FOREIGN TABLE types_write (
                     _id     text,
                     c_bool  bool,
                     c_i2    int2,
                     c_i4    int4,
                     c_i8    int8,
                     c_f4    float4,
                     c_f8    float8,
                     c_num   numeric,
                     c_str   text,
                     c_dt    timestamp,
                     c_dt_tz timestamptz,
                     c_doc   jsonb,
                     c_arr   jsonb
                   )
                   SERVER mongo_server
                   OPTIONS (database 'testdb', collection 'types_write', rowid_column '_id')"#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"INSERT INTO types_write (
                     _id, c_bool, c_i2, c_i4, c_i8, c_f4, c_f8, c_num,
                     c_str, c_dt, c_dt_tz, c_doc, c_arr
                   ) VALUES (
                     'w1',
                     true,
                     12345::int2,
                     12345::int4,
                     9000000000::int8,
                     1.25::float4,
                     3.24::float8,
                     123.456::numeric,
                     'hello',
                     '2023-11-14 22:13:20'::timestamp,
                     '2023-11-14 22:13:20+00'::timestamptz,
                     '{"k":"v","n":7}'::jsonb,
                     '[1,2,3]'::jsonb
                   )"#,
                None,
                &[],
            )
            .unwrap();

            // bool round-trip
            let v: Option<bool> = c
                .select("SELECT c_bool FROM types_write WHERE _id = 'w1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(true));

            // int2 round-trip (cell_to_bson encodes I16 as BSON Int32)
            let v: Option<i16> = c
                .select("SELECT c_i2 FROM types_write WHERE _id = 'w1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(12345i16));

            // int4 round-trip
            let v: Option<i32> = c
                .select("SELECT c_i4 FROM types_write WHERE _id = 'w1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(12345i32));

            // int8 round-trip (cell_to_bson encodes I64 as BSON Int64)
            let v: Option<i64> = c
                .select("SELECT c_i8 FROM types_write WHERE _id = 'w1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(9_000_000_000i64));

            // float4 round-trip (cell_to_bson encodes F32 as BSON Double)
            let v: Option<f32> = c
                .select("SELECT c_f4 FROM types_write WHERE _id = 'w1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(1.25f32));

            // float8 round-trip
            let v: Option<f64> = c
                .select("SELECT c_f8 FROM types_write WHERE _id = 'w1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert!((v.unwrap() - 3.24f64).abs() < 1e-10, "c_f8 mismatch: {v:?}");

            // numeric round-trip (cell_to_bson encodes as Decimal128)
            let v: Option<String> = c
                .select(
                    "SELECT c_num::text FROM types_write WHERE _id = 'w1'",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v.as_deref(), Some("123.456"));

            // text round-trip (non-_id: stays BSON String)
            let v: Option<String> = c
                .select("SELECT c_str FROM types_write WHERE _id = 'w1'", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v.as_deref(), Some("hello"));

            // timestamp round-trip
            let v: Option<bool> = c
                .select(
                    "SELECT c_dt = '2023-11-14 22:13:20'::timestamp \
                     FROM types_write WHERE _id = 'w1'",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(true), "c_dt mismatch");

            // timestamptz round-trip
            let v: Option<bool> = c
                .select(
                    "SELECT c_dt_tz = '2023-11-14 22:13:20+00'::timestamptz \
                     FROM types_write WHERE _id = 'w1'",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(true), "c_dt_tz mismatch");

            // jsonb Document round-trip
            let v: Option<String> = c
                .select(
                    "SELECT c_doc->>'k' FROM types_write WHERE _id = 'w1'",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v.as_deref(), Some("v"));

            // jsonb Array round-trip
            let v: Option<i32> = c
                .select(
                    "SELECT jsonb_array_length(c_arr) FROM types_write WHERE _id = 'w1'",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(v, Some(3));
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
