#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use clickhouse_rs as ch;
    use pgrx::prelude::*;
    use pgrx::{datum::Timestamp, pg_test, IntoDatum, Uuid};
    use supabase_wrappers::prelude::create_async_runtime;

    #[pg_test]
    fn clickhouse_smoketest() {
        Spi::connect(|mut c| {
            let clickhouse_pool = ch::Pool::new("tcp://default:default@localhost:9000/default");

            let rt = create_async_runtime().expect("failed to create runtime");
            let mut handle = rt
                .block_on(async { clickhouse_pool.get_handle().await })
                .expect("handle");

            rt.block_on(async {
                handle.execute("DROP TABLE IF EXISTS test_table").await?;
                handle
                    .execute(
                        "CREATE TABLE test_table (
                            id Int64,
                            name Nullable(TEXT),
                            amt Nullable(Float64),
                            uid Nullable(UUID),
                            created_at DateTime('UTC')
                        ) engine = Memory",
                    )
                    .await
            })
            .expect("test_table in ClickHouse");

            c.update(
                r#"CREATE FOREIGN DATA WRAPPER clickhouse_wrapper
                         HANDLER click_house_fdw_handler VALIDATOR click_house_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_clickhouse_server
                         FOREIGN DATA WRAPPER clickhouse_wrapper
                         OPTIONS (
                           conn_string 'tcp://default:default@localhost:9000/default'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_table (
                    id bigint,
                    name text,
                    amt double precision,
                    uid uuid,
                    created_at timestamp
                  )
                  SERVER my_clickhouse_server
                  OPTIONS (
                    table 'test_table',
                    rowid_column 'id'
                  )
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_cust_sql (
                    id bigint,
                    name text,
                    amt double precision,
                    uid uuid,
                    created_at timestamp
                  )
                  SERVER my_clickhouse_server
                  OPTIONS (
                    table '(select * from test_table)'
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                  CREATE FOREIGN TABLE test_param_sql (
                    id bigint,
                    name text,
                    _name text,
                    amt double precision,
                    uid uuid,
                    created_at timestamp
                  )
                  SERVER my_clickhouse_server
                  OPTIONS (
                    table '(select *, name as _name from test_table where name = ${_name})',
                    rowid_column 'id'
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            assert_eq!(
                c.select("SELECT * FROM test_table", None, None)
                    .unwrap()
                    .len(),
                0
            );
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                Some(vec![(
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    "test".into_datum(),
                )]),
            )
            .unwrap();
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                Some(vec![(
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    "test2".into_datum(),
                )]),
            )
            .unwrap();
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                Some(vec![(
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    "test3".into_datum(),
                )]),
            )
            .unwrap();
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                Some(vec![(
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    "test4".into_datum(),
                )]),
            )
            .unwrap();
            c.update(
                "INSERT INTO test_table (id, name, amt, uid, created_at) VALUES ($1, $2, $3, $4, $5)",
                None,
                Some(vec![
                    (PgOid::BuiltIn(PgBuiltInOids::INT4OID), 42.into_datum()),
                    (
                        PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                        None::<String>.into_datum(),
                    ),
                    (
                        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID),
                        123.45.into_datum(),
                    ),
                    (
                        PgOid::BuiltIn(PgBuiltInOids::UUIDOID),
                        Uuid::from_bytes([42u8; 16]).into_datum(),
                    ),
                    (
                        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
                        Timestamp::new(2025, 5, 1, 2, 3, 4.0).into_datum(),
                    ),
                ]),
            )
            .unwrap();
            assert_eq!(
                c.select("SELECT name FROM test_table ORDER BY name", None, None)
                    .unwrap()
                    .first()
                    .get_one::<&str>()
                    .unwrap()
                    .unwrap(),
                "test"
            );
            assert_eq!(
                c.select(
                    "SELECT uid, amt, created_at FROM test_table WHERE id = 42",
                    None,
                    None
                )
                .unwrap()
                .first()
                .get_three::<Uuid, f64, Timestamp>()
                .unwrap(),
                (
                    Some(Uuid::from_bytes([42u8; 16])),
                    Some(123.45),
                    Some(Timestamp::new(2025, 5, 1, 2, 3, 4.0).unwrap())
                )
            );
            assert_eq!(
                c.select("SELECT name FROM test_cust_sql ORDER BY name", None, None)
                    .unwrap()
                    .first()
                    .get_one::<&str>()
                    .unwrap()
                    .unwrap(),
                "test"
            );

            assert_eq!(
                c.select(
                    "SELECT name FROM test_table WHERE name = $1",
                    None,
                    Some(vec![(
                        PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                        "test2".into_datum()
                    )])
                )
                .unwrap()
                .first()
                .get_one::<&str>()
                .unwrap()
                .unwrap(),
                "test2"
            );

            assert_eq!(
                c.select(
                    "SELECT name FROM test_cust_sql WHERE name = $1",
                    None,
                    Some(vec![(
                        PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                        "test2".into_datum()
                    )])
                )
                .unwrap()
                .first()
                .get_one::<&str>()
                .unwrap()
                .unwrap(),
                "test2"
            );

            assert_eq!(
                c.select(
                    "SELECT name FROM test_param_sql WHERE _name = $1",
                    None,
                    Some(vec![(
                        PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                        "test2".into_datum()
                    )])
                )
                .unwrap()
                .first()
                .get_one::<&str>()
                .unwrap()
                .unwrap(),
                "test2"
            );

            assert_eq!(
                c.select(
                    "SELECT name FROM test_table ORDER by name LIMIT 1 OFFSET 1",
                    None,
                    None
                )
                .unwrap()
                .first()
                .get_one::<&str>()
                .unwrap()
                .unwrap(),
                "test2"
            );
            assert_eq!(
                c.select(
                    "SELECT name FROM test_cust_sql ORDER BY name LIMIT 2 OFFSET 2",
                    None,
                    None
                )
                .unwrap()
                .first()
                .get_one::<&str>()
                .unwrap()
                .unwrap(),
                "test3"
            );

            let remote_value: Option<String> = rt
                .block_on(async {
                    handle
                        .query("SELECT name FROM test_table ORDER BY name LIMIT 1")
                        .fetch_all()
                        .await?
                        .rows()
                        .last()
                        .unwrap()
                        .get("name")
                })
                .expect("value");
            assert_eq!(remote_value, Some("test".to_string()));
        });
    }
}
