#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use clickhouse_rs as ch;
    use pgrx::prelude::*;
    use pgrx::{pg_test, IntoDatum};
    use supabase_wrappers::prelude::create_async_runtime;

    #[pg_test]
    fn clickhouse_smoketest() {
        Spi::connect(|mut c| {
            let clickhouse_pool = ch::Pool::new("tcp://default:@localhost:9000/default");

            let rt = create_async_runtime().expect("failed to create runtime");
            let mut handle = rt
                .block_on(async { clickhouse_pool.get_handle().await })
                .expect("handle");

            rt.block_on(async {
                handle.execute("DROP TABLE IF EXISTS test_table").await?;
                handle
                    .execute("CREATE TABLE test_table (id INT, name TEXT) engine = Memory")
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
                           conn_string 'tcp://default:@localhost:9000/default'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_table (
                    id bigint,
                    name text
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
                    name text
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
                    _name text
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

            let remote_value: String = rt
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
            assert_eq!(remote_value, "test");
        });
    }
}
