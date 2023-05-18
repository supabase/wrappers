#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use clickhouse_rs as ch;
    use pgx::prelude::*;
    use pgx::{pg_test, IntoDatum};
    use supabase_wrappers::prelude::create_async_runtime;

    #[pg_test]
    fn clickhouse_smoketest() {
        Spi::execute(|c| {
            let clickhouse_pool = ch::Pool::new("tcp://default:@localhost:9000/supa");

            let rt = create_async_runtime();
            let mut handle = rt
                .block_on(async { clickhouse_pool.get_handle().await })
                .expect("handle");

            rt.block_on(async {
                handle
                    .execute("DROP TABLE IF EXISTS supa.test_table")
                    .await?;
                handle
                    .execute("CREATE TABLE supa.test_table (id INT, name TEXT) engine = Memory")
                    .await
            })
            .expect("test_table in ClickHouse");

            c.update(
                r#"CREATE FOREIGN DATA WRAPPER clickhouse_wrapper
                         HANDLER click_house_fdw_handler VALIDATOR click_house_fdw_validator"#,
                None,
                None,
            );
            c.update(
                r#"CREATE SERVER my_clickhouse_server
                         FOREIGN DATA WRAPPER clickhouse_wrapper
                         OPTIONS (
                           conn_string 'tcp://default:@localhost:9000/supa'
                         )"#,
                None,
                None,
            );
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
            );
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
            );

            assert_eq!(c.select("SELECT * FROM test_table", None, None).len(), 0);
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                Some(vec![(
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    "test".into_datum(),
                )]),
            );
            assert_eq!(
                c.select("SELECT name FROM test_table", None, None)
                    .first()
                    .get_one::<&str>()
                    .unwrap(),
                "test"
            );
            assert_eq!(
                c.select("SELECT name FROM test_cust_sql", None, None)
                    .first()
                    .get_one::<&str>()
                    .unwrap(),
                "test"
            );

            let remote_value: String = rt
                .block_on(async {
                    handle
                        .query("SELECT name FROM supa.test_table")
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
