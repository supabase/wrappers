#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use clickhouse_rs as ch;
    use pgrx::prelude::*;
    use pgrx::{datum::Timestamp, pg_test, Uuid};
    use supabase_wrappers::prelude::create_async_runtime;

    #[pg_test]
    fn clickhouse_smoketest() {
        Spi::connect_mut(|c| {
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
                            fstr Nullable(FixedString(5)),
                            bignum Nullable(UInt256),
                            dnum Nullable(Decimal(18, 3)),
                            arr_i64 Array(Int64) default [],
                            arr_str Array(String) default [],
                            is_valid Nullable(Bool),
                            i8col Nullable(Int8),
                            u8col Nullable(UInt8),
                            i16col Nullable(Int16),
                            u16col Nullable(UInt16),
                            i32col Nullable(Int32),
                            u32col Nullable(UInt32),
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
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_clickhouse_server
                         FOREIGN DATA WRAPPER clickhouse_wrapper
                         OPTIONS (
                           conn_string 'tcp://default:default@localhost:9000/default'
                         )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_table (
                    id bigint,
                    name text,
                    amt double precision,
                    uid uuid,
                    fstr text,
                    bignum text,
                    dnum numeric,
                    arr_i64 bigint[],
                    arr_str text[],
                    is_valid boolean,
                    i8col "char",
                    u8col smallint,
                    i16col smallint,
                    u16col integer,
                    i32col integer,
                    u32col bigint,
                    created_at timestamp
                  )
                  SERVER my_clickhouse_server
                  OPTIONS (
                    table 'test_table',
                    rowid_column 'id',
                    stream_buffer_size '512'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE test_cust_sql (
                    id bigint,
                    name text,
                    amt double precision,
                    uid uuid,
                    fstr text,
                    bignum text,
                    dnum numeric,
                    arr_i64 bigint[],
                    arr_str text[],
                    is_valid boolean,
                    i8col "char",
                    u8col smallint,
                    i16col smallint,
                    u16col integer,
                    i32col integer,
                    u32col bigint,
                    created_at timestamp
                  )
                  SERVER my_clickhouse_server
                  OPTIONS (
                    table '(select * from test_table)'
                  )
             "#,
                None,
                &[],
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
                    fstr text,
                    bignum text,
                    dnum numeric,
                    arr_i64 bigint[],
                    arr_str text[],
                    is_valid boolean,
                    i8col "char",
                    u8col smallint,
                    i16col smallint,
                    u16col integer,
                    i32col integer,
                    u32col bigint,
                    created_at timestamp
                  )
                  SERVER my_clickhouse_server
                  OPTIONS (
                    table '(select *, name as _name from test_table where name = ${_name})',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            assert_eq!(
                c.select("SELECT * FROM test_table", None, &[])
                    .unwrap()
                    .len(),
                0
            );
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                &["test".into()],
            )
            .unwrap();
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                &["test2".into()],
            )
            .unwrap();
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                &["test3".into()],
            )
            .unwrap();
            c.update(
                "INSERT INTO test_table (name) VALUES ($1)",
                None,
                &["test4".into()],
            )
            .unwrap();
            c.update(
                "INSERT INTO test_table (id, name, amt, uid, fstr, bignum, dnum,
                    arr_i64, arr_str, is_valid, i8col, u8col, i16col, u16col,
                    i32col, u32col, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                     $14, $15, $16, $17)",
                None,
                &[
                    42.into(),
                    None::<String>.into(),
                    123.45.into(),
                    pgrx::Uuid::from_bytes([42u8; 16]).into(),
                    "abc".into(),
                    "12345678".into(),
                    pgrx::AnyNumeric::try_from(123456.789).unwrap().into(),
                    vec![123i64, 456i64].into(),
                    vec!["abc", "def"].into(),
                    false.into(),
                    42i8.into(),
                    43i16.into(),
                    44i16.into(),
                    45i32.into(),
                    46i32.into(),
                    47i64.into(),
                    Timestamp::new(2025, 5, 1, 2, 3, 4.0).into(),
                ],
            )
            .unwrap();
            assert_eq!(
                c.select("SELECT name FROM test_table ORDER BY name", None, &[])
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
                    &[]
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
                c.select(
                    "SELECT fstr, bignum, dnum FROM test_table WHERE id = 42",
                    None,
                    &[]
                )
                .unwrap()
                .first()
                .get_three::<&str, &str, AnyNumeric>()
                .unwrap(),
                (
                    Some("abc\0\0"),
                    Some("12345678"),
                    Some(AnyNumeric::try_from(123456.789).unwrap())
                )
            );
            assert_eq!(
                c.select(
                    "SELECT arr_i64, arr_str FROM test_table WHERE id = 42 and array['abc','def'] @> arr_str",
                    None,
                    &[]
                )
                .unwrap()
                .first()
                .get_two::<Vec<i64>, Vec<String>>()
                .unwrap(),
                (
                    Some(vec![123i64, 456i64]),
                    Some(vec!["abc".to_string(), "def".to_string()]),
                )
            );
            assert_eq!(
                c.select(
                    "SELECT is_valid, i8col, u8col FROM test_table WHERE id = 42",
                    None,
                    &[]
                )
                .unwrap()
                .first()
                .get_three::<bool, i8, i16>()
                .unwrap(),
                (Some(false), Some(42), Some(43))
            );
            assert_eq!(
                c.select(
                    "SELECT i16col, u16col FROM test_table WHERE id = 42",
                    None,
                    &[]
                )
                .unwrap()
                .first()
                .get_two::<i16, i32>()
                .unwrap(),
                (Some(44), Some(45),)
            );
            assert_eq!(
                c.select(
                    "SELECT i32col, u32col FROM test_table WHERE id = 42",
                    None,
                    &[]
                )
                .unwrap()
                .first()
                .get_two::<i32, i64>()
                .unwrap(),
                (Some(46), Some(47),)
            );
            assert_eq!(
                c.select("SELECT name FROM test_cust_sql ORDER BY name", None, &[])
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
                    &["test2".into()]
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
                    &["test2".into()]
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
                    &["test2".into()]
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
                    &[]
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
                    &[]
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

            // test update data in foreign table
            c.update(
                "UPDATE test_table
                 SET uid = $1, bignum = $2, arr_i64 = $3
                 WHERE id = 42
                ",
                None,
                &[
                    pgrx::Uuid::from_bytes([43u8; 16]).into(),
                    "87654321".into(),
                    vec![444i64, 222i64].into(),
                ],
            )
            .unwrap();
            assert_eq!(
                c.select(
                    "SELECT uid, bignum, arr_i64 FROM test_table WHERE id = 42",
                    None,
                    &[]
                )
                .unwrap()
                .first()
                .get_three::<Uuid, &str, Vec<i64>>()
                .unwrap(),
                (
                    Some(Uuid::from_bytes([43u8; 16])),
                    Some("87654321"),
                    Some(vec![444i64, 222i64]),
                )
            );
            assert_eq!(
                c.select(
                    "SELECT uid FROM test_table WHERE uid = $1",
                    None,
                    &[pgrx::Uuid::from_bytes([43u8; 16]).into(),],
                )
                .unwrap()
                .first()
                .get_one::<Uuid>()
                .unwrap(),
                Some(Uuid::from_bytes([43u8; 16])),
            );

            // test delete data in foreign table
            c.update("DELETE FROM test_table WHERE id = 42", None, &[])
                .unwrap();
            assert!(c
                .select("SELECT * FROM test_table WHERE id = 42", None, &[])
                .unwrap()
                .is_empty());
        });
    }
}
