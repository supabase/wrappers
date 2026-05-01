#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use clickhouse_rs as ch;
    use pgrx::prelude::*;
    use pgrx::{
        Uuid,
        datum::{Date, Timestamp, TimestampWithTimeZone},
        pg_test,
    };
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
                handle.execute("DROP TABLE IF EXISTS test_table_nn").await?;
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
                            u64col Nullable(UInt64),
                            f32col Nullable(Float32),
                            i128col Nullable(Int128),
                            u128col Nullable(UInt128),
                            i256col Nullable(Int256),
                            u256col Nullable(UInt256),
                            dtcol Nullable(Date),
                            arr_bool Array(Bool) default [],
                            arr_i16 Array(Int16) default [],
                            arr_i32 Array(Int32) default [],
                            arr_f32 Array(Float32) default [],
                            arr_f64 Array(Float64) default [],
                            created_at DateTime('UTC'),
                            updated_at DateTime64(6, 'Asia/Singapore')
                        ) engine = Memory",
                    )
                    .await?;
                handle
                    .execute(
                        "CREATE TABLE test_table_nn (
                            id Int64,
                            name TEXT,
                            amt Float64,
                            uid UUID,
                            fstr FixedString(5),
                            bignum UInt256,
                            dnum Decimal(18, 3),
                            arr_i64 Array(Int64) default [],
                            arr_str Array(String) default [],
                            is_valid Bool,
                            i8col Int8,
                            u8col UInt8,
                            i16col Int16,
                            u16col UInt16,
                            i32col Int32,
                            u32col UInt32,
                            u64col UInt64,
                            f32col Float32,
                            i128col Int128,
                            u128col UInt128,
                            i256col Int256,
                            u256col UInt256,
                            dtcol Date,
                            arr_bool Array(Bool) default [],
                            arr_i16 Array(Int16) default [],
                            arr_i32 Array(Int32) default [],
                            arr_f32 Array(Float32) default [],
                            arr_f64 Array(Float64) default [],
                            created_at DateTime('UTC'),
                            updated_at DateTime64(6, 'Asia/Singapore')
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
                    u64col bigint,
                    i128col text,
                    u128col text,
                    i256col text,
                    u256col text,
                    dtcol date,
                    arr_bool boolean[],
                    arr_i16 smallint[],
                    arr_i32 integer[],
                    arr_f32 real[],
                    arr_f64 double precision[],
                    created_at timestamp,
                    updated_at timestamptz
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
                  CREATE FOREIGN TABLE test_table_nn (
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
                    u64col bigint,
                    i128col text,
                    u128col text,
                    i256col text,
                    u256col text,
                    dtcol date,
                    arr_bool boolean[],
                    arr_i16 smallint[],
                    arr_i32 integer[],
                    arr_f32 real[],
                    arr_f64 double precision[],
                    created_at timestamp,
                    updated_at timestamptz
                  )
                  SERVER my_clickhouse_server
                  OPTIONS (
                    table 'test_table_nn',
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
                    u64col bigint,
                    i128col text,
                    u128col text,
                    i256col text,
                    u256col text,
                    dtcol date,
                    arr_bool boolean[],
                    arr_i16 smallint[],
                    arr_i32 integer[],
                    arr_f32 real[],
                    arr_f64 double precision[],
                    created_at timestamp,
                    updated_at timestamptz
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
                    u64col bigint,
                    i128col text,
                    u128col text,
                    i256col text,
                    u256col text,
                    dtcol date,
                    arr_bool boolean[],
                    arr_i16 smallint[],
                    arr_i32 integer[],
                    arr_f32 real[],
                    arr_f64 double precision[],
                    created_at timestamp,
                    updated_at timestamptz
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
            assert_eq!(
                c.select("SELECT * FROM test_table_nn", None, &[])
                    .unwrap()
                    .len(),
                0
            );
            c.update(
                "INSERT INTO test_table_nn (
                    id, name, amt, uid, fstr, bignum, dnum,
                    is_valid, i8col, u8col, i16col, u16col,
                    i32col, u32col, u64col, i128col, u128col, i256col, u256col,
                    dtcol, created_at, updated_at
                 )
                 VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18, $19, $20,
                    $21, $22
                 )",
                None,
                &[
                    1i64.into(),
                    "sample_nn".into(),
                    99.9.into(),
                    pgrx::Uuid::from_bytes([1u8; 16]).into(),
                    "abcde".into(),
                    "99999999".into(),
                    pgrx::AnyNumeric::try_from(42.123).unwrap().into(),
                    true.into(),
                    7i8.into(),
                    8i16.into(),
                    9i16.into(),
                    10i32.into(),
                    11i32.into(),
                    12i64.into(),
                    13i64.into(),
                    "123".into(),
                    "234".into(),
                    "345".into(),
                    "456".into(),
                    Date::new(2025, 1, 1).into(),
                    Timestamp::new(2025, 5, 2, 3, 4, 5.0).into(),
                    TimestampWithTimeZone::with_timezone(2025, 5, 2, 3, 4, 5.0, "Asia/Singapore")
                        .unwrap()
                        .into(),
                ],
            )
            .unwrap();
            assert_eq!(
                c.select("SELECT * FROM test_table_nn WHERE id = 1", None, &[])
                    .unwrap()
                    .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                    .collect::<Vec<_>>(),
                vec!["sample_nn"]
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
                    i32col, u32col, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                     $14, $15, $16, $17, $18)",
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
                    TimestampWithTimeZone::with_timezone(2025, 5, 1, 2, 3, 4.0, "Asia/Singapore")
                        .unwrap()
                        .into(),
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

            // test timestamptz data type
            c.update(
                "UPDATE test_table
                 SET updated_at = '2025-05-01 01:02:03.112233+8'::timestamptz
                 WHERE id = 42
                ",
                None,
                &[],
            )
            .unwrap();
            assert_eq!(
                c.select(
                    "SELECT id FROM test_table WHERE updated_at = '2025-05-01 01:02:03.112233+8'::timestamptz",
                    None,
                    &[]
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap(),
                Some(42),
            );

            // test delete data in foreign table
            c.update("DELETE FROM test_table WHERE id = 42", None, &[])
                .unwrap();
            assert!(
                c.select("SELECT * FROM test_table WHERE id = 42", None, &[])
                    .unwrap()
                    .is_empty()
            );
        });
    }

    #[pg_test]
    fn clickhouse_join_test() {
        Spi::connect_mut(|c| {
            let clickhouse_pool = ch::Pool::new("tcp://default:default@localhost:9000/default");

            let rt = create_async_runtime().expect("failed to create runtime");
            let mut handle = rt
                .block_on(async { clickhouse_pool.get_handle().await })
                .expect("handle");

            // Create two tables in ClickHouse for join testing
            rt.block_on(async {
                handle.execute("DROP TABLE IF EXISTS join_t1").await?;
                handle.execute("DROP TABLE IF EXISTS join_t2").await?;
                handle
                    .execute("CREATE TABLE join_t1 (k Int16) engine = Memory")
                    .await?;
                handle
                    .execute("CREATE TABLE join_t2 (k Int16) engine = Memory")
                    .await?;
                // Insert values (1, 2, 3) into each table
                handle
                    .execute("INSERT INTO join_t1 VALUES (1), (2), (3)")
                    .await?;
                handle
                    .execute("INSERT INTO join_t2 VALUES (1), (2), (3)")
                    .await
            })
            .expect("join tables in ClickHouse");

            // Set up FDW and server
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

            // Create foreign tables for join testing
            c.update(
                r#"
                  CREATE FOREIGN TABLE join_t1 (
                    k smallint
                  )
                  SERVER my_clickhouse_server
                  OPTIONS (table 'join_t1')
                "#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE join_t2 (
                    k smallint
                  )
                  SERVER my_clickhouse_server
                  OPTIONS (table 'join_t2')
                "#,
                None,
                &[],
            )
            .unwrap();

            // Test inner join - should return 3 rows, not 1
            let inner_join_count: i64 = c
                .select(
                    "SELECT COUNT(*) FROM join_t1 JOIN join_t2 ON join_t1.k = join_t2.k",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(
                inner_join_count, 3,
                "Inner join should return 3 rows, got {inner_join_count}",
            );

            // Verify inner join values are correct
            let result = c
                .select(
                    "SELECT join_t1.k as k1, join_t2.k as k2 FROM join_t1 JOIN join_t2 ON join_t1.k = join_t2.k ORDER BY join_t1.k",
                    None,
                    &[],
                )
                .unwrap();
            assert_eq!(result.len(), 3, "Inner join should return 3 rows");
            let mut values: Vec<(i16, i16)> = Vec::new();
            for row in result {
                let k1: i16 = row.get_by_name("k1").unwrap().unwrap();
                let k2: i16 = row.get_by_name("k2").unwrap().unwrap();
                values.push((k1, k2));
            }
            assert_eq!(
                values,
                vec![(1, 1), (2, 2), (3, 3)],
                "Inner join values incorrect"
            );

            // Test left join - should return 3 rows with correct values
            let left_join_count: i64 = c
                .select(
                    "SELECT COUNT(*) FROM join_t1 LEFT JOIN join_t2 ON join_t1.k = join_t2.k",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(
                left_join_count, 3,
                "Left join should return 3 rows, got {left_join_count}",
            );

            // Test right join - should return 3 rows with correct values
            let right_join_count: i64 = c
                .select(
                    "SELECT COUNT(*) FROM join_t1 RIGHT JOIN join_t2 ON join_t1.k = join_t2.k",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(
                right_join_count, 3,
                "Right join should return 3 rows, got {right_join_count}",
            );

            // Cleanup
            c.update("DROP FOREIGN TABLE join_t1", None, &[]).unwrap();
            c.update("DROP FOREIGN TABLE join_t2", None, &[]).unwrap();
        });
    }

    // Test for issue #482: PostgreSQL server crash with prepared statements
    // PostgreSQL caches query plans after ~5-6 executions. Before fix, this
    // would crash because plan_foreign_modify stored a pointer that became
    // invalid after end_foreign_modify freed the state.
    #[pg_test]
    fn clickhouse_prepared_statement_stress() {
        Spi::connect_mut(|c| {
            let clickhouse_pool = ch::Pool::new("tcp://default:default@localhost:9000/default");

            let rt = create_async_runtime().expect("failed to create runtime");
            let mut handle = rt
                .block_on(async { clickhouse_pool.get_handle().await })
                .expect("handle");

            rt.block_on(async {
                handle.execute("DROP TABLE IF EXISTS stress_test").await?;
                handle
                    .execute(
                        "CREATE TABLE stress_test (
                            id Int64,
                            name String
                        ) engine = Memory",
                    )
                    .await
            })
            .expect("stress_test in ClickHouse");

            c.update(
                r#"CREATE FOREIGN DATA WRAPPER clickhouse_wrapper_stress
                         HANDLER click_house_fdw_handler VALIDATOR click_house_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER stress_server
                         FOREIGN DATA WRAPPER clickhouse_wrapper_stress
                         OPTIONS (
                           conn_string 'tcp://default:default@localhost:9000/default'
                         )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE stress_test (
                    id bigint,
                    name text
                  )
                  SERVER stress_server
                  OPTIONS (
                    table 'stress_test',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            // Execute 15 parameterized INSERTs - enough to trigger plan caching
            // (PostgreSQL switches to generic plan after ~5-6 executions)
            // Before fix: crashes around iteration 7
            // After fix: all 15 succeed
            for i in 0..15i64 {
                c.update(
                    "INSERT INTO stress_test (id, name) VALUES ($1, $2)",
                    None,
                    &[i.into(), format!("stress_{i}").into()],
                )
                .unwrap();
            }

            // Verify all inserts succeeded
            assert_eq!(
                c.select("SELECT COUNT(*) FROM stress_test", None, &[])
                    .unwrap()
                    .first()
                    .get_one::<i64>()
                    .unwrap()
                    .unwrap(),
                15
            );
        });
    }

    /// Verify that aggregate queries are pushed down to ClickHouse.
    ///
    /// With pushdown the PostgreSQL plan is a bare `ForeignScan` — there is no
    /// local `Aggregate` / `HashAggregate` / `GroupAggregate` node on top.
    /// We check this via `EXPLAIN` alongside correctness assertions.
    ///
    /// Test data (table `agg_test`):
    ///   id=1, name='alice', amt=10  (row 1)
    ///   id=1, name='alice', amt=20  (row 2 — duplicate name)
    ///   id=2, name='bob',   amt=30  (row 3)
    ///   id=2, name='carol', amt=40  (row 4)
    #[pg_test]
    fn clickhouse_aggregate_pushdown_test() {
        Spi::connect_mut(|c| {
            let clickhouse_pool = ch::Pool::new("tcp://default:default@localhost:9000/default");
            let rt = create_async_runtime().expect("failed to create runtime");
            let mut handle = rt
                .block_on(async { clickhouse_pool.get_handle().await })
                .expect("handle");

            rt.block_on(async {
                handle.execute("DROP TABLE IF EXISTS agg_test").await?;
                handle
                    .execute(
                        "CREATE TABLE agg_test (
                            id    Int64,
                            name  String,
                            amt   Float64
                        ) engine = Memory",
                    )
                    .await?;
                handle
                    .execute(
                        "INSERT INTO agg_test VALUES
                            (1, 'alice', 10.0),
                            (1, 'alice', 20.0),
                            (2, 'bob',   30.0),
                            (2, 'carol', 40.0)",
                    )
                    .await
            })
            .expect("agg_test in ClickHouse");

            c.update(
                "CREATE FOREIGN DATA WRAPPER clickhouse_agg_wrapper
                 HANDLER click_house_fdw_handler VALIDATOR click_house_fdw_validator",
                None,
                &[],
            )
            .unwrap();
            c.update(
                "CREATE SERVER agg_server FOREIGN DATA WRAPPER clickhouse_agg_wrapper
                 OPTIONS (conn_string 'tcp://default:default@localhost:9000/default')",
                None,
                &[],
            )
            .unwrap();
            c.update(
                "CREATE FOREIGN TABLE agg_test (
                    id   bigint,
                    name text,
                    amt  double precision
                 )
                 SERVER agg_server
                 OPTIONS (table 'agg_test')",
                None,
                &[],
            )
            .unwrap();

            // Collect EXPLAIN output and assert no local Aggregate node is present.
            // With pushdown the ForeignScan IS the aggregate; without pushdown
            // PostgreSQL adds an Aggregate / HashAggregate / GroupAggregate node
            // on top of the ForeignScan.
            macro_rules! assert_pushed_down {
                ($c:expr, $sql:expr) => {{
                    let explain = format!("EXPLAIN {}", $sql);
                    // EXPLAIN returns a single-column result. SpiHeapTupleData::get
                    // uses 1-based ordinal access; SpiTupleTable::get_one is only
                    // available on the table-level (after .first()), not on iterators.
                    let plan: Vec<String> = $c
                        .select(&explain, None, &[])
                        .unwrap()
                        .filter_map(|r| r.get::<&str>(1).unwrap().map(|s| s.to_string()))
                        .collect();
                    // PostgreSQL plan node lines always include "(cost=...)",
                    // e.g. "HashAggregate  (cost=...)" or "->  Aggregate  (cost=...)".
                    // The FDW's own EXPLAIN extra output (e.g. "Wrappers: aggregates = [...]")
                    // does NOT include "(cost=", so we use both substrings to avoid
                    // false positives from the FDW's debug info.
                    assert!(
                        !plan
                            .iter()
                            .any(|l| l.contains("Aggregate") && l.contains("(cost=")),
                        "Expected pushdown for [{}] but plan shows local aggregation: {:?}",
                        $sql,
                        plan
                    );
                }};
            }

            // --- COUNT(*) — whole-table scalar aggregate ---
            assert_eq!(
                c.select("SELECT COUNT(*) FROM agg_test", None, &[])
                    .unwrap()
                    .first()
                    .get_one::<i64>()
                    .unwrap()
                    .unwrap(),
                4
            );
            assert_pushed_down!(c, "SELECT COUNT(*) FROM agg_test");

            // --- COUNT(name) GROUP BY id ---
            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id, COUNT(name) AS cnt FROM agg_test GROUP BY id ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let id = r.get_by_name::<i64, _>("id").unwrap().unwrap();
                    let cnt = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (id, cnt)
                })
                .collect();
            rows.sort();
            assert_eq!(rows, vec![(1, 2), (2, 2)]);
            assert_pushed_down!(c, "SELECT id, COUNT(name) AS cnt FROM agg_test GROUP BY id");

            // --- SUM(amt) GROUP BY id ---
            let mut rows: Vec<(i64, f64)> = c
                .select(
                    "SELECT id, SUM(amt) AS s FROM agg_test GROUP BY id ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let id = r.get_by_name::<i64, _>("id").unwrap().unwrap();
                    let s = r.get_by_name::<f64, _>("s").unwrap().unwrap();
                    (id, s)
                })
                .collect();
            rows.sort_by_key(|&(id, _)| id);
            assert_eq!(rows, vec![(1, 30.0), (2, 70.0)]);
            assert_pushed_down!(c, "SELECT id, SUM(amt) AS s FROM agg_test GROUP BY id");

            // --- AVG(amt) — whole-table ---
            let avg: f64 = c
                .select("SELECT AVG(amt) FROM agg_test", None, &[])
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!(
                (avg - 25.0).abs() < 1e-9,
                "AVG(amt) expected 25.0, got {avg}"
            );
            assert_pushed_down!(c, "SELECT AVG(amt) FROM agg_test");

            // --- MIN / MAX in the same query ---
            assert_eq!(
                c.select("SELECT MIN(id), MAX(id) FROM agg_test", None, &[])
                    .unwrap()
                    .first()
                    .get_two::<i64, i64>()
                    .unwrap(),
                (Some(1), Some(2))
            );
            assert_pushed_down!(c, "SELECT MIN(id), MAX(id) FROM agg_test");

            // --- COUNT(DISTINCT name) — whole-table (alice, bob, carol = 3) ---
            assert_eq!(
                c.select("SELECT COUNT(DISTINCT name) FROM agg_test", None, &[])
                    .unwrap()
                    .first()
                    .get_one::<i64>()
                    .unwrap()
                    .unwrap(),
                3
            );
            assert_pushed_down!(c, "SELECT COUNT(DISTINCT name) FROM agg_test");

            // --- COUNT(DISTINCT name) GROUP BY id ---
            // id=1: only 'alice' → 1; id=2: 'bob','carol' → 2
            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id, COUNT(DISTINCT name) AS cnt FROM agg_test GROUP BY id ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let id = r.get_by_name::<i64, _>("id").unwrap().unwrap();
                    let cnt = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (id, cnt)
                })
                .collect();
            rows.sort();
            assert_eq!(rows, vec![(1, 1), (2, 2)]);
            assert_pushed_down!(
                c,
                "SELECT id, COUNT(DISTINCT name) AS cnt FROM agg_test GROUP BY id"
            );

            // --- WHERE + aggregate: qual is pushed alongside the aggregate ---
            // 2 rows match id = 1, so SUM(amt) = 10 + 20 = 30.
            let s: f64 = c
                .select("SELECT SUM(amt) FROM agg_test WHERE id = 1", None, &[])
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!((s - 30.0).abs() < 1e-9, "expected 30.0, got {s}");
            assert_pushed_down!(c, "SELECT SUM(amt) FROM agg_test WHERE id = 1");

            // ----------------- Negative pushdown cases -----------------
            // For these, upper.rs's extract_aggregates / extract_group_by_columns
            // bails out and we never register the upper path, so the planner
            // falls back to a local HashAggregate / GroupAggregate above the
            // base ForeignScan. The defensive guard in scan.rs (clearing
            // state.aggregates when baserel.reloptkind != UPPER_REL) keeps
            // execution on the regular scan path and produces correct results.

            macro_rules! assert_not_pushed_down {
                ($c:expr, $sql:expr) => {{
                    let explain = format!("EXPLAIN {}", $sql);
                    let plan: Vec<String> = $c
                        .select(&explain, None, &[])
                        .unwrap()
                        .filter_map(|r| r.get::<&str>(1).unwrap().map(|s| s.to_string()))
                        .collect();
                    assert!(
                        plan.iter()
                            .any(|l| l.contains("Aggregate") && l.contains("(cost=")),
                        "Expected NO pushdown (a local Aggregate node) for [{}], plan: {:?}",
                        $sql,
                        plan
                    );
                }};
            }

            // HAVING clause: extract_aggregates bails when havingQual is set.
            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id, COUNT(*)::bigint AS cnt FROM agg_test \
                     GROUP BY id HAVING COUNT(*) > 1 ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let id = r.get_by_name::<i64, _>("id").unwrap().unwrap();
                    let cnt = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (id, cnt)
                })
                .collect();
            rows.sort();
            assert_eq!(rows, vec![(1, 2), (2, 2)]);
            assert_not_pushed_down!(
                c,
                "SELECT id, COUNT(*) FROM agg_test GROUP BY id HAVING COUNT(*) > 1"
            );

            // Unsupported aggregate function: stddev_samp is not in
            // supported_aggregates() and is not even recognized by
            // oid_to_aggregate_kind, so extract_aggregates returns None.
            // values [10,20,30,40] → mean=25, sample stddev = sqrt(500/3) ≈ 12.9099
            let stddev: f64 = c
                .select("SELECT STDDEV(amt) FROM agg_test", None, &[])
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!(
                (stddev - 12.909_944).abs() < 1e-3,
                "expected ~12.910, got {stddev}"
            );
            assert_not_pushed_down!(c, "SELECT STDDEV(amt) FROM agg_test");

            // FILTER clause on aggregate: aggref->aggfilter is non-NULL.
            let cnt: i64 = c
                .select(
                    "SELECT COUNT(*) FILTER (WHERE id = 1) FROM agg_test",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 2);
            assert_not_pushed_down!(c, "SELECT COUNT(*) FILTER (WHERE id = 1) FROM agg_test");

            // DISTINCT modifier on a non-COUNT aggregate (only COUNT(DISTINCT)
            // is allowed for pushdown). All four amt values are distinct.
            let s: f64 = c
                .select("SELECT SUM(DISTINCT amt) FROM agg_test", None, &[])
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!((s - 100.0).abs() < 1e-9, "expected 100.0, got {s}");
            assert_not_pushed_down!(c, "SELECT SUM(DISTINCT amt) FROM agg_test");

            // Aggregate over a non-Var expression (OpExpr). 11+21+31+41 = 104.
            let s: f64 = c
                .select("SELECT SUM(amt + 1) FROM agg_test", None, &[])
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!((s - 104.0).abs() < 1e-9, "expected 104.0, got {s}");
            assert_not_pushed_down!(c, "SELECT SUM(amt + 1) FROM agg_test");

            // GROUP BY a non-column expression. extract_group_by_columns finds
            // the SortGroupClause's TLE expr is OpExpr (not T_Var) and bails.
            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id + 1 AS g, COUNT(*)::bigint AS cnt FROM agg_test \
                     GROUP BY id + 1 ORDER BY g",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let g = r.get_by_name::<i64, _>("g").unwrap().unwrap();
                    let cnt = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (g, cnt)
                })
                .collect();
            rows.sort();
            assert_eq!(rows, vec![(2, 2), (3, 2)]);
            assert_not_pushed_down!(
                c,
                "SELECT id + 1 AS g, COUNT(*) FROM agg_test GROUP BY id + 1"
            );
        });
    }

    /// Aggregate pushdown over a sub-query specified in the `table` foreign-table
    /// option (both the static and parameterized forms).  Reuses the `agg_test`
    /// ClickHouse table created elsewhere in this module via cargo-pgrx's shared
    /// test database; if run in isolation it (re)creates the table.
    #[pg_test]
    fn clickhouse_aggregate_subquery_test() {
        Spi::connect_mut(|c| {
            let clickhouse_pool = ch::Pool::new("tcp://default:default@localhost:9000/default");
            let rt = create_async_runtime().expect("failed to create runtime");
            let mut handle = rt
                .block_on(async { clickhouse_pool.get_handle().await })
                .expect("handle");

            rt.block_on(async {
                handle.execute("DROP TABLE IF EXISTS agg_sub_test").await?;
                handle
                    .execute(
                        "CREATE TABLE agg_sub_test (
                            id    Int64,
                            name  String,
                            amt   Float64
                        ) engine = Memory",
                    )
                    .await?;
                handle
                    .execute(
                        "INSERT INTO agg_sub_test VALUES
                            (1, 'alice', 10.0),
                            (1, 'alice', 20.0),
                            (2, 'bob',   30.0),
                            (2, 'carol', 40.0),
                            (3, 'dan',   99.0)",
                    )
                    .await
            })
            .expect("agg_sub_test in ClickHouse");

            c.update(
                "CREATE FOREIGN DATA WRAPPER ch_sub_wrapper
                 HANDLER click_house_fdw_handler VALIDATOR click_house_fdw_validator",
                None,
                &[],
            )
            .unwrap();
            c.update(
                "CREATE SERVER ch_sub_server FOREIGN DATA WRAPPER ch_sub_wrapper
                 OPTIONS (conn_string 'tcp://default:default@localhost:9000/default')",
                None,
                &[],
            )
            .unwrap();

            // --- Static sub-query (no params): aggregate must still push down.
            // The remote side sees `SELECT ... FROM (select id, name, amt from
            // agg_sub_test where id < 3) GROUP BY id`.
            c.update(
                "CREATE FOREIGN TABLE sub_static (
                    id   bigint,
                    name text,
                    amt  double precision
                 )
                 SERVER ch_sub_server
                 OPTIONS (table '(select id, name, amt from agg_sub_test where id < 3)')",
                None,
                &[],
            )
            .unwrap();

            macro_rules! assert_pushed_down {
                ($c:expr, $sql:expr) => {{
                    let explain = format!("EXPLAIN {}", $sql);
                    let plan: Vec<String> = $c
                        .select(&explain, None, &[])
                        .unwrap()
                        .filter_map(|r| r.get::<&str>(1).unwrap().map(|s| s.to_string()))
                        .collect();
                    assert!(
                        !plan
                            .iter()
                            .any(|l| l.contains("Aggregate") && l.contains("(cost=")),
                        "Expected pushdown for [{}] but plan shows local aggregation: {:?}",
                        $sql,
                        plan
                    );
                }};
            }

            // Whole-table COUNT over the static sub-query.  id < 3 filters out
            // the dan row, so only 4 rows remain.
            let cnt: i64 = c
                .select("SELECT COUNT(*) FROM sub_static", None, &[])
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 4);
            assert_pushed_down!(c, "SELECT COUNT(*) FROM sub_static");

            // GROUP BY over the sub-query.
            let mut rows: Vec<(i64, i64)> = c
                .select(
                    "SELECT id, COUNT(*) AS cnt FROM sub_static GROUP BY id ORDER BY id",
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    let id = r.get_by_name::<i64, _>("id").unwrap().unwrap();
                    let cnt = r.get_by_name::<i64, _>("cnt").unwrap().unwrap();
                    (id, cnt)
                })
                .collect();
            rows.sort();
            assert_eq!(rows, vec![(1, 2), (2, 2)]);
            assert_pushed_down!(c, "SELECT id, COUNT(*) FROM sub_static GROUP BY id");

            // SUM with extra WHERE on the foreign table: the WHERE qual is also
            // pushed remotely (deparse_aggregate's WHERE-builder skips params
            // and arrays but keeps regular quals).
            let s: f64 = c
                .select(
                    "SELECT SUM(amt) FROM sub_static WHERE id = 2",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!((s - 70.0).abs() < 1e-9, "expected 70.0, got {s}");
            assert_pushed_down!(c, "SELECT SUM(amt) FROM sub_static WHERE id = 2");

            // --- Parameterized sub-query: ${filter_id} is bound from a qual on
            // the foreign-table column `filter_id`.  The qual is consumed as a
            // parameter (not emitted as a remote WHERE clause).  Aggregate
            // pushdown still applies — the deparsed remote SQL becomes
            // `SELECT COUNT(*) AS agg_1 FROM (select 1 as filter_id, ...
            //  from agg_sub_test where id = 1)`.
            c.update(
                "CREATE FOREIGN TABLE sub_param (
                    filter_id bigint,
                    id        bigint,
                    name      text,
                    amt       double precision
                 )
                 SERVER ch_sub_server
                 OPTIONS (
                    table '(select ${filter_id} as filter_id, id, name, amt \
                            from agg_sub_test where id = ${filter_id})'
                 )",
                None,
                &[],
            )
            .unwrap();

            // id=1 → 2 rows.
            let cnt: i64 = c
                .select(
                    "SELECT COUNT(*) FROM sub_param WHERE filter_id = 1",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap()
                .unwrap();
            assert_eq!(cnt, 2);
            assert_pushed_down!(c, "SELECT COUNT(*) FROM sub_param WHERE filter_id = 1");

            // id=2 → SUM(amt) = 30 + 40 = 70.
            let s: f64 = c
                .select(
                    "SELECT SUM(amt) FROM sub_param WHERE filter_id = 2",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!((s - 70.0).abs() < 1e-9, "expected 70.0, got {s}");
            assert_pushed_down!(c, "SELECT SUM(amt) FROM sub_param WHERE filter_id = 2");
        });
    }
}
