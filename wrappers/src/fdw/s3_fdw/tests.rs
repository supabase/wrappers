#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn s3_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER s3_wrapper
                     HANDLER s3_fdw_handler VALIDATOR s3_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER s3_server
                     FOREIGN DATA WRAPPER s3_wrapper
                     OPTIONS (
                       aws_access_key_id 'admin',
                       aws_secret_access_key 'password',
                       aws_region 'us-east-1',
                       endpoint_url 'http://localhost:8000',
                       path_style_url 'true'
                     )"#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_csv (
                  name text,
                  sex text,
                  age text,
                  height text,
                  weight text
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://warehouse/test_data.csv',
                    format 'csv',
                    has_header 'true',
                    delimiter ','
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_csv_gz (
                  name text,
                  sex text,
                  age text,
                  height text,
                  weight text
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://warehouse/test_data.csv.gz',
                    format 'csv',
                    has_header 'true',
                    compress 'gzip'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_jsonl (
                  name text,
                  sex text,
                  age text,
                  height text,
                  weight text
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://warehouse/test_data.jsonl',
                    format 'jsonl'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_jsonl_bz (
                  name text,
                  sex text,
                  age text,
                  height text,
                  weight text
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://warehouse/test_data.jsonl.bz2',
                    format 'jsonl',
                    compress 'bzip2'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_parquet (
                  id integer,
                  bool_col boolean,
                  bigint_col bigint,
                  float_col real,
                  date_string_col text,
                  timestamp_col timestamp
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://warehouse/test_data.parquet',
                    format 'parquet'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_parquet_gz (
                  id integer,
                  bool_col boolean,
                  bigint_col bigint,
                  float_col real,
                  date_string_col text,
                  timestamp_col timestamp
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://warehouse/test_data.parquet.gz',
                    format 'parquet',
                    compress 'gzip'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_parquet_titanic (
                    "PassengerId" bigint,
                    "Survived" bigint,
                    "Pclass" bigint,
                    "Name" text,
                    "Age" double precision,
                    "SibSp" bigint,
                    "Parch" bigint,
                    "Fare" double precision
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://warehouse/test_data_titanic.parquet',
                    format 'parquet'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE s3_test_table_tsv (
                  name text,
                  sex text,
                  age text,
                  height text,
                  weight text
                )
                SERVER s3_server
                OPTIONS (
                    uri 's3://warehouse/test_data.tsv',
                    format 'csv',
                    has_header 'true',
                    delimiter E'\t'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let check_test_table = |table| {
                let sql = format!("SELECT * FROM {table} ORDER BY name LIMIT 1");
                let results = c
                    .select(&sql, None, &[])
                    .unwrap()
                    .filter_map(|r| {
                        r.get_by_name::<&str, _>("name")
                            .unwrap()
                            .zip(r.get_by_name::<&str, _>("age").unwrap())
                            .zip(r.get_by_name::<&str, _>("height").unwrap())
                    })
                    .collect::<Vec<_>>();
                assert_eq!(results, vec![(("Alex", "41"), "74")]);
            };

            check_test_table("s3_test_table_csv");
            check_test_table("s3_test_table_csv_gz");
            check_test_table("s3_test_table_jsonl");
            check_test_table("s3_test_table_jsonl_bz");
            check_test_table("s3_test_table_tsv");

            let check_parquet_table = |table| {
                let sql = format!("SELECT * FROM {table} ORDER BY id LIMIT 1");
                let results = c
                    .select(&sql, None, &[])
                    .unwrap()
                    .filter_map(|r| {
                        r.get_by_name::<i32, _>("id")
                            .unwrap()
                            .zip(r.get_by_name::<&str, _>("date_string_col").unwrap())
                    })
                    .collect::<Vec<_>>();
                assert_eq!(results, vec![(0, "01/01/09")]);
            };

            check_parquet_table("s3_test_table_parquet");
            check_parquet_table("s3_test_table_parquet_gz");

            let sql = "SELECT * FROM s3_test_table_parquet_titanic ORDER BY 1 LIMIT 1";
            let results = c
                .select(sql, None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<i64, _>("PassengerId")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("Name").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![(1, "Braund, Mr. Owen Harris")]);
        });
    }
}
