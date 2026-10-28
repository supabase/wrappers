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

    // The backend must still respond to query cancel, statement_timeout and
    // pg_terminate_backend() while waiting on a remote which never answers
    #[pg_test]
    fn s3_cancellable_while_remote_unresponsive() {
        let server = crate::test_utils::UnresponsiveServer::start();
        let port = server.port;
        Spi::run(
            r#"CREATE FOREIGN DATA WRAPPER s3_unresponsive_wrapper
                 HANDLER s3_fdw_handler VALIDATOR s3_fdw_validator"#,
        )
        .unwrap();
        Spi::run(&format!(
            r#"CREATE SERVER s3_unresponsive_server
                 FOREIGN DATA WRAPPER s3_unresponsive_wrapper
                 OPTIONS (
                   aws_access_key_id 'admin',
                   aws_secret_access_key 'password',
                   aws_region 'us-east-1',
                   endpoint_url 'http://127.0.0.1:{port}',
                   path_style_url 'true'
                 )"#
        ))
        .unwrap();
        Spi::run(
            r#"CREATE FOREIGN TABLE s3_unresponsive_table (name text)
                 SERVER s3_unresponsive_server
                 OPTIONS (uri 's3://warehouse/test_data.csv', format 'csv', has_header 'true')"#,
        )
        .unwrap();

        server.assert_cancellable("SELECT * FROM s3_unresponsive_table");
    }

    // Parquet row groups are read through an `AsyncRead` adapter, so the
    // backend must still respond to query cancel while a data read waits on a
    // remote which stopped answering after serving the file metadata
    #[pg_test]
    fn s3_parquet_cancellable_while_remote_unresponsive() {
        use crate::test_utils::{HttpResponse, HttpServer, assert_cancellable_when};
        use arrow_array::{ArrayRef, Int64Array, RecordBatch};
        use std::sync::Arc;

        // a parquet file large enough for its data to be read separately
        // from its footer
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..200_000));
        let batch = RecordBatch::try_from_iter(vec![("id", ids)]).unwrap();
        let mut file = Vec::new();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = file.len();
        let footer_len = u32::from_le_bytes(file[size - 8..size - 4].try_into().unwrap()) as usize;
        let metadata_start = size - 8 - footer_len;
        let file = Arc::new(file);

        // a fake S3 which serves the object size and footer, but never
        // answers reads of the data pages
        let server = HttpServer::start(move |req| {
            if req.target.contains("attributes") {
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?><GetObjectAttributesResponse xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectSize>{size}</ObjectSize></GetObjectAttributesResponse>"#
                );
                return Some(HttpResponse {
                    status: 200,
                    headers: vec![("Content-Type".into(), "application/xml".into())],
                    body: body.into_bytes(),
                });
            }
            let Some(range) = req.header("range") else {
                // the whole object is requested before deciding how to read it
                return Some(HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: file.to_vec(),
                });
            };
            let range = range.strip_prefix("bytes=")?;
            let (start, end) = range.split_once('-')?;
            let start = start.parse::<usize>().ok()?;
            let end = end.parse::<usize>().ok()?.min(size - 1);
            if end < metadata_start {
                return None;
            }
            Some(HttpResponse {
                status: 206,
                headers: vec![(
                    "Content-Range".into(),
                    format!("bytes {start}-{end}/{size}"),
                )],
                body: file[start..=end].to_vec(),
            })
        });
        let port = server.port;

        Spi::run(
            r#"CREATE FOREIGN DATA WRAPPER s3_parquet_unresponsive_wrapper
                 HANDLER s3_fdw_handler VALIDATOR s3_fdw_validator"#,
        )
        .unwrap();
        Spi::run(&format!(
            r#"CREATE SERVER s3_parquet_unresponsive_server
                 FOREIGN DATA WRAPPER s3_parquet_unresponsive_wrapper
                 OPTIONS (
                   aws_access_key_id 'admin',
                   aws_secret_access_key 'password',
                   aws_region 'us-east-1',
                   endpoint_url 'http://127.0.0.1:{port}',
                   path_style_url 'true'
                 )"#
        ))
        .unwrap();
        Spi::run(
            r#"CREATE FOREIGN TABLE s3_parquet_unresponsive_table (id bigint)
                 SERVER s3_parquet_unresponsive_server
                 OPTIONS (uri 's3://warehouse/test.parquet', format 'parquet')"#,
        )
        .unwrap();

        assert_cancellable_when(
            "SELECT * FROM s3_parquet_unresponsive_table",
            server.when_hung(),
        );
    }
}
