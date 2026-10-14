#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    use serde_json::json;
    use std::str::FromStr;

    use super::super::server_type::ServerType;

    #[pg_test]
    fn ducklake_import_foreign_schema() {
        let options = [
            ("type", "ducklake"),
            (
                "metadata_path",
                "postgres:host=localhost port=5433 dbname=ducklake user=ducklake password=ducklake",
            ),
            ("metadata_schema", "fdw_test"),
            ("key_id", "admin"),
            ("secret", "password"),
            ("region", "us-east-1"),
            ("endpoint", "localhost:8000"),
            ("url_style", "path"),
            ("use_ssl", "false"),
        ]
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .into_iter()
        .collect();
        let server_type = ServerType::new(&options).unwrap();

        // Provision an existing catalog independently of the read-only FDW.
        let conn = duckdb::Connection::open_in_memory().unwrap();
        for sql in server_type
            .get_duckdb_extension_sql()
            .into_iter()
            .chain(server_type.get_create_secret_sql(&options))
        {
            conn.execute(&sql, []).unwrap();
        }
        conn
            .execute_batch(
                "attach 'ducklake:postgres:host=localhost port=5433 dbname=ducklake user=ducklake password=ducklake'
                 as lake (metadata_schema 'fdw_test', data_path 's3://warehouse/ducklake_fdw_test/',
                          data_inlining_row_limit 0);
                 create schema if not exists lake.inventory;
                 create or replace table lake.inventory.products as
                 select 1::integer as id, 'Apple' as name
                 union all select 2, 'Pear';
                 create or replace table lake.inventory.excluded (id integer);
                 detach lake;",
            )
            .unwrap();

        // Reattach with the FDW's read-only settings.
        for sql in server_type
            .get_settings_sql(&options)
            .into_iter()
            .chain([server_type.get_attach_sql(&options).unwrap()])
        {
            conn.execute(&sql, []).unwrap();
        }
        assert!(
            conn.execute(
                "insert into ducklake.inventory.products values (3, 'Plum')",
                []
            )
            .is_err()
        );
        assert!(
            conn.prepare("select * from read_text('/etc/passwd')")
                .and_then(|mut stmt| stmt.query([]).map(|_| ()))
                .is_err()
        );

        Spi::run(
            "CREATE FOREIGN DATA WRAPPER duckdb_wrapper
               HANDLER duckdb_fdw_handler VALIDATOR duckdb_fdw_validator;
             CREATE SERVER ducklake_server FOREIGN DATA WRAPPER duckdb_wrapper OPTIONS (
               type 'ducklake',
               metadata_path 'postgres:host=localhost port=5433 dbname=ducklake user=ducklake password=ducklake',
               metadata_schema 'fdw_test',
               key_id 'admin', secret 'password', region 'us-east-1',
               endpoint 'localhost:8000', url_style 'path', use_ssl 'false'
             );
             CREATE SCHEMA ducklake_all;
             CREATE SCHEMA ducklake_limited;
             CREATE SCHEMA ducklake_except;
             IMPORT FOREIGN SCHEMA inventory FROM SERVER ducklake_server INTO ducklake_all;
             IMPORT FOREIGN SCHEMA inventory LIMIT TO (products)
               FROM SERVER ducklake_server INTO ducklake_limited;
             IMPORT FOREIGN SCHEMA inventory EXCEPT (excluded)
               FROM SERVER ducklake_server INTO ducklake_except;",
        )
        .unwrap();

        for schema in ["ducklake_all", "ducklake_limited", "ducklake_except"] {
            assert_eq!(
                Spi::get_one::<String>(&format!(
                    "select name from {schema}.products where id > 0 order by id desc limit 1"
                ))
                .unwrap(),
                Some("Pear".to_string())
            );
        }
        assert_eq!(
            Spi::get_one::<i64>(
                "select count(*) from information_schema.foreign_tables
                 where foreign_table_schema in ('ducklake_all', 'ducklake_limited', 'ducklake_except')"
            )
            .unwrap(),
            Some(4)
        );
        assert_eq!(
            Spi::get_one::<i64>("select count(*) from ducklake_all.excluded").unwrap(),
            Some(0)
        );
    }

    #[pg_test]
    fn ducklake_options_preserve_quoted_values() {
        let options = [
            ("type", "ducklake"),
            (
                "metadata_path",
                "postgres:dbname=catalog password=quo'te;value",
            ),
            ("metadata_schema", "custom's;schema"),
            ("key_id", "test"),
            ("secret", "quo'te;value"),
        ]
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .into_iter()
        .collect();
        let server_type = ServerType::new(&options).unwrap();
        let attach = server_type.get_attach_sql(&options).unwrap();
        assert!(attach.contains("password=quo''te;value'"));
        assert!(attach.contains("metadata_schema 'custom''s;schema'"));
        assert!(attach.contains("read_only, create_if_not_exists false"));

        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("install httpfs", []).unwrap();
        conn.execute("load httpfs", []).unwrap();
        for sql in server_type.get_create_secret_sql(&options) {
            conn.execute(&sql, []).unwrap();
        }

        let empty_options = Default::default();
        assert!(server_type.get_attach_sql(&empty_options).is_err());
        assert!(server_type.get_create_secret_sql(&empty_options).is_empty());
    }

    #[pg_test]
    fn duckdb_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER duckdb_wrapper
                     HANDLER duckdb_fdw_handler VALIDATOR duckdb_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER duckdb_server_s3
                     FOREIGN DATA WRAPPER duckdb_wrapper
                     OPTIONS (
                       type 's3',
                       key_id 'admin',
                       secret 'password',
                       region 'us-east-1',
                       endpoint 'localhost:8000',
                       url_style 'path',
                       use_ssl 'false'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER duckdb_server_iceberg
                     FOREIGN DATA WRAPPER duckdb_wrapper
                     OPTIONS (
                       type 'iceberg',
                       key_id 'admin',
                       secret 'password',
                       region 'us-east-1',
                       endpoint 'localhost:8000',
                       url_style 'path',
                       use_ssl 'false',
                       token 'test',
                       warehouse 'warehouse',
                       catalog_uri 'localhost:8181'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER duckdb_server_motherduck
                    FOREIGN DATA WRAPPER duckdb_wrapper
                    OPTIONS (
                        type 'md',
                        database 'my_db',
                        motherduck_token 'my_token'
                    )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS duckdb"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA s3 FROM SERVER duckdb_server_s3 INTO duckdb
                    OPTIONS (
                        tables '
                            s3://warehouse/test_data.csv,
                            s3://warehouse/test_data.parquet
                        ',
                        strict 'true'
                    )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA "docs_example" FROM SERVER duckdb_server_iceberg INTO duckdb"#,
                None,
                &[],
            )
            .unwrap();
            let results = c
                .select(
                    "SELECT * FROM duckdb.s3_0_test_data order by name",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Alex", "Bert", "Carl"]);

            let results = c
                .select(
                    "SELECT * FROM duckdb.s3_1_test_data order by id limit 3",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<pgrx::datum::Timestamp, _>("timestamp_col")
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    pgrx::datum::Timestamp::from_str("2009-01-01 00:00:00").unwrap(),
                    pgrx::datum::Timestamp::from_str("2009-01-01 00:01:00").unwrap(),
                    pgrx::datum::Timestamp::from_str("2009-02-01 00:00:00").unwrap(),
                ]
            );

            let results = c
                .select(
                    "SELECT datetime,symbol,bid,ask,details,amt,dt,tstz,bin,bcol,list,icol,map,lcol
                     FROM duckdb.iceberg_docs_example_bids
                     WHERE symbol in ('APL', 'MCS')
                     order by symbol",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("symbol").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["APL", "MCS"]);

            let results = c
                .select(
                    "SELECT *
                     FROM duckdb.iceberg_docs_example_bids
                     WHERE symbol = 'APL'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::datum::JsonB, _>("details").unwrap())
                .map(|v| v.0.clone())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![json!({
                    "created_by": "alice",
                    "balance": 222.33,
                    "count": 42,
                    "valid": true
                })]
            );
        });
    }

    #[pg_test]
    #[should_panic]
    fn duckdb_local_file_read() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER duckdb_wrapper
                     HANDLER duckdb_fdw_handler VALIDATOR duckdb_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER duckdb_server_s3
                     FOREIGN DATA WRAPPER duckdb_wrapper
                     OPTIONS (
                       type 's3',
                       key_id 'admin',
                       secret 'password',
                       region 'us-east-1',
                       endpoint 'localhost:8000',
                       url_style 'path',
                       use_ssl 'false'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE FOREIGN TABLE duckdb.passwd (a text)
                   SERVER duckdb_server_s3
                   OPTIONS (
                     table 'read_csv(''/etc/passwd'', sep = '':'')'
                   )"#,
                None,
                &[],
            )
            .unwrap();
            let _results = c.select("SELECT * FROM duckdb.passwd", None, &[]);
        });
    }
}
