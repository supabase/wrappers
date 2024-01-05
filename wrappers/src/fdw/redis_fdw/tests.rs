#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_test;
    use pgrx::prelude::*;
    use redis::Client;

    #[pg_test]
    fn redis_smoketest() {
        let client = Client::open("redis://127.0.0.1").expect("create client");
        let mut conn = client.get_connection().expect("client connect");

        // make test data

        // list
        redis::pipe()
            .atomic()
            .rpush("list", "foo")
            .ignore()
            .rpush("list", "bar")
            .ignore()
            .rpush("list", "42")
            .ignore()
            .query::<()>(&mut conn)
            .expect("list test data");

        // set
        redis::pipe()
            .atomic()
            .sadd("set", "foo")
            .ignore()
            .sadd("set", "bar")
            .ignore()
            .sadd("set", "42")
            .ignore()
            .query::<()>(&mut conn)
            .expect("set test data");

        // hash
        redis::pipe()
            .atomic()
            .hset_multiple("hash", &[("foo", "bar"), ("baz", "qux")])
            .ignore()
            .query::<()>(&mut conn)
            .expect("hash test data");

        // sorted set
        redis::pipe()
            .atomic()
            .zadd_multiple("zset", &[(30, "foo"), (20, "bar"), (10, "baz")])
            .ignore()
            .query::<()>(&mut conn)
            .expect("zset test data");

        // stream
        redis::pipe()
            .atomic()
            .xadd("stream", "*", &[("foo", "bar")])
            .ignore()
            .xadd("stream", "*", &[("aa", 42), ("bb", 43)])
            .ignore()
            .query::<()>(&mut conn)
            .expect("stream test data");

        // multi list
        redis::pipe()
            .atomic()
            .rpush("list:100", "foo")
            .ignore()
            .rpush("list:100", "bar")
            .ignore()
            .rpush("list:200", "baz")
            .ignore()
            .query::<()>(&mut conn)
            .expect("multi list test data");

        // multi set
        redis::pipe()
            .atomic()
            .sadd("set:100", "foo")
            .ignore()
            .sadd("set:200", "bar")
            .ignore()
            .query::<()>(&mut conn)
            .expect("multi set test data");

        // multi hash
        redis::pipe()
            .atomic()
            .hset_multiple("hash:100", &[("foo", "bar")])
            .ignore()
            .hset_multiple("hash:200", &[("baz", "qux")])
            .ignore()
            .query::<()>(&mut conn)
            .expect("hash test data");

        // mutl sorted set
        redis::pipe()
            .atomic()
            .zadd_multiple("zset:100", &[(10, "foo"), (20, "bar")])
            .ignore()
            .zadd_multiple("zset:200", &[(40, "baz"), (30, "qux")])
            .ignore()
            .query::<()>(&mut conn)
            .expect("zset test data");

        // perform test

        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER redis_wrapper
                         HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER redis_server
                         FOREIGN DATA WRAPPER redis_wrapper
                         OPTIONS (
                           conn_url 'redis://127.0.0.1'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE redis_list (
                    element text
                  )
                    server redis_server
                    options (
                      src_type 'list',
                      src_key 'list'
                    );
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE redis_set (
                    element text
                  )
                    server redis_server
                    options (
                      src_type 'set',
                      src_key 'set'
                    );
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE redis_hash (
                    key text,
                    value text
                  )
                    server redis_server
                    options (
                      src_type 'hash',
                      src_key 'hash'
                    );
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE redis_zset (
                    element text
                  )
                    server redis_server
                    options (
                      src_type 'zset',
                      src_key 'zset'
                    );
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE redis_stream (
                    id text,
                    items jsonb
                  )
                    server redis_server
                    options (
                      src_type 'stream',
                      src_key 'stream'
                    );
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE redis_multi_lists (
                    key text,
                    items jsonb
                  )
                    server redis_server
                    options (
                      src_type 'multi_list',
                      src_key 'list:*'
                    );
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE redis_multi_sets (
                    key text,
                    items jsonb
                  )
                    server redis_server
                    options (
                      src_type 'multi_set',
                      src_key 'set:*'
                    );
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE redis_multi_hashes (
                    key text,
                    items jsonb
                  )
                    server redis_server
                    options (
                      src_type 'multi_hash',
                      src_key 'hash:*'
                    );
             "#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE redis_multi_zsets (
                    key text,
                    items jsonb
                  )
                    server redis_server
                    options (
                      src_type 'multi_zset',
                      src_key 'zset:*'
                    );
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM redis_list", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("element").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo", "bar", "42"]);

            let results = c
                .select("SELECT * FROM redis_set ORDER BY 1", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("element").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["42", "bar", "foo"]);

            let results = c
                .select(
                    "SELECT key, value FROM redis_hash WHERE key = 'foo'",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("key")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("value").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![("foo", "bar")]);

            let results = c
                .select("SELECT * FROM redis_zset", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("element").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["baz", "bar", "foo"]);

            let results = c
                .select(
                    "SELECT items::text FROM redis_stream ORDER BY id DESC",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("items").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![r#"{"aa": "42", "bb": "43"}"#, r#"{"foo": "bar"}"#,]
            );

            let results = c
                .select(
                    "SELECT key, items::text FROM redis_multi_lists ORDER BY key",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("key")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("items").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    ("list:100", r#"["foo", "bar"]"#),
                    ("list:200", r#"["baz"]"#)
                ]
            );

            let results = c
                .select(
                    "SELECT key, items::text FROM redis_multi_sets ORDER BY key",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("key")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("items").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![("set:100", r#"["foo"]"#), ("set:200", r#"["bar"]"#)]
            );

            let results = c
                .select(
                    "SELECT key, items::text FROM redis_multi_hashes ORDER BY key",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("key")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("items").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    ("hash:100", r#"{"foo": "bar"}"#),
                    ("hash:200", r#"{"baz": "qux"}"#)
                ]
            );

            let results = c
                .select(
                    "SELECT key, items::text FROM redis_multi_zsets ORDER BY key",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("key")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("items").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    ("zset:100", r#"["foo", "bar"]"#),
                    ("zset:200", r#"["qux", "baz"]"#)
                ]
            );
        });
    }
}
