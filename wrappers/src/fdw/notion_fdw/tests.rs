#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn notion_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER notion_wrapper
                         HANDLER notion_fdw_handler VALIDATOR notion_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_notion_server
                         FOREIGN DATA WRAPPER notion_wrapper
                         OPTIONS (
                           api_url 'http://localhost:4242/v1',  -- Notion API base URL, optional
                           notion_version '2021-08-23',  -- Notion API version, optional
                           api_key 'sk_test_51LUmojFkiV6mfx3cpEzG9VaxhA86SA4DIj3b62RKHnRC0nhPp2JBbAmQ1izsX9RKD8rlzvw2xpY54AwZtXmWciif00Qi8J0w3O'  -- Notion API Key, required
                         )"#,
                None,
                None,
            ).unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE notion_users (
                  name text,
                  type text,
                  id text
                )
                SERVER my_notion_server
                OPTIONS (
                    object 'users'    -- Corrected object name if previously incorrect
                )
                "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT name, type, id FROM notion_users", None, None)
                .unwrap()
                .map(|r| {
                    (
                        r.get_by_name::<&str, _>("type").unwrap().unwrap(),
                        r.get_by_name::<&str, _>("name").unwrap().unwrap(),
                        r.get_by_name::<&str, _>("id").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    ("person", "John Doe", "d40e767c-d7af-4b18-a86d-55c61f1e39a4"),
                    ("bot", "Beep Boop", "9a3b5ae0-c6e6-482d-b0e1-ed315ee6dc57")
                ]
            );

            let results = c
                .select(
                    "SELECT * FROM notion_users WHERE id = 'd40e767c-d7af-4b18-a86d-55c61f1e39a4'",
                    None,
                    None,
                )
                .unwrap()
                .map(|r| {
                    (
                        r.get_by_name::<&str, _>("type").unwrap().unwrap(),
                        r.get_by_name::<&str, _>("name").unwrap().unwrap(),
                        r.get_by_name::<&str, _>("id").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![("person", "John Doe", "d40e767c-d7af-4b18-a86d-55c61f1e39a4")]
            );
        });
    }
}
