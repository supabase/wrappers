#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    use pgrx::JsonB;

    #[pg_test]
    fn firebase_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER firebase_wrapper
                         HANDLER firebase_fdw_handler VALIDATOR firebase_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_firebase_server
                         FOREIGN DATA WRAPPER firebase_wrapper
                         OPTIONS (
                          project_id 'supa',
                          access_token 'owner'
                         )"#,
                None,
                None,
            )
            .unwrap();

            /*
             The tables below come from the code in docker-compose.yml that looks like this:

             ```
             volumes:
                   - ../dockerfiles/firebase/baseline-data:/baseline-data
             ```
            */

            c.update(
                r#"
                  CREATE FOREIGN TABLE firebase_users (
                    uid text,
                    email text,
                    created_at timestamp,
                    attrs jsonb
                  )
                 SERVER my_firebase_server
                 OPTIONS (
                   object 'auth/users',
                   base_url 'http://localhost:9099/identitytoolkit.googleapis.com/v1/projects'
                )
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT email FROM firebase_users order by email",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("email").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["bar@example.com", "foo@example.com"]);

            c.update(
                r#"
                CREATE FOREIGN TABLE firebase_docs (
                  name text,
                  created_at timestamp,
                  updated_at timestamp,
                  attrs jsonb
                )
                SERVER my_firebase_server
                OPTIONS (
                  object 'firestore/my-collection',  -- format: 'firestore/[collection_id]'
                  base_url 'http://localhost:8080/v1/projects'
                )
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT name,attrs FROM firebase_docs", None, None)
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("name").unwrap().zip(
                        r.get_by_name::<JsonB, _>("attrs")
                            .unwrap()
                            .map(|j| j.0.get("fields").unwrap().clone()),
                    )
                })
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![
                ("projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs",
                 serde_json::json!({
                     "id": { "integerValue": "1" },
                     "name": { "stringValue": "hello" }
                 }))]
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE firebase_docs_nested (
                  name text,
                  created_at timestamp,
                  updated_at timestamp,
                  attrs jsonb
                )
                SERVER my_firebase_server
                OPTIONS (
                  object 'firestore/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2',
                  base_url 'http://localhost:8080/v1/projects'
                )
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT name,attrs FROM firebase_docs_nested", None, None)
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("name").unwrap().zip(
                        r.get_by_name::<JsonB, _>("attrs")
                            .unwrap()
                            .map(|j| j.0.get("fields").unwrap().clone()),
                    )
                })
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![
                ("projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/fkSWL4hNJ3lRc1ZIorPm",
                 serde_json::json!({ "foo": { "stringValue": "bar" } }))]);
        });
    }
}
