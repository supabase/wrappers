#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::fdw::auth0_fdw::auth0_client::row::Auth0User;
    use pgrx::pg_test;
    use pgrx::prelude::*;

    #[pg_test]
    fn auth0_smoketest() {
        let expected_data = Auth0User {
            user_id: "auth0|1234567890abcdef".to_string(),
            email: "john@doe.com".to_string(),
            email_verified: true,
            username: Some("userexample".to_string()),
            phone_number: Some("123-456-7890".to_string()),
            phone_verified: Some(true),
            created_at: "2023-05-16T07:41:08.028Z".to_string(),
            updated_at: "2023-05-16T08:41:08.028Z".to_string(),
            identities: Some(serde_json::json!([])),
            app_metadata: Some(serde_json::json!({})),
            user_metadata: Some(serde_json::json!({})),
            picture: "https://example.com/avatar.jpg".to_string(),
            name: "John Doe".to_string(),
            nickname: "Johnny".to_string(),
            multifactor: Some(serde_json::json!([])),
            last_ip: "192.168.1.1".to_string(),
            last_login: "2023-05-16T08:41:08.028Z".to_string(),
            logins_count: 1,
            blocked: Some(false),
            given_name: Some("John".to_string()),
            family_name: Some("Doe".to_string()),
        };

        Spi::connect(|mut c| {
            c.update(
                r#"create foreign data wrapper auth0_wrapper
                         handler auth0_fdw_handler validator auth0_fdw_validator"#,
                None,
                None,
            )
            .expect("Failed to create foreign data wrapper");
            c.update(
                r#"CREATE SERVER auth0_server
                         FOREIGN DATA WRAPPER auth0_wrapper
                         OPTIONS (
                            url 'http://localhost:3796',
                            api_key 'apiKey'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE auth0_users (
                    user_id text,
                    email text,
                    email_verified bool,
                    username text,
                    phone_number text,
                    phone_verified bool,
                    created_at text,
                    updated_at text,
                    identities jsonb,
                    app_metadata jsonb,
                    user_metadata jsonb,
                    picture text,
                    name text,
                    nickname text,
                    multifactor jsonb,
                    last_ip text,
                    last_login text,
                    logins_count int,
                    blocked bool,
                    given_name text,
                    family_name text
                  )
                  SERVER auth0_server
                  options (
                    object 'users'
                  )
             "#,
                None,
                None,
            )
            .unwrap();
            /*
             The table data below comes from the code in wrappers/dockerfiles/auth0/server.py
            */
            let results = c
                .select(
                    "SELECT * FROM auth0_users WHERE email = 'john@doe.com'",
                    None,
                    None,
                )
                .expect("Failed to select from auth0_users")
                .map(|r| Auth0User {
                    user_id: r.get_by_name::<String, _>("user_id").unwrap().unwrap(),
                    email: r.get_by_name::<String, _>("email").unwrap().unwrap(),
                    email_verified: r.get_by_name::<bool, _>("email_verified").unwrap().unwrap(),
                    username: r.get_by_name::<String, _>("username").unwrap(),
                    phone_number: r.get_by_name::<String, _>("phone_number").unwrap(),
                    phone_verified: r.get_by_name::<bool, _>("phone_verified").unwrap(),
                    created_at: r.get_by_name::<String, _>("created_at").unwrap().unwrap(),
                    updated_at: r.get_by_name::<String, _>("updated_at").unwrap().unwrap(),
                    identities: serde_json::from_str("[]").unwrap(),
                    app_metadata: serde_json::from_str("{}").unwrap(),
                    user_metadata: serde_json::from_str("{}").unwrap(),
                    picture: r.get_by_name::<String, _>("picture").unwrap().unwrap(),
                    name: r.get_by_name::<String, _>("name").unwrap().unwrap(),
                    nickname: r.get_by_name::<String, _>("nickname").unwrap().unwrap(),
                    multifactor: serde_json::from_str("[]").unwrap(),
                    last_ip: r.get_by_name::<String, _>("last_ip").unwrap().unwrap(),
                    last_login: r.get_by_name::<String, _>("last_login").unwrap().unwrap(),
                    logins_count: r.get_by_name::<i32, _>("logins_count").unwrap().unwrap(),
                    blocked: r.get_by_name::<bool, _>("blocked").unwrap(),
                    given_name: r.get_by_name::<String, _>("given_name").unwrap(),
                    family_name: r.get_by_name::<String, _>("family_name").unwrap(),
                })
                .collect::<Vec<_>>();

            assert_eq!(results.len(), 1);
            assert_eq!(results[0], expected_data);
        })
    }
}
