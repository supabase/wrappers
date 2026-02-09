#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn wasm_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER wasm_wrapper
                     HANDLER wasm_fdw_handler VALIDATOR wasm_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();

            // Snowflake FDW test
            c.update(
                r#"CREATE SERVER snowflake_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/snowflake_fdw.wasm',
                       fdw_package_name 'supabase:snowflake-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/snowflake/{}',
                       account_identifier 'TEST-ABC1234',
                       user 'TESTUSER',
                       timeout_secs '30',
                       public_key_fingerprint 'WVggEofeFX0jwCdImbOfGFyOggF2o8DT7S1uTLZhCJQ=',
                       private_key E'-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCOQv0mMe1yElR8\nhiQgduWu7OrMR3iW8xbu+i04LkMDKB/JtdMl1Mq3Gs/XWUB07BOIytcK4L7k1Z/3\nnkTGvib85S5+VLsngBzWltltvCOfnM2uCLWHmVpfMR1WVFrCU1r7NP92U2APpwvN\ncJps39VNyQWTm+TvQN25enqAC2uR6xdvItb86dn4Tab3KcAXj0f0qHompa8SSwBO\nGZFpgAjt7QFPWTniNQyBU7wXTntwV+a2WC/bf1i9MRAf8bqSr+yqijLHVGrmBPSb\njmvo/8bj69DzeY2//gZXAMe1m6cEjXPm2MY/POd66Xg1YZDzYsv95GZ54kJ+EgEF\n9rjsi/J5AgMBAAECggEAAsG9kh3pkgpU5Mzcqlxjew5QRoEkDxjK2vqyIaKT3d3L\nL+d8HgGPpBi66ltqalmgz0fO/wD38gtJvEyu3IMW0lPGoOAXeF59MJNfx0acEh3B\nxpuYmPYZ0DptbRzZXWasHq4aPTrEY8lC60pBU9bKlWVN3FxrBU/mfA+pjA2smflC\n54brJTsSb1/1xAExxsvB2Leb6VcNWKRCaN6Z4gdWd1Qofi080LVWxE3MXhxHpTMj\nVf3KHhKI5DHJXlZPU/w4KXOlp99UH0vnx3EJzD07kI+nR2k7tfh8PxwFzn8g6hEC\nK9Q+HmzUUTxFh1M8eXi7IMRjLRJThVSl/Kbqr6cpeQKBgQC+410aRuHUteQIgJBQ\nbceAOjEh5MetByIEFdXLEgYspl1rSjN/JoIMUguyJ8KZGj5G3NaRaJklNOofYekI\nhIL3SBWZ9U58MJVMnSUVeBdazCu0k9HnOfOFrFJIDoRPBfjnP0UJmC+9ggoVayOX\nVW5psrxGiQXWiG7mho1bshSlNwKBgQC+yX1wFSF8gGsAb41iw60K9W+O5PLVWxAu\nYst8CQTY64RVctvAypWtNTb4nmIBe9aX9k5loe+uv8Zse/t1hgCVGR7n70EyT9Y+\nGNrGqYtVjtZQ+L+dAivrlUKsTDGqzWldUTg7gpOqkFaQbV0O11ytyKJKYXCpTrL2\nwib6V4X9zwKBgQCllTJAxfXFfxZUbblBm0iwKUpPXVX7+LEAHDS9F2B1wMZOeCod\nhLjQmSb+HlFGX6Zf79bMgZA+3xyrplHviorUmBns2AaB4d7Qe4wciHSx1WOgG427\n5uAgNy+Uw8rvhX24koB/Zx0aZT/7/lj8QCYr19hL0zZWNzkEDPl37gzMlwKBgQC3\noOsww8XVNSzH4JZupvOYhp53JHltTRaH7uL3YR7fQd++9qv4JYRmj793D8o4r17e\nKF1QiMpOoZpzs+lVNkK9Ps52YduYdys33WhEqc7H7JDuolya3Ao11xWzDCsJwGdX\nP+MltAo4sm/+1qQosrQrN96sRJjQ/ERYKIqnjTIUFQKBgQC6xaC5SB1UMepFbTVa\n2tuRuYwcrU/NxjeW85SAyeyv2dMg7S+Ot8mnA1Js2O0NHlczUZgRZalYkCuSUE78\nb6rIbezIW2azrw3tqAAPLsB+rhXvaUpICoybu+j6aCiqtZYsDx7zIj/FTD27Tpwx\nYfLx1Erqd3vM/LzOIaIOqlfETw==\n-----END PRIVATE KEY-----'
                       )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE snowflake_mytable (
                      id bigint,
                      name text,
                      num numeric,
                      dt date,
                      ts timestamp
                  )
                  SERVER snowflake_server
                  OPTIONS (
                    table 'mydatabase.public.mytable',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM snowflake_mytable WHERE id = 42", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo"]);

            // Paddle FDW test
            c.update(
                r#"CREATE SERVER paddle_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/paddle_fdw.wasm',
                       fdw_package_name 'supabase:paddle-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/paddle',
                       api_key '1234567890'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS paddle"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA paddle FROM SERVER paddle_server INTO paddle"#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM paddle.customers WHERE id = 'ctm_01hymwgpkx639a6mkvg99563sp'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("email").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["test@test.com"]);

            // Notion FDW test
            c.update(
                r#"CREATE SERVER notion_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/notion_fdw.wasm',
                       fdw_package_name 'supabase:notion-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/notion',
                       api_key '1234567890'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE notion_pages (
                    id text,
                    url text,
                    created_time timestamp,
                    last_edited_time timestamp,
                    archived boolean,
                    attrs jsonb
                  )
                  SERVER notion_server
                  OPTIONS (
                    object 'page'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM notion_pages WHERE id = '5a67c86f-d0da-4d0a-9dd7-f4cf164e6247'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("url").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["https://www.notion.so/test-page3-5a67c86fd0da4d0a9dd7f4cf164e6247"]
            );

            // Calendly FDW test
            c.update(
                r#"CREATE SERVER calendly_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/calendly_fdw.wasm',
                       fdw_package_name 'supabase:calendly-fdw',
                       fdw_package_version '>=0.1.0',
                       organization 'https://api.calendly.com/organizations/xxx',
                       api_url 'http://localhost:8096/calendly',
                       api_key '1234567890'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE calendly_event_types (
                    uri text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                  )
                  SERVER calendly_server
                  OPTIONS (
                    object 'event_types'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM calendly_event_types", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("uri").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["https://api.calendly.com/event_types/158ecbf6-79bb-4205-a5fc-a7fefa5883a2"]
            );

            // Cal.com FDW test
            c.update(
                r#"CREATE SERVER cal_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/cal_fdw.wasm',
                       fdw_package_name 'supabase:cal-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/cal',
                       api_key '1234567890'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE cal_my_profile (
                    id bigint,
                    username text,
                    email text,
                    attrs jsonb
                  )
                  SERVER cal_server
                  OPTIONS (
                    object 'my_profile'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM cal_my_profile", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![1234567]);

            // Cloudflare D1 FDW test
            c.update(
                r#"CREATE SERVER cfd1_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/cfd1_fdw.wasm',
                       fdw_package_name 'supabase:cfd1-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/cfd1',
                       account_id 'aaa',
                       database_id 'bbb',
                       api_token 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE cfd1_table (
                    id bigint,
                    name text,
                    _attrs jsonb
                  )
                  SERVER cfd1_server
                  OPTIONS (
                    table 'test_table'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM cfd1_table order by id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![42, 123]);

            // Clerk FDW test
            c.update(
                r#"CREATE SERVER clerk_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/clerk_fdw.wasm',
                       fdw_package_name 'supabase:clerk-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/clerk',
                       api_key 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE clerk_table (
                    id text,
                    external_id text,
                    username text,
                    first_name text,
                    last_name text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                  )
                  SERVER clerk_server
                  OPTIONS (
                    object 'users'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM clerk_table", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["user_2rvWkk90azWI2o3PH4LDuCMDPPh"]);

            // Orb FDW test
            c.update(
                r#"CREATE SERVER orb_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/orb_fdw.wasm',
                       fdw_package_name 'supabase:orb-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/orb',
                       api_key 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE orb_table (
                    id text,
                    name text,
                    email text,
                    created_at timestamp,
                    auto_collection boolean,
                    attrs jsonb
                  )
                  SERVER orb_server
                  OPTIONS (
                    object 'customers'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM orb_table", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["XimGiw3pnsgusvc3"]);

            // HubSpot FDW test
            c.update(
                r#"CREATE SERVER hubspot_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/hubspot_fdw.wasm',
                       fdw_package_name 'supabase:hubspot-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/hubspot',
                       api_key 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE hubspot_table (
                    id text,
                    email text,
                    firstname text,
                    lastname text,
                    user_id text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                  )
                  SERVER hubspot_server
                  OPTIONS (
                    object 'objects/contacts'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT id, user_id FROM hubspot_table", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("user_id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["8527", "8528"]);

            // Gravatar FDW test
            c.update(
                r#"CREATE SERVER gravatar_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'https://github.com/Automattic/gravatar-wasm-fdw/releases/download/v0.2.0/gravatar_fdw.wasm',
                       fdw_package_name 'automattic:gravatar-fdw',
                       fdw_package_version '0.2.0',
                       fdw_package_checksum '5273ae07e66bc2f1bb5a23d7b9e0342463971691e587bbd6f9466814a8bac11c',
                       api_url 'http://localhost:8096/gravatar',
                       api_key 'test'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS gravatar"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA gravatar FROM SERVER gravatar_server INTO gravatar"#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM gravatar.profiles where email = 'email@example.com'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("display_name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Test"]);

            // Shopify FDW test
            c.update(
                r#"CREATE SERVER shopify_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/shopify_fdw.wasm',
                       fdw_package_name 'supabase:shopify-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/shopify',
                       shop 'test',
                       access_token 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS shopify"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA shopify FROM SERVER shopify_server INTO shopify"#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT id FROM shopify.products order by id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    "gid://shopify/Product/9975063609658",
                    "gid://shopify/Product/9975063904570"
                ]
            );

            // Infura FDW test
            c.update(
                r#"CREATE SERVER infura_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/infura_fdw.wasm',
                       fdw_package_name 'supabase:infura-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/infura',
                       api_key 'test_api_key',
                       network 'mainnet'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE infura_blocks (
                    number numeric,
                    hash text,
                    parent_hash text,
                    timestamp timestamp,
                    miner text,
                    gas_used numeric,
                    gas_limit numeric,
                    transaction_count bigint,
                    base_fee_per_gas numeric,
                    attrs jsonb
                  )
                  SERVER infura_server
                  OPTIONS (
                    resource 'blocks'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT hash FROM infura_blocks", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("hash").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"]
            );

            // OpenAPI FDW test
            c.update(
                r#"CREATE SERVER openapi_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/openapi_fdw.wasm',
                       fdw_package_name 'supabase:openapi-fdw',
                       fdw_package_version '>=0.1.0',
                       base_url 'http://localhost:8096/openapi',
                       api_key 'test_key'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_users (
                    id text,
                    name text,
                    email text,
                    created_at timestamptz,
                    active boolean,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/users',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            // Test list query
            let results = c
                .select("SELECT id, name, email FROM openapi_users", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("email").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["john@example.com", "jane@example.com"]);

            // Test ID pushdown query
            let results = c
                .select(
                    "SELECT name FROM openapi_users WHERE id = 'user-123'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["John Doe"]);

            // Test path parameter substitution: /users/{user_id}/posts
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_user_posts (
                    id text,
                    user_id text,
                    title text,
                    content text,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/users/{user_id}/posts',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT user_id, title FROM openapi_user_posts WHERE user_id = 'user-123'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("title").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["First Post", "Second Post"]);

            // Test multiple path parameters: /projects/{org}/{repo}/issues
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_issues (
                    id bigint,
                    org text,
                    repo text,
                    title text,
                    state text,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/projects/{org}/{repo}/issues',
                    response_path '/items',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT org, repo, title FROM openapi_issues WHERE org = 'supabase' AND repo = 'wrappers'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("org").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["supabase", "supabase"]);

            // Test GeoJSON FeatureCollection with object_path
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_locations (
                    id text,
                    name text,
                    category text,
                    population bigint,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/locations',
                    response_path '/features',
                    object_path '/properties',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT name, population FROM openapi_locations", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Austin", "Dallas"]);

            // Test direct array response
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_tags (
                    id bigint,
                    name text,
                    count bigint,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/tags',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT name FROM openapi_tags", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["rust", "postgres", "wasm"]);

            // Test resource type/id pattern: /resources/{type}/{id}
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_resources (
                    id text,
                    type text,
                    name text,
                    description text,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/resources/{type}/{id}',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT type, id, name FROM openapi_resources WHERE type = 'document' AND id = 'doc-001'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("type").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["document"]);
        });
    }
}
