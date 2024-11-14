#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn wasm_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER wasm_wrapper
                     HANDLER wasm_fdw_handler VALIDATOR wasm_fdw_validator"#,
                None,
                None,
            )
            .unwrap();

            // Snowflake FDW test
            c.update(
                r#"CREATE SERVER snowflake_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/snowflake_fdw/target/wasm32-unknown-unknown/release/snowflake_fdw.wasm',
                       fdw_package_name 'supabase:snowflake-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/snowflake/{}',
                       account_identifier 'TEST-ABC1234',
                       user 'TESTUSER',
                       public_key_fingerprint 'WVggEofeFX0jwCdImbOfGFyOggF2o8DT7S1uTLZhCJQ=',
                       private_key E'-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCOQv0mMe1yElR8\nhiQgduWu7OrMR3iW8xbu+i04LkMDKB/JtdMl1Mq3Gs/XWUB07BOIytcK4L7k1Z/3\nnkTGvib85S5+VLsngBzWltltvCOfnM2uCLWHmVpfMR1WVFrCU1r7NP92U2APpwvN\ncJps39VNyQWTm+TvQN25enqAC2uR6xdvItb86dn4Tab3KcAXj0f0qHompa8SSwBO\nGZFpgAjt7QFPWTniNQyBU7wXTntwV+a2WC/bf1i9MRAf8bqSr+yqijLHVGrmBPSb\njmvo/8bj69DzeY2//gZXAMe1m6cEjXPm2MY/POd66Xg1YZDzYsv95GZ54kJ+EgEF\n9rjsi/J5AgMBAAECggEAAsG9kh3pkgpU5Mzcqlxjew5QRoEkDxjK2vqyIaKT3d3L\nL+d8HgGPpBi66ltqalmgz0fO/wD38gtJvEyu3IMW0lPGoOAXeF59MJNfx0acEh3B\nxpuYmPYZ0DptbRzZXWasHq4aPTrEY8lC60pBU9bKlWVN3FxrBU/mfA+pjA2smflC\n54brJTsSb1/1xAExxsvB2Leb6VcNWKRCaN6Z4gdWd1Qofi080LVWxE3MXhxHpTMj\nVf3KHhKI5DHJXlZPU/w4KXOlp99UH0vnx3EJzD07kI+nR2k7tfh8PxwFzn8g6hEC\nK9Q+HmzUUTxFh1M8eXi7IMRjLRJThVSl/Kbqr6cpeQKBgQC+410aRuHUteQIgJBQ\nbceAOjEh5MetByIEFdXLEgYspl1rSjN/JoIMUguyJ8KZGj5G3NaRaJklNOofYekI\nhIL3SBWZ9U58MJVMnSUVeBdazCu0k9HnOfOFrFJIDoRPBfjnP0UJmC+9ggoVayOX\nVW5psrxGiQXWiG7mho1bshSlNwKBgQC+yX1wFSF8gGsAb41iw60K9W+O5PLVWxAu\nYst8CQTY64RVctvAypWtNTb4nmIBe9aX9k5loe+uv8Zse/t1hgCVGR7n70EyT9Y+\nGNrGqYtVjtZQ+L+dAivrlUKsTDGqzWldUTg7gpOqkFaQbV0O11ytyKJKYXCpTrL2\nwib6V4X9zwKBgQCllTJAxfXFfxZUbblBm0iwKUpPXVX7+LEAHDS9F2B1wMZOeCod\nhLjQmSb+HlFGX6Zf79bMgZA+3xyrplHviorUmBns2AaB4d7Qe4wciHSx1WOgG427\n5uAgNy+Uw8rvhX24koB/Zx0aZT/7/lj8QCYr19hL0zZWNzkEDPl37gzMlwKBgQC3\noOsww8XVNSzH4JZupvOYhp53JHltTRaH7uL3YR7fQd++9qv4JYRmj793D8o4r17e\nKF1QiMpOoZpzs+lVNkK9Ps52YduYdys33WhEqc7H7JDuolya3Ao11xWzDCsJwGdX\nP+MltAo4sm/+1qQosrQrN96sRJjQ/ERYKIqnjTIUFQKBgQC6xaC5SB1UMepFbTVa\n2tuRuYwcrU/NxjeW85SAyeyv2dMg7S+Ot8mnA1Js2O0NHlczUZgRZalYkCuSUE78\nb6rIbezIW2azrw3tqAAPLsB+rhXvaUpICoybu+j6aCiqtZYsDx7zIj/FTD27Tpwx\nYfLx1Erqd3vM/LzOIaIOqlfETw==\n-----END PRIVATE KEY-----'
                       )"#,
                None,
                None,
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
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM snowflake_mytable WHERE id = 42", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo"]);

            // Paddle FDW test
            c.update(
                r#"CREATE SERVER paddle_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/paddle_fdw/target/wasm32-unknown-unknown/release/paddle_fdw.wasm',
                       fdw_package_name 'supabase:paddle-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/paddle',
                       api_key '1234567890'
                     )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE paddle_customers (
                    id text,
                    name text,
                    email text,
                    status text,
                    custom_data jsonb,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                  )
                  SERVER paddle_server
                  OPTIONS (
                    object 'customers',
                    rowid_column 'id'
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM paddle_customers WHERE id = 'ctm_01hymwgpkx639a6mkvg99563sp'",
                    None,
                    None,
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
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/notion_fdw/target/wasm32-unknown-unknown/release/notion_fdw.wasm',
                       fdw_package_name 'supabase:notion-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/notion',
                       api_key '1234567890'
                     )"#,
                None,
                None,
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
                None,
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM notion_pages WHERE id = '5a67c86f-d0da-4d0a-9dd7-f4cf164e6247'",
                    None,
                    None,
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
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/calendly_fdw/target/wasm32-unknown-unknown/release/calendly_fdw.wasm',
                       fdw_package_name 'supabase:calendly-fdw',
                       fdw_package_version '>=0.1.0',
                       organization 'https://api.calendly.com/organizations/xxx',
                       api_url 'http://localhost:8096/calendly',
                       api_key '1234567890'
                     )"#,
                None,
                None,
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
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM calendly_event_types", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("uri").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["https://api.calendly.com/event_types/158ecbf6-79bb-4205-a5fc-a7fefa5883a2"]
            );
        });
    }
}
