#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use pgx::prelude::*;

    #[pg_test]
    fn stripe_smoketest() {
        Spi::execute(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER stripe_wrapper
                         HANDLER stripe_fdw_handler VALIDATOR stripe_fdw_validator"#,
                None,
                None,
            );
            c.update(
                r#"CREATE SERVER my_stripe_server
                         FOREIGN DATA WRAPPER stripe_wrapper
                         OPTIONS (
                           api_url 'http://localhost:12111/v1',  -- Stripe API base URL, optional
                           api_key 'sk_test_51LUmojFkiV6mfx3cpEzG9VaxhA86SA4DIj3b62RKHnRC0nhPp2JBbAmQ1izsX9RKD8rlzvw2xpY54AwZtXmWciif00Qi8J0w3O'  -- Stripe API Key, required
                         )"#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE balance (
                  amount bigint,
                  currency text
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'balance'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE customers (
                  id text,
                  email text
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'customers'    -- source object in stripe, required
                )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE subscriptions (
                  customer text,
                  currency text,
                  current_period_start bigint,
                  current_period_end bigint
                )
                SERVER my_stripe_server
                OPTIONS (
                  object 'subscriptions'    -- source object in stripe, required
                )
             "#,
                None,
                None,
            );

            let results = c
                .select("SELECT * FROM balance", None, None)
                .filter_map(|r| {
                    r.by_name("amount")
                        .ok()
                        .and_then(|v| v.value::<i64>())
                        .zip(r.by_name("currency").ok().and_then(|v| v.value::<&str>()))
                })
                .collect::<Vec<_>>();

            assert_eq!(results, vec![(0, "usd")]);

            let results = c
                .select("SELECT * FROM subscriptions", None, None)
                .filter_map(|r| {
                    r.by_name("customer")
                        .ok()
                        .and_then(|v| v.value::<&str>())
                        .zip(r.by_name("currency").ok().and_then(|v| v.value::<&str>()))
                        .zip(
                            r.by_name("current_period_start")
                                .ok()
                                .and_then(|v| v.value::<i64>()),
                        )
                        .zip(
                            r.by_name("current_period_end")
                                .ok()
                                .and_then(|v| v.value::<i64>()),
                        )
                })
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![(
                    (("cus_MJiBtCqOF1Bb3F", "usd"), 287883090000000),
                    287883090000000
                )]
            );
        });
    }
}
