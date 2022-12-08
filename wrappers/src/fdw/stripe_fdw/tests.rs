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
                CREATE FOREIGN TABLE stripe_balance (
                  balance_type text,
                  amount bigint,
                  currency text,
                  attrs jsonb
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
                CREATE FOREIGN TABLE stripe_balance_transactions (
                  id text,
                  amount bigint,
                  currency text,
                  description text,
                  fee bigint,
                  net bigint,
                  status text,
                  type text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'balance_transactions'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_charges (
                  id text,
                  amount bigint,
                  currency text,
                  customer text,
                  description text,
                  invoice text,
                  payment_intent text,
                  status text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'charges'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_customers (
                  id text,
                  email text,
                  name text,
                  description text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'customers',    -- source object in stripe, required
                    rowid_column 'id'
                )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_invoices (
                  id text,
                  customer text,
                  subscription text,
                  status text,
                  total bigint,
                  currency text,
                  period_start timestamp,
                  period_end timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'invoices'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_payment_intents (
                  id text,
                  customer text,
                  amount bigint,
                  currency text,
                  payment_method text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'payment_intents'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_products (
                  id text,
                  name text,
                  active bool,
                  default_price text,
                  description text,
                  created timestamp,
                  updated timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'products',    -- source object in stripe, required
                    rowid_column 'id'
                  )
             "#,
                None,
                None,
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_subscriptions (
                  id text,
                  customer text,
                  currency text,
                  current_period_start timestamp,
                  current_period_end timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                  object 'subscriptions',    -- source object in stripe, required
                  rowid_column 'id'
                )
             "#,
                None,
                None,
            );

            let results = c
                .select("SELECT * FROM stripe_balance", None, None)
                .filter_map(|r| {
                    r.by_name("balance_type")
                        .ok()
                        .and_then(|v| v.value::<&str>())
                        .zip(r.by_name("amount").ok().and_then(|v| v.value::<i64>()))
                        .zip(r.by_name("currency").ok().and_then(|v| v.value::<&str>()))
                })
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![("available", 0, "usd"), ("pending", 0, "usd")]
            );

            let results = c
                .select("SELECT * FROM stripe_balance_transactions", None, None)
                .filter_map(|r| {
                    r.by_name("amount")
                        .ok()
                        .and_then(|v| v.value::<i64>())
                        .zip(r.by_name("currency").ok().and_then(|v| v.value::<&str>()))
                        .zip(r.by_name("fee").ok().and_then(|v| v.value::<i64>()))
                        .zip(r.by_name("status").ok().and_then(|v| v.value::<&str>()))
                        .zip(r.by_name("type").ok().and_then(|v| v.value::<&str>()))
                })
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![(((((100, "usd"), 0), "available"), "charge"))]
            );

            let results = c
                .select("SELECT * FROM stripe_charges", None, None)
                .filter_map(|r| {
                    r.by_name("amount")
                        .ok()
                        .and_then(|v| v.value::<i64>())
                        .zip(r.by_name("currency").ok().and_then(|v| v.value::<&str>()))
                        .zip(r.by_name("status").ok().and_then(|v| v.value::<&str>()))
                })
                .collect::<Vec<_>>();

            assert_eq!(results, vec![(((100, "usd"), "succeeded"))]);

            let results = c
                .select("SELECT * FROM stripe_customers", None, None)
                .filter_map(|r| {
                    r.by_name("id")
                        .ok()
                        .and_then(|v| v.value::<&str>())
                        .zip(r.by_name("created").ok().and_then(|v| v.value::<i64>()))
                })
                .collect::<Vec<_>>();

            assert_eq!(results, vec![("cus_MJiBgSUgeWFN0z", 1234567890000)]);

            let results = c
                .select("SELECT * FROM stripe_invoices", None, None)
                .filter_map(|r| {
                    r.by_name("customer")
                        .ok()
                        .and_then(|v| v.value::<&str>())
                        .zip(r.by_name("total").ok().and_then(|v| v.value::<i64>()))
                        .zip(r.by_name("currency").ok().and_then(|v| v.value::<&str>()))
                        .zip(r.by_name("status").ok().and_then(|v| v.value::<&str>()))
                })
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![((("cus_MJiBgSUgeWFN0z", 1000), "usd"), "draft")]
            );

            let results = c
                .select("SELECT * FROM stripe_payment_intents", None, None)
                .filter_map(|r| {
                    r.by_name("amount")
                        .ok()
                        .and_then(|v| v.value::<i64>())
                        .zip(r.by_name("currency").ok().and_then(|v| v.value::<&str>()))
                })
                .collect::<Vec<_>>();

            assert_eq!(results, vec![(1099, "usd")]);

            let results = c
                .select("SELECT * FROM stripe_products", None, None)
                .filter_map(|r| {
                    r.by_name("name")
                        .ok()
                        .and_then(|v| v.value::<&str>())
                        .zip(r.by_name("active").ok().and_then(|v| v.value::<bool>()))
                        .zip(
                            r.by_name("description")
                                .ok()
                                .and_then(|v| v.value::<&str>()),
                        )
                })
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![(("T-shirt", true), "Comfortable gray cotton t-shirt")]
            );

            let results = c
                .select("SELECT * FROM stripe_subscriptions", None, None)
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
