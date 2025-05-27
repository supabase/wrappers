#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn stripe_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER stripe_wrapper
                         HANDLER stripe_fdw_handler VALIDATOR stripe_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_stripe_server
                         FOREIGN DATA WRAPPER stripe_wrapper
                         OPTIONS (
                           api_url 'http://localhost:12111/v1',  -- Stripe API base URL, optional
                           api_key 'sk_test_51LUmojFkiV6mfx3cpEzG9VaxhA86SA4DIj3b62RKHnRC0nhPp2JBbAmQ1izsX9RKD8rlzvw2xpY54AwZtXmWciif00Qi8J0w3O'  -- Stripe API Key, required
                         )"#,
                None,
                &[],
            ).unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS stripe"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA stripe FROM SERVER my_stripe_server INTO stripe"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA stripe FROM SERVER my_stripe_server INTO stripe"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA stripe
                  LIMIT TO ("checkout_sessions", "customers", "balance", "non-exists")
                  FROM SERVER my_stripe_server INTO stripe"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA stripe
                  EXCEPT ("checkout_sessions", "customers", "balance", "non-exists")
                  FROM SERVER my_stripe_server INTO stripe"#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM stripe.accounts", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("email")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("country").unwrap())
                        .zip(r.get_by_name::<&str, _>("type").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![(("site@stripe.com", "US"), "standard")]);

            let results = c
                .select(
                    "SELECT * FROM stripe.balance WHERE balance_type IS NOT NULL",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("balance_type")
                        .unwrap()
                        .zip(r.get_by_name::<i64, _>("amount").unwrap())
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(("available", 0), "usd"), (("pending", 0), "usd")]
            );

            let results = c
                .select("SELECT * FROM stripe.balance_transactions", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<i64, _>("amount")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                        .zip(r.get_by_name::<i64, _>("fee").unwrap())
                        .zip(r.get_by_name::<&str, _>("status").unwrap())
                        .zip(r.get_by_name::<&str, _>("type").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![((((100, "usd"), 0), "available"), "charge")]);

            let results = c
                .select("SELECT * FROM stripe.charges", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<i64, _>("amount")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                        .zip(r.get_by_name::<&str, _>("status").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![((100, "usd"), "succeeded")]);

            let results = c
                .select("SELECT * FROM stripe.customers", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<Timestamp, _>("created").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(
                    "cus_QXg1o8vcGmoR32",
                    Timestamp::try_from(287883090000000i64).unwrap()
                )]
            );

            let results = c
                .select(
                    "SELECT attrs->>'id' as id FROM stripe.customers",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["cus_QXg1o8vcGmoR32"]);

            let results = c
                .select(
                    "SELECT id, display_name FROM stripe.billing_meters",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["meter_123"]);

            let results = c
                .select(
                    "SELECT attrs->>'id' as id FROM stripe.checkout_sessions",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["cs_test_a1YS1URlnyQCN5fUUduORoQ7Pw41PJqDWkIVQCpJPqkfIhd6tVY8XB1OLY"]
            );

            // Stripe mock service cannot return 404 error code correctly for
            // non-exists customer, so we have to disable this test case.
            //
            // let results = c
            //     .select(
            //         "SELECT * FROM stripe.customers where id = 'non_exists'",
            //         None,
            //         None,
            //     ).unwrap()
            //     .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
            //     .collect::<Vec<_>>();
            // assert!(results.is_empty());

            let results = c
                .select("SELECT * FROM stripe.disputes", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<i64, _>("amount").unwrap())
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(("dp_1Pgc71B7WZ01zgkWMevJiAUx", 1000), "usd")]
            );

            let results = c
                .select("SELECT * FROM stripe.events", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("type").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![("evt_1Pgc76B7WZ01zgkWwyRHS12y", "plan.created")]
            );

            let results = c
                .select("SELECT * FROM stripe.files", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("filename").unwrap())
                        .zip(r.get_by_name::<&str, _>("purpose").unwrap())
                        .zip(r.get_by_name::<i64, _>("size").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(
                    (
                        (
                            "file_1Pgag2B7WZ01zgkWITx3dIQc",
                            "file_1Pgag2B7WZ01zgkWITx3dIQc"
                        ),
                        "dispute_evidence"
                    ),
                    9863
                )]
            );

            let results = c
                .select("SELECT * FROM stripe.file_links", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("file").unwrap())
                        .zip(r.get_by_name::<&str, _>("url").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![((
                            "link_1Pgc76B7WZ01zgkWPhd77i13",
                            "file_1Pgag2B7WZ01zgkWITx3dIQc"
                        ),
                        "https://sangeekp-15t6ai--upload-mydev.dev.stripe.me/links/MDB8YWNjdF8xUGdhZlRCN1daMDF6Z2tXfGZsX3Rlc3Rfb0Jkam9sNHdEZUpXRHUzSGhXNTRkZDI500qGiHOxxv"
                    )
                ]
            );

            let results = c
                .select("SELECT * FROM stripe.invoices", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("customer")
                        .unwrap()
                        .zip(r.get_by_name::<i64, _>("total").unwrap())
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                        .zip(r.get_by_name::<&str, _>("status").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![((("cus_QXg1o8vcGmoR32", 1000), "usd"), "draft")]
            );

            let results = c
                .select("SELECT * FROM stripe.payment_intents", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<i64, _>("amount")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![(1099, "usd")]);

            let results = c
                .select("SELECT * FROM stripe.payouts", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<i64, _>("amount").unwrap())
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                        .zip(r.get_by_name::<&str, _>("status").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![((("po_1Pgc79B7WZ01zgkWu1KToYf4", 1100), "usd"), "in_transit")]
            );

            let results = c
                .select("SELECT * FROM stripe.prices", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<bool, _>("active").unwrap())
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                        .zip(r.get_by_name::<&str, _>("product").unwrap())
                        .zip(r.get_by_name::<&str, _>("type").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(
                    (
                        (("price_1PgafmB7WZ01zgkW6dKueIc5", true), "usd"),
                        "prod_QXg1hqf4jFNsqG"
                    ),
                    "recurring"
                )]
            );

            let results = c
                .select("SELECT * FROM stripe.products", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("name")
                        .unwrap()
                        .zip(r.get_by_name::<bool, _>("active").unwrap())
                        .zip(r.get_by_name::<&str, _>("description").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(("T-shirt", true), "Comfortable gray cotton t-shirt")]
            );

            let results = c
                .select("SELECT * FROM stripe.refunds", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<i64, _>("amount").unwrap())
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                        .zip(r.get_by_name::<&str, _>("status").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![((("re_1Pgc72B7WZ01zgkWqPvrRrPE", 100), "usd"), "succeeded")]
            );

            let results = c
                .select("SELECT * FROM stripe.setup_attempts where setup_intent='seti_1Pgag7B7WZ01zgkWSgwGdb8Z'", None, &[]).unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("status").unwrap())
                        .zip(r.get_by_name::<&str, _>("usage").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(
                    ("setatt_123456789012345678901234", "succeeded"),
                    "off_session"
                )]
            );

            let results = c
                .select("SELECT * FROM stripe.setup_intents", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("status").unwrap())
                        .zip(r.get_by_name::<&str, _>("usage").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(
                    ("seti_1Pgag7B7WZ01zgkWSgwGdb8Z", "requires_payment_method"),
                    "off_session"
                )]
            );

            let results = c
                .select("SELECT * FROM stripe.subscriptions", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("customer")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                        .zip(
                            r.get_by_name::<Timestamp, _>("current_period_start")
                                .unwrap(),
                        )
                        .zip(r.get_by_name::<Timestamp, _>("current_period_end").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(
                    (
                        ("cus_QXg1o8vcGmoR32", "usd"),
                        Timestamp::try_from(287883090000000i64).unwrap()
                    ),
                    Timestamp::try_from(287883090000000i64).unwrap()
                )]
            );

            let results = c
                .select("SELECT * FROM stripe.topups", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<i64, _>("amount").unwrap())
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                        .zip(r.get_by_name::<&str, _>("status").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![((("tu_1Pgc7BB7WZ01zgkWwH7rkBgR", 1000), "usd"), "pending")]
            );

            let results = c
                .select("SELECT * FROM stripe.transfers", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<i64, _>("amount").unwrap())
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                        .zip(r.get_by_name::<&str, _>("destination").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![(
                    (("tr_1Pgc7BB7WZ01zgkWVJfE40RX", 1100), "usd"),
                    "acct_1PgafTB7WZ01zgkW"
                )]
            );

            // Stripe mock container is currently stateless, so we cannot test
            // data modify for now but will keep the code below for future use.
            //
            // ref: https://github.com/stripe/stripe-mock

            /*
            // test insert
            c.update(
                r#"
                INSERT INTO stripe.customers(email, name, description)
                VALUES ('test@test.com', 'test name', null)
                "#,
                None,
                &[],
            );

            let results = c
                .select(
                    "SELECT * FROM stripe.customers WHERE email = 'test@test.com'",
                    None,
                    &[],
                ).unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("email")
                        .unwrap()
                        .and_then(|v| v.value::<&str>())
                        .zip(r.get_by_name::<&str, _>("name").unwrap().and_then(|v| v.value::<&str>()))
                })
                .collect::<Vec<_>>();

            assert_eq!(results, vec![("test@test.com", "test name")]);

            // test update
            c.update(
                r#"
                UPDATE stripe.customers
                SET description = 'hello fdw'
                WHERE email = 'test@test.com'
                "#,
                None,
                &[],
            );

            let results = c
                .select(
                    "SELECT * FROM stripe.customers WHERE email = 'test@test.com'",
                    None,
                    &[],
                ).unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("email").unwrap().and_then(|v| v.value::<&str>()).zip(
                        r.get_by_name::<&str, _>("description")
                            .unwrap()
                            .and_then(|v| v.value::<&str>()),
                    )
                })
                .collect::<Vec<_>>();

            assert_eq!(results, vec![("test@test.com", "hello fdw")]);

            // test delete
            c.update(
                r#"
                DELETE FROM stripe.customers WHERE email = 'test@test.com'
                "#,
                None,
                &[],
            );

            let results = c
                .select(
                    "SELECT * FROM stripe.customers WHERE email = 'test@test.com'",
                    None,
                    &[],
                ).unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("email").unwrap().and_then(|v| v.value::<&str>()).zip(
                        r.get_by_name::<&str, _>("description")
                            .unwrap()
                            .and_then(|v| v.value::<&str>()),
                    )
                })
                .collect::<Vec<_>>();

            assert!(results.is_empty());
            */
        });
    }
}
