#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn stripe_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER stripe_wrapper
                         HANDLER stripe_fdw_handler VALIDATOR stripe_fdw_validator"#,
                None,
                None,
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
                None,
            ).unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_accounts (
                  id text,
                  business_type text,
                  country text,
                  email text,
                  type text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'accounts'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            )
            .unwrap();

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
            )
            .unwrap();

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
            )
            .unwrap();

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
            )
            .unwrap();

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
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_disputes (
                  id text,
                  amount bigint,
                  currency text,
                  charge text,
                  payment_intent text,
                  reason text,
                  status text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'disputes'    -- source object in stripe, required
                )
             "#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_events (
                  id text,
                  type text,
                  api_version text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'events'    -- source object in stripe, required
                )
             "#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_files (
                  id text,
                  filename text,
                  purpose text,
                  title text,
                  size bigint,
                  type text,
                  url text,
                  created timestamp,
                  expires_at timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'files'    -- source object in stripe, required
                )
             "#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_file_links (
                  id text,
                  file text,
                  url text,
                  created timestamp,
                  expired bool,
                  expires_at timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'file_links'    -- source object in stripe, required
                )
             "#,
                None,
                None,
            )
            .unwrap();

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
            )
            .unwrap();

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
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_payouts (
                  id text,
                  amount bigint,
                  currency text,
                  arrival_date timestamp,
                  description text,
                  statement_descriptor text,
                  status text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'payouts'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_prices (
                  id text,
                  active bool,
                  currency text,
                  product text,
                  unit_amount bigint,
                  type text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'prices'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            )
            .unwrap();

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
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_refunds (
                  id text,
                  amount bigint,
                  currency text,
                  charge text,
                  payment_intent text,
                  reason text,
                  status text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'refunds'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_setup_attempts (
                  id text,
                  application text,
                  customer text,
                  on_behalf_of text,
                  payment_method text,
                  setup_intent text,
                  status text,
                  usage text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'setup_attempts'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_setup_intents (
                  id text,
                  client_secret text,
                  customer text,
                  description text,
                  payment_method text,
                  status text,
                  usage text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                    object 'setup_intents'    -- source object in stripe, required
                  )
             "#,
                None,
                None,
            )
            .unwrap();

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
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_topups (
                  id text,
                  amount bigint,
                  currency text,
                  description text,
                  status text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                  object 'topups'    -- source object in stripe, required
                )
             "#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE stripe_transfers (
                  id text,
                  amount bigint,
                  currency text,
                  description text,
                  destination text,
                  created timestamp,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                  object 'transfers'    -- source object in stripe, required
                )
             "#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE checkout_sessions (
                  id text,
                  customer text,
                  payment_intent text,
                  subscription text,
                  attrs jsonb
                )
                SERVER my_stripe_server
                OPTIONS (
                  object 'checkout/sessions',
                  rowid_column 'id'
                )
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM stripe_accounts", None, None)
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
                    "SELECT * FROM stripe_balance WHERE balance_type IS NOT NULL",
                    None,
                    None,
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
                .select("SELECT * FROM stripe_balance_transactions", None, None)
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
                .select("SELECT * FROM stripe_charges", None, None)
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
                .select("SELECT * FROM stripe_customers", None, None)
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<Timestamp, _>("created").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![("cus_MJiBgSUgeWFN0z", Timestamp::from(287883090000000i64))]
            );

            let results = c
                .select(
                    "SELECT attrs->>'id' as id FROM stripe_customers",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["cus_MJiBgSUgeWFN0z"]);

            let results = c
                .select(
                    "SELECT attrs->>'id' as id FROM checkout_sessions",
                    None,
                    None,
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["cs_test_a1DmlfbOPqmbKHfpwpFQ0RM3pVXmKoESZbJxnKrPdMsLDPPMGYtEBcHGPR"]
            );

            // Stripe mock service cannot return 404 error code correctly for
            // non-exists customer, so we have to disable this test case.
            //
            // let results = c
            //     .select(
            //         "SELECT * FROM stripe_customers where id = 'non_exists'",
            //         None,
            //         None,
            //     ).unwrap()
            //     .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
            //     .collect::<Vec<_>>();
            // assert!(results.is_empty());

            let results = c
                .select("SELECT * FROM stripe_disputes", None, None)
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
                vec![(("dp_1Lb4lXDciZwYG8GPXn1Bh0MY", 1000), "usd")]
            );

            let results = c
                .select("SELECT * FROM stripe_events", None, None)
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("id")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("type").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![("evt_1Lb4lfDciZwYG8GPHARl3JTf", "plan.created")]
            );

            let results = c
                .select("SELECT * FROM stripe_files", None, None)
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
                            "file_1Lb4liDciZwYG8GPvkwgZXix",
                            "file_1Lb4liDciZwYG8GPvkwgZXix"
                        ),
                        "dispute_evidence"
                    ),
                    9863
                )]
            );

            let results = c
                .select("SELECT * FROM stripe_file_links", None, None)
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
                            "link_1Lb4liDciZwYG8GPQ8qAqnEa",
                            "file_1Lb4liDciZwYG8GP2VGapbrq"
                        ),
                        "https://dcr-upload-mydev.dev.stripe.me/links/MDB8YWNjdF8xTGI0bEREY2lad1lHOEdQfGZsX3Rlc3RfbFNhUld1aDYzdDd6eDYzU01RUzNzZWlE00zJ1o9SLI"
                    )
                ]
            );

            let results = c
                .select("SELECT * FROM stripe_invoices", None, None)
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
                vec![((("cus_MJiBgSUgeWFN0z", 1000), "usd"), "draft")]
            );

            let results = c
                .select("SELECT * FROM stripe_payment_intents", None, None)
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<i64, _>("amount")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("currency").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(results, vec![(1099, "usd")]);

            let results = c
                .select("SELECT * FROM stripe_payouts", None, None)
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
                vec![((("po_1Lb4lcDciZwYG8GPa5iKACTe", 1100), "usd"), "in_transit")]
            );

            let results = c
                .select("SELECT * FROM stripe_prices", None, None)
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
                        (("price_1Lb4lXDciZwYG8GPenVxKLUQ", true), "usd"),
                        "prod_MJiB8qAdQc9hgR"
                    ),
                    "recurring"
                )]
            );

            let results = c
                .select("SELECT * FROM stripe_products", None, None)
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
                .select("SELECT * FROM stripe_refunds", None, None)
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
                vec![((("re_1Lb4lXDciZwYG8GPkrV42Kaz", 100), "usd"), "succeeded")]
            );

            let results = c
                .select("SELECT * FROM stripe_setup_attempts where setup_intent='seti_1Lb4lgDciZwYG8GPdEjT5Ico'", None, None).unwrap()
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
                .select("SELECT * FROM stripe_setup_intents", None, None)
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
                    ("seti_1Lb4lgDciZwYG8GPdEjT5Ico", "requires_payment_method"),
                    "off_session"
                )]
            );

            let results = c
                .select("SELECT * FROM stripe_subscriptions", None, None)
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
                        ("cus_MJiBtCqOF1Bb3F", "usd"),
                        Timestamp::from(287883090000000i64)
                    ),
                    Timestamp::from(287883090000000i64)
                )]
            );

            let results = c
                .select("SELECT * FROM stripe_topups", None, None)
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
                vec![((("tu_1Lb4leDciZwYG8GPbKaCK9X3", 1000), "usd"), "pending")]
            );

            let results = c
                .select("SELECT * FROM stripe_transfers", None, None)
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
                    (("tr_1Lb4lcDciZwYG8GPNq6RhhYq", 1100), "usd"),
                    "acct_1Lb4lDDciZwYG8GP"
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
                INSERT INTO stripe_customers(email, name, description)
                VALUES ('test@test.com', 'test name', null)
                "#,
                None,
                None,
            );

            let results = c
                .select(
                    "SELECT * FROM stripe_customers WHERE email = 'test@test.com'",
                    None,
                    None,
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
                UPDATE stripe_customers
                SET description = 'hello fdw'
                WHERE email = 'test@test.com'
                "#,
                None,
                None,
            );

            let results = c
                .select(
                    "SELECT * FROM stripe_customers WHERE email = 'test@test.com'",
                    None,
                    None,
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
                DELETE FROM stripe_customers WHERE email = 'test@test.com'
                "#,
                None,
                None,
            );

            let results = c
                .select(
                    "SELECT * FROM stripe_customers WHERE email = 'test@test.com'",
                    None,
                    None,
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
