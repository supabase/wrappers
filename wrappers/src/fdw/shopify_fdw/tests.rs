#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    use pgrx::spi::SpiClient;

    fn setup(c: &mut SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN DATA WRAPPER shopify_wrapper
               HANDLER shopify_fdw_handler VALIDATOR shopify_fdw_validator"#,
            None,
            &[],
        )
        .unwrap();
        c.update(
            r#"CREATE SERVER my_shopify_server
               FOREIGN DATA WRAPPER shopify_wrapper
               OPTIONS (
                 shop         'test-store',
                 api_url      'http://localhost:12112',
                 access_token 'shppa_test_token'
               )"#,
            None,
            &[],
        )
        .unwrap();
        c.update("CREATE SCHEMA IF NOT EXISTS shopify", None, &[])
            .unwrap();
        c.update(
            "IMPORT FOREIGN SCHEMA shopify FROM SERVER my_shopify_server INTO shopify",
            None,
            &[],
        )
        .unwrap();
    }

    // Flow: a merchant browses and manages their product catalog.
    // Covers products, collections, product_variants, and IMPORT FOREIGN SCHEMA.
    #[pg_test]
    fn product_catalog_management() {
        Spi::connect_mut(|c| {
            setup(c);

            // IMPORT FOREIGN SCHEMA is idempotent
            c.update(
                "IMPORT FOREIGN SCHEMA shopify FROM SERVER my_shopify_server INTO shopify",
                None,
                &[],
            )
            .unwrap();

            // LIMIT TO and EXCEPT are supported
            c.update(
                r#"IMPORT FOREIGN SCHEMA shopify
                   LIMIT TO ("products", "non-exists")
                   FROM SERVER my_shopify_server INTO shopify"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA shopify
                   EXCEPT ("products", "non-exists")
                   FROM SERVER my_shopify_server INTO shopify"#,
                None,
                &[],
            )
            .unwrap();

            // Merchant lists their product catalog
            let products = c
                .select(
                    "SELECT id, title, status, vendor FROM shopify.products",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("title")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("status").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(products, vec![("T-shirt", "ACTIVE")]);

            // Merchant looks up a specific product by ID (triggers direct fetch)
            let product = c
                .select(
                    "SELECT id, title FROM shopify.products WHERE id = 'gid://shopify/Product/1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(product, vec!["gid://shopify/Product/1"]);

            // Merchant accesses full raw product data via attrs
            let from_attrs = c
                .select(
                    "SELECT attrs->>'title' AS title, attrs->>'vendor' AS vendor FROM shopify.products",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("title")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("vendor").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(from_attrs, vec![("T-shirt", "Acme")]);

            // Merchant inspects variants to manage pricing and inventory
            let variants = c
                .select(
                    "SELECT sku, price, inventory_quantity, taxable FROM shopify.product_variants",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("sku")
                        .unwrap()
                        .zip(r.get_by_name::<i64, _>("inventory_quantity").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(variants, vec![("SKU-001", 42)]);

            // Merchant organises products into collections
            let collections = c
                .select(
                    "SELECT title, sort_order FROM shopify.collections",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("title")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("sort_order").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(collections, vec![("Summer Collection", "MANUAL")]);
        });
    }

    // Flow: operations team tracks order fulfilment end-to-end.
    // Covers orders, fulfillment_orders, fulfillments, and refunds.
    #[pg_test]
    fn order_fulfillment_tracking() {
        Spi::connect_mut(|c| {
            setup(c);

            // Ops lists open orders awaiting fulfilment
            let orders = c
                .select(
                    "SELECT id, name, financial_status, fulfillment_status FROM shopify.orders",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("name")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("financial_status").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(orders, vec![("#1001", "PAID")]);

            // Ops checks which fulfilment orders are still open
            let fulfillment_orders = c
                .select(
                    "SELECT id, status, request_status FROM shopify.fulfillment_orders",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("status").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(fulfillment_orders, vec!["OPEN"]);

            // Ops drills into the fulfilment for a specific order
            let fulfillments = c
                .select(
                    "SELECT id, status FROM shopify.fulfillments WHERE order_id = 'gid://shopify/Order/1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("status").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(fulfillments, vec!["SUCCESS"]);

            // Querying fulfillments without specifying an order returns nothing
            // (the FDW needs a parent context to fetch nested resources)
            let no_qual = c
                .select("SELECT id FROM shopify.fulfillments", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert!(no_qual.is_empty());

            // Finance checks whether a refund was issued on this order
            let refunds = c
                .select(
                    "SELECT id FROM shopify.refunds WHERE order_id = 'gid://shopify/Order/1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(refunds, vec!["gid://shopify/Refund/1"]);
        });
    }

    // Flow: support agent resolves a customer inquiry.
    // Covers customers, customer_payment_methods, store_credit_accounts, returns.
    #[pg_test]
    fn customer_support_lookup() {
        Spi::connect_mut(|c| {
            setup(c);

            // Agent searches the customer list
            let customers = c
                .select(
                    "SELECT id, first_name, last_name, email FROM shopify.customers",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("email")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("first_name").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(customers, vec![("alice@example.com", "Alice")]);

            // Agent fetches the customer record directly by ID
            let customer = c
                .select(
                    "SELECT id FROM shopify.customers WHERE id = 'gid://shopify/Customer/1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(customer, vec!["gid://shopify/Customer/1"]);

            // Agent checks whether the customer's payment method can be revoked
            let payment_method = c
                .select(
                    "SELECT id, is_revocable FROM shopify.customer_payment_methods WHERE id = 'gid://shopify/CustomerPaymentMethod/1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<bool, _>("is_revocable").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(payment_method, vec![true]);

            // Listing payment methods without an ID is not supported
            let no_qual = c
                .select("SELECT id FROM shopify.customer_payment_methods", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert!(no_qual.is_empty());

            // Agent checks the store credit account
            let credit = c
                .select(
                    "SELECT id FROM shopify.store_credit_accounts WHERE id = 'gid://shopify/StoreCreditAccount/1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(credit, vec!["gid://shopify/StoreCreditAccount/1"]);

            // Agent reviews a return the customer filed
            let returns = c
                .select(
                    "SELECT id, status FROM shopify.returns WHERE id = 'gid://shopify/Return/1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("status").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(returns, vec!["OPEN"]);
        });
    }

    // Flow: analyst queries store configuration and operational data.
    // Covers shop, locations, inventory_levels, apps, business_entities.
    #[pg_test]
    fn store_operations_overview() {
        Spi::connect_mut(|c| {
            setup(c);

            // Analyst reads the store's base configuration
            let shop = c
                .select(
                    "SELECT name, email, currency_code FROM shopify.shop",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("name")
                        .unwrap()
                        .zip(r.get_by_name::<&str, _>("currency_code").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(shop, vec![("Test Store", "USD")]);

            // Analyst lists warehouse locations to plan fulfilment routing
            let locations = c
                .select("SELECT name, is_active FROM shopify.locations", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("name")
                        .unwrap()
                        .zip(r.get_by_name::<bool, _>("is_active").unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(locations, vec![("Main Warehouse", true)]);

            // Analyst checks available stock at a specific location
            let inventory = c
                .select(
                    "SELECT id, available FROM shopify.inventory_levels WHERE id = 'gid://shopify/InventoryLevel/1'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("available").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(inventory, vec![100]);

            // Analyst audits which apps are installed
            let apps = c
                .select("SELECT title FROM shopify.apps", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("title").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(apps, vec!["My Test App"]);

            // Analyst reviews registered business entities
            let entities = c
                .select("SELECT name FROM shopify.business_entities", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(entities, vec!["Acme Corp"]);
        });
    }

    // Flow: merchant creates, updates, and removes products and customers.
    // Also covers draft orders. Mock is stateless so reads after writes are not tested.
    #[pg_test]
    fn merchant_write_operations() {
        Spi::connect_mut(|c| {
            setup(c);

            // Merchant launches a new product
            c.update(
                "INSERT INTO shopify.products(title, vendor, status) VALUES ('Summer Hat', 'Acme', 'ACTIVE')",
                None,
                &[],
            )
            .unwrap();

            // Merchant edits an existing product
            c.update(
                "UPDATE shopify.products SET title = 'Premium Summer Hat' WHERE id = 'gid://shopify/Product/1'",
                None,
                &[],
            )
            .unwrap();

            // Merchant removes a discontinued product
            c.update(
                "DELETE FROM shopify.products WHERE id = 'gid://shopify/Product/1'",
                None,
                &[],
            )
            .unwrap();

            // Merchant onboards a new customer
            c.update(
                "INSERT INTO shopify.customers(first_name, last_name, email) VALUES ('Jane', 'Doe', 'jane@example.com')",
                None,
                &[],
            )
            .unwrap();

            // Merchant removes a customer
            c.update(
                "DELETE FROM shopify.customers WHERE id = 'gid://shopify/Customer/1'",
                None,
                &[],
            )
            .unwrap();

            // Merchant creates a draft order for a B2B quote
            c.update(
                "INSERT INTO shopify.draft_orders(email, note) VALUES ('buyer@corp.com', 'Q2 2025 quote')",
                None,
                &[],
            )
            .unwrap();

            // Merchant creates a collection for seasonal merchandising
            c.update(
                "INSERT INTO shopify.collections(title, description) VALUES ('Winter 2025', 'Cold weather essentials')",
                None,
                &[],
            )
            .unwrap();
        });
    }
}
