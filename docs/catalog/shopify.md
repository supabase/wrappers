---
source:
documentation:
author: supabase
tags:
  - wasm
  - official
---

# Shopify

[Shopify](https://shopify.com/) is an e-commerce platform that provides businesses with the tools to create online stores, sell online and in person.

The Shopify Wrapper is a WebAssembly(Wasm) foreign data wrapper which allows you to read data from Shopify for use within your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                                    | Checksum                                                           | Required Wrappers Version |
| ------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------- |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_shopify_fdw_v0.1.0/shopify_fdw.wasm`   | `96ff21191d46f60cddca7ee456b6f6ddf206602d63b8a0e97243a2a8d1303166` | >=0.5.0                   |

## Preparation

Before you can query Shopify, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Shopify Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Shopify Admin API access token in Vault and retrieve the created `key_id`
select vault.create_secret(
  '<Shopify API token>', -- Shopify API token
  'Shopify',
  'Shopify API token for Wrappers'
);
```

### Connecting to Shopify

We need to provide Postgres with the credentials to access Shopify and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server shopify_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_shopify_fdw_v0.1.0/shopify_fdw.wasm',
        fdw_package_name 'supabase:shopify-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '96ff21191d46f60cddca7ee456b6f6ddf206602d63b8a0e97243a2a8d1303166',
        shop '<Shop_ID>',  -- Shop ID, e.g. teststore-0b5a
        access_token_id '<key_ID>' -- The Key ID from above.
      );
    ```

=== "Without Vault"

    ```sql
    create server shopify_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_shopify_fdw_v0.1.0/shopify_fdw.wasm',
        fdw_package_name 'supabase:shopify-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '96ff21191d46f60cddca7ee456b6f6ddf206602d63b8a0e97243a2a8d1303166',
        shop '<Shop_ID>',  -- Shop ID, e.g. teststore-0b5a
        access_token 'shpat_6c3a2...'  -- Shopify Admin Access Token
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists shopify;
```

## Options

The full list of foreign table options are below:

- `object` - Object name in Shopify, required.

Supported objects are listed below:

| Object name               |
| ------------------------- |
| app                       |
| businessEntities          |
| collections               |
| customerPaymentMethod     |
| customers                 |
| draftOrders               |
| fulfillment               |
| fulfillmentOrders         |
| inventoryLevel            |
| locations                 |
| orders                    |
| productVariants           |
| products                  |
| refund                    |
| return                    |
| shop                      |
| storeCreditAccount        |

## Entities

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from Shopify.

For example, using below SQL can automatically create foreign tables in the `shopify` schema.

```sql
-- create all the foreign tables
import foreign schema shopify from server shopify_server into shopify;

-- or, create selected tables only
import foreign schema shopify
   limit to ("customers", "products")
   from server shopify_server into shopify;

-- or, create all foreign tables except selected tables
import foreign schema shopify
   except ("customers")
   from server shopify_server into shopify;
```

### App

A Shopify application.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/app)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| app                   |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.app (
  "apiKey" text,
  "appStoreAppUrl" text,
  "appStoreDeveloperUrl" text,
  "availableAccessScopes" jsonb,
  banner jsonb,
  description text,
  "developerName" text,
  "developerType" text,
  embedded bool,
  "failedRequirements" jsonb,
  features jsonb,
  feedback jsonb,
  handle text,
  icon jsonb,
  id text,
  "installUrl" text,
  installation jsonb,
  "isPostPurchaseAppInUse" bool,
  "optionalAccessScopes" jsonb,
  "previouslyInstalled" bool,
  "pricingDetails" text,
  "pricingDetailsSummary" text,
  "privacyPolicyUrl" text,
  "publicCategory" text,
  published bool,
  "requestedAccessScopes" jsonb,
  screenshots jsonb,
  "shopifyDeveloped" bool,
  title text,
  "uninstallMessage" text,
  "webhookApiVersion" text,
  attrs jsonb
)
  server shopify_server
  options (
    object 'app'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### BusinessEntity

Represents a merchant's Business Entity.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/businessentity)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| businessEntities      |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.businessEntities (
  address jsonb,
  archived bool,
  "companyName" text,
  "displayName" text,
  id text,
  "primary" bool,
  "shopifyPaymentsAccount" jsonb,
  attrs jsonb
)
  server shopify_server
  options (
    object 'businessEntities'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Collection

The Collection object represents a group of products that merchants can organize to make their stores easier to browse and help customers find related products.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/collection)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| collections           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.collections (
  "availablePublicationsCount" jsonb,
  description text,
  "descriptionHtml" text,
  events jsonb,
  feedback jsonb,
  handle text,
  "hasProduct" bool,
  id text,
  image jsonb,
  "legacyResourceId" bigint,
  metafields jsonb,
  products jsonb,
  "productsCount" jsonb,
  "publishedOnCurrentPublication" bool,
  "publishedOnPublication" bool,
  "resourcePublications" jsonb,
  "resourcePublicationsCount" jsonb,
  "resourcePublicationsV2" jsonb,
  "ruleSet" jsonb,
  seo jsonb,
  "sortOrder" text,
  "templateSuffix" text,
  title text,
  "unpublishedPublications" jsonb,
  "updatedAt" timestamp,
  attrs jsonb
)
  server shopify_server
  options (
    object 'collections'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### CustomerPaymentMethod

A customer's payment method.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/customerpaymentmethod)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| customerPaymentMethod |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.customerPaymentMethod (
  customer jsonb,
  id text,
  instrument text,
  "revokedAt" timestamp,
  "revokedReason" text,
  "subscriptionContracts" jsonb,
  attrs jsonb
)
  server shopify_server
  options (
    object 'customerPaymentMethod'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Query on this table must specify an value for `id` column

### Customer

Represents information about a customer of the shop

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/customer)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| customers             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.customers (
  addresses jsonb,
  "addressesV2" jsonb,
  "amountSpent" jsonb,
  "canDelete" bool,
  "companyContactProfiles" jsonb,
  "createdAt" timestamp,
  "dataSaleOptOut" bool,
  "defaultAddress" jsonb,
  "defaultEmailAddress" jsonb,
  "defaultPhoneNumber" jsonb,
  "displayName" text,
  events jsonb,
  "firstName" text,
  id text,
  image jsonb,
  "lastName" text,
  "lastOrder" jsonb,
  "legacyResourceId" bigint,
  "lifetimeDuration" text,
  locale text,
  mergeable jsonb,
  metafields jsonb,
  "multipassIdentifier" text,
  note text,
  "numberOfOrders" bigint,
  orders jsonb,
  "paymentMethods" jsonb,
  "productSubscriberStatus" text,
  state text,
  "statistics" jsonb,
  "storeCreditAccounts" jsonb,
  "subscriptionContracts" jsonb,
  tags text,
  "taxExempt" bool,
  "taxExemptions" text,
  "updatedAt" timestamp,
  "verifiedEmail" bool,
  attrs jsonb
)
  server shopify_server
  options (
    object 'customers'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### DraftOrder

An order that a merchant creates on behalf of a customer.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/draftorder)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| draftOrders           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.draftorders (
  "acceptAutomaticDiscounts" bool,
  "allVariantPricesOverridden" bool,
  "allowDiscountCodesInCheckout" bool,
  "anyVariantPricesOverridden" bool,
  "appliedDiscount" jsonb,
  "billingAddress" jsonb,
  "billingAddressMatchesShippingAddress" bool,
  "completedAt" timestamp,
  "createdAt" timestamp,
  "currencyCode" text,
  "customAttributes" jsonb,
  customer jsonb,
  "defaultCursor" text,
  "discountCodes" jsonb,
  email text,
  events jsonb,
  "hasTimelineComment" bool,
  id text,
  "invoiceEmailTemplateSubject" text,
  "invoiceSentAt" timestamp,
  "invoiceUrl" text,
  "legacyResourceId" bigint,
  "lineItems" jsonb,
  "lineItemsSubtotalPrice" jsonb,
  "localizedFields" jsonb,
  metafields jsonb,
  "name" text,
  note2 text,
  "order" jsonb,
  "paymentTerms" jsonb,
  phone text,
  "platformDiscounts" jsonb,
  "poNumber" text,
  "presentmentCurrencyCode" text,
  "purchasingEntity" text,
  ready bool,
  "reserveInventoryUntil" timestamp,
  "shippingAddress" jsonb,
  "shippingLine" jsonb,
  status text,
  "subtotalPriceSet" jsonb,
  tags jsonb,
  "taxExempt" bool,
  "taxLines" jsonb,
  "taxesIncluded" bool,
  "totalDiscountsSet" jsonb,
  "totalLineItemsPriceSet" jsonb,
  "totalPriceSet" jsonb,
  "totalQuantityOfLineItems" bigint,
  "totalShippingPriceSet" jsonb,
  "totalTaxSet" jsonb,
  "totalWeight" bigint,
  "transformerFingerprint" text,
  "updatedAt" timestamp,
  "visibleToCustomer" bool,
  warnings jsonb,
  attrs jsonb
)
  server shopify_server
  options (
    object 'draftOrders'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Fulfillment

Represents a fulfillment.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/fulfillment)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| fulfillment           |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.fulfillment (
  "createdAt" timestamp,
  "deliveredAt" timestamp,
  "displayStatus" text,
  "estimatedDeliveryAt" timestamp,
  events jsonb,
  "fulfillmentLineItems" jsonb,
  "fulfillmentOrders" jsonb,
  id text,
  "inTransitAt" timestamp,
  "legacyResourceId" bigint,
  "location" jsonb,
  "name" text,
  "order" jsonb,
  "originAddress" jsonb,
  "requiresShipping" bool,
  service jsonb,
  status text,
  "totalQuantity" bigint,
  "trackingInfo" jsonb,
  "updatedAt" timestamp,
  attrs jsonb
)
  server shopify_server
  options (
    object 'fulfillment'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Query on this table must specify an value for `id` column

### FulfillmentOrder

The FulfillmentOrder object represents either an item or a group of items in an Order that are expected to be fulfilled from the same location.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/fulfillmentorder)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| fulfillmentOrders     |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.fulfillmentorders (
  "assignedLocation" jsonb,
  "channelId" text,
  "createdAt" timestamp,
  "deliveryMethod" jsonb,
  destination jsonb,
  "fulfillAt" timestamp,
  "fulfillBy" timestamp,
  "fulfillmentHolds" jsonb,
  "fulfillmentOrdersForMerge" jsonb,
  fulfillments jsonb,
  id text,
  "internationalDuties" jsonb,
  "lineItems" jsonb,
  "locationsForMove" jsonb,
  "merchantRequests" jsonb,
  "order" jsonb,
  "orderId" text,
  "orderName" text,
  "orderProcessedAt" timestamp,
  "requestStatus" text,
  status text,
  "supportedActions" jsonb,
  "updatedAt" timestamp,
  attrs jsonb
)
  server shopify_server
  options (
    object 'fulfillmentOrders'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### InventoryLevel

The quantities of an inventory item that are related to a specific location.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/inventorylevel)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| inventoryLevel        |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.inventorylevel (
  "canDeactivate" bool,
  "createdAt" timestamp,
  "deactivationAlert" text,
  id text,
  item jsonb,
  "location" jsonb,
  "scheduledChanges" jsonb,
  "updatedAt" timestamp,
  attrs jsonb
)
  server shopify_server
  options (
    object 'inventoryLevel'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Query on this table must specify an value for `id` column

### Location

Represents the location where the physical good resides.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/location)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| locations             |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.locations (
  activatable bool,
  address jsonb,
  "addressVerified" bool,
  "createdAt" timestamp,
  deactivatable bool,
  "deactivatedAt" text,
  deletable bool,
  "fulfillmentService" jsonb,
  "fulfillsOnlineOrders" bool,
  "hasActiveInventory" bool,
  "hasUnfulfilledOrders" bool,
  id text,
  "inventoryLevels" jsonb,
  "isActive" bool,
  "isFulfillmentService" bool,
  "legacyResourceId" bigint,
  "localPickupSettingsV2" jsonb,
  metafields jsonb,
  "name" text,
  "shipsInventory" bool,
  "suggestedAddresses" jsonb,
  "updatedAt" timestamp,
  attrs jsonb
)
  server shopify_server
  options (
    object 'locations'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Order

The Order object represents a customer's request to purchase one or more products from a store.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/order)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| orders                |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.orders (
  "additionalFees" jsonb,
  agreements jsonb,
  alerts jsonb,
  app jsonb,
  "billingAddress" jsonb,
  "billingAddressMatchesShippingAddress" bool,
  "canMarkAsPaid" bool,
  "canNotifyCustomer" bool,
  "cancelReason" text,
  cancellation jsonb,
  "cancelledAt" timestamp,
  capturable bool,
  "cartDiscountAmountSet" jsonb,
  "channelInformation" jsonb,
  "clientIp" text,
  closed bool,
  "closedAt" timestamp,
  "confirmationNumber" text,
  confirmed bool,
  "createdAt" timestamp,
  "currencyCode" text,
  "currentCartDiscountAmountSet" jsonb,
  "currentShippingPriceSet" jsonb,
  "currentSubtotalLineItemsQuantity" bigint,
  "currentSubtotalPriceSet" jsonb,
  "currentTaxLines" jsonb,
  "currentTotalAdditionalFeesSet" jsonb,
  "currentTotalDiscountsSet" jsonb,
  "currentTotalDutiesSet" jsonb,
  "currentTotalPriceSet" jsonb,
  "currentTotalTaxSet" jsonb,
  "currentTotalWeight" bigint,
  "customAttributes" jsonb,
  customer jsonb,
  "customerAcceptsMarketing" bool,
  "customerJourneySummary" jsonb,
  "customerLocale" text,
  "discountApplications" jsonb,
  "discountCode" text,
  "discountCodes" jsonb,
  "displayAddress" jsonb,
  "displayFinancialStatus" text,
  "displayFulfillmentStatus" text,
  disputes jsonb,
  "dutiesIncluded" bool,
  edited bool,
  email text,
  "estimatedTaxes" bool,
  events jsonb,
  fulfillable bool,
  "fulfillmentOrders" jsonb,
  fulfillments jsonb,
  "fulfillmentsCount" jsonb,
  "fullyPaid" bool,
  "hasTimelineComment" bool,
  id text,
  "legacyResourceId" bigint,
  "lineItems" jsonb,
  "localizedFields" jsonb,
  "merchantBusinessEntity" jsonb,
  "merchantEditable" bool,
  "merchantEditableErrors" jsonb,
  "merchantOfRecordApp" jsonb,
  metafields jsonb,
  "name" text,
  "netPaymentSet" jsonb,
  "nonFulfillableLineItems" jsonb,
  note text,
  "number" bigint,
  "originalTotalAdditionalFeesSet" jsonb,
  "originalTotalDutiesSet" jsonb,
  "originalTotalPriceSet" jsonb,
  "paymentCollectionDetails" jsonb,
  "paymentGatewayNames" text,
  "paymentTerms" jsonb,
  phone text,
  "poNumber" text,
  "presentmentCurrencyCode" text,
  "processedAt" timestamp,
  "publication" jsonb,
  "refundDiscrepancySet" jsonb,
  refundable bool,
  refunds jsonb,
  "registeredSourceUrl" text,
  "requiresShipping" bool,
  restockable bool,
  "retailLocation" jsonb,
  "returnStatus" text,
  "returns" jsonb,
  risk jsonb,
  "shippingAddress" jsonb,
  "shippingLine" jsonb,
  "shippingLines" jsonb,
  "shopifyProtect" jsonb,
  "sourceIdentifier" text,
  "sourceName" text,
  "staffMember" jsonb,
  "statusPageUrl" text,
  "subtotalLineItemsQuantity" bigint,
  "subtotalPriceSet" jsonb,
  "suggestedRefund" jsonb,
  tags jsonb,
  "taxExempt" bool,
  "taxLines" jsonb,
  "taxesIncluded" bool,
  test bool,
  "totalCapturableSet" jsonb,
  "totalCashRoundingAdjustment" jsonb,
  "totalDiscountsSet" jsonb,
  "totalOutstandingSet" jsonb,
  "totalPriceSet" jsonb,
  "totalReceivedSet" jsonb,
  "totalRefundedSet" jsonb,
  "totalRefundedShippingSet" jsonb,
  "totalShippingPriceSet" jsonb,
  "totalTaxSet" jsonb,
  "totalTipReceivedSet" jsonb,
  "totalWeight" bigint,
  transactions jsonb,
  "transactionsCount" jsonb,
  unpaid bool,
  "updatedAt" timestamp,
  attrs jsonb
)
  server shopify_server
  options (
    object 'orders'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Product

The Product object lets you manage products in a merchant’s store.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/product)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| products              |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.products  (
  "availablePublicationsCount" jsonb,
  "bundleComponents" jsonb,
  category jsonb,
  collections jsonb,
  "combinedListing" jsonb,
  "combinedListingRole" text,
  "compareAtPriceRange" jsonb,
  "createdAt" timestamp,
  "defaultCursor" text,
  description text,
  "descriptionHtml" text,
  events jsonb,
  "featuredMedia" jsonb,
  feedback jsonb,
  "giftCardTemplateSuffix" text,
  handle text,
  "hasOnlyDefaultVariant" bool,
  "hasOutOfStockVariants" bool,
  "hasVariantsThatRequiresComponents" bool,
  id text,
  "inCollection" bool,
  "isGiftCard" bool,
  "legacyResourceId" bigint,
  media jsonb,
  "mediaCount" jsonb,
  metafields jsonb,
  "onlineStorePreviewUrl" text,
  "onlineStoreUrl" text,
  "options" jsonb,
  "priceRangeV2" jsonb,
  "productComponents" jsonb,
  "productComponentsCount" jsonb,
  "productParents" jsonb,
  "productType" text,
  "publishedAt" timestamp,
  "publishedInContext" bool,
  "publishedOnCurrentPublication" bool,
  "publishedOnPublication" bool,
  "requiresSellingPlan" bool,
  "resourcePublicationOnCurrentPublication" jsonb,
  "resourcePublications" jsonb,
  "resourcePublicationsCount" jsonb,
  "resourcePublicationsV2" jsonb,
  "sellingPlanGroups" jsonb,
  "sellingPlanGroupsCount" jsonb,
  seo jsonb,
  status text,
  tags jsonb,
  "templateSuffix" text,
  title text,
  "totalInventory" bigint,
  "tracksInventory" bool,
  "unpublishedPublications" jsonb,
  "updatedAt" timestamp,
  variants jsonb,
  "variantsCount" jsonb,
  vendor text,
  attrs jsonb
)
  server shopify_server
  options (
    object 'products'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### ProductVariant

The ProductVariant object represents a version of a product that comes in more than one option, such as size or color.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/productvariant)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| productVariants       |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.productvariants (
  "availableForSale" bool,
  barcode text,
  "compareAtPrice" text,
  "createdAt" timestamp,
  "defaultCursor" text,
  "deliveryProfile" jsonb,
  "displayName" text,
  events jsonb,
  id text,
  image jsonb,
  "inventoryItem" jsonb,
  "inventoryPolicy" text,
  "inventoryQuantity" bigint,
  "legacyResourceId" bigint,
  media jsonb,
  metafields jsonb,
  "position" bigint,
  price numeric,
  product jsonb,
  "productParents" jsonb,
  "productVariantComponents" jsonb,
  "requiresComponents" bool,
  "selectedOptions" jsonb,
  "sellableOnlineQuantity" bigint,
  "sellingPlanGroups" jsonb,
  "sellingPlanGroupsCount" jsonb,
  "showUnitPrice" bool,
  sku text,
  taxable bool,
  title text,
  "unitPrice" jsonb,
  "unitPriceMeasurement" jsonb,
  "updatedAt" timestamp,
  attrs jsonb
)
  server shopify_server
  options (
    object 'productVariants'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### Refund

The Refund object represents a financial record of money returned to a customer from an order.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/refund)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| refund                |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.refund (
  "createdAt" timestamp,
  duties jsonb,
  id text,
  "legacyResourceId" bigint,
  note text,
  "order" jsonb,
  "orderAdjustments" jsonb,
  "refundLineItems" jsonb,
  "refundShippingLines" jsonb,
  "return" jsonb,
  "staffMember" jsonb,
  "totalRefundedSet" jsonb,
  transactions jsonb,
  "updatedAt" timestamp,
  attrs jsonb
)
  server shopify_server
  options (
    object 'refund'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Query on this table must specify an value for `id` column

### Return

The Return object represents the intent of a buyer to ship one or more items from an order back to a merchant or a third-party fulfillment location.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/return)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| return                |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.return (
  "closedAt" timestamp,
  "createdAt" timestamp,
  decline jsonb,
  "exchangeLineItems" jsonb,
  id text,
  "name" text,
  "order" jsonb,
  refunds jsonb,
  "requestApprovedAt" timestamp,
  "returnLineItems" jsonb,
  "returnShippingFees" jsonb,
  "reverseFulfillmentOrders" jsonb,
  status text,
  "totalQuantity" bigint,
  attrs jsonb
)
  server shopify_server
  options (
    object 'return'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Query on this table must specify an value for `id` column

### Shop

Represents a collection of general settings and information about the shop.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/shop)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| shop                  |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.shop (
  "accountOwner" jsonb,
  alerts jsonb,
  "allProductCategoriesList" jsonb,
  "availableChannelApps" jsonb,
  "billingAddress" jsonb,
  "channelDefinitionsForInstalledChannels" jsonb,
  "checkoutApiSupported" bool,
  "contactEmail" text,
  "countriesInShippingZones" jsonb,
  "createdAt" timestamp,
  "currencyCode" text,
  "currencyFormats" jsonb,
  "currencySettings" jsonb,
  "customerAccounts" text,
  "customerAccountsV2" jsonb,
  "customerTags" jsonb,
  description text,
  "draftOrderTags" jsonb,
  email text,
  "enabledPresentmentCurrencies" text,
  entitlements jsonb,
  features jsonb,
  "fulfillmentServices" jsonb,
  "ianaTimezone" text,
  id text,
  "marketingSmsConsentEnabledAtCheckout" bool,
  "merchantApprovalSignals" jsonb,
  metafields jsonb,
  "myshopifyDomain" text,
  "name" text,
  "navigationSettings" jsonb,
  "orderNumberFormatPrefix" text,
  "orderNumberFormatSuffix" text,
  "orderTags" jsonb,
  "paymentSettings" jsonb,
  plan jsonb,
  "primaryDomain" jsonb,
  "resourceLimits" jsonb,
  "richTextEditorUrl" text,
  "searchFilters" jsonb,
  "setupRequired" bool,
  "shipsToCountries" json,
  "shopOwnerName" text,
  "shopPolicies" jsonb,
  "storefrontAccessTokens" jsonb,
  "taxShipping" bool,
  "taxesIncluded" bool,
  "timezoneAbbreviation" text,
  "timezoneOffset" text,
  "timezoneOffsetMinutes" bigint,
  "transactionalSmsDisabled" bool,
  "unitSystem" text,
  "updatedAt" timestamp,
  url text,
  "weightUnit" text,
  attrs jsonb
)
  server shopify_server
  options (
    object 'shop'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format

### StoreCreditAccount

A store credit account contains a monetary balance that can be redeemed at checkout for purchases in the shop.

Ref: [Shopify Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest/objects/storecreditaccount)

#### Operations

| Object                | Select | Insert | Update | Delete | Truncate |
| --------------------- | :----: | :----: | :----: | :----: | :------: |
| storeCreditAccount    |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table shopify.storeCreditAccount (
  balance jsonb,
  id text,
  "owner" jsonb,
  transactions jsonb,
  attrs jsonb
)
  server shopify_server
  options (
    object 'storeCreditAccount'
  );
```

#### Notes

- The `attrs` column contains additional attributes in JSON format
- Query on this table must specify an value for `id` column

## Query Pushdown Support

### `where` clause pushdown

This FDW supports `where id = 'xxx'` clause pushdown for below objects:

- collections
- customerPaymentMethod
- customers
- draftOrders
- fulfillment
- fulfillmentOrders
- inventoryLevel
- locations
- orders
- productVariants
- products
- refund
- return
- storeCreditAccount

## Supported Data Types

| Postgres Data Type | Shopify Data Type  |
| ------------------ | ------------------ |
| boolean            | Boolean            |
| bigint             | Number             |
| numeric            | Number             |
| text               | String             |
| timestamp          | Time               |
| jsonb              | Json               |

The Shopify Amdin API uses JSON formatted data, please refer to [Shopify GraphQL Admin API Docs](https://shopify.dev/docs/api/admin-graphql/latest) for more details.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Too many target columns in `SELECT` statement may exceed single query max cost limit imposed by Shopify
- Some fields which require parameters to be specified are ignored
- Large result sets may experience slower performance due to full data transfer requirement
- Materialized views using these foreign tables may fail during logical backups

## Examples

Below are some examples on how to use Shopify foreign tables.

### Basic example

This example will create a foreign table inside your Postgres database and query its data.

```sql
create foreign table shopify.customers (
  addresses jsonb,
  "addressesV2" jsonb,
  "amountSpent" jsonb,
  "canDelete" bool,
  "companyContactProfiles" jsonb,
  "createdAt" timestamp,
  "dataSaleOptOut" bool,
  "defaultAddress" jsonb,
  "defaultEmailAddress" jsonb,
  "defaultPhoneNumber" jsonb,
  "displayName" text,
  events jsonb,
  "firstName" text,
  id text,
  image jsonb,
  "lastName" text,
  "lastOrder" jsonb,
  "legacyResourceId" bigint,
  "lifetimeDuration" text,
  locale text,
  mergeable jsonb,
  metafields jsonb,
  "multipassIdentifier" text,
  note text,
  "numberOfOrders" bigint,
  orders jsonb,
  "paymentMethods" jsonb,
  "productSubscriberStatus" text,
  state text,
  "statistics" jsonb,
  "storeCreditAccounts" jsonb,
  "subscriptionContracts" jsonb,
  tags text,
  "taxExempt" bool,
  "taxExemptions" text,
  "updatedAt" timestamp,
  "verifiedEmail" bool,
  attrs jsonb
)
  server shopify_server
  options (
    object 'customers'
  );

-- query all customers
-- Note: limit the number of target columns in the query, otherwise it may
-- exceed single query max cost limit imposed by Shopify API
select
  id,
  "displayName",
  "addressesV2",
  "updatedAt"
from
  shopify.customers;
```

### Query A Single Object

To query a single object, you can specify an value to `id` column in query condition. This condition can be pushed down to Shopify API to improve query performance, see [the list of objects](#where-clause-pushdown) which support this feature.

```sql
select
  id,
  "displayName",
  "addressesV2",
  "updatedAt"
from
  shopify.customers
where
  id = 'gid://shopify/Customer/9236781315159';
```

