use std::collections::HashMap;

// object field map
// key: field name
// value: (field pg type, GraphQL query, dependent fragments)
pub(super) type FieldMap = HashMap<String, (String, String, Vec<String>)>;

pub(super) fn get_field_map(object: &str) -> FieldMap {
    // fragments
    let mailing_address = "fragment MailingAddressFields on MailingAddress {
        address1
        address2
        city
        company
        coordinatesValidated
        country
        countryCodeV2
        firstName
        formatted
        formattedArea
        id
        lastName
        latitude
        longitude
        name
        phone
        province
        provinceCode
        timeZone
        validationResultSummary
        zip
    }";
    let money_v2 = "fragment MoneyV2Fields on MoneyV2 {
        amount
        currencyCode
    }";
    let customer_email_address = "fragment CustomerEmailAddressFields on CustomerEmailAddress {
        emailAddress
        marketingOptInLevel
        marketingState
        marketingUnsubscribeUrl
        marketingUpdatedAt
        openTrackingLevel
        openTrackingUrl
        sourceLocation { id }
        validFormat
    }";
    let customer_phone_number = "fragment CustomerPhoneNumberFields on CustomerPhoneNumber {
        marketingCollectedFrom
        marketingOptInLevel
        marketingState
        marketingUpdatedAt
        phoneNumber
        sourceLocation { id }
    }";
    let event = "fragment EventFields on Event {
        action
        appTitle
        attributeToApp
        attributeToUser
        createdAt
        criticalAlert
        id
        message
    }";
    let metafield = "fragment MetafieldFields on Metafield {
        id
        key
        namespace
        type
        value
    }";
    let image = "fragment ImageFields on Image {
        altText
        height
        id
        metafields (first: 250) {
            nodes { ...MetafieldFields }
        }
        thumbhash
        url
        width
    }";
    let customer_mergeable = "fragment CustomerMergeableFields on CustomerMergeable {
        errorFields
        isMergeable
        mergeInProgress {
            customerMergeErrors {
                errorFields
                message
            }
            jobId
            resultingCustomerId
            status
        }
        reason
    }";
    let category = "fragment TaxonomyCategoryFields on TaxonomyCategory {
        ancestorIds
        attributes (first: 250) {
            nodes {
                ... on TaxonomyAttribute { id }
            }
        }
        childrenIds
        fullName
        id
        isArchived
        isLeaf
        isRoot
        level
        name
        parentId
    }";
    let resource_feedback = "fragment ResourceFeedbackFields on ResourceFeedback {
        details {
            app { id title }
            feedbackGeneratedAt
            link { label url }
            messages { field message }
            state
        }
        summary
    }";
    let media = "fragment MediaFields on Media {
        alt
        id
        mediaContentType
        status
    }";
    let publishable = "fragment PublishableFields on Publishable {
        availablePublicationsCount { count precision }
        publishedOnCurrentPublication
        resourcePublications (first: 250) {
            nodes {
                isPublished
                publication { id }
                publishDate
            }
        }
        resourcePublicationsCount { count precision }
        resourcePublicationsV2 (first: 250) {
            nodes {
                isPublished
                publication { id }
                publishDate
            } 
        }
        unpublishedPublications (first: 250) {
            nodes { id } 
        }
    }";
    let resource_publication = "fragment ResourcePublicationFields on ResourcePublication {
        isPublished,
        publication { id }
        publishable {
            ...PublishableFields
        }
        publishDate
    }";
    let resource_publication_v2 = "fragment ResourcePublicationV2Fields on ResourcePublicationV2 {
        isPublished,
        publication { id }
        publishable {
            ...PublishableFields
        }
        publishDate
    }";
    let money_bag = "fragment MoneyBagFields on MoneyBag {
        presentmentMoney {
            ...MoneyV2Fields
        }
        shopMoney {
            ...MoneyV2Fields
        }
    }";
    let tax_line = "fragment TaxLineFields on TaxLine {
        channelLiable
        priceSet {
            ...MoneyBagFields
        }
        rate
        ratePercentage
        source
        title
    }";
    let order_risk_summary = "fragment 
OrderRiskSummaryFields on 
OrderRiskSummary {
        assessments {
            facts {
                description
                sentiment
            }
            provider { id }
            riskLevel
        }
        recommendation
    }";
    let staff_member = "fragment StaffMemberFields on StaffMember {
        accountType
        active
        avatar { ...ImageFields }
        email
        exists
        firstName
        id
        initials
        isShopOwner
        lastName
        locale
        name
        phone
        privateData {
            accountSettingsUrl
            createdAt
        }
    }";
    let line_item = "fragment LineItemFields on LineItem {
        currentQuantity
        discountedTotalSet { ...MoneyBagFields }
        discountedUnitPriceAfterAllDiscountsSet { ...MoneyBagFields }
        discountedUnitPriceSet { ...MoneyBagFields }
        id
        isGiftCard
        merchantEditable
        name
        nonFulfillableQuantity
        originalTotalSet { ...MoneyBagFields }
        originalUnitPriceSet { ...MoneyBagFields }
        product { id }
        quantity
        refundableQuantity
        requiresShipping
        restockable
        sku
        taxable
        taxLines { ...TaxLineFields }
        title
        totalDiscountSet { ...MoneyBagFields }
        unfulfilledDiscountedTotalSet { ...MoneyBagFields }
        unfulfilledOriginalTotalSet { ...MoneyBagFields }
        unfulfilledQuantity
        variant { id }
        variantTitle
        vendor
    }";
    let payment_terms = "fragment PaymentTermsFields on PaymentTerms {
        draftOrder { id }
        dueInDays
        id
        order { id }
        overdue
        paymentSchedules (first: 250) {
            nodes {
                completedAt
                dueAt
                id
                issuedAt
                paymentTerms { id }
            }
        }
        paymentTermsName
        paymentTermsType
    }";
    let refund_line_item = "fragment RefundLineItemFields on RefundLineItem {
        id
        lineItem { id }
        location { id }
        priceSet { ...MoneyBagFields }
        quantity
        restocked
        restockType
        subtotalSet { ...MoneyBagFields }
        totalTaxSet { ...MoneyBagFields }
    }";
    let shipping_line = "fragment ShippingLineFields on ShippingLine {
        carrierIdentifier
        code
        currentDiscountedPriceSet { ...MoneyBagFields }
        custom
        deliveryCategory
        discountAllocations {
            allocatedAmountSet { ...MoneyBagFields }
            discountApplication {
                allocationMethod
                index
                targetSelection
                targetType
                value
            }
        }
        discountedPriceSet { ...MoneyBagFields }
        id
        isRemoved
        originalPriceSet { ...MoneyBagFields }
        phone
        shippingRateHandle
        source
        taxLines { ...TaxLineFields }
        title
    }";
    let order_transaction = "fragment OrderTransactionFields on OrderTransaction {
        accountNumber
        amountRoundingSet { ...MoneyBagFields }
        amountSet { ...MoneyBagFields }
        authorizationCode
        authorizationExpiresAt
        createdAt
        currencyExchangeAdjustment {
            adjustment { ...MoneyV2Fields }
            finalAmountSet { ...MoneyV2Fields }
            id
            originalAmountSet { ...MoneyV2Fields }
        }
        device { id }
        errorCode
        fees {
            amount { ...MoneyV2Fields }
            flatFee { ...MoneyV2Fields }
            flatFeeName
            id
            rate
            rateName
            taxAmount { ...MoneyV2Fields }
            type
        }
        formattedGateway
        gateway
        id
        kind
        location { id }
        manuallyCapturable
        manualPaymentGateway
        maximumRefundableV2 { ...MoneyV2Fields }
        multiCapturable
        order { id }
        paymentId
        status
        user { id }
    }";

    // field map
    let field_map = match object {
        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/product
        "products" => HashMap::from([
            (
                "availablePublicationsCount",
                ("jsonb", "{ count precision }", vec![]),
            ),
            (
                "bundleComponents",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            componentProduct { id }
                            componentVariants (first: 250) {
                                nodes { id }
                            }
                            componentVariantsCount {
                                count
                                precision
                            }
                            optionSelections {
                                componentOption {
                                    id
                                    name
                                }
                            }
                        }
                    }",
                    vec![],
                ),
            ),
            (
                "category",
                ("jsonb", "{ ...TaxonomyCategoryFields }", vec![category]),
            ),
            (
                "collections",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            (
                "combinedListing",
                (
                    "jsonb",
                    "{
                        combinedListingChildren (first: 250) {
                            nodes {
                                parentVariant { id }
                                product { id }
                            }
                        }
                        parentProduct { id }
                    }",
                    vec![],
                ),
            ),
            ("combinedListingRole", ("text", "", vec![])),
            (
                "compareAtPriceRange",
                (
                    "jsonb",
                    "{
                        maxVariantCompareAtPrice {
                            ...MoneyV2Fields
                        }
                        minVariantCompareAtPrice {
                            ...MoneyV2Fields
                        }
                    }",
                    vec![money_v2],
                ),
            ),
            ("createdAt", ("timestamp", "", vec![])),
            ("defaultCursor", ("text", "", vec![])),
            ("description", ("text", "", vec![])),
            ("descriptionHtml", ("text", "", vec![])),
            (
                "events",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...EventFields
                        }
                    }",
                    vec![event],
                ),
            ),
            (
                "featuredMedia",
                ("jsonb", "{ ...MediaFields }", vec![media]),
            ),
            (
                "feedback",
                (
                    "jsonb",
                    "{ ...ResourceFeedbackFields }",
                    vec![resource_feedback],
                ),
            ),
            ("giftCardTemplateSuffix", ("text", "", vec![])),
            ("handle", ("text", "", vec![])),
            ("hasOnlyDefaultVariant", ("boolean", "", vec![])),
            ("hasOutOfStockVariants", ("boolean", "", vec![])),
            ("hasVariantsThatRequiresComponents", ("boolean", "", vec![])),
            ("id", ("text", "", vec![])),
            ("isGiftCard", ("boolean", "", vec![])),
            ("legacyResourceId", ("bigint", "", vec![])),
            (
                "media",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {...MediaFields }
                    }",
                    vec![media],
                ),
            ),
            ("mediaCount", ("jsonb", "{ count precision }", vec![])),
            (
                "metafields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { ...MetafieldFields }
                    }",
                    vec![metafield],
                ),
            ),
            ("onlineStorePreviewUrl", ("text", "", vec![])),
            ("onlineStoreUrl", ("text", "", vec![])),
            (
                "options",
                (
                    "jsonb",
                    "(first: 250) {
                        id
                        name
                        optionValues {
                            hasVariants
                            id
                            linkedMetafieldValue
                            name
                            swatch {
                                color
                                image {
                                    id
                                    image { ...ImageFields }
                                }
                            }
                        }
                        position
                        values
                    }",
                    vec![metafield, image],
                ),
            ),
            (
                "priceRangeV2",
                (
                    "jsonb",
                    "{
                        maxVariantPrice {
                            ...MoneyV2Fields
                        }
                        minVariantPrice {
                            ...MoneyV2Fields
                        }
                    }",
                    vec![money_v2],
                ),
            ),
            (
                "productComponents",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            componentVariants (first: 250) {
                                nodes { id }
                            }
                            componentVariantsCount {
                                count
                                precision
                            }
                            nonComponentVariants (first: 250) {
                                nodes { id }
                            }
                            nonComponentVariantsCount {
                                count
                                precision
                            }
                            product { id }
                        }
                    }",
                    vec![],
                ),
            ),
            (
                "productComponentsCount",
                ("jsonb", "{ count precision }", vec![]),
            ),
            (
                "productParents",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("productType", ("text", "", vec![])),
            ("publishedAt", ("timestamp", "", vec![])),
            ("publishedOnCurrentPublication", ("boolean", "", vec![])),
            ("requiresSellingPlan", ("boolean", "", vec![])),
            (
                "resourcePublicationOnCurrentPublication",
                (
                    "jsonb",
                    "{ ...ResourcePublicationV2Fields }",
                    vec![publishable, resource_publication_v2],
                ),
            ),
            (
                "resourcePublications",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...ResourcePublicationFields
                        }
                    }",
                    vec![publishable, resource_publication],
                ),
            ),
            (
                "resourcePublicationsCount",
                ("jsonb", "{ count precision }", vec![]),
            ),
            (
                "resourcePublicationsV2",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...ResourcePublicationV2Fields
                        }
                    }",
                    vec![publishable, resource_publication_v2],
                ),
            ),
            (
                "sellingPlanGroups",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id name }
                    }",
                    vec![],
                ),
            ),
            (
                "sellingPlanGroupsCount",
                ("jsonb", "{ count precision }", vec![]),
            ),
            ("seo", ("jsonb", "{ description title }", vec![])),
            ("status", ("text", "", vec![])),
            ("tags", ("jsonb", "", vec![])),
            ("templateSuffix", ("text", "", vec![])),
            ("title", ("text", "", vec![])),
            ("totalInventory", ("bigint", "", vec![])),
            ("tracksInventory", ("boolean", "", vec![])),
            (
                "unpublishedPublications",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("updatedAt", ("timestamp", "", vec![])),
            (
                "variants",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("variantsCount", ("jsonb", "{ count precision }", vec![])),
            ("vendor", ("text", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/customer
        "customers" => HashMap::from([
            (
                "addresses",
                (
                    "jsonb",
                    "(first: 250) { ...MailingAddressFields }",
                    vec![mailing_address],
                ),
            ),
            (
                "addressesV2",
                (
                    "jsonb",
                    "(first: 250) { ... on MailingAddressConnection {
                        nodes {
                            ...MailingAddressFields
                        }
                    }}",
                    vec![mailing_address],
                ),
            ),
            (
                "amountSpent",
                ("jsonb", "{ ...MoneyV2Fields }", vec![money_v2]),
            ),
            ("canDelete", ("boolean", "", vec![])),
            (
                "companyContactProfiles",
                (
                    "jsonb",
                    "{ 
                        company { id }
                        createdAt
                        customer { id }
                        draftOrders (first: 250) { nodes { id } }
                        id
                        isMainContact
                        lifetimeDuration
                        locale
                        orders (first: 250) { nodes { id } }
                        roleAssignments (first: 250) {
                            nodes {
                                company { id }
                                companyContact { id }
                                companyLocation { id }
                                createdAt
                                id
                                role {
                                    id
                                    name
                                    note
                                }
                                updatedAt
                            }
                        }
                        title
                        updatedAt
                    }",
                    vec![],
                ),
            ),
            ("createdAt", ("timestamp", "", vec![])),
            ("dataSaleOptOut", ("boolean", "", vec![])),
            (
                "defaultAddress",
                (
                    "jsonb",
                    "{ ...MailingAddressFields }",
                    vec![mailing_address],
                ),
            ),
            (
                "defaultEmailAddress",
                (
                    "jsonb",
                    "{ ...CustomerEmailAddressFields }",
                    vec![customer_email_address],
                ),
            ),
            (
                "defaultPhoneNumber",
                (
                    "jsonb",
                    "{ ...CustomerPhoneNumberFields }",
                    vec![customer_phone_number],
                ),
            ),
            ("displayName", ("text", "", vec![])),
            (
                "events",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...EventFields
                        }
                    }",
                    vec![event],
                ),
            ),
            ("firstName", ("text", "", vec![])),
            ("id", ("text", "", vec![])),
            (
                "image",
                ("jsonb", "{ ...ImageFields }", vec![metafield, image]),
            ),
            ("lastName", ("text", "", vec![])),
            ("lastOrder", ("jsonb", "{ id }", vec![])),
            ("legacyResourceId", ("bigint", "", vec![])),
            ("lifetimeDuration", ("text", "", vec![])),
            ("locale", ("text", "", vec![])),
            (
                "mergeable",
                (
                    "jsonb",
                    "{ ...CustomerMergeableFields }",
                    vec![customer_mergeable],
                ),
            ),
            (
                "metafields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { ...MetafieldFields }
                    }",
                    vec![metafield],
                ),
            ),
            ("multipassIdentifier", ("text", "", vec![])),
            ("note", ("text", "", vec![])),
            ("numberOfOrders", ("bigint", "", vec![])),
            (
                "orders",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            (
                "paymentMethods",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("productSubscriberStatus", ("text", "", vec![])),
            ("state", ("text", "", vec![])),
            (
                "statistics",
                (
                    "jsonb",
                    "{
                        predictedSpendTier
                        rfmGroup
                    }",
                    vec![],
                ),
            ),
            (
                "storeCreditAccounts",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            (
                "subscriptionContracts",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("tags", ("text", "", vec![])),
            ("taxExempt", ("boolean", "", vec![])),
            ("taxExemptions", ("text", "", vec![])),
            ("updatedAt", ("timestamp", "", vec![])),
            ("verifiedEmail", ("boolean", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/order
        "orders" => HashMap::from([
            (
                "additionalFees",
                (
                    "jsonb",
                    "{
                        id
                        name
                        price { ...MoneyBagFields }
                        taxLines { ...TaxLineFields }
                    }",
                    vec![money_v2, money_bag, tax_line],
                ),
            ),
            (
                "agreements",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            (
                "alerts",
                (
                    "jsonb",
                    "{
                        actions {
                            primary
                            show
                            title
                            url
                        }
                        content
                        dismissibleHandle
                        icon
                        severity
                        title
                    }",
                    vec![],
                ),
            ),
            (
                "app",
                (
                    "jsonb",
                    "{
                        icon { ...ImageFields }
                        id
                        name
                    }",
                    vec![metafield, image],
                ),
            ),
            (
                "billingAddress",
                (
                    "jsonb",
                    "{ ...MailingAddressFields }",
                    vec![mailing_address],
                ),
            ),
            (
                "billingAddressMatchesShippingAddress",
                ("boolean", "", vec![]),
            ),
            ("cancellation", ("jsonb", "{ staffNote }", vec![])),
            ("cancelledAt", ("timestamp", "", vec![])),
            ("cancelReason", ("text", "", vec![])),
            ("canMarkAsPaid", ("boolean", "", vec![])),
            ("canNotifyCustomer", ("boolean", "", vec![])),
            ("capturable", ("boolean", "", vec![])),
            (
                "cartDiscountAmountSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "channelInformation",
                (
                    "jsonb",
                    "{
                        app { id }
                        channelDefinition {
                            channelName
                            handle
                            id
                            isMarketplace
                            subChannelName
                        }
                        channelId
                        displayName
                        id
                    }",
                    vec![],
                ),
            ),
            ("clientIp", ("text", "", vec![])),
            ("closed", ("boolean", "", vec![])),
            ("closedAt", ("timestamp", "", vec![])),
            ("confirmationNumber", ("text", "", vec![])),
            ("confirmed", ("boolean", "", vec![])),
            ("createdAt", ("timestamp", "", vec![])),
            ("currencyCode", ("text", "", vec![])),
            (
                "currentCartDiscountAmountSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "currentShippingPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            ("currentSubtotalLineItemsQuantity", ("bigint", "", vec![])),
            (
                "currentSubtotalPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "currentTaxLines",
                (
                    "jsonb",
                    "{ ...TaxLineFields }",
                    vec![money_v2, money_bag, tax_line],
                ),
            ),
            (
                "currentTotalAdditionalFeesSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "currentTotalDiscountsSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "currentTotalDutiesSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "currentTotalPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "currentTotalTaxSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            ("currentTotalWeight", ("bigint", "", vec![])),
            ("customAttributes", ("jsonb", "{ key value }", vec![])),
            ("customer", ("jsonb", "{ id }", vec![])),
            ("customerAcceptsMarketing", ("boolean", "", vec![])),
            (
                "customerJourneySummary",
                (
                    "jsonb",
                    "{
                        customerOrderIndex
                        daysToConversion
                        firstVisit { id }
                        lastVisit { id }
                        moments (first: 250) {
                            nodes { occurredAt }
                        }
                        momentsCount { count precision }
                        ready
                    }",
                    vec![],
                ),
            ),
            ("customerLocale", ("text", "", vec![])),
            (
                "discountApplications",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            allocationMethod
                            index
                            targetSelection
                            targetType
                            value
                        }
                    }",
                    vec![],
                ),
            ),
            ("discountCode", ("text", "", vec![])),
            ("discountCodes", ("jsonb", "", vec![])),
            (
                "displayAddress",
                (
                    "jsonb",
                    "{ ...MailingAddressFields }",
                    vec![mailing_address],
                ),
            ),
            ("displayFinancialStatus", ("text", "", vec![])),
            ("displayFulfillmentStatus", ("text", "", vec![])),
            (
                "disputes",
                (
                    "jsonb",
                    "{
                        id
                        initiatedAs
                        status
                    }",
                    vec![],
                ),
            ),
            ("dutiesIncluded", ("boolean", "", vec![])),
            ("edited", ("boolean", "", vec![])),
            ("email", ("text", "", vec![])),
            ("estimatedTaxes", ("boolean", "", vec![])),
            (
                "events",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...EventFields
                        }
                    }",
                    vec![event],
                ),
            ),
            ("fulfillable", ("boolean", "", vec![])),
            (
                "fulfillmentOrders",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            (
                "fulfillments",
                (
                    "jsonb",
                    "(first: 250) {
                        id
                    }",
                    vec![],
                ),
            ),
            (
                "fulfillmentsCount",
                ("jsonb", "{ count precision }", vec![]),
            ),
            ("fullyPaid", ("boolean", "", vec![])),
            ("hasTimelineComment", ("boolean", "", vec![])),
            ("id", ("text", "", vec![])),
            ("legacyResourceId", ("bigint", "", vec![])),
            (
                "lineItems",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { ...LineItemFields }
                    }",
                    vec![money_v2, money_bag, tax_line, line_item],
                ),
            ),
            (
                "localizedFields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            countryCode
                            key
                            purpose
                            title
                            value
                        }
                    }",
                    vec![],
                ),
            ),
            (
                "merchantBusinessEntity",
                ("jsonb", "{ id displayName }", vec![]),
            ),
            ("merchantEditable", ("boolean", "", vec![])),
            ("merchantEditableErrors", ("jsonb", "", vec![])),
            (
                "merchantOfRecordApp",
                (
                    "jsonb",
                    "{
                        icon { ...ImageFields }
                        id
                        name
                    }",
                    vec![metafield, image],
                ),
            ),
            (
                "metafields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { ...MetafieldFields }
                    }",
                    vec![metafield],
                ),
            ),
            ("name", ("text", "", vec![])),
            (
                "netPaymentSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "nonFulfillableLineItems",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("note", ("text", "", vec![])),
            ("number", ("bigint", "", vec![])),
            (
                "originalTotalAdditionalFeesSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "originalTotalDutiesSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "originalTotalPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "paymentCollectionDetails",
                (
                    "jsonb",
                    "{
                        additionalPaymentCollectionUrl
                        vaultedPaymentMethods {
                            id
                            paymentInstrument
                        }
                    }",
                    vec![],
                ),
            ),
            ("paymentGatewayNames", ("text", "", vec![])),
            (
                "paymentTerms",
                ("jsonb", "{ ...PaymentTermsFields }", vec![payment_terms]),
            ),
            ("phone", ("text", "", vec![])),
            ("poNumber", ("text", "", vec![])),
            ("presentmentCurrencyCode", ("text", "", vec![])),
            ("processedAt", ("timestamp", "", vec![])),
            ("processedAt", ("timestamp", "", vec![])),
            ("publication", ("jsonb", "{ id }", vec![])),
            ("refundable", ("boolean", "", vec![])),
            (
                "refundDiscrepancySet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            ("refunds", ("jsonb", "(first: 250) { id }", vec![])),
            ("registeredSourceUrl", ("text", "", vec![])),
            ("requiresShipping", ("boolean", "", vec![])),
            ("restockable", ("boolean", "", vec![])),
            ("retailLocation", ("jsonb", "{ id }", vec![])),
            (
                "returns",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("returnStatus", ("text", "", vec![])),
            (
                "risk",
                (
                    "jsonb",
                    "{ ...OrderRiskSummaryFields }",
                    vec![order_risk_summary],
                ),
            ),
            (
                "shippingAddress",
                (
                    "jsonb",
                    "{ ...MailingAddressFields }",
                    vec![mailing_address],
                ),
            ),
            (
                "shippingLine",
                (
                    "jsonb",
                    "{ ...ShippingLineFields }",
                    vec![money_v2, money_bag, tax_line, shipping_line],
                ),
            ),
            (
                "shippingLines",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...ShippingLineFields
                        }
                    }",
                    vec![money_v2, money_bag, tax_line, shipping_line],
                ),
            ),
            (
                "shopifyProtect",
                (
                    "jsonb",
                    "{
                        eligibility {
                            status
                        }
                        status
                    }",
                    vec![],
                ),
            ),
            ("sourceIdentifier", ("text", "", vec![])),
            ("sourceName", ("text", "", vec![])),
            (
                "staffMember",
                (
                    "jsonb",
                    "{ ...StaffMemberFields }",
                    vec![metafield, image, staff_member],
                ),
            ),
            ("statusPageUrl", ("text", "", vec![])),
            ("subtotalLineItemsQuantity", ("bigint", "", vec![])),
            (
                "subtotalPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "suggestedRefund",
                (
                    "jsonb",
                    "{
                        amountSet { ...MoneyBagFields }
                        discountedSubtotalSet { ...MoneyBagFields }
                        maximumRefundableSet { ...MoneyBagFields }
                        refundDuties {
                            amountSet { ...MoneyBagFields }
                            originalDuty {
                                countryCodeOfOrigin
                                harmonizedSystemCode
                                id
                                price { ...MoneyBagFields }
                                taxLines {
                                    ...TaxLineFields
                                }
                            }
                        }
                        refundLineItems {
                            ...RefundLineItemFields
                        }
                        shipping {
                            amountSet { ...MoneyBagFields }
                            maximumRefundableSet { ...MoneyBagFields }
                            taxSet { ...MoneyBagFields }
                        }
                        subtotalSet { ...MoneyBagFields }
                        suggestedRefundMethods {
                            amount { ...MoneyBagFields }
                            maximumRefundable { ...MoneyBagFields }
                        }
                        suggestedTransactions {
                            accountNumber
                            amountSet { ...MoneyBagFields }
                            formattedGateway
                            gateway
                            kind
                            maximumRefundableSet { ...MoneyBagFields }
                            parentTransaction { id }
                            paymentDetails
                        }
                        totalCartDiscountAmountSet { ...MoneyBagFields }
                        totalDutiesSet { ...MoneyBagFields }
                        totalTaxSet { ...MoneyBagFields }
                    }",
                    vec![money_v2, money_bag, tax_line, refund_line_item],
                ),
            ),
            ("tags", ("jsonb", "", vec![])),
            ("taxesIncluded", ("boolean", "", vec![])),
            ("taxExempt", ("boolean", "", vec![])),
            (
                "taxLines",
                (
                    "jsonb",
                    "{ ...TaxLineFields }",
                    vec![money_v2, money_bag, tax_line],
                ),
            ),
            ("test", ("boolean", "", vec![])),
            (
                "totalCapturableSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalCashRoundingAdjustment",
                (
                    "jsonb",
                    "{
                        paymentSet { ...MoneyBagFields }
                        refundSet { ...MoneyBagFields }
                    }",
                    vec![money_v2, money_bag],
                ),
            ),
            (
                "totalDiscountsSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalOutstandingSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalReceivedSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalRefundedSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalRefundedShippingSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalShippingPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalTaxSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalTipReceivedSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            ("totalWeight", ("bigint", "", vec![])),
            (
                "transactions",
                (
                    "jsonb",
                    "{ ...OrderTransactionFields }",
                    vec![money_v2, money_bag, order_transaction],
                ),
            ),
            (
                "transactionsCount",
                ("jsonb", "{ count precision }", vec![]),
            ),
            ("unpaid", ("boolean", "", vec![])),
            ("updatedAt", ("timestamp", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/location
        "locations" => HashMap::from([
            ("activatable", ("boolean", "", vec![])),
            (
                "address",
                (
                    "jsonb",
                    "{
                        address1
                        address2
                        city
                        country
                        countryCode
                        formatted
                        latitude
                        longitude
                        phone
                        province
                        provinceCode
                        zip
                    }",
                    vec![],
                ),
            ),
            ("addressVerified", ("boolean", "", vec![])),
            ("createdAt", ("timestamp", "", vec![])),
            ("deactivatable", ("boolean", "", vec![])),
            ("deactivatedAt", ("text", "", vec![])),
            ("deletable", ("boolean", "", vec![])),
            (
                "fulfillmentService",
                (
                    "jsonb",
                    "{
                        callbackUrl
                        handle
                        id
                        inventoryManagement
                        location { id }
                        permitsSkuSharing
                        requiresShippingMethod
                        serviceName
                        trackingSupport
                        type
                    }",
                    vec![],
                ),
            ),
            ("fulfillsOnlineOrders", ("boolean", "", vec![])),
            ("hasActiveInventory", ("boolean", "", vec![])),
            ("hasUnfulfilledOrders", ("boolean", "", vec![])),
            ("id", ("text", "", vec![])),
            (
                "inventoryLevels",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("isActive", ("boolean", "", vec![])),
            ("isFulfillmentService", ("boolean", "", vec![])),
            ("legacyResourceId", ("bigint", "", vec![])),
            (
                "localPickupSettingsV2",
                (
                    "jsonb",
                    "{
                        instructions
                        pickupTime
                    }",
                    vec![],
                ),
            ),
            (
                "metafields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { ...MetafieldFields }
                    }",
                    vec![metafield],
                ),
            ),
            ("name", ("text", "", vec![])),
            ("shipsInventory", ("boolean", "", vec![])),
            (
                "suggestedAddresses",
                (
                    "jsonb",
                    "{
                        address1
                        address2
                        city
                        country
                        countryCode
                        formatted
                        province
                        provinceCode
                        zip
                    }",
                    vec![],
                ),
            ),
            ("updatedAt", ("timestamp", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/draftorder
        "draftOrders" => HashMap::from([
            ("acceptAutomaticDiscounts", ("boolean", "", vec![])),
            ("allowDiscountCodesInCheckout", ("boolean", "", vec![])),
            ("allVariantPricesOverridden", ("boolean", "", vec![])),
            ("anyVariantPricesOverridden", ("boolean", "", vec![])),
            (
                "appliedDiscount",
                (
                    "jsonb",
                    "{
                        amountSet { ...MoneyBagFields }
                        description
                        title
                        value
                        valueType
                    }",
                    vec![money_v2, money_bag],
                ),
            ),
            (
                "billingAddress",
                (
                    "jsonb",
                    "{ ...MailingAddressFields }",
                    vec![mailing_address],
                ),
            ),
            (
                "billingAddressMatchesShippingAddress",
                ("boolean", "", vec![]),
            ),
            ("completedAt", ("timestamp", "", vec![])),
            ("createdAt", ("timestamp", "", vec![])),
            ("currencyCode", ("text", "", vec![])),
            ("customAttributes", ("jsonb", "{ key value }", vec![])),
            ("customer", ("jsonb", "{ id }", vec![])),
            ("defaultCursor", ("text", "", vec![])),
            ("discountCodes", ("jsonb", "", vec![])),
            ("email", ("text", "", vec![])),
            (
                "events",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...EventFields
                        }
                    }",
                    vec![event],
                ),
            ),
            ("hasTimelineComment", ("boolean", "", vec![])),
            ("id", ("text", "", vec![])),
            ("invoiceEmailTemplateSubject", ("text", "", vec![])),
            ("invoiceSentAt", ("timestamp", "", vec![])),
            ("invoiceUrl", ("text", "", vec![])),
            ("legacyResourceId", ("bigint", "", vec![])),
            (
                "lineItems",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            appliedDiscount {
                                amountSet { ...MoneyBagFields }
                                description
                                title
                                value
                                valueType
                            }
                            approximateDiscountedUnitPriceSet { ...MoneyBagFields }
                            components { id } 
                            custom
                            customAttributes { key value }
                            customAttributesV2 { key value }
                            discountedTotalSet { ...MoneyBagFields }
                            fulfillmentService {
                                callbackUrl
                                handle
                                id
                                inventoryManagement
                                location { id }
                                permitsSkuSharing
                                requiresShippingMethod
                                serviceName
                                trackingSupport
                                type
                            }
                            id
                            image { ...ImageFields }
                            isGiftCard
                            name
                            originalTotalSet { ...MoneyBagFields }
                            originalUnitPriceSet { ...MoneyBagFields }
                            originalUnitPriceWithCurrency { ...MoneyV2Fields }
                            priceOverride { ...MoneyV2Fields }
                            product { id }
                            quantity
                            requiresShipping
                            sku
                            taxable
                            taxLines { ...TaxLineFields }
                            title
                            totalDiscountSet { ...MoneyBagFields }
                            uuid
                            variant { id }
                            variantTitle
                            vendor
                            weight { unit value }
                        }
                    }",
                    vec![money_v2, money_bag, tax_line, metafield, image],
                ),
            ),
            (
                "lineItemsSubtotalPrice",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "localizedFields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            countryCode
                            key
                            purpose
                            title
                            value
                        }
                    }",
                    vec![],
                ),
            ),
            (
                "metafields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { ...MetafieldFields }
                    }",
                    vec![metafield],
                ),
            ),
            ("name", ("text", "", vec![])),
            ("note2", ("text", "", vec![])),
            ("order", ("jsonb", "{ id }", vec![])),
            (
                "paymentTerms",
                ("jsonb", "{ ...PaymentTermsFields }", vec![payment_terms]),
            ),
            ("phone", ("text", "", vec![])),
            (
                "platformDiscounts",
                (
                    "jsonb",
                    "{
                        allocations {
                            id
                            quantity
                            reductionAmount { ...MoneyV2Fields }
                            reductionAmountSet { ...MoneyBagFields }
                            target
                        }
                        automaticDiscount
                        bxgyDiscount
                        code
                        discountClasses
                        discountNode {
                            discount
                            events (first: 250) {
                                nodes { ...EventFields }
                            }
                            id
                            metafields (first: 250) {
                                nodes { ...MetafieldFields }
                            }
                        }
                        id
                        presentationLevel
                        shortSummary
                        summary
                        title
                        totalAmount { ...MoneyV2Fields }
                        totalAmountPriceSet { ...MoneyBagFields }
                    }",
                    vec![event, metafield, money_v2, money_bag],
                ),
            ),
            ("poNumber", ("text", "", vec![])),
            ("presentmentCurrencyCode", ("text", "", vec![])),
            ("purchasingEntity", ("text", "", vec![])),
            ("ready", ("boolean", "", vec![])),
            ("reserveInventoryUntil", ("timestamp", "", vec![])),
            (
                "shippingAddress",
                (
                    "jsonb",
                    "{ ...MailingAddressFields }",
                    vec![mailing_address],
                ),
            ),
            (
                "shippingLine",
                (
                    "jsonb",
                    "{ ...ShippingLineFields }",
                    vec![money_v2, money_bag, tax_line, shipping_line],
                ),
            ),
            ("status", ("text", "", vec![])),
            (
                "subtotalPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            ("tags", ("jsonb", "", vec![])),
            ("taxesIncluded", ("boolean", "", vec![])),
            ("taxExempt", ("boolean", "", vec![])),
            (
                "taxLines",
                (
                    "jsonb",
                    "{ ...TaxLineFields }",
                    vec![money_v2, money_bag, tax_line],
                ),
            ),
            (
                "totalDiscountsSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalLineItemsPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            ("totalQuantityOfLineItems", ("bigint", "", vec![])),
            (
                "totalShippingPriceSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "totalTaxSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            ("totalWeight", ("bigint", "", vec![])),
            ("transformerFingerprint", ("text", "", vec![])),
            ("updatedAt", ("timestamp", "", vec![])),
            ("visibleToCustomer", ("boolean", "", vec![])),
            (
                "warnings",
                (
                    "jsonb",
                    "{
                        errorCode
                        field
                        message
                    }",
                    vec![],
                ),
            ),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/customerpaymentmethod
        "customerPaymentMethod" => HashMap::from([
            ("customer", ("jsonb", "{ id }", vec![])),
            ("id", ("text", "", vec![])),
            ("instrument", ("text", "", vec![])),
            ("revokedAt", ("timestamp", "", vec![])),
            ("revokedReason", ("text", "", vec![])),
            (
                "subscriptionContracts",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/storecreditaccount
        "storeCreditAccount" => HashMap::from([
            ("balance", ("jsonb", "{ ...MoneyV2Fields }", vec![money_v2])),
            ("id", ("text", "", vec![])),
            (
                "owner",
                (
                    "jsonb",
                    "{
                        storeCreditAccounts (first: 250) {
                            nodes { id }
                        }
                    }",
                    vec![],
                ),
            ),
            (
                "transactions",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            account { id }
                            amount { ...MoneyV2Fields }
                            balanceAfterTransaction { ...MoneyV2Fields }
                            createdAt
                            event
                            origin
                        }
                    }",
                    vec![money_v2],
                ),
            ),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/collection
        "collections" => HashMap::from([
            (
                "availablePublicationsCount",
                ("jsonb", "{ count precision }", vec![]),
            ),
            ("description", ("text", "", vec![])),
            ("descriptionHtml", ("text", "", vec![])),
            (
                "events",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...EventFields
                        }
                    }",
                    vec![event],
                ),
            ),
            (
                "feedback",
                (
                    "jsonb",
                    "{ ...ResourceFeedbackFields }",
                    vec![resource_feedback],
                ),
            ),
            ("handle", ("text", "", vec![])),
            ("id", ("text", "", vec![])),
            (
                "image",
                ("jsonb", "{ ...ImageFields }", vec![metafield, image]),
            ),
            ("legacyResourceId", ("bigint", "", vec![])),
            (
                "metafields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { ...MetafieldFields }
                    }",
                    vec![metafield],
                ),
            ),
            (
                "products",
                ("jsonb", "(first: 250) { nodes { id } }", vec![]),
            ),
            ("productsCount", ("jsonb", "{ count precision }", vec![])),
            ("publishedOnCurrentPublication", ("boolean", "", vec![])),
            (
                "resourcePublications",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...ResourcePublicationFields
                        }
                    }",
                    vec![publishable, resource_publication],
                ),
            ),
            (
                "resourcePublicationsCount",
                ("jsonb", "{ count precision }", vec![]),
            ),
            (
                "resourcePublicationsV2",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...ResourcePublicationV2Fields
                        }
                    }",
                    vec![publishable, resource_publication_v2],
                ),
            ),
            (
                "ruleSet",
                (
                    "jsonb",
                    "{
                        appliedDisjunctively
                        rules {
                            column
                            condition
                            conditionObject
                            relation
                        }
                    }",
                    vec![],
                ),
            ),
            ("seo", ("jsonb", "{ description title }", vec![])),
            ("sortOrder", ("text", "", vec![])),
            ("templateSuffix", ("text", "", vec![])),
            ("title", ("text", "", vec![])),
            (
                "unpublishedPublications",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("updatedAt", ("timestamp", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/productvariant
        "productVariants" => HashMap::from([
            ("availableForSale", ("boolean", "", vec![])),
            ("barcode", ("text", "", vec![])),
            ("compareAtPrice", ("text", "", vec![])),
            ("createdAt", ("timestamp", "", vec![])),
            ("defaultCursor", ("text", "", vec![])),
            (
                "deliveryProfile",
                (
                    "jsonb",
                    "{
                        activeMethodDefinitionsCount
                        default
                        id
                        legacyMode
                        locationsWithoutRatesCount
                        name
                        originLocationCount
                        productVariantsCount { count precision }
                        profileItems (first: 250) {
                            nodes {
                                id
                                product { id }
                                variants (first: 250) {
                                    nodes { id }
                                }
                            }
                        }
                        profileLocationGroups {
                            countriesInAnyZone {
                                country {
                                    code { countryCode restOfWorld }
                                    id
                                    name
                                    provinces {
                                        code
                                        id
                                        name
                                        translatedName
                                    }
                                }
                                zone
                            }
                            locationGroup {
                                id
                                locations (first: 250) {
                                    nodes { id }
                                }
                                locationsCount { count precision }
                            }
                            locationGroupZones (first: 250) {
                                nodes {
                                    methodDefinitionCounts {
                                        participantDefinitionsCount
                                        rateDefinitionsCount
                                    }
                                    methodDefinitions (first: 250) {
                                        nodes {
                                            active
                                            description
                                            id
                                            methodConditions {
                                                conditionCriteria
                                                field
                                                id
                                                operator
                                            }
                                            name
                                            rateProvider
                                        }
                                    }
                                    zone {
                                        countries { id name }
                                        id
                                        name
                                    }
                                }
                            }
                        }
                        sellingPlanGroups (first: 250) {
                            nodes { id name }
                        }
                        unassignedLocations { id }
                        unassignedLocationsPaginated (first: 250) {
                            nodes { id }
                        }
                        zoneCountryCount
                    }",
                    vec![],
                ),
            ),
            ("displayName", ("text", "", vec![])),
            (
                "events",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...EventFields
                        }
                    }",
                    vec![event],
                ),
            ),
            ("id", ("text", "", vec![])),
            (
                "image",
                ("jsonb", "{ ...ImageFields }", vec![metafield, image]),
            ),
            ("inventoryItem", ("jsonb", "{ id sku }", vec![])),
            ("inventoryPolicy", ("text", "", vec![])),
            ("inventoryQuantity", ("bigint", "", vec![])),
            ("legacyResourceId", ("bigint", "", vec![])),
            (
                "media",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {...MediaFields }
                    }",
                    vec![media],
                ),
            ),
            (
                "metafields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { ...MetafieldFields }
                    }",
                    vec![metafield],
                ),
            ),
            ("position", ("bigint", "", vec![])),
            ("price", ("numeric", "", vec![])),
            ("product", ("jsonb", "{ id }", vec![])),
            (
                "productParents",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            (
                "productVariantComponents",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            id
                            productVariant { id }
                            quantity
                        }
                    }",
                    vec![],
                ),
            ),
            ("requiresComponents", ("boolean", "", vec![])),
            (
                "selectedOptions",
                (
                    "jsonb",
                    "{
                        name
                        optionValue {
                            hasVariants
                            id
                            linkedMetafieldValue
                            name
                            swatch {
                                color
                                image {
                                    alt
                                    createdAt
                                    fileStatus
                                    id
                                    image { ...ImageFields }
                                    mediaContentType
                                    mimeType
                                    status
                                    updatedAt
                                }
                            }
                        }
                        value
                    }",
                    vec![metafield, image],
                ),
            ),
            ("sellableOnlineQuantity", ("bigint", "", vec![])),
            (
                "sellingPlanGroups",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id name }
                    }",
                    vec![],
                ),
            ),
            (
                "sellingPlanGroupsCount",
                ("jsonb", "{ count precision }", vec![]),
            ),
            ("showUnitPrice", ("boolean", "", vec![])),
            ("sku", ("text", "", vec![])),
            ("taxable", ("boolean", "", vec![])),
            ("title", ("text", "", vec![])),
            (
                "unitPrice",
                ("jsonb", "{ ...MoneyV2Fields }", vec![money_v2]),
            ),
            (
                "unitPriceMeasurement",
                (
                    "jsonb",
                    "{
                        measuredType
                        quantityUnit
                        quantityValue
                        referenceUnit
                        referenceValue
                    }",
                    vec![],
                ),
            ),
            ("updatedAt", ("timestamp", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/app
        "app" => HashMap::from([
            ("apiKey", ("text", "", vec![])),
            ("appStoreAppUrl", ("text", "", vec![])),
            ("appStoreDeveloperUrl", ("text", "", vec![])),
            (
                "availableAccessScopes",
                ("jsonb", "{ description handle }", vec![]),
            ),
            (
                "banner",
                ("jsonb", "{ ...ImageFields }", vec![metafield, image]),
            ),
            ("description", ("text", "", vec![])),
            ("developerName", ("text", "", vec![])),
            ("developerType", ("text", "", vec![])),
            ("embedded", ("boolean", "", vec![])),
            (
                "failedRequirements",
                (
                    "jsonb",
                    "{
                        action { id title url }
                        message
                    }",
                    vec![],
                ),
            ),
            ("features", ("jsonb", "", vec![])),
            (
                "feedback",
                (
                    "jsonb",
                    "{
                        app { id title }
                        feedbackGeneratedAt
                        link { label url }
                        messages { field message }
                        state
                    }",
                    vec![],
                ),
            ),
            ("handle", ("text", "", vec![])),
            (
                "icon",
                ("jsonb", "{ ...ImageFields }", vec![metafield, image]),
            ),
            ("id", ("text", "", vec![])),
            ("installation", ("jsonb", "{ id launchUrl }", vec![])),
            ("installUrl", ("text", "", vec![])),
            ("isPostPurchaseAppInUse", ("boolean", "", vec![])),
            (
                "optionalAccessScopes",
                ("jsonb", "{ description handle }", vec![]),
            ),
            ("previouslyInstalled", ("boolean", "", vec![])),
            ("pricingDetails", ("text", "", vec![])),
            ("pricingDetailsSummary", ("text", "", vec![])),
            ("privacyPolicyUrl", ("text", "", vec![])),
            ("publicCategory", ("text", "", vec![])),
            ("published", ("boolean", "", vec![])),
            (
                "requestedAccessScopes",
                ("jsonb", "{ description handle }", vec![]),
            ),
            (
                "screenshots",
                ("jsonb", "{ ...ImageFields }", vec![metafield, image]),
            ),
            ("shopifyDeveloped", ("boolean", "", vec![])),
            ("title", ("text", "", vec![])),
            ("uninstallMessage", ("text", "", vec![])),
            ("webhookApiVersion", ("text", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/fulfillmentorder
        "fulfillmentOrders" => HashMap::from([
            (
                "assignedLocation",
                (
                    "jsonb",
                    "{
                        address1
                        address2
                        city
                        countryCode
                        location { id }
                        name
                        phone
                        province
                        zip
                    }",
                    vec![],
                ),
            ),
            ("channelId", ("text", "", vec![])),
            ("createdAt", ("timestamp", "", vec![])),
            (
                "deliveryMethod",
                (
                    "jsonb",
                    "{
                        additionalInformation {
                            instructions
                            phone
                        }
                        brandedPromise {
                            handle
                            name
                        }
                        id
                        maxDeliveryDateTime
                        methodType
                        minDeliveryDateTime
                        presentedName
                        serviceCode
                        sourceReference
                    }",
                    vec![],
                ),
            ),
            (
                "destination",
                (
                    "jsonb",
                    "{
                        address1
                        address2
                        city
                        company
                        countryCode
                        email
                        firstName
                        id
                        lastName
                        location { id }
                        phone
                        province
                        zip
                    }",
                    vec![],
                ),
            ),
            ("fulfillAt", ("timestamp", "", vec![])),
            ("fulfillBy", ("timestamp", "", vec![])),
            (
                "fulfillmentHolds",
                (
                    "jsonb",
                    "{
                        displayReason
                        handle
                        heldByApp { id }
                        heldByRequestingApp
                        id
                        reason
                        reasonNotes
                    }",
                    vec![],
                ),
            ),
            (
                "fulfillmentOrdersForMerge",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            (
                "fulfillments",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("id", ("text", "", vec![])),
            ("internationalDuties", ("jsonb", "{ incoterm }", vec![])),
            (
                "lineItems",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            financialSummaries {
                                approximateDiscountedUnitPriceSet { ...MoneyBagFields }
                                discountAllocations {
                                    approximateAllocatedAmountPerItem { ...MoneyBagFields }
                                    discountApplication {
                                        allocationMethod
                                        targetSelection
                                        targetType
                                    }
                                }
                                originalUnitPriceSet { ...MoneyBagFields }
                                quantity
                            }
                            id
                            image { ...ImageFields }
                            inventoryItemId
                            lineItem { ...LineItemFields }
                            productTitle
                            remainingQuantity
                            requiresShipping
                            sku
                            totalQuantity
                            variant { id }
                            variantTitle
                            vendor
                            warnings { description title }
                            weight { unit value }
                        }
                    }",
                    vec![money_v2, money_bag, tax_line, line_item, metafield, image],
                ),
            ),
            (
                "locationsForMove",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            availableLineItems (first: 250) {
                                nodes { id }
                            }
                            availableLineItemsCount {
                                count
                                precision
                            }
                            location { id }
                            message
                            movable
                            unavailableLineItems (first: 250) {
                                nodes { id }
                            }
                            unavailableLineItemsCount {
                                count
                                precision
                            }
                        }
                    }",
                    vec![],
                ),
            ),
            (
                "merchantRequests",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            fulfillmentOrder { id }
                            id
                            kind
                            message
                            requestOptions
                            responseData
                            sentAt
                        }
                    }",
                    vec![],
                ),
            ),
            ("order", ("jsonb", "{ id }", vec![])),
            ("orderId", ("text", "", vec![])),
            ("orderName", ("text", "", vec![])),
            ("orderProcessedAt", ("timestamp", "", vec![])),
            ("requestStatus", ("text", "", vec![])),
            ("status", ("text", "", vec![])),
            (
                "supportedActions",
                ("jsonb", "{ action externalUrl }", vec![]),
            ),
            ("updatedAt", ("timestamp", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/fulfillment
        "fulfillment" => HashMap::from([
            ("createdAt", ("timestamp", "", vec![])),
            ("deliveredAt", ("timestamp", "", vec![])),
            ("displayStatus", ("text", "", vec![])),
            ("estimatedDeliveryAt", ("timestamp", "", vec![])),
            (
                "events",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...EventFields
                        }
                    }",
                    vec![event],
                ),
            ),
            (
                "fulfillmentLineItems",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            (
                "fulfillmentOrders",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("id", ("text", "", vec![])),
            ("inTransitAt", ("timestamp", "", vec![])),
            ("legacyResourceId", ("bigint", "", vec![])),
            ("location", ("jsonb", "{ id }", vec![])),
            ("name", ("text", "", vec![])),
            ("order", ("jsonb", "{ id }", vec![])),
            (
                "originAddress",
                (
                    "jsonb",
                    "{
                        address1
                        address2
                        city
                        countryCode
                        provinceCode
                        zip
                    }",
                    vec![],
                ),
            ),
            ("requiresShipping", ("boolean", "", vec![])),
            (
                "service",
                (
                    "jsonb",
                    "{
                        callbackUrl
                        handle
                        id
                        inventoryManagement
                        location { id }
                        permitsSkuSharing
                        requiresShippingMethod
                        serviceName
                        trackingSupport
                        type
                    }",
                    vec![],
                ),
            ),
            ("status", ("text", "", vec![])),
            ("totalQuantity", ("bigint", "", vec![])),
            (
                "trackingInfo",
                (
                    "jsonb",
                    "(first: 250) {
                        company
                        number
                        url
                    }",
                    vec![],
                ),
            ),
            ("updatedAt", ("timestamp", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/businessentity
        "businessEntities" => HashMap::from([
            (
                "address",
                (
                    "jsonb",
                    "{
                        address1
                        address2
                        city
                        countryCode
                        province
                        zip
                    }",
                    vec![],
                ),
            ),
            ("archived", ("boolean", "", vec![])),
            ("companyName", ("text", "", vec![])),
            ("displayName", ("text", "", vec![])),
            ("id", ("text", "", vec![])),
            ("primary", ("boolean", "", vec![])),
            (
                "shopifyPaymentsAccount",
                (
                    "jsonb",
                    "{
                        accountOpenerName
                        activated
                        balance { ...MoneyV2Fields }
                        balanceTransactions (first: 250) {
                            nodes { id }
                        }
                        bankAccounts (first: 250) {
                            nodes {
                                accountNumberLastDigits
                                bankName
                                country
                                createdAt
                                currency
                                id
                                payouts (first: 250) {
                                    nodes { id }
                                }
                                status
                            }
                        }
                        chargeStatementDescriptors {
                            default
                            prefix
                        }
                        country
                        defaultCurrency
                        disputes (first: 250) {
                            nodes { id }
                        }
                        id
                        onboardable
                        payouts (first: 250) {
                            nodes { id }
                        }
                        payoutSchedule {
                            interval
                            monthlyAnchor
                            weeklyAnchor
                        }
                        payoutStatementDescriptor
                    }",
                    vec![money_v2],
                ),
            ),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/refund
        "refund" => HashMap::from([
            ("createdAt", ("timestamp", "", vec![])),
            (
                "duties",
                (
                    "jsonb",
                    "{
                        amountSet { ...MoneyBagFields }
                        originalDuty {
                            countryCodeOfOrigin
                            harmonizedSystemCode
                            id
                            price { ...MoneyBagFields }
                            taxLines { ...TaxLineFields }
                        }
                    }",
                    vec![money_v2, money_bag, tax_line],
                ),
            ),
            ("id", ("text", "", vec![])),
            ("legacyResourceId", ("bigint", "", vec![])),
            ("note", ("text", "", vec![])),
            ("order", ("jsonb", "{ id }", vec![])),
            (
                "orderAdjustments",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            amountSet { ...MoneyBagFields }
                            id
                            reason
                            taxAmountSet { ...MoneyBagFields }
                        }
                    }",
                    vec![money_v2, money_bag],
                ),
            ),
            (
                "refundLineItems",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            ...RefundLineItemFields
                        }
                    }",
                    vec![money_v2, money_bag, refund_line_item],
                ),
            ),
            (
                "refundShippingLines",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            id
                            shippingLine {
                                carrierIdentifier
                                code
                                currentDiscountedPriceSet { ...MoneyBagFields }
                                custom
                                deliveryCategory
                                discountAllocations {
                                    allocatedAmountSet { ...MoneyBagFields }
                                    discountApplication {
                                        allocationMethod
                                        index
                                        targetSelection
                                        targetType
                                        value
                                    }
                                }
                                discountedPriceSet { ...MoneyBagFields }
                                id
                                isRemoved
                                originalPriceSet { ...MoneyBagFields }
                                phone
                                shippingRateHandle
                                source
                                taxLines { ...TaxLineFields }
                                title
                            }
                            subtotalAmountSet { ...MoneyBagFields }
                            taxAmountSet { ...MoneyBagFields }
                        }
                    }",
                    vec![money_v2, money_bag, tax_line],
                ),
            ),
            ("return", ("jsonb", "{ id }", vec![])),
            (
                "staffMember",
                (
                    "jsonb",
                    "{ ...StaffMemberFields }",
                    vec![metafield, image, staff_member],
                ),
            ),
            (
                "totalRefundedSet",
                ("jsonb", "{ ...MoneyBagFields }", vec![money_v2, money_bag]),
            ),
            (
                "transactions",
                (
                    "jsonb",
                    "{ ...OrderTransactionFields }",
                    vec![money_v2, money_bag, order_transaction],
                ),
            ),
            ("updatedAt", ("timestamp", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/return
        "return" => HashMap::from([
            ("closedAt", ("timestamp", "", vec![])),
            ("createdAt", ("timestamp", "", vec![])),
            ("decline", ("jsonb", "{ note reason }", vec![])),
            (
                "exchangeLineItems",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            id
                            lineItems { id }
                            processableQuantity
                            processedQuantity
                            quantity
                            unprocessedQuantity
                            variantId
                        }
                    }",
                    vec![],
                ),
            ),
            ("id", ("text", "", vec![])),
            ("name", ("text", "", vec![])),
            ("order", ("jsonb", "{ id }", vec![])),
            (
                "refunds",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            ("requestApprovedAt", ("timestamp", "", vec![])),
            (
                "returnLineItems",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            customerNote
                            id
                            processableQuantity
                            processedQuantity
                            quantity
                            refundableQuantity
                            refundedQuantity
                            returnReason
                            returnReasonNote
                            unprocessedQuantity
                        }
                    }",
                    vec![],
                ),
            ),
            (
                "returnShippingFees",
                (
                    "jsonb",
                    "{
                        amountSet { ...MoneyBagFields }
                        id
                    }",
                    vec![money_v2, money_bag],
                ),
            ),
            (
                "reverseFulfillmentOrders",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            id
                            lineItems (first: 250) {
                                nodes { id }
                            }
                            order { id }
                            reverseDeliveries (first: 250) {
                                nodes { id }
                            }
                            status
                            thirdPartyConfirmation {
                                    status
                            }
                        }
                    }",
                    vec![],
                ),
            ),
            ("status", ("text", "", vec![])),
            ("totalQuantity", ("bigint", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/inventorylevel
        "inventoryLevel" => HashMap::from([
            ("canDeactivate", ("boolean", "", vec![])),
            ("createdAt", ("timestamp", "", vec![])),
            ("deactivationAlert", ("text", "", vec![])),
            ("id", ("text", "", vec![])),
            ("item", ("jsonb", "{ id sku }", vec![])),
            ("location", ("jsonb", "{ id }", vec![])),
            (
                "scheduledChanges",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            expectedAt
                            fromName
                            inventoryLevel { id }
                            ledgerDocumentUri
                            quantity
                            toName
                        }
                    }",
                    vec![],
                ),
            ),
            ("updatedAt", ("timestamp", "", vec![])),
        ]),

        // ref: https://shopify.dev/docs/api/admin-graphql/latest/objects/shop
        "shop" => HashMap::from([
            (
                "accountOwner",
                (
                    "jsonb",
                    "{ ...StaffMemberFields }",
                    vec![metafield, image, staff_member],
                ),
            ),
            (
                "alerts",
                (
                    "jsonb",
                    "{
                        action {
                            title
                            url
                        }
                        description
                    }",
                    vec![],
                ),
            ),
            (
                "allProductCategoriesList",
                (
                    "jsonb",
                    "{
                        ancestorIds
                        childrenIds
                        fullName
                        id
                        isArchived
                        isLeaf
                        isRoot
                        level
                        name
                        parentId
                    }",
                    vec![],
                ),
            ),
            (
                "availableChannelApps",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { id }
                    }",
                    vec![],
                ),
            ),
            (
                "billingAddress",
                (
                    "jsonb",
                    "{
                        address1
                        address2
                        city
                        company
                        coordinatesValidated
                        country
                        countryCodeV2
                        formatted
                        formattedArea
                        id
                        latitude
                        longitude
                        phone
                        province
                        provinceCode
                        zip
                    }",
                    vec![],
                ),
            ),
            (
                "channelDefinitionsForInstalledChannels",
                (
                    "jsonb",
                    "{
                        channelDefinitions {
                            channelName
                            handle
                            id
                            isMarketplace
                            subChannelName
                        }
                        channelName
                    }",
                    vec![],
                ),
            ),
            ("checkoutApiSupported", ("boolean", "", vec![])),
            ("contactEmail", ("text", "", vec![])),
            (
                "countriesInShippingZones",
                (
                    "jsonb",
                    "{
                        countryCodes
                        includeRestOfWorld
                    }",
                    vec![],
                ),
            ),
            ("createdAt", ("timestamp", "", vec![])),
            ("currencyCode", ("text", "", vec![])),
            (
                "currencyFormats",
                (
                    "jsonb",
                    "{
                        moneyFormat
                        moneyInEmailsFormat
                        moneyWithCurrencyFormat
                        moneyWithCurrencyInEmailsFormat
                    }",
                    vec![],
                ),
            ),
            (
                "currencySettings",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            currencyCode
                            currencyName
                            enabled
                            manualRate
                            rateUpdatedAt
                        }
                    }",
                    vec![],
                ),
            ),
            ("customerAccounts", ("text", "", vec![])),
            (
                "customerAccountsV2",
                (
                    "jsonb",
                    "{
                        customerAccountsVersion
                        loginLinksVisibleOnStorefrontAndCheckout
                        loginRequiredAtCheckout
                        url
                    }",
                    vec![],
                ),
            ),
            ("customerTags", ("jsonb", "(first: 250) { nodes }", vec![])),
            ("description", ("text", "", vec![])),
            (
                "draftOrderTags",
                ("jsonb", "(first: 250) { nodes }", vec![]),
            ),
            ("email", ("text", "", vec![])),
            ("enabledPresentmentCurrencies", ("text", "", vec![])),
            (
                "entitlements",
                (
                    "jsonb",
                    "{
                        markets {
                            b2b {
                                catalogs { enabled }
                                enabled
                            }
                            regions {
                                catalogs { enabled }
                                enabled
                            }
                            retail {
                                catalogs { enabled }
                                enabled
                            }
                            themes { enabled }
                        }
                    }",
                    vec![],
                ),
            ),
            (
                "features",
                (
                    "jsonb",
                    "{
                        avalaraAvatax
                        branding
                        bundles {
                            eligibleForBundles
                            ineligibilityReason
                            sellsBundles
                        }
                        captcha
                        cartTransform {
                            eligibleOperations {
                                expandOperation
                                mergeOperation
                                updateOperation
                            }
                        }
                        dynamicRemarketing
                        eligibleForSubscriptionMigration
                        eligibleForSubscriptions
                        giftCards
                        harmonizedSystemCode
                        legacySubscriptionGatewayEnabled
                        liveView
                        paypalExpressSubscriptionGatewayStatus
                        reports
                        sellsSubscriptions
                        showMetrics
                        storefront
                        unifiedMarkets
                        usingShopifyBalance
                    }",
                    vec![],
                ),
            ),
            (
                "fulfillmentServices",
                (
                    "jsonb",
                    "{
                        callbackUrl
                        handle
                        id
                        inventoryManagement
                        location { id }
                        permitsSkuSharing
                        requiresShippingMethod
                        serviceName
                        trackingSupport
                        type
                    }",
                    vec![],
                ),
            ),
            ("ianaTimezone", ("text", "", vec![])),
            ("id", ("text", "", vec![])),
            (
                "marketingSmsConsentEnabledAtCheckout",
                ("boolean", "", vec![]),
            ),
            (
                "merchantApprovalSignals",
                (
                    "jsonb",
                    "{
                        identityVerified
                        verifiedByShopify
                        verifiedByShopifyTier
                    }",
                    vec![],
                ),
            ),
            (
                "metafields",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes { ...MetafieldFields }
                    }",
                    vec![metafield],
                ),
            ),
            ("myshopifyDomain", ("text", "", vec![])),
            ("name", ("text", "", vec![])),
            (
                "navigationSettings",
                (
                    "jsonb",
                    "{
                        id
                        title
                        url
                    }",
                    vec![],
                ),
            ),
            ("orderNumberFormatPrefix", ("text", "", vec![])),
            ("orderNumberFormatSuffix", ("text", "", vec![])),
            (
                "orderTags",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes
                    }",
                    vec![],
                ),
            ),
            (
                "paymentSettings",
                ("jsonb", "{ supportedDigitalWallets }", vec![]),
            ),
            (
                "plan",
                (
                    "jsonb",
                    "{
                        partnerDevelopment
                        publicDisplayName
                        shopifyPlus
                    }",
                    vec![],
                ),
            ),
            (
                "primaryDomain",
                (
                    "jsonb",
                    "{
                        host
                        id
                        localization {
                            alternateLocales
                            country
                            defaultLocale
                        }
                        marketWebPresence {
                            alternateLocales {
                                locale
                                marketWebPresences { id }
                                name
                                primary
                                published
                            }
                            defaultLocale {
                                locale
                                name
                                primary
                                published
                            }
                            domain {
                                host
                                id
                                localization {
                                    alternateLocales
                                    country
                                    defaultLocale
                                }
                                marketWebPresence { id }
                                sslEnabled
                                url
                            }
                            id
                            markets (first: 250) {
                                nodes { id name }
                            }
                            rootUrls {
                                locale
                                url
                            }
                            subfolderSuffix
                        }
                        sslEnabled
                        url
                    }",
                    vec![],
                ),
            ),
            (
                "resourceLimits",
                (
                    "jsonb",
                    "{
                        locationLimit
                        maxProductOptions
                        maxProductVariants
                        redirectLimitReached
                    }",
                    vec![],
                ),
            ),
            ("richTextEditorUrl", ("text", "", vec![])),
            (
                "searchFilters",
                (
                    "jsonb",
                    "{
                        productAvailability {
                            label
                            value
                        }
                    }",
                    vec![],
                ),
            ),
            ("setupRequired", ("boolean", "", vec![])),
            ("shipsToCountries", ("json", "", vec![])),
            ("shopOwnerName", ("text", "", vec![])),
            (
                "shopPolicies",
                (
                    "jsonb",
                    "{
                        body
                        createdAt
                        id
                        title
                        type
                        updatedAt
                        url
                    }",
                    vec![],
                ),
            ),
            (
                "storefrontAccessTokens",
                (
                    "jsonb",
                    "(first: 250) {
                        nodes {
                            accessScopes {
                                description
                                handle
                            }
                            accessToken
                            createdAt
                            id
                            title
                            updatedAt
                        }
                    }",
                    vec![],
                ),
            ),
            ("taxesIncluded", ("boolean", "", vec![])),
            ("taxShipping", ("boolean", "", vec![])),
            ("timezoneAbbreviation", ("text", "", vec![])),
            ("timezoneOffset", ("text", "", vec![])),
            ("timezoneOffsetMinutes", ("bigint", "", vec![])),
            ("transactionalSmsDisabled", ("boolean", "", vec![])),
            ("unitSystem", ("text", "", vec![])),
            ("updatedAt", ("timestamp", "", vec![])),
            ("url", ("text", "", vec![])),
            ("weightUnit", ("text", "", vec![])),
        ]),

        _ => HashMap::new(),
    };

    // convert all elements to owned
    field_map
        .into_iter()
        .map(|(k, v)| {
            (
                k.to_owned(),
                (
                    v.0.to_owned(),
                    v.1.to_owned(),
                    v.2.iter().map(|v| v.to_string()).collect(),
                ),
            )
        })
        .collect()
}
