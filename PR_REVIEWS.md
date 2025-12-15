## Review 1

```
The response handling logic has a potential issue when the API returns a single object nested in a "data" field (e.g., {"data": {...}}). The code first tries to parse /data as an array (line 258-259), which fails for single objects. Then it falls back to checking if the root resp_json is an object (line 261-262), which would incorrectly wrap the entire response including the "data" wrapper, rather than extracting the inner object from the "data" field.

Consider handling the case where /data contains a single object by checking if it's an object and wrapping it in an array before the or_else fallback.
```

## Review 2

```
The comment states these endpoints "return single objects", but the "billing/payment_attempts" endpoint is documented as "ListBillingStatementPaymentAttempts" in the Clerk API docs (line 775 in docs/catalog/clerk.md), which suggests it returns an array. Consider clarifying this comment to reflect that some of these endpoints may return arrays while others return single objects, or verify the actual API response format for billing/payment_attempts.
```

## Review 3

```
The billing_statement table schema is inconsistent with billing_statements. The singular endpoint table uses statement_id as the identifying column while the plural endpoint uses id. This forces users to access the actual statement ID through the attrs jsonb column. Consider adding an id column to billing_statement to match the schema pattern of other billing tables and allow direct access to the statement's ID field from the API response, while keeping statement_id for the query parameter.
```

## Review 4

```
The billing_payment_attempts table schema may be missing an id column. Payment attempts likely have unique identifiers in the API response, but the current schema only exposes statement_id (from the query parameter), status, and timestamps. Users cannot access the payment attempt's own ID except through the attrs jsonb column. Consider adding an id column to match the schema pattern used in other billing tables.
```

## Review 5

```
The query string with offset/limit parameters is built for all endpoints (lines 131-142) and used to construct the URL (line 143), but for parameterized endpoints (users/billing/subscription, organizations/billing/subscription, billing/statement, billing/payment_attempts), the URL is completely replaced (lines 153, 167, 181, 195), discarding these parameters. Consider checking if the endpoint is parameterized before building the query string to avoid unnecessary string allocation and formatting.
```