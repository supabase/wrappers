# Wrappers Platform Security

> **Security Disclosure**: If you discover a security vulnerability, please report it via https://supabase.com/.well-known/security.txt

This document covers security measures implemented across the entire Wrappers platform. For FDW-specific security concerns, see the individual FDW documentation.

---

## 1. Supply Chain Security - WASM Package Verification

### Protection
All WASM FDW servers **require** the `fdw_package_checksum` option to prevent supply chain attacks.

```sql
-- This will FAIL - checksum is required
CREATE SERVER my_server
  FOREIGN DATA WRAPPER wasm_wrapper
  OPTIONS (
    fdw_package_url 'https://example.com/my_fdw.wasm',
    fdw_package_name 'my-fdw',
    fdw_package_version '1.0.0'
    -- Missing fdw_package_checksum!
  );

-- This will work
CREATE SERVER my_server
  FOREIGN DATA WRAPPER wasm_wrapper
  OPTIONS (
    fdw_package_url 'https://example.com/my_fdw.wasm',
    fdw_package_name 'my-fdw',
    fdw_package_version '1.0.0',
    fdw_package_checksum 'sha256:abc123...'  -- Required!
  );
```

### Implementation
- Enforced in `wrappers/src/fdw/wasm_fdw/wasm_fdw.rs` validator function
- Checksum is verified when the WASM package is loaded
- Mismatch causes the query to fail

### Calculating Checksums
```bash
sha256sum my_fdw.wasm | awk '{print "sha256:" $1}'
```

---

## 2. Credential Masking in Error Messages

### Protection
All FDW error messages are sanitized to prevent credential leakage using the `sanitize_error_message()` utility.

### Sensitive Patterns Detected
The following option names are automatically masked:
- Generic: `password`, `secret`, `token`, `api_key`, `credentials`
- AWS: `aws_secret_access_key`, `aws_session_token`
- GCP: `service_account_key`
- Azure: `client_secret`, `storage_key`, `connection_string`
- Database: `conn_string`, `db_password`
- Service-specific: `stripe_api_key`, `firebase_credentials`, `motherduck_token`

### Example
```
Before: "Error: aws_secret_access_key = 'wJalrXUtnFEMI/K7MDENG' is invalid"
After:  "Error: aws_secret_access_key = 'wJal***' is invalid"
```

### Implementation
- Core utility in `supabase-wrappers/src/utils.rs`
- Applied to error handlers in all FDWs:
  - DuckDB FDW (SQL with embedded credentials)
  - Airtable, Auth0, Firebase, Stripe, Logflare, Cognito FDWs

### Usage in Custom FDWs
```rust
use supabase_wrappers::prelude::sanitize_error_message;

impl From<MyFdwError> for ErrorReport {
    fn from(value: MyFdwError) -> Self {
        let error_message = sanitize_error_message(&format!("{value}"));
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
    }
}
```

---

## 3. Response Size Limits

### Protection
HTTP-based FDWs enforce configurable response size limits to prevent denial-of-service attacks via maliciously large responses. Responses exceeding the limit are rejected before JSON parsing.

### Configuration
Set `max_response_size` (in bytes) as a server option:

```sql
CREATE SERVER stripe_server
  FOREIGN DATA WRAPPER stripe_wrapper
  OPTIONS (
    api_key_id 'your-vault-secret-id',
    max_response_size '5242880'  -- 5 MB limit
  );
```

### Default Value
All supported FDWs default to **10 MB** (10,485,760 bytes) if not specified.

### Supported FDWs

| FDW | Support | Notes |
|-----|---------|-------|
| Stripe | ✅ | Full implementation |
| Airtable | ✅ | Full implementation |
| Firebase | ✅ | Full implementation |
| Logflare | ✅ | Full implementation |
| Auth0 | ❌ | Uses SDK architecture |
| Cognito | ❌ | Uses AWS SDK |
| DuckDB | ❌ | Not HTTP-based |
| WASM FDWs | Per-FDW | Implemented individually |

### Error Behavior
When a response exceeds the limit:
```
ERROR: response too large (15728640 bytes). Maximum allowed: 10485760 bytes
```

### Implementation
- Each FDW has a `ResponseTooLarge` error variant
- Response body is read as text and size-checked before JSON parsing
- Example in `wrappers/src/fdw/stripe_fdw/stripe_fdw.rs`

### Known Limitations
The size check occurs **after** the response body is read into memory. This means:
- An extremely large response could temporarily consume memory before being rejected
- This protects against storing/parsing oversized data, but not against memory exhaustion during the HTTP read phase
- For stronger protection, consider network-level controls (e.g., reverse proxy response size limits)

Future improvement: Implement streaming reads with size checking during the read phase.

---

## 4. Option Value Security

### Protection
Option values that fail UTF-8 validation do not include the raw bytes in error messages, preventing binary credential leakage.

### Implementation
- `supabase-wrappers/src/options.rs` - `OptionsError::OptionValueIsInvalidUtf8` only includes option name, not value

---

## Security Status

| Protection | Status | Location |
|------------|--------|----------|
| WASM checksum required | ✓ Implemented | `wasm_fdw.rs` validator |
| Credential masking | ✓ Implemented | `utils.rs`, all FDW error handlers |
| Response size limits | ✓ Implemented | Stripe, Airtable, Firebase, Logflare FDWs |
| Option value protection | ✓ Implemented | `options.rs` |

---

## FDW-Specific Security

Each FDW may have additional security considerations:

- **AWS FDW**: SSRF protection for `endpoint_url` - see `wasm-wrappers/fdw/aws_fdw/SECURITY.md`
- **DuckDB FDW**: SQL injection via server type options
- **HTTP-based FDWs**: Request/response validation

---

## Reporting Security Issues

Please report security vulnerabilities to the Supabase security team rather than opening public issues.
