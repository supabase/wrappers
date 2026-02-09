# WASM Wrappers Platform Tests

This directory contains security tests that apply to ALL WASM-based Foreign Data Wrappers.

For FDW-specific tests, see the individual FDW directories (e.g., `fdw/aws_fdw/tests/`).

## Test Files

| File | Description |
|------|-------------|
| `test_wasm_security.sql` | Platform-wide security tests |

## Security Tests Coverage

### Supply Chain Security (SUPPLY-*)
Tests that verify WASM package integrity:
- `SUPPLY-001`: Missing checksum must be rejected
- `SUPPLY-002`: Invalid checksum verification at load time
- `SUPPLY-003`: Malicious URL handling with checksum requirement

### Credential Masking (CRED-*)
Tests that verify credential protection in error messages:
- `CRED-001`: Credentials should not appear in error messages

### DoS Protection (DOS-*)
Documentation tests for denial-of-service protection:
- `DOS-001`: HTTP timeout handling
- `DOS-002`: Large response protection

## Running Tests

```bash
# Run platform-wide security tests
psql -f tests/test_wasm_security.sql
```

## Related Documentation

- Platform-wide security: [/SECURITY.md](/SECURITY.md)
- FDW-specific security: See individual FDW directories
