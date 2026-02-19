#!/usr/bin/env bash
# Integration test runner for OpenAPI FDW
# Uses MockServer for deterministic testing (no network dependency).
# Set RUN_NWS_TESTS=1 to also run live NWS API tests.
set -euo pipefail

cd "$(dirname "$0")/.."

WASM_BIN="../target/wasm32-unknown-unknown/release/openapi_fdw.wasm"

PASS=0
FAIL=0

psql_cmd() {
  docker compose -f test/docker-compose.yml exec -T -e PGPASSWORD="${POSTGRES_PASSWORD:-postgres}" db psql -U postgres -P pager=off "$@"
}

# Run a query, check output contains expected substring
run_test() {
  local test_name="$1"
  local sql="$2"
  local expected="$3"

  echo ""
  echo "--- $test_name ---"
  local output
  output=$(psql_cmd -c "$sql" 2>&1) || true
  echo "$output"

  if echo "$output" | grep -q "$expected"; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL — expected to find: $expected"
    FAIL=$((FAIL + 1))
  fi
}

# Run a count query, check exact row count
run_count_test() {
  local test_name="$1"
  local sql="$2"
  local expected_count="$3"

  echo ""
  echo "--- $test_name ---"
  local output
  output=$(psql_cmd -t -c "$sql" 2>&1) || true
  local count
  count=$(echo "$output" | tr -d ' \n')
  echo "count=$count"

  if [ "$count" = "$expected_count" ]; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL — expected $expected_count, got $count"
    FAIL=$((FAIL + 1))
  fi
}

# Run a query, check output contains expected ERROR substring
run_error_test() {
  local test_name="$1"
  local sql="$2"
  local expected="$3"

  echo ""
  echo "--- $test_name ---"
  local output
  output=$(psql_cmd -c "$sql" 2>&1) || true
  echo "$output"

  if echo "$output" | grep -qi "$expected"; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL — expected error containing: $expected"
    FAIL=$((FAIL + 1))
  fi
}

# ---- Build ----

echo "=== Building WASM binary ==="
make build
chmod +r "$WASM_BIN"

# ---- Start services ----

echo ""
echo "=== Starting services ==="
docker compose -f test/docker-compose.yml down -v 2>/dev/null || true
docker compose -f test/docker-compose.yml up -d

echo "Waiting for MockServer..."
for i in $(seq 1 180); do
  if curl -sf --max-time 2 -X PUT http://localhost:1080/mockserver/status > /dev/null 2>&1; then
    echo "MockServer ready after ${i}s"
    break
  fi
  sleep 1
done
if ! curl -sf --max-time 2 -X PUT http://localhost:1080/mockserver/status > /dev/null 2>&1; then
  echo "ERROR: MockServer failed to start within 180s"
  docker compose -f test/docker-compose.yml logs mockserver
  exit 1
fi

echo "Loading MockServer expectations..."
curl -sf -X PUT http://localhost:1080/mockserver/expectation -d @test/expectations.json > /dev/null
echo "MockServer expectations loaded"

echo "Waiting for Postgres..."
for i in $(seq 1 120); do
  if docker compose -f test/docker-compose.yml exec -T db pg_isready -U supabase_admin > /dev/null 2>&1; then
    echo "Postgres ready after ${i}s"
    break
  fi
  sleep 1
done
if ! docker compose -f test/docker-compose.yml exec -T db pg_isready -U supabase_admin > /dev/null 2>&1; then
  echo "ERROR: Postgres failed to start within 120s"
  docker compose -f test/docker-compose.yml logs db
  exit 1
fi
sleep 3  # wait for init scripts

echo ""
echo "=== Copying WASM binary into container ==="
container=$(docker compose -f test/docker-compose.yml ps -q db)
docker cp "$WASM_BIN" "$container":/openapi_fdw.wasm
docker compose -f test/docker-compose.yml exec -T db chmod 644 /openapi_fdw.wasm

echo ""
echo "=== Loading custom spec into MockServer ==="
# Serve the spec at /openapi.json for the FDW's spec_url
python3 -c "
import json, sys
spec = json.load(open('test/mock-spec.json'))
exp = {
    'httpRequest': {'method': 'GET', 'path': '/openapi.json'},
    'httpResponse': {
        'statusCode': 200,
        'headers': {'content-type': ['application/json']},
        'body': json.dumps(spec)
    }
}
sys.stdout.write(json.dumps(exp))
" | curl -sf -X PUT http://localhost:1080/mockserver/expectation -d @- > /dev/null
echo "Spec served at /openapi.json"

# Load spec into MockServer OpenAPI mode (auto-generates endpoint responses)
# May fail if MockServer rejects advanced OpenAPI features (e.g., 2XX wildcards) — non-fatal
if curl -sf -X PUT http://localhost:1080/mockserver/openapi \
  -d '{"specUrlOrPayload":"file:/config/mock-spec.json"}' > /dev/null 2>&1; then
  echo "OpenAPI expectations loaded"
else
  echo "Warning: MockServer OpenAPI auto-generation failed (expected with 2XX/3.1 features)"
  echo "IMPORT FOREIGN SCHEMA tests still work via /openapi.json endpoint"
fi

# ---- Mock API Tests: Core ----

echo ""
echo "========================================="
echo "  Mock API Tests — Core"
echo "========================================="

run_test "Test 1: Basic JSON array" \
  "SELECT id, name, price, in_stock FROM mock_items;" \
  "Widget"

run_count_test "Test 1b: Row count" \
  "SELECT count(*) FROM mock_items;" \
  "3"

run_test "Test 2: Wrapped response (auto-detect 'data' key)" \
  "SELECT id, name, price FROM mock_products;" \
  "Laptop"

run_count_test "Test 2b: Row count" \
  "SELECT count(*) FROM mock_products;" \
  "2"

run_test "Test 3: GeoJSON nested objects" \
  "SELECT station_id, name, state, elevation FROM mock_stations;" \
  "Denver International"

run_count_test "Test 3b: Row count" \
  "SELECT count(*) FROM mock_stations;" \
  "3"

run_count_test "Test 4: Cursor pagination (token) — all pages" \
  "SELECT count(*) FROM mock_paginated_items;" \
  "5"

run_test "Test 4b: Verify last page data" \
  "SELECT id, value FROM mock_paginated_items;" \
  "page3-a"

run_count_test "Test 5: URL-based pagination — all pages" \
  "SELECT count(*) FROM mock_url_paginated;" \
  "3"

run_test "Test 5b: Verify page 2 data" \
  "SELECT id, label FROM mock_url_paginated;" \
  "third"

run_test "Test 6: Query parameter pushdown" \
  "SELECT id, name, category FROM mock_search WHERE category = 'electronics';" \
  "Phone"

run_count_test "Test 6b: Row count" \
  "SELECT count(*) FROM mock_search WHERE category = 'electronics';" \
  "2"

run_test "Test 7: Path parameter substitution" \
  "SELECT id, title FROM mock_user_posts WHERE user_id = '42';" \
  "First Post"

run_count_test "Test 7b: Row count" \
  "SELECT count(*) FROM mock_user_posts WHERE user_id = '42';" \
  "2"

run_test "Test 8: camelCase column matching" \
  "SELECT first_name, last_name, email_address FROM mock_camel;" \
  "Alice"

run_test "Test 8b: Second row" \
  "SELECT first_name, last_name FROM mock_camel;" \
  "Bob"

run_count_test "Test 9: 404 returns empty" \
  "SELECT count(*) FROM mock_not_found WHERE id = '999';" \
  "0"

run_test "Test 10: Rate limiting 429 retry" \
  "SELECT id, status FROM mock_rate_limited;" \
  "ok"

# ---- Auth Tests ----

echo ""
echo "========================================="
echo "  Auth Tests"
echo "========================================="

run_test "Test 11: API key (default Authorization: Bearer)" \
  "SELECT status FROM mock_auth_bearer_data;" \
  "authenticated"

run_test "Test 12: API key (custom header X-API-Key)" \
  "SELECT status FROM mock_auth_custom_data;" \
  "custom_auth"

run_test "Test 13: API key (custom prefix Token)" \
  "SELECT status FROM mock_auth_prefix_data;" \
  "prefix_auth"

run_test "Test 14: Bearer token" \
  "SELECT status FROM mock_auth_token_data;" \
  "bearer_auth"

run_test "Test 15: Custom headers JSON" \
  "SELECT status FROM mock_auth_headers_data;" \
  "headers_json"

run_test "Test 15b: API key in query parameter (Fix 6)" \
  "SELECT status FROM mock_auth_query_data;" \
  "query_auth"

run_test "Test 15c: API key in cookie (Fix 13)" \
  "SELECT status FROM mock_auth_cookie_data;" \
  "cookie_auth"

# ---- Type Coercion & Data Format Tests ----

echo ""
echo "========================================="
echo "  Type Coercion & Data Format Tests"
echo "========================================="

run_test "Test 16: Type coercion — text, bool, int" \
  "SELECT name, active, count FROM mock_typed_data;" \
  "typed-row"

run_test "Test 16b: Date parsing" \
  "SELECT created_date FROM mock_typed_data;" \
  "2024-01-15"

run_test "Test 16c: Timestamp parsing" \
  "SELECT updated_at FROM mock_typed_data;" \
  "2024-06-15"

run_count_test "Test 16d: Null handling" \
  "SELECT count(*) FROM mock_typed_data WHERE nullable_field IS NULL;" \
  "1"

run_test "Test 16e: UUID as text" \
  "SELECT uuid_field FROM mock_typed_data;" \
  "550e8400"

run_test "Test 16f: JSONB array column" \
  "SELECT tags FROM mock_typed_data;" \
  "a"

run_test "Test 16g: Float (real) column" \
  "SELECT score FROM mock_typed_data;" \
  "3.14"

run_test "Test 17: Single object response" \
  "SELECT id, name, version FROM mock_singleton;" \
  "singleton"

run_count_test "Test 17b: Singleton row count" \
  "SELECT count(*) FROM mock_singleton;" \
  "1"

run_count_test "Test 18: Empty array response" \
  "SELECT count(*) FROM mock_empty;" \
  "0"

run_test "Test 19: Records wrapper auto-detect" \
  "SELECT id, value FROM mock_records;" \
  "rec-wrap"

run_count_test "Test 19b: Records row count" \
  "SELECT count(*) FROM mock_records;" \
  "2"

run_test "Test 20: Entries wrapper auto-detect" \
  "SELECT id, label FROM mock_entries;" \
  "entry-1"

run_count_test "Test 20b: Entries row count" \
  "SELECT count(*) FROM mock_entries;" \
  "2"

run_test "Test 21: Results wrapper auto-detect" \
  "SELECT id, value FROM mock_results;" \
  "res-wrap"

run_count_test "Test 21b: Results row count" \
  "SELECT count(*) FROM mock_results;" \
  "1"

# ---- Column Matching Tests ----

echo ""
echo "========================================="
echo "  Column Matching Tests"
echo "========================================="

run_test "Test 22: PascalCase → case-insensitive match" \
  "SELECT id, name, age FROM mock_pascal;" \
  "PascalAlice"

run_count_test "Test 22b: PascalCase row count" \
  "SELECT count(*) FROM mock_pascal;" \
  "2"

# ---- Pagination Edge Cases ----

echo ""
echo "========================================="
echo "  Pagination Edge Cases"
echo "========================================="

run_count_test "Test 23: has_more pattern — all pages" \
  "SELECT count(*) FROM mock_has_more;" \
  "2"

run_test "Test 23b: has_more data verification" \
  "SELECT id, value FROM mock_has_more;" \
  "hm2"

run_count_test "Test 24: Relative URL pagination — all pages" \
  "SELECT count(*) FROM mock_relative_paged;" \
  "2"

run_test "Test 24b: Relative URL data verification" \
  "SELECT id, label FROM mock_relative_paged;" \
  "rel2"

# Table-level page_size isolation: query table with override first, then server default.
# MockServer expectations require exact limit= values — if config leaks between scans,
# the second query gets no matching expectation and fails.
run_test "Test 47: Table-level page_size override (limit=5)" \
  "SELECT id, value FROM mock_config_custom_page;" \
  "custom-page-size"

run_test "Test 48: Server default page_size restored after override (limit=10)" \
  "SELECT id, value FROM mock_config_default_page;" \
  "server-default-size"

# ---- URL Construction Tests ----

echo ""
echo "========================================="
echo "  URL Construction Tests"
echo "========================================="

run_test "Test 25: Multiple path parameters" \
  "SELECT id, title FROM mock_multi_path WHERE org = 'acme' AND repo = 'widget';" \
  "Bug report"

run_count_test "Test 25b: Multi-path row count" \
  "SELECT count(*) FROM mock_multi_path WHERE org = 'acme' AND repo = 'widget';" \
  "2"

run_test "Test 26: Rowid pushdown (single resource)" \
  "SELECT id, name FROM mock_resources WHERE id = 'res-42';" \
  "found-by-id"

run_count_test "Test 26b: Rowid pushdown count" \
  "SELECT count(*) FROM mock_resources WHERE id = 'res-42';" \
  "1"

# ---- Error Handling Tests ----

echo ""
echo "========================================="
echo "  Error Handling Tests"
echo "========================================="

run_error_test "Test 27: HTTP 500 error" \
  "SELECT * FROM mock_server_error;" \
  "error"

run_error_test "Test 28: Invalid JSON response" \
  "SELECT * FROM mock_invalid_json;" \
  "error"

# ---- Edge Case Tests ----

echo ""
echo "========================================="
echo "  Edge Case Tests"
echo "========================================="

run_test "Test 33: Unix timestamp → timestamptz" \
  "SELECT id, name, created_at FROM mock_unix_timestamps;" \
  "epoch-row"

run_test "Test 33b: Unix timestamp parsed as date" \
  "SELECT created_at FROM mock_unix_timestamps;" \
  "2023"

run_test "Test 33c: String datetime still works alongside unix" \
  "SELECT updated_at FROM mock_unix_timestamps;" \
  "2024-06-15"

run_test "Test 34: Acronym field names (clusterIP → cluster_ip)" \
  "SELECT cluster_ip, api_url, html_parser FROM mock_acronym_fields;" \
  "10.0.0.1"

run_test "Test 34b: API URL acronym" \
  "SELECT api_url FROM mock_acronym_fields;" \
  "https://api.test.com"

run_count_test "Test 35: Mixed types with nulls — row count" \
  "SELECT count(*) FROM mock_mixed_types;" \
  "2"

run_count_test "Test 35b: Null handling — count non-null names" \
  "SELECT count(*) FROM mock_mixed_types WHERE name IS NOT NULL;" \
  "1"

run_count_test "Test 35c: Null handling — count null booleans" \
  "SELECT count(*) FROM mock_mixed_types WHERE enabled IS NULL;" \
  "1"

# ---- POST-for-Read Tests ----

echo ""
echo "========================================="
echo "  POST-for-Read Tests"
echo "========================================="

run_test "Test 45: POST-for-read — query data via POST" \
  "SELECT id, label FROM mock_search_post;" \
  "found-via-post"

run_count_test "Test 45b: POST-for-read — row count" \
  "SELECT count(*) FROM mock_search_post;" \
  "1"

# ---- JSON-LD Tests ----

echo ""
echo "========================================="
echo "  JSON-LD Tests"
echo "========================================="

run_count_test "Test 36: @graph wrapper auto-detection" \
  "SELECT count(*) FROM mock_jsonld_alerts;" \
  "2"

run_test "Test 36b: @-prefixed key matching (_id → @id)" \
  "SELECT _id, headline FROM mock_jsonld_alerts;" \
  "urn:alert:1"

run_test "Test 36c: @type key matching" \
  "SELECT _type FROM mock_jsonld_alerts;" \
  "wx:Alert"

run_test "Test 36d: Non-@ column alongside @-columns" \
  "SELECT headline, severity FROM mock_jsonld_alerts;" \
  "Storm warning"

run_count_test "Test 37: JSON-LD GeoJSON with @-keys in properties" \
  "SELECT count(*) FROM mock_jsonld_stations;" \
  "2"

run_test "Test 37b: @id in nested object_path" \
  "SELECT _id FROM mock_jsonld_stations;" \
  "api.weather.gov"

run_test "Test 37c: camelCase + @-keys together" \
  "SELECT station_identifier, name, time_zone FROM mock_jsonld_stations;" \
  "Denver International"

# ---- IMPORT FOREIGN SCHEMA Tests ----

echo ""
echo "========================================="
echo "  IMPORT FOREIGN SCHEMA Tests"
echo "========================================="

# Grant permissions for IMPORT FOREIGN SCHEMA
psql_cmd -U supabase_admin -d postgres \
  -c "GRANT USAGE ON FOREIGN SERVER mock_openapi_server TO postgres;" \
  -c "GRANT ALL ON SCHEMA public TO postgres;" > /dev/null 2>&1

run_test "Test 29: IMPORT FOREIGN SCHEMA from custom spec" \
  "IMPORT FOREIGN SCHEMA \"openapi\" FROM SERVER mock_openapi_server INTO public;" \
  "IMPORT FOREIGN SCHEMA"

run_count_test "Test 29b: Correct number of tables imported (no parameterized paths)" \
  "SELECT count(*) FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server';" \
  "14"

run_test "Test 29c: typed_records table imported" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' ORDER BY foreign_table_name;" \
  "typed_records"

run_test "Test 29d: composed_items table imported (allOf)" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' ORDER BY foreign_table_name;" \
  "composed_items"

run_test "Test 29e: Parameterized paths excluded" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' ORDER BY foreign_table_name;" \
  "singleton"

# Verify type mappings in typed_records
run_test "Test 30: Type mapping — date column" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'typed_records' AND column_name = 'created_date';" \
  "date"

run_test "Test 30b: Type mapping — timestamptz column" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'typed_records' AND column_name = 'updated_at';" \
  "timestamp with time zone"

run_test "Test 30c: Type mapping — integer (int32) column" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'typed_records' AND column_name = 'count';" \
  "integer"

run_test "Test 30d: Type mapping — real (float) column" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'typed_records' AND column_name = 'score';" \
  "real"

run_test "Test 30e: Type mapping — boolean column" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'typed_records' AND column_name = 'active';" \
  "boolean"

run_test "Test 30f: Type mapping — jsonb (array) column" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'typed_records' AND column_name = 'tags';" \
  "jsonb"

# Verify allOf merged properties in composed_items
run_test "Test 31: allOf — has base property (created_at)" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'composed_items' AND column_name = 'created_at';" \
  "created_at"

run_test "Test 31b: allOf — has extended property (title)" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'composed_items' AND column_name = 'title';" \
  "title"

run_test "Test 31c: allOf — has extended property (priority)" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'composed_items' AND column_name = 'priority';" \
  "priority"

# Verify oneOf merged properties in polymorphic (all nullable)
run_test "Test 32: oneOf — has variant 1 property" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'polymorphic' AND column_name = 'user_id';" \
  "user_id"

run_test "Test 32b: oneOf — has variant 2 property" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'polymorphic' AND column_name = 'org_id';" \
  "org_id"

run_test "Test 32c: unix-time format maps to timestamptz" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'typed_records' AND column_name = 'created_epoch';" \
  "timestamp with time zone"

# ---- Fix 4: Multi-type arrays → jsonb ----

run_test "Test 32d: Multi-type array [\"string\",\"integer\"] → jsonb (Fix 4)" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'typed_records' AND column_name = 'multi_type_field';" \
  "jsonb"

# ---- Fix 1: $ref in Response objects ----

run_test "Test 38: ref_response table imported (\$ref resolution)" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' AND foreign_table_name = 'ref_response';" \
  "ref_response"

run_test "Test 38b: \$ref response — has label column" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'ref_response' AND column_name = 'label';" \
  "label"

run_test "Test 38c: \$ref response — has id column" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'ref_response' AND column_name = 'id';" \
  "id"

# ---- Fix 2: 2XX wildcard status codes ----

run_test "Test 39: wildcard_response table imported (2XX)" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' AND foreign_table_name = 'wildcard_response';" \
  "wildcard_response"

run_test "Test 39b: 2XX wildcard — has status column" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'wildcard_response' AND column_name = 'status';" \
  "status"

# ---- Fix 3: writeOnly properties filtered ----

run_test "Test 40: users_write_only table imported" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' AND foreign_table_name = 'users_write_only';" \
  "users_write_only"

run_test "Test 40b: writeOnly — has username (non-writeOnly)" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'users_write_only' AND column_name = 'username';" \
  "username"

run_test "Test 40c: writeOnly — has email (non-writeOnly)" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'users_write_only' AND column_name = 'email';" \
  "email"

run_count_test "Test 40d: writeOnly — password excluded" \
  "SELECT count(*) FROM information_schema.columns WHERE table_name = 'users_write_only' AND column_name = 'password';" \
  "0"

run_count_test "Test 40e: writeOnly — password_hash excluded" \
  "SELECT count(*) FROM information_schema.columns WHERE table_name = 'users_write_only' AND column_name = 'password_hash';" \
  "0"

# ---- Fix 5: Primitive oneOf composition → jsonb ----

run_test "Test 41: primitive_union table imported" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' AND foreign_table_name = 'primitive_union';" \
  "primitive_union"

run_test "Test 41b: Primitive oneOf — value column is jsonb" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'primitive_union' AND column_name = 'value';" \
  "jsonb"

# ---- Fix 8: Content-type with charset ----

run_test "Test 42: charset_endpoint table imported (Fix 8)" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' AND foreign_table_name = 'charset_endpoint';" \
  "charset_endpoint"

run_test "Test 42b: charset endpoint — has id column" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'charset_endpoint' AND column_name = 'id';" \
  "id"

run_test "Test 42c: charset endpoint — has label column" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'charset_endpoint' AND column_name = 'label';" \
  "label"

# ---- Fix 11: uuid format maps to uuid PG type ----

run_test "Test 43: uuid format → uuid type (Fix 11)" \
  "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'typed_records' AND column_name = 'code';" \
  "uuid"

# ---- Fix 12: \$ref with sibling properties ----

run_test "Test 44: ref_with_siblings table imported (Fix 12)" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' AND foreign_table_name = 'ref_with_siblings';" \
  "ref_with_siblings"

run_test "Test 44b: \$ref sibling — has base property (id from BaseEntity)" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'ref_with_siblings' AND column_name = 'id';" \
  "id"

run_test "Test 44c: \$ref sibling — has extra_field (from sibling properties)" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'ref_with_siblings' AND column_name = 'extra_field';" \
  "extra_field"

# ---- POST-for-Read IMPORT Tests ----

run_test "Test 46: POST table imported (search_post)" \
  "SELECT foreign_table_name FROM information_schema.foreign_tables WHERE foreign_server_name = 'mock_openapi_server' AND foreign_table_name = 'search_post';" \
  "search_post"

run_test "Test 46b: POST table has method option" \
  "SELECT ftoptions::text FROM pg_foreign_table WHERE ftrelid = 'search_post'::regclass;" \
  "method=POST"

run_test "Test 46c: POST table has correct columns (id)" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'search_post' AND column_name = 'id';" \
  "id"

run_test "Test 46d: POST table has correct columns (label)" \
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'search_post' AND column_name = 'label';" \
  "label"

# ---- Summary ----

echo ""
echo "========================================="
echo "  Results: $PASS passed, $FAIL failed"
echo "========================================="

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi

# ---- Optional: NWS API Tests ----

if [ "${RUN_NWS_TESTS:-}" = "1" ]; then
  echo ""
  echo "========================================="
  echo "  NWS API Tests (network-dependent)"
  echo "========================================="

  run_test "NWS: Basic stations query" \
    "SELECT station_identifier, name, time_zone FROM nws_stations LIMIT 5;" \
    "America/"

  run_test "NWS: Debug timing" \
    "SELECT station_identifier, name FROM nws_stations_debug LIMIT 5;" \
    "HTTP fetch"

  run_test "NWS: Active weather alerts" \
    "SELECT id, severity, event FROM nws_alerts LIMIT 5;" \
    "urn:oid"

  echo ""
  echo "========================================="
  echo "  NWS Results: $PASS passed, $FAIL failed"
  echo "========================================="
fi

echo ""
echo "=== Done! ==="
echo "Connect manually: psql -h localhost -p 54322 -U postgres"
echo "Tear down: docker compose -f test/docker-compose.yml down -v"
