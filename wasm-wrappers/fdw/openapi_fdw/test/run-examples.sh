#!/usr/bin/env bash
# Unified test runner for examples.
#
# Usage:
#   ./test/run-examples.sh                  Run all examples
#   ./test/run-examples.sh nws              Run a single example
#   ./test/run-examples.sh nws pokeapi      Run specific examples
#   ./test/run-examples.sh --no-cleanup     Keep container running after tests
#   ./test/run-examples.sh nws --no-cleanup Run one example, keep it running
set -euo pipefail
cd "$(dirname "$0")/.."

WASM_BIN="../target/wasm32-unknown-unknown/release/openapi_fdw.wasm"

CLEANUP=true
EXAMPLES=()
ALL_EXAMPLES=(nws carapi pokeapi github threads)
COMPOSE="test/docker-compose.yml"

while [[ $# -gt 0 ]]; do
  case $1 in
    --no-cleanup) CLEANUP=false; shift ;;
    -h|--help)
      echo "Usage: ./test/run-examples.sh [EXAMPLE...] [--no-cleanup]"
      echo ""
      echo "Examples: ${ALL_EXAMPLES[*]}"
      echo ""
      echo "Options:"
      echo "  --no-cleanup  Keep Docker container running after tests"
      echo ""
      echo "Authenticated examples (github, threads) require tokens in test/.env."
      echo "See test/.env.example for the template."
      exit 0
      ;;
    *)
      EXAMPLES+=("$1"); shift ;;
  esac
done

if [ ${#EXAMPLES[@]} -eq 0 ]; then
  EXAMPLES=("${ALL_EXAMPLES[@]}")
fi

# Validate example names
for ex in "${EXAMPLES[@]}"; do
  found=false
  for valid in "${ALL_EXAMPLES[@]}"; do
    if [ "$ex" = "$valid" ]; then found=true; break; fi
  done
  if [ "$found" = false ]; then
    echo "ERROR: Unknown example '$ex'. Valid: ${ALL_EXAMPLES[*]}"
    exit 1
  fi
done

# Load env vars for authenticated examples
if [ -f "test/.env" ]; then
  set -a
  source test/.env
  set +a
fi

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

PASS=0
FAIL=0

psql_cmd() {
  docker compose -f "$COMPOSE" exec -T -e PGPASSWORD=postgres db psql -U supabase_admin -d postgres -P pager=off "$@"
}

run_test() {
  local name="$1" sql="$2" expected="$3"
  printf "  %-40s " "$name"
  local output
  output=$(psql_cmd -c "$sql" 2>&1) || true
  if echo "$output" | grep -q "$expected"; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL"
    echo "    Expected: $expected"
    echo "    Output:   $(echo "$output" | head -5)"
    FAIL=$((FAIL + 1))
  fi
}

run_count_test() {
  local name="$1" sql="$2" min_count="$3"
  printf "  %-40s " "$name"
  local output
  output=$(psql_cmd -t -c "$sql" 2>&1) || true
  local count
  count=$(echo "$output" | tr -d ' \n')
  if [ "$count" -ge "$min_count" ] 2>/dev/null; then
    echo "PASS ($count rows)"
    PASS=$((PASS + 1))
  else
    echo "FAIL (got $count, expected >= $min_count)"
    FAIL=$((FAIL + 1))
  fi
}

# ---------------------------------------------------------------------------
# Container lifecycle
# ---------------------------------------------------------------------------

CONTAINER_STARTED=false

cleanup() {
  if [ "$CONTAINER_STARTED" = true ] && [ "$CLEANUP" = true ]; then
    echo "==> Cleaning up..."
    docker compose -f "$COMPOSE" down -v 2>/dev/null || true
  elif [ "$CONTAINER_STARTED" = true ]; then
    echo ""
    echo "Container still running (--no-cleanup). To tear down manually:"
    echo "  docker compose -f $COMPOSE down -v"
  fi
}
trap cleanup EXIT

check_auth() {
  case $1 in
    github)
      if [ -z "${GITHUB_TOKEN:-}" ]; then
        echo "ERROR: GITHUB_TOKEN not set."
        echo "  cp test/.env.example test/.env  # then add your token"
        exit 1
      fi
      ;;
    threads)
      if [ -z "${THREADS_ACCESS_TOKEN:-}" ]; then
        echo "ERROR: THREADS_ACCESS_TOKEN not set."
        echo "  cp test/.env.example test/.env  # then add your token"
        exit 1
      fi
      ;;
  esac
}

start_container() {
  echo "==> Starting PostgreSQL..."
  docker compose -f "$COMPOSE" down -v 2>/dev/null || true
  docker compose -f "$COMPOSE" up -d
  CONTAINER_STARTED=true

  echo "Waiting for PostgreSQL..."
  for i in $(seq 1 60); do
    if docker compose -f "$COMPOSE" exec -T db pg_isready -U supabase_admin > /dev/null 2>&1; then
      echo "PostgreSQL ready after ${i}s"
      break
    fi
    if [ "$i" -eq 60 ]; then
      echo "ERROR: PostgreSQL failed to start"
      exit 1
    fi
    sleep 1
  done
  sleep 3

  echo "==> Copying WASM binary into container..."
  local container
  container=$(docker compose -f "$COMPOSE" ps -q db)
  docker cp "$WASM_BIN" "$container":/openapi_fdw.wasm
  docker compose -f "$COMPOSE" exec -T db chmod 644 /openapi_fdw.wasm
}

# Drop all FDW objects so the next example starts clean
reset_fdw() {
  psql_cmd -c "DROP FOREIGN DATA WRAPPER IF EXISTS wasm_wrapper CASCADE;" > /dev/null 2>&1
}

load_example() {
  local name=$1

  echo "==> Loading $name..."
  reset_fdw
  psql_cmd < "examples/$name/init.sql" > /dev/null 2>&1

  case $name in
    github)
      psql_cmd -c "
        ALTER SERVER github OPTIONS (SET api_key '${GITHUB_TOKEN}');
        ALTER SERVER github_debug OPTIONS (SET api_key '${GITHUB_TOKEN}');
      " > /dev/null 2>&1
      ;;
    threads)
      psql_cmd -c "
        ALTER SERVER threads OPTIONS (SET api_key '${THREADS_ACCESS_TOKEN}');
        ALTER SERVER threads_debug OPTIONS (SET api_key '${THREADS_ACCESS_TOKEN}');
        ALTER SERVER threads_import OPTIONS (SET api_key '${THREADS_ACCESS_TOKEN}');
      " > /dev/null 2>&1
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Verify functions — one per example
# ---------------------------------------------------------------------------

verify_nws() {
  echo "=== NWS Weather API ==="
  echo ""

  echo "Stations (GeoJSON + pagination):"
  run_test "Basic query" \
    "SELECT station_identifier, name, time_zone FROM stations LIMIT 3;" \
    "station_identifier"
  run_count_test "Pagination (50+ rows fetched)" \
    "SELECT count(*) FROM (SELECT 1 FROM stations LIMIT 60) t;" \
    51
  run_test "Lookup by rowid_column" \
    "SELECT station_identifier, name FROM stations WHERE station_identifier = 'KDEN';" \
    "KDEN"

  echo ""
  echo "JSONB Columns:"
  run_test "Elevation as jsonb" \
    "SELECT station_identifier, elevation->>'value' AS elev, elevation->>'unitCode' AS unit FROM stations LIMIT 3;" \
    "wmoUnit"

  echo ""
  echo "camelCase Matching:"
  run_test "stationIdentifier -> station_identifier" \
    "SELECT station_identifier FROM stations LIMIT 1;" \
    "station_identifier"
  run_test "timeZone -> time_zone" \
    "SELECT time_zone FROM stations WHERE time_zone IS NOT NULL LIMIT 1;" \
    "(1 row)"

  echo ""
  echo "Active Alerts (timestamptz coercion):"
  run_test "Alerts with timestamps" \
    "SELECT event, severity, headline, onset FROM active_alerts LIMIT 3;" \
    "severity"
  run_test "timestamptz format (onset)" \
    "SELECT onset FROM active_alerts WHERE onset IS NOT NULL LIMIT 1;" \
    "+00"

  echo ""
  echo "Query Param Pushdown (severity=Severe):"
  run_test "Filter by severity" \
    "SELECT event, severity, headline FROM active_alerts WHERE severity = 'Severe' LIMIT 3;" \
    "severity"

  echo ""
  echo "Station Observations (path param):"
  run_test "KDEN observations" \
    "SELECT timestamp, text_description, temperature->>'value' AS temp FROM station_observations WHERE station_id = 'KDEN' LIMIT 3;" \
    "text_description"

  echo ""
  echo "Latest Observation (single object):"
  run_test "Single row response" \
    "SELECT text_description, temperature->>'value' AS temp FROM latest_observation WHERE station_id = 'KDEN';" \
    "(1 row)"

  echo ""
  echo "Point Metadata (composite path param):"
  run_test "Denver coordinates" \
    "SELECT grid_id, grid_x, grid_y FROM point_metadata WHERE point = '39.7456,-104.9887';" \
    "BOU"

  echo ""
  echo "Forecast (multi-path-param + nested response):"
  run_test "Denver forecast" \
    "SELECT name, temperature, temperature_unit, is_daytime, short_forecast FROM forecast_periods WHERE wfo = 'BOU' AND x = '63' AND y = '62' LIMIT 3;" \
    "temperature_unit"

  echo ""
  echo "Type Coercion:"
  run_test "Boolean (is_daytime)" \
    "SELECT is_daytime FROM forecast_periods WHERE wfo = 'BOU' AND x = '63' AND y = '62' LIMIT 1;" \
    "t"
  run_test "Integer (temperature)" \
    "SELECT temperature FROM forecast_periods WHERE wfo = 'BOU' AND x = '63' AND y = '62' LIMIT 1;" \
    "(1 row)"

  echo ""
  echo "LIMIT Pushdown:"
  run_count_test "LIMIT 3 returns exactly 3" \
    "SELECT count(*) FROM (SELECT 1 FROM stations LIMIT 3) t;" \
    3

  echo ""
  echo "Debug Mode:"
  run_test "HTTP request details" \
    "SELECT station_identifier FROM stations_debug LIMIT 1;" \
    "HTTP GET"
  run_test "Scan statistics" \
    "SELECT station_identifier FROM stations_debug LIMIT 1;" \
    "Scan complete"

  echo ""
  echo "IMPORT FOREIGN SCHEMA:"
  psql_cmd -c "DROP SCHEMA IF EXISTS nws_verify CASCADE;" > /dev/null 2>&1
  psql_cmd -c "CREATE SCHEMA nws_verify;" > /dev/null 2>&1
  run_test "Auto-generate tables" \
    "IMPORT FOREIGN SCHEMA \"unused\" FROM SERVER nws_import INTO nws_verify;" \
    "IMPORT FOREIGN SCHEMA"
  run_count_test "Generated tables" \
    "SELECT count(*) FROM information_schema.foreign_tables WHERE foreign_table_schema = 'nws_verify';" \
    1
  psql_cmd -c "DROP SCHEMA nws_verify CASCADE;" > /dev/null 2>&1

  echo ""
  echo "Attrs catch-all column:"
  run_test "Extra fields in attrs" \
    "SELECT station_identifier, attrs->>'county' AS county FROM stations LIMIT 3;" \
    "county"
}

verify_carapi() {
  echo "=== CarAPI ==="
  echo ""

  echo "Makes (pagination + auto-detected wrapper):"
  run_test "Basic query" \
    "SELECT id, name FROM makes LIMIT 5;" \
    "Acura"
  run_count_test "Has makes" \
    "SELECT count(*) FROM (SELECT 1 FROM makes LIMIT 10) t;" \
    5

  echo ""
  echo "Models (query pushdown):"
  run_test "Toyota 2020 models" \
    "SELECT id, name, make FROM models WHERE make = 'Toyota' AND year = '2020' LIMIT 5;" \
    "Toyota"

  echo ""
  echo "Trims (pricing + query pushdown):"
  run_test "2020 Camry trims" \
    "SELECT trim, msrp, description FROM trims WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry' LIMIT 3;" \
    "Sedan"

  echo ""
  echo "Bodies (dimensions):"
  run_test "2020 Camry body" \
    "SELECT type, doors, length, curb_weight FROM bodies WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry' LIMIT 2;" \
    "Sedan"

  echo ""
  echo "Engines (performance data):"
  run_test "2020 Camry engines" \
    "SELECT engine_type, horsepower_hp, cylinders, transmission FROM engines WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry' LIMIT 2;" \
    "horsepower_hp"

  echo ""
  echo "Mileages (fuel economy):"
  run_test "2020 Camry mileage" \
    "SELECT combined_mpg, epa_city_mpg, epa_highway_mpg, range_city FROM mileages WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry' LIMIT 2;" \
    "combined_mpg"

  echo ""
  echo "Exterior Colors (color data):"
  run_test "2020 Camry colors" \
    "SELECT color, rgb FROM exterior_colors WHERE year = '2020' AND make = 'Toyota' AND model = 'Camry' LIMIT 3;" \
    "color"

  echo ""
  echo "OBD Codes:"
  run_test "Fetch codes" \
    "SELECT code, description FROM obd_codes LIMIT 5;" \
    "code"

  echo ""
  echo "Debug Mode:"
  run_test "HTTP request details" \
    "SELECT id FROM makes_debug LIMIT 1;" \
    "HTTP GET"
}

verify_pokeapi() {
  echo "=== PokeAPI ==="
  echo ""

  echo "Pokemon List (offset-based pagination):"
  run_test "Basic query" \
    "SELECT name, url FROM pokemon LIMIT 5;" \
    "name"
  run_count_test "Pagination (20+ rows fetched)" \
    "SELECT count(*) FROM (SELECT 1 FROM pokemon LIMIT 25) t;" \
    1

  echo ""
  echo "Pokemon Detail (path param):"
  run_test "Pikachu lookup" \
    "SELECT id, name, height, weight, base_experience FROM pokemon_detail WHERE name = 'pikachu';" \
    "(1 row)"

  echo ""
  echo "Types List:"
  run_test "Basic query" \
    "SELECT name, url FROM types LIMIT 5;" \
    "name"

  echo ""
  echo "Type Detail (path param):"
  run_test "Fire type lookup" \
    "SELECT id, name FROM type_detail WHERE name = 'fire';" \
    "(1 row)"

  echo ""
  echo "Berries List:"
  run_test "Basic query" \
    "SELECT name, url FROM berries LIMIT 5;" \
    "name"

  echo ""
  echo "Berry Detail (path param):"
  run_test "Cheri berry lookup" \
    "SELECT id, name, growth_time, max_harvest FROM berry_detail WHERE name = 'cheri';" \
    "(1 row)"

  echo ""
  echo "Debug Mode:"
  run_test "HTTP request details" \
    "SELECT name FROM pokemon_debug LIMIT 1;" \
    "HTTP GET"
}

verify_github() {
  echo "=== GitHub API ==="
  echo ""

  echo "My Profile (single object):"
  run_test "Fetch profile" \
    "SELECT login, id, name FROM my_profile;" \
    "(1 row)"
  run_test "Has login" \
    "SELECT login FROM my_profile;" \
    "login"

  echo ""
  echo "My Repos (pagination):"
  run_test "Basic query" \
    "SELECT id, name, language FROM my_repos LIMIT 5;" \
    "id"
  run_count_test "Has repos" \
    "SELECT count(*) FROM (SELECT 1 FROM my_repos LIMIT 5) t;" \
    1

  echo ""
  echo "Repo Detail (path params):"
  run_test "Fetch supabase/wrappers" \
    "SELECT name, stargazers_count, language FROM repo_detail WHERE owner = 'supabase' AND repo = 'wrappers';" \
    "(1 row)"

  echo ""
  echo "Repo Issues (path params + pagination):"
  run_test "Fetch issues" \
    "SELECT number, title, state FROM repo_issues WHERE owner = 'supabase' AND repo = 'wrappers' LIMIT 5;" \
    "number"

  echo ""
  echo "Repo Pulls (path params + state pushdown):"
  run_test "Fetch closed PRs" \
    "SELECT number, title, state FROM repo_pulls WHERE owner = 'supabase' AND repo = 'wrappers' AND state = 'closed' LIMIT 5;" \
    "closed"

  echo ""
  echo "Repo Releases (path params):"
  run_test "Fetch releases" \
    "SELECT tag_name, name, prerelease FROM repo_releases WHERE owner = 'supabase' AND repo = 'wrappers' LIMIT 5;" \
    "tag_name"

  echo ""
  echo "Search Repos (query pushdown):"
  run_test "Search for repos" \
    "SELECT name, full_name, stargazers_count FROM search_repos WHERE q = 'openapi foreign data wrapper' LIMIT 5;" \
    "name"

  echo ""
  echo "Debug Mode:"
  run_test "HTTP request details" \
    "SELECT id FROM search_repos_debug WHERE q = 'supabase' LIMIT 1;" \
    "HTTP GET"
}

verify_threads() {
  echo "=== Threads API ==="
  echo ""

  echo "My Profile (single object):"
  run_test "Fetch profile" \
    "SELECT id, username, name FROM my_profile;" \
    "(1 row)"
  run_test "Has username" \
    "SELECT username FROM my_profile;" \
    "username"

  echo ""
  echo "My Threads (pagination + timestamptz):"
  run_test "Basic query" \
    "SELECT id, text, media_type, timestamp FROM my_threads LIMIT 5;" \
    "id"
  run_count_test "Has posts" \
    "SELECT count(*) FROM (SELECT 1 FROM my_threads LIMIT 5) t;" \
    1

  echo ""
  echo "My Replies:"
  run_test "Basic query" \
    "SELECT id, text, timestamp FROM my_replies LIMIT 5;" \
    "id"

  echo ""
  echo "Thread Detail (path param):"
  local thread_id
  thread_id=$(psql_cmd -t -c "SELECT id FROM my_threads LIMIT 1;" 2>/dev/null | tr -d ' \n')
  if [ -n "$thread_id" ]; then
    run_test "Fetch by ID" \
      "SELECT id, text, media_type FROM thread_detail WHERE thread_id = '$thread_id';" \
      "(1 row)"
  else
    echo "  SKIP (no threads found)"
  fi

  echo ""
  echo "Thread Replies (path param + pagination):"
  if [ -n "$thread_id" ]; then
    run_test "Fetch replies" \
      "SELECT id, text, username FROM thread_replies WHERE thread_id = '$thread_id' LIMIT 5;" \
      "id"
  else
    echo "  SKIP (no threads found)"
  fi

  echo ""
  echo "Thread Conversation (all-depth replies):"
  if [ -n "$thread_id" ]; then
    run_test "Fetch conversation" \
      "SELECT id, text, username FROM thread_conversation WHERE thread_id = '$thread_id' LIMIT 5;" \
      "id"
  else
    echo "  SKIP (no threads found)"
  fi

  echo ""
  echo "Keyword Search (query param pushdown):"
  run_test "Search for 'threads'" \
    "SELECT id, text, username FROM keyword_search WHERE q = 'threads' LIMIT 5;" \
    "id"

  echo ""
  echo "Profile Lookup (query param):"
  printf "  %-40s " "Look up @threads"
  local pl_output
  pl_output=$(psql_cmd -c "SELECT username, name, is_verified FROM profile_lookup WHERE username = 'threads';" 2>&1) || true
  if echo "$pl_output" | grep -q "threads"; then
    echo "PASS"
    PASS=$((PASS + 1))
  elif echo "$pl_output" | grep -qi "error\|permission\|500"; then
    echo "SKIP (permission not available)"
  else
    echo "FAIL"
    echo "    Expected: threads"
    echo "    Output:   $(echo "$pl_output" | head -5)"
    FAIL=$((FAIL + 1))
  fi

  echo ""
  echo "Publishing Limit:"
  run_test "Fetch quota" \
    "SELECT quota_usage, config FROM publishing_limit;" \
    "quota_usage"

  echo ""
  echo "Debug Mode:"
  run_test "HTTP request details" \
    "SELECT id FROM keyword_search_debug WHERE q = 'meta' LIMIT 1;" \
    "HTTP GET"

  echo ""
  echo "IMPORT FOREIGN SCHEMA:"
  psql_cmd -c "DROP SCHEMA IF EXISTS threads_auto CASCADE;" > /dev/null 2>&1
  psql_cmd -c "CREATE SCHEMA threads_auto;" > /dev/null 2>&1
  run_test "Auto-generate tables" \
    "IMPORT FOREIGN SCHEMA \"unused\" FROM SERVER threads_import INTO threads_auto;" \
    "IMPORT FOREIGN SCHEMA"
  run_count_test "Generated tables" \
    "SELECT count(*) FROM information_schema.foreign_tables WHERE foreign_table_schema = 'threads_auto';" \
    1
  psql_cmd -c "DROP SCHEMA threads_auto CASCADE;" > /dev/null 2>&1

  echo ""
  echo "Attrs catch-all column:"
  run_test "Extra fields in attrs" \
    "SELECT id, attrs->>'media_product_type' AS product_type FROM my_threads LIMIT 3;" \
    "THREADS"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "==> Building WASM binary..."
make build
chmod +r "$WASM_BIN"

start_container

EXAMPLE_PASS=0
EXAMPLE_FAIL=0

for example in "${EXAMPLES[@]}"; do
  check_auth "$example"

  echo ""
  echo "============================================"
  echo "  Example: $example"
  echo "============================================"
  echo ""

  load_example "$example"

  echo ""
  PASS=0
  FAIL=0
  "verify_$example"

  echo ""
  echo "--------------------------------------------"
  echo "  $example: $PASS passed, $FAIL failed"
  echo "--------------------------------------------"

  if [ "$FAIL" -eq 0 ]; then
    EXAMPLE_PASS=$((EXAMPLE_PASS + 1))
  else
    EXAMPLE_FAIL=$((EXAMPLE_FAIL + 1))
  fi
done

echo ""
echo "============================================"
echo "  Examples: $((EXAMPLE_PASS + EXAMPLE_FAIL)) run, $EXAMPLE_PASS passed, $EXAMPLE_FAIL failed"
echo "============================================"

[ "$EXAMPLE_FAIL" -eq 0 ]
