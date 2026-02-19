#!/usr/bin/env bash
# Benchmark: openapi-fdw vs pg_http vs pg_net
# Compares query performance for the same MockServer endpoints.
#
# Usage:
#   bash test/run.sh          # start containers + run tests first
#   bash test/benchmark.sh    # default 10 iterations
#   bash test/benchmark.sh 50 # custom iteration count
set -euo pipefail

cd "$(dirname "$0")/.."

# ---- Configuration ----
ITERATIONS="${1:-10}"
MOCKSERVER_URL="http://mockserver:1080"
COMPOSE_FILE="test/docker-compose.yml"
PGNET_POLL_INTERVAL=0.01  # seconds between pg_net polls
PGNET_MAX_WAIT=5000       # max milliseconds to wait for pg_net response
PGNET_AVAILABLE=false

# ---- Helpers ----

# All psql calls set search_path first via separate -c flag
psql_cmd() {
  docker compose -f "$COMPOSE_FILE" exec -T -e PGPASSWORD="${POSTGRES_PASSWORD:-postgres}" db \
    psql -U postgres -P pager=off \
    -c "SET search_path TO public, extensions, net;" \
    "$@" 2>&1 | sed '/^SET$/d'
}

# Portable nanosecond timestamp (macOS date doesn't support %N)
now_ns() {
  if date +%s%N 2>/dev/null | grep -qv N; then
    date +%s%N
  else
    python3 -c "import time; print(int(time.time()*1e9))"
  fi
}

# Compute min/avg/max from a list of numbers
compute_stats() {
  echo "$@" | tr ' ' '\n' | awk '
    NR==1 { min=$1; max=$1; sum=0 }
    { sum+=$1; if($1<min) min=$1; if($1>max) max=$1 }
    END { printf "%.1f %.1f %.1f\n", min, sum/NR, max }
  '
}

print_row() {
  printf "  %-18s | %8s | %8s | %8s | %4s\n" "$1" "$2" "$3" "$4" "$5"
}

print_separator() {
  printf "  %-18s-+-%8s-+-%8s-+-%8s-+-%4s\n" \
    "------------------" "--------" "--------" "--------" "----"
}

# Benchmark a synchronous query by inserting results into a temp table.
# This measures the full lifecycle: API call -> parse -> write to local table.
bench_sync() {
  local sql="$1"
  local iterations="$2"
  local table="${3:-_bench_sink}"
  local times=()

  # Create the destination table from the query schema (once)
  psql_cmd -c "DROP TABLE IF EXISTS $table;" > /dev/null
  psql_cmd -c "CREATE TABLE $table AS $sql LIMIT 0;" > /dev/null

  for _ in $(seq 1 "$iterations"); do
    psql_cmd -c "TRUNCATE $table;" > /dev/null
    local elapsed
    elapsed=$(psql_cmd -c "
      DO \$bench\$
      DECLARE
        t0 timestamptz := clock_timestamp();
        t1 timestamptz;
      BEGIN
        INSERT INTO $table $sql;
        t1 := clock_timestamp();
        RAISE NOTICE '%', extract(epoch from (t1 - t0)) * 1000;
      END \$bench\$;
    " | grep 'NOTICE' | head -1 | sed 's/.*NOTICE: *//')
    if [ -n "$elapsed" ]; then
      times+=("$elapsed")
    fi
  done

  psql_cmd -c "DROP TABLE IF EXISTS $table;" > /dev/null

  if [ ${#times[@]} -gt 0 ]; then
    compute_stats "${times[@]}"
  else
    echo "ERR ERR ERR"
  fi
}

# Benchmark pg_net (async: fire request, poll, extract)
bench_pgnet() {
  local fire_sql="$1"
  local extract_sql_template="$2"  # contains REQID placeholder
  local iterations="$3"
  local times=()

  for _ in $(seq 1 "$iterations"); do
    local start_ns
    start_ns=$(now_ns)

    # Fire request (auto-commits when psql returns)
    local req_id
    req_id=$(psql_cmd -t -c "$fire_sql" | tr -d ' \n')

    # Poll until response arrives
    local waited=0
    local ready="f"
    while [ "$ready" != "t" ] && [ "$waited" -lt "$PGNET_MAX_WAIT" ]; do
      sleep "$PGNET_POLL_INTERVAL"
      ready=$(psql_cmd -t -c \
        "SELECT EXISTS(SELECT 1 FROM net._http_response WHERE id = $req_id);" \
        | tr -d ' \n')
      waited=$((waited + 10))
    done

    # Extract data
    local extract_sql="${extract_sql_template//REQID/$req_id}"
    psql_cmd -t -c "$extract_sql" > /dev/null || true

    local end_ns
    end_ns=$(now_ns)
    local elapsed_ms
    elapsed_ms=$(echo "scale=1; ($end_ns - $start_ns) / 1000000" | bc)
    times+=("$elapsed_ms")

    # Clean up response row
    psql_cmd -c "DELETE FROM net._http_response WHERE id = $req_id;" > /dev/null || true
  done

  if [ ${#times[@]} -gt 0 ]; then
    compute_stats "${times[@]}"
  else
    echo "ERR ERR ERR"
  fi
}

# Get row count for a query
row_count() {
  psql_cmd -t -c "SELECT count(*) FROM ($1) AS _v;" | tr -d ' \n'
}

# ---- Pre-flight Checks ----

echo "============================================================================="
echo "  OpenAPI FDW Benchmark"
echo "============================================================================="
echo ""

# Check containers
if ! docker compose -f "$COMPOSE_FILE" ps 2>/dev/null | grep -q "db.*Up"; then
  echo "ERROR: Containers not running. Start them first:"
  echo "  bash test/run.sh"
  exit 1
fi

# Verify FDW is working
echo "Checking openapi-fdw..."
if ! psql_cmd -c "SELECT count(*) FROM mock_items;" > /dev/null; then
  echo "ERROR: openapi-fdw not working. Run test/run.sh first."
  exit 1
fi
echo "  openapi-fdw: OK"

# ---- Extension Setup ----

echo "Setting up extensions..."

if psql_cmd -c "CREATE EXTENSION IF NOT EXISTS http WITH SCHEMA extensions;" > /dev/null || \
   psql_cmd -c "CREATE EXTENSION IF NOT EXISTS http;" > /dev/null; then
  echo "  http: OK"
else
  echo "ERROR: Could not create http extension."
  exit 1
fi

if psql_cmd -c "CREATE EXTENSION IF NOT EXISTS pg_net WITH SCHEMA extensions;" > /dev/null || \
   psql_cmd -c "CREATE EXTENSION IF NOT EXISTS pg_net;" > /dev/null; then
  PGNET_AVAILABLE=true
  echo "  pg_net: OK"
else
  echo "  pg_net: UNAVAILABLE (skipping)"
fi

# ---- Warmup ----

echo ""
echo "Warming up..."
psql_cmd -c "SELECT * FROM mock_items;" > /dev/null
psql_cmd -c "SELECT * FROM extensions.http_get('$MOCKSERVER_URL/items');" > /dev/null
if $PGNET_AVAILABLE; then
  psql_cmd -c "SELECT net.http_get('$MOCKSERVER_URL/items');" > /dev/null
  sleep 1  # let bg worker process
fi

# ---- Benchmark ----

echo ""
echo "============================================================================="
echo "  openapi-fdw vs pg_http vs pg_net"
echo "  Iterations: $ITERATIONS | MockServer: $MOCKSERVER_URL"
echo "============================================================================="

# ============================
# Scenario 1: Simple Array
# ============================
echo ""
echo "Scenario 1: Simple Array (GET /items -> 3 rows, extract id + name)"
echo "-----------------------------------------------------------------------------"
print_row "Approach" "Min (ms)" "Avg (ms)" "Max (ms)" "Rows"
print_separator

FDW_SQL="SELECT id, name FROM mock_items"
ROWS=$(row_count "$FDW_SQL")
STATS=$(bench_sync "$FDW_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "openapi-fdw" $STATS "$ROWS"

HTTP_SQL="SELECT (elem->>'id')::bigint AS id, elem->>'name' AS name
  FROM (SELECT content FROM extensions.http_get('$MOCKSERVER_URL/items')) r,
       jsonb_array_elements(r.content::jsonb) AS elem"
STATS=$(bench_sync "$HTTP_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "pg_http" $STATS "$ROWS"

if $PGNET_AVAILABLE; then
  FIRE="SELECT net.http_get('$MOCKSERVER_URL/items')"
  EXTRACT="SELECT (elem->>'id')::bigint, elem->>'name'
    FROM net._http_response r,
         jsonb_array_elements(r.content::jsonb) AS elem
    WHERE r.id = REQID"
  STATS=$(bench_pgnet "$FIRE" "$EXTRACT" "$ITERATIONS")
  # shellcheck disable=SC2086
  print_row "pg_net (async)" $STATS "$ROWS *"
fi

# ============================
# Scenario 2: Wrapped Response
# ============================
echo ""
echo "Scenario 2: Wrapped Response (GET /products -> unwrap 'data' -> 2 rows)"
echo "-----------------------------------------------------------------------------"
print_row "Approach" "Min (ms)" "Avg (ms)" "Max (ms)" "Rows"
print_separator

FDW_SQL="SELECT id, name, price FROM mock_products"
ROWS=$(row_count "$FDW_SQL")
STATS=$(bench_sync "$FDW_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "openapi-fdw" $STATS "$ROWS"

HTTP_SQL="SELECT elem->>'id' AS id, elem->>'name' AS name,
         (elem->>'price')::double precision AS price
  FROM (SELECT content FROM extensions.http_get('$MOCKSERVER_URL/products')) r,
       jsonb_array_elements((r.content::jsonb)->'data') AS elem"
STATS=$(bench_sync "$HTTP_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "pg_http" $STATS "$ROWS"

if $PGNET_AVAILABLE; then
  FIRE="SELECT net.http_get('$MOCKSERVER_URL/products')"
  EXTRACT="SELECT elem->>'id', elem->>'name',
           (elem->>'price')::double precision
    FROM net._http_response r,
         jsonb_array_elements((r.content::jsonb)->'data') AS elem
    WHERE r.id = REQID"
  STATS=$(bench_pgnet "$FIRE" "$EXTRACT" "$ITERATIONS")
  # shellcheck disable=SC2086
  print_row "pg_net (async)" $STATS "$ROWS *"
fi

# ============================
# Scenario 3: Type Coercion
# ============================
echo ""
echo "Scenario 3: Type Coercion (GET /typed-data -> 1 row, mixed PG types)"
echo "-----------------------------------------------------------------------------"
print_row "Approach" "Min (ms)" "Avg (ms)" "Max (ms)" "Rows"
print_separator

FDW_SQL="SELECT id, name, score, count, active, created_date, updated_at FROM mock_typed_data"
ROWS=$(row_count "$FDW_SQL")
STATS=$(bench_sync "$FDW_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "openapi-fdw" $STATS "$ROWS"

HTTP_SQL="SELECT (elem->>'id')::bigint AS id, elem->>'name' AS name,
         (elem->>'score')::real AS score, (elem->>'count')::integer AS count,
         (elem->>'active')::boolean AS active,
         (elem->>'created_date')::date AS created_date,
         (elem->>'updated_at')::timestamptz AS updated_at
  FROM (SELECT content FROM extensions.http_get('$MOCKSERVER_URL/typed-data')) r,
       jsonb_array_elements(r.content::jsonb) AS elem"
STATS=$(bench_sync "$HTTP_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "pg_http" $STATS "$ROWS"

if $PGNET_AVAILABLE; then
  FIRE="SELECT net.http_get('$MOCKSERVER_URL/typed-data')"
  EXTRACT="SELECT (elem->>'id')::bigint, elem->>'name',
           (elem->>'score')::real, (elem->>'count')::integer,
           (elem->>'active')::boolean, (elem->>'created_date')::date,
           (elem->>'updated_at')::timestamptz
    FROM net._http_response r,
         jsonb_array_elements(r.content::jsonb) AS elem
    WHERE r.id = REQID"
  STATS=$(bench_pgnet "$FIRE" "$EXTRACT" "$ITERATIONS")
  # shellcheck disable=SC2086
  print_row "pg_net (async)" $STATS "$ROWS *"
fi

# ============================
# Scenario 4: GeoJSON Nested
# ============================
echo ""
echo "Scenario 4: GeoJSON Nested (GET /features -> features[].properties -> 3 rows)"
echo "-----------------------------------------------------------------------------"
print_row "Approach" "Min (ms)" "Avg (ms)" "Max (ms)" "Rows"
print_separator

FDW_SQL="SELECT station_id, name, state, elevation FROM mock_stations"
ROWS=$(row_count "$FDW_SQL")
STATS=$(bench_sync "$FDW_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "openapi-fdw" $STATS "$ROWS"

HTTP_SQL="SELECT props->>'station_id' AS station_id, props->>'name' AS name,
         props->>'state' AS state, (props->>'elevation')::bigint AS elevation
  FROM (SELECT content FROM extensions.http_get('$MOCKSERVER_URL/features')) r,
       jsonb_array_elements((r.content::jsonb)->'features') AS feature,
       LATERAL (SELECT feature->'properties' AS props) AS sub"
STATS=$(bench_sync "$HTTP_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "pg_http" $STATS "$ROWS"

if $PGNET_AVAILABLE; then
  FIRE="SELECT net.http_get('$MOCKSERVER_URL/features')"
  EXTRACT="SELECT props->>'station_id', props->>'name',
           props->>'state', (props->>'elevation')::bigint
    FROM net._http_response r,
         jsonb_array_elements((r.content::jsonb)->'features') AS feature,
         LATERAL (SELECT feature->'properties' AS props) AS sub
    WHERE r.id = REQID"
  STATS=$(bench_pgnet "$FIRE" "$EXTRACT" "$ITERATIONS")
  # shellcheck disable=SC2086
  print_row "pg_net (async)" $STATS "$ROWS *"
fi

# ============================
# Scenario 5: POST-for-Read
# ============================
echo ""
echo "Scenario 5: POST-for-Read (POST /search -> 1 row)"
echo "-----------------------------------------------------------------------------"
print_row "Approach" "Min (ms)" "Avg (ms)" "Max (ms)" "Rows"
print_separator

FDW_SQL="SELECT id, label FROM mock_search_post"
ROWS=$(row_count "$FDW_SQL")
STATS=$(bench_sync "$FDW_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "openapi-fdw" $STATS "$ROWS"

HTTP_SQL="SELECT (elem->>'id')::bigint AS id, elem->>'label' AS label
  FROM (SELECT content FROM extensions.http_post('$MOCKSERVER_URL/search', '{}', 'application/json')) r,
       jsonb_array_elements(r.content::jsonb) AS elem"
STATS=$(bench_sync "$HTTP_SQL" "$ITERATIONS")
# shellcheck disable=SC2086
print_row "pg_http" $STATS "$ROWS"

if $PGNET_AVAILABLE; then
  FIRE="SELECT net.http_post('$MOCKSERVER_URL/search', '{}'::jsonb)"
  EXTRACT="SELECT (elem->>'id')::bigint, elem->>'label'
    FROM net._http_response r,
         jsonb_array_elements(r.content::jsonb) AS elem
    WHERE r.id = REQID"
  STATS=$(bench_pgnet "$FIRE" "$EXTRACT" "$ITERATIONS")
  # shellcheck disable=SC2086
  print_row "pg_net (async)" $STATS "$ROWS *"
fi

# ---- Ergonomics Comparison ----

echo ""
echo "============================================================================="
echo "  SQL Ergonomics Comparison (Scenario 4: GeoJSON Nested)"
echo "============================================================================="
echo ""
echo "  openapi-fdw (1 line):"
echo "    SELECT station_id, name, state, elevation FROM mock_stations;"
echo ""
echo "  pg_http (6 lines):"
echo "    SELECT props->>'station_id', props->>'name',"
echo "           props->>'state', (props->>'elevation')::bigint"
echo "    FROM (SELECT content FROM extensions.http_get('.../features')) r,"
echo "         jsonb_array_elements((r.content::jsonb)->'features') AS feature,"
echo "         LATERAL (SELECT feature->'properties' AS props) AS sub;"
echo ""

# ---- Notes ----

echo "============================================================================="
echo "  NOTES"
echo "  * pg_net timing includes async dispatch + polling overhead"
echo "  - openapi-fdw: automatic JSON unwrapping, type coercion, column mapping"
echo "  - pg_http: manual jsonb extraction and type casting required"
echo "  - pg_net: async model — response requires polling net._http_response"
echo "  - All approaches hit same MockServer (near-zero network latency)"
echo "============================================================================="
