#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run-tests.sh
#   ./run-tests.sh memgraphs postgres
#   ./run-tests.sh tigergraphs arangos
#   ./run-tests.sh --run-performance
#   ./run-tests.sh --run-bulk-e2e --run-nebula tigergraphs nebulas

ROOT="test/db"
LOG_DIR="${TMPDIR:-/tmp}/graflo-test-logs-$$"
mkdir -p "$LOG_DIR"

# Default suites (safe-ish parallelism: backend-level only)
suites=()
pytest_opts=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --run-bulk-e2e|--run-performance|--run-nebula|--reset)
      pytest_opts+=("$1")
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage:
  ./run-tests.sh [OPTIONS] [DB_SUITES...]

DB_SUITES:
  memgraphs postgres tigergraphs arangos falkordbs neo4js nebulas

OPTIONS:
  --run-bulk-e2e     Include tests marked bulk_e2e
  --run-performance  Include tests marked performance
  --run-nebula       Include tests marked nebula
  --reset            Enable reset fixture behavior
  -h, --help         Show this help
EOF
      exit 0
      ;;
    *)
      suites+=("$1")
      shift
      ;;
  esac
done

if [ "${#suites[@]}" -eq 0 ]; then
  suites=(memgraphs postgres tigergraphs arangos falkordbs neo4js nebulas)
fi

pids=()
names=()
logs=()

run_suite() {
  local name="$1"
  local command="$2"
  local log_file="${LOG_DIR}/${name}.log"

  echo "==> Starting ${name}"
  bash -lc "$command" >"$log_file" 2>&1 &
  pids+=("$!")
  names+=("$name")
  logs+=("$log_file")
}

for s in "${suites[@]}"; do
  path="${ROOT}/${s}"
  if [ ! -d "$path" ]; then
    echo "Skipping unknown suite: $s" >&2
    continue
  fi
  run_suite "db-${s}" "uv run pytest \"$path\" ${pytest_opts[*]}"
done

# Run all non-db tests as a separate job.
if [ -d "test" ]; then
  if uv run pytest --help 2>/dev/null | rg -q -- '^\s*-n\s'; then
    run_suite "other-tests" "uv run pytest test --ignore=test/db -n auto --dist=loadfile ${pytest_opts[*]}"
  else
    run_suite "other-tests" "uv run pytest test --ignore=test/db ${pytest_opts[*]}"
  fi
fi

fail=0
for i in "${!pids[@]}"; do
  pid="${pids[$i]}"
  name="${names[$i]}"
  log_file="${logs[$i]}"
  if wait "$pid"; then
    echo "✅ ${name} passed (log: ${log_file})"
  else
    echo "❌ ${name} failed (log: ${log_file})"
    echo "--- ${name} (last 60 lines) ---"
    tail -n 60 "$log_file"
    echo "--- end ${name} ---"
    fail=1
  fi
done

echo "Logs written to: ${LOG_DIR}"
exit "$fail"