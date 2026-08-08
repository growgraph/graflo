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

# Every job is fanned out into the background with its output redirected, so a
# wedged suite used to be indistinguishable from a slow one: no terminal output,
# and `wait` below blocks forever. Bound each job instead.
#
# SIGINT rather than SIGTERM: pytest traps it and still prints the results it
# collected, which is what tells you *which* test hung. --kill-after covers the
# case where pytest itself is the thing that is stuck.
SUITE_TIMEOUT="${SUITE_TIMEOUT:-1800}"
TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_BIN="timeout --signal=INT --kill-after=60 ${SUITE_TIMEOUT}"
else
  echo "warning: GNU timeout not found; suites will run unbounded" >&2
fi

# Default suites (safe-ish parallelism: backend-level only)
suites=()
pytest_opts=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --run-bulk-e2e|--run-performance|--run-nebula|--run-kafka|--run-tigergraph|--reset)
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
  --run-kafka        Include tests marked kafka
  --run-tigergraph   Include live TigerGraph tests (adds ~4 min: GSQL schema DDL
                     costs 15-40s per graph, and every other backend runs in
                     seconds)
  --reset            Enable reset fixture behavior
  -h, --help         Show this help

ENVIRONMENT:
  SUITE_TIMEOUT      Seconds before a suite is interrupted (default 1800)
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

  # Print the log path up front, not just at the end of the run: a suite that
  # stalls is only diagnosable if you can tail it while it is still stuck.
  echo "==> Starting ${name} (log: ${log_file})"
  bash -lc "${TIMEOUT_BIN} $command" >"$log_file" 2>&1 &
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

reap() {
  for i in "${!pids[@]}"; do
    pid="${pids[$i]}"
    name="${names[$i]}"
    log_file="${logs[$i]}"
    status=0
    wait "$pid" || status=$?
    if [ "$status" -eq 0 ]; then
      echo "✅ ${name} passed (log: ${log_file})"
    else
      # 124 is GNU timeout's "the command was still running". Worth calling out
      # separately: a timeout means a hang to investigate, not a failed assertion.
      if [ "$status" -eq 124 ]; then
        echo "⏱️  ${name} TIMED OUT after ${SUITE_TIMEOUT}s (log: ${log_file})"
        echo "    pytest was interrupted; the tail below shows where it was."
        echo "    Raise the budget with SUITE_TIMEOUT=<seconds> if this is expected."
      else
        echo "❌ ${name} failed with exit ${status} (log: ${log_file})"
      fi
      echo "--- ${name} (last 60 lines) ---"
      tail -n 60 "$log_file"
      echo "--- end ${name} ---"
      fail=1
    fi
  done
  pids=()
  names=()
  logs=()
}

reap

# Cross-backend suites live at the root of test/db rather than in a per-backend
# directory, so the loop above never reaches them and `--ignore=test/db` excludes
# them too. Without this job they run only when invoked by hand.
#
# They run *after* the per-backend jobs, not alongside them, because by
# definition they touch every backend at once. Parallelism here is only safe
# where each suite owns a namespace, and Neo4j and Memgraph have none to own:
# a cross-backend `recreate_schema=True` issues a database-wide wipe that
# deletes whatever the per-backend suite just ingested. The two jobs were
# racing for one database, and the loser reported a data bug.
cross_backend_files=$(printf '%s ' "${ROOT}"/test_*.py)
if [ -n "${cross_backend_files// /}" ]; then
  run_suite "db-cross-backend" "uv run pytest ${cross_backend_files} ${pytest_opts[*]}"
  reap
fi

echo "Logs written to: ${LOG_DIR}"
exit "$fail"