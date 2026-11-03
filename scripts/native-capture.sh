#!/usr/bin/env bash
# Orchestrates one native capture of HyBench, YCSB or sysbench against the instrumented PG 14.24,
# through the canonical index trace.
# All semantics live in the Java CLI; this script only starts and stops the local service and
# chains the stages. It refuses to reuse a database, baseline or raw directory.
#
#   scripts/native-capture.sh config/benchmark/hybench.yaml
#   scripts/native-capture.sh config/benchmark/ycsb.yaml
#   scripts/native-capture.sh config/benchmark/sysbench.yaml
set -euo pipefail

AUDIT_BASELINE=0
if [ "${1:-}" = --audit-baseline ]; then
  AUDIT_BASELINE=1
  shift
fi
if [ "$#" -ne 1 ]; then
  echo "usage: $0 [--audit-baseline] PROFILE" >&2
  exit 2
fi
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROFILE=$1
if [ -n "${SIBENCH_JAVA_HOME:-}" ]; then
  export JAVA_HOME=$SIBENCH_JAVA_HOME
fi
# Heap budgets cover capture and Trace compilation.
HEAP=${SIBENCH_HEAP:--Xmx12g}
JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java
PG=$ROOT/build/services/postgresql-capture
CORE=$ROOT/build/sibench/sibench.jar

cd "$ROOT"
ASSIGNMENTS=$("$JAVA" -cp "$CORE" org.sibench.capture.NativeCapture paths "$ROOT" "$PROFILE")
eval "$ASSIGNMENTS"
# PG and the native clients read the detail level and trace encoding from the environment.
export SIBENCH_CAPTURE_DETAIL SIBENCH_CAPTURE_COMPRESS
export SIBENCH_BUDGET_STOP
SOCKET_DIRECTORY=
# Only HyBench needs upstream classes in the orchestrator JVM.
NATIVE_CP=
if [ -n "${SIBENCH_HYBENCH:-}" ]; then
  NATIVE_CP=:$SIBENCH_HYBENCH/target/classes:$SIBENCH_HYBENCH/lib/\*
fi
CAPTURE_CP="$CORE$NATIVE_CP"
"$JAVA" -cp "$CAPTURE_CP" org.sibench.capture.NativeBuildCheck verify "$ROOT" "$PROFILE"

if [ -e "$SIBENCH_ARTIFACTS" ]; then
  echo "artifacts already exist: $SIBENCH_ARTIFACTS (move or archive them first)" >&2
  exit 1
fi
mkdir -p "$SIBENCH_RAW" "$SIBENCH_ARTIFACTS/native"
# Every stage appends its phase timings here, so one run leaves one comparable artifact.
TIMING=$SIBENCH_ARTIFACTS/run-timing.json
# The running JAR is an immutable copy; rebuilding sibench.jar never changes a run in flight.
JAR=$SIBENCH_ARTIFACTS/native/sibench-run.jar
cp "$CORE" "$JAR"
CAPTURE_CP="$JAR$NATIVE_CP"
sha256sum "$JAR" > "$JAR.sha256"
cp "$PROFILE" "$SIBENCH_ARTIFACTS/native/benchmark.yaml"
PROFILE=$SIBENCH_ARTIFACTS/native/benchmark.yaml
cp "$ROOT/config/sibench.yaml" "$SIBENCH_ARTIFACTS/native/sibench.yaml"
PG_CONFIG=$SIBENCH_ARTIFACTS/native/postgres.conf
"$JAVA" -cp "$JAR" org.sibench.cli.Sibench postgres-config \
  "$SIBENCH_ARTIFACTS/native/sibench.yaml" "$PG_CONFIG" > /dev/null

# HyBench generates Data_<sf> in this run's work directory.
WORKDIR=${SIBENCH_WORKDIR:-$ROOT}
mkdir -p "$WORKDIR"

if "$PG/runtime/bin/pg_isready" -q -h 127.0.0.1 -p "$SIBENCH_PORT"; then
  echo "a PostgreSQL server already listens on port $SIBENCH_PORT; stop it first" >&2
  exit 1
fi
CLUSTER=$PG/data
if [ -e "$CLUSTER" ]; then
  echo "a capture cluster already exists: $CLUSTER (left by a failed capture, kept for diagnosis);" \
    "stop its server if one runs and remove the directory" >&2
  exit 1
fi
# Server parameters come from the effective snapshot; the load WAL cap derives from its minimum.
# start_cluster DATA LOG [CAPTURE_DIR]: the instrumented server traces only with a capture directory.
start_cluster() {
  # pg_ctl passes -o through /bin/sh; preserve literal quotes in the config path there too.
  local quoted_config=${PG_CONFIG//\'/\'\\\'\'}
  local load_wal_options=
  if [ -z "${3:-}" ]; then
    # Loading needs no extra WAL reserve for later workloads.
    local load_wal
    load_wal=$("$PG/runtime/bin/postgres" -D "$1" -C min_wal_size -c "config_file=$PG_CONFIG")
    load_wal_options=" -c max_wal_size=$load_wal"
    printf 'load WAL cap: max_wal_size=%s (from min_wal_size)\n' "$load_wal" >&2
  fi
  SIBENCH_CAPTURE_DIR=${3:-} "$PG/runtime/bin/pg_ctl" -D "$1" \
    -o "-c config_file='$quoted_config' -c port=$SIBENCH_PORT -c unix_socket_directories=$load_wal_options" \
    -l "$2" -w start >/dev/null
}

# Preserve the first failure even if stopping the disposable server also fails.
cleanup() {
  local status=$?
  trap - EXIT
  if "$PG/runtime/bin/pg_ctl" -D "$CLUSTER" status >/dev/null 2>&1; then
    if ! "$PG/runtime/bin/pg_ctl" -D "$CLUSTER" -m fast -t 3600 stop >/dev/null; then
      echo "failed to stop disposable PostgreSQL; retained $CLUSTER" >&2
      if [ "$status" -eq 0 ]; then status=1; fi
    fi
  fi
  if [ -n "$SOCKET_DIRECTORY" ]; then
    if [ -S "$SIBENCH_CAPTURE_SOCKET" ] && ! rm -f "$SIBENCH_CAPTURE_SOCKET"; then
      echo "failed to remove capture socket: $SIBENCH_CAPTURE_SOCKET" >&2
      if [ "$status" -eq 0 ]; then status=1; fi
    fi
    if ! rmdir "$SOCKET_DIRECTORY"; then
      echo "retained capture socket directory: $SOCKET_DIRECTORY" >&2
      if [ "$status" -eq 0 ]; then status=1; fi
    fi
  fi
  exit "$status"
}

stage() {
  local started ended
  started=$(date +%s%N)
  # HyBench resolves Data_1x relative to the working directory, so every stage runs from there.
  ( cd "$WORKDIR" && "$JAVA" $HEAP -cp "$CAPTURE_CP" org.sibench.capture.NativeCapture \
      "$1" "$ROOT" "$PROFILE" "$TIMING" )
  ended=$(date +%s%N)
  printf 'stage %-8s %d ms\n' "$1" "$(( (ended - started) / 1000000 ))" >&2
}
URL=jdbc:postgresql://127.0.0.1:$SIBENCH_PORT/$SIBENCH_DATABASE

# Load once into this run's database. No PG state is reused by another capture.
"$PG/runtime/bin/initdb" -D "$CLUSTER" -U postgres --locale=C -E UTF8 --auth=trust > "$SIBENCH_ARTIFACTS/native/initdb.log"
trap cleanup EXIT
# Unix socket paths must stay short even when artifact paths are long.
SOCKET_DIRECTORY=$(mktemp -d /tmp/sibench-capture.XXXXXXXX)
export SIBENCH_CAPTURE_SOCKET="$SOCKET_DIRECTORY/capture.sock"
start_cluster "$CLUSTER" "$SIBENCH_ARTIFACTS/postgres.log"
stage load
# The capture directory is inherited by PG at startup. Restart this same cluster before export,
# so baseline reads warm the very database and shared buffers used by the native workload.
"$PG/runtime/bin/pg_ctl" -D "$CLUSTER" -m fast -t 3600 stop >/dev/null
start_cluster "$CLUSTER" "$SIBENCH_ARTIFACTS/postgres.log" "$SIBENCH_RAW"
# Optional sequential prewarm is measured separately and defaults to off.
if [ "${SIBENCH_PREWARM:-0}" = 1 ]; then
  started=$(date +%s%N)
  find "$CLUSTER/base" -type f -print0 | xargs -0 -P 8 -n 32 cat > /dev/null
  printf 'stage %-8s %d ms\n' prewarm "$(( ($(date +%s%N) - started) / 1000000 ))" >&2
fi
stage export > "$SIBENCH_ARTIFACTS/baseline.log"
# Independent initial-state acceptance must run before the native workload changes this database.
if [ "$AUDIT_BASELINE" = 1 ]; then
  ACCEPTANCE=$SIBENCH_ARTIFACTS/acceptance
  mkdir -p "$ACCEPTANCE"
  AUDIT_COMMAND=("$JAVA" $HEAP -cp "$CAPTURE_CP" org.sibench.cli.Sibench audit-baseline
    "$PROFILE" "$SIBENCH_ARTIFACTS/baseline" --jdbc "$URL" --user postgres)
  printf '%q ' "${AUDIT_COMMAND[@]}" > "$ACCEPTANCE/baseline-audit-command.sh"
  printf '\n' >> "$ACCEPTANCE/baseline-audit-command.sh"
  started=$(date +%s%N)
  if "${AUDIT_COMMAND[@]}" > "$ACCEPTANCE/baseline-audit.json"; then
    audit_status=0
  else
    audit_status=$?
  fi
  ended=$(date +%s%N)
  printf '{"started_epoch_ns":%s,"ended_epoch_ns":%s,"wall_millis":%d,"exit_code":%d}\n' \
    "$started" "$ended" "$(( (ended - started) / 1000000 ))" "$audit_status" \
    > "$ACCEPTANCE/baseline-audit-timing.json"
  if [ "$audit_status" -ne 0 ]; then exit "$audit_status"; fi
fi
# PG's own index counters bracket only the native run: an independent check that the ledger saw
# every index access, including ones the instrumentation might never have emitted.
counters() {
  "$JAVA" -cp "$CAPTURE_CP" org.sibench.cli.Sibench snapshot-index-counters "$PROFILE" "$1" \
    --jdbc "$URL" --user postgres > /dev/null
}
counters "$SIBENCH_ARTIFACTS/index-counters-before.json"
stage run > "$SIBENCH_ARTIFACTS/native-run.log"
# The run stage checks source completion/counters and publishes the final Trace directly.
# Retain the database until that publication succeeds; replay is a separate command.
"$PG/runtime/bin/pg_ctl" -D "$CLUSTER" -m fast -t 3600 stop >/dev/null
cp "$SIBENCH_ARTIFACTS/trace/access-ledger.json" "$SIBENCH_ARTIFACTS/access-ledger-summary.json"
cp "$SIBENCH_ARTIFACTS/trace/capture-completeness.json" "$SIBENCH_ARTIFACTS/capture-completeness.json"
if [ "$SIBENCH_TRACE_OUTPUT" = proto ]; then
  # Row data remains available for physical lookup and future rewrites.
  rm -rf "$SIBENCH_RAW"
fi

# The next capture loads fresh data. Explicit acceptance can retain the final database for audit.
if [ "${SIBENCH_KEEP_DATABASE:-0}" != 1 ]; then
  rm -rf "$CLUSTER"
fi
echo "native capture complete: $SIBENCH_ARTIFACTS; timing: $TIMING"
