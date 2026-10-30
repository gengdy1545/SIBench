#!/usr/bin/env bash
# Orchestrates one native capture of HyBench, YCSB or sysbench against the instrumented PG 14.24,
# through the canonical index trace.
# All semantics live in the Java CLI; this script only starts and stops the local service and
# chains the stages. It refuses to reuse a database, baseline or raw directory.
#
#   scripts/native-capture.sh config/hybench_sf1.yaml
#   scripts/native-capture.sh config/ycsb_small.yaml
#   scripts/native-capture.sh config/sysbench_small.yaml
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 PROFILE" >&2
  exit 2
fi
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROFILE=$1
export JAVA_HOME=${SIBENCH_JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
# HyBench runs in-process through reflection, so its build is added to the classpath.
HYBENCH=$ROOT/build/benchmarks/HyBench-2024
# The row audit keeps one open Parquet reader per baseline file; large tables need a larger heap.
HEAP=${SIBENCH_HEAP:--Xmx12g}
JAVA=$JAVA_HOME/bin/java
PG=$ROOT/build/services/postgresql-capture
CORE=$ROOT/build/sibench/sibench.jar

cd "$ROOT"
eval "$("$JAVA" -cp "$CORE" org.sibench.capture.NativeCapture paths "$ROOT" "$PROFILE")"
# PG and the native clients read the detail level and trace encoding from the environment.
export SIBENCH_CAPTURE_DETAIL SIBENCH_CAPTURE_COMPRESS
if [ "$SIBENCH_VERIFICATION" = certified ] && [ ! -f "$SIBENCH_CERTIFICATION" ]; then
  echo "certification not found: $SIBENCH_CERTIFICATION" >&2
  exit 1
fi
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
sha256sum "$JAR" > "$JAR.sha256"

# HyBench keeps its own classes next to the build; absent classes simply leave the path empty.
HYBENCH_CLASSES=$HYBENCH/target/classes
[ -d "$HYBENCH_CLASSES" ] || HYBENCH_CLASSES=
HYBENCH_CP="$JAR:$HYBENCH_CLASSES:$HYBENCH/lib/*"
# HyBench runs from its work directory, where its base stage generates Data_<sf> on first use.
WORKDIR=${SIBENCH_WORKDIR:-$ROOT}
mkdir -p "$WORKDIR"

if "$PG/runtime/bin/pg_isready" -q -h 127.0.0.1 -p "$SIBENCH_PORT"; then
  echo "a PostgreSQL server already listens on port $SIBENCH_PORT; stop it first" >&2
  exit 1
fi
# The still database lives in a cluster of its own, stopped between captures; each capture runs on
# a reflink copy of it (see StillBase), which shares every block the capture does not change.
STILL=$ROOT/build/services/postgresql-still/$SIBENCH_BASE
CLUSTER=$PG/data
if [ -e "$CLUSTER" ]; then
  echo "a capture cluster already exists: $CLUSTER (left by a failed capture, kept for diagnosis);" \
    "stop its server if one runs and remove the directory" >&2
  exit 1
fi
# The server parameters are versioned in config/; only the machine-specific ones are added here.
# start_cluster DATA LOG [CAPTURE_DIR]: the instrumented server traces only with a capture directory.
start_cluster() {
  SIBENCH_CAPTURE_DIR=${3:-} "$PG/runtime/bin/pg_ctl" -D "$1" \
    -o "-c config_file=$ROOT/config/postgres_capture.conf -c port=$SIBENCH_PORT -c unix_socket_directories=$1" \
    -l "$2" -w start >/dev/null
}

stage() {
  local started ended
  started=$(date +%s%N)
  # HyBench resolves Data_1x relative to the working directory, so every stage runs from there.
  if [ "$1" = export ]; then
    # The baseline is the still database's, exported and verified once and linked per capture.
    ( cd "$WORKDIR" && "$JAVA" $HEAP -cp "$HYBENCH_CP" org.sibench.capture.SharedBaseline "$ROOT" "$PROFILE" "$TIMING" )
  else
    ( cd "$WORKDIR" && "$JAVA" $HEAP -cp "$HYBENCH_CP" org.sibench.capture.NativeCapture \
        "$1" "$ROOT" "$PROFILE" "$TIMING" )
  fi
  ended=$(date +%s%N)
  printf 'stage %-8s %d ms\n' "$1" "$(( (ended - started) / 1000000 ))" >&2
}
URL=jdbc:postgresql://127.0.0.1:$SIBENCH_PORT/$SIBENCH_DATABASE

# The still database is built once: the benchmark's own load, then what every capture needs before
# it runs. No capture directory is set, so the instrumented server writes no trace while it loads.
if [ ! -f "$STILL/READY" ]; then
  if [ -e "$STILL" ]; then
    echo "incomplete still cluster: $STILL (its build failed); remove it to build it again" >&2
    exit 1
  fi
  mkdir -p "$STILL"
  "$PG/runtime/bin/initdb" -D "$STILL/data" -U postgres --locale=C -E UTF8 --auth=trust > "$STILL/initdb.log"
  start_cluster "$STILL/data" "$STILL/server.log"
  trap '"$PG/runtime/bin/pg_ctl" -D "$STILL/data" -m fast -t 3600 stop >/dev/null' EXIT
  stage base
  # A clean shutdown checkpoints, so a copy of the stopped cluster needs no recovery; flushing 16 GB
  # of shared buffers can take longer than pg_ctl's default minute.
  "$PG/runtime/bin/pg_ctl" -D "$STILL/data" -m fast -t 3600 stop >/dev/null
  trap - EXIT
  touch "$STILL/READY"
fi
cp -a --reflink=always "$STILL/data" "$CLUSTER"
# PostgreSQL writes 8 KiB pages. XFS's default CoW reservations can approach another whole
# relation's size under random writes, exhausting quota even while its visible size barely grows.
# Set only this disposable clone's allocation hint; directories pass it to new relation segments.
if [ "$(stat -f -c %T "$CLUSTER")" = xfs ]; then
  xfs_io -c 'cowextsize -R 8192' "$CLUSTER" > /dev/null
fi
start_cluster "$CLUSTER" "$SIBENCH_ARTIFACTS/postgres.log" "$SIBENCH_RAW"
trap '"$PG/runtime/bin/pg_ctl" -D "$CLUSTER" -m fast -t 3600 stop >/dev/null' EXIT
stage prepare
# A copy of the still cluster shares its blocks but not its page cache, so a native run on it starts
# cold. SIBENCH_PREWARM=1 reads its relation files sequentially first: it pays off for short,
# read-heavy runs (a sysbench 87m calibration: 284 s cold, 97 s after 123 s of reading), but a long
# write-heavy run then competes with a full page cache (sysbench 87m 5%: 754 s cold, 831 s after
# reading), and HyBench wrote as much cold as warm, so it is off by default.
if [ "${SIBENCH_PREWARM:-0}" = 1 ]; then
  started=$(date +%s%N)
  find "$CLUSTER/base" -type f -print0 | xargs -0 -P 8 -n 32 cat > /dev/null
  printf 'stage %-8s %d ms\n' prewarm "$(( ($(date +%s%N) - started) / 1000000 ))" >&2
fi
stage check > "$SIBENCH_ARTIFACTS/schema-preflight.json"
stage export > "$SIBENCH_ARTIFACTS/baseline.log"
# PG's own index counters bracket only the native run: an independent check that the ledger saw
# every index access, including ones the instrumentation might never have emitted.
counters() {
  "$JAVA" -cp "$HYBENCH_CP" org.sibench.cli.Sibench snapshot-index-counters "$PROFILE" "$1" \
    --jdbc "$URL" --user postgres > /dev/null
}
counters "$SIBENCH_ARTIFACTS/index-counters-before.json"
stage run > "$SIBENCH_ARTIFACTS/run-summary.json"
counters "$SIBENCH_ARTIFACTS/index-counters-after.json"
# A full capture audits its row changes against the baseline and PG's final state, then certifies
# its build and coverage; a certified capture relies on such a certificate instead.
if [ "$SIBENCH_VERIFICATION" = full ]; then
  "$JAVA" $HEAP -cp "$HYBENCH_CP" org.sibench.cli.Sibench audit-rows "$PROFILE" "$SIBENCH_ARTIFACTS/baseline" "$SIBENCH_RAW" \
    --jdbc "$URL" --user postgres --timing "$TIMING" > "$SIBENCH_ARTIFACTS/row-state-audit-summary.json"
  VERIFY=()
else
  VERIFY=(--certification "$SIBENCH_CERTIFICATION")
fi
# One pass compiles the trace and builds the access ledger; the trace is committed only when the
# ledger matches PG's index counters (and, when certified, stays inside the certificate).
"$JAVA" $HEAP -cp "$HYBENCH_CP" org.sibench.cli.Sibench compile-trace "$PROFILE" "$SIBENCH_ARTIFACTS/trace" \
  "${VERIFY[@]}" --counters "$SIBENCH_ARTIFACTS/index-counters-before.json" "$SIBENCH_ARTIFACTS/index-counters-after.json" \
  --timing "$TIMING" > "$SIBENCH_ARTIFACTS/trace-summary.json"
cp "$SIBENCH_ARTIFACTS/trace/access-ledger.json" "$SIBENCH_ARTIFACTS/access-ledger-summary.json"
cp "$SIBENCH_ARTIFACTS/trace/capture-completeness.json" "$SIBENCH_ARTIFACTS/capture-completeness.json"
if [ "$SIBENCH_VERIFICATION" = full ]; then
  "$JAVA" -cp "$HYBENCH_CP" org.sibench.cli.Sibench certify-capture "$PROFILE" > "$SIBENCH_ARTIFACTS/certification-summary.json"
  # The audited row transitions served the audit and the certificate; the capture is recompiled
  # with its own certificate from now on, which does not read them.
  rm -f "$SIBENCH_RAW/row-state-audit.json.changes.jsonl"
fi
# The capture cluster holds the workload's final state, which the audit has already read; the next
# capture copies the still cluster again. SIBENCH_KEEP_DATABASE=1 keeps it for diagnosis.
"$PG/runtime/bin/pg_ctl" -D "$CLUSTER" -m fast -t 3600 stop >/dev/null
trap - EXIT
if [ "${SIBENCH_KEEP_DATABASE:-0}" != 1 ]; then
  rm -rf "$CLUSTER"
fi
echo "native capture complete: $SIBENCH_ARTIFACTS; timing: $TIMING"
