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
# Templates in the access ledger are PostgreSQL query identifiers; computing them changes no plan.
SIBENCH_CAPTURE_DIR=$SIBENCH_RAW "$PG/runtime/bin/pg_ctl" -D "$PG/data" \
  -o "-c config_file=$PG/data/sibench.conf -c compute_query_id=on" -l "$PG/server.log" -w start >/dev/null
trap '"$PG/runtime/bin/pg_ctl" -D "$PG/data" -m fast stop >/dev/null' EXIT

stage() {
  local started ended
  started=$(date +%s%N)
  # HyBench resolves Data_1x relative to the working directory, so every stage runs from there.
  ( cd "$WORKDIR" && "$JAVA" $HEAP -cp "$HYBENCH_CP" org.sibench.capture.NativeCapture \
      "$1" "$ROOT" "$PROFILE" "$TIMING" )
  ended=$(date +%s%N)
  printf 'stage %-8s %d ms\n' "$1" "$(( (ended - started) / 1000000 ))" >&2
}
URL=jdbc:postgresql://127.0.0.1:$SIBENCH_PORT/$SIBENCH_DATABASE

# HyBench builds, or verifies and reuses, the still database its captures clone.
if [ -n "${SIBENCH_WORKDIR:-}" ]; then
  stage base
fi
stage prepare
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
fi
echo "native capture complete: $SIBENCH_ARTIFACTS; timing: $TIMING"
