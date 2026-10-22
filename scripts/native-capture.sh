#!/usr/bin/env bash
# Orchestrates one native capture of YCSB or sysbench against the instrumented PG 14.24, through
# the canonical index trace.
# All semantics live in the Java CLI; this script only starts and stops the local service and
# chains the stages. It refuses to reuse a database, baseline or raw directory.
#
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
# The row audit keeps one open Parquet reader per baseline file; large tables need a larger heap.
HEAP=${SIBENCH_HEAP:--Xmx12g}
JAVA=$JAVA_HOME/bin/java
PG=$ROOT/build/services/postgresql-capture
CORE=$ROOT/build/sibench/sibench.jar

cd "$ROOT"
eval "$("$JAVA" -cp "$CORE" org.sibench.capture.NativeCapture paths "$ROOT" "$PROFILE")"
if [ -e "$SIBENCH_ARTIFACTS" ]; then
  echo "artifacts already exist: $SIBENCH_ARTIFACTS (move or archive them first)" >&2
  exit 1
fi
mkdir -p "$SIBENCH_RAW" "$SIBENCH_ARTIFACTS/native"
# The running JAR is an immutable copy; rebuilding sibench.jar never changes a run in flight.
JAR=$SIBENCH_ARTIFACTS/native/sibench-run.jar
cp "$CORE" "$JAR"
sha256sum "$JAR" > "$JAR.sha256"

if "$PG/runtime/bin/pg_isready" -q -h 127.0.0.1 -p "$SIBENCH_PORT"; then
  echo "a PostgreSQL server already listens on port $SIBENCH_PORT; stop it first" >&2
  exit 1
fi
# Templates in the access ledger are PostgreSQL query identifiers; computing them changes no plan.
SIBENCH_CAPTURE_DIR=$SIBENCH_RAW "$PG/runtime/bin/pg_ctl" -D "$PG/data" \
  -o "-c config_file=$PG/data/sibench.conf -c compute_query_id=on" -l "$PG/server.log" -w start >/dev/null
trap '"$PG/runtime/bin/pg_ctl" -D "$PG/data" -m fast stop >/dev/null' EXIT

stage() { "$JAVA" $HEAP -cp "$JAR" org.sibench.capture.NativeCapture "$1" "$ROOT" "$PROFILE"; }
URL=jdbc:postgresql://127.0.0.1:$SIBENCH_PORT/$SIBENCH_DATABASE

stage prepare
stage check > "$SIBENCH_ARTIFACTS/schema-preflight.json"
stage export > "$SIBENCH_ARTIFACTS/baseline.log"
stage run > "$SIBENCH_ARTIFACTS/run-summary.json"
"$JAVA" $HEAP -jar "$JAR" audit-rows "$PROFILE" "$SIBENCH_ARTIFACTS/baseline" "$SIBENCH_RAW" \
  --jdbc "$URL" --user postgres > "$SIBENCH_ARTIFACTS/row-state-audit-summary.json"
"$JAVA" $HEAP -jar "$JAR" access-ledger "$PROFILE" "$SIBENCH_RAW" > "$SIBENCH_ARTIFACTS/access-ledger-summary.json"
"$JAVA" $HEAP -jar "$JAR" compile-trace "$PROFILE" "$SIBENCH_ARTIFACTS/trace" > "$SIBENCH_ARTIFACTS/trace-summary.json"
echo "native capture complete: $SIBENCH_ARTIFACTS"
