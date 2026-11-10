#!/usr/bin/env bash
# Build the pinned client in a fresh copy; native-build.json binds the executable inputs.
set -euo pipefail
[ "$#" -eq 1 ] || { echo "usage: $0 config/benchmark/{hybench,ycsb,sysbench}.yaml" >&2; exit 2; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java
CORE=$ROOT/build/sibench/sibench.jar
PROFILE=$1
cd "$ROOT"
ASSIGNMENTS=$("$JAVA" -cp "$CORE" org.sibench.capture.NativeBuildCheck paths "$ROOT" "$PROFILE")
eval "$ASSIGNMENTS"
[ ! -e "$BUILD" ] || { echo "build already exists: $BUILD; use a fresh build directory" >&2; exit 1; }
mkdir -p "$(dirname "$BUILD")"
# Keep source Git identity: upstream version generators must not read the enclosing SIBench repo.
git clone --shared "$SOURCE" "$BUILD"
patch --batch --forward -d "$BUILD" -p1 < "$NATIVE_STOP_PATCH"
case "$BENCHMARK" in
  hybench)
    # Use the POM dependencies and newly compiled classes, not the upstream bundled application JAR.
    rm -f "$BUILD"/lib/*.jar
    mvn -f "$BUILD/pom.xml" -DskipTests compile dependency:copy-dependencies -DoutputDirectory="$BUILD/lib"
    ;;
  ycsb)
    mvn -f "$BUILD/pom.xml" -pl core -am -Psource-run -DskipTests package
    mkdir -p "$BUILD/sibench-api/lib" "$BUILD/sibench-api/classes"
    cp "$BUILD"/core/target/core-*.jar "$BUILD"/core/target/dependency/*.jar "$BUILD/sibench-api/lib/"
    "${JAVA_HOME:+$JAVA_HOME/bin/}javac" --release 17 -cp "$CORE:$BUILD/sibench-api/lib/*" \
      -d "$BUILD/sibench-api/classes" "$ROOT/src/main/native/ycsb/YcsbBinding.java"
    "${JAVA_HOME:+$JAVA_HOME/bin/}jar" --create --file "$BUILD/sibench-api/lib/sibench-ycsb.jar" \
      -C "$BUILD/sibench-api/classes" .
    ;;
  sysbench)
    mkdir -p "$BUILD/src/drivers/duckdb"
    cp "$ROOT/src/main/native/sysbench/drv_duckdb.cc" "$BUILD/src/drivers/duckdb/"
    cp "$ROOT/src/main/native/sysbench/retryable_error.hpp" "$BUILD/src/drivers/duckdb/"
    (cd "$BUILD" && ./autogen.sh && \
      CPPFLAGS="${CPPFLAGS:-} -I$ROOT/third_party/duckdb/src/include" \
      CXXFLAGS="${CXXFLAGS:--O2 -g} -std=c++17" \
      ./configure --with-pgsql --without-mysql && make -j"${JOBS:-2}")
    ;;
esac
"$JAVA" -cp "$CORE" org.sibench.capture.NativeBuildCheck record "$ROOT" "$PROFILE"
