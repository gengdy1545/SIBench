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
case "$BENCHMARK" in
  hybench)
    patch --batch --forward -d "$BUILD" -p1 < "$ROOT/scripts/patches/hybench-1.0-snapshot.patch"
    # Use the POM dependencies and newly compiled classes, not the upstream bundled application JAR.
    rm -f "$BUILD"/lib/*.jar
    mvn -f "$BUILD/pom.xml" -DskipTests compile dependency:copy-dependencies -DoutputDirectory="$BUILD/lib"
    ;;
  ycsb)
    mvn -f "$BUILD/pom.xml" -pl jdbc -am -DskipTests package
    tar -xzf "$BUILD"/jdbc/target/ycsb-jdbc-binding-*.tar.gz -C "$BUILD/jdbc/target"
    ;;
  sysbench)
    (cd "$BUILD" && ./autogen.sh && ./configure --with-pgsql --without-mysql && make -j"${JOBS:-2}")
    ;;
esac
"$JAVA" -cp "$CORE" org.sibench.capture.NativeBuildCheck record "$ROOT" "$PROFILE"
