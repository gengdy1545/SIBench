#!/usr/bin/env bash
# Build fixed submodule snapshots outside the working submodules.
set -euo pipefail
sibench_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$sibench_root"
sibench_java=${SIBENCH_JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
mkdir -p build/benchmarks results/native-validation
# Check every source and destination before exporting any snapshots.
for benchmark in YCSB sysbench HyBench-2024; do
    source_dir="benchmark/$benchmark"
    destination="build/benchmarks/$benchmark"
    if [[ -e "$destination" ]]; then
        echo "Refusing to overwrite $destination; move previous build before rebuilding." >&2
        exit 1
    fi
    git -C "$source_dir" rev-parse --verify HEAD > /dev/null
done
for benchmark in YCSB sysbench HyBench-2024; do
    destination="build/benchmarks/$benchmark"
    mkdir -p "$destination"
    git -C "benchmark/$benchmark" archive HEAD | tar -x -C "$destination"
done
git submodule status > results/native-validation/source-versions.txt
(
    cd build/benchmarks/YCSB
    JAVA_HOME="$sibench_java" mvn -B -ntp -pl site.ycsb:jdbc-binding -am -DskipTests package
    tar -xzf jdbc/target/ycsb-jdbc-binding-0.18.0-SNAPSHOT.tar.gz -C jdbc/target
) > results/native-validation/ycsb-build.log 2>&1
(
    cd build/benchmarks/sysbench
    bash autogen.sh
    # Prevent configure from embedding the enclosing SIBench repository hash.
    GIT_CEILING_DIRECTORIES="$sibench_root/build/benchmarks" ./configure --without-mysql --with-pgsql
    make -j4
) > results/native-validation/sysbench-build.log 2>&1
# Maven dependency graph includes obsolete HTTP repositories; use declared jars.
ssj_jar=${SIBENCH_SSJ_JAR:-$HOME/.m2/repository/ca/umontreal/iro/ssj/2.5/ssj-2.5.jar}
if [[ ! -f "$ssj_jar" ]]; then
    JAVA_HOME="$sibench_java" mvn -B -ntp dependency:get -Dartifact=ca.umontreal.iro:ssj:2.5 -Dtransitive=false
fi
mkdir -p build/benchmarks/HyBench-2024/target/classes
cp "$ssj_jar" build/benchmarks/HyBench-2024/lib/ssj-2.5.jar
rg --files build/benchmarks/HyBench-2024/src/main/java -g '*.java' > build/benchmarks/hybench-sources.txt
"$sibench_java/bin/javac" --release 16 -cp 'build/benchmarks/HyBench-2024/lib/*' \
    -d build/benchmarks/HyBench-2024/target/classes @build/benchmarks/hybench-sources.txt \
    > results/native-validation/hybench-javac.log 2>&1
cp -R build/benchmarks/HyBench-2024/src/main/resources/. build/benchmarks/HyBench-2024/target/classes/
echo 'Built all three benchmark snapshots; see results/native-validation for logs.'
