#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source_dir="$root/third_party/duckdb"
native_dir="$root/src/main/native/duckdb"
[[ "$#" -le 1 ]] || { echo "usage: $0 [RUNTIME_CONFIG]" >&2; exit 2; }
java=${JAVA_HOME:+$JAVA_HOME/bin/}java
assignments=$("$java" -cp "$root/build/sibench/sibench.jar" org.sibench.capture.NativeBuildCheck \
  engine-path "$root" "${1:-config/sibench.yaml}")
eval "$assignments"
build_root=$BUILD
[[ ! -e "$build_root" ]] || { echo "build already exists: $build_root; use a fresh build directory" >&2; exit 1; }
jobs=${JOBS:-4}
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || { echo 'JOBS must be a positive integer' >&2; exit 1; }
mkdir -p "$(dirname "$build_root")"
mkdir "$build_root" "$build_root/source"
"$java" -cp "$root/build/sibench/sibench.jar" org.sibench.capture.NativeBuildCheck \
  engine-inputs "$root" "${1:-config/sibench.yaml}"
pin=$(git -C "$root" ls-files --stage -- third_party/duckdb | awk '$1 == "160000" && $3 == "0" {print $2}')
[[ -n "$pin" && $(git -C "$source_dir" rev-parse HEAD) == "$pin" ]] || {
  echo 'Initialize third_party/duckdb at its repository gitlink before building' >&2
  exit 1
}
version=$(git -C "$source_dir" describe --tags --exact-match "$pin")
patch_file="$native_dir/duckdb-${version#v}.patch"
git -C "$source_dir" archive "$pin" | tar -x -C "$build_root/source"
patch --batch --forward -d "$build_root/source" -p1 < "$patch_file"
cp "$native_dir"/*.hpp "$build_root/source/src/include/"
cp "$native_dir/duckdb_host.cpp" "$build_root/duckdb_host.cpp"
"$java" -cp "$root/build/sibench/sibench.jar" org.sibench.capture.NativeBuildCheck \
  engine-prepared "$root" "${1:-config/sibench.yaml}"
cmake -S "$build_root/source" -B "$build_root" \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_UNITTESTS=OFF -DBUILD_SHELL=ON \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DSKIP_EXTENSIONS='parquet;json;icu;jemalloc' \
  -DGIT_COMMIT_HASH="$pin" -DDUCKDB_EXPLICIT_VERSION="$version"
cmake --build "$build_root" --target shell duckdb -j "$jobs"
compiler=$(sed -n 's/^CMAKE_CXX_COMPILER:FILEPATH=//p' "$build_root/CMakeCache.txt")
"$compiler" --version > "$build_root/compiler-version.txt"
"$compiler" -std=c++17 -O2 -pthread -I "$build_root/source/src/include" \
  "$build_root/duckdb_host.cpp" -L "$build_root/src" -lduckdb \
  -Wl,-rpath,'$ORIGIN/src' -o "$build_root/duckdb_host"
"$java" -cp "$root/build/sibench/sibench.jar" org.sibench.capture.NativeBuildCheck \
  engine-record "$root" "${1:-config/sibench.yaml}"
printf '\nDuckDB CLI: %s\n' "$build_root/duckdb"
printf 'DuckDB library: %s\n' "$build_root/src"
printf 'DuckDB host: %s\n' "$build_root/duckdb_host"
