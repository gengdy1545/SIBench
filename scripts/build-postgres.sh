#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source_dir="$root/third_party/postgresql"
native_dir="$root/src/main/native/postgres"
[[ "$#" -le 1 ]] || { echo "usage: $0 [RUNTIME_CONFIG]" >&2; exit 2; }
java=${JAVA_HOME:+$JAVA_HOME/bin/}java
config=${1:-config/sibench.yaml}
check() { "$java" -cp "$root/build/sibench/sibench.jar" org.sibench.capture.NativeBuildCheck "$1" "$root" "$config"; }
assignments=$(check postgres-path)
eval "$assignments"
[[ ! -e "$BUILD" ]] || { echo "build already exists: $BUILD; use a fresh build directory" >&2; exit 1; }
jobs=${JOBS:-4}
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || { echo 'JOBS must be a positive integer' >&2; exit 1; }
mkdir -p "$(dirname "$BUILD")"
mkdir "$BUILD" "$BUILD/source"
check postgres-inputs
pin=$(git -C "$root" ls-files --stage -- third_party/postgresql | awk '$1 == "160000" && $3 == "0" {print $2}')
[[ -n "$pin" && $(git -C "$source_dir" rev-parse HEAD) == "$pin" ]] || {
  echo 'Initialize third_party/postgresql at its repository gitlink before building' >&2; exit 1;
}
version=$(git -C "$source_dir" describe --tags --exact-match "$pin")
version=${version#REL_}
version=${version//_/.}
git -C "$source_dir" archive "$pin" | tar -x -C "$BUILD/source"
patch --batch --forward -d "$BUILD/source" -p1 < "$native_dir/postgresql-$version.patch"
cp "$native_dir/sibench_capture.c" "$BUILD/source/src/backend/access/index/"
cp "$native_dir/sibench_capture.h" "$BUILD/source/src/include/"
cp "$native_dir/row_capture.c" "$BUILD/source/"
check postgres-prepared
compiler=${CC:-cc}
"$compiler" --version > "$BUILD/compiler-version.txt"
cd "$BUILD/source"
./configure --prefix="$BUILD/runtime" --without-readline --without-zlib CC="$compiler"
make -C src/backend generated-headers
make -j "$jobs"
make install
"$compiler" -fPIC -shared -I src/include -o "$BUILD/runtime/lib/row_capture.so" row_capture.c
check postgres-record
printf '\nPostgreSQL runtime: %s\n' "$BUILD/runtime"
