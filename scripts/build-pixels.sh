#!/usr/bin/env bash
# Builds the Pixels CDC generation engine into a fresh directory under build/services.
#   java   BUILD  Pixels daemon/CLI, patched pixels-sink and the SIBench index tracer (Maven, JDK 17)
#   native BUILD  Retina JNI library, the uninstrumented source PostgreSQL and the pinned etcd release
#                 (cmake, C/C++ toolchain, network access); run on the operating system that runs Pixels
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
[[ "$#" -eq 2 && "$1" =~ ^(java|native)$ ]] || { echo "usage: $0 java|native build/services/<directory>" >&2; exit 2; }
phase=$1
build=$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$2")
[[ "$build" == "$root/build/services/"?* ]] || { echo "Pixels build must be under build/services" >&2; exit 1; }
etcd_version=v3.5.35
etcd_sha256=ca9f94cdc80be66923ef571d82b128819d0f5f3aa9f01522bafd991618ee29d1
jobs=${JOBS:-8}
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || { echo 'JOBS must be a positive integer' >&2; exit 1; }

gitlink() {
  local pin
  pin=$(git -C "$root" ls-files --stage -- "$1" | awk '$1 == "160000" && $3 == "0" {print $2}')
  [[ -n "$pin" && $(git -C "$root/$1" rev-parse HEAD) == "$pin" ]] || {
    echo "Initialize $1 at its repository gitlink before building" >&2; exit 1; }
  echo "$pin"
}
sha() { sha256sum "$1" | cut -c1-64; }
tree_sha() { (cd "$1" && find . -type f | LC_ALL=C sort | xargs sha256sum) | sha256sum | cut -c1-64; }

pixels=$(gitlink third_party/pixels)
sink=$(gitlink third_party/pixels-sink)
patch_file="$root/src/main/native/pixels-sink/pixels-sink-0.2.0-SNAPSHOT.patch"
pixels_patch="$root/src/main/native/pixels/pixels-0.2.0-SNAPSHOT.patch"

if [[ "$phase" == java ]]; then
  [[ ! -e "$build" ]] || { echo "build already exists: $build; use a fresh build directory" >&2; exit 1; }
  script_sha=$(sha "$0")
  pixels_patch_sha=$(sha "$pixels_patch")
  sink_patch_sha=$(sha "$patch_file")
  tracer_sha=$(tree_sha "$root/src/main/native/pixels")
  mkdir -p "$build/source/pixels" "$build/source/pixels-sink" "$build/jars" "$build/defaults" "$build/home/etc" "$build/tracer"
  git -C "$root/third_party/pixels" archive "$pixels" | tar -x -C "$build/source/pixels"
  git -C "$root/third_party/pixels-sink" archive "$sink" | tar -x -C "$build/source/pixels-sink"
  patch --batch --forward -d "$build/source/pixels" -p1 < "$pixels_patch"
  patch --batch --forward -d "$build/source/pixels-sink" -p1 < "$patch_file"
  cp "$build/source/pixels/pixels-common/src/main/resources/pixels.properties" "$build/defaults/pixels.properties"
  cp "$build/source/pixels-sink/src/main/resources/pixels-sink.properties" "$build/defaults/pixels-sink.properties"
  # The Retina native build reads the visibility capacity from PIXELS_HOME; upstream defaults set it.
  cp "$build/defaults/pixels.properties" "$build/home/etc/pixels.properties"
  maven=(mvn -B -T "$jobs" "-Dmaven.repo.local=$build/m2" "-Dmaven.repo.local.tail=$HOME/.m2/repository"
         -DskipTests -Dmaven.javadoc.skip=true)
  (cd "$build/source/pixels" && PIXELS_HOME="$build/home" "${maven[@]}" -pl pixels-daemon,pixels-cli -am install) > "$build/pixels-maven.log" 2>&1
  (cd "$build/source/pixels-sink" && "${maven[@]}" -Dmaven.test.skip=true package) > "$build/sink-maven.log" 2>&1
  cp "$build/source/pixels/pixels-daemon/target/pixels-daemon-0.2.0-SNAPSHOT-full.jar" "$build/jars/pixels-daemon.jar"
  cp "$build/source/pixels/pixels-cli/target/pixels-cli-0.2.0-SNAPSHOT-full.jar" "$build/jars/pixels-cli.jar"
  cp "$build/source/pixels-sink/target/pixels-sink-0.2.0-SNAPSHOT-full.jar" "$build/jars/pixels-sink.jar"
  mkdir -p "$build/tracer/classes/META-INF/services"
  javac --release 17 -nowarn -d "$build/tracer/classes" -cp "$build/jars/pixels-sink.jar:$build/jars/pixels-daemon.jar:$build/jars/pixels-cli.jar" \
    "$root"/src/main/native/pixels/*.java
  echo org.sibench.pixels.TracingIndexProvider \
    > "$build/tracer/classes/META-INF/services/io.pixelsdb.pixels.common.index.SinglePointIndexProvider"
  (cd "$build/tracer/classes" && jar cf "$build/jars/sibench-pixels.jar" .)
  [[ $(sha "$0") == "$script_sha" && $(sha "$pixels_patch") == "$pixels_patch_sha" &&
     $(sha "$patch_file") == "$sink_patch_sha" && $(tree_sha "$root/src/main/native/pixels") == "$tracer_sha" ]] || {
    echo "Pixels Java build inputs changed during compilation" >&2; exit 1; }
  python3 - "$build/java-build.json" <<PY
import json, sys
json.dump({"pixels_commit": "$pixels", "pixels_patch_sha256": "$pixels_patch_sha", "pixels_sink_commit": "$sink", "pixels_sink_patch_sha256": "$sink_patch_sha",
           "build_script_sha256": "$script_sha", "tracer_sources_sha256": "$tracer_sha", "java": "$(java -version 2>&1 | head -1 | tr -d '"')",
           "jars": {name: sha for name, sha in [$(for jar in "$build"/jars/*.jar; do printf '("%s", "%s"),' "$(basename "$jar")" "$(sha "$jar")"; done)]}},
          open(sys.argv[1], "x"), indent=2)
PY
  rm -rf "$build/source/pixels/cpp/pixels-retina/build" "$build/home/lib"
  printf '\nPixels Java build: %s\n' "$build"
  exit 0
fi

[[ -f "$build/java-build.json" && ! -e "$build/native-build.json" ]] || {
  echo "native phase needs a completed java phase and no previous native build in $build" >&2; exit 1; }
python3 - "$build/java-build.json" "$pixels" "$sink" <<'PY'
import json, sys
identity = json.load(open(sys.argv[1]))
if (identity["pixels_commit"], identity["pixels_sink_commit"]) != (sys.argv[2], sys.argv[3]):
    raise SystemExit("Java build was made from different Pixels sources")
PY
python3 - "$root/third_party/pixels" "$pixels" "$build" <<'PY'
import hashlib, pathlib, subprocess, sys
source, pin, build = sys.argv[1], sys.argv[2], pathlib.Path(sys.argv[3])
paths = ["cpp/pixels-retina", "scripts/etc/coordinator-jvm.config", "scripts/etc/retina-jvm.config",
         "pixels-common/src/main/resources/pixels.properties"]
entries = subprocess.check_output(["git", "-C", source, "ls-tree", "-rz", pin, "--", *paths]).split(b"\0")
for entry in filter(None, entries):
    metadata, name = entry.split(b"\t", 1)
    name = name.decode()
    expected = metadata.split()[2].decode()
    data = (build / "source/pixels" / name).read_bytes()
    if hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest() != expected:
        raise SystemExit("Pixels native input differs from its pin: " + name)
original = (build / "source/pixels/pixels-common/src/main/resources/pixels.properties").read_bytes()
for name in ["defaults/pixels.properties", "home/etc/pixels.properties"]:
    if (build / name).read_bytes() != original:
        raise SystemExit("Pixels capacity/defaults differ from their pin: " + name)
PY
runtime_configs=(defaults/pixels.properties home/etc/pixels.properties source/pixels/scripts/etc/coordinator-jvm.config source/pixels/scripts/etc/retina-jvm.config)
runtime_sha() { (cd "$build" && sha256sum "${runtime_configs[@]}") | sha256sum | cut -c1-64; }
native_sources_sha=$(tree_sha "$build/source/pixels/cpp/pixels-retina")
runtime_inputs_sha=$(runtime_sha)
capacity=$(sed -n 's/^retina.tile.visibility.capacity=//p' "$build/defaults/pixels.properties")
PIXELS_HOME="$build/home" cmake -B "$build/native/retina" -S "$build/source/pixels/cpp/pixels-retina" "-DRETINA_CAPACITY=$capacity" > "$build/native-retina.log" 2>&1
PIXELS_HOME="$build/home" make -C "$build/native/retina" -j "$jobs" install >> "$build/native-retina.log" 2>&1
[[ -f "$build/home/lib/libpixels-retina.so" && -f "$build/home/lib/libjemalloc.so" ]] || { echo "Retina native library missing" >&2; exit 1; }

postgres=$(gitlink third_party/postgresql)
mkdir -p "$build/postgres/source"
git -C "$root/third_party/postgresql" archive "$postgres" | tar -x -C "$build/postgres/source"
compiler=${CC:-cc}
(cd "$build/postgres/source" && ./configure --prefix="$build/postgres/runtime" --without-readline --without-zlib CC="$compiler" \
  && make -j "$jobs" && make install) > "$build/postgres.log" 2>&1

archive="$build/etcd-$etcd_version-linux-amd64.tar.gz"
curl -sSLf --retry 5 -o "$archive" "https://github.com/etcd-io/etcd/releases/download/$etcd_version/etcd-$etcd_version-linux-amd64.tar.gz"
[[ $(sha "$archive") == "$etcd_sha256" ]] || { echo "etcd release checksum mismatch" >&2; exit 1; }
mkdir -p "$build/etcd" && tar -xzf "$archive" -C "$build/etcd" --strip-components=1 && rm "$archive"

[[ $(tree_sha "$build/source/pixels/cpp/pixels-retina") == "$native_sources_sha" && $(runtime_sha) == "$runtime_inputs_sha" ]] || {
  echo "Pixels native inputs changed during compilation" >&2; exit 1; }
python3 - "$build/native-build.json" "$build" "${runtime_configs[@]}" <<PY
import json, sys
import hashlib, pathlib
root = pathlib.Path(sys.argv[2])
configs = sys.argv[3:]
json.dump({"pixels_commit": "$pixels", "postgresql_commit": "$postgres", "retina_capacity": int("$capacity"),
           "native_sources_sha256": "$native_sources_sha",
           "runtime_configs": {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in configs},
           "compiler": "$("$compiler" --version | head -1)", "cmake": "$(cmake --version | head -1)",
           "etcd_version": "$etcd_version", "etcd_sha256": "$etcd_sha256",
           "files": {name: sha for name, sha in [("libpixels-retina.so", "$(sha "$build/home/lib/libpixels-retina.so")"),
                     ("libjemalloc.so", "$(sha "$build/home/lib/libjemalloc.so")"),
                     ("postgres", "$(sha "$build/postgres/runtime/bin/postgres")"),
                     ("pg_ctl", "$(sha "$build/postgres/runtime/bin/pg_ctl")"), ("initdb", "$(sha "$build/postgres/runtime/bin/initdb")"),
                     ("etcd", "$(sha "$build/etcd/etcd")")]}},
          open(sys.argv[1], "x"), indent=2)
PY
printf '\nPixels native build: %s\n' "$build"
