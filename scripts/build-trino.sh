#!/usr/bin/env bash
# Builds the pinned connector from source. SIBench/Pixels use JDK 17; Trino uses JDK 23.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
[[ "$#" -eq 1 ]] || { echo 'usage: TRINO_JAVA_HOME=/path/to/jdk23 scripts/build-trino.sh build/services/<directory>' >&2; exit 2; }
: "${TRINO_JAVA_HOME:?Set TRINO_JAVA_HOME to JDK 23}"
build=$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$1")
[[ "$build" == "$root/build/services/"?* && ! -e "$build" ]] || { echo 'Use a fresh directory under build/services' >&2; exit 1; }
java -version 2>&1 | head -1 | grep -q '"17\.' || { echo 'Build Pixels using JDK 17' >&2; exit 1; }
"$TRINO_JAVA_HOME/bin/java" -version 2>&1 | head -1 | grep -q '"23\.' || { echo 'Trino requires JDK 23' >&2; exit 1; }
pin() {
  local value
  value=$(git -C "$root" ls-files --stage -- "$1" | awk '$1 == "160000" && $3 == "0" {print $2}')
  [[ -n "$value" && $(git -C "$root/$1" rev-parse HEAD) == "$value" ]] || { echo "Initialize $1 at its gitlink" >&2; exit 1; }
  echo "$value"
}
pixels=$(pin third_party/pixels)
trino=$(pin third_party/pixels-trino)
patch_file="$root/src/main/native/pixels/pixels-0.2.0-SNAPSHOT.patch"
patch_sha=$(sha256sum "$patch_file" | cut -c1-64)
connector_patch="$root/src/main/native/pixels-trino/pixels-trino-0.2.0-SNAPSHOT.patch"
connector_patch_sha=$(sha256sum "$connector_patch" | cut -c1-64)
mkdir -p "$build/source/pixels" "$build/source/pixels-trino" "$build/home/etc" "$build/artifacts"
git -C "$root/third_party/pixels" archive "$pixels" | tar -x -C "$build/source/pixels"
git -C "$root/third_party/pixels-trino" archive "$trino" | tar -x -C "$build/source/pixels-trino"
patch --batch --forward -d "$build/source/pixels" -p1 < "$patch_file"
patch --batch --forward -d "$build/source/pixels-trino" -p1 < "$connector_patch"
cp "$build/source/pixels/pixels-common/src/main/resources/pixels.properties" "$build/home/etc/pixels.properties"
maven=(mvn -B -T "${JOBS:-4}" "-Dmaven.repo.local=$build/m2" "-Dmaven.repo.local.tail=$HOME/.m2/repository" -DskipTests -Dmaven.javadoc.skip=true)
(cd "$build/source/pixels" && PIXELS_HOME="$build/home" "${maven[@]}" \
  -pl pixels-planner,pixels-executor,pixels-storage/pixels-storage-redis,pixels-turbo/pixels-invoker-spike,pixels-turbo/pixels-invoker-lambda,pixels-turbo/pixels-invoker-vhive,pixels-turbo/pixels-scaling-ec2 -am install) > "$build/pixels-build.log" 2>&1
(cd "$build/source/pixels-trino" && JAVA_HOME="$TRINO_JAVA_HOME" PATH="$TRINO_JAVA_HOME/bin:$PATH" "${maven[@]}" -pl connector -am -Dmaven.test.skip=true package) > "$build/trino-build.log" 2>&1
cp "$build"/source/pixels-trino/connector/target/pixels-trino-connector-*.zip "$build/artifacts/connector.zip"
[[ $(sha256sum "$patch_file" | cut -c1-64) == "$patch_sha" ]] || { echo 'Pixels patch changed during build' >&2; exit 1; }
[[ $(sha256sum "$connector_patch" | cut -c1-64) == "$connector_patch_sha" ]] || { echo 'Connector patch changed during build' >&2; exit 1; }
python3 - "$build" "$pixels" "$trino" "$patch_sha" "$0" "$connector_patch_sha" <<'PY'
import hashlib,json,pathlib,sys,xml.etree.ElementTree as ET
build=pathlib.Path(sys.argv[1])
pom=ET.parse(build/'source/pixels-trino/pom.xml')
version=pom.find('{*}properties/{*}dep.trino.version').text
sha=lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
(build/'java-build.json').write_text(json.dumps({'pixels_commit':sys.argv[2],'connector_commit':sys.argv[3],
    'pixels_patch_sha256':sys.argv[4],'connector_patch_sha256':sys.argv[6],'build_script_sha256':sha(pathlib.Path(sys.argv[5])), 'trino_version':version,
    'files':{str(p.relative_to(build)):sha(p) for p in sorted((build/'artifacts').glob('*.zip'))}},indent=2))
PY
echo "Trino connector build: $build"
