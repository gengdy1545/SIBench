#!/bin/bash
# Builds the rocksdb-cloud JNI binding (build/index/librocksdb_cloud_jni.so) on a target host.
# Upstream sources are pinned by submodule gitlinks in index/ and third_party/.
# Needs root for yum prerequisites. All generated files stay under build/index/.
# Build on a system compatible with the hosts that load the library.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
JOBS=${JOBS:-2}
[[ "$JOBS" =~ ^[1-9][0-9]*$ ]] || { echo "JOBS must be a positive integer" >&2; exit 1; }

for source in third_party/aws-sdk-cpp index/rocksdb-cloud; do
    if [ "$(git -C "$ROOT/$source" rev-parse HEAD)" != "$(git -C "$ROOT" rev-parse ":$source")" ] ||
       [ -n "$(git -C "$ROOT/$source" status --porcelain)" ]; then
        echo "$source must be clean and match its pinned submodule commit. Run git submodule update --init --recursive." >&2
        exit 1
    fi
done

CLOUD_PIN=$(git -C "$ROOT" rev-parse :index/rocksdb-cloud)
AWS_PIN=$(git -C "$ROOT" rev-parse :third_party/aws-sdk-cpp)
SDK=$ROOT/build/index/aws-sdk
AWS_BUILD=$ROOT/build/index/aws-sdk-cpp
ROCKS=$ROOT/build/index/rocksdb-cloud
LIBRARY=$ROOT/build/index/librocksdb_cloud_jni.so
for path in "$SDK" "$AWS_BUILD" "$ROCKS" "$LIBRARY" "$ROOT/build/index/rocksdb-cloud-build.json"; do
    if [ -e "$path" ] || [ -L "$path" ]; then
        echo "build output already exists: $path; use fresh build outputs" >&2
        exit 1
    fi
done
inputs() {
    python3 - "$ROOT" "$CLOUD_PIN" "$AWS_PIN" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
names = ["scripts/build-rocksdb-cloud.sh", "src/main/native/index/rocksdb_cloud/Makefile",
         "src/main/native/index/rocksdb_cloud/jni_exports.map", "src/main/native/index/rocksdb_cloud/rocksdb_cloud_jni.cc",
         "src/main/native/index/rocksdb_cloud/rocksdb-cloud-9.1.1.patch"]
print(json.dumps({"rocksdb_cloud_commit": sys.argv[2], "aws_sdk_commit": sys.argv[3],
                  "files": {n: hashlib.sha256((root / n).read_bytes()).hexdigest() for n in names}}, sort_keys=True))
PY
}
EXPECTED_INPUTS=$(inputs)

yum install -y -q cmake libcurl-devel openssl-devel zlib-devel bzip2-devel lz4-devel \
    libzstd-devel snappy-devel java-17-openjdk-devel
export JAVA_HOME=${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")}

mkdir -p "$AWS_BUILD/source" "$ROCKS"
printf '%s\n' "$EXPECTED_INPUTS" > "$ROCKS/engine-inputs.json"
git -C "$ROOT/index/rocksdb-cloud" archive "$CLOUD_PIN" | tar -x -C "$ROCKS"
git -C "$ROOT/third_party/aws-sdk-cpp" archive "$AWS_PIN" | tar -x -C "$AWS_BUILD/source"
patch --batch --forward -d "$ROCKS" -p1 < "$ROOT/src/main/native/index/rocksdb_cloud/rocksdb-cloud-9.1.1.patch"
cd "$AWS_BUILD"
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON '-DBUILD_ONLY=kinesis;core;s3;transfer' \
    -DCMAKE_INSTALL_PREFIX="$SDK" -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_CXX_FLAGS='-Wno-error' "$AWS_BUILD/source"
make -j"$JOBS" && make install

cd "$ROCKS"
# Upstream gcc 8 warnings (unused parameters in cloud/aws) are not errors here.
AWS_SDK="$SDK" USE_AWS=1 PORTABLE=1 DEBUG_LEVEL=0 USE_RTTI=1 DISABLE_WARNING_AS_ERROR=1 \
    EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC make -j"$JOBS" static_lib

cd "$ROOT/src/main/native/index/rocksdb_cloud"
make ROCKSDB_CLOUD="$ROCKS" AWS_PREFIX="$SDK" OUT="$ROOT/build/index/librocksdb_cloud_jni.so"
strip --strip-unneeded "$ROOT/build/index/librocksdb_cloud_jni.so"
[[ $(inputs) == "$EXPECTED_INPUTS" ]] || { echo "rocksdb-cloud build inputs changed during compilation" >&2; exit 1; }
"${CXX:-g++}" --version > "$ROCKS/compiler-version.txt"
python3 - "$ROOT" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]); build = root / "build/index"
files = [build / "librocksdb_cloud_jni.so", build / "rocksdb-cloud/librocksdb.a", build / "rocksdb-cloud/compiler-version.txt"]
files += sorted((build / "aws-sdk").rglob("*.a"))
def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""): digest.update(block)
    return digest.hexdigest()
receipt = {"inputs": json.loads((build / "rocksdb-cloud/engine-inputs.json").read_text()),
           "artifacts": {str(p.relative_to(root)): sha(p) for p in files}}
with (build / "rocksdb-cloud-build.json").open("x") as output: json.dump(receipt, output, indent=2)
PY
sha256sum "$ROOT/build/index/librocksdb_cloud_jni.so"
