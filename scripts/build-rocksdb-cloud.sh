#!/bin/bash
# Builds the rocksdb-cloud JNI binding (build/index/librocksdb_cloud_jni.so) on a target host.
# Upstream sources are pinned by submodule gitlinks in index/ and third_party/.
# Needs root for yum prerequisites. All generated files stay under build/index/.
# Build on a system compatible with the hosts that load the library.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
JOBS=${JOBS:-2}

for source in third_party/aws-sdk-cpp index/rocksdb-cloud; do
    if [ "$(git -C "$ROOT/$source" rev-parse HEAD)" != "$(git -C "$ROOT" rev-parse ":$source")" ] ||
       [ -n "$(git -C "$ROOT/$source" status --porcelain)" ]; then
        echo "$source must be clean and match its pinned submodule commit. Run git submodule update --init --recursive." >&2
        exit 1
    fi
done

yum install -y -q cmake libcurl-devel openssl-devel zlib-devel bzip2-devel lz4-devel \
    libzstd-devel snappy-devel java-17-openjdk-devel
export JAVA_HOME=${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")}

SDK=$ROOT/build/index/aws-sdk
ROCKS=$ROOT/build/index/rocksdb-cloud
if [ -e "$ROCKS" ]; then
    echo "build copy already exists: $ROCKS; use a fresh build directory" >&2
    exit 1
fi
mkdir -p "$ROOT/build/index/aws-sdk-cpp"
git clone --shared "$ROOT/index/rocksdb-cloud" "$ROCKS"
patch --batch --forward -d "$ROCKS" -p1 < "$ROOT/src/main/native/index/rocksdb_cloud/rocksdb-cloud-9.1.1.patch"
cd "$ROOT/build/index/aws-sdk-cpp"
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON '-DBUILD_ONLY=kinesis;core;s3;transfer' \
    -DCMAKE_INSTALL_PREFIX="$SDK" -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_CXX_FLAGS='-Wno-error' "$ROOT/third_party/aws-sdk-cpp"
make -j"$JOBS" && make install

cd "$ROCKS"
# Upstream gcc 8 warnings (unused parameters in cloud/aws) are not errors here.
AWS_SDK="$SDK" USE_AWS=1 PORTABLE=1 DEBUG_LEVEL=0 USE_RTTI=1 DISABLE_WARNING_AS_ERROR=1 \
    EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC make -j"$JOBS" static_lib

cd "$ROOT/src/main/native/index/rocksdb_cloud"
make ROCKSDB_CLOUD="$ROCKS" AWS_PREFIX="$SDK" OUT="$ROOT/build/index/librocksdb_cloud_jni.so"
strip --strip-unneeded "$ROOT/build/index/librocksdb_cloud_jni.so"
sha256sum "$ROOT/build/index/librocksdb_cloud_jni.so"
