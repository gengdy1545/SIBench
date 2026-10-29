#!/bin/bash
# Builds the rocksdb-cloud JNI binding (build/index/librocksdb_cloud_jni.so) on a target host.
# Pinned upstream versions are in index/versions.yaml; sources are cloned into index/.
# Needs root: build dependencies come from yum, and rocksdb-cloud's build looks for the AWS SDK
# in /usr/local. Run on the glibc of the hosts that load the library (the sibench hosts: 2.28).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
AWS_SDK_TAG=1.8.8
ROCKSDB_CLOUD_COMMIT=20a0918820ef89d71f4a059da4864db66678c38b
JOBS=${JOBS:-32}

yum install -y -q cmake libcurl-devel openssl-devel zlib-devel bzip2-devel lz4-devel \
    libzstd-devel snappy-devel java-17-openjdk-devel
mkdir -p "$ROOT/index" "$ROOT/build/index"
cd "$ROOT/index"
[ -d aws-sdk-cpp ] || git clone -q --single-branch --depth 1 --branch "$AWS_SDK_TAG" \
    https://github.com/aws/aws-sdk-cpp.git
[ -d rocksdb-cloud ] || git clone -q https://github.com/rockset/rocksdb-cloud.git
git -C rocksdb-cloud checkout -q "$ROCKSDB_CLOUD_COMMIT"
test -z "$(git -C rocksdb-cloud status --porcelain)"

mkdir -p "$ROOT/build/index/aws-sdk-cpp"
cd "$ROOT/build/index/aws-sdk-cpp"
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON '-DBUILD_ONLY=kinesis;core;s3;transfer' \
    -DCMAKE_INSTALL_PREFIX=/usr/local -DCMAKE_CXX_FLAGS='-Wno-error' "$ROOT/index/aws-sdk-cpp"
make -j"$JOBS" && make install

cd "$ROOT/index/rocksdb-cloud"
# Upstream gcc 8 warnings (unused parameters in cloud/aws) are not errors here.
USE_AWS=1 PORTABLE=1 DEBUG_LEVEL=0 USE_RTTI=1 DISABLE_WARNING_AS_ERROR=1 \
    EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC make -j"$JOBS" static_lib

cd "$ROOT/src/main/native/index/rocksdb_cloud"
make ROCKSDB_CLOUD="$ROOT/index/rocksdb-cloud" OUT="$ROOT/build/index/librocksdb_cloud_jni.so"
strip --strip-unneeded "$ROOT/build/index/librocksdb_cloud_jni.so"
sha256sum "$ROOT/build/index/librocksdb_cloud_jni.so"
