#!/usr/bin/env bash
# Build the instrumented TiDB and TiKV v8.5.8 used by the TiDB capture PoC.
# Pins: TiDB tag v8.5.8 (8b857efa), TiKV tag v8.5.8 (a1fdd6a5); PD runs the official v8.5.8 binary.
# Prerequisites: Go 1.25.12 (TiDB go.mod), rustup with the toolchain in TiKV's rust-toolchain.toml
# (nightly-2026-01-30), cmake >= 3.x, gcc/g++, perl, make. Output: build/capture/tidb-v8.5.8/.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-$ROOT/build/capture/tidb-v8.5.8}
TIDB_COMMIT=8b857efa20363d50a8fa2ea7dd9809a85a61b115
TIKV_COMMIT=a1fdd6a59bb9c8a282c15677e1894501a52425cf
[ ! -e "$OUT" ] || { echo "build already exists: $OUT; use a fresh directory" >&2; exit 1; }
mkdir -p "$OUT/src"
fetch() {
  git clone --filter=blob:none --no-checkout "$1" "$OUT/src/$2"
  git -C "$OUT/src/$2" checkout -q "$3"
  git -C "$OUT/src/$2" apply "$ROOT/src/main/native/tidb/$2-v8.5.8.patch"
}
fetch https://github.com/pingcap/tidb.git tidb "$TIDB_COMMIT"
fetch https://github.com/tikv/tikv.git tikv "$TIKV_COMMIT"
(cd "$OUT/src/tidb" && go build -tags codes -ldflags \
  "-X github.com/pingcap/tidb/pkg/parser/mysql.TiDBReleaseVersion=v8.5.8-sibench-capture -X github.com/pingcap/tidb/pkg/util/versioninfo.TiDBGitHash=$TIDB_COMMIT" \
  -o "$OUT/tidb-server" ./cmd/tidb-server)
# Thin LTO and one codegen unit (TiKV's release profile) are relaxed for build time only.
(cd "$OUT/src/tikv" && TIKV_FRAME_POINTER=0 CARGO_TARGET_DIR="$OUT/tikv-target" \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 CARGO_PROFILE_RELEASE_LTO=off make release)
cp "$OUT/tikv-target/release/tikv-server" "$OUT/tikv-server"
(cd "$OUT" && sha256sum tidb-server tikv-server "$ROOT"/src/main/native/tidb/*.patch > SHA256SUMS)
