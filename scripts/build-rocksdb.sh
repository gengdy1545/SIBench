#!/usr/bin/env bash
# Build the native timestamp/checksum bridge; Maven owns the RocksDB runtime dependency.
set -euo pipefail
[ "$#" -eq 1 ] || { echo "usage: $0 build/services/<fresh-rocksdb-directory>" >&2; exit 2; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
python3 - "$ROOT" "$1" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

root = Path(sys.argv[1]).resolve()
out = (root / sys.argv[2]).resolve()
if not out.is_relative_to(root / 'build/services') or out == root / 'build/services' or out.exists():
    raise SystemExit('RocksDB build requires a fresh directory under build/services')
out.mkdir(parents=True)
subprocess.run(['mvn', '-q', 'dependency:copy-dependencies', '-DincludeArtifactIds=rocksdbjni',
                '-DoutputDirectory=' + str(out)], check=True)
jars = list(out.glob('rocksdbjni-*.jar'))
if len(jars) != 1:
    raise SystemExit('Expected exactly one Maven-owned RocksDB runtime')
with zipfile.ZipFile(jars[0]) as archive:
    library = 'librocksdbjni-linux64.so'
    (out / library).write_bytes(archive.read(library))
java = Path(shutil.which('javac')).resolve().parent.parent
headers = root / 'index/rocksdb-cloud/include'
source = root / 'src/main/native/rocksdb/timestamps_jni.cc'
crypto = Path(subprocess.check_output(['pkg-config', '--variable=libdir', 'libcrypto'], text=True).strip()) / 'libcrypto.a'
if not crypto.is_file():
    raise SystemExit('OpenSSL development headers and static libcrypto are required')
command = [os.environ.get('CXX', 'c++'), '-std=c++17', '-O2', '-fPIC', '-shared', '-fno-rtti',
           '-D_GLIBCXX_USE_CXX11_ABI=0', '-I' + str(headers), '-I' + str(java / 'include'),
           '-I' + str(java / 'include/linux'), str(source), '-L' + str(out),
           '-l:librocksdbjni-linux64.so', str(crypto), '-ldl', '-pthread', '-Wl,-rpath,$ORIGIN', '-Wl,--no-undefined',
           '-o', str(out / 'libsibench_rocksdb.so')]
subprocess.run(command, check=True)
def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as file:
        for block in iter(lambda: file.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()
identity = {'inputs': {'bridge_sha256': digest(source),
                       'build_script_sha256': digest(root / 'scripts/build-rocksdb.sh'),
                       'headers': {str(p.relative_to(headers)): digest(p) for p in sorted(headers.rglob('*.h'))}},
            'artifacts': {p.name: digest(p) for p in sorted(out.iterdir()) if p.is_file()},
            'compile_command': command,
            'openssl': {'version': subprocess.check_output(['pkg-config', '--modversion', 'libcrypto'], text=True).strip(),
                        'static_library_sha256': digest(crypto)}}
(out / 'engine-build.json').write_text(json.dumps(identity, indent=2) + '\n')
print(out)
PY
