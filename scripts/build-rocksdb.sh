#!/usr/bin/env bash
# Build RocksJava, its native runtime and the timestamp bridge from one gitlink.
set -euo pipefail
[ "$#" -eq 1 ] || { echo "usage: $0 build/services/<fresh-rocksdb-directory>" >&2; exit 2; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
python3 - "$ROOT" "$1" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys

root = Path(sys.argv[1]).resolve()
out = (root / sys.argv[2]).resolve()
if not out.is_relative_to(root / 'build/services') or out == root / 'build/services' or out.exists():
    raise SystemExit('RocksDB build requires a fresh directory under build/services')
if platform.system() != 'Linux' or platform.machine() != 'x86_64':
    raise SystemExit('RocksDB build requires Linux x86-64')
jobs = os.environ.get('JOBS', '4')
if not jobs.isdigit() or int(jobs) < 1:
    raise SystemExit('JOBS must be a positive integer')
upstream = root / 'third_party/rocksdb'
native = root / 'src/main/native/rocksdb'

def git(directory, *args):
    return subprocess.check_output(['git', '-C', str(directory), *args], text=True).strip()

def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as file:
        for block in iter(lambda: file.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()

def tree(directory):
    return {str(p.relative_to(directory)): digest(p)
            for p in sorted(directory.rglob('*')) if p.is_file()}

def inputs():
    pin = git(root, 'rev-parse', ':third_party/rocksdb')
    if not re.fullmatch('[0-9a-f]{40}', pin) or git(upstream, 'rev-parse', 'HEAD') != pin or git(
            upstream, 'status', '--porcelain=v1', '--untracked-files=all'):
        raise SystemExit('RocksDB source must be clean and at its gitlink')
    return {'source_commit': pin, 'native_sources': tree(native),
            'build_script_sha256': digest(root / 'scripts/build-rocksdb.sh')}

expected = inputs()
java = Path(os.environ['JAVA_HOME']) if os.environ.get('JAVA_HOME') else Path(shutil.which('javac')).resolve().parent.parent
compiler = os.environ.get('CXX', 'c++')
out.mkdir(parents=True)
(out / 'engine-inputs.json').write_text(json.dumps(expected, indent=2) + '\n')
source = out / 'source'
source.mkdir()
archive = out / 'source.tar'
subprocess.run(['git', '-C', str(upstream), 'archive', '--output=' + str(archive),
                expected['source_commit']], check=True)
subprocess.run(['tar', '-xf', str(archive), '-C', str(source)], check=True)
archive.unlink()
version = subprocess.check_output(['bash', 'build_tools/version.sh', 'full'], cwd=source, text=True).strip()
subprocess.run(['patch', '--batch', '--forward', '-d', str(source), '-p1',
                '-i', str(native / ('rocksdb-' + version + '.patch'))], check=True)
shutil.copytree(native, out / 'native')
prepared = tree(source)
(out / 'source-files.json').write_text(json.dumps(prepared, indent=2) + '\n')
cmake = out / 'cmake'
configure = ['cmake', '-S', str(source), '-B', str(cmake), '-DCMAKE_BUILD_TYPE=Release',
             '-DCMAKE_CXX_COMPILER=' + compiler, '-DCMAKE_CXX_STANDARD=17',
             '-DCMAKE_EXPORT_COMPILE_COMMANDS=ON', '-DWITH_JNI=ON', '-DWITH_TESTS=OFF',
             '-DWITH_BENCHMARK_TOOLS=OFF', '-DWITH_TOOLS=OFF', '-DWITH_GFLAGS=OFF',
             '-DROCKSDB_BUILD_SHARED=OFF', '-DUSE_RTTI=OFF', '-DPORTABLE=1',
             '-DWITH_LZ4=ON', '-DWITH_ZSTD=ON', '-DWITH_ZLIB=ON',
             '-DWITH_SNAPPY=ON', '-DWITH_BZ2=ON', '-DFAIL_ON_WARNINGS=OFF']
environment = {**os.environ, 'JAVA_HOME': str(java)}
subprocess.run(configure, env=environment, check=True)
subprocess.run(['cmake', '--build', str(cmake), '--target', 'rocksdbjni_classes', '-j', jobs],
               env=environment, check=True)
subprocess.run(['cmake', '--build', str(cmake), '--target', 'rocksdbjava', '-j', jobs],
               env=environment, check=True)
library = 'librocksdbjni-linux64.so'
shutil.copy2(cmake / 'java' / library, out / library)
shutil.copy2(cmake / 'java' / ('rocksdbjni-' + version + '-linux64.jar'), out / 'rocksdbjni.jar')
# Keep upstream license texts with the redistributed Java/native artifact.
subprocess.run([str(java / 'bin/jar'), '--update', '--file', str(out / 'rocksdbjni.jar'),
                '-C', str(source), 'COPYING', '-C', str(source), 'LICENSE.Apache',
                '-C', str(source), 'LICENSE.leveldb'], check=True)
command = [compiler, '-std=c++17', '-O2', '-fPIC', '-shared', '-fno-rtti',
           '-I' + str(source / 'include'), '-I' + str(java / 'include'),
           '-I' + str(java / 'include/linux'), str(out / 'native/timestamps_jni.cc'), '-L' + str(out),
           '-l:librocksdbjni-linux64.so', '-ldl', '-pthread', '-Wl,-rpath,$ORIGIN', '-Wl,--no-undefined',
           '-o', str(out / 'libsibench_rocksdb.so')]
subprocess.run(command, check=True)
(out / 'compiler-version.txt').write_text(subprocess.check_output([compiler, '--version'], text=True))
# Verify the original prepared files; CMake also generates files under the source copy.
if inputs() != expected or tree(out / 'native') != expected['native_sources'] or any(
        not (source / name).is_file() or digest(source / name) != checksum for name, checksum in prepared.items()):
    raise SystemExit('RocksDB build inputs changed during compilation')
artifacts = ['rocksdbjni.jar', library, 'libsibench_rocksdb.so', 'source-files.json',
             'compiler-version.txt', 'cmake/CMakeCache.txt', 'cmake/compile_commands.json']
identity = {'inputs': expected,
            'artifacts': {name: digest(out / name) for name in artifacts},
            'configure_command': configure,
            'compile_command': command}
(out / 'engine-build.json').write_text(json.dumps(identity, indent=2) + '\n')
print(out)
PY
