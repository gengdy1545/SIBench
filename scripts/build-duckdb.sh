#!/usr/bin/env bash
# Build the instrumented engine and JDBC from the same core source copy.
set -euo pipefail
[ "$#" -eq 1 ] || { echo "usage: $0 build/services/<fresh-duckdb-directory>" >&2; exit 2; }
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
    raise SystemExit('DuckDB build requires a fresh directory under build/services')
if platform.system() != 'Linux' or platform.machine() != 'x86_64':
    raise SystemExit('DuckDB build requires Linux x86-64')
jobs = os.environ.get('JOBS', '4')
if not jobs.isdigit() or int(jobs) < 1:
    raise SystemExit('JOBS must be a positive integer')
upstream = root / 'third_party/duckdb'
native = root / 'src/main/native/duckdb'

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

def source_pin(name):
    source = root / ('third_party/' + name)
    pin = git(root, 'rev-parse', ':third_party/' + name)
    if not re.fullmatch('[0-9a-f]{40}', pin) or git(source, 'rev-parse', 'HEAD') != pin or git(
            source, 'status', '--porcelain=v1', '--untracked-files=all'):
        raise SystemExit(name + ' source must be clean and at its gitlink')
    return pin

def inputs():
    return {'source_commit': source_pin('duckdb'), 'jdbc_source_commit': source_pin('duckdb-java'),
            'native_sources': tree(native), 'build_script_sha256': digest(root / 'scripts/build-duckdb.sh')}

def run(*args, **kwargs):
    subprocess.run(list(map(str, args)), check=True, **kwargs)

def archive(name, pin, target):
    target.mkdir()
    tar = out / (name + '.tar')
    run('git', '-C', root / ('third_party/' + name), 'archive', '--output=' + str(tar), pin)
    run('tar', '-xf', tar, '-C', target)
    tar.unlink()

expected = inputs()
java = Path(os.environ['JAVA_HOME']) if os.environ.get('JAVA_HOME') else Path(shutil.which('javac')).resolve().parent.parent
environment = {**os.environ, 'JAVA_HOME': str(java)}
out.mkdir(parents=True)
(out / 'engine-inputs.json').write_text(json.dumps(expected, indent=2) + '\n')
source = out / 'source'
archive('duckdb', expected['source_commit'], source)
version = git(upstream, 'describe', '--tags', '--exact-match', expected['source_commit'])
run('patch', '--batch', '--forward', '-d', source, '-p1', '-i', native / ('duckdb-' + version.removeprefix('v') + '.patch'))
for header in native.glob('*.hpp'):
    shutil.copy2(header, source / 'src/include' / header.name)
shutil.copy2(native / 'duckdb_host.cpp', out / 'duckdb_host.cpp')
jdbc = out / 'jdbc-source'
archive('duckdb-java', expected['jdbc_source_commit'], jdbc)
jdbc_version = git(root / 'third_party/duckdb-java', 'describe', '--tags', '--exact-match', expected['jdbc_source_commit'])
run('patch', '--batch', '--forward', '-d', jdbc, '-p1', '-i', native / ('duckdb-java-' + jdbc_version.removeprefix('v') + '.patch'))
jni = out / 'jdbc'
jni.mkdir()
shutil.copy2(native / 'jdbc.cmake', jni / 'CMakeLists.txt')
prepared = {'source/' + name: sha for name, sha in tree(source).items()}
prepared.update({'jdbc-source/' + name: sha for name, sha in tree(jdbc).items()})
prepared['duckdb_host.cpp'] = digest(out / 'duckdb_host.cpp')
prepared['jdbc/CMakeLists.txt'] = digest(jni / 'CMakeLists.txt')
(out / 'source-files.json').write_text(json.dumps(prepared, indent=2) + '\n')
run('cmake', '-S', source, '-B', out, '-DCMAKE_BUILD_TYPE=Release', '-DBUILD_UNITTESTS=OFF',
    '-DBUILD_SHELL=ON', '-DCMAKE_EXPORT_COMPILE_COMMANDS=ON', '-DSKIP_EXTENSIONS=parquet;json;icu;jemalloc',
    '-DGIT_COMMIT_HASH=' + expected['source_commit'], '-DDUCKDB_EXPLICIT_VERSION=' + version)
run('cmake', '--build', out, '--target', 'shell', 'duckdb', '-j', jobs)
compiler = next(line.split('=', 1)[1] for line in (out / 'CMakeCache.txt').read_text().splitlines()
                if line.startswith('CMAKE_CXX_COMPILER:FILEPATH='))
(out / 'compiler-version.txt').write_text(subprocess.check_output([compiler, '--version'], text=True))
run(compiler, '-std=c++17', '-O2', '-pthread', '-I' + str(source / 'src/include'),
    out / 'duckdb_host.cpp', '-L' + str(out / 'src'), '-lduckdb', '-Wl,-rpath,$ORIGIN/src', '-o', out / 'duckdb_host')
run(java / 'bin/javac', '--release', '17', '-encoding', 'UTF-8', '-h', jni / 'headers',
    '-d', jni / 'classes', *sorted((jdbc / 'src/main/java').rglob('*.java')))
run('cmake', '-S', jni, '-B', jni / 'build', '-DCMAKE_BUILD_TYPE=Release',
    '-DCMAKE_EXPORT_COMPILE_COMMANDS=ON', '-DCORE_BUILD=' + str(out), '-DJDBC_SOURCE=' + str(jdbc), env=environment)
run('cmake', '--build', jni / 'build', '-j', jobs)
library = 'libduckdb_java.so_linux_amd64'
shutil.copy2(jni / 'build' / library, out / library)
licenses = jni / 'licenses/META-INF/licenses'
licenses.mkdir(parents=True)
shutil.copy2(source / 'LICENSE', licenses / 'duckdb-LICENSE')
shutil.copy2(jdbc / 'LICENSE', licenses / 'duckdb-java-LICENSE')
run(java / 'bin/jar', '--create', '--file', out / 'duckdb_jdbc.jar', '-C', jni / 'classes', '.',
    '-C', jdbc, 'META-INF/services/java.sql.Driver', '-C', jni / 'licenses', 'META-INF', '-C', out, library)
if inputs() != expected or any(not (out / name).is_file() or digest(out / name) != sha for name, sha in prepared.items()):
    raise SystemExit('DuckDB build inputs changed during compilation')
artifacts = ['duckdb_host', 'src/libduckdb.so', 'duckdb', 'CMakeCache.txt', 'compile_commands.json',
             'compiler-version.txt', 'source-files.json', 'duckdb_jdbc.jar', library,
             'jdbc/build/CMakeCache.txt', 'jdbc/build/compile_commands.json']
(out / 'engine-build.json').write_text(json.dumps({'inputs': expected,
    'artifacts': {name: digest(out / name) for name in artifacts}}, indent=2) + '\n')
print(out)
PY
