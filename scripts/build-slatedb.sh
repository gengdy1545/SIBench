#!/usr/bin/env bash
# Build the SlateDB JNI byte store from the source gitlink and its owning Cargo lockfile.
set -euo pipefail
[ "$#" -eq 1 ] || { echo "usage: $0 build/index/<fresh-slatedb-directory>" >&2; exit 2; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
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
if out == root / 'build/index' or root / 'build/index' not in out.parents or out.exists():
    raise SystemExit('SlateDB build requires a fresh directory under build/index')
if platform.system() != 'Linux' or platform.machine() != 'x86_64':
    raise SystemExit('SlateDB JNI currently requires Linux x86-64')
jobs = os.environ.get('JOBS', '4')
if not jobs.isdigit() or int(jobs) < 1:
    raise SystemExit('JOBS must be a positive integer')
upstream = root / 'index/slatedb'
native = root / 'src/main/native/index/slatedb'

def git(path, *args):
    return subprocess.check_output(['git', '-C', str(path), *args], universal_newlines=True).strip()

def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()

def files(path):
    return {str(p.relative_to(path)): digest(p) for p in sorted(path.rglob('*')) if p.is_file()}

def inputs():
    pin = git(root, 'rev-parse', ':index/slatedb')
    if not re.fullmatch('[0-9a-f]{40}', pin) or git(upstream, 'rev-parse', 'HEAD') != pin or git(upstream, 'status', '--porcelain=v1', '--untracked-files=all'):
        raise SystemExit('SlateDB source must be clean and at its gitlink')
    return {'source_commit': pin, 'native_sources': files(native),
            'build_script_sha256': digest(root / 'scripts/build-slatedb.sh')}

expected = inputs()
toolchain = re.search(r'^channel\s*=\s*"([^"]+)"', (upstream / 'rust-toolchain.toml').read_text(), re.M).group(1)
version = re.search(r'\[workspace.package\]\s*\nversion\s*=\s*"([^"]+)"', (upstream / 'Cargo.toml').read_text()).group(1)
rustup = shutil.which('rustup')
if not rustup:
    raise SystemExit('rustup with the upstream toolchain is required; see index/slatedb/rust-toolchain.toml')
compiler = subprocess.check_output([rustup, 'run', toolchain, 'rustc', '--version', '--verbose'], universal_newlines=True)
out.mkdir(parents=True)
(out / 'engine-inputs.json').write_text(json.dumps(expected, indent=2) + '\n')
source = out / 'source/index/slatedb'
source.mkdir(parents=True)
archive = out / 'upstream.tar'
subprocess.run(['git', '-C', str(upstream), 'archive', '--output=' + str(archive), expected['source_commit']], check=True)
subprocess.run(['tar', '-xf', str(archive), '-C', str(source)], check=True)
archive.unlink()
project = out / 'source/src/main/native/index/slatedb'
shutil.copytree(native, project)
environment = {**os.environ, 'SIBENCH_SLATE_SOURCE': expected['source_commit'],
               'SIBENCH_SLATE_VERSION': version, 'CARGO_TARGET_DIR': str(out / 'target')}
command = [rustup, 'run', toolchain, 'cargo', 'build', '--release', '--locked',
           '--manifest-path', str(project / 'Cargo.toml'), '-j', jobs]
subprocess.run(command, env=environment, check=True)
test = [rustup, 'run', toolchain, 'cargo', 'test', '--release', '--locked',
        '--manifest-path', str(project / 'Cargo.toml'), '-j', jobs]
subprocess.run(test, env=environment, check=True)
library = out / 'libslatedb_jni.so'
shutil.copy2(out / 'target/release/libslatedb_jni.so', library)
if inputs() != expected or files(project) != expected['native_sources']:
    raise SystemExit('SlateDB JNI build inputs changed during compilation')
(out / 'compiler-version.txt').write_text(compiler)
receipt = {'inputs': expected, 'upstream_version': version, 'toolchain': toolchain,
           'build_command': command, 'test_command': test,
           'artifacts': {'libslatedb_jni.so': digest(library), 'compiler-version.txt': digest(out / 'compiler-version.txt')}}
(out / 'engine-build.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(out)
PY
