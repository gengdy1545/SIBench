import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/build-duckdb.sh"


class DuckDbBuildTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "scripts").mkdir()
        shutil.copyfile(SCRIPT, self.root / "scripts/build-duckdb.sh")
        source = self.root / "third_party/duckdb"
        (source / "src/include").mkdir(parents=True)
        (source / "src/include/upstream.hpp").write_text("upstream\n")
        self.git(source, "init", "-q")
        self.git(source, "add", ".")
        self.git(source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "source")
        self.git(source, "tag", "v2.0.0")
        pin = self.git(source, "rev-parse", "HEAD")
        self.git(self.root, "init", "-q")
        self.git(self.root, "update-index", "--add", "--cacheinfo", f"160000,{pin},third_party/duckdb")
        native = self.root / "src/main/native/duckdb"
        native.mkdir(parents=True)
        (native / "duckdb-2.0.0.patch").write_text(
            "--- a/src/include/upstream.hpp\n+++ b/src/include/upstream.hpp\n@@ -1 +1 @@\n-upstream\n+patched\n")
        (native / "capture.hpp").write_text("capture\n")
        (native / "duckdb_host.cpp").write_text("host\n")
        self.bin = self.root / "test bin"
        self.bin.mkdir()
        self.executable("java", '''#!/bin/sh
case "$*" in
  *"engine-path"*) printf "BUILD='%s/build/services/custom'\\n" "$FIXTURE_ROOT" ;;
  *"engine-inputs"*) echo inputs > "$FIXTURE_ROOT/build/services/custom/engine-inputs.json" ;;
  *"engine-prepared"*) echo sources > "$FIXTURE_ROOT/build/services/custom/source-files.json" ;;
  *"engine-record"*)
    test -f "$FIXTURE_ROOT/build/services/custom/duckdb_host" || exit 73
    echo receipt > "$FIXTURE_ROOT/build/services/custom/engine-build.json" ;;
  *) exit 74 ;;
esac
''')
        self.executable("cmake", '''#!/bin/sh
if [ "$1" = --build ]; then
  [ "${FIXTURE_BUILD_EXIT:-0}" = 0 ] || exit "$FIXTURE_BUILD_EXIT"
  mkdir -p "$2/src"
  echo library > "$2/src/libduckdb.so"
  echo shell > "$2/duckdb"
else
  build=$FIXTURE_ROOT/build/services/custom
  test -f "$build/source-files.json" || exit 72
  printf 'CMAKE_CXX_COMPILER:FILEPATH=%s/test bin/compiler\\n' "$FIXTURE_ROOT" > "$build/CMakeCache.txt"
fi
''')
        self.executable("compiler", '''#!/bin/sh
[ "$1" != --version ] || { echo test-compiler; exit 0; }
for output do :; done
echo host > "$output"
''')

    def executable(self, name, contents):
        path = self.bin / name
        path.write_text(contents)
        path.chmod(0o755)

    @staticmethod
    def git(directory, *args):
        return subprocess.check_output(["git", "-C", str(directory), *args], text=True).strip()

    def run_build(self, exit_code=0):
        return subprocess.run(["bash", str(self.root / "scripts/build-duckdb.sh"), "runtime.yaml"],
            env={**os.environ, "JAVA_HOME": "",
                 "PATH": f"{self.bin}:{os.environ['PATH']}", "FIXTURE_ROOT": str(self.root),
                 "FIXTURE_BUILD_EXIT": str(exit_code)}, capture_output=True, text=True, timeout=10)

    def test_build_uses_configured_path_and_preserves_source_and_existing_output(self):
        result = self.run_build()
        self.assertEqual(0, result.returncode, result.stderr)
        build = self.root / "build/services/custom"
        self.assertEqual("patched\n", (build / "source/src/include/upstream.hpp").read_text())
        self.assertEqual("upstream\n", (self.root / "third_party/duckdb/src/include/upstream.hpp").read_text())
        self.assertTrue((build / "duckdb_host").is_file())
        self.assertTrue((build / "engine-build.json").is_file())
        self.assertEqual("host\n", (build / "duckdb_host.cpp").read_text())
        before = (build / "duckdb_host").read_bytes()
        result = self.run_build()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("build already exists", result.stderr)
        self.assertEqual(before, (build / "duckdb_host").read_bytes())

    def test_failed_build_keeps_diagnostics_without_a_host(self):
        result = self.run_build(75)
        self.assertEqual(75, result.returncode, result.stderr)
        build = self.root / "build/services/custom"
        self.assertTrue((build / "CMakeCache.txt").is_file())
        self.assertFalse((build / "duckdb_host").exists())
        self.assertFalse((build / "engine-build.json").exists())


if __name__ == "__main__":
    unittest.main()
