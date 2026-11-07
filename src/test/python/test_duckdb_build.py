import json
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
        jdbc = self.root / "third_party/duckdb-java"
        (jdbc / "src/main/java").mkdir(parents=True)
        (jdbc / "src/main/java/Driver.java").write_text("driver\n")
        for name in ("LICENSE",):
            (source / name).write_text("license\n")
            (jdbc / name).write_text("license\n")
        self.git(source, "add", ".")
        self.git(source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "license")
        self.git(source, "tag", "-f", "v2.0.0")
        self.git(self.root, "update-index", "--cacheinfo", f"160000,{self.git(source, 'rev-parse', 'HEAD')},third_party/duckdb")
        self.git(jdbc, "init", "-q")
        self.git(jdbc, "add", ".")
        self.git(jdbc, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "jdbc")
        self.git(jdbc, "tag", "v2.0.0.1")
        self.git(self.root, "update-index", "--add", "--cacheinfo",
                 f"160000,{self.git(jdbc, 'rev-parse', 'HEAD')},third_party/duckdb-java")
        (native / "duckdb-java-2.0.0.1.patch").write_text(
            "--- a/src/main/java/Driver.java\n+++ b/src/main/java/Driver.java\n@@ -1 +1 @@\n-driver\n+source driver\n")
        (native / "jdbc.cmake").write_text("jdbc build\n")
        self.executable("javac", "#!/bin/sh\nexit 0\n")
        self.executable("jar", '#!/bin/sh\necho jar > "$3"\n')
        self.executable("cmake", '''#!/bin/sh
if [ "$1" = --build ]; then
  [ "${FIXTURE_BUILD_EXIT:-0}" = 0 ] || exit "$FIXTURE_BUILD_EXIT"
  mkdir -p "$2/src"
  echo library > "$2/src/libduckdb.so"
  echo shell > "$2/duckdb"
  echo jdbc > "$2/libduckdb_java.so_linux_amd64"
  if [ "${FIXTURE_MUTATE:-}" = yes ]; then
    echo changed > "$FIXTURE_ROOT/build/services/custom/source/src/include/upstream.hpp"
  fi
else
  build=$4
  mkdir -p "$build"
  printf 'CMAKE_CXX_COMPILER:FILEPATH=%s/test bin/compiler\\n' "$FIXTURE_ROOT" > "$build/CMakeCache.txt"
  echo commands > "$build/compile_commands.json"
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
        if name in ("javac", "jar"):
            (self.root / "jdk/bin").mkdir(parents=True, exist_ok=True)
            (self.root / "jdk/bin" / name).symlink_to(path)

    @staticmethod
    def git(directory, *args):
        return subprocess.check_output(["git", "-C", str(directory), *args], text=True).strip()

    def run_build(self, exit_code=0, mutate=False):
        return subprocess.run(["bash", str(self.root / "scripts/build-duckdb.sh"), "build/services/custom"],
            env={**os.environ, "JAVA_HOME": str(self.root / "jdk"),
                 "PATH": f"{self.bin}:{os.environ['PATH']}", "FIXTURE_ROOT": str(self.root),
                 "FIXTURE_BUILD_EXIT": str(exit_code), "FIXTURE_MUTATE": "yes" if mutate else "no"}, capture_output=True, text=True, timeout=10)

    def test_build_uses_selected_path_and_preserves_source_and_existing_output(self):
        result = self.run_build()
        self.assertEqual(0, result.returncode, result.stderr)
        build = self.root / "build/services/custom"
        self.assertEqual("patched\n", (build / "source/src/include/upstream.hpp").read_text())
        self.assertEqual("upstream\n", (self.root / "third_party/duckdb/src/include/upstream.hpp").read_text())
        self.assertTrue((build / "duckdb_host").is_file())
        receipt = json.loads((build / "engine-build.json").read_text())
        self.assertIn("jdbc_source_commit", receipt["inputs"])
        self.assertIn("duckdb_jdbc.jar", receipt["artifacts"])
        self.assertEqual("host\n", (build / "duckdb_host.cpp").read_text())
        before = (build / "duckdb_host").read_bytes()
        result = self.run_build()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("requires a fresh directory", result.stderr)
        self.assertEqual(before, (build / "duckdb_host").read_bytes())

    def test_failed_build_keeps_diagnostics_without_a_host(self):
        result = self.run_build(75)
        self.assertNotEqual(0, result.returncode, result.stderr)
        self.assertIn("75", result.stderr)
        build = self.root / "build/services/custom"
        self.assertTrue((build / "CMakeCache.txt").is_file())
        self.assertFalse((build / "duckdb_host").exists())
        self.assertFalse((build / "engine-build.json").exists())

    def test_changed_prepared_input_cannot_be_certified(self):
        result = self.run_build(mutate=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("inputs changed during compilation", result.stderr)
        self.assertFalse((self.root / "build/services/custom/engine-build.json").exists())

    def test_dirty_jdbc_is_rejected_before_build(self):
        (self.root / "third_party/duckdb-java/src/main/java/Driver.java").write_text("changed\n")
        result = self.run_build()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("duckdb-java source must be clean", result.stderr)
        self.assertFalse((self.root / "build/services/custom").exists())


if __name__ == "__main__":
    unittest.main()
