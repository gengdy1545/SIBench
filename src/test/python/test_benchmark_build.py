import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[3] / "scripts/build-benchmarks.sh"


class BenchmarkBuildTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "scripts").mkdir()
        shutil.copyfile(SCRIPT, self.root / "scripts" / SCRIPT.name)
        driver = self.root / "src/main/native/sysbench/drv_duckdb.cc"
        driver.parent.mkdir(parents=True)
        driver.write_text("driver source\n")
        driver.with_name("retryable_error.hpp").write_text("retry policy\n")
        (self.root / "scripts/patches").mkdir()
        self.source = self.root / "benchmark/source"
        self.source.mkdir(parents=True)
        for name in ("autogen.sh", "configure"):
            path = self.source / name
            path.write_text("#!/bin/sh\nexit 0\n")
            path.chmod(0o755)
        (self.source / "source.c").write_text("pinned input\n")
        (self.root / "scripts/patches/native-stop-fixture.patch").write_text(
            "--- a/source.c\n+++ b/source.c\n@@ -1 +1 @@\n-pinned input\n+patched input\n")
        shutil.copyfile(SCRIPT.parent.parent / "benchmark/HyBench-2024/pom.xml", self.source / "pom.xml")
        self.git("init", "-q")
        self.git("add", ".")
        self.git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "pinned")
        self.jdk = self.root / "test jdk"
        (self.jdk / "bin").mkdir(parents=True)
        java = self.jdk / "bin/java"
        java.write_text('''#!/bin/sh
case "$*" in
  *" paths "*)
    printf "BENCHMARK='%s'\\nSOURCE='%s/benchmark/source'\\nBUILD='%s/build/benchmarks/client'\\n" \
      "$SIBENCH_FIXTURE_BENCHMARK" "$SIBENCH_FIXTURE_ROOT" "$SIBENCH_FIXTURE_ROOT"
    printf "NATIVE_STOP_PATCH='%s/scripts/patches/native-stop-fixture.patch'\\n" "$SIBENCH_FIXTURE_ROOT" ;;
  *" record "*) echo recorded > "$SIBENCH_FIXTURE_ROOT/build/benchmarks/client/native-build.json" ;;
  *) exit 74 ;;
esac
''')
        java.chmod(0o755)
        for command in ("mvn", "make", "javac", "jar"):
            executable = self.jdk / "bin" / command
            executable.write_text('''#!/bin/sh
printf "%s %s\\n" "$PWD" "$*" >> "$SIBENCH_FIXTURE_ROOT/build.log"
[ "${SIBENCH_FIXTURE_EXIT:-0}" = 0 ] || exit "$SIBENCH_FIXTURE_EXIT"
case "$*" in
  *"-pl core"*)
    target=$SIBENCH_FIXTURE_ROOT/build/benchmarks/client/core/target
    mkdir -p "$target/dependency"
    echo jar > "$target/core-fixture.jar"
    echo dependency > "$target/dependency/runtime.jar"
    ;;
  *"--create --file"*) echo binding > "$3" ;;
esac
''')
            executable.chmod(0o755)

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.source), *args], text=True).strip()

    def run_build(self, benchmark, exit_code=0):
        return subprocess.run(["bash", str(self.root / "scripts" / SCRIPT.name), "fixture.yaml"],
            env={**os.environ, "JAVA_HOME": str(self.jdk), "PATH": f"{self.jdk}/bin:{os.environ['PATH']}",
                 "SIBENCH_FIXTURE_ROOT": str(self.root), "SIBENCH_FIXTURE_BENCHMARK": benchmark,
                 "SIBENCH_FIXTURE_EXIT": str(exit_code)}, capture_output=True, text=True, timeout=10)

    def test_builds_each_client_in_a_copy_and_records_only_success(self):
        build = self.root / "build/benchmarks/client"
        for benchmark in ("hybench", "ycsb", "sysbench"):
            with self.subTest(benchmark=benchmark):
                result = self.run_build(benchmark)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertEqual("patched input\n", (build / "source.c").read_text())
                self.assertTrue((build / "native-build.json").is_file())
                if benchmark == "ycsb":
                    self.assertTrue((build / "sibench-api/lib/sibench-ycsb.jar").is_file())
                    self.assertTrue((build / "sibench-api/lib/core-fixture.jar").is_file())
                    self.assertIn("-pl core -am -Psource-run", (self.root / "build.log").read_text())
                self.assertEqual("", self.git("status", "--porcelain"))
                shutil.rmtree(build)
        result = self.run_build("ycsb", 75)
        self.assertEqual(75, result.returncode, result.stderr)
        self.assertFalse((build / "native-build.json").exists())

    def test_cli_failure_does_not_create_a_build(self):
        (self.jdk / "bin/java").write_text('#!/bin/sh\nexit 76\n')
        result = self.run_build("ycsb")
        self.assertEqual(76, result.returncode, result.stderr)
        self.assertFalse((self.root / "build").exists())

    def test_existing_build_is_never_overwritten(self):
        build = self.root / "build/benchmarks/client"
        build.mkdir(parents=True)
        (build / "keep").write_text("existing evidence")
        result = self.run_build("ycsb")
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertEqual("existing evidence", (build / "keep").read_text())
        self.assertFalse((self.root / "build.log").exists())


if __name__ == "__main__":
    unittest.main()
