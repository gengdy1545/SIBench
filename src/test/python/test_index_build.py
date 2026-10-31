import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/build-rocksdb-cloud.sh"


class IndexBuildTest(unittest.TestCase):
    def git(self, directory, *args):
        return subprocess.check_output(
            ["git", "-C", str(directory), *args], text=True, stderr=subprocess.PIPE).strip()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.git(self.root, "init", "-q")
        (self.root / "scripts").mkdir()
        shutil.copyfile(SCRIPT, self.root / "scripts" / SCRIPT.name)
        native = self.root / "src/main/native/index/rocksdb_cloud"
        native.mkdir(parents=True)
        shutil.copyfile(SCRIPT.parent.parent / "src/main/native/index/rocksdb_cloud/rocksdb-cloud-9.1.1.patch",
                        native / "rocksdb-cloud-9.1.1.patch")
        self.sources = [self.root / "third_party/aws-sdk-cpp", self.root / "index/rocksdb-cloud"]
        self.pins = {}
        for source in self.sources:
            source.mkdir(parents=True)
            self.git(source, "init", "-q")
            (source / "source.txt").write_text("pinned source\n")
            if source.name == "rocksdb-cloud":
                (source / "build_tools").mkdir()
                shutil.copyfile(SCRIPT.parent.parent / "index/rocksdb-cloud/build_tools/build_detect_platform",
                                source / "build_tools/build_detect_platform")
            self.git(source, "add", ".")
            self.git(source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                     "-c", "commit.gpgsign=false", "commit", "-qm", "pinned source")
            self.pins[source] = self.git(source, "rev-parse", "HEAD")
            self.git(self.root, "update-index", "--add", "--cacheinfo",
                     f"160000,{self.pins[source]},{source.relative_to(self.root)}")
        (self.root / "bin").mkdir()
        yum = self.root / "bin/yum"
        yum.write_text('#!/bin/sh\necho "dependency installation reached"\nexit 73\n')
        yum.chmod(0o755)

    def run_build(self):
        return subprocess.run(
            ["bash", str(self.root / "scripts" / SCRIPT.name)],
            env={**os.environ, "PATH": f"{self.root / 'bin'}:{os.environ['PATH']}"},
            capture_output=True, text=True)

    def assert_rejected(self, source):
        result = self.run_build()
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertIn(str(source.relative_to(self.root)), result.stderr)
        self.assertNotIn("dependency installation reached", result.stdout)

    def test_clean_pinned_sources_reach_dependency_installation(self):
        result = self.run_build()
        self.assertEqual(73, result.returncode, result.stderr)
        self.assertIn("dependency installation reached", result.stdout)

    def test_generated_files_stay_under_build_index(self):
        (self.root / "src/main/native/index/rocksdb_cloud").mkdir(parents=True, exist_ok=True)
        for command in ("yum", "cmake", "make", "strip"):
            executable = self.root / "bin" / command
            executable.write_text('#!/bin/sh\nprintf "%s %s %s\\n" "$PWD" "$0" "$* SDK=${AWS_SDK:-}" >> "' +
                                  str(self.root / "build.log") + '"\n'
                                  'for arg in "$@"; do case "$arg" in OUT=*) '
                                  'printf "native fixture" > "${arg#OUT=}";; esac; done\n')
            executable.chmod(0o755)
        result = self.run_build()
        self.assertEqual(0, result.returncode, result.stderr)
        log = (self.root / "build.log").read_text()
        self.assertIn(f"-DCMAKE_INSTALL_PREFIX={self.root}/build/index/aws-sdk", log)
        self.assertIn(f"{self.root}/build/index/rocksdb-cloud", log)
        self.assertNotIn("-DCMAKE_INSTALL_PREFIX=/usr/local", log)
        self.assertIn(f"SDK={self.root}/build/index/aws-sdk", log)
        self.assertTrue((self.root / "build/index/librocksdb_cloud_jni.so").is_file())
        for source in self.sources:
            self.assertEqual("", self.git(source, "status", "--porcelain"))

    def test_missing_sources_are_rejected(self):
        for source in self.sources:
            with self.subTest(source=source.name):
                hidden = source.with_suffix(".hidden")
                source.rename(hidden)
                self.assert_rejected(source)
                source.mkdir()
                self.assert_rejected(source)
                source.rmdir()
                hidden.rename(source)

    def test_wrong_commits_are_rejected(self):
        for source in self.sources:
            with self.subTest(source=source.name):
                self.git(source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-qm", "wrong commit")
                self.assert_rejected(source)
                self.git(source, "checkout", "-q", self.pins[source])

    def test_dirty_sources_are_rejected(self):
        for source in self.sources:
            for state in ("unstaged", "staged", "untracked"):
                with self.subTest(source=source.name, state=state):
                    path = source / ("extra.txt" if state == "untracked" else "source.txt")
                    path.write_text("local change\n")
                    if state == "staged":
                        self.git(source, "add", "source.txt")
                    self.assert_rejected(source)
                    if state == "untracked":
                        path.unlink()
                    else:
                        self.git(source, "reset", "--hard", "HEAD")


if __name__ == "__main__":
    unittest.main()
