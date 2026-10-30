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
        self.sources = [self.root / "third_party/aws-sdk-cpp", self.root / "index/rocksdb-cloud"]
        self.pins = {}
        for source in self.sources:
            source.mkdir(parents=True)
            self.git(source, "init", "-q")
            (source / "source.txt").write_text("pinned source\n")
            self.git(source, "add", "source.txt")
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
