import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class RocksDbBuildTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "third_party/rocksdb"
        self.build = self.root / "build/services/rocksdb"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        (self.root / "scripts").mkdir()
        shutil.copy(Path(__file__).resolve().parents[3] / "scripts/build-rocksdb.sh",
                    self.root / "scripts")
        (self.source / "build_tools").mkdir(parents=True)
        (self.source / "build_tools/version.sh").write_text("echo 9.1.1\n")
        (self.source / "upstream.cc").write_text("original\n")
        self.git(self.source, "init", "-q")
        self.git(self.source, "add", ".")
        self.git(self.source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "source")
        self.git(self.root, "init", "-q")
        self.git(self.root, "update-index", "--add", "--cacheinfo",
                 "160000," + self.git(self.source, "rev-parse", "HEAD") + ",third_party/rocksdb")
        native = self.root / "src/main/native/rocksdb"
        native.mkdir(parents=True)
        (native / "timestamps_jni.cc").write_text("bridge\n")
        (native / "rocksdb-9.1.1.patch").write_text(
            "--- a/upstream.cc\n+++ b/upstream.cc\n@@ -1 +1 @@\n-original\n+patched\n")
        (self.bin / "libcrypto.a").write_text("crypto\n")
        self.tool("pkg-config", '#!/bin/sh\nif [ "$1" = "--modversion" ]; then echo fixture; else dirname "$0"; fi\n')
        self.tool("jar", "#!/bin/sh\nexit 0\n")
        self.tool("mvn", "#!/bin/sh\necho 'Prebuilt Maven runtime requested' >&2\nexit 91\n")
        self.tool("c++", """#!/usr/bin/env python3
import pathlib, sys
if '--version' in sys.argv:
    print('fixture compiler')
else:
    pathlib.Path(sys.argv[sys.argv.index('-o') + 1]).write_text('bridge binary')
""")
        self.tool("cmake", """#!/usr/bin/env python3
import os, pathlib, sys
if '--build' not in sys.argv:
    out = pathlib.Path(sys.argv[sys.argv.index('-B') + 1])
    out.mkdir()
    (out / 'CMakeCache.txt').write_text('settings')
    (out / 'compile_commands.json').write_text('[]')
else:
    if os.environ.get('FAIL_BUILD'):
        sys.exit(75)
    out = pathlib.Path(sys.argv[2])
    (out / 'java').mkdir(exist_ok=True)
    (out / 'java/librocksdbjni-linux64.so').write_text('source runtime')
    (out / 'java/rocksdbjni-9.1.1-linux64.jar').write_text('source classes and runtime')
    if os.environ.get('CHANGE_SOURCE'):
        (out.parent / 'source/upstream.cc').write_text('changed during build')
""")

    @staticmethod
    def git(directory, *args):
        return subprocess.check_output(["git", "-C", str(directory), *args], text=True).strip()

    def tool(self, name, text):
        path = self.bin / name
        path.write_text(text)
        path.chmod(0o755)

    def run_build(self, **extra):
        return subprocess.run(["bash", str(self.root / "scripts/build-rocksdb.sh"),
                               "build/services/rocksdb"], cwd=self.root,
                              env={**os.environ, "PATH": str(self.bin) + ":" + os.environ["PATH"],
                                   "JAVA_HOME": str(self.root), "CXX": str(self.bin / "c++"), **extra},
                              capture_output=True, text=True, timeout=30)

    def test_build_uses_gitlink_without_cloud_or_maven_and_preserves_existing_output(self):
        result = self.run_build()
        self.assertEqual(0, result.returncode, result.stderr)
        receipt = json.loads((self.build / "engine-build.json").read_text())
        self.assertEqual(self.git(self.source, "rev-parse", "HEAD"), receipt["inputs"]["source_commit"])
        self.assertIn("rocksdbjni.jar", receipt["artifacts"])
        self.assertIn("cmake/compile_commands.json", receipt["artifacts"])
        self.assertEqual("original\n", (self.source / "upstream.cc").read_text())
        self.assertEqual("patched\n", (self.build / "source/upstream.cc").read_text())
        before = (self.build / "rocksdbjni.jar").read_bytes()
        self.assertNotEqual(0, self.run_build().returncode)
        self.assertEqual(before, (self.build / "rocksdbjni.jar").read_bytes())

    def test_dirty_source_is_rejected_before_creating_output(self):
        (self.source / "upstream.cc").write_text("modified")
        result = self.run_build()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("clean and at its gitlink", result.stderr)
        self.assertFalse(self.build.exists())

    def test_source_must_match_parent_gitlink(self):
        self.git(self.root, "update-index", "--cacheinfo",
                 "160000," + "0" * 39 + "1,third_party/rocksdb")
        result = self.run_build()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("clean and at its gitlink", result.stderr)
        self.assertFalse(self.build.exists())

    def test_failure_keeps_diagnostics_without_certification(self):
        result = self.run_build(FAIL_BUILD="1")
        self.assertNotEqual(0, result.returncode)
        self.assertTrue((self.build / "cmake/CMakeCache.txt").exists())
        self.assertFalse((self.build / "engine-build.json").exists())

    def test_source_change_during_build_cannot_be_certified(self):
        result = self.run_build(CHANGE_SOURCE="1")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("inputs changed during compilation", result.stderr)
        self.assertFalse((self.build / "engine-build.json").exists())


if __name__ == "__main__":
    unittest.main()
