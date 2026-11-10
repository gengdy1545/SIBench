import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class CloudBuildTest(unittest.TestCase):
    """Real git archives with fixture sources and fake compiler/package tools; no services run."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        (self.root / "scripts").mkdir()
        shutil.copy(Path(__file__).resolve().parents[3] / "scripts/build-rocksdb-cloud.sh", self.root / "scripts")
        self.git(self.root, "init", "-q")
        for name, text in [("index/rocksdb-cloud", "pinned cloud\n"), ("third_party/aws-sdk-cpp", "pinned aws\n")]:
            source = self.root / name
            source.mkdir(parents=True)
            (source / "source.txt").write_text(text)
            self.git(source, "init", "-q")
            self.git(source, "add", ".")
            self.git(source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                     "-c", "commit.gpgsign=false", "commit", "-qm", "pin")
            self.git(self.root, "update-index", "--add", "--cacheinfo",
                     "160000," + self.git(source, "rev-parse", "HEAD") + "," + name)
        native = self.root / "src/main/native/index/rocksdb_cloud"
        native.mkdir(parents=True)
        for name in ["Makefile", "rocksdb_cloud_jni.cc", "jni_exports.map"]:
            (native / name).write_text("fixture\n")
        (native / "rocksdb-cloud-9.1.1.patch").write_text(
            "--- a/source.txt\n+++ b/source.txt\n@@ -1 +1 @@\n-pinned cloud\n+patched cloud\n")
        self.tool("yum", r"""#!/usr/bin/env python3
import os, pathlib
root = pathlib.Path(os.environ['FIXTURE_ROOT'])
(root / 'yum-ran').touch()
if os.environ.get('MUTATE_UPSTREAM'):
    (root / 'third_party/aws-sdk-cpp/source.txt').write_text('concurrent working edit\n')
""")
        self.tool("cmake", r"""#!/usr/bin/env python3
import pathlib, sys
assert (pathlib.Path(sys.argv[-1]) / 'source.txt').read_text() == 'pinned aws\n', 'compiled mutable AWS working tree'
""")
        self.tool("make", """#!/usr/bin/env python3
import os, pathlib, sys
root = pathlib.Path(os.environ['FIXTURE_ROOT']); cwd = pathlib.Path.cwd()
if cwd.name == 'aws-sdk-cpp' and 'install' in sys.argv:
    lib = root / 'build/index/aws-sdk/lib/libaws-cpp-sdk-core.a'
    lib.parent.mkdir(parents=True); lib.write_text('aws archive')
elif cwd.name == 'rocksdb-cloud':
    (cwd / 'librocksdb.a').write_text('cloud archive')
elif cwd.name == 'rocksdb_cloud':
    out = pathlib.Path(next(arg[4:] for arg in sys.argv if arg.startswith('OUT=')))
    out.write_text('native library')
    if os.environ.get('MUTATE_NATIVE'):
        (cwd / 'rocksdb_cloud_jni.cc').write_text('changed during compilation')
""")
        self.tool("strip", "#!/bin/sh\nexit 0\n")
        self.tool("g++", "#!/bin/sh\necho fixture compiler\n")

    @staticmethod
    def git(directory, *args):
        return subprocess.check_output(["git", "-C", str(directory), *args], text=True).strip()

    def tool(self, name, text):
        file = self.bin / name
        file.write_text(text)
        file.chmod(0o755)

    def run_build(self, **variables):
        return subprocess.run(["bash", str(self.root / "scripts/build-rocksdb-cloud.sh")], cwd=self.root,
                              env={**os.environ, "PATH": str(self.bin) + ":" + os.environ["PATH"],
                                   "FIXTURE_ROOT": str(self.root), "CXX": "g++", **variables},
                              capture_output=True, text=True, timeout=60)

    def test_archived_pins_survive_concurrent_upstream_edits_and_bind_artifacts(self):
        result = self.run_build(MUTATE_UPSTREAM="1")
        self.assertEqual(0, result.returncode, result.stderr)
        build = self.root / "build/index"
        self.assertEqual("patched cloud\n", (build / "rocksdb-cloud/source.txt").read_text())
        self.assertEqual("pinned aws\n", (build / "aws-sdk-cpp/source/source.txt").read_text())
        receipt = json.loads((build / "rocksdb-cloud-build.json").read_text())
        self.assertEqual(self.git(self.root / "index/rocksdb-cloud", "rev-parse", "HEAD"), receipt["inputs"]["rocksdb_cloud_commit"])
        self.assertIn("build/index/librocksdb_cloud_jni.so", receipt["artifacts"])

    def test_existing_library_is_preserved_before_installing_packages(self):
        library = self.root / "build/index/librocksdb_cloud_jni.so"
        library.parent.mkdir(parents=True)
        library.write_text("existing binary")
        self.assertNotEqual(0, self.run_build().returncode)
        self.assertEqual("existing binary", library.read_text())
        self.assertFalse((self.root / "yum-ran").exists())

    def test_input_changes_during_compilation_cannot_get_a_receipt(self):
        result = self.run_build(MUTATE_NATIVE="1")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("inputs changed during compilation", result.stderr)
        self.assertFalse((self.root / "build/index/rocksdb-cloud-build.json").exists())


if __name__ == "__main__":
    unittest.main()
