import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class PixelsBuildTest(unittest.TestCase):
    """build-pixels.sh with fixture sources and fake toolchains; real builds are recorded separately."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.build = self.root / "build/services/pixels"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        (self.root / "scripts").mkdir()
        shutil.copy(Path(__file__).resolve().parents[3] / "scripts/build-pixels.sh", self.root / "scripts")
        self.git(self.root, "init", "-q")
        self.pins = {
            "third_party/pixels": {"pixels-common/src/main/resources/pixels.properties":
                                   "retina.tile.visibility.capacity=256\nretina.enable=false\n",
                                   "cpp/pixels-retina/CMakeLists.txt": "fixture\n",
                                   "scripts/etc/coordinator-jvm.config": "-Xmx1g\n",
                                   "scripts/etc/retina-jvm.config": "-Xmx1g\n"},
            "third_party/pixels-sink": {"src/main/resources/pixels-sink.properties": "sink.mode=none\n",
                                        "App.java": "original\n"},
            "third_party/postgresql": {"configure": "#!/bin/sh\necho \"$1\" > prefix\n"},
        }
        for path, files in self.pins.items():
            source = self.root / path
            for name, text in files.items():
                (source / name).parent.mkdir(parents=True, exist_ok=True)
                (source / name).write_text(text)
            if (source / "configure").exists():
                (source / "configure").chmod(0o755)
            self.git(source, "init", "-q")
            self.git(source, "add", ".")
            self.git(source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                     "-c", "commit.gpgsign=false", "commit", "-qm", "source")
            self.git(self.root, "update-index", "--add", "--cacheinfo",
                     "160000," + self.git(source, "rev-parse", "HEAD") + "," + path)
        sink_patch = self.root / "src/main/native/pixels-sink/pixels-sink-0.2.0-SNAPSHOT.patch"
        sink_patch.parent.mkdir(parents=True)
        sink_patch.write_text("--- a/App.java\n+++ b/App.java\n@@ -1 +1 @@\n-original\n+patched\n")
        pixels_patch = self.root / "src/main/native/pixels/pixels-0.2.0-SNAPSHOT.patch"
        pixels_patch.parent.mkdir(parents=True, exist_ok=True)
        pixels_patch.write_text("--- a/Load.java\n+++ b/Load.java\n@@ -1 +1 @@\n-original\n+patched\n")
        pixels_load = self.root / "third_party/pixels/Load.java"
        pixels_load.write_text("original\n")
        self.git(pixels_load.parent, "add", ".")
        self.git(pixels_load.parent, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-qm", "load")
        self.git(self.root, "update-index", "--cacheinfo", "160000," + self.git(pixels_load.parent, "rev-parse", "HEAD") + ",third_party/pixels")
        tracer = self.root / "src/main/native/pixels/TracingIndex.java"
        tracer.parent.mkdir(parents=True, exist_ok=True)
        tracer.write_text("tracer\n")
        self.tool("mvn", """#!/usr/bin/env python3
import pathlib
cwd = pathlib.Path.cwd()
jars = ['pixels-daemon/target/pixels-daemon-0.2.0-SNAPSHOT-full.jar', 'pixels-cli/target/pixels-cli-0.2.0-SNAPSHOT-full.jar'] \\
    if cwd.name == 'pixels' else ['target/pixels-sink-0.2.0-SNAPSHOT-full.jar']
for jar in jars:
    (cwd / jar).parent.mkdir(parents=True, exist_ok=True)
    (cwd / jar).write_text(jar + (cwd / 'App.java').read_text() if (cwd / 'App.java').exists() else jar)
""")
        self.tool("javac", "#!/bin/sh\nexit 0\n")
        self.tool("jar", "#!/bin/sh\necho tracer > \"$2\"\n")
        self.tool("java", "#!/bin/sh\necho 'fixture java' >&2\n")
        self.tool("cmake", "#!/bin/sh\necho fixture cmake\n")
        self.tool("make", """#!/bin/sh
if [ "$1" = "-C" ]; then mkdir -p "$PIXELS_HOME/lib"; echo lib > "$PIXELS_HOME/lib/libpixels-retina.so"; echo lib > "$PIXELS_HOME/lib/libjemalloc.so"; fi
if [ "$1" = "install" ]; then p=$(sed 's/--prefix=//' prefix); mkdir -p "$p/bin"; echo postgres > "$p/bin/postgres"; fi
""")
        self.tool("curl", "#!/usr/bin/env python3\nimport sys\nopen(sys.argv[sys.argv.index('-o') + 1], 'w').write('not the release')\n")

    @staticmethod
    def git(directory, *args):
        return subprocess.check_output(["git", "-C", str(directory), *args], text=True).strip()

    def tool(self, name, text):
        path = self.bin / name
        path.write_text(text)
        path.chmod(0o755)

    def run_build(self, phase):
        return subprocess.run(["bash", str(self.root / "scripts/build-pixels.sh"), phase, "build/services/pixels"],
                              cwd=self.root, env={**os.environ, "PATH": str(self.bin) + ":" + os.environ["PATH"]},
                              capture_output=True, text=True, timeout=60)

    def test_java_build_uses_gitlinks_patches_a_copy_and_keeps_existing_output(self):
        # Uncommitted upstream edits are not built: sources come from the pinned commits.
        (self.root / "third_party/pixels/pixels-common/src/main/resources/pixels.properties").write_text("edited\n")
        result = self.run_build("java")
        self.assertEqual(0, result.returncode, result.stderr)
        identity = json.loads((self.build / "java-build.json").read_text())
        self.assertEqual(self.git(self.root / "third_party/pixels", "rev-parse", "HEAD"), identity["pixels_commit"])
        self.assertEqual(self.git(self.root / "third_party/pixels-sink", "rev-parse", "HEAD"), identity["pixels_sink_commit"])
        self.assertEqual({"pixels-cli.jar", "pixels-daemon.jar", "pixels-sink.jar", "sibench-pixels.jar"}, set(identity["jars"]))
        self.assertIn("retina.tile.visibility.capacity=256", (self.build / "defaults/pixels.properties").read_text())
        self.assertEqual("patched\n", (self.build / "source/pixels-sink/App.java").read_text())
        self.assertEqual("original\n", (self.root / "third_party/pixels-sink/App.java").read_text())
        self.assertEqual("patched\n", (self.build / "source/pixels/Load.java").read_text())
        self.assertEqual("original\n", (self.root / "third_party/pixels/Load.java").read_text())
        before = (self.build / "java-build.json").read_bytes()
        self.assertNotEqual(0, self.run_build("java").returncode)
        self.assertEqual(before, (self.build / "java-build.json").read_bytes())

    def test_checkout_off_its_gitlink_is_rejected_before_creating_output(self):
        self.git(self.root, "update-index", "--cacheinfo", "160000," + "0" * 39 + "1,third_party/pixels")
        result = self.run_build("java")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("at its repository gitlink", result.stderr)
        self.assertFalse(self.build.exists())

    def test_native_phase_needs_matching_java_build_and_verified_etcd(self):
        self.assertIn("needs a completed java phase", self.run_build("native").stderr)
        self.assertEqual(0, self.run_build("java").returncode)
        java = self.build / "java-build.json"
        original = java.read_text()
        java.write_text(original.replace(json.loads(original)["pixels_commit"], "0" * 40))
        self.assertIn("different Pixels sources", self.run_build("native").stderr)
        java.write_text(original)
        result = self.run_build("native")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("etcd release checksum mismatch", result.stderr)
        self.assertFalse((self.build / "native-build.json").exists())
        self.assertFalse((self.build / "etcd/etcd").exists())

    def test_native_phase_rejects_changed_source_and_capacity_before_compilation(self):
        self.assertEqual(0, self.run_build("java").returncode)
        source = self.build / "source/pixels/cpp/pixels-retina/CMakeLists.txt"
        original = source.read_text()
        source.write_text("changed\n")
        self.assertIn("differs from its pin", self.run_build("native").stderr)
        self.assertFalse((self.build / "native-retina.log").exists())
        source.write_text(original)
        (self.build / "defaults/pixels.properties").write_text("retina.tile.visibility.capacity=512\n")
        self.assertIn("capacity/defaults differ", self.run_build("native").stderr)
        self.assertFalse((self.build / "native-retina.log").exists())

    def test_java_receipt_never_claims_inputs_changed_during_compilation(self):
        tool = self.bin / "mvn"
        tool.write_text(tool.read_text() + "\nfrom pathlib import Path\nPath(" + repr(str(self.root /
            "src/main/native/pixels-sink/pixels-sink-0.2.0-SNAPSHOT.patch")) + ").write_text('changed during build')\n")
        result = self.run_build("java")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("inputs changed during compilation", result.stderr)
        self.assertFalse((self.build / "java-build.json").exists())


if __name__ == "__main__":
    unittest.main()
