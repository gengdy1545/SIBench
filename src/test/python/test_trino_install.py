"""Trust-boundary checks for the offline Trino deployment installer."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("trino_install", ROOT / "scripts/install_trino.py")
INSTALLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INSTALLER)


class TrinoInstallTest(unittest.TestCase):
    def test_distribution_identity_and_path_escape_are_checked_before_publication(self):
        for member, mutate in [("trino-server-466/bin/launcher", False), ("../escape", False), ("trino-server-466/bin/launcher", True)]:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                build = root / "build"
                (build / "artifacts").mkdir(parents=True)
                plugin = build / "artifacts/connector.zip"
                with zipfile.ZipFile(plugin, "w") as package:
                    package.writestr("pixels/connector.jar", b"connector")
                (build / "java-build.json").write_text(json.dumps({"trino_version": "466", "pixels_commit": "pixels", "connector_commit": "connector", "pixels_patch_sha256": "patch", "connector_patch_sha256": "connector-patch",
                    "files": {"artifacts/connector.zip": hashlib.sha256(plugin.read_bytes()).hexdigest()}}))
                java = root / "java"
                (java / "bin").mkdir(parents=True)
                (java / "bin/java").write_bytes(b"java")
                archive = root / "server.tar.gz"
                with tarfile.open(archive, "w:gz") as package:
                    entry = tarfile.TarInfo(member);entry.size = 8;package.addfile(entry, io.BytesIO(b"launcher"))
                    directory = tarfile.TarInfo("trino-server-466/plugin");directory.type = tarfile.DIRTYPE;package.addfile(directory)
                checksum = root / "server.sha1"
                checksum.write_text("0" * 40 if mutate else hashlib.sha1(archive.read_bytes()).hexdigest())
                destination = root / "deployed"
                with patch.object(INSTALLER.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, b'openjdk version "23.0.2"')):
                    if ".." in member or mutate:
                        with self.assertRaises(ValueError): INSTALLER.install(build, archive, checksum, java, destination)
                        self.assertFalse((destination / "deployment.json").exists())
                        self.assertFalse((root / "escape").exists())
                    else:
                        INSTALLER.install(build, archive, checksum, java, destination)
                        receipt = json.loads((destination / "deployment.json").read_text())
                        self.assertIn("trino-server-466/plugin/pixels/connector.jar", receipt["files"])


if __name__ == "__main__":
    unittest.main()
