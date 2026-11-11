#!/usr/bin/env python3
"""Install a checked Trino distribution, source-built Pixels connector and isolated JDK."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import zipfile


def digest(path, algorithm="sha256"):
    value = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def install(connector, archive, central_checksum, java, destination):
    version = subprocess.run([str(java / "bin/java"), "-version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True).stdout.decode()
    if '"23.' not in version:
        raise ValueError("Trino requires a JDK 23 runtime")
    receipt = json.loads((connector / "java-build.json").read_text())
    for name, expected in receipt["files"].items():
        if digest(connector / name) != expected:
            raise ValueError("Connector build file changed: " + name)
    expected = central_checksum.read_text().strip()
    if len(expected) != 40 or digest(archive, "sha1") != expected:
        raise ValueError("Distribution differs from its Maven Central SHA-1 checksum")
    destination.mkdir(parents=True, exist_ok=False)
    with tarfile.open(str(archive)) as package:
        prefix = "trino-server-" + receipt["trino_version"]
        selected = []
        for member in package.getmembers():
            parts = Path(member.name).parts
            if not parts or parts[0] != prefix or ".." in parts or Path(member.name).is_absolute():
                raise ValueError("Unsafe distribution member: " + member.name)
            if len(parts) > 1 and parts[1] == "plugin":
                continue
            if member.issym() or member.islnk():
                raise ValueError("Unexpected distribution link: " + member.name)
            selected.append(member)
        package.extractall(str(destination), members=selected)
    server = destination / prefix
    # Only this connector is used; other bundled plugins remain recoverable from the archive.
    (server / "plugin").mkdir()
    with zipfile.ZipFile(str(connector / "artifacts/connector.zip")) as package:
        package.extractall(str(server / "plugin"))
    copied_java = destination / "tools/java"
    shutil.copytree(str(java), str(copied_java), symlinks=True)
    files = {str(p.relative_to(destination)): digest(p)
             for directory in [server / "bin", server / "lib", server / "plugin", copied_java]
             for p in sorted(directory.rglob("*")) if p.is_file()}
    manifest = {key: receipt[key] for key in ["pixels_commit", "connector_commit", "pixels_patch_sha256", "connector_patch_sha256", "trino_version"]}
    manifest.update(server_directory=prefix, java_home="tools/java", files=files,
                    connector_build=receipt, distribution_sha1=expected,
                    distribution_sha256=digest(archive))
    (destination / "deployment.json").write_text(json.dumps(manifest, indent=2))
    print(destination)


if __name__ == "__main__":
    if len(sys.argv) != 6:
        sys.exit("usage: install_trino.py CONNECTOR_BUILD SERVER_TAR_GZ CENTRAL_SHA1_FILE JDK23 FRESH_DESTINATION")
    install(*(Path(value).absolute() for value in sys.argv[1:]))
