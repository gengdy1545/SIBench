import json
import os
import shlex
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/native-capture.sh"


class NativeCaptureJavaTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.log = self.root / "java.log"
        self.env = dict(os.environ)
        self.env.pop("JAVA_HOME", None)
        self.env.pop("SIBENCH_JAVA_HOME", None)
        self.env["SIBENCH_TEST_JAVA_LOG"] = str(self.log)

    def jdk(self, name):
        home = self.root / name
        java = home / "bin/java"
        java.parent.mkdir(parents=True)
        # Exit at the first CLI call, before creating any capture or starting PG.
        java.write_text('#!/bin/sh\nprintf "%s" "$0" > "$SIBENCH_TEST_JAVA_LOG"\n'
                        'printf "exit 73\\n"\n')
        java.chmod(0o755)
        return home

    def check_java(self, home):
        result = subprocess.run(
            ["bash", str(SCRIPT), "__jdk_selection_test__.yaml"],
            env=self.env, capture_output=True, text=True, timeout=10)
        self.assertEqual(73, result.returncode, result.stderr)
        self.assertEqual(str(home / "bin/java"), self.log.read_text())

    def test_respects_java_home_with_spaces(self):
        home = self.jdk("configured jdk")
        self.env["JAVA_HOME"] = str(home)
        self.check_java(home)

    def test_capture_override_takes_precedence(self):
        self.env["JAVA_HOME"] = str(self.jdk("default jdk"))
        home = self.jdk("capture jdk")
        self.env["SIBENCH_JAVA_HOME"] = str(home)
        self.check_java(home)

    def test_cli_failure_is_preserved_before_any_capture_setup(self):
        home = self.jdk("failing jdk")
        (home / "bin/java").write_text('#!/bin/sh\necho "partial configuration"\nexit 76\n')
        self.env["JAVA_HOME"] = str(home)
        result = subprocess.run(["bash", str(SCRIPT), "__invalid_profile__.yaml"],
                                env=self.env, capture_output=True, text=True, timeout=10)
        self.assertEqual(76, result.returncode, result.stderr)

    def test_uses_path_without_java_home(self):
        home = self.jdk("path jdk")
        self.env["PATH"] = f"{home / 'bin'}:{self.env['PATH']}"
        self.check_java(home)


class NativeCapturePostgresTest(unittest.TestCase):
    def test_pg_ctl_preserves_config_path_through_its_posix_shell(self):
        for name in ("plain run", "owner's run", 'quotes\'"$(touch injected)`touch injected`\\\nrun'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)

                def executable(relative, contents):
                    path = root / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(textwrap.dedent(contents))
                    path.chmod(0o755)
                    return path

                script = root / "scripts/native-capture.sh"
                script.parent.mkdir()
                shutil.copyfile(SCRIPT, script)
                core = root / "build/sibench/sibench.jar"
                core.parent.mkdir(parents=True)
                core.touch()
                (root / "config").mkdir()
                (root / "config/sibench.yaml").touch()
                profile = root / "profile.yaml"
                profile.touch()
                artifacts = root / "results/poc" / name
                config = artifacts / "native/postgres.conf"
                paths = root / "paths.sh"
                paths.write_text("\n".join(key + "=" + shlex.quote(str(value)) for key, value in {
                    "SIBENCH_ARTIFACTS": artifacts,
                    "SIBENCH_RAW": artifacts / "raw",
                    "SIBENCH_CAPTURE_DETAIL": "full",
                    "SIBENCH_CAPTURE_COMPRESS": "none",
                    "SIBENCH_VERIFICATION": "full",
                    "SIBENCH_PORT": 5544,
                    "SIBENCH_DATABASE": "test",
                    "SIBENCH_BASE": "shell_test",
                }.items()))
                java = executable("jdk/bin/java", """\
                    #!/bin/sh
                    case "$4" in
                      paths) cat "$SIBENCH_TEST_PATHS" ;;
                      verify) ;;
                      postgres-config) touch "$6" ;;
                      *) exit 78 ;;
                    esac
                    """)
                pg = "build/services/postgresql-capture/runtime/bin/"
                executable(pg + "pg_isready", "#!/bin/sh\nexit 1\n")
                executable(pg + "initdb", '#!/bin/sh\nmkdir -p "$2"\n')
                # pg_ctl inserts post_opts into /bin/sh -c before starting postgres.
                executable(pg + "pg_ctl", """\
                    #!/usr/bin/env python3
                    import subprocess, sys
                    options = sys.argv[sys.argv.index("-o") + 1]
                    sys.exit(subprocess.run(["/bin/sh", "-c",
                        'exec "$SIBENCH_TEST_POSTGRES" ' + options]).returncode)
                    """)
                postgres = executable("postgres", """\
                    #!/usr/bin/env python3
                    import json, os, sys
                    with open(os.environ["SIBENCH_TEST_POSTGRES_LOG"], "w") as output:
                        json.dump(sys.argv[1:], output)
                    sys.exit(73)
                    """)
                log = root / "postgres.json"
                env = dict(os.environ, SIBENCH_JAVA_HOME=str(java.parent.parent),
                           SIBENCH_WORKDIR=str(root),
                           SIBENCH_TEST_PATHS=str(paths), SIBENCH_TEST_POSTGRES=str(postgres),
                           SIBENCH_TEST_POSTGRES_LOG=str(log))
                result = subprocess.run(["bash", str(script), str(profile)], env=env,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(73, result.returncode, result.stderr)
                self.assertEqual(["-c", "config_file=" + str(config), "-c", "port=5544",
                                  "-c", "unix_socket_directories="], json.loads(log.read_text()))
                self.assertTrue(config.is_file())
                self.assertFalse((root / "injected").exists())
                self.assertFalse((root / "build/services/postgresql-capture/data").exists())


if __name__ == "__main__":
    unittest.main()
