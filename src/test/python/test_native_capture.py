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
                executable(pg + "postgres", '#!/bin/sh\nprintf "4096\\n"\n')
                # pg_ctl inserts post_opts into /bin/sh -c before starting postgres.
                executable(pg + "pg_ctl", """\
                    #!/usr/bin/env python3
                    import subprocess, sys
                    if "status" in sys.argv:
                        sys.exit(3)
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
                                  "-c", "unix_socket_directories=", "-c", "max_wal_size=4096"],
                                 json.loads(log.read_text()))
                self.assertTrue(config.is_file())
                self.assertFalse((root / "injected").exists())
                self.assertTrue((root / "build/services/postgresql-capture/data").is_dir())


class NativeCaptureLifecycleTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.artifacts = self.root / "results/poc/capture"
        self.cluster = self.root / "build/services/postgresql-capture/data"
        script = self.root / "scripts/native-capture.sh"
        script.parent.mkdir()
        shutil.copyfile(SCRIPT, script)
        for relative in ("build/sibench/sibench.jar", "config/sibench.yaml", "profile.yaml"):
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        stub = self.root / "stub.py"
        stub.write_text(textwrap.dedent(r'''            #!/usr/bin/env python3
            import json, os, pathlib, shlex, sys
            root = pathlib.Path(os.environ["SIBENCH_TEST_ROOT"])
            data = root / "build/services/postgresql-capture/data"
            artifacts = root / "results/poc/capture"
            name, args = pathlib.Path(sys.argv[0]).name, sys.argv[1:]
            def record(event):
                with (root / "events.jsonl").open("a") as output:
                    output.write(json.dumps(event) + "\n")
            if name == "java":
                owner = next(x for x in args if x.startswith("org.sibench."))
                stage = args[args.index(owner) + 1]
                if stage == "paths":
                    for k, v in {"SIBENCH_ARTIFACTS": artifacts, "SIBENCH_RAW": artifacts / "raw",
                                 "SIBENCH_CAPTURE_DETAIL": "summary", "SIBENCH_CAPTURE_COMPRESS": "zstd",
                                 "SIBENCH_PORT": 5544, "SIBENCH_DATABASE": "capture"}.items():
                        print(k + "=" + shlex.quote(str(v)))
                elif stage == "verify": pass
                elif stage == "postgres-config": pathlib.Path(args[-1]).touch()
                else:
                    record({"stage": stage})
                    if stage == os.environ.get("SIBENCH_TEST_FAIL_STAGE"):
                        (root / "failed").touch()
                        sys.exit(79)
                    if stage in ("load", "check", "export", "run"):
                        assert (data / "running").is_file()
                        capture = (data / "capture").read_text()
                        assert bool(capture) == (stage != "load")
                    if stage == "load": (data / "identity").write_text("original database")
                    elif stage in ("export", "run"):
                        assert (data / "identity").read_text() == "original database"
                    elif stage == "compile-trace":
                        assert not (data / "running").exists()
                        trace = artifacts / "trace"
                        trace.mkdir()
                        for n in ("access-ledger.json", "capture-completeness.json"):
                            (trace / n).write_text("{}")
            elif name == "pg_isready": sys.exit(1)
            elif name == "postgres": print("80")
            elif name == "initdb":
                data.mkdir()
                record({"stage": "initdb", "data": str(data)})
            elif name == "pg_ctl":
                action = args[-1]
                if action == "status": sys.exit(0 if (data / "running").exists() else 3)
                record({"stage": action, "data": args[args.index("-D") + 1]})
                if action == "start":
                    (data / "running").touch()
                    (data / "capture").write_text(os.environ.get("SIBENCH_CAPTURE_DIR", ""))
                elif action == "stop":
                    if os.environ.get("SIBENCH_TEST_FAIL_STOP") and (root / "failed").exists(): sys.exit(86)
                    (data / "running").unlink()
            else: raise AssertionError(name)
            '''))
        stub.chmod(0o755)
        for relative in ["jdk/bin/java"] + ["build/services/postgresql-capture/runtime/bin/" + name
                                             for name in ("pg_isready", "initdb", "postgres", "pg_ctl")]:
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.symlink_to(stub)
        self.env = dict(os.environ, SIBENCH_JAVA_HOME=str(self.root / "jdk"),
                        SIBENCH_WORKDIR=str(self.root), SIBENCH_TEST_ROOT=str(self.root))
        self.env.pop("SIBENCH_KEEP_DATABASE", None)
        self.env.pop("SIBENCH_PREWARM", None)
        self.command = ["bash", str(script), str(self.root / "profile.yaml")]

    def execute(self):
        return subprocess.run(self.command, env=self.env, capture_output=True, text=True, timeout=15)

    def events(self):
        return [json.loads(line) for line in (self.root / "events.jsonl").read_text().splitlines()]

    def test_one_database_is_loaded_exported_run_and_deleted_after_publication(self):
        result = self.execute()
        self.assertEqual(0, result.returncode, result.stderr)
        events = self.events()
        self.assertEqual(["initdb", "start", "load", "stop", "start", "check", "export",
                          "snapshot-index-counters", "run", "snapshot-index-counters", "stop", "compile-trace"],
                         [event["stage"] for event in events])
        self.assertEqual({str(self.cluster)}, {event["data"] for event in events if "data" in event})
        self.assertFalse(self.cluster.exists())
        self.assertFalse((self.root / "build/services/postgresql-still").exists())
        self.assertTrue((self.artifacts / "trace/access-ledger.json").is_file())

    def test_explicit_acceptance_can_retain_the_final_database(self):
        self.env["SIBENCH_KEEP_DATABASE"] = "1"
        result = self.execute()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(self.cluster.is_dir())
        self.assertFalse((self.cluster / "running").exists())

    def test_failed_export_stops_but_retains_database_and_does_not_run_workload(self):
        self.env["SIBENCH_TEST_FAIL_STAGE"] = "export"
        result = self.execute()
        self.assertEqual(79, result.returncode, result.stderr)
        self.assertTrue(self.cluster.is_dir())
        self.assertFalse((self.cluster / "running").exists())
        self.assertNotIn("run", [event["stage"] for event in self.events()])

    def test_failed_compilation_retains_stopped_database(self):
        self.env["SIBENCH_TEST_FAIL_STAGE"] = "compile-trace"
        result = self.execute()
        self.assertEqual(79, result.returncode, result.stderr)
        self.assertTrue(self.cluster.is_dir())
        self.assertFalse((self.cluster / "running").exists())

    def test_cleanup_error_does_not_replace_first_failure(self):
        self.env.update(SIBENCH_TEST_FAIL_STAGE="run", SIBENCH_TEST_FAIL_STOP="1")
        result = self.execute()
        self.assertEqual(79, result.returncode, result.stderr)
        self.assertIn("failed to stop disposable PostgreSQL", result.stderr)
        self.assertTrue(self.cluster.is_dir())

    def test_explicit_baseline_audit_runs_before_counters_and_workload(self):
        self.command.insert(-1, "--audit-baseline")
        result = self.execute()
        self.assertEqual(0, result.returncode, result.stderr)
        stages = [event["stage"] for event in self.events()]
        self.assertLess(stages.index("export"), stages.index("audit-baseline"))
        self.assertLess(stages.index("audit-baseline"), stages.index("snapshot-index-counters"))
        acceptance = self.artifacts / "acceptance"
        timing = json.loads((acceptance / "baseline-audit-timing.json").read_text())
        self.assertEqual(0, timing["exit_code"])
        self.assertGreaterEqual(timing["ended_epoch_ns"], timing["started_epoch_ns"])
        self.assertIn("audit-baseline", (acceptance / "baseline-audit-command.sh").read_text())

    def test_baseline_audit_failure_prevents_workload_and_retains_database(self):
        self.command.insert(-1, "--audit-baseline")
        self.env["SIBENCH_TEST_FAIL_STAGE"] = "audit-baseline"
        result = self.execute()
        self.assertEqual(79, result.returncode, result.stderr)
        self.assertNotIn("run", [event["stage"] for event in self.events()])
        self.assertFalse((self.cluster / "running").exists())
        timing = json.loads((self.artifacts / "acceptance/baseline-audit-timing.json").read_text())
        self.assertEqual(79, timing["exit_code"])
        self.assertTrue(self.cluster.is_dir())

    def test_existing_database_is_not_reused_or_deleted(self):
        self.cluster.mkdir()
        sentinel = self.cluster / "diagnostic"
        sentinel.write_text("preserve")
        result = self.execute()
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertEqual("preserve", sentinel.read_text())
        self.assertFalse((self.root / "events.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
