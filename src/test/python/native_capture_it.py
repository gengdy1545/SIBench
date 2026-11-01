"""Explicit real-PG check of base WAL limits and capture startup; not a default unit test."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time
import traceback


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-bin", required=True, type=Path)
    parser.add_argument("--capture-script", required=True, type=Path)
    parser.add_argument("--work-directory", required=True, type=Path)
    parser.add_argument("--port", default=56372, type=int)
    args = parser.parse_args()
    if os.geteuid() == 0:
        parser.error("Run as a non-root PostgreSQL service user")
    root = args.work_directory.resolve()
    root.mkdir(parents=True, exist_ok=False)
    binary = args.postgres_bin.resolve()
    source = args.capture_script.read_text()
    function = "start_cluster() {\n" + source.split("start_cluster() {\n", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
    config = root / "postgres.conf"
    config.write_text("listen_addresses='127.0.0.1'\nshared_buffers='32MB'\nmin_wal_size='64MB'\nmax_wal_size='2GB'\ncheckpoint_timeout='1h'\nautovacuum=off\n")
    report = {"status": "running", "cases": [], "stop_seconds": 180}
    active = None

    def command(values, timeout=40):
        result = subprocess.run([str(x) for x in values], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(str(values[0]) + ": " + result.stderr[-4000:])
        return result.stdout.strip()

    def sql(text):
        return command([binary / "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1",
                        "-h", "127.0.0.1", "-p", args.port, "-U", "postgres", "-d", "postgres", "-c", text])

    def start(data, capture):
        nonlocal active
        env = dict(os.environ, PG=str(binary.parent.parent), PG_CONFIG=str(config),
                   SIBENCH_PORT=str(args.port), DATA=str(data), LOG=str(data.parent / "postgres.log"),
                   CAPTURE=str(capture) if capture else "", SIBENCH_CAPTURE_DETAIL="summary",
                   SIBENCH_CAPTURE_COMPRESS="zstd", PGPASSWORD="")
        result = subprocess.run(["bash", "-e", "-c", function + '\nstart_cluster "$DATA" "$LOG" "$CAPTURE"'],
                                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, timeout=40)
        active = data
        if result.returncode:
            raise RuntimeError(result.stderr[-4000:])
        return result.stderr

    def stop():
        nonlocal active
        if active is not None:
            command([binary / "pg_ctl", "-D", active, "-m", "fast", "-w", "-t", "20", "stop"], 25)
            active = None

    def wal(data):
        return sum(p.stat().st_size for p in (data / "pg_wal").iterdir() if p.is_file())

    def expired(*unused):
        raise TimeoutError("Real-PG startup integration exceeded 180 seconds")

    signal.signal(signal.SIGALRM, expired)
    signal.alarm(180)
    started = time.monotonic()
    digest = "SELECT count(*),md5(string_agg(id::text || ':' || value, ',' ORDER BY id)) FROM sample"
    try:
        usage = os.statvfs(str(root))
        if usage.f_bavail * usage.f_frsize < 20 << 30:
            raise RuntimeError("Disk reserve below 20 GiB")
        for name, capture in [("control", True), ("base", False)]:
            case = root / name
            case.mkdir()
            data = case / "data"
            raw = case / "raw"
            raw.mkdir()
            command([binary / "initdb", "-D", data, "-U", "postgres", "--locale=C", "-E", "UTF8", "--auth=trust"])
            startup = start(data, raw if capture else None)
            settings = sql("SELECT current_setting('max_wal_size'),current_setting('min_wal_size')")
            assert settings == ("2GB|64MB" if capture else "64MB|64MB"), settings
            sql("CREATE TABLE sample(id int PRIMARY KEY,value text); INSERT INTO sample SELECT i,repeat('x',100) FROM generate_series(1,10000) i; CREATE TABLE filler(counter int); INSERT INTO filler VALUES(0)")
            sql("DO $$ BEGIN FOR i IN 1..40 LOOP UPDATE filler SET counter=counter+1; PERFORM pg_switch_wal(); END LOOP; END $$")
            sql("CHECKPOINT")
            rows = sql(digest)
            stop()
            report["cases"].append({"name": name, "settings": settings, "wal_bytes": wal(data),
                                     "rows": rows, "startup": startup})
        clone = root / "clone"
        command(["cp", "-a", "--reflink=always", root / "base/data", clone])
        raw = root / "clone-raw"
        raw.mkdir()
        start(clone, raw)
        report["clone_rows"] = sql(digest)
        report["clone_settings"] = sql("SELECT current_setting('max_wal_size'),current_setting('min_wal_size')")
        stop()
        assert report["cases"][0]["rows"] == report["cases"][1]["rows"] == report["clone_rows"]
        assert report["clone_settings"] == "2GB|64MB", report["clone_settings"]
        assert report["cases"][1]["wal_bytes"] < report["cases"][0]["wal_bytes"]
        report["status"] = "passed"
    except BaseException:
        report["status"] = "failed"
        report["first_error"] = traceback.format_exc()
    finally:
        signal.alarm(0)
        try:
            stop()
        except BaseException:
            report["cleanup_error"] = traceback.format_exc()
            report["status"] = "failed"
        report["seconds"] = time.monotonic() - started
        (root / "result.json").write_text(json.dumps(report, indent=2))
        print(json.dumps(report))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
