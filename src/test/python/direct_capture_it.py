"""Explicit bounded native-PG direct-generation probe; independent of benchmark performance runs."""
import argparse
import datetime
import hashlib
import json
import os
import resource
from pathlib import Path
import socket
import shutil
import subprocess
import time


def main():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-bin", required=True, type=Path)
    parser.add_argument("--work-directory", required=True, type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--native-clients", action="store_true",
                        help="Run two-table sysbench (1024 rows) and YCSB (64 rows) production stages using freshly built pinned clients")
    mode.add_argument("--generation-budgets", action="store_true",
                      help="Check transaction-safe operation/output targets with 64 rows and 2/32 native clients")
    args = parser.parse_args()
    execution_seconds = 180 if args.generation_budgets else 120
    if os.geteuid() == 0:
        parser.error("Run as a non-root PostgreSQL service user")
    root = Path(__file__).resolve().parents[3]
    work = args.work_directory.resolve()
    if not work.is_relative_to(root / "results/capture"):
        parser.error("work-directory must be under this project's results/capture")
    work.mkdir(parents=True, exist_ok=False)
    if shutil.disk_usage(work).free < 20 * 1024**3:
        parser.error("Keep at least 20 GiB free for the native check")
    binary = args.postgres_bin.resolve()
    data = work / "postgres"
    config = work / "postgres.conf"
    config.write_text("listen_addresses='127.0.0.1'\nshared_buffers='32MB'\nmin_wal_size='64MB'\nmax_wal_size='128MB'\nfsync=off\nfull_page_writes=off\nautovacuum=off\ncompute_query_id=on\nmax_parallel_workers_per_gather=0\nrestart_after_crash=off\n")
    endpoint = Path("/tmp") / ("sibench-source-" + str(os.getpid()) + ".sock")
    with socket.socket() as port_socket:
        port_socket.bind(("127.0.0.1", 0))
        port = port_socket.getsockname()[1]
    control = work / "control"
    control.mkdir()
    env = dict(os.environ, SIBENCH_CAPTURE_DIR=str(control), SIBENCH_CAPTURE_SOCKET=str(endpoint),
               SIBENCH_CAPTURE_DETAIL="summary", SIBENCH_CAPTURE_COMPRESS="zstd",
               SIBENCH_DIRECT_RUN=str(work),
               SIBENCH_DIRECT_PG_JDBC=f"jdbc:postgresql://127.0.0.1:{port}/postgres",
               MAVEN_OPTS="-Xmx512m")
    report = {"status": "running", "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "input": ("sysbench: 2 tables x 64 rows; YCSB: 64 rows; 2 clients target 40 Trace records or 4096 proto bytes; 32 clients target one record; 50000 requested native operations"
                        if args.generation_budgets else "sysbench: 2 tables x 512 rows; YCSB: 64 rows; 1 client; 4 events/operations per pinned workload"
                        if args.native_clients else "3 rows each in reviewed sysbench/YCSB schemas; 2 sessions; controlled SQL"),
              "scope": "native generation and local service correctness, not benchmark performance",
              "budgets": {"setup_seconds": 60, "execution_seconds": execution_seconds, "teardown_seconds": 25,
                          "java_heap": "1GiB", "pg_shared_buffers": "32MiB", "core_dump_bytes": 0},
              "postgres_sha256": hashlib.sha256((binary / "postgres").read_bytes()).hexdigest(),
              "row_capture_sha256": hashlib.sha256((root / "build/services/postgresql-capture/row_capture.so").read_bytes()).hexdigest(),
              "config_sha256": hashlib.sha256(config.read_bytes()).hexdigest()}
    if args.native_clients or args.generation_budgets:
        report["generator_sha256"] = hashlib.sha256((root / "build/sibench/sibench.jar").read_bytes()).hexdigest()
        report["native_build_sha256"] = {
            name: hashlib.sha256((root / "build/benchmarks" / name / "native-build.json").read_bytes()).hexdigest()
            for name in ("sysbench", "YCSB")}
    started = False
    error = None

    def run(command, name, timeout):
        before = time.monotonic()
        with (work / (name + ".log")).open("w") as log:
            result = subprocess.run([str(value) for value in command], cwd=root, env=env,
                                    stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
        report[name + "_seconds"] = time.monotonic() - before
        if result.returncode:
            raise RuntimeError(f"{name} failed with exit {result.returncode}; see {name}.log")

    try:
        run([binary / "initdb", "-D", data, "-U", "postgres", "--locale=C", "-E", "UTF8", "--auth=trust"], "initdb", 30)
        options = f"-c config_file='{config}' -c port={port} -c unix_socket_directories=''"
        run([binary / "pg_ctl", "-D", data, "-l", work / "postgres.log", "-o", options, "-w", "-t", "25", "start"], "start", 30)
        started = True
        test = ("GenerationBudgetIntegrationIT" if args.generation_budgets
                else "NativeCaptureIntegrationIT" if args.native_clients else "DirectCaptureIntegrationIT")
        run(["mvn", "-o", "-DargLine=-Xmx1g", "-Dtest=" + test, "test"], "execution", execution_seconds)
        report["status"] = "passed"
    except Exception as failure:
        error = failure
        report.update(status="failed", error=str(failure))
    finally:
        if started:
            try:
                run([binary / "pg_ctl", "-D", data, "-m", "fast", "-w", "-t", "20", "stop"], "stop", 25)
            except Exception as cleanup:
                report["cleanup_error"] = str(cleanup)
                if error is None:
                    error = cleanup
                    report["status"] = "failed"
        endpoint.unlink(missing_ok=True)
        if error is None and data.exists():
            shutil.rmtree(data)
        report["working_database_retained"] = data.exists()
        (work / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if error:
        raise error
    print(work / "report.json")


if __name__ == "__main__":
    main()
