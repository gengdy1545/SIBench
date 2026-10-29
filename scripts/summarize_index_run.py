#!/usr/bin/env python3
"""Summarize a successful service report and its RocksDB EVENT_LOG_v1 evidence.

This reads reports and engine logs, never Trace or workload configuration. Event durations
are observations from the supplied logs; incomplete jobs prevent a claim of completed drain.
"""

import argparse
import hashlib
import json
from pathlib import Path


def engine_events(paths):
    if not paths:
        return {"sources": [], "coverage": "not_collected", "totals": "N/A", "events": []}
    jobs = {}
    created = {}
    sources = []
    for path in paths:
        path = Path(path)
        sources.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        for number, line in enumerate(path.read_text().splitlines(), 1):
            _, marker, text = line.partition("EVENT_LOG_v1 ")
            if not marker:
                continue
            try:
                event = json.loads(text)
            except ValueError as error:
                raise ValueError("Malformed engine event at %s:%s" % (path, number)) from error
            name = event.get("event", "")
            job = event.get("job")
            if name == "table_file_creation":
                created.setdefault((str(path), job), []).append({
                    key: event[key] for key in
                    ("file_number", "file_size", "smallest_seqno", "largest_seqno") if key in event
                })
            elif name in ("flush_started", "flush_finished", "compaction_started", "compaction_finished"):
                kind, boundary = name.split("_")
                key = (str(path), job, kind)
                row = jobs.setdefault(key, {"log": str(path), "job": job, "kind": kind})
                if boundary in row:
                    raise ValueError("Duplicate %s boundary for job %s in %s" % (boundary, job, path))
                row[boundary] = event
    events = []
    for row in jobs.values():
        begin, end = row.get("started"), row.get("finished")
        files = created.get((row["log"], row["job"]), [])
        item = {"log": row["log"], "job": row["job"], "kind": row["kind"],
                "complete": begin is not None and end is not None, "files_created": files}
        if begin:
            item.update(start_micros=begin["time_micros"], reason=begin.get(
                "flush_reason" if row["kind"] == "flush" else "compaction_reason"),
                input_bytes=begin.get("total_data_size" if row["kind"] == "flush" else "input_data_size"),
                input_bytes_kind="uncompressed_memtable" if row["kind"] == "flush" else "sst")
        if end:
            item.update(end_micros=end["time_micros"], lsm_state=end.get("lsm_state"))
        if item["complete"]:
            duration = end["time_micros"] - begin["time_micros"]
            if duration < 0:
                raise ValueError("Engine job ends before it starts")
            item["seconds"] = duration / 1_000_000
            item["output_bytes"] = (sum(f["file_size"] for f in files) if row["kind"] == "flush"
                                    else end["total_output_size"])
            if "compaction_time_cpu_micros" in end:
                item["cpu_seconds"] = end["compaction_time_cpu_micros"] / 1_000_000
        events.append(item)
    events.sort(key=lambda x: x.get("start_micros", x.get("end_micros")))
    totals = {}
    for kind in ("flush", "compaction"):
        rows = [x for x in events if x["kind"] == kind]
        complete = [x for x in rows if x["complete"]]
        totals[kind] = {"completed_jobs": len(complete), "incomplete_jobs": len(rows) - len(complete),
                        "sum_job_seconds": sum(x["seconds"] for x in complete),
                        "output_bytes": sum(x["output_bytes"] for x in complete)}
    return {"sources": sources, "coverage": "observed_log_files", "totals": totals, "events": events}


def summarize(report, logs, after_flush_millis=None):
    report = Path(report)
    data = json.loads(report.read_text())
    if data.get("status") != "passed":
        raise ValueError("A failed or incomplete service run cannot be a successful measurement")
    result = data["result"]
    if (result["per_query_digest_mismatches"] != 0
            or result["per_query_digest_checks"] != result["search_requests"]):
        raise ValueError("Service query checks are incomplete or mismatched")
    phases = {}
    for name in ("baseline", "workload"):
        phase = result[name]
        dispatch = phase["dispatch"]
        if dispatch["pending_responses"] or dispatch["completed_responses"] != dispatch["target_calls"]:
            raise ValueError("Service responses are incomplete")
        phases[name] = {key: phase[key] for key in (
            "operations", "seconds", "operations_per_second", "request_bytes", "result_bytes",
            "flush_seconds", "published_version")}
        phases[name].update({key: dispatch[key] for key in (
            "source_operations", "filtered_secondary_operations", "transaction_control_records",
            "target_calls", "commit_markers", "backpressure_seconds", "peak_in_flight", "measurement",
            "search_latency", "write_latency", "commit_latency")})
    engines = engine_events(logs)
    if after_flush_millis is not None:
        cutoff = after_flush_millis * 1000
        after = [e for e in engines["events"] if e["kind"] == "compaction"
                 and e.get("end_micros", cutoff + 1) > cutoff]
        engines["after_flush"] = {
            "flush_time_millis": after_flush_millis,
            "observed_compaction_seconds": "N/A" if not logs else max([0] + [
                (e["end_micros"] - cutoff) / 1_000_000 for e in after if e["complete"]]),
            "incomplete_compactions": sum(not e["complete"] for e in after) if logs else "N/A",
            "scope": "last observed completion after Flush, not proof that future compaction is impossible"}
    return {"analysis_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "report": {"path": str(report), "sha256": hashlib.sha256(report.read_bytes()).hexdigest()},
            "status": "passed", "service": result["service"], "phases": phases,
            "queries": result["queries"], "digest_checks": result["per_query_digest_checks"],
            "checked_result_items": result["checked_result_items"], "engine": engines}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--engine-log", type=Path, action="append", default=[])
    parser.add_argument("--after-flush-millis", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = summarize(args.report, args.engine_log, args.after_flush_millis)
    # Archived evidence is immutable; choose a new output name for a revised analysis.
    with args.output.open("x") as stream:
        json.dump(data, stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
