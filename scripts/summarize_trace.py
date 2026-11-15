#!/usr/bin/env python3
"""Report recorded primitive Index Service operations and native IQ coverage.

Reads generation manifests, not Trace payloads. Run check-replay separately for payload
integrity and correctness checks. Baseline entries and workload requests stay separate;
ReadGroup members count individually, and replay-created WriteBatch calls are not counted.
"""

import argparse
import hashlib
import json
from pathlib import Path


KINDS = ("get", "scan", "put", "delete")
SCOPES = ("primary", "secondary")


def count(value):
    if type(value) is not int or value < 0:
        raise ValueError("Operation counts must be nonnegative integers")
    return value


def phase(files):
    counts = {scope: dict.fromkeys(KINDS, 0) for scope in SCOPES}
    origins = dict.fromkeys(("business_insert", "business_update"), 0)
    total = 0
    for file in files:
        stats = file["statistics"]
        if set(stats["write_origins"]) - set(origins):
            raise ValueError("Unknown write origin")
        for origin, value in stats["write_origins"].items():
            origins[origin] += count(value)
        recorded = stats["index_operations"]
        if set(recorded) != set(SCOPES):
            raise ValueError("Missing or unknown index scope")
        subtotal = 0
        for scope in SCOPES:
            if set(recorded[scope]) - set(KINDS):
                raise ValueError("Unknown primitive operation")
            values = {kind: count(recorded[scope].get(kind, 0)) for kind in KINDS}
            size = sum(values.values())
            if size != count(stats[scope + "_entries"]):
                raise ValueError("Index scope count mismatch")
            subtotal += size
            for kind, value in values.items():
                counts[scope][kind] += value
        if subtotal != count(stats["effective_requests"]):
            raise ValueError("Effective request count mismatch")
        total += subtotal
    result = {}
    for scope, values in counts.items():
        reads = values["get"] + values["scan"]
        writes = values["put"] + values["delete"]
        result[scope] = {"operations": values, "reads": reads, "writes": writes,
                        "total": reads + writes, "share": (reads + writes) / total if total else 0,
                        "operation_shares": {k: v / total if total else 0 for k, v in values.items()}}
    return {"total": total, "write_origins": origins, **result}


def summarize(path):
    path = Path(path)
    contents = path.read_bytes()
    data = json.loads(contents)
    if (path.parent / "INCOMPLETE").exists() or data.get("status") != "complete_for_declared_projection":
        raise ValueError("Trace generation is incomplete")
    phases = {name: phase(data["proto_files"][name]) for name in ("baseline", "workload")}
    for scope in SCOPES:
        baseline = phases["baseline"][scope]
        if baseline["reads"] or baseline["operations"]["delete"]:
            raise ValueError("Baseline contains an operation other than Put")
    return {"manifest": str(path), "manifest_sha256": hashlib.sha256(contents).hexdigest(),
            "baseline_rows": count(data["baseline_rows"]), "phases": phases,
            "query_coverage": data.get("query_coverage"),
            "scope": "Recorded exported primitive requests; fractions use all requests of the same phase. "
                     "Baseline entries are not final live entries. This report does not validate payloads or measure RPCs."}


def markdown(reports):
    lines = ["| Trace | Baseline PK entries | Baseline secondary entries | Scope | Get | Scan | Put | Delete | Reads | Writes | Workload share |",
             "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for report in reports:
        baseline, workload = [report["phases"][p] for p in ("baseline", "workload")]
        for scope in SCOPES:
            stats = workload[scope]
            cells = [report["manifest"], baseline["primary"]["total"], baseline["secondary"]["total"], scope,
                     *(f'{stats["operations"][k]} ({stats["operation_shares"][k]:.2%})' for k in KINDS),
                     stats["reads"], stats["writes"], f'{stats["share"]:.2%}']
            lines.append("| " + " | ".join(map(str, cells)) + " |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifests", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--markdown", type=Path)
    args = parser.parse_args()
    reports = [summarize(path) for path in args.manifests]
    with args.output.open("x") as stream:
        json.dump(reports, stream, indent=2)
        stream.write("\n")
    if args.markdown:
        with args.markdown.open("x") as stream:
            stream.write(markdown(reports))


if __name__ == "__main__":
    main()
