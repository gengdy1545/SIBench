#!/usr/bin/env python3
"""Extract exact Trace row-access distributions with the Java analyzer."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from _common import DEFAULT_CONFIG, ROOT, load_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args()
    config_bytes = args.config.read_bytes()
    config = load_config(args.config)
    runtime_bytes = config["runtime"].read_bytes()
    jar = config["jar"]
    if not jar.is_file():
        raise FileNotFoundError(f"Build SIBench with mvn package first: {jar}")
    identity = hashlib.sha256(jar.read_bytes()).hexdigest()
    for trace in config["traces"]:
        output = config["analysis_directory"] / trace["name"] / "accesses"
        output.mkdir(parents=True, exist_ok=True)
        command = ["java", "-Xmx2g", "-jar", str(jar), "analyze-accesses", str(config["runtime"]), trace["name"],
                   str(output), str(config["window_requests"]), str(config["pk_bins"]),
                   str(config["sort_memory_bytes"])]
        print(f"Analyzing {trace['label']} ...", flush=True)
        (output / "summary.json").unlink(missing_ok=True)
        (output / "preparation.json").unlink(missing_ok=True)
        with (output / "analysis.log").open("w") as log:
            subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
        if hashlib.sha256(jar.read_bytes()).hexdigest() != identity:
            raise RuntimeError("SIBench JAR changed during analysis")
        if args.config.read_bytes() != config_bytes or config["runtime"].read_bytes() != runtime_bytes:
            raise RuntimeError("Trace configuration changed during analysis")
        (output / "preparation.json").write_text(json.dumps({
            "command": command, "jar_sha256": identity,
            "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
            "runtime_sha256": hashlib.sha256(runtime_bytes).hexdigest(),
            "effective_config": json.loads(config_bytes),
        }, indent=2) + "\n")


if __name__ == "__main__":
    main()
