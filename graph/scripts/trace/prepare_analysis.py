#!/usr/bin/env python3
"""Extract exact Trace row-access distributions with the Java analyzer."""

import hashlib
import json
from pathlib import Path
import subprocess

from _common import ROOT, load_inputs, parse_arguments
from graph.config import access_locality_config, global_config


def main():
    arguments = parse_arguments(__doc__)
    config_bytes = {Path(module.__file__): Path(module.__file__).read_bytes()
                    for module in (global_config, access_locality_config)}
    config = load_inputs(access_locality_config.TRACES, arguments)
    runtime_bytes = config["runtime"].read_bytes()
    jar = config["jar"]
    if not jar.is_file():
        raise FileNotFoundError(f"Build SIBench with mvn package first: {jar}")
    identity = hashlib.sha256(jar.read_bytes()).hexdigest()
    for trace in config["traces"]:
        output = config["analysis_directory"] / trace["name"] / "accesses"
        output.mkdir(parents=True, exist_ok=True)
        command = ["java", "-Xmx2g", "-jar", str(jar), "analyze-accesses", str(config["runtime"]), trace["name"],
                   str(output), str(config["window_requests"]),
                   str(config["sort_memory_bytes"])]
        print(f"Analyzing {trace['label']} ...", flush=True)
        (output / "summary.json").unlink(missing_ok=True)
        (output / "preparation.json").unlink(missing_ok=True)
        with (output / "analysis.log").open("w") as log:
            subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
        if hashlib.sha256(jar.read_bytes()).hexdigest() != identity:
            raise RuntimeError("SIBench JAR changed during analysis")
        if (any(path.read_bytes() != data for path, data in config_bytes.items())
                or config["runtime"].read_bytes() != runtime_bytes):
            raise RuntimeError("Trace configuration changed during analysis")
        (output / "preparation.json").write_text(json.dumps({
            "command": command, "jar_sha256": identity,
            "config_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(data).hexdigest()
                              for path, data in config_bytes.items()},
            "runtime_sha256": hashlib.sha256(runtime_bytes).hexdigest(),
            "effective_config": config,
        }, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
