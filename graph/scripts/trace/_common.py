"""Paths, input selection and export shared by the Trace plotting entry points."""

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
# Allow direct script execution from any working directory.
sys.path.insert(0, str(ROOT))
from graph.config import global_config
from graph.config.global_config import OUTPUT_FORMATS, SAVE_PARAMS


def parse_arguments(description, *, analysis_paths=False):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--runtime", type=Path, help="runtime YAML (default: RUNTIME_CONFIG in global_config.py)")
    parser.add_argument("--jar", type=Path, help="SIBench JAR (default: SIBENCH_JAR in global_config.py)")
    if analysis_paths:
        parser.add_argument("--analysis-directory", type=Path, help="reuse an existing analysis directory")
        parser.add_argument("--out", type=Path, help="output directory (default: PLOT_DIRECTORY in global_config.py)")
    return parser.parse_args()


def load_inputs(traces, arguments):
    analysis_directory = getattr(arguments, "analysis_directory", None)
    plot_directory = getattr(arguments, "out", None)
    config = {
        # Command-line paths are relative to the working directory; ROOT / absolute keeps them unchanged.
        "jar": arguments.jar.resolve() if arguments.jar else global_config.SIBENCH_JAR,
        "runtime": arguments.runtime.resolve() if arguments.runtime else global_config.RUNTIME_CONFIG,
        "analysis_directory": analysis_directory.resolve() if analysis_directory else global_config.ANALYSIS_DIRECTORY,
        "plot_directory": plot_directory.resolve() if plot_directory else global_config.PLOT_DIRECTORY,
        "traces": deepcopy(traces),
    }
    for key in ("jar", "analysis_directory", "plot_directory", "runtime"):
        config[key] = ROOT / config[key]
    if not isinstance(config["traces"], list) or not config["traces"]:
        raise ValueError("Select at least one Trace")
    paths = json.loads(subprocess.check_output(
        ["java", "-jar", str(config["jar"]), "trace-paths", str(config["runtime"])], cwd=ROOT, text=True))
    names = set()
    for trace in config["traces"]:
        if set(trace) != {"name", "label"}:
            raise ValueError("Each Trace needs name and label")
        name = trace["name"]
        if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name) or name in names:
            raise ValueError(f"Invalid or duplicate Trace name: {name}")
        if not isinstance(trace["label"], str) or not trace["label"].strip():
            raise ValueError("Trace labels must be nonempty strings")
        names.add(name)
        if name not in paths:
            raise ValueError(f"Unknown benchmark entry: {name}")
        trace["directory"] = Path(paths[name])
        if not (trace["directory"] / "manifest.json").is_file():
            raise FileNotFoundError(f"Missing Trace: {trace['directory']}; generate or fetch the selected input first")
        source = trace["directory"].resolve()
        for key in ("analysis_directory", "plot_directory"):
            destination = config[key].resolve()
            if destination == source or source in destination.parents or destination in source.parents:
                raise ValueError(f"{key} must not overlap a Trace directory")
    return config


def load_analysis(config, trace):
    directory = config["analysis_directory"] / trace["name"] / "accesses"
    summary = json.loads((directory / "summary.json").read_text())
    preparation = json.loads((directory / "preparation.json").read_text())
    identity = hashlib.sha256((trace["directory"] / "manifest.json").read_bytes()).hexdigest()
    if (summary.get("status") != "complete" or summary["manifest_sha256"] != identity
            or preparation["jar_sha256"] != hashlib.sha256(config["jar"].read_bytes()).hexdigest()
            or preparation["runtime_sha256"] != hashlib.sha256(config["runtime"].read_bytes()).hexdigest()):
        raise ValueError(f"Stale analysis for {trace['name']}; run prepare_analysis.py")
    return directory, summary


def save_figure(figure, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    for extension in OUTPUT_FORMATS:
        figure.savefig(f"{path}.{extension}", **SAVE_PARAMS)
