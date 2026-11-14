"""Paths, input selection and export shared by the Trace plotting entry points."""

import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = ROOT / "graph/config/traces.json"
STYLE = ROOT / "graph/config/style.mplstyle"


def load_config(path):
    config = json.loads(Path(path).read_text())
    expected = {"jar", "analysis_directory", "plot_directory", "window_requests",
                "pk_bins", "sort_memory_bytes", "traces", "runtime"}
    if set(config) != expected:
        raise ValueError(f"Expected configuration keys: {sorted(expected)}")
    for key in ("window_requests", "pk_bins", "sort_memory_bytes"):
        if type(config[key]) is not int or config[key] < 1:
            raise ValueError(f"{key} must be a positive integer")
    if config["pk_bins"] > 1_000_000:
        raise ValueError("pk_bins must not exceed 1000000")
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
            or summary["window_requests"] != config["window_requests"]
            or summary["requested_pk_bins"] != config["pk_bins"]
            or summary["sort_memory_bytes"] != config["sort_memory_bytes"]):
        raise ValueError(f"Stale analysis for {trace['name']}; run prepare_analysis.py")
    return directory, summary


def save_figure(figure, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    for extension in ("pdf", "png"):
        figure.savefig(f"{path}.{extension}")
