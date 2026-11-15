"""Trace input identities and publication-sized plot exports."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import runpy
import sys
import tempfile
import unittest
from unittest.mock import patch
from contextlib import redirect_stderr


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("trace_plot_common", ROOT / "graph/scripts/trace/_common.py")
COMMON = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMMON)
from graph.config import access_locality_config, global_config, operation_mix_config, update_locality_config


class TraceConfigTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.trace = self.root / "trace"
        self.trace.mkdir()
        (self.trace / "manifest.json").write_text("{}")
        self.traces = [{"name": "case", "label": "Case"}]
        settings = patch.multiple(global_config, ANALYSIS_DIRECTORY=str(self.root / "analysis"),
                                  PLOT_DIRECTORY=str(self.root / "plots"), WINDOW_REQUESTS=10)
        self.addCleanup(settings.stop)
        settings.start()
        resolver = patch.object(COMMON.subprocess, "check_output",
                                return_value=json.dumps({"case": str(self.trace)}))
        self.addCleanup(resolver.stop)
        resolver.start()

    def test_global_and_figure_settings_resolve_inputs_without_mutating_source_values(self):
        resolved = COMMON.load_inputs(self.traces)
        self.assertEqual(10, resolved["window_requests"])
        self.assertEqual(self.trace, resolved["traces"][0]["directory"])
        self.assertEqual(ROOT / global_config.SIBENCH_JAR, resolved["jar"])
        self.assertNotIn("directory", self.traces[0])

    def test_invalid_inputs_and_overlapping_outputs_are_rejected(self):
        for setting, value, message in (("WINDOW_REQUESTS", 0, "positive integer"),
                                        ("PLOT_DIRECTORY", str(self.trace), "must not overlap")):
            with self.subTest(setting=setting), patch.object(global_config, setting, value):
                with self.assertRaisesRegex(ValueError, message):
                    COMMON.load_inputs(self.traces)
        with self.assertRaisesRegex(ValueError, "Unknown benchmark"):
            COMMON.load_inputs([{"name": "unknown", "label": "Unknown"}])
        with self.assertRaisesRegex(ValueError, "Select at least one"):
            COMMON.load_inputs([])
        with self.assertRaisesRegex(ValueError, "duplicate Trace"):
            COMMON.load_inputs([dict(self.traces[0]), dict(self.traces[0])])
        (self.trace / "manifest.json").unlink()
        with self.assertRaisesRegex(FileNotFoundError, "Missing Trace"):
            COMMON.load_inputs(self.traces)

    def test_preparation_records_global_and_figure_config(self):
        jar = self.root / "analyzer.jar"
        jar.write_bytes(b"analyzer identity")
        runtime = self.root / "runtime.yaml"
        runtime.write_text("{}")
        script = ROOT / "graph/scripts/trace/prepare_analysis.py"
        with patch.dict(sys.modules, {"_common": COMMON}), \
                patch.object(sys, "argv", [str(script)]), \
                patch.multiple(global_config, SIBENCH_JAR=str(jar), RUNTIME_CONFIG=str(runtime)), \
                patch.object(access_locality_config, "TRACES", self.traces), \
                patch.object(COMMON.subprocess, "run") as analyzer:
            runpy.run_path(str(script), run_name="__main__")
        analyzer.assert_called_once()
        receipt = json.loads((self.root / "analysis/case/accesses/preparation.json").read_text())
        for module in (global_config, access_locality_config):
            path = Path(module.__file__)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),
                             receipt["config_sha256"][str(path.relative_to(ROOT))])
        self.assertEqual(hashlib.sha256(jar.read_bytes()).hexdigest(), receipt["jar_sha256"])
        self.assertEqual(str(self.trace), receipt["effective_config"]["traces"][0]["directory"])
        self.assertEqual(global_config.WINDOW_REQUESTS, receipt["effective_config"]["window_requests"])

    def test_entry_points_reject_external_config_arguments(self):
        for name in ("plot_operation_mix", "plot_access_locality", "prepare_analysis"):
            script = ROOT / f"graph/scripts/trace/{name}.py"
            with self.subTest(name=name), patch.dict(sys.modules, {"_common": COMMON}), \
                    patch.object(sys, "argv", [str(script), "--config", "unused.py"]), \
                    redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    runpy.run_path(str(script), run_name="__main__")
                self.assertEqual(2, error.exception.code)


class AnalysisIdentityTest(unittest.TestCase):
    def test_changed_analyzer_rejects_previously_matching_extraction(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            trace = {"name": "case", "directory": root / "trace"}
            trace["directory"].mkdir()
            manifest = trace["directory"] / "manifest.json"
            manifest.write_text("{}")
            jar = root / "sibench.jar"
            jar.write_bytes(b"old analyzer")
            config = {"jar": jar, "analysis_directory": root, "window_requests": 10,
                      "sort_memory_bytes": 4096}
            directory = root / "case/accesses"
            directory.mkdir(parents=True)
            summary = {"status": "complete", "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
                       "window_requests": 10, "sort_memory_bytes": 4096}
            (directory / "summary.json").write_text(json.dumps(summary))
            (directory / "preparation.json").write_text(json.dumps({"jar_sha256": hashlib.sha256(jar.read_bytes()).hexdigest()}))
            self.assertEqual(COMMON.load_analysis(config, trace), (directory, summary))
            jar.write_bytes(b"new analyzer")
            with self.assertRaisesRegex(ValueError, "Stale analysis"):
                COMMON.load_analysis(config, trace)


class PlotStyleTest(unittest.TestCase):
    def test_global_margin_controls_all_sides_without_changing_canvas_size(self):
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        for width in (global_config.SINGLE_COLUMN_WIDTH, global_config.DOUBLE_COLUMN_WIDTH):
            for margin in (1.0, 3.0):
                with self.subTest(width=width, margin=margin), matplotlib.rc_context(), \
                        patch.object(global_config, "FIGURE_MARGIN_MM", margin):
                    global_config.apply_style()
                    fig, ax = plt.subplots(figsize=(width, 2.6))
                    try:
                        ax.plot([0, 1], [0, 1], label="Series")
                        ax.set(xlabel="Operation share", ylabel="Benchmark")
                        ax.annotate("0.05%", (1, 0.5), xytext=(3, 0),
                                    textcoords="offset points", annotation_clip=False)
                        ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1))
                        fig.canvas.draw()
                        bounds = fig.get_tightbbox(fig.canvas.get_renderer())
                        actual = (bounds.x0, bounds.y0, width - bounds.x1, 2.6 - bounds.y1)
                        for padding in actual:
                            self.assertAlmostEqual(margin, padding * 25.4, delta=0.1)
                        self.assertEqual((width, 2.6), tuple(fig.get_size_inches()))
                    finally:
                        plt.close(fig)

    def test_access_points_preserve_real_keys_and_split_tables_and_operations_without_titles(self):
        import pandas as pd
        import numpy as np

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            columns = ["window", "pk", "key", "get_hits", "scan_hits", "put_hits", "delete_hits"]
            cases = [
                ("int", [(0, "10", "01800000000000000a", 2, 0, 0, 0),
                         (0, "20", "018000000000000014", 0, 4, 0, 0),
                         (1, "30", "01800000000000001e", 1, 0, 1, 1)]),
                ("int", [(0, "10", "01800000000000000a", 7, 0, 0, 0)]),
                ("varchar", [(0, "01", "01303100", 1, 0, 0, 0),
                             (0, "1", "013100", 2, 0, 0, 0),
                             (1, "NA", "014e4100", 3, 0, 0, 0),
                             (1, 'a,"b', "01612c226200", 4, 0, 0, 0)]),
                ("varchar", []),
            ]
            tables = []
            for table_id, (key_type, rows) in enumerate(cases, 1):
                tables.append({"id": table_id, "name": f"records{table_id}", "key_type": key_type})
                pd.DataFrame(rows, columns=columns).to_csv(root / f"table-{table_id}.csv.gz", index=False)
            summary = {"tables": tables, "window_requests": 10, "workload_requests": 15}
            captured = {}
            save = COMMON.save_figure

            def capture(figure, path):
                captured[(path.parent.name, path.name)] = figure
                self.assertEqual([], figure.texts)
                self.assertTrue(all(not ax.get_title() and not ax.get_title(loc="left") for ax in figure.axes))
                self.assertFalse(any("PK-rank" in text.get_text() for ax in figure.axes for text in ax.texts))
                save(figure, path)

            script = ROOT / "graph/scripts/trace/plot_access_locality.py"
            with patch.dict(sys.modules, {"_common": COMMON}), \
                    patch.object(sys, "argv", [str(script)]), \
                    patch.object(COMMON, "load_inputs", return_value={
                        "traces": [{"name": "case"}], "plot_directory": root / "plots"}), \
                    patch.object(COMMON, "load_analysis", return_value=(root, summary)), \
                    patch.object(COMMON, "save_figure", side_effect=capture):
                runpy.run_path(str(script))["main"]()
            self.assertEqual(12, len(captured))
            points = captured[("table-1", "access_get")].axes[0].collections[0]
            np.testing.assert_array_equal([[5, 10], [12.5, 30]], points.get_offsets())
            np.testing.assert_array_equal([2, 1], points.get_array())
            points = captured[("table-1", "access_write")].axes[0].collections[0]
            np.testing.assert_array_equal([[12.5, 30]], points.get_offsets())
            np.testing.assert_array_equal([2], points.get_array())
            points = captured[("table-2", "access_get")].axes[0].collections[0]
            np.testing.assert_array_equal([7], points.get_array())
            axes = captured[("table-3", "access_get")].axes[0]
            self.assertEqual(["01", "1", "NA", 'a,"b'], [tick.get_text() for tick in axes.get_yticklabels()])
            self.assertEqual(4, len(axes.collections[0].get_offsets()))
            self.assertEqual(["No hits"], [text.get_text() for text in captured[("table-4", "access_get")].axes[0].texts])
            empty_label = captured[("table-4", "access_get")].axes[0].texts[0]
            self.assertEqual(global_config.ANNOTATION_FONT_SIZE, empty_label.get_fontsize())
            self.assertEqual(access_locality_config.EMPTY_TEXT_POSITION, empty_label.get_position())

    def test_operation_mix_stacks_hand_checked_shares_and_preserves_empty_workloads(self):
        import matplotlib.pyplot as plt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            traces = []
            workloads = [
                [{"statistics": {"effective_requests": 10, "primary_entries": 10, "secondary_entries": 0,
                   "write_origins": {"business_insert": 4, "business_update": 1},
                   "index_operations": {"primary": {"get": 2, "scan": 3, "put": 5}, "secondary": {}}}}],
                [{"statistics": {"effective_requests": 1, "primary_entries": 1, "secondary_entries": 0,
                   "write_origins": {},
                   "index_operations": {"primary": {"get": 1}, "secondary": {}}}}],
                [],
                [{"statistics": {"effective_requests": 10000, "primary_entries": 10000, "secondary_entries": 0,
                   "write_origins": {"business_insert": 1},
                   "index_operations": {"primary": {"get": 9999, "put": 1}, "secondary": {}}}}],
            ]
            for name, workload in zip(("mixed", "get_only", "empty", "thin"), workloads):
                directory = root / name
                directory.mkdir()
                (directory / "manifest.json").write_text(json.dumps({
                    "status": "complete_for_declared_projection", "baseline_rows": 100,
                    "proto_files": {"workload": workload, "baseline": [
                        {"statistics": {"effective_requests": 100, "primary_entries": 100, "secondary_entries": 0,
                         "write_origins": {},
                         "index_operations": {"primary": {"put": 100}, "secondary": {}}}}]}}))
                traces.append({"name": name, "label": name, "directory": directory})
            captured = []
            save = COMMON.save_figure

            def capture(figure, path):
                captured.append(figure)
                save(figure, path)

            script = ROOT / "graph/scripts/trace/plot_operation_mix.py"
            with patch.dict(sys.modules, {"_common": COMMON}), \
                    patch.object(sys, "argv", [str(script)]), \
                    patch.object(COMMON, "load_inputs", return_value={"traces": traces, "plot_directory": root}), \
                    patch.object(COMMON, "save_figure", side_effect=capture):
                runpy.run_path(str(script))["main"]()
            self.assertEqual(1, len(captured))
            figure = captured[0]
            self.assertEqual([], figure.texts)
            axes = figure.axes[0]
            self.assertEqual("", axes.get_title())
            self.assertEqual((0, 1), axes.get_xlim())
            self.assertTrue(axes.yaxis_inverted())
            self.assertEqual([t["label"] for t in traces], [t.get_text() for t in axes.get_yticklabels()])
            self.assertTrue(all(t.get_rotation() == 0 for t in axes.get_yticklabels()))
            self.assertEqual(["Get", "Scan", "Update", "Insert"],
                             [container.get_label() for container in axes.containers])
            self.assertEqual(["Get", "Scan", "Update", "Insert"],
                             [text.get_text() for text in axes.get_legend().get_texts()])
            from matplotlib.colors import to_rgba
            for container, color_index in zip(axes.containers, (0, 1, 3, 2)):
                self.assertEqual(to_rgba(global_config.COLOR_PALETTE[color_index]), container[0].get_facecolor())
            self.assertTrue(all(text.get_fontsize() == operation_mix_config.LEGEND_FONT_SIZE
                                for text in axes.get_legend().get_texts()))
            self.assertTrue(all(text.get_fontsize() == operation_mix_config.ANNOTATION_FONT_SIZE
                                for text in axes.texts))
            for index, (widths, lefts) in enumerate((
                    ((0.2, 0.3, 0.1, 0.4), (0, 0.2, 0.5, 0.6)),
                    ((1, 0, 0, 0), (0, 1, 1, 1)),
                    ((0, 0, 0, 0), (0, 0, 0, 0)),
                    ((0.9999, 0, 0, 0.0001), (0, 0.9999, 0.9999, 0.9999)))):
                for container, width, left in zip(axes.containers, widths, lefts):
                    bar = container[index]
                    self.assertAlmostEqual(width, bar.get_width())
                    self.assertAlmostEqual(left, bar.get_x())
                    self.assertAlmostEqual(index, bar.get_y() + bar.get_height() / 2)
                    self.assertAlmostEqual(operation_mix_config.BAR_HEIGHT, bar.get_height())
            self.assertGreater(operation_mix_config.BAR_HEIGHT, 0)
            self.assertLess(operation_mix_config.BAR_HEIGHT, 1)
            self.assertAlmostEqual(global_config.SINGLE_COLUMN_WIDTH, figure.get_figwidth())
            self.assertLessEqual(figure.get_figheight(), 3)
            self.assertEqual(["20%", "100%", "99.99%", "30%", "10%", "40%", "0.01%"],
                             [text.get_text() for text in axes.texts if text.get_text()])
            self.assertEqual(1, len(axes.lines))
            marker = axes.lines[0]
            self.assertEqual(global_config.COLOR_PALETTE[2], marker.get_color())
            self.assertEqual("s", marker.get_marker())
            self.assertEqual(2.5, marker.get_markersize())
            self.assertEqual([1], list(marker.get_xdata()))
            self.assertEqual([3], list(marker.get_ydata()))
            plt.close(figure)

            import csv
            from matplotlib.colors import to_rgba

            for selected, labels in ((traces[1:], ["Get", "Insert"]), (traces[2:3], [])):
                with self.subTest(labels=labels), patch.dict(sys.modules, {"_common": COMMON}), \
                        patch.object(sys, "argv", [str(script)]), \
                        patch.object(COMMON, "load_inputs", return_value={"traces": selected, "plot_directory": root}), \
                        patch.object(COMMON, "save_figure", side_effect=lambda figure, path: captured.append(figure)):
                    runpy.run_path(str(script))["main"]()
                axes = captured[-1].axes[0]
                self.assertEqual(labels, [container.get_label() for container in axes.containers])
                legend = axes.get_legend()
                self.assertEqual(labels, [text.get_text() for text in legend.get_texts()] if legend else [])
                if labels:
                    self.assertEqual(to_rgba(global_config.COLOR_PALETTE[2]), axes.containers[1][0].get_facecolor())
                    self.assertAlmostEqual(1, sum(container[0].get_width() for container in axes.containers))
                    self.assertAlmostEqual(1, sum(container[2].get_width() for container in axes.containers))
                with (root / "operation_mix.csv").open() as stream:
                    rows = list(csv.DictReader(stream))
                self.assertEqual(4 * len(selected), len(rows))
                self.assertEqual({"get", "scan", "insert", "update"}, {row["operation"] for row in rows})
                self.assertTrue(all(int(row["count"]) == 0 for row in rows if row["operation"] == "update"))
                plt.close(captured[-1])

    def test_operation_mix_rejects_unsupported_or_unclassified_requests(self):
        script = ROOT / "graph/scripts/trace/plot_operation_mix.py"
        cases = (
            ({"get": 1}, {"get": 1}, {}, "primary-only"),
            ({"delete": 1}, {}, {}, "Delete"),
            ({"put": 1}, {}, {}, "Put origin count mismatch"),
            ({"put": 1}, {}, {"business_insert": 1, "business_update": 1}, "Put origin count mismatch"),
        )
        for primary, secondary, origins, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                stats = {"primary_entries": sum(primary.values()), "secondary_entries": sum(secondary.values()),
                         "effective_requests": sum(primary.values()) + sum(secondary.values()),
                         "index_operations": {"primary": primary, "secondary": secondary}, "write_origins": origins}
                (root / "manifest.json").write_text(json.dumps({
                    "status": "complete_for_declared_projection", "baseline_rows": 0,
                    "proto_files": {"baseline": [], "workload": [{"statistics": stats}]}}))
                traces = [{"name": "invalid", "label": "Invalid", "directory": root}]
                with patch.dict(sys.modules, {"_common": COMMON}), \
                        patch.object(sys, "argv", [str(script)]), \
                        patch.object(COMMON, "load_inputs", return_value={"traces": traces, "plot_directory": root}):
                    with self.assertRaisesRegex(ValueError, message):
                        runpy.run_path(str(script))["main"]()
                self.assertFalse((root / "operation_mix.csv").exists())
                self.assertFalse((root / "operation_mix.png").exists())

    def test_operation_mix_separates_adjacent_small_write_labels(self):
        import matplotlib.pyplot as plt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            traces = []
            for name, get, insert, update in (("tiny", 9996, 1, 3), ("middle", 85, 5, 10)):
                directory = root / name
                directory.mkdir()
                total = get + insert + update
                stats = {"effective_requests": total, "primary_entries": total, "secondary_entries": 0,
                         "index_operations": {"primary": {"get": get, "put": insert + update}, "secondary": {}},
                         "write_origins": {"business_insert": insert, "business_update": update}}
                (directory / "manifest.json").write_text(json.dumps({
                    "status": "complete_for_declared_projection", "baseline_rows": 0,
                    "proto_files": {"baseline": [], "workload": [{"statistics": stats}]}}))
                traces.append({"name": name, "label": name, "directory": directory})
            captured = []
            script = ROOT / "graph/scripts/trace/plot_operation_mix.py"
            overrides = {
                "MIN_INSIDE_LABEL_SHARE": 0.11, "PERCENT_LABEL_DECIMALS": 0,
                "OUTSIDE_LABEL_GAP_PT": 3, "OUTSIDE_MARKER_OFFSET_PT": 4, "OUTSIDE_LABEL_OFFSET_PT": 7,
                "PERCENT_LABEL_STYLE": {**operation_mix_config.PERCENT_LABEL_STYLE, "fontsize": 5},
                "LEGEND_STYLE": {**operation_mix_config.LEGEND_STYLE, "fontsize": 11},
            }
            for settings, markers in (({}, 3), (overrides, 4)):
                with self.subTest(settings=settings), patch.dict(sys.modules, {"_common": COMMON}), \
                        patch.object(sys, "argv", [str(script)]), \
                        patch.dict(operation_mix_config.__dict__, settings), \
                        patch.object(COMMON, "load_inputs", return_value={"traces": traces, "plot_directory": root}), \
                        patch.object(COMMON, "save_figure", side_effect=lambda figure, path: captured.append(figure)):
                    runpy.run_path(str(script))["main"]()
                    figure = captured[-1]
                    self.addCleanup(plt.close, figure)
                    figure.canvas.draw()
                    axes = figure.axes[0]
                    boxes = [text.get_window_extent(figure.canvas.get_renderer()) for text in axes.texts]
                    for index, box in enumerate(boxes):
                        self.assertTrue(all(figure.bbox.contains(x, y) for x, y in box.get_points()))
                        self.assertFalse(any(box.overlaps(other) for other in boxes[index + 1:]))
                    self.assertEqual(markers, len(axes.lines))
                    self.assertTrue(all(list(marker.get_xdata()) == [1] for marker in axes.lines))
                    if settings:
                        self.assertEqual(["100%", "85%", "<1%", "10%", "<1%", "5%"],
                                         [text.get_text() for text in axes.texts])
                        self.assertTrue(all(text.get_fontsize() == 5 for text in axes.texts))
                        self.assertTrue(all(text.get_fontsize() == 11 for text in axes.get_legend().get_texts()))
                        external = [text for text in axes.texts if hasattr(text, "xyann")]
                        self.assertEqual([(7, 4), (7, 4), (7, -4), (7, -4)], [t.xyann for t in external])
                        marker = axes.lines[0]
                        original = axes.transData.transform((1, 0))
                        shifted = marker.get_transform().transform((1, 0))
                        self.assertAlmostEqual(4, (shifted[0] - original[0]) * 72 / figure.dpi)
                        self.assertAlmostEqual(4, (shifted[1] - original[1]) * 72 / figure.dpi)

    def test_figure_exports_preserve_configured_dimensions_and_shared_typography(self):
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib import font_manager
        from matplotlib.text import Text

        sizes = (operation_mix_config.FIGURE_SIZE, access_locality_config.FIGURE_SIZE,
                 update_locality_config.HEATMAP_FIGURE_SIZE, update_locality_config.COVERAGE_FIGURE_SIZE,
                 update_locality_config.BUSINESS_FIGURE_SIZE, update_locality_config.L0_FIGURE_SIZE)
        for width, height in sizes:
            with self.subTest(size=(width, height)), tempfile.TemporaryDirectory() as temporary:
                with matplotlib.rc_context():
                    global_config.apply_style()
                    fig, ax = plt.subplots(figsize=(width, height), layout="constrained")
                    try:
                        title = fig.suptitle("Figure title")
                        label = fig.supxlabel("Figure label")
                        ax.plot([0, 1], [1, 10], label="Series")
                        ax.set_yscale("log")
                        ax.set(title="Axes title", xlabel="X label", ylabel="Y label")
                        legend = ax.legend(title="Legend title")
                        annotation = ax.text(0.5, 2.5, "Annotation", fontsize=global_config.ANNOTATION_FONT_SIZE)
                        fig.canvas.draw()
                        expected_sizes = {
                            title: global_config.FIGURE_TITLE_FONT_SIZE,
                            label: global_config.FIGURE_LABEL_FONT_SIZE,
                            ax.title: global_config.AXES_TITLE_FONT_SIZE,
                            ax.xaxis.label: global_config.AXES_LABEL_FONT_SIZE,
                            ax.yaxis.label: global_config.AXES_LABEL_FONT_SIZE,
                            annotation: global_config.ANNOTATION_FONT_SIZE,
                            legend.get_title(): global_config.LEGEND_TITLE_FONT_SIZE,
                        }
                        expected_sizes.update({t: global_config.X_TICK_FONT_SIZE for t in ax.get_xticklabels()})
                        expected_sizes.update({t: global_config.Y_TICK_FONT_SIZE for t in ax.get_yticklabels()})
                        expected_sizes.update({t: global_config.LEGEND_FONT_SIZE for t in legend.get_texts()})
                        for text in fig.findobj(Text):
                            if text.get_text():
                                self.assertEqual(expected_sizes.get(text, global_config.BASE_FONT_SIZE), text.get_fontsize())
                                font = font_manager.findfont(text.get_fontproperties(), fallback_to_default=False)
                                self.assertEqual(global_config.FONT_FAMILY, font_manager.FontProperties(fname=font).get_name())
                        output = Path(temporary) / "figure"
                        COMMON.save_figure(fig, output)
                        png = plt.imread(output.with_suffix(".png"))
                        self.assertLessEqual(abs(png.shape[1] - width * 300), 1)
                        self.assertLessEqual(abs(png.shape[0] - height * 300), 1)
                        page = re.search(rb"/MediaBox\s*\[\s*0\s+0\s+([\d.]+)\s+([\d.]+)",
                                         output.with_suffix(".pdf").read_bytes())
                        self.assertIsNotNone(page)
                        self.assertAlmostEqual(width * 72, float(page[1]), places=5)
                        self.assertAlmostEqual(height * 72, float(page[2]), places=5)
                    finally:
                        plt.close(fig)

    def test_heatmap_uses_its_own_fixed_size_for_many_windows(self):
        import pandas as pd
        import matplotlib.pyplot as plt

        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "summary.json").write_text(json.dumps({
                "analysis": {"indexes": [{"table_id": 1, "index_id": 1, "name": "pk"}]}}))
            pd.DataFrame({"table": [1] * 30, "index": [1] * 30, "window": range(30),
                          "bin": [0] * 30, "count": range(1, 31)}).to_csv(directory / "heatmap.csv.gz", index=False)
            pd.DataFrame({"table": [1], "index": [1], "window": [1], "category": ["business"],
                          "cumulative_block_ratio": [0.5], "hit_blocks": [1], "blocks": [2]}).to_csv(
                              directory / "windows.csv", index=False)
            pd.DataFrame({"table": [1, 1], "index": [1, 1], "batch": [1, 2],
                          "hit_files": [2, 3], "span_files": [3, 4], "l1_files": [5, 5]}).to_csv(
                              directory / "l0-batches.csv", index=False)
            captured = {}
            save = COMMON.save_figure

            def capture(figure, path):
                captured[path.name] = figure
                save(figure, path)

            script = ROOT / "graph/scripts/trace/plot_update_locality.py"
            with patch.dict(sys.modules, {"_common": COMMON}), \
                    patch.object(sys, "argv", [str(script), str(directory), "--out", str(directory / "plots")]), \
                    patch.object(COMMON, "save_figure", side_effect=capture), \
                    patch.object(update_locality_config, "HEATMAP_FIGURE_SIZE", (4.1, 2.6)):
                self.assertEqual(0, runpy.run_path(str(script))["main"]())
            image = captured["heatmap-pk"].axes[0].images[0]
            self.assertEqual(update_locality_config.HEATMAP_STYLE["origin"], image.origin)
            self.assertEqual(update_locality_config.HEATMAP_STYLE["interpolation"], image.get_interpolation())
            for ax, template in zip(captured["coverage-pk"].axes, update_locality_config.COVERAGE_LABELS):
                self.assertEqual(template.format(category="business"), ax.lines[0].get_label())
            l0 = captured["l0-overlap-pk"].axes[0].lines
            self.assertEqual(["files with actual hits", "files in min..max range", "all L1 files"],
                             [line.get_label() for line in l0])
            self.assertEqual([[2, 3], [3, 4], [5, 5]], [list(line.get_ydata()) for line in l0])
            self.assertEqual(update_locality_config.L0_SERIES["l1_files"]["linestyle"], l0[2].get_linestyle())
            heatmap = plt.imread(directory / "plots/heatmap-pk.png")
            coverage = plt.imread(directory / "plots/coverage-pk.png")
            self.assertLessEqual(abs(heatmap.shape[1] - 4.1 * 300), 1)
            self.assertLessEqual(abs(heatmap.shape[0] - 2.6 * 300), 1)
            self.assertLessEqual(abs(coverage.shape[1] - update_locality_config.COVERAGE_FIGURE_SIZE[0] * 300), 1)
            self.assertLessEqual(abs(coverage.shape[0] - update_locality_config.COVERAGE_FIGURE_SIZE[1] * 300), 1)


if __name__ == "__main__":
    unittest.main()
