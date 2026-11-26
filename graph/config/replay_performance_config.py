"""Replay results, backend selection and appearance for the replay performance figure."""

from graph.config.global_config import DOUBLE_COLUMN_WIDTH, MUTED_TEXT_COLOR

# One sweep directory holding <trace>/<backend>/report.json for every selected pair.
RESULTS_DIRECTORY = "results/replay"

# Colors follow list order through COLOR_PALETTE; the hatch is the non-color identity cue.
BACKENDS = [
    {'name': 'slatedb', 'label': 'SlateDB', 'hatch': ''},
    {'name': 'rocksdb_cloud', 'label': 'RocksDB-Cloud', 'hatch': '////'},
]

# Fields of report.json "result"; a latency with zero samples is drawn as n/a.
METRICS = [
    {'name': 'load_throughput', 'path': ('baseline', 'operations_per_second'), 'divisor': 1000,
     'label': 'Load\n(Kops/s)', 'scale': 'linear'},
    {'name': 'workload_throughput', 'path': ('workload', 'operations_per_second'), 'divisor': 1000,
     'label': 'Workload\n(Kops/s)', 'scale': 'linear'},
    {'name': 'read_p50', 'path': ('workload', 'dispatch', 'search_latency', 'p50_ms'), 'divisor': 1,
     'label': 'Read P50\n(ms)', 'scale': 'log'},
    {'name': 'read_p99', 'path': ('workload', 'dispatch', 'search_latency', 'p99_ms'), 'divisor': 1,
     'label': 'Read P99\n(ms)', 'scale': 'log'},
    {'name': 'write_p50', 'path': ('workload', 'dispatch', 'write_latency', 'p50_ms'), 'divisor': 1,
     'label': 'Write P50\n(ms)', 'scale': 'log'},
    {'name': 'write_p99', 'path': ('workload', 'dispatch', 'write_latency', 'p99_ms'), 'divisor': 1,
     'label': 'Write P99\n(ms)', 'scale': 'log'},
]

FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 8.0)
OUTPUT_NAME = "replay_performance"
GROUP_WIDTH = 0.8
BAR_STYLE = {"edgecolor": "0.3", "linewidth": 0.4}
X_TICK_LABEL_STYLE = {"rotation": 30, "ha": "right", "rotation_mode": "anchor"}
MISSING_LABEL = "n/a"
MISSING_LABEL_STYLE = {"fontsize": 7, "color": MUTED_TEXT_COLOR, "ha": "center", "va": "bottom"}
GRID_STYLE = {"axis": "y", "alpha": 0.2}
# Log-axis ticks at these multiples of each decade, labeled as plain numbers.
LOG_TICK_SUBS = (1, 2, 5)
LEGEND_STYLE = {"loc": "lower center", "bbox_to_anchor": (0.5, 1), "ncol": 4,
                "handlelength": 1.5, "handletextpad": 0.4, "columnspacing": 1.2,
                "borderpad": 0.1, "borderaxespad": 0.2}
