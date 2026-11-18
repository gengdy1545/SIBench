"""Trace selection and appearance for per-table, per-operation access locality."""

from graph.config.global_config import ANNOTATION_FONT_SIZE, DOUBLE_COLUMN_WIDTH, MUTED_TEXT_COLOR

TRACES = [{'name': 'hybench-tp', 'label': 'HyBench-TP'},
 {'name': 'hybench-xp', 'label': 'HyBench-XP'},
 {'name': 'sysbench-zipf080', 'label': 'sysbench-0.8'},
 {'name': 'sysbench-zipf099', 'label': 'sysbench-0.99'},
 {'name': 'ycsb-a', 'label': 'YCSB-A'},
 {'name': 'ycsb-b', 'label': 'YCSB-B'},
 {'name': 'ycsb-c', 'label': 'YCSB-C'},
 {'name': 'ycsb-d', 'label': 'YCSB-D'},
 {'name': 'ycsb-e', 'label': 'YCSB-E'},
 {'name': 'ycsb-f', 'label': 'YCSB-F'}]

FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 4.8)
KEY_BINS = 256
WINDOWS_PER_HEATMAP_COLUMN = 10
HEATMAP_SHARE_LIMITS = (0.0001, 1)
X_LABEL = "Workload request sequence ({window_requests:,} requests per reuse window)"
REUSE_LINES = {
    "Repeat hit in this window": {"color": "#355c7d"},
    "Key hit in the previous window": {"color": "#b35440"},
}
EMPTY_TEXT_STYLE = {"ha": "center", "va": "center", "color": MUTED_TEXT_COLOR,
                    "fontsize": ANNOTATION_FONT_SIZE}
