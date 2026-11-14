"""Trace selection and appearance for per-table, per-operation exact-PK locality."""

from graph.config.global_config import DOUBLE_COLUMN_WIDTH, MUTED_TEXT_COLOR

TRACES = [{'name': 'hybench-tp', 'label': 'HyBench-TP'},
 {'name': 'hybench-xp', 'label': 'HyBench-XP'},
 {'name': 'sysbench', 'label': 'sysbench'},
 {'name': 'ycsb-a', 'label': 'YCSB-A'},
 {'name': 'ycsb-b', 'label': 'YCSB-B'},
 {'name': 'ycsb-c', 'label': 'YCSB-C'},
 {'name': 'ycsb-d', 'label': 'YCSB-D'},
 {'name': 'ycsb-e', 'label': 'YCSB-E'},
 {'name': 'ycsb-f', 'label': 'YCSB-F'}]

FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 3)
X_LABEL = "Workload request sequence ({window_requests:,} requests/window)"
Y_LABEL = "Primary key"
POINT_STYLE = {"s": 1, "linewidths": 0, "rasterized": True}
KEY_TICKS = 5
EMPTY_TEXT_STYLE = {"ha": "center", "va": "center", "color": MUTED_TEXT_COLOR}
COLORBAR_STYLE = {"fraction": 0.035, "pad": 0.04,
                  "label": "Hits per PK per window (log)"}
