"""Trace selection and appearance for per-table, per-operation exact-PK locality."""

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

FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 3)
X_LABEL = "Workload request sequence ({window_requests:,} requests/window)"
Y_LABEL = "Primary key"
POINT_STYLE = {"s": 1, "linewidths": 0, "rasterized": True}
KEY_TICKS = 5
Y_PADDING_FRACTION = 0.02
MIN_Y_PADDING = 0.5
Y_TICK_FORMAT = {"style": "plain", "useOffset": False}
EMPTY_TEXT = "No hits"
EMPTY_TEXT_POSITION = (0.5, 0.5)
EMPTY_TEXT_STYLE = {"ha": "center", "va": "center", "color": MUTED_TEXT_COLOR,
                    "fontsize": ANNOTATION_FONT_SIZE}
COLORBAR_STYLE = {"fraction": 0.035, "pad": 0.04,
                  "label": "Hits per PK per window (log)"}
