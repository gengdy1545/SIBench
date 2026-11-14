"""Benchmark selection and appearance for the workload operation-mix figure."""

from graph.config.global_config import ANNOTATION_FONT_SIZE, BACKGROUND_COLOR, SINGLE_COLUMN_WIDTH

TRACES = [{'name': 'hybench-tp', 'label': 'HyBench-TP'},
 {'name': 'hybench-xp', 'label': 'HyBench-XP'},
 {'name': 'sysbench', 'label': 'sysbench'},
 {'name': 'ycsb-a', 'label': 'YCSB-A'},
 {'name': 'ycsb-b', 'label': 'YCSB-B'},
 {'name': 'ycsb-c', 'label': 'YCSB-C'},
 {'name': 'ycsb-d', 'label': 'YCSB-D'},
 {'name': 'ycsb-e', 'label': 'YCSB-E'},
 {'name': 'ycsb-f', 'label': 'YCSB-F'}]

FIGURE_SIZE = (SINGLE_COLUMN_WIDTH, 2.6)
X_LABEL = "Share of replay operations"
X_TICKS = (0, 0.2, 0.4, 0.6, 0.8, 1)
BAR_HEIGHT = 0.7
BAR_STYLE = {"edgecolor": BACKGROUND_COLOR, "linewidth": 0.4}
Y_TICK_LABEL_STYLE = {"rotation": 0}
PERCENT_LABEL_STYLE = {"rotation": 0, "fontsize": ANNOTATION_FONT_SIZE}
PERCENT_MARKER_STYLE = {"marker": "s", "markersize": 2.5, "markeredgewidth": 0, "linestyle": "none"}
GRID_STYLE = {"axis": "x", "alpha": 0.2}
LEGEND_STYLE = {"loc": "lower center", "bbox_to_anchor": (0.5, 1), "ncol": 4,
                "handlelength": 1, "handletextpad": 0.4, "columnspacing": 1,
                "borderpad": 0.1, "borderaxespad": 0.2}
