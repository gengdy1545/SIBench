"""Appearance for the cumulative share of hits by PK popularity."""

from graph.config.access_coverage_config import COLOR_MAP, LINE_STYLES
from graph.config.global_config import DOUBLE_COLUMN_WIDTH

FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 3.5)
X_LABEL = "Share of most-accessed primary keys"
Y_LABEL = "Cumulative key-hit share"
TARGET_PERCENT = 90
CURVE_POINTS = 512
LEGEND_STYLE = {"loc": "upper left", "bbox_to_anchor": (0, 0.88), "ncol": 2}
