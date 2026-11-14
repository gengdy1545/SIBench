"""Update-locality figures: explicit dimensions in inches for each output."""

from graph.config.global_config import DOUBLE_COLUMN_WIDTH

HEATMAP_FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 3.2)
HEATMAP_TITLE = "{name}: update count by window and ordered block bin"
HEATMAP_X_LABEL = "block bin (0..{last_bin})"
HEATMAP_Y_LABEL = "business-change window"
HEATMAP_COLORBAR_LABEL = "modifications (log scale)"

COVERAGE_FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 4.2)
COVERAGE_TITLES = ("{name}\nCumulative block coverage", "{name}\nBlocks touched in each window")
COVERAGE_X_LABEL = "window"
COVERAGE_Y_LABEL = "fraction of baseline blocks"
COVERAGE_Y_LIMITS = (0, 1.02)
COVERAGE_LINE_STYLES = {"business": "-", "index": "--"}
COVERAGE_LEGEND_STYLE = {}

BUSINESS_FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 4.5)
BUSINESS_TITLE = "Business changes: cumulative block coverage"
BUSINESS_LEGEND_STYLE = {"ncol": 2}

L0_FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 3.8)
L0_TITLE = "{name}: assumed L0 file overlap with static L1"
L0_X_LABEL = "assumed L0 batch"
L0_Y_LABEL = "files"
L0_LEGEND_STYLE = {}
