"""Selection and appearance for cumulative primary-key read coverage."""

from graph.config.access_locality_config import TRACES
from graph.config.global_config import DOUBLE_COLUMN_WIDTH

FIGURE_SIZE = (DOUBLE_COLUMN_WIDTH, 3.5)
X_LABEL = "Cumulative workload requests"
Y_LABEL = "Cumulative key coverage"
COLOR_MAP = "tab10"
LINE_STYLES = ("-", "--", "-.", ":")
LEGEND_STYLE = {"loc": "lower right", "ncol": 2}
