"""Shared data locations, analysis settings and appearance for SIBench figures."""

SIBENCH_JAR = "build/sibench/sibench.jar"
RUNTIME_CONFIG = "config/sibench.yaml"
ANALYSIS_DIRECTORY = "results/analysis/trace"
PLOT_DIRECTORY = "results/plots/trace"
REPLAY_PLOT_DIRECTORY = "results/plots/replay"

# Shared selection and display order for analysis preparation and comparison plots.
# Names resolve through RUNTIME_CONFIG; replay reports use the same names.
TRACES = [
    {"name": "hybench-pixels-sf10", "label": "HyBench / Pixels"},
    {"name": "sysbench-pixels-zipf099", "label": "Sysbench 0.99 / Pixels"},
    # Retained native Traces: enable the desired entries and select their runtime paths.
    # {"name": "hybench-tp", "label": "HyBench-TP"},
    # {"name": "hybench-xp", "label": "HyBench-XP"},
    # {"name": "sysbench-zipf080", "label": "sysbench-0.8"},
    # {"name": "sysbench-zipf099", "label": "sysbench-0.99"},
    # {"name": "ycsb-a", "label": "YCSB-A"},
    # {"name": "ycsb-b", "label": "YCSB-B"},
    # {"name": "ycsb-c", "label": "YCSB-C"},
    # {"name": "ycsb-d", "label": "YCSB-D"},
    # {"name": "ycsb-e", "label": "YCSB-E"},
    # {"name": "ycsb-f", "label": "YCSB-F"},
]

# Figure widths in inches, following retina-graph's layout constants.
SINGLE_COLUMN_WIDTH = 3.3374
DOUBLE_COLUMN_WIDTH = 2 * SINGLE_COLUMN_WIDTH
# Outer layout padding around labels and legends, in millimeters.
FIGURE_MARGIN_MM = 1.0

FONT_FAMILY = "Linux Libertine O"
BASE_FONT_SIZE = 10
FIGURE_TITLE_FONT_SIZE = 10
FIGURE_LABEL_FONT_SIZE = 10
AXES_TITLE_FONT_SIZE = 10
AXES_LABEL_FONT_SIZE = 10
X_TICK_FONT_SIZE = 10
Y_TICK_FONT_SIZE = 10
LEGEND_FONT_SIZE = 9
LEGEND_TITLE_FONT_SIZE = 9
ANNOTATION_FONT_SIZE = 9

# Morandi-inspired blue, sand, sage, rose, mauve, taupe and warm gray.
COLOR_PALETTE = ["#8FA3B0", "#C9B99D", "#A1AE98", "#B9908D",
                 "#ABA0B2", "#AA9787", "#B7B5AD"]
BACKGROUND_COLOR = "white"
MUTED_TEXT_COLOR = "0.4"
LINE_WIDTH = 1.8
HEATMAP_COLOR_MAP = "magma"
OUTPUT_FORMATS = ("pdf", "png")
SAVE_PARAMS = {"dpi": 300, "facecolor": BACKGROUND_COLOR}


def apply_style():
    import matplotlib as mpl

    mpl.rcParams.update({
        "font.family": FONT_FAMILY,
        "font.size": BASE_FONT_SIZE,
        "figure.titlesize": FIGURE_TITLE_FONT_SIZE,
        "figure.labelsize": FIGURE_LABEL_FONT_SIZE,
        "axes.titlesize": AXES_TITLE_FONT_SIZE,
        "axes.labelsize": AXES_LABEL_FONT_SIZE,
        "xtick.labelsize": X_TICK_FONT_SIZE,
        "ytick.labelsize": Y_TICK_FONT_SIZE,
        "legend.fontsize": LEGEND_FONT_SIZE,
        "legend.title_fontsize": LEGEND_TITLE_FONT_SIZE,
        "legend.frameon": False,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.axisbelow": True,
        "axes.prop_cycle": mpl.cycler(color=COLOR_PALETTE),
        "lines.linewidth": LINE_WIDTH,
        "figure.facecolor": BACKGROUND_COLOR,
        "figure.constrained_layout.use": True,
        "figure.constrained_layout.w_pad": FIGURE_MARGIN_MM / 25.4,
        "figure.constrained_layout.h_pad": FIGURE_MARGIN_MM / 25.4,
        "axes.facecolor": BACKGROUND_COLOR,
        "image.cmap": HEATMAP_COLOR_MAP,
        "savefig.bbox": None,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "mathtext.fontset": "custom",
        "mathtext.rm": FONT_FAMILY,
        "mathtext.it": f"{FONT_FAMILY}:italic",
        "mathtext.bf": f"{FONT_FAMILY}:bold",
        "mathtext.bfit": f"{FONT_FAMILY}:italic:bold",
        "mathtext.cal": f"{FONT_FAMILY}:italic",
        "mathtext.sf": FONT_FAMILY,
        "mathtext.tt": FONT_FAMILY,
    })
