"""
Deloitte Color Theme for DDAnalyze visualizations.

Usage in analysis scripts:

    import deloitte_theme
    deloitte_theme.apply_deloitte_style()

    fig, ax = plt.subplots()
    ax.bar(x, y, color=deloitte_theme.COLORS[0])
    deloitte_theme.style_title(ax, "My Chart Title")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
"""

# ─── Deloitte Color Palette ───────────────────────────────────────────────────

GREEN_MEDIUM  = "#26890D"   # Primary green — chart titles & primary series
GREEN_DARK    = "#046A38"   # Dark green   — table column headings & accents
GREY          = "#404040"   # Grey          — secondary / neutral series
ELECTRIC_BLUE = "#0D8390"   # Electric blue — contrast series
AQUA_BLUE     = "#00ABAB"   # Aqua blue     — additional series

# Ordered palette for sequential series assignment
COLORS = [GREEN_MEDIUM, GREEN_DARK, GREY, ELECTRIC_BLUE, AQUA_BLUE]

TITLE_COLOR  = GREEN_MEDIUM   # Chart & figure titles
HEADER_COLOR = GREEN_DARK     # Table column-heading background (Word docs)

FONT_FAMILY  = "Arial"
_FONT_STACK  = [FONT_FAMILY, "Liberation Sans", "sans-serif"]


def apply_deloitte_style() -> None:
    """Apply Deloitte rcParams to matplotlib globally.

    Call once at the top of any analysis script (after setting the backend)
    and all subsequent figures will inherit the theme automatically.
    """
    import matplotlib.pyplot as plt

    plt.rcParams.update({
        # Font
        "font.family":           _FONT_STACK,
        "font.size":             10,
        # Title
        "axes.titlesize":        14,
        "axes.titleweight":      "bold",
        "axes.titlecolor":       TITLE_COLOR,
        # Color cycle
        "axes.prop_cycle":       plt.cycler("color", COLORS),
        # Spines
        "axes.spines.top":       False,
        "axes.spines.right":     False,
        # Backgrounds
        "figure.facecolor":      "white",
        "axes.facecolor":        "white",
        # Grid
        "grid.color":            "#E0E0E0",
        "grid.linestyle":        "--",
        "grid.linewidth":        0.5,
        # Tick labels
        "xtick.labelsize":       9,
        "ytick.labelsize":       9,
    })


def style_title(ax, title: str, fontsize: int = 14) -> None:
    """Set an axis title using the Deloitte green and Arial font stack."""
    ax.set_title(
        title,
        color=TITLE_COLOR,
        fontsize=fontsize,
        fontweight="bold",
        fontfamily=_FONT_STACK,
    )


def deloitte_colors(n: int = None) -> list:
    """Return the Deloitte color list, optionally limited to the first *n* items."""
    return COLORS[:n] if n else list(COLORS)
