"""Visual design: premium dark navy theme with restrained blue accents.

The base colours are also set in .streamlit/config.toml (so Streamlit's own widgets
match); this module adds card/typography CSS and one consistent Altair style.
"""

from __future__ import annotations

import html

import altair as alt

NAVY_950 = "#070D1A"  # page background
NAVY_900 = "#0B1426"  # cards
NAVY_800 = "#13203A"  # raised surfaces / hover
BORDER = "#1F2E4D"
TEXT = "#E6ECF5"
MUTED = "#8C9BB5"
ACCENT = "#3B82F6"  # primary blue
ACCENT_SOFT = "#60A5FA"
SUCCESS = "#34D399"
WARNING = "#FBBF24"
DANGER = "#F87171"

# Ordered blues for categorical series (restrained: one hue family).
SERIES = ["#3B82F6", "#60A5FA", "#93C5FD", "#1D4ED8", "#BFDBFE", "#2563EB"]
FONT = "Inter, 'Segoe UI', system-ui, -apple-system, sans-serif"

CSS = f"""
<style>
  html, body, [class*="css"] {{ font-family: {FONT}; }}
  .block-container {{ padding-top: 1.6rem; padding-bottom: 3rem; max-width: 1400px; }}
  h1, h2, h3 {{ letter-spacing: -0.01em; color: {TEXT}; }}
  .sf-header {{ display: flex; align-items: baseline; justify-content: space-between;
               gap: 1rem; flex-wrap: wrap; border-bottom: 1px solid {BORDER};
               padding-bottom: 0.9rem; margin-bottom: 1.2rem; }}
  .sf-title {{ font-size: 1.65rem; font-weight: 650; color: {TEXT}; margin: 0; }}
  .sf-title span {{ color: {ACCENT_SOFT}; }}
  .sf-subtitle {{ color: {MUTED}; font-size: 0.9rem; margin-top: 0.15rem; }}
  .sf-badge {{ font-size: 0.78rem; color: {MUTED}; border: 1px solid {BORDER};
              border-radius: 999px; padding: 0.25rem 0.75rem; background: {NAVY_900}; }}
  .sf-badge b {{ color: {TEXT}; font-weight: 600; }}
  .sf-section {{ color: {MUTED}; font-size: 0.75rem; font-weight: 600; letter-spacing: 0.08em;
                text-transform: uppercase; margin: 1.4rem 0 0.6rem; }}
  .sf-card {{ background: {NAVY_900}; border: 1px solid {BORDER}; border-radius: 12px;
             padding: 1rem 1.1rem; height: 100%; }}
  .sf-card .k {{ color: {MUTED}; font-size: 0.78rem; font-weight: 500; letter-spacing: 0.02em; }}
  .sf-card .v {{ color: {TEXT}; font-size: 1.55rem; font-weight: 650; margin-top: 0.35rem;
                font-variant-numeric: tabular-nums; white-space: nowrap; overflow: hidden;
                text-overflow: ellipsis; }}
  .sf-card .s {{ color: {MUTED}; font-size: 0.75rem; margin-top: 0.25rem; }}
  .sf-card.accent {{ border-top: 2px solid {ACCENT}; }}
  .sf-card.ok {{ border-top: 2px solid {SUCCESS}; }}
  .sf-card.warn {{ border-top: 2px solid {WARNING}; }}
  .sf-card.bad {{ border-top: 2px solid {DANGER}; }}
  .sf-empty {{ border: 1px dashed {BORDER}; border-radius: 12px; padding: 1.4rem;
              color: {MUTED}; text-align: center; font-size: 0.9rem; }}
  .sf-note {{ color: {MUTED}; font-size: 0.78rem; margin-top: 0.4rem; }}
  [data-testid="stSidebar"] {{ border-right: 1px solid {BORDER}; }}
  [data-testid="stTabs"] button p {{ font-size: 0.92rem; }}
</style>
"""


# Table column sizing. Streamlit sizes columns to their *cell* contents by default and
# ignores the header, which clips long headings such as "Payment success". Columns get
# an explicit width that fits both the header and the longest value; if the columns are
# narrower than the page they stretch to fill it, otherwise the table scrolls
# horizontally - nothing is clipped.
COL_CHAR_PX = 8.5  # approx. width of one character at the table font size
COL_PADDING_PX = 36  # cell padding + sort/menu icon space
COL_MIN_PX = 72
COL_MAX_PX = 420


def column_width(header: str, values: list[str]) -> int:
    """Pixel width that shows ``header`` and every (already formatted) value in full."""
    chars = max([len(str(header))] + [len(v) for v in values])
    return int(min(COL_MAX_PX, max(COL_MIN_PX, round(chars * COL_CHAR_PX + COL_PADDING_PX))))


TONES = frozenset({"accent", "ok", "warn", "bad"})


def card(title: str, value: str, subtitle: str = "", tone: str = "accent") -> str:
    """HTML for one KPI card. Text is HTML-escaped (defence in depth: callers pass
    formatted numbers/labels, never raw data) and the tone is whitelisted."""
    tone = tone if tone in TONES else "accent"
    sub = f'<div class="s">{html.escape(subtitle)}</div>' if subtitle else ""
    return (
        f'<div class="sf-card {tone}"><div class="k">{html.escape(title)}</div>'
        f'<div class="v">{html.escape(value)}</div>{sub}</div>'
    )


def style(chart: alt.Chart, height: int = 300) -> alt.Chart:
    """Apply the dashboard's chart style (transparent background, navy grid, muted axes)."""
    return (
        chart.properties(height=height, background="transparent")
        .configure_view(stroke=None)
        .configure_axis(
            labelColor=MUTED,
            titleColor=MUTED,
            gridColor=BORDER,
            domainColor=BORDER,
            tickColor=BORDER,
            labelFont=FONT,
            titleFont=FONT,
            labelFontSize=11,
            titleFontSize=11,
            titleFontWeight=500,
        )
        .configure_legend(
            labelColor=MUTED,
            titleColor=MUTED,
            labelFont=FONT,
            titleFont=FONT,
            orient="bottom",
            symbolType="circle",
        )
        .configure_title(color=TEXT, font=FONT, fontSize=13, fontWeight=600, anchor="start")
    )
