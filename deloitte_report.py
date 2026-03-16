"""
Deloitte Report — Premium HTML Report Generator

Multi-stage pipeline that transforms analysis results into a polished,
print-ready HTML report with embedded Chart.js visualizations and
Deloitte visual identity.

Pipeline:
  Stage 1 (Planner):   Read all context + archive + final_report → design report blueprint (JSON)
  Stage 2 (Sections):  For each planned section, generate HTML content + chart specs
  Stage 3 (Assembler): Stitch sections into a single self-contained HTML file

Run after loop.py and phase2.py have completed, or standalone with existing data.
"""

import json
import logging
import os
import re
import sys
import time
import base64
from datetime import datetime
from pathlib import Path
from typing import Optional

import anthropic
import yaml

# Change to the directory containing this script so relative paths work
os.chdir(Path(__file__).parent)

from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

# ─── Logging Setup ────────────────────────────────────────────────────────────

logger = logging.getLogger("ddanalyze.deloitte_report")


def setup_logging(debug: bool = False, log_dir: str = "logs") -> Path:
    """Configure console + file logging. Returns the path of the log file."""
    Path(log_dir).mkdir(exist_ok=True)
    log_file = Path(log_dir) / f"deloitte_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    console_level = logging.DEBUG if debug else logging.INFO

    root = logging.getLogger("ddanalyze")
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level)
    ch.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    root.addHandler(ch)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)-7s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(fh)

    return log_file


# ─── Deloitte Brand Constants ────────────────────────────────────────────────

DELOITTE_GREEN_MEDIUM = "#26890D"
DELOITTE_GREEN_DARK = "#046A38"
DELOITTE_BLACK = "#000000"
DELOITTE_GREY = "#404040"
DELOITTE_ELECTRIC_BLUE = "#0D8390"
DELOITTE_AQUA = "#00ABAB"
DELOITTE_LIGHT_GREY = "#F2F2F2"
DELOITTE_WHITE = "#FFFFFF"

CHART_COLORS = [
    DELOITTE_GREEN_MEDIUM,
    DELOITTE_GREEN_DARK,
    DELOITTE_GREY,
    DELOITTE_ELECTRIC_BLUE,
    DELOITTE_AQUA,
    "#86BC25",  # Deloitte lime
    "#43B02A",  # Deloitte bright green
    "#009A44",  # Deloitte forest green
]


# ─── File Utilities ───────────────────────────────────────────────────────────

def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_file(path: str, content: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]"


# ─── Archive Parser (shared logic with phase2) ───────────────────────────────

_INTERNAL_ERROR_LABELS = {
    "JSON_PARSE_ERROR", "CRITIC_JSON_PARSE_ERROR", "TIMEOUT", "FATAL_ERROR",
}


def parse_archive(archive_text: str) -> list:
    """Parse full_archive.txt into a list of dicts for all SUCCESS entries."""
    separator = "=" * 80
    entries = []

    for block in archive_text.split(separator):
        block = block.strip()
        if not block or "ITERATION:" not in block:
            continue

        status_match = re.search(r"\nSTATUS:\s*(\S+)", block)
        if not status_match:
            continue
        status = status_match.group(1).strip()
        if status.upper() in _INTERNAL_ERROR_LABELS:
            continue
        if status.lower() != "success":
            continue

        entry: dict = {"status": status}

        for field, pattern in [
            ("iteration", r"ITERATION:\s*(\d+)"),
            ("date", r"DATE:\s*(.+)"),
            ("analysis_type", r"ANALYSIS TYPE:\s*(.+)"),
            ("hypothesis", r"HYPOTHESIS:\s*(.+)"),
            ("columns_used", r"COLUMNS USED:\s*(.+)"),
        ]:
            m = re.search(pattern, block)
            if m:
                entry[field] = m.group(1).strip()

        dash_sep = "-" * 80
        code_m = re.search(
            r"CODE:\n(.*?)\n" + re.escape(dash_sep), block, re.DOTALL
        )
        if code_m:
            entry["code"] = code_m.group(1).strip()

        output_m = re.search(
            r"OUTPUT:\n(.*?)(?:\n" + re.escape(dash_sep) + r"|\Z)", block, re.DOTALL
        )
        if output_m:
            entry["output"] = output_m.group(1).strip()

        eval_m = re.search(r"EVALUATION:\n(.*?)(?:\Z)", block, re.DOTALL)
        if eval_m:
            entry["evaluation"] = eval_m.group(1).strip()

        if "iteration" in entry:
            entries.append(entry)

    logger.info(f"[Archive] Parsed {len(entries)} successful entries")
    return entries


def list_available_graphs(graphs_folder: str) -> list:
    """Return list of (filepath, filename) for all PNG files in the graphs folder."""
    folder = Path(graphs_folder)
    if not folder.exists():
        return []
    return [(str(p), p.name) for p in sorted(folder.glob("*.png"))]


def encode_graph_base64(filepath: str) -> str:
    """Encode a PNG file as a base64 data URI for embedding in HTML."""
    try:
        with open(filepath, "rb") as f:
            data = base64.b64encode(f.read()).decode("utf-8")
        return f"data:image/png;base64,{data}"
    except Exception as e:
        logger.warning(f"Failed to encode graph {filepath}: {e}")
        return ""


# ─── LLM Call Helper ─────────────────────────────────────────────────────────

def _llm_call(
    client,
    model: str,
    system_prompt: str,
    user_message: str,
    max_tokens: int,
    label: str = "",
) -> tuple:
    """Make a single streaming LLM call. Returns (text, usage)."""
    logger.info(
        f"  [{label}] Sending request ({len(user_message):,}ch prompt, max_tokens={max_tokens})"
    )

    text = ""
    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    ) as stream:
        for chunk in stream.text_stream:
            print(chunk, end="", flush=True)
        response = stream.get_final_message()
    print()

    for block in response.content:
        if block.type == "text":
            text = block.text
            break

    usage = response.usage
    logger.info(
        f"  [{label}] Done — {len(text):,}ch | "
        f"tokens in={usage.input_tokens:,} out={usage.output_tokens:,}"
    )
    return text, usage


# ─── Stage 1: Report Planner ─────────────────────────────────────────────────

PLANNER_SYSTEM_PROMPT = """\
You are a senior report designer at Deloitte creating the blueprint for a premium HTML \
report. You will receive analysis data from a due diligence engagement and must design \
a visually stunning, print-ready report structure.

Output ONLY a single valid JSON object — no markdown fences, no commentary.

The JSON schema:
{
  "report_title": "string — main title for the report",
  "report_subtitle": "string — subtitle (e.g. 'Due Diligence Analysis' or date range)",
  "executive_summary": {
    "key_metrics": [
      {
        "label": "string — metric name (e.g. 'Total Revenue')",
        "value": "string — formatted value (e.g. '€62.5m')",
        "detail": "string — context (e.g. 'Jan 2021 – Apr 2023')",
        "trend": "up|down|neutral — direction indicator"
      }
    ],
    "narrative_guidance": "string — what the summary paragraph should cover"
  },
  "sections": [
    {
      "section_number": int,
      "title": "string — section heading",
      "description": "string — what this section covers",
      "iterations": [int, ...],
      "charts": [
        {
          "chart_id": "string — unique ID for the chart (e.g. 'revenue_trend_bar')",
          "chart_type": "bar|line|doughnut|horizontalBar|stackedBar|stackedArea|waterfall",
          "title": "string — chart title",
          "data_guidance": "string — describe what data to extract from the analysis outputs \
to populate labels and datasets. Be specific about which numbers from which iteration outputs \
to use. The section writer will use this to construct Chart.js data objects.",
          "height": "string — CSS height (e.g. '320px', '400px')"
        }
      ],
      "tables": [
        {
          "table_id": "string — unique ID",
          "title": "string — table caption",
          "data_guidance": "string — describe what rows/columns to extract from analysis outputs"
        }
      ],
      "narrative_guidance": "string — what the prose should cover, tone, key points to highlight",
      "graphs_to_embed": ["filename.png", ...],
      "callout_boxes": [
        {
          "type": "insight|risk|highlight",
          "text_guidance": "string — what the callout should say"
        }
      ]
    }
  ],
  "glossary_terms": ["term1", "term2", ...]
}

Rules:
1. Design 4-8 sections ordered by business logic: revenue/growth first, then product, \
customer/channel, risk/concentration, then remaining themes.
2. For each section, plan 1-3 charts that best visualize the key findings. Choose chart \
types that are most effective for the data:
   - bar/horizontalBar for comparisons
   - line for time series / trends
   - doughnut for composition / share
   - stackedBar for decomposition over time
   - waterfall for bridge/variance analysis
3. Plan 3-6 key metrics for the executive summary hero section (large KPI cards at the top).
4. Include callout boxes for the most important insights or risk flags.
5. Map existing graph PNG files to sections where relevant (they'll be embedded as images).
6. All monetary values use € prefix with lowercase suffix (€16.7m, not EUR 16.7M).
7. The report is for PRINT — no interactive elements needed, but charts should render beautifully.
8. Output ONLY the JSON object."""


def build_planner_prompt(
    entries: list,
    context_text: str,
    final_report_text: str,
    available_graphs: list,
) -> str:
    """Build the prompt for Stage 1 — the Planner."""
    analyses_summary = []
    for e in entries:
        analyses_summary.append(
            f"- Iteration {e.get('iteration', '?')}: "
            f"[{e.get('analysis_type', 'Unknown')}] {e.get('hypothesis', '')}"
            f"\n  Output excerpt: {_truncate(e.get('output', ''), 1200)}"
            f"\n  Evaluation excerpt: {_truncate(e.get('evaluation', ''), 600)}"
        )
    analyses_text = "\n".join(analyses_summary)

    graphs_text = ""
    if available_graphs:
        graphs_text = "\n## Available Graph Images\n" + "\n".join(
            f"- {fname}" for _, fname in available_graphs
        )

    report_excerpt = ""
    if final_report_text:
        report_excerpt = f"\n## Existing Final Report (for reference)\n{_truncate(final_report_text, 15000)}"

    return f"""## Knowledge Base
{context_text}
{graphs_text}
{report_excerpt}

## Successful Analyses ({len(entries)} total)
{analyses_text}

---

Design the premium Deloitte report structure. Output a single JSON object following the schema."""


def parse_planner_json(raw_text: str) -> dict:
    """Parse the Planner's JSON output."""
    text = raw_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)

    blueprint = json.loads(text)

    if "sections" not in blueprint or not isinstance(blueprint["sections"], list):
        raise ValueError("Blueprint JSON missing 'sections' array")
    for sec in blueprint["sections"]:
        sec.setdefault("section_number", 0)
        sec.setdefault("charts", [])
        sec.setdefault("tables", [])
        sec.setdefault("narrative_guidance", "")
        sec.setdefault("graphs_to_embed", [])
        sec.setdefault("callout_boxes", [])
    blueprint.setdefault("report_title", "Due Diligence Analysis")
    blueprint.setdefault("report_subtitle", "Confidential")
    blueprint.setdefault("executive_summary", {"key_metrics": [], "narrative_guidance": ""})
    blueprint.setdefault("glossary_terms", [])

    return blueprint


# ─── Stage 2: Section Writer ─────────────────────────────────────────────────

SECTION_WRITER_SYSTEM_PROMPT = """\
You are a senior consultant writing ONE section of a premium Deloitte HTML report. \
You generate structured JSON that will be rendered into beautiful HTML.

Output a single valid JSON object — no markdown fences, no commentary.

The JSON schema:
{
  "section_html": "string — the full HTML content for this section. Use these elements:\\n\
    - <h2 class='section-title'>Title</h2> for main heading\\n\
    - <h3 class='subsection-title'>Subtitle</h3> for sub-headings\\n\
    - <p class='body-text'>...</p> for narrative paragraphs\\n\
    - <div class='basis-of-prep'><h4>Basis of Preparation</h4><p>...</p></div> for methodology\\n\
    - <div class='callout callout-insight'>...</div> or callout-risk for callout boxes\\n\
    - <div class='table-container'><table class='data-table'>...</table></div> for tables\\n\
    - Tables MUST use <thead> with <th> for headers and <tbody> with <td> for data rows\\n\
    - <div class='graph-embed'><img src='GRAPH_PLACEHOLDER:filename.png' /></div> for graph images\\n\
    - Use <strong> for bold, <em> for emphasis\\n\
    - For chart placeholders: <div class='chart-wrapper'><canvas id='CHART_ID'></canvas></div>",

  "charts_data": [
    {
      "chart_id": "string — must match the canvas id in section_html",
      "chart_type": "bar|line|doughnut|horizontalBar|pie",
      "title": "string",
      "labels": ["label1", "label2", ...],
      "datasets": [
        {
          "label": "string — dataset name",
          "data": [number, number, ...],
          "backgroundColor": "string or array — hex color(s)",
          "borderColor": "string — hex color (for line charts)"
        }
      ],
      "options": {
        "y_prefix": "string — prefix for y-axis labels, e.g. '€'",
        "y_suffix": "string — suffix, e.g. 'm'",
        "show_legend": true,
        "stacked": false,
        "index_axis": "x|y"
      }
    }
  ]
}

Rules:
1. Write in formal senior consulting register. No conversational phrases.
2. All monetary values: €-prefix, lowercase suffix (€16.7m, ~€0.57m). NEVER use "EUR".
3. Every sub-section MUST open with a Basis of Preparation paragraph.
4. Tables must have proper <thead>/<tbody> structure.
5. Chart data must use REAL NUMBERS extracted from the analysis outputs provided.
   Do NOT invent data. If exact numbers aren't available, omit the chart.
6. Be thorough: 300-600 words of prose per sub-section (excluding tables).
7. Use callout boxes for key insights or risk flags.
8. For chart colors, use these Deloitte brand colors in order:
   ["#26890D", "#046A38", "#404040", "#0D8390", "#00ABAB", "#86BC25", "#43B02A", "#009A44"]
9. Output ONLY the JSON object."""


def build_section_writer_prompt(
    section: dict,
    section_entries: list,
    context_text: str,
    section_graphs: list,
    previous_titles: list,
) -> str:
    """Build the prompt for Stage 2 — writing one section."""
    parts = []
    for e in section_entries:
        part = f"""--- ANALYSIS {e.get('iteration', '?')} ---
Type       : {e.get('analysis_type', '')}
Hypothesis : {e.get('hypothesis', '')}
Columns    : {e.get('columns_used', '')}

Output:
{_truncate(e.get('output', ''), 3000)}

Evaluation:
{_truncate(e.get('evaluation', ''), 1200)}
"""
        parts.append(part)
    analyses_text = "\n\n".join(parts)

    graphs_text = ""
    if section_graphs:
        graphs_text = "\n## Graph Images Available for This Section\n" + "\n".join(
            f"- {fname} (use src='GRAPH_PLACEHOLDER:{fname}' in img tags)"
            for _, fname in section_graphs
        )

    prev_text = ""
    if previous_titles:
        prev_text = (
            "\n## Sections Already Written (avoid repeating their content)\n"
            + "\n".join(f"- {t}" for t in previous_titles)
        )

    charts_guidance = ""
    if section.get("charts"):
        charts_guidance = "\n## Charts to Include in This Section\n"
        for chart in section["charts"]:
            charts_guidance += (
                f"- Chart ID: {chart['chart_id']} | Type: {chart['chart_type']} | "
                f"Title: {chart['title']}\n"
                f"  Data guidance: {chart.get('data_guidance', 'Extract from analysis outputs')}\n"
            )

    tables_guidance = ""
    if section.get("tables"):
        tables_guidance = "\n## Tables to Include\n"
        for tbl in section["tables"]:
            tables_guidance += (
                f"- {tbl.get('title', 'Table')}: {tbl.get('data_guidance', '')}\n"
            )

    callout_guidance = ""
    if section.get("callout_boxes"):
        callout_guidance = "\n## Callout Boxes to Include\n"
        for cb in section["callout_boxes"]:
            callout_guidance += f"- [{cb.get('type', 'insight')}]: {cb.get('text_guidance', '')}\n"

    return f"""## Section to Write
Title: {section['title']}
Description: {section.get('description', '')}
Narrative Guidance: {section.get('narrative_guidance', '')}
{charts_guidance}
{tables_guidance}
{callout_guidance}
{graphs_text}
{prev_text}

## Knowledge Base (for context and grounding)
{context_text}

## Analyses Assigned to This Section ({len(section_entries)} total)

{analyses_text}

---

Generate the JSON for this section now. Include real data in charts. Be thorough and detailed."""


# ─── Stage 2b: Executive Summary Writer ──────────────────────────────────────

EXEC_SUMMARY_WRITER_SYSTEM_PROMPT = """\
You are writing the Executive Summary section for a premium Deloitte HTML report.

Output a single valid JSON object — no markdown fences, no commentary.

The JSON schema:
{
  "summary_html": "string — HTML for the executive summary narrative. Use:\\n\
    - <div class='exec-summary-narrative'><p>...</p></div> for the overview\\n\
    - <ul class='exec-bullets'><li>...</li></ul> for key findings (5-7 bullets)\\n\
    - Each bullet MUST include a cross-reference: <span class='section-ref'>(see Section N: Title)</span>\\n\
    - <div class='exec-outlook'><h3>Outlook & Key Risks</h3><p>...</p></div> for forward-looking section\\n\
    - Use <strong> for emphasis on key figures"
}

Rules:
1. CURRENCY: € prefix, lowercase suffix (€16.7m). NEVER "EUR".
2. Each bullet must cross-reference a section number and title.
3. Include specific numbers — no vague statements.
4. Formal, senior consulting register.
5. Output ONLY the JSON object."""


def build_exec_summary_prompt(
    blueprint: dict,
    section_titles: list,
    context_text: str,
) -> str:
    """Build the prompt for the executive summary."""
    section_list = "\n".join(
        f"- Section {i + 1}: {t}" for i, t in enumerate(section_titles)
    )

    metrics_text = ""
    exec_config = blueprint.get("executive_summary", {})
    if exec_config.get("key_metrics"):
        metrics_text = "\n## Key Metrics Already Designed (for the hero cards above the summary)\n"
        for m in exec_config["key_metrics"]:
            metrics_text += f"- {m.get('label', '')}: {m.get('value', '')} ({m.get('detail', '')})\n"

    return f"""## Report Structure (sections for cross-references)
{section_list}

## Summary Guidance from Planner
{exec_config.get('narrative_guidance', 'Cover the 5-7 most important findings.')}
{metrics_text}

## Knowledge Base
{context_text}

---

Write the executive summary JSON now. Each bullet must reference a section."""


# ─── Stage 2c: Glossary Writer ───────────────────────────────────────────────

GLOSSARY_WRITER_SYSTEM_PROMPT = """\
You are compiling the Glossary for a Deloitte report. Output a single valid JSON object.

The JSON schema:
{
  "glossary_entries": [
    {
      "term": "string — the term or abbreviation",
      "definition": "string — concise but complete definition (1-2 sentences)"
    }
  ]
}

Rules:
1. Sort alphabetically by term.
2. Include every abbreviation, internal term, cohort name, product line, metric.
3. For cohort names, include classification criteria.
4. Output ONLY the JSON object."""


def build_glossary_prompt(blueprint: dict, section_titles: list, context_text: str) -> str:
    suggested = blueprint.get("glossary_terms", [])
    terms_text = "\n".join(f"- {t}" for t in suggested) if suggested else "None provided"

    return f"""## Suggested Terms
{terms_text}

## Knowledge Base (scan for additional terms)
{context_text}

## Report Sections
{chr(10).join(f'- {t}' for t in section_titles)}

---

Generate the glossary JSON now. Include all terms used in the report."""


# ─── HTML Template ────────────────────────────────────────────────────────────

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{report_title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
  /* ─── Reset & Base ─────────────────────────────────────────────── */
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  :root {{
    --green-medium: {green_medium};
    --green-dark: {green_dark};
    --grey: {grey};
    --electric-blue: {electric_blue};
    --aqua: {aqua};
    --light-grey: {light_grey};
    --black: {black};
    --white: {white};
  }}

  body {{
    font-family: 'Arial', 'Helvetica Neue', Helvetica, sans-serif;
    font-size: 10.5pt;
    line-height: 1.55;
    color: var(--black);
    background: var(--white);
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
  }}

  /* ─── Page Container ───────────────────────────────────────────── */
  .report-container {{
    max-width: 210mm;
    margin: 0 auto;
    padding: 0 20mm;
  }}

  /* ─── Cover Page ───────────────────────────────────────────────── */
  .cover-page {{
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    justify-content: center;
    padding: 60mm 0;
    page-break-after: always;
  }}

  .cover-accent {{
    width: 80px;
    height: 6px;
    background: var(--green-medium);
    margin-bottom: 32px;
  }}

  .cover-title {{
    font-size: 36pt;
    font-weight: 700;
    color: var(--black);
    line-height: 1.15;
    margin-bottom: 16px;
    letter-spacing: -0.5px;
  }}

  .cover-subtitle {{
    font-size: 16pt;
    font-weight: 400;
    color: var(--grey);
    margin-bottom: 48px;
  }}

  .cover-meta {{
    font-size: 10pt;
    color: var(--grey);
    border-top: 1px solid #E0E0E0;
    padding-top: 20px;
  }}

  .cover-meta span {{
    display: block;
    margin-bottom: 4px;
  }}

  .cover-deloitte {{
    color: var(--green-dark);
    font-weight: 700;
    font-size: 11pt;
  }}

  /* ─── KPI Hero Section ─────────────────────────────────────────── */
  .kpi-hero {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 16px;
    margin: 32px 0;
    page-break-inside: avoid;
  }}

  .kpi-card {{
    background: var(--light-grey);
    border-left: 4px solid var(--green-medium);
    padding: 18px 16px;
    border-radius: 2px;
  }}

  .kpi-card .kpi-value {{
    font-size: 22pt;
    font-weight: 700;
    color: var(--green-dark);
    line-height: 1.2;
  }}

  .kpi-card .kpi-label {{
    font-size: 8.5pt;
    font-weight: 600;
    color: var(--grey);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
  }}

  .kpi-card .kpi-detail {{
    font-size: 8pt;
    color: var(--grey);
    margin-top: 4px;
  }}

  .kpi-card .kpi-trend {{
    font-size: 9pt;
    font-weight: 600;
    margin-top: 2px;
  }}

  .kpi-trend.up {{ color: var(--green-medium); }}
  .kpi-trend.down {{ color: #D32F2F; }}
  .kpi-trend.neutral {{ color: var(--grey); }}

  /* ─── Executive Summary ────────────────────────────────────────── */
  .exec-summary {{
    margin: 24px 0 40px;
    page-break-after: always;
  }}

  .exec-summary-narrative p {{
    font-size: 11pt;
    line-height: 1.65;
    margin-bottom: 16px;
  }}

  .exec-bullets {{
    list-style: none;
    padding: 0;
    margin: 20px 0;
  }}

  .exec-bullets li {{
    position: relative;
    padding: 12px 16px 12px 28px;
    margin-bottom: 10px;
    background: var(--light-grey);
    border-radius: 2px;
    font-size: 10.5pt;
    line-height: 1.55;
  }}

  .exec-bullets li::before {{
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 4px;
    background: var(--green-medium);
    border-radius: 2px 0 0 2px;
  }}

  .section-ref {{
    color: var(--electric-blue);
    font-style: italic;
    font-size: 9.5pt;
  }}

  .exec-outlook {{
    margin-top: 28px;
    padding: 20px;
    background: linear-gradient(135deg, #f8fdf6 0%, #f0f9ec 100%);
    border-left: 4px solid var(--green-dark);
    border-radius: 2px;
  }}

  .exec-outlook h3 {{
    color: var(--green-dark);
    font-size: 13pt;
    margin-bottom: 12px;
  }}

  /* ─── Section Headings ─────────────────────────────────────────── */
  .section-divider {{
    page-break-before: always;
    margin-top: 48px;
  }}

  .section-divider:first-of-type {{
    page-break-before: auto;
  }}

  h2.section-title {{
    font-size: 20pt;
    font-weight: 700;
    color: var(--green-dark);
    margin: 40px 0 8px;
    padding-bottom: 10px;
    border-bottom: 3px solid var(--green-medium);
  }}

  h3.subsection-title {{
    font-size: 13pt;
    font-weight: 700;
    color: var(--green-dark);
    margin: 28px 0 10px;
    padding-bottom: 6px;
    border-bottom: 1px solid #E0E0E0;
  }}

  h4 {{
    font-size: 11pt;
    font-weight: 700;
    color: var(--grey);
    margin: 20px 0 8px;
  }}

  /* ─── Body Text ────────────────────────────────────────────────── */
  p.body-text {{
    margin-bottom: 14px;
    text-align: justify;
    hyphens: auto;
  }}

  /* ─── Basis of Preparation ─────────────────────────────────────── */
  .basis-of-prep {{
    background: #f8f9fa;
    border-left: 3px solid var(--grey);
    padding: 14px 18px;
    margin: 16px 0;
    border-radius: 0 2px 2px 0;
  }}

  .basis-of-prep h4 {{
    font-size: 10pt;
    font-weight: 700;
    color: var(--grey);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin: 0 0 8px;
  }}

  .basis-of-prep p {{
    font-size: 9.5pt;
    color: #555;
    margin: 0;
    line-height: 1.5;
  }}

  /* ─── Callout Boxes ────────────────────────────────────────────── */
  .callout {{
    padding: 16px 20px;
    margin: 20px 0;
    border-radius: 2px;
    page-break-inside: avoid;
    font-size: 10pt;
  }}

  .callout-insight {{
    background: #f0f9ec;
    border-left: 4px solid var(--green-medium);
  }}

  .callout-risk {{
    background: #fff3f0;
    border-left: 4px solid #D32F2F;
  }}

  .callout-highlight {{
    background: #e8f6f7;
    border-left: 4px solid var(--electric-blue);
  }}

  .callout strong:first-child {{
    display: block;
    margin-bottom: 4px;
    font-size: 10.5pt;
  }}

  /* ─── Data Tables ──────────────────────────────────────────────── */
  .table-container {{
    margin: 20px 0;
    overflow-x: auto;
    page-break-inside: avoid;
  }}

  .data-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 9.5pt;
  }}

  .data-table thead th {{
    background: var(--green-dark);
    color: var(--white);
    font-weight: 700;
    padding: 10px 12px;
    text-align: left;
    font-size: 9pt;
    text-transform: uppercase;
    letter-spacing: 0.3px;
    border: none;
  }}

  .data-table tbody td {{
    padding: 8px 12px;
    border-bottom: 1px solid #E8E8E8;
    vertical-align: top;
  }}

  .data-table tbody tr:nth-child(even) {{
    background: #FAFAFA;
  }}

  .data-table tbody tr:last-child {{
    border-bottom: 2px solid var(--green-dark);
  }}

  .data-table tbody tr.total-row {{
    font-weight: 700;
    background: var(--light-grey);
    border-top: 2px solid var(--green-dark);
  }}

  .data-table tbody tr.total-row td {{
    border-bottom: 2px solid var(--green-dark);
  }}

  /* ─── Charts ───────────────────────────────────────────────────── */
  .chart-wrapper {{
    margin: 24px 0;
    padding: 16px;
    background: var(--white);
    border: 1px solid #E8E8E8;
    border-radius: 4px;
    page-break-inside: avoid;
  }}

  .chart-wrapper canvas {{
    width: 100% !important;
  }}

  .chart-title {{
    font-size: 11pt;
    font-weight: 700;
    color: var(--green-dark);
    margin-bottom: 12px;
    text-align: center;
  }}

  /* ─── Graph Embeds (PNG images) ────────────────────────────────── */
  .graph-embed {{
    margin: 20px 0;
    text-align: center;
    page-break-inside: avoid;
  }}

  .graph-embed img {{
    max-width: 100%;
    height: auto;
    border: 1px solid #E8E8E8;
    border-radius: 4px;
  }}

  .graph-embed .graph-caption {{
    font-size: 9pt;
    color: var(--grey);
    font-style: italic;
    margin-top: 8px;
  }}

  /* ─── Glossary ─────────────────────────────────────────────────── */
  .glossary-section {{
    page-break-before: always;
    margin-top: 48px;
  }}

  .glossary-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 9.5pt;
  }}

  .glossary-table th {{
    background: var(--green-dark);
    color: var(--white);
    padding: 8px 12px;
    text-align: left;
    font-weight: 700;
  }}

  .glossary-table td {{
    padding: 6px 12px;
    border-bottom: 1px solid #E8E8E8;
    vertical-align: top;
  }}

  .glossary-table td:first-child {{
    font-weight: 700;
    color: var(--green-dark);
    white-space: nowrap;
    width: 120px;
  }}

  /* ─── Footer ───────────────────────────────────────────────────── */
  .report-footer {{
    margin-top: 48px;
    padding-top: 20px;
    border-top: 2px solid var(--green-medium);
    text-align: center;
    font-size: 8.5pt;
    color: var(--grey);
  }}

  .report-footer .deloitte-mark {{
    color: var(--green-dark);
    font-weight: 700;
    font-size: 9pt;
  }}

  /* ─── Print Styles ─────────────────────────────────────────────── */
  @media print {{
    body {{
      font-size: 10pt;
    }}

    .report-container {{
      max-width: none;
      padding: 0;
    }}

    .cover-page {{
      min-height: auto;
      padding: 40mm 0;
    }}

    .kpi-hero {{
      grid-template-columns: repeat(3, 1fr);
    }}

    .chart-wrapper {{
      border: none;
      padding: 8px 0;
    }}

    .section-divider {{
      page-break-before: always;
    }}

    .callout, .basis-of-prep, .exec-bullets li, .kpi-card,
    .exec-outlook, .data-table, .chart-wrapper, .graph-embed {{
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }}
  }}

  @page {{
    size: A4;
    margin: 20mm 15mm;
  }}
</style>
</head>
<body>
<div class="report-container">

<!-- ═══ Cover Page ═══ -->
<div class="cover-page">
  <div class="cover-accent"></div>
  <h1 class="cover-title">{report_title}</h1>
  <p class="cover-subtitle">{report_subtitle}</p>
  <div class="cover-meta">
    <span class="cover-deloitte">Deloitte Financial Advisory</span>
    <span>Confidential — Prepared for Management</span>
    <span>{generation_date}</span>
  </div>
</div>

<!-- ═══ KPI Hero Cards ═══ -->
<div class="kpi-hero">
{kpi_cards_html}
</div>

<!-- ═══ Executive Summary ═══ -->
<div class="exec-summary">
  <h2 class="section-title">Executive Summary</h2>
{exec_summary_html}
</div>

<!-- ═══ Report Sections ═══ -->
{sections_html}

<!-- ═══ Glossary ═══ -->
{glossary_html}

<!-- ═══ Footer ═══ -->
<div class="report-footer">
  <p class="deloitte-mark">Deloitte</p>
  <p>This document is confidential and prepared solely for the use of the intended recipient.</p>
  <p>Generated {generation_date}</p>
</div>

</div>

<!-- ═══ Chart.js Initialization ═══ -->
<script>
{charts_js}
</script>

</body>
</html>"""


# ─── Assembly Helpers ─────────────────────────────────────────────────────────

def build_kpi_cards_html(blueprint: dict) -> str:
    """Generate HTML for the KPI hero cards from the blueprint."""
    exec_config = blueprint.get("executive_summary", {})
    metrics = exec_config.get("key_metrics", [])
    if not metrics:
        return ""

    cards = []
    for m in metrics:
        trend = m.get("trend", "neutral")
        trend_icon = {"up": "&#9650;", "down": "&#9660;", "neutral": "&#9679;"}.get(trend, "")
        cards.append(f"""  <div class="kpi-card">
    <div class="kpi-label">{m.get('label', '')}</div>
    <div class="kpi-value">{m.get('value', '')}</div>
    <div class="kpi-detail">{m.get('detail', '')}</div>
    <div class="kpi-trend {trend}">{trend_icon}</div>
  </div>""")

    return "\n".join(cards)


def build_charts_js(all_charts: list) -> str:
    """Generate Chart.js initialization code for all charts."""
    if not all_charts:
        return "// No charts to render"

    js_parts = [
        "document.addEventListener('DOMContentLoaded', function() {",
        "  Chart.defaults.font.family = \"'Arial', 'Helvetica Neue', sans-serif\";",
        "  Chart.defaults.font.size = 11;",
        "",
    ]

    for chart in all_charts:
        chart_id = chart.get("chart_id", "")
        chart_type = chart.get("chart_type", "bar")
        if not chart_id:
            continue

        # Map custom types to Chart.js types
        if chart_type == "horizontalBar":
            chart_type = "bar"
            is_horizontal = True
        else:
            is_horizontal = False

        labels = json.dumps(chart.get("labels", []))
        datasets = chart.get("datasets", [])

        # Build datasets array
        ds_parts = []
        for ds in datasets:
            ds_obj = {"label": ds.get("label", ""), "data": ds.get("data", [])}
            if "backgroundColor" in ds:
                ds_obj["backgroundColor"] = ds["backgroundColor"]
            if "borderColor" in ds:
                ds_obj["borderColor"] = ds["borderColor"]
                ds_obj["borderWidth"] = 2
                ds_obj["fill"] = ds.get("fill", False)
                ds_obj["tension"] = 0.3
            if "pointBackgroundColor" in ds:
                ds_obj["pointBackgroundColor"] = ds["pointBackgroundColor"]
            ds_parts.append(json.dumps(ds_obj))

        datasets_js = ",\n        ".join(ds_parts)

        # Build options
        opts = chart.get("options", {})
        y_prefix = opts.get("y_prefix", "")
        y_suffix = opts.get("y_suffix", "")
        stacked = "true" if opts.get("stacked") else "false"
        show_legend = "true" if opts.get("show_legend", True) else "false"
        index_axis = "'y'" if is_horizontal or opts.get("index_axis") == "y" else "'x'"

        # Build tick callback
        tick_callback = ""
        if y_prefix or y_suffix:
            tick_callback = f"""
              ticks: {{
                callback: function(value) {{
                  return '{y_prefix}' + value + '{y_suffix}';
                }}
              }}"""

        # Tooltip callback
        tooltip_callback = ""
        if y_prefix or y_suffix:
            tooltip_callback = f"""
          tooltip: {{
            callbacks: {{
              label: function(context) {{
                return context.dataset.label + ': {y_prefix}' + context.parsed.{'x' if is_horizontal else 'y'} + '{y_suffix}';
              }}
            }}
          }},"""

        chart_title = chart.get("title", "")

        js_parts.append(f"""  // Chart: {chart_title}
  (function() {{
    var ctx = document.getElementById('{chart_id}');
    if (!ctx) return;
    new Chart(ctx, {{
      type: '{chart_type}',
      data: {{
        labels: {labels},
        datasets: [
        {datasets_js}
        ]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: true,
        indexAxis: {index_axis},
        plugins: {{
          legend: {{ display: {show_legend}, position: 'bottom' }},{tooltip_callback}
          title: {{
            display: true,
            text: '{chart_title}',
            color: '{DELOITTE_GREEN_DARK}',
            font: {{ size: 13, weight: 'bold' }}
          }}
        }},
        scales: {{
          x: {{ stacked: {stacked}, grid: {{ display: false }} }},
          y: {{ stacked: {stacked},{tick_callback}
            grid: {{ color: '#E0E0E0', lineWidth: 0.5 }}
          }}
        }}
      }}
    }});
  }})();
""")

    js_parts.append("});")
    return "\n".join(js_parts)


def build_glossary_html(glossary_entries: list) -> str:
    """Generate HTML for the glossary section."""
    if not glossary_entries:
        return ""

    rows = []
    for entry in glossary_entries:
        term = entry.get("term", "")
        definition = entry.get("definition", "")
        rows.append(f"    <tr><td>{term}</td><td>{definition}</td></tr>")

    return f"""<div class="glossary-section section-divider">
  <h2 class="section-title">Glossary</h2>
  <table class="glossary-table">
    <thead><tr><th>Term</th><th>Definition</th></tr></thead>
    <tbody>
{chr(10).join(rows)}
    </tbody>
  </table>
</div>"""


def embed_graph_placeholders(html: str, graphs_folder: str) -> str:
    """Replace GRAPH_PLACEHOLDER:filename.png with base64-encoded images."""
    def replace_graph(match):
        filename = match.group(1)
        filepath = Path(graphs_folder) / filename
        if filepath.exists():
            data_uri = encode_graph_base64(str(filepath))
            if data_uri:
                return data_uri
        logger.warning(f"Graph not found for embedding: {filename}")
        return f"GRAPH_PLACEHOLDER:{filename}"

    return re.sub(r"GRAPH_PLACEHOLDER:([\w\-\.]+\.png)", replace_graph, html)


# ─── Main Pipeline ────────────────────────────────────────────────────────────

def generate_deloitte_report(
    client,
    model: str,
    entries: list,
    context_text: str,
    final_report_text: str,
    available_graphs: list,
    graphs_folder: str,
    config: dict,
) -> str:
    """
    Full pipeline: Planner → Section Writers → Assembler.
    Returns the final HTML report string.
    """
    planner_max_tokens = config.get("deloitte_report_planner_max_tokens", 6000)
    section_max_tokens = config.get("deloitte_report_section_max_tokens", 10000)
    summary_max_tokens = config.get("deloitte_report_summary_max_tokens", 4000)
    glossary_max_tokens = config.get("deloitte_report_glossary_max_tokens", 3000)

    total_usage = {"input": 0, "output": 0}

    # ── Stage 1: The Planner ──────────────────────────────────────────────────
    logger.info("\n" + "=" * 70)
    logger.info("[Stage 1] Report Planner — designing report blueprint...")
    logger.info("=" * 70)

    planner_prompt = build_planner_prompt(
        entries, context_text, final_report_text, available_graphs
    )

    planner_raw, usage = _llm_call(
        client, model, PLANNER_SYSTEM_PROMPT, planner_prompt,
        planner_max_tokens, label="Stage 1 Planner",
    )
    total_usage["input"] += usage.input_tokens
    total_usage["output"] += usage.output_tokens

    try:
        blueprint = parse_planner_json(planner_raw)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(f"[Stage 1] JSON parse failed ({e}), retrying...")
        retry_prompt = (
            planner_prompt
            + "\n\nIMPORTANT: Your previous response was not valid JSON. "
            "Output ONLY a valid JSON object, no markdown fences, no commentary."
        )
        planner_raw, usage = _llm_call(
            client, model, PLANNER_SYSTEM_PROMPT, retry_prompt,
            planner_max_tokens, label="Stage 1 Planner (retry)",
        )
        total_usage["input"] += usage.input_tokens
        total_usage["output"] += usage.output_tokens
        blueprint = parse_planner_json(planner_raw)  # Let it raise on second failure

    logger.info(
        f"[Stage 1] Blueprint: {len(blueprint['sections'])} sections, "
        f"title=\"{blueprint.get('report_title', 'N/A')}\""
    )
    for sec in blueprint["sections"]:
        n_charts = len(sec.get("charts", []))
        n_tables = len(sec.get("tables", []))
        logger.info(
            f"       Section {sec.get('section_number', '?')}: {sec['title']} "
            f"(iters={sec.get('iterations', [])}, charts={n_charts}, tables={n_tables})"
        )

    # ── Stage 2: Section Writers ──────────────────────────────────────────────
    logger.info("\n" + "=" * 70)
    logger.info("[Stage 2] Writing report sections...")
    logger.info("=" * 70)

    section_htmls = []
    all_charts = []
    previous_titles = []

    for sec_idx, section in enumerate(blueprint["sections"]):
        sec_num = sec_idx + 1
        logger.info(
            f"\n[Stage 2.{sec_num}] Writing section {sec_num}/{len(blueprint['sections'])}: "
            f"{section['title']}"
        )

        # Filter entries for this section
        iter_ids = {str(i) for i in section.get("iterations", [])}
        section_entries = [e for e in entries if str(e.get("iteration", "")) in iter_ids]
        section_graphs = [
            g for g in available_graphs if g[1] in section.get("graphs_to_embed", [])
        ]

        if not section_entries:
            logger.warning(f"  No entries for section {sec_num}, generating with context only.")

        # Build and send prompt
        section_prompt = build_section_writer_prompt(
            section, section_entries, context_text, section_graphs, previous_titles,
        )

        section_raw, usage = _llm_call(
            client, model, SECTION_WRITER_SYSTEM_PROMPT, section_prompt,
            section_max_tokens, label=f"Stage 2.{sec_num} Section",
        )
        total_usage["input"] += usage.input_tokens
        total_usage["output"] += usage.output_tokens

        # Parse JSON response
        try:
            section_data = _parse_section_json(section_raw)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"  Section {sec_num} JSON parse failed ({e}), retrying...")
            retry_prompt = (
                section_prompt
                + "\n\nIMPORTANT: Your previous response was not valid JSON. "
                "Output ONLY a valid JSON object with 'section_html' and 'charts_data' keys."
            )
            section_raw, usage = _llm_call(
                client, model, SECTION_WRITER_SYSTEM_PROMPT, retry_prompt,
                section_max_tokens, label=f"Stage 2.{sec_num} Section (retry)",
            )
            total_usage["input"] += usage.input_tokens
            total_usage["output"] += usage.output_tokens
            try:
                section_data = _parse_section_json(section_raw)
            except Exception as e2:
                logger.error(f"  Section {sec_num} retry failed ({e2}), using fallback HTML.")
                section_data = {
                    "section_html": f"<h2 class='section-title'>{section['title']}</h2>"
                                    f"<p class='body-text'><em>Section generation failed.</em></p>",
                    "charts_data": [],
                }

        section_html = section_data.get("section_html", "")
        charts_data = section_data.get("charts_data", [])

        section_htmls.append(f'<div class="section-divider">\n{section_html}\n</div>')
        all_charts.extend(charts_data)
        previous_titles.append(section["title"])

        logger.info(
            f"  Section {sec_num}: {len(section_html):,}ch HTML, "
            f"{len(charts_data)} charts"
        )

    # ── Stage 2b: Executive Summary ───────────────────────────────────────────
    logger.info(f"\n[Stage 2.ES] Writing Executive Summary...")

    exec_prompt = build_exec_summary_prompt(blueprint, previous_titles, context_text)

    exec_raw, usage = _llm_call(
        client, model, EXEC_SUMMARY_WRITER_SYSTEM_PROMPT, exec_prompt,
        summary_max_tokens, label="Stage 2.ES Exec Summary",
    )
    total_usage["input"] += usage.input_tokens
    total_usage["output"] += usage.output_tokens

    try:
        exec_data = _parse_json_safe(exec_raw)
        exec_summary_html = exec_data.get("summary_html", "")
    except Exception as e:
        logger.warning(f"  Exec summary JSON parse failed ({e}), using raw text.")
        exec_summary_html = f"<p class='body-text'>{exec_raw}</p>"

    # ── Stage 2c: Glossary ────────────────────────────────────────────────────
    logger.info(f"\n[Stage 2.GL] Writing Glossary...")

    glossary_prompt = build_glossary_prompt(blueprint, previous_titles, context_text)

    glossary_raw, usage = _llm_call(
        client, model, GLOSSARY_WRITER_SYSTEM_PROMPT, glossary_prompt,
        glossary_max_tokens, label="Stage 2.GL Glossary",
    )
    total_usage["input"] += usage.input_tokens
    total_usage["output"] += usage.output_tokens

    try:
        glossary_data = _parse_json_safe(glossary_raw)
        glossary_entries = glossary_data.get("glossary_entries", [])
    except Exception as e:
        logger.warning(f"  Glossary JSON parse failed ({e}), using empty glossary.")
        glossary_entries = []

    # ── Stage 3: Assembly ─────────────────────────────────────────────────────
    logger.info("\n" + "=" * 70)
    logger.info("[Stage 3] Assembling final HTML report...")
    logger.info("=" * 70)

    # Build all components
    kpi_cards_html = build_kpi_cards_html(blueprint)
    sections_html = "\n\n".join(section_htmls)
    glossary_html = build_glossary_html(glossary_entries)
    charts_js = build_charts_js(all_charts)

    # Assemble HTML
    html = HTML_TEMPLATE.format(
        report_title=blueprint.get("report_title", "Due Diligence Analysis"),
        report_subtitle=blueprint.get("report_subtitle", "Confidential"),
        generation_date=datetime.now().strftime("%B %d, %Y"),
        green_medium=DELOITTE_GREEN_MEDIUM,
        green_dark=DELOITTE_GREEN_DARK,
        grey=DELOITTE_GREY,
        electric_blue=DELOITTE_ELECTRIC_BLUE,
        aqua=DELOITTE_AQUA,
        light_grey=DELOITTE_LIGHT_GREY,
        black=DELOITTE_BLACK,
        white=DELOITTE_WHITE,
        kpi_cards_html=kpi_cards_html,
        exec_summary_html=exec_summary_html,
        sections_html=sections_html,
        glossary_html=glossary_html,
        charts_js=charts_js,
    )

    # Embed graph images as base64
    html = embed_graph_placeholders(html, graphs_folder)

    logger.info(
        f"[Stage 3] Assembly complete — {len(html):,}ch | "
        f"{len(all_charts)} charts | {len(glossary_entries)} glossary terms"
    )
    logger.info(
        f"Total tokens: in={total_usage['input']:,} out={total_usage['output']:,} "
        f"combined={total_usage['input'] + total_usage['output']:,}"
    )

    return html


def _parse_section_json(raw_text: str) -> dict:
    """Parse a section writer's JSON output."""
    data = _parse_json_safe(raw_text)
    if "section_html" not in data:
        raise ValueError("Missing 'section_html' in section JSON")
    data.setdefault("charts_data", [])
    return data


def _parse_json_safe(raw_text: str) -> dict:
    """Parse JSON from LLM output, handling markdown fences."""
    text = raw_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
    return json.loads(text)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    config = yaml.safe_load(read_file("config.yaml"))
    model = config.get("deloitte_report_model", config.get("model", "claude-sonnet-4-6"))
    archive_path = config.get("archive_file", "full_archive.txt")
    context_path = config.get("active_context_file", "active_context.md")
    graphs_folder = config.get("graphs_folder", "workspace/graphs")
    debug_logging = config.get("debug_logging", False)

    # Check for final report (phase2 output)
    final_report_path = config.get("deloitte_report_final_report", "final_report.md")
    output_path = config.get("deloitte_report_output", "deloitte_report.html")

    # Setup logging
    log_file = setup_logging(debug=debug_logging)

    logger.info("=" * 70)
    logger.info("Deloitte Report — Premium HTML Report Generator")
    logger.info(f"Archive  : {archive_path}")
    logger.info(f"Context  : {context_path}")
    logger.info(f"Graphs   : {graphs_folder}")
    logger.info(f"Model    : {model}")
    logger.info(f"Output   : {output_path}")
    logger.info(f"Log      : {log_file}")
    logger.info("=" * 70)
    logger.info("")

    # Validate inputs
    if not Path(archive_path).exists():
        logger.error(f"ERROR: {archive_path} not found. Run loop.py first.")
        sys.exit(1)

    archive_text = read_file(archive_path)
    context_text = read_file(context_path) if Path(context_path).exists() else ""
    final_report_text = read_file(final_report_path) if Path(final_report_path).exists() else ""

    # Parse archive
    entries = parse_archive(archive_text)
    logger.info(f"Found {len(entries)} successful analyses in archive.")

    if not entries and not context_text:
        logger.error("No data to report on. Exiting.")
        sys.exit(0)

    available_graphs = list_available_graphs(graphs_folder)
    logger.info(f"Available graphs: {len(available_graphs)}")
    for _, fname in available_graphs:
        logger.info(f"  - {fname}")

    # Run pipeline
    client = anthropic.Anthropic(api_key=API_KEY)
    t_start = time.time()

    html = generate_deloitte_report(
        client=client,
        model=model,
        entries=entries,
        context_text=context_text,
        final_report_text=final_report_text,
        available_graphs=available_graphs,
        graphs_folder=graphs_folder,
        config=config,
    )

    elapsed = time.time() - t_start

    # Save output
    write_file(output_path, html)

    logger.info("")
    logger.info("=" * 70)
    logger.info("Deloitte Report generation complete.")
    logger.info(f"  Output:  {output_path} ({len(html):,} chars)")
    logger.info(f"  Time:    {elapsed:.1f}s")
    logger.info(f"  Log:     {log_file}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
