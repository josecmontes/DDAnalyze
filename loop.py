#!/usr/bin/env python3
"""
Autonomous Data Analysis Loop
Main orchestrator — runs N sequential iterations of:
  Analyst (plan) → Execute (code) → Critic (evaluate) → Archive → Update context

Features:
  - Year-by-year tables (3-4 years + LTM + CAGR) enforced for trend analyses
  - Graph generation and saving to workspace/graphs/
  - Automatic code retry (up to max_code_retries) on execution errors
  - Rich active_context with Dead Ends and Generated Graphs sections
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import anthropic
import yaml

from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('API_KEY')

# Change to the directory containing this script so relative paths work
os.chdir(Path(__file__).parent)

# ─── Logging Setup ────────────────────────────────────────────────────────────

logger = logging.getLogger("ddanalyze.loop")

def setup_logging(debug: bool = False, log_dir: str = "logs") -> Path:
    """
    Configure logging to console (INFO+) and a timestamped file (always DEBUG).
    Returns the path of the created log file.
    """
    Path(log_dir).mkdir(exist_ok=True)
    log_file = Path(log_dir) / f"loop_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    console_level = logging.DEBUG if debug else logging.INFO

    root = logging.getLogger("ddanalyze")
    root.setLevel(logging.DEBUG)  # capture everything; handlers filter

    # Console handler — clean format, respects debug flag
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level)
    ch.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    root.addHandler(ch)

    # File handler — always DEBUG, full timestamp + level
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(fh)

    return log_file

# ─── System Prompts ───────────────────────────────────────────────────────────

ANALYST_SYSTEM_PROMPT = """You are the Financial Due Dilligence Analyst agent in an autonomous data analysis loop. Your job is to investigate
a business dataset by writing Python code that produces readable, printed output.

You will receive:
- A task description with business context and a catalog of analysis types
- A knowledge base showing what has already been done

Your job:
1. Read the knowledge base carefully. Do not repeat an analysis that has already been done
   unless you can add a meaningfully different angle (different columns, different time window,
   different breakdown). Avoid approaches listed in "Dead Ends & Closed Paths".
2. Choose one analysis from the catalog, or a logical extension of it.
3. Write clean, simple Python code that loads the data and prints results.
4. Focus on business understanding: who buys, how much, when, how concentrated, how it changes.
   Do NOT perform regression, hypothesis testing, or statistical modeling.
5. Return ONLY a valid JSON object with fields: hypothesis, analysis_type, columns_used, code.
   No preamble. No markdown. No explanation outside the JSON.

MANDATORY — YEARLY TABLES AND LTM:
For any analysis spanning multiple time periods (revenue trends, customer counts, retention, etc.)
you MUST produce a structured year-by-year comparison table using ONLY full 12-month windows.

CRITICAL RULE — PARTIAL YEARS (read carefully):
If the latest calendar year is incomplete (fewer than 12 months of data), you MUST:
1. Detect the latest available month in the data (e.g. max(df['Fecha_Mes'].dt.to_period('M'))).
2. Build an LTM (Last-Twelve-Months) window = the 12 months ending at that latest month.
   Example: if data runs through March 2023, LTM23 = Apr 2022 – Mar 2023.
3. NEVER compare a partial year (e.g. Jan–Mar 2023) directly against a full prior year (FY2022).
   That comparison is misleading and forbidden. Always use LTM instead.
4. Name the LTM column with the ending year: LTM23, LTM22, etc.
5. For the LTM YoY %: compare LTM against the same 12-month window shifted back exactly one year
   (e.g. LTM23 vs Apr 2021 – Mar 2022).

CAGR WITH PARTIAL YEARS:
When the dataset ends mid-year, compute CAGR using the fractional number of years:
  latest_month  = last month in data (e.g. 2023-03)
  earliest_month = first month of the first full year (e.g. 2021-01)
  N = months between them / 12   →  e.g. 27 months / 12 = 2.25 years
  CAGR = (LTM_value / FY_first_value) ^ (1 / N) − 1
Always print N explicitly (e.g. "CAGR (N=2.25y)") so readers understand the time span.

Table format — TRANSPOSED (columns = time periods, rows = metrics):
Print with print() and aligned columns. This is the standard layout:

  Metric    | FY2021 | FY2022 | LTM23 | CAGR (N=2.25y)
  ----------|--------|--------|-------|---------------
  Metric A  |   x    |   x    |   x   |     XX%
  Metric B  |   x    |   x    |   x   |     XX%
  YoY %     |   —    |  +X%   |  +X%  |

Rules:
- Columns left-to-right: full fiscal years in order, then LTM (if applicable), then CAGR as the last column.
- CAGR column applies to the row metrics (not YoY %). Use fractional N and label it "CAGR (N=X.XXy)".
- Use this transposed layout whenever the table represents time-series data by year.
- For other table types (e.g. rankings, client lists, segment breakdowns) use whatever layout is clearest.

Always compute and print CAGR when you have 3+ years of data (use fractional N for partial years).
Use pandas tabulate or manual string formatting — never raw DataFrame repr.

GRAPHS:
You may (and should, when insightful) generate charts using matplotlib or seaborn.
- Save every chart to the graphs folder provided in the iteration info.
- Use a descriptive filename: e.g. iter{N}_revenue_by_year.png
- After saving, print EXACTLY this line (one per graph):
    GRAPH_SAVED: <filename> — <one-line description>
- Always call plt.close() after saving to free memory.
- Do NOT call plt.show() — running headless.
- Set matplotlib backend before importing pyplot: import matplotlib; matplotlib.use('Agg')

DELOITTE COLOR THEME — MANDATORY FOR ALL CHARTS:
Every chart MUST follow the Deloitte visual identity. Apply it by importing the theme module
at the very start of your code (right after setting the backend):

    import matplotlib; matplotlib.use('Agg')
    import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()

If the import fails for any reason, manually apply the theme using these exact values:

    import matplotlib.pyplot as plt
    DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
    plt.rcParams.update({
        'font.family': ['Arial', 'Liberation Sans', 'sans-serif'],
        'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
        'axes.titlesize': 14, 'axes.titleweight': 'bold',
        'axes.titlecolor': '#26890D',
        'axes.spines.top': False, 'axes.spines.right': False,
        'figure.facecolor': 'white', 'axes.facecolor': 'white',
    })

Color usage rules:
- Data series colors (in order): #26890D (green), #046A38 (dark green), #404040 (grey),
  #0D8390 (electric blue), #00ABAB (aqua blue).
- Chart / figure title: always color='#26890D', fontweight='bold', font Arial.
- Axis labels and tick labels: font Arial.
- Example title call: ax.set_title("My Title", color='#26890D', fontweight='bold')
- For seaborn, pass palette=DELOITTE_COLORS explicitly to each plot call."""

CRITIC_SYSTEM_PROMPT = """You are the Critic agent in an autonomous data analysis loop. Your job is to evaluate \
whether a data analysis run produced useful business insight.

You will receive the original hypothesis, the Python code, and the terminal output.

Your job:
1. If the code errored (stderr present, exception in output), mark status as "failure".
   Describe the error clearly and suggest what the next agent should try.
2. If the code ran but the output is empty, trivial, or uninformative (e.g. "0 rows found"),
   mark status as "failure" with error_type "EmptyResult".
3. If the output contains real business findings (numbers, distributions, trends, rankings),
   mark status as "success". Write a plain-English summary in a formal, senior consulting tone.
   Avoid conversational or explanatory phrases (e.g. "This is exactly how X works", "It makes
   sense that..."). Use professional formulations: "This pattern is consistent with...",
   "This trend reflects...", "As is typical in the X segment, ...".
4. Be specific in suggested_followup: explain WHAT to investigate next, WHY it would be valuable,
   and WHAT you expect to find. A vague "investigate further" is not acceptable.
5. In dead_ends: list any analysis directions that this result confirms are NOT worth pursuing
   (e.g. "Seasonality analysis — revenue is flat across all months with no meaningful variation").
6. Return ONLY a valid JSON object with fields:
   - status: "success" or "failure"
   - quality: "high" / "medium" / "low" (for successes only)
   - summary: full plain-English paragraph of what was found (do not truncate)
   - key_findings: list of specific insight strings
   - suggested_followup: specific next investigation with reasoning and expected findings
   - error_type: short label for failures (e.g. "CodeError", "EmptyResult", "KeyError")
   - dead_ends: list of investigation directions confirmed not worth pursuing (may be empty list)
   No preamble. No markdown fences."""

SUMMARIZER_SYSTEM_PROMPT = """You are the Financial Due Dilligence Summarizer agent in an autonomous data analysis loop. The active knowledge
base has grown and needs to be condensed so future agents can read it efficiently.

Your job:
1. Preserve ALL section headers exactly as they are (including ## Dead Ends & Closed Paths
   and ## Generated Graphs).
2. Keep the Analysis Index table COMPLETE — do not remove any rows.
3. Keep the Generated Graphs table COMPLETE — do not remove any rows.
4. Condense "Established Facts": merge overlapping facts, remove redundancy,
   keep the most specific version of each insight.
5. Condense "What Has Been Tried": keep one line per analysis done, remove verbose detail.
6. Keep "Open Questions / Suggested Next Steps" current and pruned to the most relevant 5–8 items,
   preserving the most specific and actionable ones.
7. Keep "Dead Ends & Closed Paths": consolidate duplicates but never remove confirmed dead ends.
8. Do not add new analysis. Do not invent findings. Only compress what is there.
9. Return the full new content of active_context.md as plain text. No JSON. No preamble.
10. Readers expect year-by-year tables (Fiscal year if given, else natural years).
    When the last year is incomplete, always use LTM (Last-Twelve-Months = the 12 months ending
    at the latest data point, e.g. LTM23 = Apr 2022–Mar 2023 if data ends March 2023).
    Never show a raw partial year as a comparison column — always substitute LTM.
    CAGR must use fractional N: N = total months from start of first full year to end of LTM / 12
    (e.g. Jan 2021 → Mar 2023 = 27 months → N = 2.25). Print N explicitly: "CAGR (N=2.25y)"."""

ARCHIVE_SUMMARIZER_SYSTEM_PROMPT = """You are the Summarizer agent in an autonomous data analysis loop. \
The active knowledge base is empty or stale. Your job is to RECONSTRUCT it from scratch using the full iteration \
archive provided.

The archive contains one block per iteration in this format:
  ITERATION / DATE / STATUS / ANALYSIS TYPE / HYPOTHESIS / COLUMNS USED / CODE / OUTPUT / STDERR / EVALUATION

Your job:
1. Produce the full content of active_context.md using EXACTLY this structure (preserve all section headers):

   # Active Knowledge Base
   ## Overarching Goal
   ## Established Facts
   ## Analysis Index
   ## What Has Been Tried
   ## Dead Ends & Closed Paths
   ## Generated Graphs
   ## Open Questions / Suggested Next Steps

2. Analysis Index — one row per iteration (success and failure), columns: Iter | Type | Columns Used | Status | Date
3. Established Facts — synthesise all confirmed findings from SUCCESS iterations. Merge overlapping facts.
   Include year-by-year tables where the data supports it. When the last year is incomplete,
   always use LTM (Last-Twelve-Months = 12 months ending at the latest data point, e.g. LTM23).
   Never present a raw partial year as a comparison column. CAGR must use fractional N:
   N = total months from start of first full year to end of LTM period / 12 (e.g. 2.25 for 27 months).
   Always label it "CAGR (N=X.XXy)" in tables.
4. What Has Been Tried — one line per iteration describing what was done and what was found (or why it failed).
5. Dead Ends & Closed Paths — list every direction confirmed NOT worth pursuing (from EVALUATION dead_ends fields
   and failed iterations).
6. Generated Graphs — one row per graph ever saved: Iter | Filename | Description. Extract from OUTPUT lines
   that start with "GRAPH_SAVED:".
7. Open Questions / Suggested Next Steps — synthesise the most relevant 5–8 open threads from EVALUATION
   suggested_followup fields, prioritising recent iterations.
8. Do not invent findings. Only use what is in the archive.
9. Return the full new content of active_context.md as plain text. No JSON. No preamble."""

ACTIVE_CONTEXT_TEMPLATE = """# Active Knowledge Base

## Overarching Goal
{goal}

## Established Facts
- [None yet]

## Analysis Index
| Iter | Type | Columns Used | Status | Date |
|------|------|--------------|--------|------|

## What Has Been Tried
- [Nothing yet]

## Open Questions / Suggested Next Steps
- [None yet]

## Dead Ends & Closed Paths
- [None yet]

## Generated Graphs
| Iter | Filename | Description |
|------|----------|-------------|
"""

# ─── File Utilities ───────────────────────────────────────────────────────────

def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path: str, content: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def append_file(path: str, content: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(content)

# ─── LLM Utilities ───────────────────────────────────────────────────────────

def call_llm(
    client: anthropic.Anthropic,
    system: str,
    user: str,
    model: str,
    max_tokens: int,
    tag: str = "LLM",
) -> tuple[str, int, int]:
    """
    Call Claude with streaming and return (text, input_tokens, output_tokens).
    Logs prompt sizes at DEBUG and timing + token usage at INFO.
    """
    logger.debug(
        f"[{tag}] Sending request | system={len(system):,}ch "
        f"user={len(user):,}ch max_tokens={max_tokens}"
    )
    t0 = time.time()

    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    ) as stream:
        response = stream.get_final_message()

    elapsed = time.time() - t0
    in_tok = response.usage.input_tokens
    out_tok = response.usage.output_tokens

    logger.info(
        f"  [{tag}] Done in {elapsed:.1f}s | tokens in={in_tok:,} out={out_tok:,}"
    )

    for block in response.content:
        if block.type == "text":
            return block.text, in_tok, out_tok
    return "", in_tok, out_tok


def parse_json_response(text: str, tag: str = "JSON") -> Optional[dict]:
    """Robustly extract a JSON object from an LLM response."""
    text = text.strip()

    # Attempt 1: direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.debug(f"[{tag}] Direct JSON parse failed — trying markdown fence extraction")

    # Attempt 2: extract from markdown code fence
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.DOTALL)
    if match:
        try:
            result = json.loads(match.group(1).strip())
            logger.debug(f"[{tag}] JSON extracted from markdown fence (attempt 2)")
            return result
        except json.JSONDecodeError:
            logger.debug(f"[{tag}] Markdown fence extraction failed — trying brace search")

    # Attempt 3: find outermost { ... }
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            result = json.loads(text[start : end + 1])
            logger.debug(f"[{tag}] JSON extracted via brace search (attempt 3)")
            return result
        except json.JSONDecodeError:
            pass

    preview = text[:300].replace("\n", " ")
    logger.warning(f"[{tag}] All JSON parse attempts failed. Response preview: {preview!r}")
    return None

# ─── Message Builders ─────────────────────────────────────────────────────────

def build_analyst_user_message(
    task_content: str, context: str, iteration: int, config: dict
) -> str:
    n = config["n_iterations"]
    mode = config["repetition_mode"]
    threshold = config.get("hybrid_threshold", 12)
    data_file = config.get("data_file", "workspace/data.xlsx")
    graphs_folder = config.get("graphs_folder", "workspace/graphs")

    # Resolve effective repetition mode
    if mode == "hybrid":
        effective_mode = "soft" if iteration <= threshold else "hard"
    else:
        effective_mode = mode

    msg = f"""## Task Description

{task_content}

## Current Knowledge Base

{context}

## Iteration Info
- Current iteration: {iteration} of {n}
- Data file path (use exactly): {data_file}
- Load data with: df = pd.read_excel("{data_file}")
- Graphs folder (save charts here): {graphs_folder}
"""

    if effective_mode == "hard":
        done = _extract_done_analysis_types(context)
        if done:
            types_str = ", ".join(f'"{t}"' for t in done)
            msg += (
                f"\nThe following analysis types are ALREADY DONE and must not be "
                f"repeated: {types_str}. You must choose something not in this list."
            )

    return msg

def build_critic_user_message(
    parsed: dict, stdout: str, stderr: str, iteration: int
) -> str:
    return f"""## Analysis Being Evaluated
Iteration: {iteration}
Hypothesis: {parsed.get("hypothesis", "")}
Analysis Type: {parsed.get("analysis_type", "")}

## Code Executed
```python
{parsed.get("code", "")}
```

## Terminal Output
### STDOUT:
{stdout or "(empty)"}

### STDERR:
{stderr or "(none)"}

Evaluate this analysis and return a JSON object with the specified fields."""

def build_retry_analyst_message(
    task_content: str,
    context: str,
    iteration: int,
    config: dict,
    original_parsed: dict,
    stdout: str,
    stderr: str,
    retry_num: int,
) -> str:
    """Build a message asking the Analyst to fix code that errored."""
    data_file = config.get("data_file", "workspace/data.xlsx")
    graphs_folder = config.get("graphs_folder", "workspace/graphs")
    n = config["n_iterations"]

    return f"""## RETRY REQUEST (Attempt {retry_num})

The code you generated for this iteration produced an error. Please fix it.

## Original Analysis Goal
- Hypothesis: {original_parsed.get("hypothesis", "")}
- Analysis Type: {original_parsed.get("analysis_type", "")}
- Columns to use: {", ".join(original_parsed.get("columns_used", []))}

## Code That Failed
```python
{original_parsed.get("code", "")}
```

## Full Error Output
### STDOUT:
{stdout or "(empty)"}

### STDERR (Error):
{stderr}

## Your Task
Fix the code to resolve this error. Maintain the same analysis goal if possible.
Only pivot to a different approach if the error reveals the original goal is fundamentally broken.

## Iteration Info
- Current iteration: {iteration} of {n}
- Data file path (use exactly): {data_file}
- Load data with: df = pd.read_excel("{data_file}")
- Graphs folder: {graphs_folder}

## Current Knowledge Base
{context}

Return ONLY a valid JSON object with fields: hypothesis, analysis_type, columns_used, code."""

# ─── Active Context Utilities ─────────────────────────────────────────────────

def _extract_done_analysis_types(context: str) -> list:
    """Extract the Type column values from the Analysis Index table."""
    types = []
    in_index = False
    for line in context.split("\n"):
        if "## Analysis Index" in line:
            in_index = True
            continue
        if in_index:
            if line.startswith("## "):
                break
            if "|" in line:
                parts = [p.strip() for p in line.split("|")]
                if len(parts) < 4:
                    continue
                type_val = parts[2] if len(parts) > 2 else ""
                # Skip header row ("Type") and separator rows ("---")
                if not type_val or type_val.startswith("-") or type_val.lower() == "type":
                    continue
                types.append(type_val)
    return types

def _insert_after_header(content: str, header: str, new_text: str) -> str:
    """Insert new_text on the line immediately after a ## section header."""
    lines = content.split("\n")
    for i, line in enumerate(lines):
        if line.strip() == header:
            lines.insert(i + 1, new_text)
            return "\n".join(lines)
    return content

def _append_to_analysis_table(content: str, new_row: str) -> str:
    """Append a new row at the end of the Analysis Index table."""
    lines = content.split("\n")
    last_table_line = -1
    in_index = False

    for i, line in enumerate(lines):
        if "## Analysis Index" in line:
            in_index = True
            continue
        if in_index:
            if line.startswith("## "):
                break
            if "|" in line:
                last_table_line = i

    if last_table_line == -1:
        for i, line in enumerate(lines):
            if "## Analysis Index" in line:
                insert_at = min(i + 3, len(lines))
                lines.insert(insert_at, new_row)
                return "\n".join(lines)
        return content

    lines.insert(last_table_line + 1, new_row)
    return "\n".join(lines)

def _append_to_graphs_table(content: str, new_row: str) -> str:
    """Append a new row at the end of the Generated Graphs table."""
    lines = content.split("\n")
    last_table_line = -1
    in_graphs = False

    for i, line in enumerate(lines):
        if "## Generated Graphs" in line:
            in_graphs = True
            continue
        if in_graphs:
            if line.startswith("## "):
                break
            if "|" in line:
                last_table_line = i

    if last_table_line == -1:
        for i, line in enumerate(lines):
            if "## Generated Graphs" in line:
                insert_at = min(i + 3, len(lines))
                lines.insert(insert_at, new_row)
                return "\n".join(lines)
        return content

    lines.insert(last_table_line + 1, new_row)
    return "\n".join(lines)

def _extract_graph_saves(stdout: str, graphs_folder: str) -> list:
    """
    Parse stdout for GRAPH_SAVED: filename — description lines.
    Only records graphs where the file actually exists on disk.
    Returns list of (filename, description) tuples.
    """
    graphs = []
    for line in stdout.split("\n"):
        line = line.strip()
        if not line.startswith("GRAPH_SAVED:"):
            continue
        rest = line[len("GRAPH_SAVED:"):].strip()
        if " — " in rest:
            filename, description = rest.split(" — ", 1)
        elif " - " in rest:
            filename, description = rest.split(" - ", 1)
        else:
            filename, description = rest, ""

        filename = filename.strip()
        description = description.strip()

        graph_path = Path(graphs_folder) / filename
        if graph_path.exists():
            graphs.append((filename, description))
            logger.debug(f"[Graphs] Recorded graph: {filename} — {description}")
        else:
            logger.warning(f"[Graphs] Graph declared but file not found: {graph_path}")
    return graphs

def update_active_context_success(
    path: str,
    iteration: int,
    parsed: dict,
    evaluation: dict,
    graph_refs: list,
) -> None:
    content = read_file(path)
    size_before = len(content)
    now = datetime.now().strftime("%Y-%m-%d")

    # 1. Add row to Analysis Index table
    atype = parsed.get("analysis_type", "unknown")
    cols = ", ".join(parsed.get("columns_used", []))
    new_row = f"| {iteration} | {atype} | {cols} | SUCCESS | {now} |"
    content = _append_to_analysis_table(content, new_row)

    # 2. Add key findings to Established Facts
    findings = evaluation.get("key_findings", [])
    if findings:
        facts_text = "\n".join(f"- [Iter {iteration}] {f}" for f in findings)
        content = _insert_after_header(content, "## Established Facts", facts_text)

    # 3. Add full summary to What Has Been Tried (no truncation)
    summary_full = evaluation.get("summary", "")
    tried = f"- [Iter {iteration}] {atype}: {summary_full}"
    content = _insert_after_header(content, "## What Has Been Tried", tried)

    # 4. Add suggested followup to Open Questions (full context preserved)
    followup = evaluation.get("suggested_followup", "")
    if followup:
        content = _insert_after_header(
            content,
            "## Open Questions / Suggested Next Steps",
            f"- [From Iter {iteration}] {followup}",
        )

    # 5. Add confirmed dead ends
    dead_ends = evaluation.get("dead_ends", [])
    for de in dead_ends:
        content = _insert_after_header(
            content,
            "## Dead Ends & Closed Paths",
            f"- [From Iter {iteration}] {de}",
        )

    # 6. Add graph references to Generated Graphs table
    for filename, description in graph_refs:
        new_graph_row = f"| {iteration} | {filename} | {description} |"
        content = _append_to_graphs_table(content, new_graph_row)

    write_file(path, content)
    size_after = len(content)
    logger.debug(
        f"[Context] Updated (success): {size_before:,}ch → {size_after:,}ch "
        f"(+{size_after - size_before:,}ch)"
    )

def update_active_context_failure(
    path: str, iteration: int, parsed: dict, evaluation: dict
) -> None:
    content = read_file(path)
    size_before = len(content)
    now = datetime.now().strftime("%Y-%m-%d")

    # 1. Add FAILED row to Analysis Index
    atype = parsed.get("analysis_type", "unknown")
    cols = ", ".join(parsed.get("columns_used", []))
    new_row = f"| {iteration} | {atype} | {cols} | FAILED | {now} |"
    content = _append_to_analysis_table(content, new_row)

    # 2. Add failure note to What Has Been Tried
    error_type = evaluation.get("error_type", "unknown")
    suggested = evaluation.get("suggested_followup", "")
    tried = f"- [Iter {iteration}] FAILED — {atype} ({error_type}). {suggested}"
    content = _insert_after_header(content, "## What Has Been Tried", tried)

    # 3. Add confirmed dead ends (even failures can identify dead ends)
    dead_ends = evaluation.get("dead_ends", [])
    for de in dead_ends:
        content = _insert_after_header(
            content,
            "## Dead Ends & Closed Paths",
            f"- [From Iter {iteration}] {de}",
        )

    write_file(path, content)
    size_after = len(content)
    logger.debug(
        f"[Context] Updated (failure): {size_before:,}ch → {size_after:,}ch "
        f"(+{size_after - size_before:,}ch)"
    )

# ─── Archive Utilities ────────────────────────────────────────────────────────

def _format_archive_entry(
    iteration: int,
    parsed: Optional[dict],
    stdout: str,
    stderr: str,
    evaluation: Optional[dict],
    error_label: Optional[str] = None,
) -> str:
    sep_major = "=" * 80
    sep_minor = "-" * 80
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if error_label:
        return "\n".join([
            sep_major,
            f"ITERATION: {iteration}",
            f"DATE: {now}",
            f"STATUS: {error_label}",
            sep_minor,
            "RAW RESPONSE / ERROR:",
            stdout or "(none)",
            "",
            "STDERR:",
            stderr or "(none)",
            sep_major,
            "",
        ])

    status = evaluation.get("status", "unknown") if evaluation else "unknown"

    lines = [
        sep_major,
        f"ITERATION: {iteration}",
        f"DATE: {now}",
        f"STATUS: {status}",
        f"ANALYSIS TYPE: {parsed.get('analysis_type', 'unknown')}",
        f"HYPOTHESIS: {parsed.get('hypothesis', '')}",
        f"COLUMNS USED: {', '.join(parsed.get('columns_used', []))}",
        sep_minor,
        "CODE:",
        parsed.get("code", ""),
        "",
        sep_minor,
        "OUTPUT:",
        stdout or "(no output)",
    ]

    if stderr:
        lines += ["", "STDERR:", stderr]

    lines += ["", sep_minor, "EVALUATION:"]

    if evaluation:
        if status == "success":
            lines.append(f"Quality: {evaluation.get('quality', 'unknown')}")
            lines.append(f"Summary: {evaluation.get('summary', '')}")
            lines.append("Key findings:")
            for finding in evaluation.get("key_findings", []):
                lines.append(f"  - {finding}")
            lines.append(f"Suggested followup: {evaluation.get('suggested_followup', '')}")
            dead_ends = evaluation.get("dead_ends", [])
            if dead_ends:
                lines.append("Confirmed dead ends:")
                for de in dead_ends:
                    lines.append(f"  - {de}")
        else:
            lines.append(f"Error type: {evaluation.get('error_type', 'unknown')}")
            lines.append(f"Summary: {evaluation.get('summary', '')}")
            lines.append(f"Suggested followup: {evaluation.get('suggested_followup', '')}")

    lines += [sep_major, ""]
    return "\n".join(lines)

# ─── Code Runner ──────────────────────────────────────────────────────────────

def run_code(script_path: str, timeout: int = 120) -> tuple:
    """Execute a Python script; return (stdout, stderr)."""
    t0 = time.time()
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        elapsed = time.time() - t0
        stdout_lines = result.stdout.count("\n")
        logger.debug(
            f"[Runner] Exited code={result.returncode} in {elapsed:.1f}s "
            f"| stdout={len(result.stdout):,}ch ({stdout_lines} lines) "
            f"stderr={len(result.stderr):,}ch"
        )
        return result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        logger.warning(f"[Runner] Script timed out after {elapsed:.0f}s (limit={timeout}s)")
        return "", "TIMEOUT: Script exceeded 120 seconds"
    except Exception as e:
        logger.error(f"[Runner] Unexpected execution error: {e}")
        return "", f"EXECUTION_ERROR: {e}"

# ─── Initialisation Helpers ───────────────────────────────────────────────────

def _extract_goal_from_task(task_content: str) -> str:
    """Pull the Business Description + Business Questions sections as the goal."""
    lines = []
    capture = False
    for line in task_content.split("\n"):
        if line.startswith("## Business Description") or line.startswith("## Business Questions"):
            capture = True
        elif line.startswith("## Dataset") or line.startswith("## Analysis Catalog"):
            capture = False
        if capture:
            lines.append(line)
    return "\n".join(lines).strip() or "[See task.md for business context and goals]"

def _init_active_context(path: str, task_content: str) -> None:
    goal = _extract_goal_from_task(task_content)
    write_file(path, ACTIVE_CONTEXT_TEMPLATE.format(goal=goal))
    logger.info(f"  [Init] Created {path} from template")

# ─── Main Loop ────────────────────────────────────────────────────────────────

def main() -> None:
    # ── Load config ──────────────────────────────────────────────────────────
    config = yaml.safe_load(read_file("config.yaml"))

    model = config["model"]
    max_tokens = config["max_tokens"]
    analyst_max_tokens = config.get("analyst_max_tokens", 16384)
    n_iterations = config["n_iterations"]
    archive_path = config["archive_file"]
    context_path = config["active_context_file"]
    task_path = config["task_file"]
    script_path = config["workspace_script"]
    summarizer_every = config.get("summarizer_every_n", 10)
    max_code_retries = config.get("max_code_retries", 2)
    graphs_folder = config.get("graphs_folder", "workspace/graphs")
    debug_logging = config.get("debug_logging", False)

    # ── Setup logging ─────────────────────────────────────────────────────────
    log_file = setup_logging(debug=debug_logging)

    task_content = read_file(task_path)
    client = anthropic.Anthropic(api_key=API_KEY)

    # ── fresh_start ──────────────────────────────────────────────────────────
    if config.get("fresh_start", False):
        logger.info("[fresh_start] Resetting active_context.md and full_archive.txt")
        _init_active_context(context_path, task_content)
        write_file(archive_path, "")
    else:
        if not Path(context_path).exists():
            _init_active_context(context_path, task_content)

    # Ensure archive, workspace, and graphs folder exist
    if not Path(archive_path).exists():
        write_file(archive_path, "")
    Path("workspace").mkdir(exist_ok=True)
    Path(graphs_folder).mkdir(parents=True, exist_ok=True)

    # ── summarize_on_start ────────────────────────────────────────────────────
    if config.get("summarize_on_start", False) and not config.get("fresh_start", False):
        logger.info("[summarize_on_start] Reconstructing active_context.md from full_archive.txt...")
        archive_content = read_file(archive_path) if Path(archive_path).exists() else ""
        goal = _extract_goal_from_task(task_content)
        summarizer_msg = (
            f"Overarching Goal:\n{goal}\n\n"
            f"Total planned iterations for next run: {n_iterations}\n\n"
            f"--- FULL ARCHIVE ---\n{archive_content}"
        )
        new_ctx, _, _ = call_llm(
            client, ARCHIVE_SUMMARIZER_SYSTEM_PROMPT, summarizer_msg, model, max_tokens,
            tag="SummarizeOnStart",
        )
        write_file(context_path, new_ctx)
        logger.info("[summarize_on_start] active_context.md reconstructed from archive")

    # ── Banner ───────────────────────────────────────────────────────────────
    logger.info(f"\n{'=' * 60}")
    logger.info("AUTONOMOUS DATA ANALYSIS LOOP")
    logger.info(f"Model  : {model}")
    logger.info(f"Runs   : {n_iterations}  |  Mode: {config['repetition_mode']}")
    logger.info(f"Interval: {config['interval_minutes']} min  |  Summarizer every {summarizer_every} iters")
    logger.info(f"Retries : up to {max_code_retries} per iteration")
    logger.info(f"Tokens  : analyst={analyst_max_tokens}  critic={max_tokens}")
    logger.info(f"Graphs  : {graphs_folder}")
    logger.info(f"Log     : {log_file}")
    logger.info(f"Debug   : {'ON' if debug_logging else 'OFF (set debug_logging: true in config.yaml)'}")
    logger.info(f"{'=' * 60}")

    # Log full config at DEBUG level
    logger.debug(f"[Config] Full settings: {json.dumps(config, indent=2)}")

    # ── Token tracking ───────────────────────────────────────────────────────
    total_input_tokens = 0
    total_output_tokens = 0

    # ── Iteration loop ────────────────────────────────────────────────────────
    for iteration in range(1, n_iterations + 1):
        iter_start = time.time()
        logger.info(f"\n=== ITERATION {iteration} / {n_iterations} ===")

        try:
            # ── Step 1: Read context ──────────────────────────────────────────
            context = read_file(context_path)
            context_size = len(context)
            logger.debug(f"[Context] Loaded {context_path}: {context_size:,}ch")

            # ── Step 2: Call Analyst ──────────────────────────────────────────
            logger.info("  [Analyst] Planning analysis...")
            analyst_msg = build_analyst_user_message(task_content, context, iteration, config)
            raw_analyst, in_tok, out_tok = call_llm(
                client, ANALYST_SYSTEM_PROMPT, analyst_msg, model, analyst_max_tokens, tag="Analyst"
            )
            total_input_tokens += in_tok
            total_output_tokens += out_tok

            parsed = parse_json_response(raw_analyst, tag="Analyst")

            if parsed is None:
                logger.error("  [ERROR] Analyst returned invalid JSON — skipping iteration")
                logger.debug(f"[Analyst] Raw response (first 500ch): {raw_analyst[:500]!r}")
                entry = _format_archive_entry(
                    iteration, None, raw_analyst, "", None, "JSON_PARSE_ERROR"
                )
                append_file(archive_path, entry)
                continue

            analysis_type = parsed.get("analysis_type", "unknown")
            hypothesis = parsed.get("hypothesis", "")
            columns_used = parsed.get("columns_used", [])
            logger.info(f"  [Analyst] Type : {analysis_type}")
            logger.info(f"  [Analyst] Hypo : {hypothesis[:120]}")
            logger.debug(f"  [Analyst] Columns: {columns_used}")
            logger.debug(f"  [Analyst] Full hypothesis: {hypothesis}")
            logger.debug(f"  [Analyst] Code length: {len(parsed.get('code', '')):,}ch")

            # ── Step 3: Write and run code (with retry on error) ──────────────
            write_file(script_path, parsed.get("code", ""))
            logger.info(f"  [Runner] Executing {script_path}...")
            stdout, stderr = run_code(script_path)

            if "TIMEOUT" in stderr:
                logger.error("  [ERROR] Script timed out")
                entry = _format_archive_entry(
                    iteration, parsed, stdout, stderr, None, "TIMEOUT"
                )
                append_file(archive_path, entry)
                continue

            # Retry loop: on code error, ask Analyst to fix (up to max_code_retries)
            retry_count = 0
            while stderr and retry_count < max_code_retries:
                retry_count += 1
                # Log first line of stderr so it's visible without debug mode
                stderr_first_line = stderr.strip().split("\n")[-1][:120]
                logger.info(
                    f"  [Retry {retry_count}/{max_code_retries}] "
                    f"Code errored — asking Analyst to fix..."
                )
                logger.info(f"  [Retry {retry_count}/{max_code_retries}] Error: {stderr_first_line}")
                logger.debug(f"  [Retry {retry_count}] Full stderr:\n{stderr}")
                logger.debug(f"  [Retry {retry_count}] stdout before error:\n{stdout[:500]}")

                retry_msg = build_retry_analyst_message(
                    task_content, context, iteration, config,
                    parsed, stdout, stderr, retry_count,
                )
                raw_retry, in_tok, out_tok = call_llm(
                    client, ANALYST_SYSTEM_PROMPT, retry_msg, model, analyst_max_tokens,
                    tag=f"Retry{retry_count}",
                )
                total_input_tokens += in_tok
                total_output_tokens += out_tok

                parsed_retry = parse_json_response(raw_retry, tag=f"Retry{retry_count}")

                if parsed_retry is None:
                    logger.warning(
                        f"  [Retry {retry_count}] Analyst returned invalid JSON — stopping retries"
                    )
                    break

                parsed = parsed_retry
                write_file(script_path, parsed.get("code", ""))
                logger.info(f"  [Retry {retry_count}] Re-executing fixed code...")
                stdout, stderr = run_code(script_path)

                if "TIMEOUT" in stderr:
                    logger.warning(f"  [Retry {retry_count}] Timed out after fix — stopping retries")
                    break

                if not stderr:
                    logger.info(f"  [Retry {retry_count}] Code fixed and ran successfully!")

            # Log full output at debug, short preview at info
            preview = (stdout or stderr or "(no output)")[:200].replace("\n", " ")
            logger.info(f"  [Runner] Output : {preview}")
            logger.debug(f"  [Runner] Full stdout ({len(stdout):,}ch):\n{stdout}")
            if stderr:
                logger.debug(f"  [Runner] Full stderr ({len(stderr):,}ch):\n{stderr}")

            # ── Step 4: Detect saved graphs ───────────────────────────────────
            graph_refs = _extract_graph_saves(stdout, graphs_folder)
            if graph_refs:
                logger.info(
                    f"  [Graphs] {len(graph_refs)} graph(s) saved: "
                    f"{', '.join(fn for fn, _ in graph_refs)}"
                )

            # ── Step 5: Call Critic ───────────────────────────────────────────
            logger.info("  [Critic] Evaluating...")
            critic_msg = build_critic_user_message(parsed, stdout, stderr, iteration)
            raw_critic, in_tok, out_tok = call_llm(
                client, CRITIC_SYSTEM_PROMPT, critic_msg, model, max_tokens, tag="Critic"
            )
            total_input_tokens += in_tok
            total_output_tokens += out_tok

            evaluation = parse_json_response(raw_critic, tag="Critic")

            if evaluation is None:
                logger.error("  [ERROR] Critic returned invalid JSON — logging and continuing")
                logger.debug(f"[Critic] Raw response (first 500ch): {raw_critic[:500]!r}")
                entry = _format_archive_entry(
                    iteration, parsed, stdout, stderr, None, "CRITIC_JSON_PARSE_ERROR"
                )
                append_file(archive_path, entry)
                continue

            status = evaluation.get("status", "unknown")
            quality = evaluation.get("quality", "") if status == "success" else ""
            dead_ends_count = len(evaluation.get("dead_ends", []))
            logger.info(
                f"  [Critic] Status : {status}"
                + (f" | Quality: {quality}" if quality else "")
                + (f" | Dead ends confirmed: {dead_ends_count}" if dead_ends_count else "")
            )
            logger.debug(f"  [Critic] Summary: {evaluation.get('summary', '')[:300]}")
            logger.debug(f"  [Critic] Key findings: {evaluation.get('key_findings', [])}")
            logger.debug(f"  [Critic] Suggested followup: {evaluation.get('suggested_followup', '')}")

            # ── Step 6: Write to archive (always) ─────────────────────────────
            entry = _format_archive_entry(iteration, parsed, stdout, stderr, evaluation)
            append_file(archive_path, entry)
            logger.debug(f"  [Archive] Appended {len(entry):,}ch to {archive_path}")

            # ── Step 7: Update active_context ─────────────────────────────────
            if status == "success":
                update_active_context_success(
                    context_path, iteration, parsed, evaluation, graph_refs
                )
                n_findings = len(evaluation.get("key_findings", []))
                logger.info(f"  [Context] Added {n_findings} finding(s), {len(graph_refs)} graph(s)")
            else:
                update_active_context_failure(context_path, iteration, parsed, evaluation)
                logger.info(f"  [Context] Logged failure: {evaluation.get('error_type', 'unknown')}")

            # ── Step 8: Run Summarizer ────────────────────────────────────────
            if iteration % summarizer_every == 0 and iteration < n_iterations:
                ctx_before = read_file(context_path)
                logger.info("  [Summarizer] Compressing active_context.md...")
                logger.debug(f"  [Summarizer] Context size before: {len(ctx_before):,}ch")
                summarizer_msg = (
                    f"Current iteration: {iteration} of {n_iterations} total.\n\n"
                    + ctx_before
                )
                new_ctx, in_tok, out_tok = call_llm(
                    client, SUMMARIZER_SYSTEM_PROMPT, summarizer_msg, model, max_tokens,
                    tag="Summarizer",
                )
                total_input_tokens += in_tok
                total_output_tokens += out_tok

                write_file(context_path, new_ctx)
                logger.info(
                    f"  [Summarizer] Compressed: {len(ctx_before):,}ch → {len(new_ctx):,}ch"
                )

        except KeyboardInterrupt:
            logger.info("\n[Interrupted] Exiting loop cleanly.")
            break
        except Exception as exc:
            logger.error(f"  [FATAL ERROR] Iteration {iteration}: {exc}", exc_info=True)
            error_entry = _format_archive_entry(
                iteration, None, str(exc), "", None, "FATAL_ERROR"
            )
            append_file(archive_path, error_entry)

        # ── Iteration summary ─────────────────────────────────────────────────
        iter_elapsed = time.time() - iter_start
        logger.info(
            f"  [Iter {iteration}] Done in {iter_elapsed:.0f}s | "
            f"cumulative tokens in={total_input_tokens:,} out={total_output_tokens:,}"
        )

        # ── Step 9: Wait between iterations ──────────────────────────────────
        if iteration < n_iterations:
            wait_sec = config["interval_minutes"] * 60
            if wait_sec > 0:
                logger.info(f"  [Wait] Sleeping {config['interval_minutes']} minute(s)...")
                time.sleep(wait_sec)

    # ── Done ─────────────────────────────────────────────────────────────────
    logger.info(f"\n{'=' * 60}")
    logger.info("LOOP COMPLETE")
    logger.info(f"Archive : {archive_path}")
    logger.info(f"Context : {context_path}")
    logger.info(f"Graphs  : {graphs_folder}")
    logger.info(f"Log     : {log_file}")
    logger.info(
        f"Total tokens — input: {total_input_tokens:,}  output: {total_output_tokens:,}  "
        f"total: {total_input_tokens + total_output_tokens:,}"
    )
    logger.info("Run phase2.py to generate the final report.")
    logger.info(f"{'=' * 60}")

if __name__ == "__main__":
    main()
