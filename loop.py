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

# ─── System Prompts ───────────────────────────────────────────────────────────

ANALYST_SYSTEM_PROMPT = """You are the Analyst agent in an autonomous data analysis loop. Your job is to investigate \
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

MANDATORY — YEARLY TABLES:
For any analysis spanning multiple time periods (revenue trends, customer counts, retention, etc.)
you MUST produce a structured year-by-year comparison table. Use exactly 3–4 fiscal/calendar years
plus an LTM (Last-Twelve-Months) column when the latest year is incomplete.
Table format (print with print() and aligned columns):

  Year    | Metric A  | Metric B  | YoY %
  --------|-----------|-----------|-------
  FY2021  |    x      |    x      |  —
  FY2022  |    x      |    x      | +X%
  FY2023  |    x      |    x      | +X%
  LTM     |    x      |    x      | +X%
  CAGR    |           |           | XX%

Always compute and print CAGR when you have 3+ years of data.
Use pandas tabulate or manual string formatting — never raw DataFrame repr.

GRAPHS:
You may (and should, when insightful) generate charts using matplotlib or seaborn.
- Save every chart to the graphs folder provided in the iteration info.
- Use a descriptive filename: e.g. iter{N}_revenue_by_year.png
- After saving, print EXACTLY this line (one per graph):
    GRAPH_SAVED: <filename> — <one-line description>
- Always call plt.close() after saving to free memory.
- Do NOT call plt.show() — running headless.
- Set matplotlib backend before importing pyplot: import matplotlib; matplotlib.use('Agg')"""

CRITIC_SYSTEM_PROMPT = """You are the Critic agent in an autonomous data analysis loop. Your job is to evaluate \
whether a data analysis run produced useful business insight.

You will receive the original hypothesis, the Python code, and the terminal output.

Your job:
1. If the code errored (stderr present, exception in output), mark status as "failure".
   Describe the error clearly and suggest what the next agent should try.
2. If the code ran but the output is empty, trivial, or uninformative (e.g. "0 rows found"),
   mark status as "failure" with error_type "EmptyResult".
3. If the output contains real business findings (numbers, distributions, trends, rankings),
   mark status as "success". Write a plain-English summary a business person can read.
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

SUMMARIZER_SYSTEM_PROMPT = """You are the Summarizer agent in an autonomous data analysis loop. The active knowledge \
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
    When the last year is incomplete, use LTM (Last-Twelve-Months). Include CAGR when applicable."""

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
) -> str:
    """Call Claude with streaming and return the full text response."""
    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    ) as stream:
        response = stream.get_final_message()

    for block in response.content:
        if block.type == "text":
            return block.text
    return ""

def parse_json_response(text: str) -> Optional[dict]:
    """Robustly extract a JSON object from an LLM response."""
    text = text.strip()

    # Attempt 1: direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Attempt 2: extract from markdown code fence
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass

    # Attempt 3: find outermost { ... }
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass

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
        else:
            print(f"  [Warning] Graph declared but file not found: {graph_path}")
    return graphs

def update_active_context_success(
    path: str,
    iteration: int,
    parsed: dict,
    evaluation: dict,
    graph_refs: list,
) -> None:
    content = read_file(path)
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

def update_active_context_failure(
    path: str, iteration: int, parsed: dict, evaluation: dict
) -> None:
    content = read_file(path)
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
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT: Script exceeded 120 seconds"
    except Exception as e:
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
    print(f"  [Init] Created {path} from template")

# ─── Main Loop ────────────────────────────────────────────────────────────────

def main() -> None:
    # ── Load config ──────────────────────────────────────────────────────────
    config = yaml.safe_load(read_file("config.yaml"))

    model = config["model"]
    max_tokens = config["max_tokens"]
    n_iterations = config["n_iterations"]
    archive_path = config["archive_file"]
    context_path = config["active_context_file"]
    task_path = config["task_file"]
    script_path = config["workspace_script"]
    summarizer_every = config.get("summarizer_every_n", 10)
    max_code_retries = config.get("max_code_retries", 2)
    graphs_folder = config.get("graphs_folder", "workspace/graphs")

    task_content = read_file(task_path)
    client = anthropic.Anthropic(api_key=API_KEY)

    # ── fresh_start ──────────────────────────────────────────────────────────
    if config.get("fresh_start", False):
        print("[fresh_start] Resetting active_context.md and full_archive.txt")
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

    # ── Banner ───────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("AUTONOMOUS DATA ANALYSIS LOOP")
    print(f"Model  : {model}")
    print(f"Runs   : {n_iterations}  |  Mode: {config['repetition_mode']}")
    print(f"Interval: {config['interval_minutes']} min  |  Summarizer every {summarizer_every} iters")
    print(f"Retries : up to {max_code_retries} per iteration")
    print(f"Graphs  : {graphs_folder}")
    print(f"{'=' * 60}")

    # ── Iteration loop ────────────────────────────────────────────────────────
    for iteration in range(1, n_iterations + 1):
        print(f"\n=== ITERATION {iteration} / {n_iterations} ===")

        try:
            # ── Step 1: Read context ──────────────────────────────────────────
            context = read_file(context_path)

            # ── Step 2: Call Analyst ──────────────────────────────────────────
            print("  [Analyst] Planning analysis...")
            analyst_msg = build_analyst_user_message(task_content, context, iteration, config)
            raw_analyst = call_llm(client, ANALYST_SYSTEM_PROMPT, analyst_msg, model, max_tokens)
            parsed = parse_json_response(raw_analyst)

            if parsed is None:
                print("  [ERROR] Analyst returned invalid JSON — skipping iteration")
                entry = _format_archive_entry(
                    iteration, None, raw_analyst, "", None, "JSON_PARSE_ERROR"
                )
                append_file(archive_path, entry)
                continue

            print(f"  [Analyst] Type : {parsed.get('analysis_type', 'unknown')}")
            print(f"  [Analyst] Hypo : {parsed.get('hypothesis', '')[:120]}")

            # ── Step 3: Write and run code (with retry on error) ──────────────
            write_file(script_path, parsed.get("code", ""))
            print(f"  [Runner] Executing {script_path}...")
            stdout, stderr = run_code(script_path)

            if "TIMEOUT" in stderr:
                print("  [ERROR] Script timed out")
                entry = _format_archive_entry(
                    iteration, parsed, stdout, stderr, None, "TIMEOUT"
                )
                append_file(archive_path, entry)
                continue

            # Retry loop: on code error, ask Analyst to fix (up to max_code_retries)
            retry_count = 0
            while stderr and retry_count < max_code_retries:
                retry_count += 1
                print(
                    f"  [Retry {retry_count}/{max_code_retries}] "
                    f"Code errored — asking Analyst to fix..."
                )
                retry_msg = build_retry_analyst_message(
                    task_content, context, iteration, config,
                    parsed, stdout, stderr, retry_count,
                )
                raw_retry = call_llm(client, ANALYST_SYSTEM_PROMPT, retry_msg, model, max_tokens)
                parsed_retry = parse_json_response(raw_retry)

                if parsed_retry is None:
                    print(f"  [Retry {retry_count}] Analyst returned invalid JSON — stopping retries")
                    break

                parsed = parsed_retry
                write_file(script_path, parsed.get("code", ""))
                print(f"  [Retry {retry_count}] Re-executing fixed code...")
                stdout, stderr = run_code(script_path)

                if "TIMEOUT" in stderr:
                    print(f"  [Retry {retry_count}] Timed out after fix — stopping retries")
                    break

                if not stderr:
                    print(f"  [Retry {retry_count}] Code fixed and ran successfully!")

            preview = (stdout or stderr or "(no output)")[:200].replace("\n", " ")
            print(f"  [Runner] Output : {preview}")

            # ── Step 4: Detect saved graphs ───────────────────────────────────
            graph_refs = _extract_graph_saves(stdout, graphs_folder)
            if graph_refs:
                print(f"  [Graphs] {len(graph_refs)} graph(s) saved: "
                      f"{', '.join(fn for fn, _ in graph_refs)}")

            # ── Step 5: Call Critic ───────────────────────────────────────────
            print("  [Critic] Evaluating...")
            critic_msg = build_critic_user_message(parsed, stdout, stderr, iteration)
            raw_critic = call_llm(client, CRITIC_SYSTEM_PROMPT, critic_msg, model, max_tokens)
            evaluation = parse_json_response(raw_critic)

            if evaluation is None:
                print("  [ERROR] Critic returned invalid JSON — logging and continuing")
                entry = _format_archive_entry(
                    iteration, parsed, stdout, stderr, None, "CRITIC_JSON_PARSE_ERROR"
                )
                append_file(archive_path, entry)
                continue

            status = evaluation.get("status", "unknown")
            quality = evaluation.get("quality", "") if status == "success" else ""
            dead_ends_count = len(evaluation.get("dead_ends", []))
            print(
                f"  [Critic] Status : {status}"
                + (f" | Quality: {quality}" if quality else "")
                + (f" | Dead ends confirmed: {dead_ends_count}" if dead_ends_count else "")
            )

            # ── Step 6: Write to archive (always) ─────────────────────────────
            entry = _format_archive_entry(iteration, parsed, stdout, stderr, evaluation)
            append_file(archive_path, entry)

            # ── Step 7: Update active_context ─────────────────────────────────
            if status == "success":
                update_active_context_success(
                    context_path, iteration, parsed, evaluation, graph_refs
                )
                n_findings = len(evaluation.get("key_findings", []))
                print(f"  [Context] Added {n_findings} finding(s), {len(graph_refs)} graph(s)")
            else:
                update_active_context_failure(context_path, iteration, parsed, evaluation)
                print(f"  [Context] Logged failure: {evaluation.get('error_type', 'unknown')}")

            # ── Step 8: Run Summarizer ────────────────────────────────────────
            if iteration % summarizer_every == 0 and iteration < n_iterations:
                print("  [Summarizer] Compressing active_context.md...")
                current_ctx = read_file(context_path)
                summarizer_msg = (
                    f"Current iteration: {iteration} of {n_iterations} total.\n\n"
                    + current_ctx
                )
                new_ctx = call_llm(
                    client, SUMMARIZER_SYSTEM_PROMPT, summarizer_msg, model, max_tokens
                )
                write_file(context_path, new_ctx)
                print("  [Summarizer] active_context.md compressed")

        except KeyboardInterrupt:
            print("\n[Interrupted] Exiting loop cleanly.")
            break
        except Exception as exc:
            print(f"  [FATAL ERROR] Iteration {iteration}: {exc}")
            error_entry = _format_archive_entry(
                iteration, None, str(exc), "", None, "FATAL_ERROR"
            )
            append_file(archive_path, error_entry)

        # ── Step 9: Wait between iterations ──────────────────────────────────
        if iteration < n_iterations:
            wait_sec = config["interval_minutes"] * 60
            if wait_sec > 0:
                print(f"  [Wait] Sleeping {config['interval_minutes']} minute(s)...")
                time.sleep(wait_sec)

    # ── Done ─────────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("LOOP COMPLETE")
    print(f"Archive : {archive_path}")
    print(f"Context : {context_path}")
    print(f"Graphs  : {graphs_folder}")
    print("Run phase2.py to generate the final report.")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
