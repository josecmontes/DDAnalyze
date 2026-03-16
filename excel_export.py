#!/usr/bin/env python3
"""
Excel Export & Databook Generator

Exports completed analysis iterations to a structured Excel workbook that serves
as a reviewable backup of all findings. Can also generate an LLM-curated
"databook" — a polished multi-sheet Excel file organized by analysis theme,
with clean tables and key metrics ready for stakeholder review.

Usage:
  # Standalone — export raw iteration data
  python excel_export.py

  # Standalone — generate curated databook
  python excel_export.py --databook

  # Called from orchestrator via /export-all or /databook commands
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
import pandas as pd
import yaml
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

os.chdir(Path(__file__).parent)

# ─── Logging Setup ────────────────────────────────────────────────────────────

logger = logging.getLogger("ddanalyze.excel_export")


def setup_logging(debug: bool = False, log_dir: str = "logs") -> Path:
    Path(log_dir).mkdir(exist_ok=True)
    log_file = Path(log_dir) / f"excel_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    console_level = logging.DEBUG if debug else logging.INFO

    root = logging.getLogger("ddanalyze")
    root.setLevel(logging.DEBUG)

    if not root.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(console_level)
        ch.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
        root.addHandler(ch)

        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)-7s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        root.addHandler(fh)

    return log_file


# ─── File Utilities ───────────────────────────────────────────────────────────

def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_file(path: str, content: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


# ─── Archive Parser ──────────────────────────────────────────────────────────

_INTERNAL_ERROR_LABELS = {
    "JSON_PARSE_ERROR", "CRITIC_JSON_PARSE_ERROR", "TIMEOUT", "FATAL_ERROR",
    "SYNTH_JSON_PARSE_ERROR",
}


def parse_archive_entries(archive_text: str) -> list:
    """Parse full_archive.txt into structured dicts for all non-internal-error entries."""
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

        entry = {"status": status}
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
        code_m = re.search(r"CODE:\n(.*?)\n" + re.escape(dash_sep), block, re.DOTALL)
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

    return entries


def _parse_evaluation_fields(eval_text: str) -> dict:
    """Extract structured fields from the evaluation text block."""
    fields = {}
    if not eval_text:
        return fields

    quality_m = re.search(r"Quality:\s*(.+)", eval_text)
    if quality_m:
        fields["quality"] = quality_m.group(1).strip()

    summary_m = re.search(r"Summary:\s*(.+?)(?=\nKey findings:|\nSuggested followup:|\nError type:|\Z)", eval_text, re.DOTALL)
    if summary_m:
        fields["summary"] = summary_m.group(1).strip()

    findings = re.findall(r"  - (.+)", eval_text)
    if findings:
        fields["key_findings"] = findings

    followup_m = re.search(r"Suggested followup:\s*(.+?)(?=\nConfirmed dead ends:|\Z)", eval_text, re.DOTALL)
    if followup_m:
        fields["suggested_followup"] = followup_m.group(1).strip()

    error_m = re.search(r"Error type:\s*(.+)", eval_text)
    if error_m:
        fields["error_type"] = error_m.group(1).strip()

    return fields


# ─── Excel Styling ────────────────────────────────────────────────────────────

# Deloitte-inspired color palette
HEADER_FILL = PatternFill(start_color="86BC25", end_color="86BC25", fill_type="solid")  # Deloitte green
HEADER_FONT = Font(name="Arial", bold=True, color="FFFFFF", size=11)
TITLE_FONT = Font(name="Arial", bold=True, size=14, color="000000")
SUBTITLE_FONT = Font(name="Arial", bold=True, size=11, color="333333")
BODY_FONT = Font(name="Arial", size=10, color="333333")
SUCCESS_FILL = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
FAILURE_FILL = PatternFill(start_color="FFEBEE", end_color="FFEBEE", fill_type="solid")
THIN_BORDER = Border(
    left=Side(style="thin", color="CCCCCC"),
    right=Side(style="thin", color="CCCCCC"),
    top=Side(style="thin", color="CCCCCC"),
    bottom=Side(style="thin", color="CCCCCC"),
)
WRAP_ALIGNMENT = Alignment(wrap_text=True, vertical="top")
TOP_ALIGNMENT = Alignment(vertical="top")


def _style_header_row(ws, row: int, max_col: int) -> None:
    """Apply header styling to a row."""
    for col in range(1, max_col + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.border = THIN_BORDER
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _style_data_cell(ws, row: int, col: int, wrap: bool = False) -> None:
    """Apply body styling to a data cell."""
    cell = ws.cell(row=row, column=col)
    cell.font = BODY_FONT
    cell.border = THIN_BORDER
    cell.alignment = WRAP_ALIGNMENT if wrap else TOP_ALIGNMENT


def _auto_width(ws, max_width: int = 50, min_width: int = 10) -> None:
    """Set column widths based on content, capped at max_width."""
    for col_cells in ws.columns:
        col_letter = get_column_letter(col_cells[0].column)
        max_len = 0
        for cell in col_cells:
            if cell.value:
                lines = str(cell.value).split("\n")
                max_line = max(len(line) for line in lines) if lines else 0
                max_len = max(max_len, max_line)
        adjusted = min(max(max_len + 2, min_width), max_width)
        ws.column_dimensions[col_letter].width = adjusted


# ─── Raw Export: All Iterations to Excel ──────────────────────────────────────

def export_iterations_to_excel(
    archive_path: str,
    context_path: str,
    output_path: str,
    graphs_folder: str = "workspace/graphs",
) -> str:
    """
    Export all completed iterations from the archive to a structured Excel workbook.

    Creates sheets:
      1. Summary — iteration index with status, type, hypothesis, date
      2. Findings — all key findings extracted from successful iterations
      3. Per-iteration detail sheets (Iter_01, Iter_02, ...) with full output + evaluation
      4. Graphs Index — catalog of all generated charts

    Returns the path of the created Excel file.
    """
    archive_text = read_file(archive_path)
    entries = parse_archive_entries(archive_text)

    if not entries:
        logger.warning("[Export] No entries found in archive.")
        return ""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if output_path.endswith(".xlsx"):
        final_path = output_path
    else:
        final_path = os.path.join(output_path, f"iterations_export_{timestamp}.xlsx")
        Path(final_path).parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"[Export] Writing {len(entries)} iterations to {final_path}")

    with pd.ExcelWriter(final_path, engine="openpyxl") as writer:
        # ── Sheet 1: Summary ──────────────────────────────────────────────
        summary_rows = []
        for e in entries:
            eval_fields = _parse_evaluation_fields(e.get("evaluation", ""))
            summary_rows.append({
                "Iteration": int(e.get("iteration", 0)),
                "Date": e.get("date", ""),
                "Status": e.get("status", ""),
                "Analysis Type": e.get("analysis_type", ""),
                "Hypothesis": e.get("hypothesis", ""),
                "Quality": eval_fields.get("quality", ""),
                "Summary": eval_fields.get("summary", ""),
                "Columns Used": e.get("columns_used", ""),
            })

        df_summary = pd.DataFrame(summary_rows)
        df_summary.to_excel(writer, sheet_name="Summary", index=False, startrow=1)

        ws_summary = writer.sheets["Summary"]
        ws_summary.cell(row=1, column=1, value="DDAnalyze — Iteration Summary")
        ws_summary.cell(row=1, column=1).font = TITLE_FONT
        _style_header_row(ws_summary, 2, len(df_summary.columns))

        for row_idx in range(3, len(df_summary) + 3):
            for col_idx in range(1, len(df_summary.columns) + 1):
                _style_data_cell(ws_summary, row_idx, col_idx, wrap=(col_idx >= 5))
            # Color code by status
            status_val = ws_summary.cell(row=row_idx, column=3).value
            if status_val and str(status_val).lower() == "success":
                ws_summary.cell(row=row_idx, column=3).fill = SUCCESS_FILL
            elif status_val:
                ws_summary.cell(row=row_idx, column=3).fill = FAILURE_FILL

        _auto_width(ws_summary)

        # ── Sheet 2: Findings ────────────────────────────────────────────
        findings_rows = []
        for e in entries:
            if e.get("status", "").lower() != "success":
                continue
            eval_fields = _parse_evaluation_fields(e.get("evaluation", ""))
            for finding in eval_fields.get("key_findings", []):
                findings_rows.append({
                    "Iteration": int(e.get("iteration", 0)),
                    "Analysis Type": e.get("analysis_type", ""),
                    "Finding": finding,
                })
            if eval_fields.get("suggested_followup"):
                findings_rows.append({
                    "Iteration": int(e.get("iteration", 0)),
                    "Analysis Type": e.get("analysis_type", ""),
                    "Finding": f"[FOLLOWUP] {eval_fields['suggested_followup']}",
                })

        if findings_rows:
            df_findings = pd.DataFrame(findings_rows)
            df_findings.to_excel(writer, sheet_name="Findings", index=False, startrow=1)

            ws_findings = writer.sheets["Findings"]
            ws_findings.cell(row=1, column=1, value="Key Findings & Suggested Follow-ups")
            ws_findings.cell(row=1, column=1).font = TITLE_FONT
            _style_header_row(ws_findings, 2, len(df_findings.columns))
            for row_idx in range(3, len(df_findings) + 3):
                for col_idx in range(1, len(df_findings.columns) + 1):
                    _style_data_cell(ws_findings, row_idx, col_idx, wrap=(col_idx == 3))
            _auto_width(ws_findings)

        # ── Sheet 3+: Per-iteration detail sheets ────────────────────────
        for e in entries:
            iter_num = int(e.get("iteration", 0))
            sheet_name = f"Iter_{iter_num:02d}"
            # Excel sheet names max 31 chars
            if len(sheet_name) > 31:
                sheet_name = sheet_name[:31]

            eval_fields = _parse_evaluation_fields(e.get("evaluation", ""))

            # Build a detail dataframe with labeled rows
            detail_rows = [
                {"Field": "Iteration", "Value": iter_num},
                {"Field": "Date", "Value": e.get("date", "")},
                {"Field": "Status", "Value": e.get("status", "")},
                {"Field": "Analysis Type", "Value": e.get("analysis_type", "")},
                {"Field": "Hypothesis", "Value": e.get("hypothesis", "")},
                {"Field": "Columns Used", "Value": e.get("columns_used", "")},
                {"Field": "Quality", "Value": eval_fields.get("quality", "")},
                {"Field": "Summary", "Value": eval_fields.get("summary", "")},
            ]

            for i, finding in enumerate(eval_fields.get("key_findings", []), 1):
                detail_rows.append({"Field": f"Finding {i}", "Value": finding})

            if eval_fields.get("suggested_followup"):
                detail_rows.append({"Field": "Suggested Followup", "Value": eval_fields["suggested_followup"]})

            if eval_fields.get("error_type"):
                detail_rows.append({"Field": "Error Type", "Value": eval_fields["error_type"]})

            # Add code and output as the last rows
            detail_rows.append({"Field": "Code", "Value": e.get("code", "")})
            detail_rows.append({"Field": "Output", "Value": e.get("output", "")})

            df_detail = pd.DataFrame(detail_rows)
            df_detail.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)

            ws_detail = writer.sheets[sheet_name]
            title = f"Iteration {iter_num} — {e.get('analysis_type', 'Unknown')}"
            ws_detail.cell(row=1, column=1, value=title)
            ws_detail.cell(row=1, column=1).font = TITLE_FONT
            _style_header_row(ws_detail, 2, 2)

            for row_idx in range(3, len(df_detail) + 3):
                _style_data_cell(ws_detail, row_idx, 1)
                _style_data_cell(ws_detail, row_idx, 2, wrap=True)
                # Bold the field labels
                ws_detail.cell(row=row_idx, column=1).font = Font(
                    name="Arial", bold=True, size=10, color="333333"
                )

            ws_detail.column_dimensions["A"].width = 20
            ws_detail.column_dimensions["B"].width = 100

        # ── Graphs Index sheet ───────────────────────────────────────────
        graphs_dir = Path(graphs_folder)
        if graphs_dir.exists():
            png_files = sorted(graphs_dir.glob("*.png"))
            if png_files:
                graph_rows = []
                for png in png_files:
                    # Try to extract iteration number from filename
                    iter_m = re.match(r"iter(\d+)", png.stem)
                    iter_num = int(iter_m.group(1)) if iter_m else 0
                    graph_rows.append({
                        "Iteration": iter_num,
                        "Filename": png.name,
                        "Size (KB)": round(png.stat().st_size / 1024, 1),
                        "Path": str(png),
                    })

                df_graphs = pd.DataFrame(graph_rows)
                df_graphs.to_excel(writer, sheet_name="Graphs", index=False, startrow=1)

                ws_graphs = writer.sheets["Graphs"]
                ws_graphs.cell(row=1, column=1, value="Generated Graphs Index")
                ws_graphs.cell(row=1, column=1).font = TITLE_FONT
                _style_header_row(ws_graphs, 2, len(df_graphs.columns))
                for row_idx in range(3, len(df_graphs) + 3):
                    for col_idx in range(1, len(df_graphs.columns) + 1):
                        _style_data_cell(ws_graphs, row_idx, col_idx)
                _auto_width(ws_graphs)

    size_kb = Path(final_path).stat().st_size / 1024
    logger.info(f"[Export] SUCCESS — {final_path} ({size_kb:.1f} KB, {len(entries)} iterations)")
    return final_path


# ─── LLM Utilities ───────────────────────────────────────────────────────────

def call_llm(
    client: anthropic.Anthropic,
    system: str,
    user: str,
    model: str,
    max_tokens: int,
    tag: str = "LLM",
) -> str:
    """Call Claude and return text response."""
    logger.debug(f"[{tag}] Sending request | system={len(system):,}ch user={len(user):,}ch")
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
    logger.info(f"  [{tag}] Done in {elapsed:.1f}s | tokens in={in_tok:,} out={out_tok:,}")

    for block in response.content:
        if block.type == "text":
            return block.text
    return ""


def parse_json_response(text: str) -> Optional[dict]:
    """Robustly extract a JSON object from LLM response."""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass

    return None


# ─── Databook Generator (Two-Stage Pipeline) ─────────────────────────────────
#
# Stage 1 (Planner): Reads condensed findings + column info → returns a JSON
#          blueprint listing sheets, their contents, and the data transformations
#          needed. Small output (~2K tokens).
#
# Stage 2 (Coder):   Receives the blueprint + column schema → writes one
#          self-contained Python script that creates all sheets. Focused prompt
#          keeps output within token budget.
#

DATABOOK_PLANNER_PROMPT = """You are a financial due diligence data specialist planning a "databook" —
a polished multi-sheet Excel workbook organized by business theme.

You will receive:
- A summary of all successful analysis iterations (type, hypothesis, key output tables)
- Column names and dtypes from the raw dataset
- The established facts from the knowledge base

Design a databook blueprint. Choose 4-7 sheets that best represent the analysis findings.
Each sheet should correspond to a BUSINESS THEME (not an iteration number).

Return ONLY a JSON object:
{
  "description": "One-line description of the databook",
  "sheets": [
    {
      "name": "Sheet Name (max 31 chars)",
      "purpose": "What this sheet shows",
      "tables": ["table 1 description", "table 2 description"],
      "metrics": ["metric 1", "metric 2"],
      "relevant_columns": ["col1", "col2"]
    }
  ]
}
No preamble. No markdown fences."""

DATABOOK_CODER_PROMPT = """You are a Python data engineer. Write a SINGLE self-contained Python script
that creates a multi-sheet Excel databook.

CRITICAL RULES:
- Import only pandas, openpyxl, and standard libraries
- Load data: df = pd.read_excel("workspace/data.xlsx")
- Save to the EXACT path provided (use pd.ExcelWriter with openpyxl engine)
- Keep the code COMPACT — avoid verbose formatting/styling code
- Use simple column headers and clean numeric formatting
- For currency, divide large numbers and add column suffix like "Revenue (€k)" or "Revenue (€m)"
- Print the output file path and sheet names when done
- Do NOT import any project modules

You will receive:
- The databook blueprint (sheets, their purpose, metrics)
- The dataset column names and dtypes
- Sample output from relevant analyses (to understand data patterns)

Return ONLY a JSON object:
{
  "code": "full Python code"
}
No preamble. No markdown fences around the JSON."""


def _build_condensed_context(entries: list, context_text: str, data_file: str) -> tuple:
    """
    Build compact representations of the analysis results and dataset schema.
    Returns (analyses_summary: str, column_info: str).
    """
    # Get column info from the actual data file
    column_info = "(column info unavailable)"
    try:
        df_sample = pd.read_excel(data_file, nrows=5)
        col_lines = []
        for col in df_sample.columns:
            dtype = str(df_sample[col].dtype)
            sample = str(df_sample[col].iloc[0]) if len(df_sample) > 0 else ""
            col_lines.append(f"  {col} ({dtype}): e.g. {sample}")
        column_info = f"Columns ({len(df_sample.columns)}):\n" + "\n".join(col_lines)
    except Exception as e:
        logger.warning(f"[Databook] Could not read data file for schema: {e}")

    # Build condensed iteration summaries — OUTPUT only (no code), truncated
    analyses_parts = []
    for e in entries:
        eval_fields = _parse_evaluation_fields(e.get("evaluation", ""))
        findings = eval_fields.get("key_findings", [])
        findings_str = ("\n".join(f"  - {f}" for f in findings)) if findings else "(none)"

        # Include output tables but cap at 1500 chars to keep prompt manageable
        output_preview = e.get("output", "")[:1500]

        analyses_parts.append(
            f"--- Iter {e.get('iteration', '?')}: {e.get('analysis_type', 'unknown')} ---\n"
            f"Hypothesis: {e.get('hypothesis', '')}\n"
            f"Columns: {e.get('columns_used', '')}\n"
            f"Key findings:\n{findings_str}\n"
            f"Output preview:\n{output_preview}\n"
        )

    analyses_summary = "\n".join(analyses_parts)

    return analyses_summary, column_info


def _run_script_with_retry(
    client: anthropic.Anthropic,
    model: str,
    code: str,
    script_path: str,
    output_path: str,
    coder_context: str,
    max_tokens: int,
) -> str:
    """Execute the databook script, retry once via LLM on error. Returns output path or ''."""
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    for attempt in range(1, 3):
        write_file(script_path, code)
        logger.info(f"  [Databook] Running code (attempt {attempt})...")

        try:
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                timeout=180,
            )
        except subprocess.TimeoutExpired:
            logger.error("[Databook] Code execution timed out")
            return ""

        if result.stdout:
            logger.info(f"  [Databook] stdout:\n{result.stdout}")

        if not result.stderr:
            # Success — check file
            if Path(output_path).exists():
                size_kb = Path(output_path).stat().st_size / 1024
                logger.info(f"  [Databook] SUCCESS — {output_path} ({size_kb:.1f} KB)")
                return output_path
            # Maybe saved with slightly different name
            parent = Path(output_path).parent
            xlsx_files = list(parent.glob("*.xlsx"))
            if xlsx_files:
                latest = max(xlsx_files, key=lambda p: p.stat().st_mtime)
                size_kb = latest.stat().st_size / 1024
                logger.info(f"  [Databook] SUCCESS — {latest} ({size_kb:.1f} KB)")
                return str(latest)
            logger.error("  [Databook] No Excel file was created")
            return ""

        # Error — retry once
        if attempt >= 2:
            logger.error(f"  [Databook] Final error:\n{result.stderr}")
            return ""

        logger.warning(f"  [Databook] Error:\n{result.stderr[:500]}")
        logger.info("  [Databook] Asking LLM to fix...")

        retry_msg = f"""The databook code produced an error. Fix it.

## Code
```python
{code}
```

## Error
{result.stderr}

## stdout
{result.stdout[:1000] if result.stdout else '(none)'}

## Output File Path
{output_path}

Return ONLY a JSON object: {{"code": "fixed Python code"}}"""

        retry_response = call_llm(
            client, DATABOOK_CODER_PROMPT, retry_msg, model, max_tokens, tag="DatabookFix"
        )
        retry_parsed = parse_json_response(retry_response)
        if retry_parsed and retry_parsed.get("code"):
            code = retry_parsed["code"]
        else:
            logger.error("  [Databook] Failed to parse fix response")
            return ""

    return ""


def generate_databook(
    client: anthropic.Anthropic,
    model: str,
    archive_path: str,
    context_path: str,
    output_path: str,
    max_tokens: int = 20000,
) -> str:
    """
    Two-stage databook generation:
      Stage 1 (Planner): LLM designs sheet blueprint from condensed findings
      Stage 2 (Coder):   LLM writes compact Python code to create all sheets

    Returns the path of the created Excel file, or empty string on failure.
    """
    if not Path(archive_path).exists():
        logger.error("[Databook] No archive found. Run data analysis first.")
        return ""

    archive_text = read_file(archive_path)
    entries = parse_archive_entries(archive_text)
    success_entries = [e for e in entries if e.get("status", "").lower() == "success"]

    if not success_entries:
        logger.error("[Databook] No successful analyses in archive.")
        return ""

    context_text = ""
    if Path(context_path).exists():
        context_text = read_file(context_path)

    data_file = "workspace/data.xlsx"
    analyses_summary, column_info = _build_condensed_context(
        success_entries, context_text, data_file
    )

    # ── Stage 1: Plan the databook ───────────────────────────────────────
    logger.info(f"[Databook] Stage 1 — Planning from {len(success_entries)} successful iterations...")

    planner_msg = f"""## Successful Analyses ({len(success_entries)} iterations)
{analyses_summary}

## Dataset Schema
{column_info}

## Established Facts (from knowledge base)
{context_text[:4000]}

Design a databook with 4-7 thematic sheets based on the analyses above."""

    planner_response = call_llm(
        client, DATABOOK_PLANNER_PROMPT, planner_msg, model, 4000, tag="DatabookPlan"
    )

    blueprint = parse_json_response(planner_response)
    if blueprint is None:
        logger.error("[Databook] Failed to parse blueprint from planner")
        return ""

    sheet_plans = blueprint.get("sheets", [])
    logger.info(f"  [Databook] Blueprint: {blueprint.get('description', '')}")
    for sp in sheet_plans:
        logger.info(f"    Sheet: {sp.get('name', '?')} — {sp.get('purpose', '')[:80]}")

    # ── Stage 2: Generate code ───────────────────────────────────────────
    logger.info("[Databook] Stage 2 — Generating code...")

    # Build a focused coder prompt with blueprint + schema + sample outputs
    # Only include output previews for iterations relevant to the blueprint
    relevant_iters = set()
    for sp in sheet_plans:
        for col in sp.get("relevant_columns", []):
            for e in success_entries:
                if col.lower() in e.get("columns_used", "").lower():
                    relevant_iters.add(e.get("iteration", ""))

    # If no specific matches, include all
    if not relevant_iters:
        relevant_iters = {e.get("iteration", "") for e in success_entries}

    sample_outputs = []
    for e in success_entries:
        if e.get("iteration", "") in relevant_iters:
            sample_outputs.append(
                f"--- Iter {e.get('iteration')}: {e.get('analysis_type', '')} ---\n"
                f"Columns: {e.get('columns_used', '')}\n"
                f"Output:\n{e.get('output', '')[:1200]}\n"
            )

    blueprint_json = json.dumps(sheet_plans, indent=2)

    coder_msg = f"""## Databook Blueprint
{blueprint_json}

## Output File Path
{output_path}

## Dataset Schema
{column_info}

## Sample Analysis Outputs (for reference)
{chr(10).join(sample_outputs[:8])}

Write a single Python script that creates ALL sheets from the blueprint above.
Keep it compact — focus on data transformations, not Excel styling."""

    coder_response = call_llm(
        client, DATABOOK_CODER_PROMPT, coder_msg, model, max_tokens, tag="DatabookCode"
    )

    coder_parsed = parse_json_response(coder_response)
    if coder_parsed is None:
        logger.error("[Databook] Failed to parse code from coder")
        return ""

    code = coder_parsed.get("code", "")
    if not code:
        logger.error("[Databook] Coder returned empty code")
        return ""

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # ── Execute with retry ───────────────────────────────────────────────
    script_path = "workspace/databook_script.py"
    return _run_script_with_retry(
        client, model, code, script_path, output_path, coder_msg, max_tokens
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="DDAnalyze Excel Export & Databook Generator"
    )
    parser.add_argument(
        "--databook",
        action="store_true",
        help="Generate an LLM-curated databook (requires API key)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output file path (default: workspace/exports/iterations_export_<timestamp>.xlsx "
             "or workspace/exports/databook_<timestamp>.xlsx)",
    )
    args = parser.parse_args()

    # Load config
    config = yaml.safe_load(read_file("config.yaml"))
    debug_logging = config.get("debug_logging", False)
    log_file = setup_logging(debug=debug_logging)

    archive_path = config.get("archive_file", "full_archive.txt")
    context_path = config.get("active_context_file", "active_context.md")
    graphs_folder = config.get("graphs_folder", "workspace/graphs")

    if not Path(archive_path).exists():
        logger.error(f"Archive file not found: {archive_path}")
        logger.error("Run data analysis first (python loop.py or python orchestrator.py)")
        sys.exit(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.databook:
        # ── Databook mode: LLM-curated thematic workbook ────────────────
        model = config.get("model", "claude-sonnet-4-6")
        max_tokens = config.get("databook_max_tokens", 20000)

        output_path = args.output or f"workspace/exports/databook_{timestamp}.xlsx"
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        client = anthropic.Anthropic(api_key=API_KEY)

        logger.info(f"\n{'═' * 60}")
        logger.info("DDAnalyze DATABOOK GENERATOR")
        logger.info(f"Model  : {model}")
        logger.info(f"Output : {output_path}")
        logger.info(f"Log    : {log_file}")
        logger.info(f"{'═' * 60}")

        result = generate_databook(
            client, model, archive_path, context_path, output_path, max_tokens
        )

        if result:
            print(f"\nDatabook created: {result}")
        else:
            print("\nDatabook generation failed. Check the log for details.")
            sys.exit(1)

    else:
        # ── Raw export mode: structured iteration dump ────────────────────
        output_path = args.output or f"workspace/exports/iterations_export_{timestamp}.xlsx"

        logger.info(f"\n{'═' * 60}")
        logger.info("DDAnalyze EXCEL EXPORT")
        logger.info(f"Output : {output_path}")
        logger.info(f"Log    : {log_file}")
        logger.info(f"{'═' * 60}")

        result = export_iterations_to_excel(
            archive_path, context_path, output_path, graphs_folder
        )

        if result:
            print(f"\nExport created: {result}")
        else:
            print("\nExport failed. Check the log for details.")
            sys.exit(1)


if __name__ == "__main__":
    main()
