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


# ─── Databook Generator ─────────────────────────────────────────────────────

DATABOOK_SYSTEM_PROMPT = """You are a financial due diligence data specialist. Your task is to create a
structured "databook" — a polished Excel workbook that organizes analysis results into clean,
stakeholder-ready data tables.

You will receive:
- The archive of all successful analysis iterations (code + output + evaluation)
- The active knowledge base with established facts

Your job:
1. Review all successful iterations and their outputs
2. Write Python code that creates a multi-sheet Excel workbook organized by BUSINESS THEME
   (not by iteration number)

The databook should contain sheets like:
- "Executive Summary" — key metrics, headline numbers, date range
- "Revenue Overview" — revenue by year, channel, product (pivot tables)
- "Client Analysis" — top clients, concentration, new vs returning
- "Product Analysis" — product mix, top SKUs, category breakdown
- "Trends & Seasonality" — time series data, growth rates, CAGR
- "Risk Metrics" — concentration indices, dependency ratios
- Any other themes that emerge from the data

Rules for the code:
- Import pandas and openpyxl
- Load data from: df = pd.read_excel("workspace/data.xlsx")
- Save to the path provided as the output file
- Use pd.ExcelWriter with openpyxl engine
- Create well-structured tables with clear headers
- Include computed metrics (totals, percentages, growth rates, CAGR)
- Format currency as € with appropriate suffixes (€k, €m)
- Each sheet should be self-contained and understandable
- Print the file path and sheet names after saving
- Be self-contained (no imports from project modules except standard libs)

Return ONLY a JSON object:
{
  "description": "What this databook contains",
  "sheets": ["sheet1", "sheet2", ...],
  "code": "full Python code"
}
No preamble. No markdown fences around the JSON."""


def generate_databook(
    client: anthropic.Anthropic,
    model: str,
    archive_path: str,
    context_path: str,
    output_path: str,
    max_tokens: int = 20000,
) -> str:
    """
    Use the LLM to generate a curated databook Excel file organized by business theme.

    The LLM reads all successful iterations, understands what data is available,
    and writes Python code that creates a polished multi-sheet workbook.

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

    # Read active context for established facts
    context_text = ""
    if Path(context_path).exists():
        context_text = read_file(context_path)

    # Build condensed view of successful analyses
    analyses_summary = []
    for e in success_entries:
        analyses_summary.append(
            f"--- Iteration {e.get('iteration', '?')}: {e.get('analysis_type', 'unknown')} ---\n"
            f"Hypothesis: {e.get('hypothesis', '')}\n"
            f"Columns: {e.get('columns_used', '')}\n"
            f"Code:\n{e.get('code', '')}\n"
            f"Output:\n{e.get('output', '')[:2000]}\n"
        )

    user_msg = f"""## Task
Create a comprehensive databook Excel file that organizes all analysis findings into
clean, theme-based sheets suitable for stakeholder review.

## Output File Path
{output_path}

## Successful Analyses ({len(success_entries)} iterations)
{chr(10).join(analyses_summary)}

## Knowledge Base (Established Facts)
{context_text[:6000]}

## Data File
Path: workspace/data.xlsx

Write Python code that creates a polished databook with thematic sheets. Each sheet should
contain well-formatted tables with business-relevant metrics derived from the raw data.
Re-run the actual data transformations (don't just copy printed output) to ensure accuracy."""

    logger.info(f"[Databook] Generating databook from {len(success_entries)} successful iterations...")

    response = call_llm(
        client, DATABOOK_SYSTEM_PROMPT, user_msg, model, max_tokens, tag="Databook"
    )

    parsed = parse_json_response(response)
    if parsed is None:
        logger.error("[Databook] Failed to parse databook plan from LLM")
        return ""

    description = parsed.get("description", "Analysis databook")
    sheets = parsed.get("sheets", [])
    code = parsed.get("code", "")

    logger.info(f"  [Databook] Description: {description}")
    logger.info(f"  [Databook] Planned sheets: {sheets}")

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Write and execute the databook generation code
    script_path = "workspace/databook_script.py"
    write_file(script_path, code)

    logger.info("  [Databook] Running databook generation code...")

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    max_attempts = 2
    for attempt in range(1, max_attempts + 1):
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

            if result.stdout:
                logger.info(f"  [Databook] Output:\n{result.stdout}")

            if result.stderr and attempt < max_attempts:
                logger.warning(f"  [Databook] Error (attempt {attempt}):\n{result.stderr}")
                logger.info("  [Databook] Asking LLM to fix the code...")

                retry_msg = f"""The databook code produced an error. Fix it.

## Original Code
```python
{code}
```

## Error
{result.stderr}

## stdout (if any)
{result.stdout}

## Output File Path
{output_path}

Return the same JSON format with corrected code."""

                retry_response = call_llm(
                    client, DATABOOK_SYSTEM_PROMPT, retry_msg, model, max_tokens, tag="DatabookRetry"
                )
                retry_parsed = parse_json_response(retry_response)
                if retry_parsed and retry_parsed.get("code"):
                    code = retry_parsed["code"]
                    write_file(script_path, code)
                    continue
                else:
                    logger.error("  [Databook] Failed to parse retry response")
                    return ""

            elif result.stderr:
                logger.error(f"  [Databook] Final error:\n{result.stderr}")
                return ""

            # Check if file was created
            if Path(output_path).exists():
                size_kb = Path(output_path).stat().st_size / 1024
                logger.info(f"  [Databook] SUCCESS — {output_path} ({size_kb:.1f} KB)")
                return output_path

            # Check for any xlsx in the expected directory
            parent = Path(output_path).parent
            xlsx_files = list(parent.glob("*.xlsx"))
            if xlsx_files:
                latest = max(xlsx_files, key=lambda p: p.stat().st_mtime)
                size_kb = latest.stat().st_size / 1024
                logger.info(f"  [Databook] SUCCESS — {latest} ({size_kb:.1f} KB)")
                return str(latest)

            logger.error("  [Databook] No Excel file was created")
            return ""

        except subprocess.TimeoutExpired:
            logger.error("[Databook] Code execution timed out")
            return ""

    return ""


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
