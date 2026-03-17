#!/usr/bin/env python3
"""
Excel Export & Databook Generator

Two modes:
  /export-all  →  Quick structured dump of all iterations into one Excel file.
  /databook    →  One professional databook PER successful iteration with Excel formulas.

Usage:
  python excel_export.py                # export-all mode
  python excel_export.py --databook     # generate per-iteration databooks
  python excel_export.py --databook -n 3  # only iteration 3
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

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

os.chdir(Path(__file__).parent)

from utils import (
    read_file, write_file, load_config, setup_logging,
    call_llm, parse_json_response, parse_archive_all, create_client,
)

logger = logging.getLogger("ddanalyze.excel_export")

# ─── Archive Parser (extended with evaluation fields) ─────────────────────────


def parse_archive_entries(archive_text: str) -> list:
    """Alias for shared parse_archive_all."""
    return parse_archive_all(archive_text)


def _parse_evaluation_fields(eval_text: str) -> dict:
    fields = {}
    if not eval_text:
        return fields
    quality_m = re.search(r"Quality:\s*(.+)", eval_text)
    if quality_m:
        fields["quality"] = quality_m.group(1).strip()
    summary_m = re.search(
        r"Summary:\s*(.+?)(?=\nKey findings:|\nSuggested followup:|\nError type:|\Z)",
        eval_text, re.DOTALL,
    )
    if summary_m:
        fields["summary"] = summary_m.group(1).strip()
    findings = re.findall(r"  - (.+)", eval_text)
    if findings:
        fields["key_findings"] = findings
    followup_m = re.search(
        r"Suggested followup:\s*(.+?)(?=\nConfirmed dead ends:|\Z)", eval_text, re.DOTALL,
    )
    if followup_m:
        fields["suggested_followup"] = followup_m.group(1).strip()
    error_m = re.search(r"Error type:\s*(.+)", eval_text)
    if error_m:
        fields["error_type"] = error_m.group(1).strip()
    return fields


# ─── Excel Styling Constants ─────────────────────────────────────────────────

DARK_GREEN = "046A38"
GREEN = "86BC25"
WHITE = "FFFFFF"
LIGHT_GREY = "F2F2F2"
MID_GREY = "D9D9D9"

HEADER_FILL = PatternFill(start_color=DARK_GREEN, end_color=DARK_GREEN, fill_type="solid")
HEADER_FONT = Font(name="Arial", bold=True, color=WHITE, size=10)
TITLE_FONT = Font(name="Arial", bold=True, size=14, color=DARK_GREEN)
BODY_FONT = Font(name="Arial", size=10, color="333333")
ZEBRA_FILL = PatternFill(start_color=LIGHT_GREY, end_color=LIGHT_GREY, fill_type="solid")
SUCCESS_FILL = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
FAILURE_FILL = PatternFill(start_color="FFEBEE", end_color="FFEBEE", fill_type="solid")
THIN_BORDER = Border(
    left=Side(style="thin", color=MID_GREY), right=Side(style="thin", color=MID_GREY),
    top=Side(style="thin", color=MID_GREY), bottom=Side(style="thin", color=MID_GREY),
)
WRAP_ALIGNMENT = Alignment(wrap_text=True, vertical="top")
TOP_ALIGNMENT = Alignment(vertical="top")


def _style_header_row(ws, row: int, max_col: int) -> None:
    for col in range(1, max_col + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.border = THIN_BORDER
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _style_data_cell(ws, row: int, col: int, wrap: bool = False) -> None:
    cell = ws.cell(row=row, column=col)
    cell.font = BODY_FONT
    cell.border = THIN_BORDER
    cell.alignment = WRAP_ALIGNMENT if wrap else TOP_ALIGNMENT


def _auto_width(ws, max_width: int = 50, min_width: int = 10) -> None:
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


def _style_data_sheet(ws, n_rows: int, n_cols: int) -> None:
    _style_header_row(ws, 1, n_cols)
    for row_idx in range(2, n_rows + 2):
        for col_idx in range(1, n_cols + 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            cell.font = BODY_FONT
            cell.border = THIN_BORDER
            if row_idx % 2 == 0:
                cell.fill = ZEBRA_FILL
    ws.auto_filter.ref = f"A1:{get_column_letter(n_cols)}{n_rows + 1}"
    ws.freeze_panes = "A2"
    _auto_width(ws, max_width=30, min_width=8)


# ─── Per-Iteration Auto XLSX (formula-based) ─────────────────────────────────


def generate_iteration_xlsx(
    iteration: int, parsed: dict, stdout: str, data_file: str,
    output_dir: str = "workspace/exports",
) -> str:
    """
    Auto-generate a per-iteration xlsx with:
      - Data sheet (raw data from data.xlsx)
      - Output sheet (text output of the iteration)
      - Analysis sheet (SUMIF/COUNTIF/AVERAGEIF formulas based on columns_used)
    No LLM call needed — fast and deterministic.
    """
    from openpyxl import Workbook

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    analysis_type = parsed.get("analysis_type", "unknown")
    safe_type = re.sub(r"[^\w\s-]", "", analysis_type).strip().replace(" ", "_")[:30]
    output_path = os.path.join(output_dir, f"iter{iteration:03d}_{safe_type}.xlsx")

    try:
        df = pd.read_excel(data_file)
    except Exception as e:
        logger.warning(f"[AutoXLSX] Cannot read data file {data_file}: {e}")
        return ""

    n = len(df)
    col_map = {col: get_column_letter(i + 1) for i, col in enumerate(df.columns)}

    wb = Workbook()

    # ── Sheet 1: Data ──
    ws_data = wb.active
    ws_data.title = "Data"
    for col_idx, col_name in enumerate(df.columns, 1):
        ws_data.cell(row=1, column=col_idx, value=col_name)
    for row_idx, row in enumerate(df.itertuples(index=False), 2):
        for col_idx, value in enumerate(row, 1):
            cell = ws_data.cell(row=row_idx, column=col_idx)
            if pd.isna(value):
                cell.value = None
            else:
                cell.value = value
    _style_data_sheet(ws_data, n, len(df.columns))

    # ── Sheet 2: Output ──
    ws_output = wb.create_sheet("Output")
    ws_output.cell(row=1, column=1, value=f"Iteration {iteration}: {analysis_type}").font = TITLE_FONT
    ws_output.cell(row=2, column=1, value=f"Hypothesis: {parsed.get('hypothesis', '')}").font = BODY_FONT
    ws_output.cell(row=3, column=1, value=f"Columns: {', '.join(parsed.get('columns_used', []))}").font = BODY_FONT
    ws_output.cell(row=4, column=1).value = ""
    for i, line in enumerate(stdout.split("\n")[:500], 5):
        ws_output.cell(row=i, column=1, value=line).font = BODY_FONT
    ws_output.column_dimensions["A"].width = 120

    # ── Sheet 3+: Formula-based analysis sheets ──
    columns_used = parsed.get("columns_used", [])
    # Match columns to actual data columns (case-insensitive fuzzy match)
    actual_cols = list(df.columns)
    col_lower_map = {c.lower(): c for c in actual_cols}

    matched_cols = []
    for c in columns_used:
        if c in actual_cols:
            matched_cols.append(c)
        elif c.lower() in col_lower_map:
            matched_cols.append(col_lower_map[c.lower()])

    numeric_cols = [c for c in matched_cols if c in df.columns and df[c].dtype.kind in ('f', 'i')]
    categorical_cols = [c for c in matched_cols if c in df.columns and df[c].dtype.kind in ('O', 'S', 'U')]

    # Also scan all data columns if matched set is too narrow
    if not numeric_cols:
        numeric_cols = [c for c in actual_cols if df[c].dtype.kind in ('f', 'i')][:3]
    if not categorical_cols:
        categorical_cols = [c for c in actual_cols if df[c].dtype.kind in ('O', 'S', 'U')][:2]

    if numeric_cols and categorical_cols:
        _build_formula_sheets(wb, df, col_map, n, numeric_cols[:3], categorical_cols[:2])

    try:
        wb.save(output_path)
        wb.close()
        size_kb = Path(output_path).stat().st_size / 1024
        logger.info(f"[AutoXLSX] Created {output_path} ({size_kb:.1f} KB)")
        return output_path
    except Exception as e:
        logger.warning(f"[AutoXLSX] Failed to save {output_path}: {e}")
        return ""


def _build_formula_sheets(
    wb, df: pd.DataFrame, col_map: dict, n: int,
    numeric_cols: list, categorical_cols: list,
) -> None:
    """Build formula-based analysis sheets using SUMIF/COUNTIF/AVERAGEIF."""
    for cat_col in categorical_cols:
        cat_letter = col_map.get(cat_col)
        if not cat_letter:
            continue

        unique_vals = df[cat_col].dropna().unique()
        if len(unique_vals) > 100:
            unique_vals = unique_vals[:100]
        if len(unique_vals) == 0:
            continue

        sheet_name = f"By {cat_col}"[:31]
        ws = wb.create_sheet(sheet_name)

        # Title
        ws.cell(row=1, column=1, value=f"Analysis by {cat_col}").font = TITLE_FONT

        # Headers
        row = 3
        ws.cell(row=row, column=1, value=cat_col)
        ws.cell(row=row, column=2, value="Count")
        col_offset = 3
        for nc in numeric_cols:
            ws.cell(row=row, column=col_offset, value=f"Sum of {nc}")
            ws.cell(row=row, column=col_offset + 1, value=f"Avg of {nc}")
            col_offset += 2
        _style_header_row(ws, row, col_offset - 1)
        row += 1

        data_range_cat = f"Data!${cat_letter}$2:${cat_letter}${n + 1}"
        start_data_row = row

        # Data rows with formulas
        for val in unique_vals:
            ws.cell(row=row, column=1, value=str(val)).font = BODY_FONT
            ws.cell(row=row, column=1).border = THIN_BORDER
            # COUNTIF
            ws.cell(row=row, column=2).value = f'=COUNTIF({data_range_cat},A{row})'
            ws.cell(row=row, column=2).font = BODY_FONT
            ws.cell(row=row, column=2).border = THIN_BORDER

            col_offset = 3
            for nc in numeric_cols:
                nc_letter = col_map.get(nc)
                if nc_letter:
                    data_range_num = f"Data!${nc_letter}$2:${nc_letter}${n + 1}"
                    # SUMIF
                    ws.cell(row=row, column=col_offset).value = (
                        f'=SUMIF({data_range_cat},A{row},{data_range_num})'
                    )
                    ws.cell(row=row, column=col_offset).font = BODY_FONT
                    ws.cell(row=row, column=col_offset).border = THIN_BORDER
                    ws.cell(row=row, column=col_offset).number_format = '#,##0.00'
                    # AVERAGEIF
                    ws.cell(row=row, column=col_offset + 1).value = (
                        f'=AVERAGEIF({data_range_cat},A{row},{data_range_num})'
                    )
                    ws.cell(row=row, column=col_offset + 1).font = BODY_FONT
                    ws.cell(row=row, column=col_offset + 1).border = THIN_BORDER
                    ws.cell(row=row, column=col_offset + 1).number_format = '#,##0.00'
                col_offset += 2

            # Zebra striping
            if (row - start_data_row) % 2 == 1:
                for c in range(1, col_offset):
                    ws.cell(row=row, column=c).fill = ZEBRA_FILL
            row += 1

        # Total row
        end_data_row = row - 1
        ws.cell(row=row, column=1, value="TOTAL").font = Font(
            name="Arial", bold=True, size=10, color=DARK_GREEN,
        )
        ws.cell(row=row, column=1).border = THIN_BORDER
        total_col_letter = get_column_letter(2)
        ws.cell(row=row, column=2).value = (
            f'=SUM({total_col_letter}{start_data_row}:{total_col_letter}{end_data_row})'
        )
        ws.cell(row=row, column=2).font = Font(name="Arial", bold=True, size=10)
        ws.cell(row=row, column=2).border = THIN_BORDER

        col_offset = 3
        for nc in numeric_cols:
            sum_letter = get_column_letter(col_offset)
            ws.cell(row=row, column=col_offset).value = (
                f'=SUM({sum_letter}{start_data_row}:{sum_letter}{end_data_row})'
            )
            ws.cell(row=row, column=col_offset).font = Font(name="Arial", bold=True, size=10)
            ws.cell(row=row, column=col_offset).border = THIN_BORDER
            ws.cell(row=row, column=col_offset).number_format = '#,##0.00'
            col_offset += 2

        _auto_width(ws)


# ─── Export-All: Quick Structured Dump ────────────────────────────────────────

def export_iterations_to_excel(
    archive_path: str, context_path: str, output_path: str,
    graphs_folder: str = "workspace/graphs",
) -> str:
    archive_text = read_file(archive_path)
    entries = parse_archive_entries(archive_text)
    if not entries:
        logger.warning("[Export] No entries found in archive.")
        return ""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_path = output_path if output_path.endswith(".xlsx") else \
        os.path.join(output_path, f"iterations_export_{timestamp}.xlsx")
    Path(final_path).parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"[Export] Writing {len(entries)} iterations to {final_path}")

    with pd.ExcelWriter(final_path, engine="openpyxl") as writer:
        # Sheet 1: Summary
        summary_rows = []
        for e in entries:
            ef = _parse_evaluation_fields(e.get("evaluation", ""))
            summary_rows.append({
                "Iteration": int(e.get("iteration", 0)),
                "Date": e.get("date", ""),
                "Status": e.get("status", ""),
                "Analysis Type": e.get("analysis_type", ""),
                "Hypothesis": e.get("hypothesis", ""),
                "Quality": ef.get("quality", ""),
                "Summary": ef.get("summary", ""),
                "Columns Used": e.get("columns_used", ""),
            })
        df_summary = pd.DataFrame(summary_rows)
        df_summary.to_excel(writer, sheet_name="Summary", index=False, startrow=1)
        ws = writer.sheets["Summary"]
        ws.cell(row=1, column=1, value="DDAnalyze — Iteration Summary").font = TITLE_FONT
        _style_header_row(ws, 2, len(df_summary.columns))
        for r in range(3, len(df_summary) + 3):
            for c in range(1, len(df_summary.columns) + 1):
                _style_data_cell(ws, r, c, wrap=(c >= 5))
            sv = ws.cell(row=r, column=3).value
            if sv and str(sv).lower() == "success":
                ws.cell(row=r, column=3).fill = SUCCESS_FILL
            elif sv:
                ws.cell(row=r, column=3).fill = FAILURE_FILL
        _auto_width(ws)

        # Sheet 2: Findings
        findings_rows = []
        for e in entries:
            if e.get("status", "").lower() != "success":
                continue
            ef = _parse_evaluation_fields(e.get("evaluation", ""))
            for f in ef.get("key_findings", []):
                findings_rows.append({
                    "Iteration": int(e.get("iteration", 0)),
                    "Analysis Type": e.get("analysis_type", ""),
                    "Finding": f,
                })
            if ef.get("suggested_followup"):
                findings_rows.append({
                    "Iteration": int(e.get("iteration", 0)),
                    "Analysis Type": e.get("analysis_type", ""),
                    "Finding": f"[FOLLOWUP] {ef['suggested_followup']}",
                })
        if findings_rows:
            df_f = pd.DataFrame(findings_rows)
            df_f.to_excel(writer, sheet_name="Findings", index=False, startrow=1)
            ws2 = writer.sheets["Findings"]
            ws2.cell(row=1, column=1, value="Key Findings & Follow-ups").font = TITLE_FONT
            _style_header_row(ws2, 2, len(df_f.columns))
            for r in range(3, len(df_f) + 3):
                for c in range(1, len(df_f.columns) + 1):
                    _style_data_cell(ws2, r, c, wrap=(c == 3))
            _auto_width(ws2)

        # Per-iteration detail sheets
        for e in entries:
            it = int(e.get("iteration", 0))
            sn = f"Iter_{it:02d}"[:31]
            ef = _parse_evaluation_fields(e.get("evaluation", ""))
            rows = [
                {"Field": "Iteration", "Value": it},
                {"Field": "Date", "Value": e.get("date", "")},
                {"Field": "Status", "Value": e.get("status", "")},
                {"Field": "Analysis Type", "Value": e.get("analysis_type", "")},
                {"Field": "Hypothesis", "Value": e.get("hypothesis", "")},
                {"Field": "Columns Used", "Value": e.get("columns_used", "")},
                {"Field": "Quality", "Value": ef.get("quality", "")},
                {"Field": "Summary", "Value": ef.get("summary", "")},
            ]
            for i, finding in enumerate(ef.get("key_findings", []), 1):
                rows.append({"Field": f"Finding {i}", "Value": finding})
            if ef.get("suggested_followup"):
                rows.append({"Field": "Suggested Followup", "Value": ef["suggested_followup"]})
            if ef.get("error_type"):
                rows.append({"Field": "Error Type", "Value": ef["error_type"]})
            rows.append({"Field": "Code", "Value": e.get("code", "")})
            rows.append({"Field": "Output", "Value": e.get("output", "")})
            df_d = pd.DataFrame(rows)
            df_d.to_excel(writer, sheet_name=sn, index=False, startrow=1)
            wsd = writer.sheets[sn]
            wsd.cell(row=1, column=1, value=f"Iteration {it} — {e.get('analysis_type', '')}").font = TITLE_FONT
            _style_header_row(wsd, 2, 2)
            for r in range(3, len(df_d) + 3):
                _style_data_cell(wsd, r, 1)
                _style_data_cell(wsd, r, 2, wrap=True)
                wsd.cell(row=r, column=1).font = Font(name="Arial", bold=True, size=10, color="333333")
            wsd.column_dimensions["A"].width = 20
            wsd.column_dimensions["B"].width = 100

        # Graphs Index
        graphs_dir = Path(graphs_folder)
        if graphs_dir.exists():
            pngs = sorted(graphs_dir.glob("*.png"))
            if pngs:
                gr = []
                for p in pngs:
                    im = re.match(r"iter(\d+)", p.stem)
                    gr.append({
                        "Iteration": int(im.group(1)) if im else 0,
                        "Filename": p.name,
                        "Size (KB)": round(p.stat().st_size / 1024, 1),
                        "Path": str(p),
                    })
                df_g = pd.DataFrame(gr)
                df_g.to_excel(writer, sheet_name="Graphs", index=False, startrow=1)
                wsg = writer.sheets["Graphs"]
                wsg.cell(row=1, column=1, value="Generated Graphs Index").font = TITLE_FONT
                _style_header_row(wsg, 2, len(df_g.columns))
                for r in range(3, len(df_g) + 3):
                    for c in range(1, len(df_g.columns) + 1):
                        _style_data_cell(wsg, r, c)
                _auto_width(wsg)

    size_kb = Path(final_path).stat().st_size / 1024
    logger.info(f"[Export] SUCCESS — {final_path} ({size_kb:.1f} KB, {len(entries)} iterations)")
    return final_path


# ─── Per-Iteration Databook Generator ─────────────────────────────────────────

DATABOOK_SYSTEM_PROMPT = """You are an expert Excel data engineer producing a professional financial
due diligence databook. You will receive:

1. The DATA SHEET SCHEMA — column names, their Excel column letters (A, B, C…),
   data types, and sample values. The raw data occupies sheet "Data" rows 2..N+1
   (row 1 = headers). Total data rows = N.
2. ONE analysis iteration — its type, hypothesis, the Python code that was run,
   and the printed output with the results/tables.

Your job: write Python code using openpyxl that OPENS an existing workbook
(already containing the "Data" sheet) and ADDS one or more analysis sheets.

CRITICAL RULES:
A) FORMULAS, NOT VALUES — Every number must come from an Excel formula referencing the Data sheet.
B) REFERENCE FORMAT — Use Data!$C$2:$C${n+1} with anchored column references.
C) STRUCTURE — Your code receives: wb, n, col_map. Create sheets, write headers + formulas.
D) PROFESSIONAL STYLING — Dark green headers, zebra striping, thin borders, number formats.
E) SHEET NAMING — Max 31 chars, descriptive names.
F) RETURN FORMAT — JSON: {"sheets": [...], "code": "def build_analysis(wb, n, col_map): ..."}
G) KEEP IT FOCUSED — 1-3 sheets per iteration."""


def _get_data_schema(data_file: str) -> tuple:
    df = pd.read_excel(data_file)
    col_map = {}
    schema_lines = []
    for i, col in enumerate(df.columns):
        letter = get_column_letter(i + 1)
        col_map[col] = letter
        dtype = str(df[col].dtype)
        sample = str(df[col].iloc[0]) if len(df) > 0 else ""
        sample2 = str(df[col].iloc[min(1, len(df) - 1)]) if len(df) > 1 else ""
        schema_lines.append(f"  Column {letter}: \"{col}\" ({dtype}) — e.g. {sample}, {sample2}")
    schema_text = (
        f"Total rows: {len(df)} (data in rows 2..{len(df)+1}, headers in row 1)\n"
        f"Total columns: {len(df.columns)}\n" + "\n".join(schema_lines)
    )
    return df, col_map, schema_text


def _write_data_sheet(wb, df: pd.DataFrame) -> None:
    ws = wb.active
    ws.title = "Data"
    for col_idx, col_name in enumerate(df.columns, 1):
        ws.cell(row=1, column=col_idx, value=col_name)
    for row_idx, row in enumerate(df.itertuples(index=False), 2):
        for col_idx, value in enumerate(row, 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            if pd.isna(value):
                cell.value = None
            else:
                cell.value = value
    _style_data_sheet(ws, len(df), len(df.columns))


def _build_iteration_context(entry: dict) -> str:
    eval_fields = _parse_evaluation_fields(entry.get("evaluation", ""))
    findings = eval_fields.get("key_findings", [])
    findings_str = "\n".join(f"  - {f}" for f in findings) if findings else "(none)"
    return f"""## Analysis Iteration {entry.get('iteration', '?')}

Type: {entry.get('analysis_type', 'unknown')}
Hypothesis: {entry.get('hypothesis', '')}
Columns Used: {entry.get('columns_used', '')}
Status: {entry.get('status', '')}
Quality: {eval_fields.get('quality', '')}

### Key Findings
{findings_str}

### Summary
{eval_fields.get('summary', '')}

### Original Python Code
```python
{entry.get('code', '')}
```

### Printed Output
{entry.get('output', '')}
"""


def generate_single_databook(
    client, model: str, entry: dict, data_file: str,
    output_dir: str, max_tokens: int = 16000,
) -> str:
    iter_num = int(entry.get("iteration", 0))
    analysis_type = entry.get("analysis_type", "unknown")
    safe_type = re.sub(r"[^\w\s-]", "", analysis_type).strip().replace(" ", "_")[:30]
    output_filename = f"databook_iter{iter_num:02d}_{safe_type}.xlsx"
    output_path = os.path.join(output_dir, output_filename)

    logger.info(f"\n{'─' * 50}")
    logger.info(f"[Databook] Iteration {iter_num}: {analysis_type}")

    try:
        df, col_map, schema_text = _get_data_schema(data_file)
    except Exception as e:
        logger.error(f"[Databook] Failed to read data file: {e}")
        return ""

    n = len(df)

    from openpyxl import Workbook
    wb = Workbook()
    _write_data_sheet(wb, df)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
    wb.close()
    logger.info(f"  [Databook] Data sheet written ({n} rows, {len(df.columns)} cols)")

    iteration_context = _build_iteration_context(entry)
    col_map_str = json.dumps(col_map, indent=2)

    user_msg = f"""## Data Sheet Schema
{schema_text}

## Column Map (column_name → Excel letter)
{col_map_str}

## Variable n = {n}  (total data rows; data in Data!A2:A{n+1})

{iteration_context}

Write a `build_analysis(wb, n, col_map)` function that adds analysis sheets.
Use Excel formulas referencing the Data sheet for ALL numbers."""

    response = call_llm(client, DATABOOK_SYSTEM_PROMPT, user_msg, model, max_tokens, tag=f"Databook-Iter{iter_num}")
    parsed = parse_json_response(response)
    if parsed is None:
        logger.error(f"  [Databook] Failed to parse LLM response for iteration {iter_num}")
        return output_path

    code = parsed.get("code", "")
    if not code:
        logger.warning(f"  [Databook] LLM returned no code for iteration {iter_num}")
        return output_path

    logger.info(f"  [Databook] Planned sheets: {parsed.get('sheets', [])}")

    wrapper_script = f'''import json
import sys
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment, numbers

# LLM-generated code
{code}

# Execute
wb = load_workbook("{output_path}")
col_map = json.loads("""{json.dumps(col_map)}""")
n = {n}

try:
    build_analysis(wb, n, col_map)
    wb.save("{output_path}")
    print("SUCCESS: Saved to {output_path}")
    print(f"Sheets: {{[s.title for s in wb.worksheets]}}")
except Exception as e:
    print(f"ERROR: {{e}}", file=sys.stderr)
    try:
        wb.save("{output_path}")
    except:
        pass
    sys.exit(1)
finally:
    wb.close()
'''

    script_path = "workspace/databook_script.py"
    write_file(script_path, wrapper_script)

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    for attempt in range(1, 3):
        try:
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True, text=True, encoding="utf-8",
                errors="replace", env=env, timeout=180,
            )
        except subprocess.TimeoutExpired:
            logger.error(f"  [Databook] Timed out for iteration {iter_num}")
            return output_path

        if result.stdout:
            logger.info(f"  [Databook] {result.stdout.strip()}")
        if not result.stderr:
            break

        if attempt >= 2:
            logger.error(f"  [Databook] Failed after retry:\n{result.stderr[:500]}")
            return output_path

        logger.warning(f"  [Databook] Error (attempt {attempt}):\n{result.stderr[:400]}")
        fix_msg = f"""The build_analysis code produced an error. Fix it.

## Error
{result.stderr}

## Original Code
```python
{code}
```

## Data Sheet Schema
{schema_text}

## Column Map
{col_map_str}

## n = {n}

Return ONLY a JSON object: {{"sheets": [...], "code": "fixed code"}}"""

        fix_response = call_llm(client, DATABOOK_SYSTEM_PROMPT, fix_msg, model, max_tokens, tag=f"DatabookFix-Iter{iter_num}")
        fix_parsed = parse_json_response(fix_response)
        if fix_parsed and fix_parsed.get("code"):
            code = fix_parsed["code"]
            wrapper_script = f'''import json
import sys
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment, numbers

# LLM-generated code (fixed)
{code}

# Execute
wb = load_workbook("{output_path}")
col_map = json.loads("""{json.dumps(col_map)}""")
n = {n}

try:
    build_analysis(wb, n, col_map)
    wb.save("{output_path}")
    print("SUCCESS: Saved to {output_path}")
    print(f"Sheets: {{[s.title for s in wb.worksheets]}}")
except Exception as e:
    print(f"ERROR: {{e}}", file=sys.stderr)
    try:
        wb.save("{output_path}")
    except:
        pass
    sys.exit(1)
finally:
    wb.close()
'''
            write_file(script_path, wrapper_script)
        else:
            logger.error("  [Databook] Failed to parse fix response")
            return output_path

    if Path(output_path).exists():
        size_kb = Path(output_path).stat().st_size / 1024
        try:
            check_wb = load_workbook(output_path, read_only=True)
            sheet_names = [s.title for s in check_wb.worksheets]
            check_wb.close()
            logger.info(f"  [Databook] DONE — {output_path} ({size_kb:.1f} KB) Sheets: {sheet_names}")
        except Exception:
            logger.info(f"  [Databook] DONE — {output_path} ({size_kb:.1f} KB)")
        return output_path
    return ""


def generate_databooks(
    client, model: str, archive_path: str, data_file: str,
    output_dir: str, max_tokens: int = 16000,
    only_iteration: Optional[int] = None,
) -> list:
    if not Path(archive_path).exists():
        logger.error("[Databook] No archive found. Run data analysis first.")
        return []

    archive_text = read_file(archive_path)
    entries = parse_archive_entries(archive_text)
    success_entries = [e for e in entries if e.get("status", "").lower() == "success"]

    if not success_entries:
        logger.error("[Databook] No successful analyses in archive.")
        return []

    if only_iteration is not None:
        success_entries = [e for e in success_entries if int(e.get("iteration", 0)) == only_iteration]
        if not success_entries:
            logger.error(f"[Databook] No successful iteration #{only_iteration} found.")
            return []

    logger.info(f"\n{'═' * 60}")
    logger.info(f"DDAnalyze DATABOOK GENERATOR — {len(success_entries)} iteration(s)")
    logger.info(f"Output dir: {output_dir}")
    logger.info(f"{'═' * 60}")

    created = []
    for i, entry in enumerate(success_entries, 1):
        iter_num = entry.get("iteration", "?")
        logger.info(f"\n[Databook] Processing {i}/{len(success_entries)} (Iteration {iter_num})...")
        result = generate_single_databook(client, model, entry, data_file, output_dir, max_tokens)
        if result:
            created.append(result)

    logger.info(f"\n{'═' * 60}")
    logger.info(f"[Databook] COMPLETE — {len(created)}/{len(success_entries)} databooks created")
    for p in created:
        size_kb = Path(p).stat().st_size / 1024
        logger.info(f"  {Path(p).name} ({size_kb:.1f} KB)")
    logger.info(f"{'═' * 60}")
    return created


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="DDAnalyze Excel Export & Databook Generator")
    parser.add_argument("--databook", action="store_true")
    parser.add_argument("--output", "-o", type=str, default=None)
    parser.add_argument("-n", "--iteration", type=int, default=None)
    args = parser.parse_args()

    config = load_config()
    debug_logging = config.get("debug_logging", False)
    setup_logging("excel_export", debug=debug_logging)

    archive_path = config.get("archive_file", "full_archive.txt")
    context_path = config.get("active_context_file", "active_context.md")
    data_file = config.get("data_file", "workspace/data.xlsx")
    graphs_folder = config.get("graphs_folder", "workspace/graphs")

    if not Path(archive_path).exists():
        logger.error(f"Archive not found: {archive_path}")
        sys.exit(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.databook:
        if not Path(data_file).exists():
            logger.error(f"Data file not found: {data_file}")
            sys.exit(1)

        model = config.get("model", "claude-sonnet-4-6")
        max_tokens = config.get("databook_max_tokens", 16000)
        output_dir = args.output or f"workspace/exports/databooks_{timestamp}"
        client = create_client()
        results = generate_databooks(
            client, model, archive_path, data_file, output_dir, max_tokens,
            only_iteration=args.iteration,
        )
        if results:
            print(f"\n{len(results)} databook(s) created in {output_dir}/")
            for p in results:
                print(f"  {Path(p).name}")
        else:
            print("\nNo databooks created. Check the log for details.")
            sys.exit(1)
    else:
        output_path = args.output or f"workspace/exports/iterations_export_{timestamp}.xlsx"
        result = export_iterations_to_excel(archive_path, context_path, output_path, graphs_folder)
        if result:
            print(f"\nExport created: {result}")
        else:
            print("\nExport failed. Check the log for details.")
            sys.exit(1)


if __name__ == "__main__":
    main()
