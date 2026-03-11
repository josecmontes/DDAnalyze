"""
Phase 2 — Final Report Generator

Three-step process:
  Step A: Extract all successful analysis code → extracted_code.md
  Step B: LLM synthesises findings into a structured markdown report → final_report.md
  Step C: Convert the markdown report to a Word document → final_report.docx
          (embeds tables as Word tables and inserts graph images)

Run this manually after loop.py finishes.
"""

import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import anthropic
import yaml

# Change to the directory containing this script so relative paths work
os.chdir(Path(__file__).parent)

from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('API_KEY')

# ─── System Prompt ────────────────────────────────────────────────────────────

PHASE2_SYSTEM_PROMPT = """You are a senior data analyst preparing a final BUSINESS report for executives. \
You have access to a complete log of autonomous data analyses performed on a company dataset.

IMPORTANT: This report will become a Word document for business readers. Do NOT include Python code.
Code has already been extracted separately. Focus entirely on findings, tables, and business narrative.

Your job:
1. Select the most valuable findings — those that best explain the business, its customers,
   its revenue dynamics, and its key patterns.

2. For each selected finding, write:
   - A clear, business-readable title (no jargon)
   - MANDATORY: A year-by-year comparison table whenever the finding spans multiple periods.
     Always show 3–4 years + LTM (Last-Twelve-Months) if the latest year is incomplete.
     Always include a CAGR row when you have 3+ years of data.
     Format tables as proper markdown tables:
       | Year   | Revenue  | Customers | YoY %  |
       |--------|----------|-----------|--------|
       | FY2021 | $X       | X         | —      |
       | FY2022 | $X       | X         | +X%    |
       | FY2023 | $X       | X         | +X%    |
       | LTM    | $X       | X         | +X%    |
       | CAGR   |          |           | XX%    |
   - A 3–5 sentence explanation of what was found and why it matters for the business.
   - If a graph was generated for this analysis, reference it with EXACTLY this syntax:
       [GRAPH: filename.png]
     (use the filename from the Available Graphs list)

3. Organize findings into logical sections (e.g. Revenue Trends, Customer Analysis,
   Concentration & Risk, Seasonality & Timing, Geographic / Segment Breakdown).

4. End with a one-page Executive Summary: the 5–7 most important things to know about
   this business from the data. Use clear bullet points.

Write for a business audience. Avoid statistical jargon.
Output as clean markdown. Do NOT include Python code blocks."""

# ─── File Utilities ───────────────────────────────────────────────────────────

def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path: str, content: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

# ─── Archive Parser ───────────────────────────────────────────────────────────

_INTERNAL_ERROR_LABELS = {
    "JSON_PARSE_ERROR", "CRITIC_JSON_PARSE_ERROR", "TIMEOUT", "FATAL_ERROR",
}

def parse_archive(archive_text: str) -> list:
    """
    Parse full_archive.txt into a list of dicts for all SUCCESS entries.
    Each entry has: iteration, date, status, analysis_type, hypothesis,
                    columns_used, code, output, evaluation.
    """
    separator = "=" * 80
    entries = []

    for block in archive_text.split(separator):
        block = block.strip()
        if not block or "ITERATION:" not in block:
            continue

        # Skip internal error blocks
        status_match = re.search(r"\nSTATUS:\s*(\S+)", block)
        if not status_match:
            continue
        status = status_match.group(1).strip()
        if status.upper() in _INTERNAL_ERROR_LABELS:
            continue
        if status.lower() != "success":
            continue

        entry: dict = {"status": status}

        # Simple line-level fields
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

        # Code block (between "CODE:\n" and the next "---" separator)
        dash_sep = "-" * 80
        code_m = re.search(
            r"CODE:\n(.*?)\n" + re.escape(dash_sep), block, re.DOTALL
        )
        if code_m:
            entry["code"] = code_m.group(1).strip()

        # Output block (between "OUTPUT:\n" and next "---" separator or end)
        output_m = re.search(
            r"OUTPUT:\n(.*?)(?:\n" + re.escape(dash_sep) + r"|\Z)", block, re.DOTALL
        )
        if output_m:
            entry["output"] = output_m.group(1).strip()

        # Evaluation block (from "EVALUATION:\n" to end)
        eval_m = re.search(r"EVALUATION:\n(.*?)(?:\Z)", block, re.DOTALL)
        if eval_m:
            entry["evaluation"] = eval_m.group(1).strip()

        if "iteration" in entry:
            entries.append(entry)

    return entries

# ─── Step A: Code Extraction ──────────────────────────────────────────────────

def extract_code_to_file(entries: list, output_path: str) -> None:
    """
    Generate extracted_code.md with all successful analysis code blocks,
    organised by analysis type and iteration number.
    """
    lines = [
        "# Extracted Analysis Code\n",
        f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n",
        f"*Total successful analyses: {len(entries)}*\n",
        "\n---\n",
    ]

    for e in entries:
        iter_num = e.get("iteration", "?")
        atype = e.get("analysis_type", "Unknown")
        hypothesis = e.get("hypothesis", "")
        columns = e.get("columns_used", "")
        code = e.get("code", "")
        output_preview = e.get("output", "")[:600]

        lines.append(f"\n## Analysis {iter_num}: {atype}\n")
        if hypothesis:
            lines.append(f"**Hypothesis:** {hypothesis}\n\n")
        if columns:
            lines.append(f"**Columns:** {columns}\n\n")
        lines.append(f"```python\n{code}\n```\n")
        if output_preview:
            lines.append(f"\n**Output (preview):**\n```\n{output_preview}\n```\n")
        lines.append("\n---\n")

    write_file(output_path, "".join(lines))
    print(f"  Extracted code saved to: {output_path}")

# ─── Step B: LLM Report Generation ───────────────────────────────────────────

def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]"

def list_available_graphs(graphs_folder: str) -> list:
    """Return list of (filepath, filename) for all PNG files in the graphs folder."""
    folder = Path(graphs_folder)
    if not folder.exists():
        return []
    return [(str(p), p.name) for p in sorted(folder.glob("*.png"))]

def build_report_prompt(entries: list, context_text: str, available_graphs: list) -> str:
    parts = []
    for e in entries:
        part = f"""--- ANALYSIS {e.get('iteration', '?')} ---
Type       : {e.get('analysis_type', '')}
Hypothesis : {e.get('hypothesis', '')}
Columns    : {e.get('columns_used', '')}

Output:
{_truncate(e.get('output', ''), 2000)}

Evaluation:
{_truncate(e.get('evaluation', ''), 800)}
"""
        parts.append(part)

    analyses_text = "\n\n".join(parts)

    graphs_section = ""
    if available_graphs:
        graphs_section = "\n\n## Available Graphs\n"
        graphs_section += "The following graph files were saved during the analysis loop.\n"
        graphs_section += "Reference them in the report using [GRAPH: filename.png] syntax.\n\n"
        for filepath, filename in available_graphs:
            graphs_section += f"- {filename}\n"

    return f"""## Final State of the Knowledge Base

{context_text}
{graphs_section}
---

## Complete Log of Successful Analyses ({len(entries)} total)

{analyses_text}

---

Please generate a comprehensive final business report based on all of the above.
Select the most valuable analyses, group them into logical sections, include year-by-year
tables wherever relevant, embed graph references using [GRAPH: filename.png] syntax,
and conclude with a concise executive summary. Do NOT include Python code."""

# ─── Step C: Word Document Builder ───────────────────────────────────────────

def _parse_table_row(line: str) -> list:
    """Parse a markdown table row string into a list of cell strings."""
    cells = [c.strip() for c in line.split("|")]
    return [c for c in cells if c]  # Remove empty strings from leading/trailing |

def _is_separator_row(line: str) -> bool:
    """Return True if the line is a markdown table separator (|---|---|)."""
    return bool(re.match(r"^\|[\s\-:|]+\|$", line.strip()))

def _add_markdown_table_to_doc(doc, header_line: str, data_lines: list) -> None:
    """Convert parsed markdown table lines into a python-docx Word table."""
    from docx.shared import Pt

    header_cells = _parse_table_row(header_line)
    if not header_cells:
        return

    n_cols = len(header_cells)
    data_rows = []
    for line in data_lines:
        row = _parse_table_row(line)
        if row:
            # Pad or trim to match column count
            while len(row) < n_cols:
                row.append("")
            data_rows.append(row[:n_cols])

    all_rows = [header_cells] + data_rows
    if not all_rows:
        return

    table = doc.add_table(rows=len(all_rows), cols=n_cols)
    try:
        table.style = "Table Grid"
    except Exception:
        pass  # style may not exist in default template

    for row_idx, row_data in enumerate(all_rows):
        for col_idx, cell_text in enumerate(row_data):
            if col_idx < n_cols:
                cell = table.rows[row_idx].cells[col_idx]
                cell.text = cell_text
                if row_idx == 0:  # Bold the header row
                    for para in cell.paragraphs:
                        for run in para.runs:
                            run.bold = True
                        para.runs[0].font.size = Pt(10) if para.runs else None

    # Add a blank paragraph after the table for spacing
    doc.add_paragraph()

def _add_formatted_paragraph(doc, text: str):
    """Add a paragraph with basic **bold** support."""
    if "**" not in text:
        doc.add_paragraph(text)
        return

    para = doc.add_paragraph()
    parts = re.split(r"(\*\*[^*]+\*\*)", text)
    for part in parts:
        if part.startswith("**") and part.endswith("**"):
            run = para.add_run(part[2:-2])
            run.bold = True
        else:
            para.add_run(part)

def build_word_document(report_md: str, graphs_folder: str, output_path: str) -> None:
    """
    Convert a markdown report to a Word .docx file.

    Handles:
      - # / ## / ### headings → Word heading styles
      - | markdown tables | → Word tables (Table Grid style)
      - [GRAPH: filename.png] references → embedded PNG images
      - ![alt](path) markdown images → embedded images
      - **bold** text in paragraphs
      - Regular paragraphs
    """
    try:
        from docx import Document
        from docx.shared import Inches, Pt
    except ImportError:
        print(
            "\n[WARNING] python-docx is not installed. Cannot create Word document.\n"
            "Install it with:  pip install python-docx\n"
            "The markdown report (final_report.md) was saved and is complete.\n"
        )
        return

    doc = Document()

    # Set page margins
    for section in doc.sections:
        section.left_margin = Inches(1.0)
        section.right_margin = Inches(1.0)
        section.top_margin = Inches(1.0)
        section.bottom_margin = Inches(1.0)

    lines = report_md.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # ── Headers ──────────────────────────────────────────────────────────
        if stripped.startswith("### "):
            doc.add_heading(stripped[4:], level=3)
        elif stripped.startswith("## "):
            doc.add_heading(stripped[3:], level=2)
        elif stripped.startswith("# "):
            doc.add_heading(stripped[2:], level=1)

        # ── Markdown tables ───────────────────────────────────────────────────
        elif stripped.startswith("|") and i + 1 < len(lines) and _is_separator_row(lines[i + 1]):
            header_line = stripped
            i += 2  # Skip header + separator rows
            data_lines = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                data_lines.append(lines[i].strip())
                i += 1
            _add_markdown_table_to_doc(doc, header_line, data_lines)
            continue  # already advanced i

        # ── Graph references: [GRAPH: filename.png] ───────────────────────────
        elif "[GRAPH:" in stripped:
            match = re.search(r"\[GRAPH:\s*([\w\-\.]+\.png)\]", stripped, re.IGNORECASE)
            if match:
                filename = match.group(1)
                img_path = Path(graphs_folder) / filename
                if img_path.exists():
                    try:
                        doc.add_picture(str(img_path), width=Inches(6.0))
                        # Caption: any text on the line outside the [GRAPH:...] tag
                        caption = re.sub(r"\[GRAPH:.*?\]", "", stripped, flags=re.IGNORECASE).strip()
                        if caption:
                            p = doc.add_paragraph(caption)
                            for run in p.runs:
                                run.italic = True
                    except Exception as e:
                        doc.add_paragraph(f"[Could not embed graph {filename}: {e}]")
                else:
                    doc.add_paragraph(f"[Graph not found: {filename}]")
            else:
                _add_formatted_paragraph(doc, stripped)

        # ── Standard markdown image: ![alt](path) ────────────────────────────
        elif stripped.startswith("!["):
            match = re.search(r"!\[([^\]]*)\]\(([^\)]+)\)", stripped)
            if match:
                img_path = match.group(2)
                if Path(img_path).exists():
                    try:
                        doc.add_picture(img_path, width=Inches(6.0))
                    except Exception as e:
                        doc.add_paragraph(f"[Could not embed image: {e}]")
                else:
                    doc.add_paragraph(f"[Image not found: {img_path}]")
            else:
                _add_formatted_paragraph(doc, stripped)

        # ── Horizontal rule ───────────────────────────────────────────────────
        elif stripped.startswith("---") and stripped == "-" * len(stripped):
            doc.add_paragraph()  # Just add spacing for HR

        # ── Non-empty text / paragraph ────────────────────────────────────────
        elif stripped:
            _add_formatted_paragraph(doc, stripped)

        # ── Empty line (ignored — paragraph spacing handles visual gaps) ───────
        # else: pass

        i += 1

    doc.save(output_path)
    print(f"  Word document saved to: {output_path}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    # Load config
    config = yaml.safe_load(read_file("config.yaml"))
    model = config.get("model", "claude-sonnet-4-6")
    archive_path = config.get("archive_file", "full_archive.txt")
    context_path = config.get("active_context_file", "active_context.md")
    graphs_folder = config.get("graphs_folder", "workspace/graphs")

    extracted_code_path = "extracted_code.md"
    report_md_path = "final_report.md"
    report_docx_path = "final_report.docx"

    print("=" * 60)
    print("Phase 2 — Final Report Generator")
    print(f"Archive : {archive_path}")
    print(f"Context : {context_path}")
    print(f"Graphs  : {graphs_folder}")
    print(f"Model   : {model}")
    print("=" * 60)
    print()

    # Validate inputs
    if not Path(archive_path).exists():
        print(f"ERROR: {archive_path} not found. Run loop.py first.")
        sys.exit(1)

    archive_text = read_file(archive_path)
    context_text = read_file(context_path) if Path(context_path).exists() else ""

    # Parse successful entries
    entries = parse_archive(archive_text)
    print(f"Found {len(entries)} successful analyses in archive.")

    if not entries:
        print("No successful analyses to report on. Exiting.")
        sys.exit(0)

    # ── Step A: Extract code ──────────────────────────────────────────────────
    print(f"\n[Step A] Extracting analysis code to {extracted_code_path}...")
    extract_code_to_file(entries, extracted_code_path)

    # ── Step B: Generate markdown report (LLM) ────────────────────────────────
    available_graphs = list_available_graphs(graphs_folder)
    print(f"\n[Step B] Generating markdown report (streaming)...")
    print(f"         Available graphs: {len(available_graphs)}")
    if available_graphs:
        for _, fname in available_graphs:
            print(f"           - {fname}")
    print()

    user_message = build_report_prompt(entries, context_text, available_graphs)

    client = anthropic.Anthropic(api_key=API_KEY)
    print("-" * 60)

    report_text = ""
    with client.messages.stream(
        model=model,
        max_tokens=8192,
        system=PHASE2_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    ) as stream:
        for chunk in stream.text_stream:
            print(chunk, end="", flush=True)
        response = stream.get_final_message()

    print(f"\n{'-' * 60}\n")

    for block in response.content:
        if block.type == "text":
            report_text = block.text
            break

    write_file(report_md_path, report_text)
    usage = response.usage
    print(f"Markdown report saved to: {report_md_path}")
    print(f"Tokens — input: {usage.input_tokens:,}  output: {usage.output_tokens:,}")

    # ── Step C: Build Word document ───────────────────────────────────────────
    print(f"\n[Step C] Building Word document ({report_docx_path})...")
    build_word_document(report_text, graphs_folder, report_docx_path)

    print()
    print("=" * 60)
    print("Phase 2 complete.")
    print(f"  {extracted_code_path}  — all successful analysis code")
    print(f"  {report_md_path}       — full markdown report")
    print(f"  {report_docx_path}     — Word document with tables and graphs")
    print("=" * 60)

if __name__ == "__main__":
    main()
