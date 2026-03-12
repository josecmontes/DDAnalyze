"""
Phase 2 — Final Report Generator

Three-step process:
  Step A: Extract all successful analysis code → extracted_code.md
  Step B: LLM synthesises findings into a structured markdown report → final_report.md
  Step C: Convert the markdown report to a Word document → final_report.docx
          (embeds tables as Word tables and inserts graph images)

Run this manually after loop.py finishes.
"""

import logging
import os
import re
import sys
import time
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

# ─── Logging Setup ────────────────────────────────────────────────────────────

logger = logging.getLogger("ddanalyze.phase2")

def setup_logging(debug: bool = False, log_dir: str = "logs") -> Path:
    """Configure console + file logging. Returns the path of the log file."""
    Path(log_dir).mkdir(exist_ok=True)
    log_file = Path(log_dir) / f"phase2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    console_level = logging.DEBUG if debug else logging.INFO

    root = logging.getLogger("ddanalyze")
    root.setLevel(logging.DEBUG)

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
    all_blocks = 0
    skipped_internal = 0
    skipped_non_success = 0

    for block in archive_text.split(separator):
        block = block.strip()
        if not block or "ITERATION:" not in block:
            continue

        all_blocks += 1

        # Skip internal error blocks
        status_match = re.search(r"\nSTATUS:\s*(\S+)", block)
        if not status_match:
            continue
        status = status_match.group(1).strip()
        if status.upper() in _INTERNAL_ERROR_LABELS:
            skipped_internal += 1
            continue
        if status.lower() != "success":
            skipped_non_success += 1
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

    logger.debug(
        f"[Archive] Parsed {all_blocks} blocks: "
        f"{len(entries)} success, {skipped_non_success} failure/unknown, "
        f"{skipped_internal} internal errors"
    )
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
    logger.info(f"  Extracted code saved to: {output_path} ({len(entries)} analyses)")

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

def _set_cell_shading(cell, hex_color: str) -> None:
    """Apply a solid background fill to a table cell using direct XML manipulation."""
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color.lstrip("#"))
    tcPr.append(shd)


def _add_markdown_table_to_doc(doc, header_line: str, data_lines: list) -> None:
    """Convert parsed markdown table lines into a python-docx Word table.

    Header row: Deloitte dark-green (#046A38) background, white bold Arial text.
    Body rows: Arial text, normal weight.
    """
    from docx.shared import Pt, RGBColor

    # Deloitte table header colours
    HEADER_BG  = "046A38"   # dark green (no leading #)
    HEADER_FG  = RGBColor(0xFF, 0xFF, 0xFF)   # white
    BODY_FONT  = "Arial"

    header_cells = _parse_table_row(header_line)
    if not header_cells:
        return

    n_cols = len(header_cells)
    data_rows = []
    for line in data_lines:
        row = _parse_table_row(line)
        if row:
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
        pass

    for row_idx, row_data in enumerate(all_rows):
        for col_idx, cell_text in enumerate(row_data):
            if col_idx >= n_cols:
                continue
            cell = table.rows[row_idx].cells[col_idx]
            cell.text = ""  # clear before adding a styled run

            para = cell.paragraphs[0]
            run = para.add_run(cell_text)
            run.font.name = BODY_FONT
            run.font.size = Pt(10)

            if row_idx == 0:
                # Header: dark-green background, white bold text
                _set_cell_shading(cell, HEADER_BG)
                run.bold = True
                run.font.color.rgb = HEADER_FG

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

def _add_deloitte_heading(doc, text: str, level: int, title_rgb, font: str) -> None:
    """Add a Word heading styled with the Deloitte green color and Arial font."""
    para = doc.add_heading(text, level=level)
    for run in para.runs:
        run.font.color.rgb = title_rgb
        run.font.name = font


def build_word_document(report_md: str, graphs_folder: str, output_path: str) -> None:
    """
    Convert a markdown report to a Word .docx file.

    Handles:
      - # / ## / ### headings → Word heading styles (Deloitte green, Arial)
      - | markdown tables | → Word tables (dark-green header, white font, Arial)
      - [GRAPH: filename.png] references → embedded PNG images
      - ![alt](path) markdown images → embedded images
      - **bold** text in paragraphs
      - Regular paragraphs
    """
    try:
        from docx import Document
        from docx.shared import Inches, Pt
    except ImportError:
        logger.warning(
            "\n[WARNING] python-docx is not installed. Cannot create Word document.\n"
            "Install it with:  pip install python-docx\n"
            "The markdown report (final_report.md) was saved and is complete.\n"
        )
        return

    t0 = time.time()
    doc = Document()
    graphs_embedded = 0
    graphs_missing = 0
    tables_added = 0

    # Set page margins
    for section in doc.sections:
        section.left_margin = Inches(1.0)
        section.right_margin = Inches(1.0)
        section.top_margin = Inches(1.0)
        section.bottom_margin = Inches(1.0)

    # ── Deloitte theme: set document-level default font to Arial ─────────────
    from docx.shared import RGBColor
    _DELOITTE_TITLE_RGB = RGBColor(0x26, 0x89, 0x0D)   # #26890D green medium
    _ARIAL = "Arial"
    try:
        normal_style = doc.styles["Normal"]
        normal_style.font.name = _ARIAL
        normal_style.font.size = Pt(10)
    except Exception:
        pass

    lines = report_md.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # ── Headers ──────────────────────────────────────────────────────────
        if stripped.startswith("### "):
            _add_deloitte_heading(doc, stripped[4:], level=3,
                                  title_rgb=_DELOITTE_TITLE_RGB, font=_ARIAL)
        elif stripped.startswith("## "):
            _add_deloitte_heading(doc, stripped[3:], level=2,
                                  title_rgb=_DELOITTE_TITLE_RGB, font=_ARIAL)
        elif stripped.startswith("# "):
            _add_deloitte_heading(doc, stripped[2:], level=1,
                                  title_rgb=_DELOITTE_TITLE_RGB, font=_ARIAL)

        # ── Markdown tables ───────────────────────────────────────────────────
        elif stripped.startswith("|") and i + 1 < len(lines) and _is_separator_row(lines[i + 1]):
            header_line = stripped
            i += 2  # Skip header + separator rows
            data_lines = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                data_lines.append(lines[i].strip())
                i += 1
            _add_markdown_table_to_doc(doc, header_line, data_lines)
            tables_added += 1
            logger.debug(f"[DocBuilder] Added table with {len(data_lines)} data rows")
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
                        graphs_embedded += 1
                        logger.debug(f"[DocBuilder] Embedded graph: {filename}")
                    except Exception as e:
                        doc.add_paragraph(f"[Could not embed graph {filename}: {e}]")
                        logger.warning(f"[DocBuilder] Failed to embed graph {filename}: {e}")
                else:
                    doc.add_paragraph(f"[Graph not found: {filename}]")
                    graphs_missing += 1
                    logger.warning(f"[DocBuilder] Graph not found: {img_path}")
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
                        graphs_embedded += 1
                    except Exception as e:
                        doc.add_paragraph(f"[Could not embed image: {e}]")
                        logger.warning(f"[DocBuilder] Failed to embed image {img_path}: {e}")
                else:
                    doc.add_paragraph(f"[Image not found: {img_path}]")
                    graphs_missing += 1
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
    elapsed = time.time() - t0
    logger.info(f"  Word document saved to: {output_path}")
    logger.info(
        f"  DocBuilder stats: {tables_added} tables, "
        f"{graphs_embedded} graphs embedded, {graphs_missing} missing"
        f" | built in {elapsed:.1f}s"
    )

# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    # Load config
    config = yaml.safe_load(read_file("config.yaml"))
    model = config.get("model", "claude-sonnet-4-6")
    archive_path = config.get("archive_file", "full_archive.txt")
    context_path = config.get("active_context_file", "active_context.md")
    graphs_folder = config.get("graphs_folder", "workspace/graphs")
    debug_logging = config.get("debug_logging", False)

    extracted_code_path = "extracted_code.md"
    report_md_path = "final_report.md"
    report_docx_path = "final_report.docx"

    # ── Setup logging ─────────────────────────────────────────────────────────
    log_file = setup_logging(debug=debug_logging)

    logger.info("=" * 60)
    logger.info("Phase 2 — Final Report Generator")
    logger.info(f"Archive : {archive_path}")
    logger.info(f"Context : {context_path}")
    logger.info(f"Graphs  : {graphs_folder}")
    logger.info(f"Model   : {model}")
    logger.info(f"Log     : {log_file}")
    logger.info("=" * 60)
    logger.info("")

    # Validate inputs
    if not Path(archive_path).exists():
        logger.error(f"ERROR: {archive_path} not found. Run loop.py first.")
        sys.exit(1)

    archive_text = read_file(archive_path)
    context_text = read_file(context_path) if Path(context_path).exists() else ""
    logger.debug(
        f"[Input] archive={len(archive_text):,}ch  context={len(context_text):,}ch"
    )

    # Parse successful entries
    t_parse = time.time()
    entries = parse_archive(archive_text)
    logger.info(f"Found {len(entries)} successful analyses in archive (parsed in {time.time()-t_parse:.2f}s).")

    if not entries:
        logger.error("No successful analyses to report on. Exiting.")
        sys.exit(0)

    # ── Step A: Extract code ──────────────────────────────────────────────────
    logger.info(f"\n[Step A] Extracting analysis code to {extracted_code_path}...")
    t_a = time.time()
    extract_code_to_file(entries, extracted_code_path)
    logger.info(f"[Step A] Done in {time.time()-t_a:.1f}s")

    # ── Step B: Generate markdown report (LLM) ────────────────────────────────
    available_graphs = list_available_graphs(graphs_folder)
    logger.info(f"\n[Step B] Generating markdown report (streaming)...")
    logger.info(f"         Analyses: {len(entries)}  |  Available graphs: {len(available_graphs)}")
    if available_graphs:
        for _, fname in available_graphs:
            logger.info(f"           - {fname}")
    logger.info("")

    user_message = build_report_prompt(entries, context_text, available_graphs)
    logger.debug(
        f"[Step B] Prompt size: {len(user_message):,}ch "
        f"(system={len(PHASE2_SYSTEM_PROMPT):,}ch  user={len(user_message):,}ch)"
    )

    client = anthropic.Anthropic(api_key=API_KEY)
    logger.info("-" * 60)

    t_b = time.time()
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

    b_elapsed = time.time() - t_b
    usage = response.usage
    write_file(report_md_path, report_text)
    logger.info(f"Markdown report saved to: {report_md_path} ({len(report_text):,}ch)")
    logger.info(
        f"[Step B] Done in {b_elapsed:.1f}s | "
        f"tokens in={usage.input_tokens:,}  out={usage.output_tokens:,}  "
        f"total={usage.input_tokens + usage.output_tokens:,}"
    )

    # ── Step C: Build Word document ───────────────────────────────────────────
    logger.info(f"\n[Step C] Building Word document ({report_docx_path})...")
    t_c = time.time()
    build_word_document(report_text, graphs_folder, report_docx_path)
    logger.info(f"[Step C] Done in {time.time()-t_c:.1f}s")

    logger.info("")
    logger.info("=" * 60)
    logger.info("Phase 2 complete.")
    logger.info(f"  {extracted_code_path}  — all successful analysis code")
    logger.info(f"  {report_md_path}       — full markdown report")
    logger.info(f"  {report_docx_path}     — Word document with tables and graphs")
    logger.info(f"  {log_file}  — full debug log")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
