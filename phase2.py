"""
Phase 2 — Final Report Generator
 
Reads full_archive.txt and active_context.md, selects the most valuable findings,
and uses an LLM to produce a structured final_report.md written for a business audience.
 
Run this manually after loop.py finishes.
"""
 
import os
import re
import sys
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
 
PHASE2_SYSTEM_PROMPT = """You are a senior data analyst preparing a final business report. You have access to a \
complete log of autonomous data analyses performed on a company dataset.
 
Your job:
1. Select the most valuable findings — those that best explain the business, its customers,
   its revenue dynamics, and its key patterns.
2. For each selected finding, write:
   - A clear business-readable title
   - Whenever posible: Readers are very acustomed to consume insights in year-by-year tables (Fiscal year if given, else use natural years). 
        When te last year is not complete, use (LTM) Last-twelf-months to create a comparable 12-month window. Add CAGR calculation when applicable.
   - A 3-5 sentence explanation of what was found and why it matters
   - The Python code that produced it (copied from the archive, cleaned if needed)
3. Organize findings into logical sections (e.g. Customer Analysis, Revenue Trends,
   Concentration, Seasonality).
4. End with a one-page executive summary: the 5-7 most important things to know about
   this business from the data.
 
Write for a business audience, not a technical one. Avoid statistical jargon.
Output as clean markdown."""
 
# ─── File Utilities ───────────────────────────────────────────────────────────
 
def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
 
def write_file(path: str, content: str) -> None:
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
 
# ─── LLM Payload Builder ──────────────────────────────────────────────────────
 
def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]"
 
def build_report_prompt(entries: list, context_text: str) -> str:
    parts = []
    for e in entries:
        code_block = f"```python\n{_truncate(e.get('code', ''), 1200)}\n```"
        part = f"""--- ANALYSIS {e.get('iteration', '?')} ---
Type       : {e.get('analysis_type', '')}
Hypothesis : {e.get('hypothesis', '')}
Columns    : {e.get('columns_used', '')}
 
Output:
{_truncate(e.get('output', ''), 2000)}
 
Evaluation:
{_truncate(e.get('evaluation', ''), 600)}
 
Code:
{code_block}
"""
        parts.append(part)
 
    analyses_text = "\n\n".join(parts)
 
    return f"""## Final State of the Knowledge Base
 
{context_text}
 
---
 
## Complete Log of Successful Analyses ({len(entries)} total)
 
{analyses_text}
 
---
 
Please generate a comprehensive final business report based on all of the above.
Select the most valuable analyses, group them into logical sections, and conclude \
with a concise executive summary."""
 
# ─── Main ─────────────────────────────────────────────────────────────────────
 
def main() -> None:
    # Load config
    config = yaml.safe_load(read_file("config.yaml"))
    model = config.get("model", "claude-sonnet-4-6")
    archive_path = config.get("archive_file", "full_archive.txt")
    context_path = config.get("active_context_file", "active_context.md")
    output_path = "final_report.md"
 
    print("Phase 2 — Final Report Generator")
    print(f"Archive : {archive_path}")
    print(f"Context : {context_path}")
    print(f"Model   : {model}")
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
 
    # Build user message
    user_message = build_report_prompt(entries, context_text)
 
    # Stream the report
    client = anthropic.Anthropic(api_key=API_KEY)
    print(f"Generating final_report.md (streaming)...\n")
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
 
    # Extract full text
    for block in response.content:
        if block.type == "text":
            report_text = block.text
            break
 
    # Save
    write_file(output_path, report_text)
    usage = response.usage
    print(f"Report saved to: {output_path}")
    print(f"Tokens — input: {usage.input_tokens:,}  output: {usage.output_tokens:,}")
 
if __name__ == "__main__":
    main()