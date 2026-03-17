"""
Phase 2 — Final Report Generator

Two-step process:
  Step A: Extract all successful analysis code → extracted_code.md
  Step B: LLM synthesises findings into a structured markdown report → final_report.md

Run this manually after Analysts.py finishes.
The Deloitte HTML report is generated separately by deloitte_report.py.
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml

os.chdir(Path(__file__).parent)

from utils import (
    read_file, write_file, load_config, setup_logging,
    parse_archive_success, create_client,
)

logger = logging.getLogger("ddanalyze.phase2")

# ─── System Prompt ────────────────────────────────────────────────────────────

PHASE2_SYSTEM_PROMPT = """You are a senior data analyst preparing a final BUSINESS report for executives. \
You have access to a complete log of autonomous data analyses performed on a company dataset.

IMPORTANT: This report will be used for generating an HTML report. Do NOT include Python code.
Code has already been extracted separately. Focus entirely on findings, tables, and business narrative.

Your job:
1. Select the most valuable findings — those that best explain the business, its customers,
   its revenue dynamics, and its key patterns.

2. For each selected finding, write:
   - A clear, business-readable title (no jargon)
   - MANDATORY: A comparison table whenever the finding spans multiple periods.
     You must strictly adhere to the following standard professional financial formatting for all tables:
       1. Table Title: Place the title above the table in **bold**.
       2. Header Row: The top-left cell MUST be the unit of measurement (e.g., `€k`, `#`). Subsequent columns MUST be the chronological time periods (e.g., `Dec 23`, `Dec 24`, `LTM`).
       3. Alignment: Left-align the first column (categories/line items). Right-align all numerical columns.
       4. Number Formatting: Use 2 decimal places for currency/averages (e.g., 5.61), whole numbers for counts (e.g., 135), and comma separators for thousands (e.g., 6,495).
       5. Summary/Total Row: The final row of the main data block must be a total/blended row (or CAGR if 3+ years). **Bold** the entire row (both the label and the numbers).
       6. Sub-tables (if applicable): If showing percentages, append them within the same table beneath a bolded row stating "**As % of Total**". Format the percentage numbers in *italics*.
       7. Footer: Immediately below the Markdown table, add the data source on a new line (e.g., "Source: Company Dataset"). Do not use bold for the footer.

     Example Format:
     **Revenue by Segment**
     | €k | Dec 22 | Dec 23 | LTM |
     |:---|---:|---:|---:|
     | Segment A | 3,670.00 | 5,610.00 | 6,310.00 |
     | Segment B | 4,980.00 | 5,690.00 | 5,980.00 |
     | **Total** | **8,650.00** | **11,300.00** | **12,290.00** |
     | **As % of Total** | | | |
     | Segment A | *42.4%* | *49.6%* | *51.3%* |
     | Segment B | *57.6%* | *50.4%* | *48.7%* |
     Source: Customer Cube

   - A 3–5 sentence explanation of what was found and why it matters for the business.
   - If a graph was generated for this analysis, reference it with EXACTLY this syntax:
       [GRAPH: filename.png]
     (use the filename from the Available Graphs list)

3. Organize findings into logical sections (e.g. Revenue Trends, Customer Analysis,
   Concentration & Risk, Seasonality & Timing, Geographic / Segment Breakdown).

4. Begin with a one-page Executive Summary: the 5–7 most important things to know about
   this business from the data. Use clear bullet points. Each bullet must cross-reference
   the relevant section number and title where the supporting analysis is found, using the
   format "(see Section N: Title)". The Executive Summary must be the FIRST section of the
   report, immediately after the title, so that readers get the key takeaways up front.

5. TONE — Write in a formal, senior consulting register throughout. Avoid conversational,
   didactic, or explanatory phrases. Use professional formulations such as "This pattern is
   consistent with...", "This trend reflects...", "This dynamic is in line with...".

6. TERM INTRODUCTION — When a specific product model name, customer segment label, or
   internal category name is referenced for the first time in a section, provide brief
   contextual identification so that a reader unfamiliar with the company's internal
   naming conventions can follow.

7. CURRENCY FORMAT — MANDATORY:
   All monetary values MUST use the euro symbol prefix with lowercase magnitude suffix.
   Correct format: ~€0.57m, €16.7m, €32.8m, €62.5m, €1.2k, €3.4bn
   WRONG formats (never use these): "EUR 0.57M", "EUR ~0.57M", "EUR 16.7M", "0.57M EUR"

8. BASIS OF PREPARATION — For every major finding or analytical section, the report MUST
   begin with a brief "Basis of Preparation" paragraph that explains HOW the analysis was
   performed BEFORE presenting any results.

9. VERBOSITY AND DEPTH — Be thorough and expansive in your narrative. Each section should
   typically be 400-800 words of prose (excluding tables).

10. GLOSSARY — At the end of the report, include a ## Glossary section that defines every
   abbreviation, acronym, internal term, analytical category, and technical metric used.

Write for a senior business audience. Avoid statistical jargon.
Output as clean markdown. Do NOT include Python code blocks."""

# ─── Iterative Pipeline Prompts ──────────────────────────────────────────────

ARCHITECT_SYSTEM_PROMPT = """\
You are a report architect. You will receive the full knowledge base and analysis log from an \
autonomous data-analysis pipeline. Your ONLY job is to design the structure of the final report.

Output a single valid JSON object — no markdown fences, no commentary, no prose.

The JSON schema:
{
  "report_title": "string — title for the overall report",
  "sections": [
    {
      "section_number": int,
      "title": "string — business-readable section heading",
      "description": "string — 1-2 sentence scope of this section",
      "iterations": [int, ...],
      "graphs": ["filename.png", ...],
      "guidance": "string — writing instructions for the section author."
    }
  ],
  "executive_summary_guidance": "string — key points the exec summary must cover.",
  "glossary_terms": ["list of terms to define in the glossary"]
}

Rules:
1. Order sections by business logic: revenue/growth → product → customer/channel → risk → remaining.
2. Include all successful iterations but discard repetitive or low-value ones.
3. Aim for 4-8 sections. Merge small related analyses; split large heterogeneous ones.
4. In "guidance", specify tables, narrative angle, and comparisons.
5. Map graphs to sections based on filenames and analyses.
6. The executive_summary_guidance should list the 5-7 most important business takeaways.
7. The glossary_terms list should include every abbreviation and internal term.
8. Output ONLY the JSON object. No extra text before or after."""

SECTION_WRITER_SYSTEM_PROMPT = """\
You are a senior data analyst writing ONE section of a business report for executives.

Your job for THIS SECTION ONLY:
1. Write a clear ## section heading followed by the detailed narrative.
2. For each key finding, include:
   - A "Basis of Preparation" opening paragraph
   - MANDATORY year-by-year comparison tables when findings span multiple periods
   - A thorough narrative (5-10 sentences minimum)
   - Graph references using EXACTLY: [GRAPH: filename.png]
3. Be thorough, detailed, and VERBOSE — 300-600 words per sub-section.
4. Do NOT write an executive summary — that will be handled separately.
5. Do NOT include Python code.

CURRENCY FORMAT — MANDATORY:
All monetary values MUST use the euro symbol prefix with lowercase magnitude suffix.
Correct: ~€0.57m, €16.7m  |  WRONG: "EUR 0.57M"

TONE — Formal, senior consulting register.
Output clean markdown for this section only. Start with ## heading."""

EXEC_SUMMARY_SYSTEM_PROMPT = """\
You are a senior data analyst writing the Executive Summary for a business due-diligence report.

Write a comprehensive Executive Summary:
1. Start with ## Executive Summary
2. Open with a 3-4 sentence overview.
3. List 5-7 bullet points with cross-references "(see Section N: Title)".
4. Close with a forward-looking paragraph on key risks and opportunities.

CURRENCY FORMAT — MANDATORY:
Correct: ~€0.57m, €16.7m  |  WRONG: "EUR 0.57M"
TONE — Formal, senior consulting register.
Output clean markdown. Do NOT include Python code."""

GLOSSARY_SYSTEM_PROMPT = """\
You are a technical editor compiling a Glossary for a business due-diligence report.

Write a ## Glossary section formatted as a markdown table.
Include EVERY abbreviation, acronym, internal category name, and technical metric.
Sort alphabetically. Each definition should be 1-2 sentences.
Output clean markdown starting with ## Glossary."""

# ─── Report Building Helpers ──────────────────────────────────────────────────

config = yaml.safe_load(read_file("config.yaml"))
PHASE2_MAX_TOKENS = config.get("analyst_max_tokens", 16384)


def parse_archive(archive_text: str) -> list:
    """Parse full_archive.txt into SUCCESS entries only (legacy compatibility)."""
    return parse_archive_success(archive_text)


def extract_code_to_file(entries: list, output_path: str) -> None:
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


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]"


def list_available_graphs(graphs_folder: str) -> list:
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
        graphs_section += "Reference them using [GRAPH: filename.png] syntax.\n\n"
        for _, filename in available_graphs:
            graphs_section += f"- {filename}\n"

    return f"""## Final State of the Knowledge Base

{context_text}
{graphs_section}
---

## Complete Log of Successful Analyses ({len(entries)} total)

{analyses_text}

---

Please generate a comprehensive final business report based on all of the above."""


def build_architect_prompt(entries: list, context_text: str, available_graphs: list) -> str:
    analyses_summary = []
    for e in entries:
        analyses_summary.append(
            f"- Iteration {e.get('iteration', '?')}: "
            f"[{e.get('analysis_type', 'Unknown')}] {e.get('hypothesis', '')}"
            f"\n  Output excerpt: {_truncate(e.get('output', ''), 800)}"
            f"\n  Evaluation excerpt: {_truncate(e.get('evaluation', ''), 400)}"
        )
    analyses_text = "\n".join(analyses_summary)

    graphs_text = ""
    if available_graphs:
        graphs_text = "\n## Available Graphs\n" + "\n".join(
            f"- {fname}" for _, fname in available_graphs
        )

    return f"""## Knowledge Base
{context_text}
{graphs_text}

## Successful Analyses ({len(entries)} total)
{analyses_text}

---

Design the report structure. Output a single JSON object following the schema in your instructions."""


def parse_outline_json(raw_text: str) -> dict:
    text = raw_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
    outline = json.loads(text)
    if "sections" not in outline or not isinstance(outline["sections"], list):
        raise ValueError("Outline JSON missing 'sections' array")
    for sec in outline["sections"]:
        if "iterations" not in sec or "title" not in sec:
            raise ValueError(f"Section missing required fields: {sec}")
        sec.setdefault("section_number", 0)
        sec.setdefault("description", "")
        sec.setdefault("graphs", [])
        sec.setdefault("guidance", "")
    outline.setdefault("report_title", "Final Analysis Report")
    outline.setdefault("executive_summary_guidance", "")
    outline.setdefault("glossary_terms", [])
    return outline


def filter_entries_for_section(entries: list, iteration_ids: list) -> list:
    id_set = {str(i) for i in iteration_ids}
    return [e for e in entries if str(e.get("iteration", "")) in id_set]


def build_section_prompt(
    section: dict, section_entries: list, context_text: str,
    section_graphs: list, previous_titles: list,
) -> str:
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
        graphs_text = "\n## Graphs Available for This Section\n" + "\n".join(
            f"- {fname}" for _, fname in section_graphs
        )

    prev_text = ""
    if previous_titles:
        prev_text = (
            "\n## Sections Already Written (avoid repeating their content)\n"
            + "\n".join(f"- {t}" for t in previous_titles)
        )

    return f"""## Section to Write
Title: {section['title']}
Description: {section.get('description', '')}
Architect Guidance: {section.get('guidance', '')}
{prev_text}

## Knowledge Base (for context and grounding)
{context_text}
{graphs_text}

## Analyses Assigned to This Section ({len(section_entries)} total)

{analyses_text}

---

Write this section now. Start with ## {section['title']} and be thorough and detailed.
Include year-by-year tables wherever the data supports it. Reference graphs using [GRAPH: filename.png]."""


def build_executive_summary_prompt(
    outline: dict, section_markdowns: list, context_text: str,
) -> str:
    section_digest = []
    for i, md in enumerate(section_markdowns):
        lines = md.split("\n")
        digest_lines = [l.strip() for l in lines if l.strip().startswith("#") or l.strip().startswith("|")]
        section_digest.append(
            f"Section {i+1}: " + ("\n".join(digest_lines) if digest_lines else f"Section {i+1}")
        )
    digest_text = "\n\n".join(section_digest)
    section_list = "\n".join(
        f"- Section {i+1}: {sec['title']}"
        for i, sec in enumerate(outline.get("sections", []))
    )
    return f"""## Architect's Summary Guidance
{outline.get('executive_summary_guidance', 'Summarize the 5-7 most important findings.')}

## Report Section Structure (use these for cross-references)
{section_list}

## Section Headings and Key Tables from the Completed Report
{digest_text}

## Knowledge Base
{context_text}

---

Write the Executive Summary now. Start with ## Executive Summary.
Each bullet point MUST include a cross-reference "(see Section N: Title)"."""


def build_glossary_prompt(
    outline: dict, section_markdowns: list, summary_md: str,
) -> str:
    full_report_text = summary_md + "\n\n" + "\n\n".join(section_markdowns)
    suggested_terms = outline.get("glossary_terms", [])
    terms_text = "\n".join(f"- {t}" for t in suggested_terms) if suggested_terms else "None provided"
    return f"""## Suggested Terms from Report Architect
{terms_text}

## Full Report Text (scan for any additional terms to define)
{full_report_text}

---

Write the Glossary now. Start with ## Glossary. Include every abbreviation and internal term."""


def _llm_call(client, model: str, system_prompt: str, user_message: str,
              max_tokens: int, label: str = "") -> tuple:
    logger.info(f"  [{label}] Sending request ({len(user_message):,}ch prompt, max_tokens={max_tokens})")
    text = ""
    with client.messages.stream(
        model=model, max_tokens=max_tokens, system=system_prompt,
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
    logger.info(f"  [{label}] Done — {len(text):,}ch | tokens in={usage.input_tokens:,} out={usage.output_tokens:,}")
    return text, usage


def generate_report_iterative(
    client, model: str, entries: list, context_text: str,
    available_graphs: list, config: dict,
) -> str:
    architect_max_tokens = config.get("phase2_architect_max_tokens", 4000)
    section_max_tokens = config.get("phase2_section_max_tokens", 8000)
    summary_max_tokens = config.get("phase2_summary_max_tokens", 4000)
    total_usage = {"input": 0, "output": 0}

    logger.info("\n[B.1] The Architect — generating report outline...")
    architect_prompt = build_architect_prompt(entries, context_text, available_graphs)
    architect_raw, usage = _llm_call(
        client, model, ARCHITECT_SYSTEM_PROMPT, architect_prompt,
        architect_max_tokens, label="B.1 Architect",
    )
    total_usage["input"] += usage.input_tokens
    total_usage["output"] += usage.output_tokens

    try:
        outline = parse_outline_json(architect_raw)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(f"[B.1] JSON parse failed ({e}), retrying...")
        retry_prompt = architect_prompt + "\n\nIMPORTANT: Output ONLY valid JSON."
        architect_raw, usage = _llm_call(
            client, model, ARCHITECT_SYSTEM_PROMPT, retry_prompt,
            architect_max_tokens, label="B.1 Architect (retry)",
        )
        total_usage["input"] += usage.input_tokens
        total_usage["output"] += usage.output_tokens
        try:
            outline = parse_outline_json(architect_raw)
        except (json.JSONDecodeError, ValueError):
            logger.error("[B.1] Retry also failed. Falling back to legacy single-call.")
            return None

    logger.info(f"[B.1] Outline: {len(outline['sections'])} sections")
    for sec in outline["sections"]:
        logger.info(f"       Section {sec['section_number']}: {sec['title']} (iterations={sec['iterations']})")

    section_markdowns = []
    previous_titles = []
    for sec_idx, section in enumerate(outline["sections"]):
        sec_num = sec_idx + 1
        logger.info(f"\n[B.3] Writing section {sec_num}/{len(outline['sections'])}: {section['title']}")

        section_entries = filter_entries_for_section(entries, section["iterations"])
        section_graphs = [g for g in available_graphs if g[1] in section.get("graphs", [])]

        if not section_entries:
            section_markdowns.append(f"## {section['title']}\n\n*No analyses available.*\n")
            previous_titles.append(section["title"])
            continue

        section_prompt = build_section_prompt(
            section, section_entries, context_text, section_graphs, previous_titles,
        )
        section_md, usage = _llm_call(
            client, model, SECTION_WRITER_SYSTEM_PROMPT, section_prompt,
            section_max_tokens, label=f"B.3 Section {sec_num}",
        )
        total_usage["input"] += usage.input_tokens
        total_usage["output"] += usage.output_tokens
        section_markdowns.append(section_md)
        previous_titles.append(section["title"])

    logger.info("\n[B.3] Writing Executive Summary...")
    summary_prompt = build_executive_summary_prompt(outline, section_markdowns, context_text)
    summary_md, usage = _llm_call(
        client, model, EXEC_SUMMARY_SYSTEM_PROMPT, summary_prompt,
        summary_max_tokens, label="B.3 Exec Summary",
    )
    total_usage["input"] += usage.input_tokens
    total_usage["output"] += usage.output_tokens

    glossary_max_tokens = config.get("phase2_glossary_max_tokens", 4000)
    logger.info("\n[B.3] Writing Glossary...")
    glossary_prompt = build_glossary_prompt(outline, section_markdowns, summary_md)
    glossary_md, usage = _llm_call(
        client, model, GLOSSARY_SYSTEM_PROMPT, glossary_prompt,
        glossary_max_tokens, label="B.3 Glossary",
    )
    total_usage["input"] += usage.input_tokens
    total_usage["output"] += usage.output_tokens

    logger.info("\n[B.4] Assembling final report...")
    report_title = outline.get("report_title", "Final Analysis Report")
    final_parts = [f"# {report_title}\n"]
    final_parts.append(summary_md)
    for section_md in section_markdowns:
        final_parts.append(section_md)
    final_parts.append(glossary_md)
    final_report = "\n\n---\n\n".join(final_parts)

    logger.info(
        f"[B.4] Assembly complete — {len(final_report):,}ch | "
        f"Total tokens: in={total_usage['input']:,} out={total_usage['output']:,}"
    )
    return final_report


def _generate_report_legacy(client, model: str, entries: list, context_text: str, available_graphs: list) -> str:
    user_message = build_report_prompt(entries, context_text, available_graphs)
    logger.info(f"  [Legacy] Single-call generation ({len(user_message):,}ch prompt)")
    report_text, _ = _llm_call(client, model, PHASE2_SYSTEM_PROMPT, user_message, PHASE2_MAX_TOKENS, label="Legacy")
    return report_text


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    config = load_config()
    model = config.get("model", "claude-sonnet-4-6")
    archive_path = config.get("archive_file", "full_archive.txt")
    context_path = config.get("active_context_file", "active_context.md")
    web_context_path = config.get("web_research_context_file", "web_research_context.md")
    graphs_folder = config.get("graphs_folder", "workspace/graphs")
    debug_logging = config.get("debug_logging", False)

    extracted_code_path = config.get("extracted_code_file", "extracted_code.md")
    report_md_path = "final_report.md"

    log_file = setup_logging("phase2", debug=debug_logging)

    logger.info("=" * 60)
    logger.info("Phase 2 — Final Report Generator")
    logger.info(f"Archive     : {archive_path}")
    logger.info(f"Context     : {context_path}")
    logger.info(f"Web Context : {web_context_path}")
    logger.info(f"Graphs      : {graphs_folder}")
    logger.info(f"Model       : {model}")
    logger.info(f"Log         : {log_file}")
    logger.info("=" * 60)

    if not Path(archive_path).exists():
        logger.error(f"ERROR: {archive_path} not found. Run Analysts.py first.")
        sys.exit(1)

    archive_text = read_file(archive_path)
    context_text = read_file(context_path) if Path(context_path).exists() else ""
    web_context_text = read_file(web_context_path) if Path(web_context_path).exists() else ""
    if web_context_text:
        context_text = context_text + "\n\n## Web Research Context\n" + web_context_text
        logger.info(f"Web research context loaded ({len(web_context_text):,} chars)")

    entries = parse_archive(archive_text)
    logger.info(f"Found {len(entries)} successful analyses in archive.")

    if not entries:
        logger.error("No successful analyses to report on. Exiting.")
        sys.exit(0)

    # Step A: Extract code
    logger.info(f"\n[Step A] Extracting analysis code to {extracted_code_path}...")
    extract_code_to_file(entries, extracted_code_path)

    # Step B: Generate markdown report
    available_graphs = list_available_graphs(graphs_folder)
    logger.info(f"\n[Step B] Generating markdown report...")
    logger.info(f"         Analyses: {len(entries)}  |  Available graphs: {len(available_graphs)}")

    client = create_client()
    t_b = time.time()

    report_text = generate_report_iterative(
        client, model, entries, context_text, available_graphs, config,
    )
    if report_text is None:
        logger.warning("[Step B] Iterative pipeline failed, using legacy single-call...")
        report_text = _generate_report_legacy(client, model, entries, context_text, available_graphs)

    write_file(report_md_path, report_text)
    logger.info(f"\nMarkdown report saved to: {report_md_path} ({len(report_text):,}ch)")
    logger.info(f"[Step B] Total time: {time.time() - t_b:.1f}s")

    logger.info("")
    logger.info("=" * 60)
    logger.info("Phase 2 complete.")
    logger.info(f"  {extracted_code_path}  — all successful analysis code")
    logger.info(f"  {report_md_path}       — full markdown report")
    logger.info(f"  {log_file}  — full debug log")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
