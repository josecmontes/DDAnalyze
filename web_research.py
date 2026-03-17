#!/usr/bin/env python3
"""
Web Research Module for DDAnalyze
Autonomous web research loop that complements the data analysis loop.

Runs N sequential iterations of:
  Researcher (plan search) → Web Search (execute) → Synthesizer (evaluate) → Archive → Update context
"""

import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

os.chdir(Path(__file__).parent)

from utils import (
    read_file, write_file, append_file, load_config, setup_logging,
    call_llm_with_tokens, parse_json_response, create_client,
)

logger = logging.getLogger("ddanalyze.web_research")

# ─── System Prompts ───────────────────────────────────────────────────────────

RESEARCHER_SYSTEM_PROMPT = """You are an objective and professional Financial Due Diligence Web Researcher \
in an autonomous research loop that complements a data analysis loop.

You have access to data analysis findings about a business (provided in the "Data Analysis Context").
Your job is to search the web for external information that enriches, validates, or contextualises \
these findings.

You will receive:
- Business context (task description)
- Data analysis findings (what the data analysis loop has discovered)
- Web research knowledge base (what web research has already been done)

Your job:
1. Read the web research knowledge base carefully. Do not repeat research already done.
   Avoid directions listed in "Dead Ends & Closed Paths".
2. Choose ONE specific research topic that would add the most value given:
   - What the data analysis has found (enrich or contextualise it)
   - What web research has NOT yet covered
   - What open questions exist from either the data analysis or prior web research
3. Focus on business-relevant external intelligence:
   - Industry and market context (market size, growth rates, trends)
   - Competitive landscape (key competitors, market positioning, market share)
   - Company-specific public information (news, press releases, funding, strategy)
   - Regulatory or macroeconomic factors affecting the business
   - Consumer trends and channel dynamics relevant to the industry
   - Comparable transactions or valuations in the sector
   - Supply chain, sourcing, or distribution trends
4. Return ONLY a valid JSON object with fields:
   - research_topic: short label for this research (e.g. "Spanish footwear market size")
   - hypothesis: what you expect to find and why it matters for the DD
   - search_queries: list of 2-4 specific web search queries to execute
   - connection_to_data: how this research connects to specific data analysis findings
   No preamble. No markdown. No explanation outside the JSON.

LANGUAGE:
- Search queries should be in the language most likely to yield results. For a Spanish company,
  mix English queries (for industry reports) and Spanish queries (for local market intelligence).
- Your JSON output fields must be in English."""

SYNTHESIZER_SYSTEM_PROMPT = """You are the Synthesizer agent in an autonomous web research loop that \
complements a data analysis loop. Your job is to evaluate whether web research produced useful \
business intelligence that enriches the data analysis findings.

You will receive the research topic, the search queries executed, and the search results.

Your job:
1. If the search returned no useful results or only irrelevant information, mark status as "failure".
   Explain why and suggest what to search differently.
2. If the search returned real business intelligence (market data, competitive info, industry trends,
   company news), mark status as "success". Write a plain-English synthesis in a formal, senior
   consulting tone.
3. Be specific about HOW the web findings connect to the data analysis findings. For example:
   "The data analysis shows 96.5% YoY revenue growth in FY22. Web research reveals the Spanish
   footwear market grew only 8% in the same period, confirming the company is gaining share rapidly."
4. In suggested_followup: explain WHAT to research next, WHY it would be valuable, and HOW it
   connects to the data analysis or previous web research.
5. In dead_ends: list any research directions confirmed NOT worth pursuing.
6. Return ONLY a valid JSON object with fields:
   - status: "success" or "failure"
   - quality: "high" / "medium" / "low" (for successes only)
   - summary: full plain-English synthesis of what was found and how it connects to data analysis
   - key_findings: list of specific intelligence strings
   - data_connections: list of specific connections between web findings and data analysis findings
   - suggested_followup: specific next research with reasoning
   - dead_ends: list of research directions confirmed not worth pursuing (may be empty list)
   No preamble. No markdown fences."""

SUMMARIZER_SYSTEM_PROMPT = """You are a Summarizer agent for the web research knowledge base in an \
autonomous due diligence system. The web research knowledge base has grown and needs compression.

Your job:
1. Preserve ALL section headers exactly as they are.
2. Keep the Research Index table COMPLETE — do not remove any rows.
3. Condense "Key Intelligence": merge overlapping findings, remove redundancy,
   keep the most specific version of each insight.
4. Condense "What Has Been Researched": keep one line per research done, remove verbose detail.
5. Keep "Open Questions / Suggested Next Research" current and pruned to the most relevant 5-8 items.
6. Keep "Dead Ends & Closed Paths": consolidate duplicates but never remove confirmed dead ends.
7. Keep "Cross-References to Data Analysis": consolidate but preserve key connections.
8. Do not add new findings. Do not invent intelligence. Only compress what is there.
9. Return the full new content of the knowledge base as plain text. No JSON. No preamble."""

WEB_RESEARCH_CONTEXT_TEMPLATE = """# Web Research Knowledge Base

## Overarching Goal
{goal}

## Key Intelligence
- [None yet]

## Research Index
| Iter | Topic | Queries | Status | Date |
|------|-------|---------|--------|------|

## What Has Been Researched
- [Nothing yet]

## Cross-References to Data Analysis
- [None yet]

## Open Questions / Suggested Next Research
- [None yet]

## Dead Ends & Closed Paths
- [None yet]
"""

# ─── Web Search LLM Call ──────────────────────────────────────────────────────


def call_llm_with_web_search(
    client, system: str, user: str, model: str, max_tokens: int, tag: str = "WebSearch",
) -> tuple[str, list[dict], int, int]:
    """
    Call Claude with the web search tool enabled.
    Returns (text, search_results, input_tokens, output_tokens).
    """
    import anthropic
    logger.debug(
        f"[{tag}] Sending request with web search | system={len(system):,}ch "
        f"user={len(user):,}ch max_tokens={max_tokens}"
    )
    t0 = time.time()
    total_in_tok = 0
    total_out_tok = 0
    all_search_results = []

    messages = [{"role": "user", "content": user}]

    for turn in range(10):
        response = client.messages.create(
            model=model, max_tokens=max_tokens, system=system,
            messages=messages,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
        )
        total_in_tok += response.usage.input_tokens
        total_out_tok += response.usage.output_tokens

        text_parts = []
        for block in response.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "web_search_tool_result":
                for content_block in getattr(block, "content", []):
                    if hasattr(content_block, "type") and content_block.type == "web_search_result":
                        all_search_results.append({
                            "title": getattr(content_block, "title", ""),
                            "url": getattr(content_block, "url", ""),
                            "snippet": getattr(content_block, "encrypted_content", "")[:200],
                        })

        if response.stop_reason == "end_turn" or response.stop_reason != "tool_use":
            final_text = "\n".join(text_parts)
            elapsed = time.time() - t0
            logger.info(
                f"  [{tag}] Done in {elapsed:.1f}s | turns={turn + 1} "
                f"| search_results={len(all_search_results)} "
                f"| tokens in={total_in_tok:,} out={total_out_tok:,}"
            )
            return final_text, all_search_results, total_in_tok, total_out_tok

        messages.append({"role": "assistant", "content": response.content})
        messages.append({
            "role": "user",
            "content": "Continue with your analysis based on the search results above."
        })

    elapsed = time.time() - t0
    final_text = "\n".join(text_parts) if text_parts else ""
    logger.warning(f"[{tag}] Max turns reached in {elapsed:.1f}s")
    return final_text, all_search_results, total_in_tok, total_out_tok


# ─── Message Builders ─────────────────────────────────────────────────────────


def build_researcher_user_message(
    task_content: str, data_context: str, web_context: str,
    iteration: int, config: dict,
) -> str:
    n = config.get("web_research_iterations", 10)
    return f"""## Task Description (Business Context)

{task_content}

## Data Analysis Context (What the data analysis loop has found)

{data_context}

## Web Research Knowledge Base (What web research has already covered)

{web_context}

## Iteration Info
- Current web research iteration: {iteration} of {n}
- Focus on finding EXTERNAL intelligence that enriches the data analysis findings.
- Do NOT repeat research topics already covered in the Web Research Knowledge Base.
- Prioritise research that connects to specific data analysis findings."""


def build_search_user_message(parsed: dict, task_content: str, data_context: str) -> str:
    queries = parsed.get("search_queries", [])
    topic = parsed.get("research_topic", "")
    hypothesis = parsed.get("hypothesis", "")
    connection = parsed.get("connection_to_data", "")
    queries_str = "\n".join(f"  {i+1}. {q}" for i, q in enumerate(queries))

    return f"""You are conducting web research for a financial due diligence analysis.

## Research Topic
{topic}

## Hypothesis
{hypothesis}

## Connection to Data Analysis
{connection}

## Search Queries to Execute
{queries_str}

## Business Context
{task_content}

## Key Data Analysis Findings (for cross-referencing)
{data_context[:3000]}

## Instructions
Search the web using the queries above (and any follow-up queries you deem useful).
Then provide a comprehensive synthesis of what you found, structured as:

1. **Key Facts Found**: Specific data points, statistics, market figures with sources
2. **Market Context**: How the industry/market is performing
3. **Competitive Intelligence**: Information about competitors or comparable companies
4. **Relevance to Data Analysis**: How these findings connect to the data analysis results
5. **Sources**: List all sources consulted with their credibility assessment

Be thorough and cite specific numbers, dates, and sources. If a search returns nothing useful,
say so explicitly rather than speculating."""


def build_synthesizer_user_message(
    parsed: dict, search_response: str, search_results: list, iteration: int
) -> str:
    sources_str = ""
    if search_results:
        sources_str = "\n## Sources Consulted\n"
        for i, sr in enumerate(search_results[:20], 1):
            sources_str += f"  {i}. [{sr.get('title', 'Unknown')}]({sr.get('url', '')})\n"

    return f"""## Research Being Evaluated
Iteration: {iteration}
Research Topic: {parsed.get("research_topic", "")}
Hypothesis: {parsed.get("hypothesis", "")}
Connection to Data: {parsed.get("connection_to_data", "")}

## Search Queries Executed
{json.dumps(parsed.get("search_queries", []), indent=2)}

## Web Search Results & Synthesis
{search_response}
{sources_str}

Evaluate this web research and return a JSON object with the specified fields."""


# ─── Active Context Utilities ─────────────────────────────────────────────────


def _insert_after_header(content: str, header: str, new_text: str) -> str:
    lines = content.split("\n")
    for i, line in enumerate(lines):
        if line.strip() == header:
            lines.insert(i + 1, new_text)
            return "\n".join(lines)
    return content


def _append_to_table(content: str, section_header: str, new_row: str) -> str:
    lines = content.split("\n")
    last_table_line = -1
    in_section = False
    for i, line in enumerate(lines):
        if section_header in line:
            in_section = True
            continue
        if in_section:
            if line.startswith("## "):
                break
            if "|" in line:
                last_table_line = i
    if last_table_line == -1:
        for i, line in enumerate(lines):
            if section_header in line:
                insert_at = min(i + 3, len(lines))
                lines.insert(insert_at, new_row)
                return "\n".join(lines)
        return content
    lines.insert(last_table_line + 1, new_row)
    return "\n".join(lines)


def update_web_context_success(
    path: str, iteration: int, parsed: dict, evaluation: dict,
) -> None:
    content = read_file(path)
    now = datetime.now().strftime("%Y-%m-%d")

    topic = parsed.get("research_topic", "unknown")
    queries = "; ".join(parsed.get("search_queries", []))[:80]
    new_row = f"| {iteration} | {topic} | {queries} | SUCCESS | {now} |"
    content = _append_to_table(content, "## Research Index", new_row)

    findings = evaluation.get("key_findings", [])
    if findings:
        facts_text = "\n".join(f"- [Iter {iteration}] {f}" for f in findings)
        content = _insert_after_header(content, "## Key Intelligence", facts_text)

    summary_full = evaluation.get("summary", "")
    tried = f"- [Iter {iteration}] {topic}: {summary_full}"
    content = _insert_after_header(content, "## What Has Been Researched", tried)

    data_connections = evaluation.get("data_connections", [])
    if data_connections:
        connections_text = "\n".join(f"- [Iter {iteration}] {dc}" for dc in data_connections)
        content = _insert_after_header(content, "## Cross-References to Data Analysis", connections_text)

    followup = evaluation.get("suggested_followup", "")
    if followup:
        content = _insert_after_header(
            content, "## Open Questions / Suggested Next Research",
            f"- [From Iter {iteration}] {followup}",
        )

    dead_ends = evaluation.get("dead_ends", [])
    for de in dead_ends:
        content = _insert_after_header(
            content, "## Dead Ends & Closed Paths", f"- [From Iter {iteration}] {de}",
        )

    write_file(path, content)


def update_web_context_failure(
    path: str, iteration: int, parsed: dict, evaluation: dict
) -> None:
    content = read_file(path)
    now = datetime.now().strftime("%Y-%m-%d")

    topic = parsed.get("research_topic", "unknown")
    queries = "; ".join(parsed.get("search_queries", []))[:80]
    new_row = f"| {iteration} | {topic} | {queries} | FAILED | {now} |"
    content = _append_to_table(content, "## Research Index", new_row)

    suggested = evaluation.get("suggested_followup", "")
    tried = f"- [Iter {iteration}] FAILED — {topic}. {suggested}"
    content = _insert_after_header(content, "## What Has Been Researched", tried)

    dead_ends = evaluation.get("dead_ends", [])
    for de in dead_ends:
        content = _insert_after_header(
            content, "## Dead Ends & Closed Paths", f"- [From Iter {iteration}] {de}",
        )

    write_file(path, content)


# ─── Archive Utilities ────────────────────────────────────────────────────────


def _format_archive_entry(
    iteration: int, parsed: Optional[dict], search_response: str,
    search_results: list, evaluation: Optional[dict],
    error_label: Optional[str] = None,
) -> str:
    sep_major = "=" * 80
    sep_minor = "-" * 80
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if error_label:
        return "\n".join([
            sep_major, f"ITERATION: {iteration}", f"DATE: {now}",
            f"STATUS: {error_label}", sep_minor,
            "RAW RESPONSE / ERROR:", search_response or "(none)",
            sep_major, "",
        ])

    status = evaluation.get("status", "unknown") if evaluation else "unknown"
    lines = [
        sep_major, f"ITERATION: {iteration}", f"DATE: {now}",
        f"STATUS: {status}",
        f"RESEARCH TOPIC: {parsed.get('research_topic', 'unknown')}",
        f"HYPOTHESIS: {parsed.get('hypothesis', '')}",
        f"SEARCH QUERIES: {json.dumps(parsed.get('search_queries', []))}",
        f"CONNECTION TO DATA: {parsed.get('connection_to_data', '')}",
        sep_minor, "WEB SEARCH RESULTS:", search_response or "(no results)",
    ]
    if search_results:
        lines += ["", "SOURCES:"]
        for sr in search_results[:20]:
            lines.append(f"  - {sr.get('title', 'Unknown')}: {sr.get('url', '')}")
    lines += ["", sep_minor, "EVALUATION:"]
    if evaluation:
        if status == "success":
            lines.append(f"Quality: {evaluation.get('quality', 'unknown')}")
            lines.append(f"Summary: {evaluation.get('summary', '')}")
            lines.append("Key findings:")
            for finding in evaluation.get("key_findings", []):
                lines.append(f"  - {finding}")
            lines.append("Data connections:")
            for dc in evaluation.get("data_connections", []):
                lines.append(f"  - {dc}")
            lines.append(f"Suggested followup: {evaluation.get('suggested_followup', '')}")
            dead_ends = evaluation.get("dead_ends", [])
            if dead_ends:
                lines.append("Confirmed dead ends:")
                for de in dead_ends:
                    lines.append(f"  - {de}")
        else:
            lines.append(f"Summary: {evaluation.get('summary', '')}")
            lines.append(f"Suggested followup: {evaluation.get('suggested_followup', '')}")
    lines += [sep_major, ""]
    return "\n".join(lines)


# ─── Initialisation Helpers ───────────────────────────────────────────────────


def _extract_goal_from_task(task_content: str) -> str:
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


def _init_web_context(path: str, task_content: str) -> None:
    goal = _extract_goal_from_task(task_content)
    write_file(path, WEB_RESEARCH_CONTEXT_TEMPLATE.format(goal=goal))
    logger.info(f"  [Init] Created {path} from template")


# ─── Main Loop ────────────────────────────────────────────────────────────────


def main() -> None:
    config = load_config()
    model = config.get("web_research_model", config["model"])
    max_tokens = config.get("web_research_max_tokens", config["max_tokens"])
    researcher_max_tokens = config.get("web_research_researcher_max_tokens", 8000)
    search_max_tokens = config.get("web_research_search_max_tokens", 16000)
    n_iterations = config.get("web_research_iterations", 10)
    task_path = config["task_file"]
    data_context_path = config["active_context_file"]
    web_context_path = config.get("web_research_context_file", "web_research_context.md")
    web_archive_path = config.get("web_research_archive_file", "web_research_archive.txt")
    summarizer_every = config.get("web_research_summarizer_every_n", 5)
    debug_logging = config.get("debug_logging", False)
    fresh_start = config.get("web_research_fresh_start", False)

    log_file = setup_logging("web_research", debug=debug_logging)

    task_content = read_file(task_path)
    client = create_client()

    if fresh_start:
        logger.info("[fresh_start] Resetting web research context and archive")
        _init_web_context(web_context_path, task_content)
        write_file(web_archive_path, "")
    else:
        if not Path(web_context_path).exists():
            _init_web_context(web_context_path, task_content)

    if not Path(web_archive_path).exists():
        write_file(web_archive_path, "")

    logger.info(f"\n{'=' * 60}")
    logger.info("AUTONOMOUS WEB RESEARCH LOOP")
    logger.info(f"Model   : {model}")
    logger.info(f"Runs    : {n_iterations}  |  Summarizer every {summarizer_every} iters")
    logger.info(f"Tokens  : researcher={researcher_max_tokens}  search={search_max_tokens}  synthesizer={max_tokens}")
    logger.info(f"Context : {web_context_path}")
    logger.info(f"Archive : {web_archive_path}")
    logger.info(f"Data ctx: {data_context_path}")
    logger.info(f"Log     : {log_file}")
    logger.info(f"Debug   : {'ON' if debug_logging else 'OFF'}")
    logger.info(f"{'=' * 60}")

    total_input_tokens = 0
    total_output_tokens = 0

    for iteration in range(1, n_iterations + 1):
        iter_start = time.time()
        logger.info(f"\n=== WEB RESEARCH ITERATION {iteration} / {n_iterations} ===")

        try:
            web_context = read_file(web_context_path)
            data_context = ""
            if Path(data_context_path).exists():
                data_context = read_file(data_context_path)
            else:
                logger.warning(f"[Context] Data analysis context not found: {data_context_path}")

            logger.info("  [Researcher] Planning web research...")
            researcher_msg = build_researcher_user_message(
                task_content, data_context, web_context, iteration, config
            )
            raw_researcher, in_tok, out_tok = call_llm_with_tokens(
                client, RESEARCHER_SYSTEM_PROMPT, researcher_msg, model,
                researcher_max_tokens, tag="Researcher"
            )
            total_input_tokens += in_tok
            total_output_tokens += out_tok

            parsed = parse_json_response(raw_researcher, tag="Researcher")
            if parsed is None:
                logger.error("  [ERROR] Researcher returned invalid JSON — skipping iteration")
                entry = _format_archive_entry(iteration, None, raw_researcher, [], None, "JSON_PARSE_ERROR")
                append_file(web_archive_path, entry)
                continue

            research_topic = parsed.get("research_topic", "unknown")
            search_queries = parsed.get("search_queries", [])
            logger.info(f"  [Researcher] Topic  : {research_topic}")
            logger.info(f"  [Researcher] Queries: {search_queries}")

            logger.info("  [Search] Executing web search...")
            search_msg = build_search_user_message(parsed, task_content, data_context)
            search_response, search_results, in_tok, out_tok = call_llm_with_web_search(
                client, "", search_msg, model, search_max_tokens, tag="WebSearch"
            )
            total_input_tokens += in_tok
            total_output_tokens += out_tok

            logger.info(f"  [Search] Got {len(search_results)} sources, response: {len(search_response):,}ch")

            logger.info("  [Synthesizer] Evaluating research...")
            synth_msg = build_synthesizer_user_message(parsed, search_response, search_results, iteration)
            raw_synth, in_tok, out_tok = call_llm_with_tokens(
                client, SYNTHESIZER_SYSTEM_PROMPT, synth_msg, model, max_tokens, tag="Synthesizer"
            )
            total_input_tokens += in_tok
            total_output_tokens += out_tok

            evaluation = parse_json_response(raw_synth, tag="Synthesizer")
            if evaluation is None:
                logger.error("  [ERROR] Synthesizer returned invalid JSON — logging and continuing")
                entry = _format_archive_entry(
                    iteration, parsed, search_response, search_results, None, "SYNTH_JSON_PARSE_ERROR"
                )
                append_file(web_archive_path, entry)
                continue

            status = evaluation.get("status", "unknown")
            quality = evaluation.get("quality", "") if status == "success" else ""
            logger.info(f"  [Synthesizer] Status: {status}" + (f" | Quality: {quality}" if quality else ""))

            entry = _format_archive_entry(iteration, parsed, search_response, search_results, evaluation)
            append_file(web_archive_path, entry)

            if status == "success":
                update_web_context_success(web_context_path, iteration, parsed, evaluation)
                n_findings = len(evaluation.get("key_findings", []))
                n_connections = len(evaluation.get("data_connections", []))
                logger.info(f"  [Context] Added {n_findings} finding(s), {n_connections} data connection(s)")
            else:
                update_web_context_failure(web_context_path, iteration, parsed, evaluation)
                logger.info("  [Context] Logged failure")

            if iteration % summarizer_every == 0 and iteration < n_iterations:
                ctx_before = read_file(web_context_path)
                logger.info("  [Summarizer] Compressing web research context...")
                summarizer_msg = (
                    f"Current iteration: {iteration} of {n_iterations} total.\n\n"
                    + ctx_before
                )
                new_ctx, in_tok, out_tok = call_llm_with_tokens(
                    client, SUMMARIZER_SYSTEM_PROMPT, summarizer_msg, model,
                    max_tokens, tag="Summarizer",
                )
                total_input_tokens += in_tok
                total_output_tokens += out_tok
                write_file(web_context_path, new_ctx)
                logger.info(f"  [Summarizer] Compressed: {len(ctx_before):,}ch → {len(new_ctx):,}ch")

        except KeyboardInterrupt:
            logger.info("\n[Interrupted] Exiting loop cleanly.")
            break
        except Exception as exc:
            logger.error(f"  [FATAL ERROR] Iteration {iteration}: {exc}", exc_info=True)
            error_entry = _format_archive_entry(iteration, None, str(exc), [], None, "FATAL_ERROR")
            append_file(web_archive_path, error_entry)

        iter_elapsed = time.time() - iter_start
        logger.info(
            f"  [Iter {iteration}] Done in {iter_elapsed:.0f}s | "
            f"cumulative tokens in={total_input_tokens:,} out={total_output_tokens:,}"
        )

        wait_min = config.get("web_research_interval_minutes", config.get("interval_minutes", 0))
        if iteration < n_iterations and wait_min > 0:
            logger.info(f"  [Wait] Sleeping {wait_min} minute(s)...")
            time.sleep(wait_min * 60)

    logger.info(f"\n{'=' * 60}")
    logger.info("WEB RESEARCH LOOP COMPLETE")
    logger.info(f"Archive : {web_archive_path}")
    logger.info(f"Context : {web_context_path}")
    logger.info(f"Log     : {log_file}")
    logger.info(
        f"Total tokens — input: {total_input_tokens:,}  output: {total_output_tokens:,}  "
        f"total: {total_input_tokens + total_output_tokens:,}"
    )
    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
