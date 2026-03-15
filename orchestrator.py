#!/usr/bin/env python3
"""
DDAnalyze Orchestrator
Master controller that coordinates the full due diligence pipeline:

  1. Scheduled phases of data analysis (loop.py) and web research (web_research.py)
  2. Final report generation (phase2.py)
  3. Interactive feedback loop for user-guided refinements
  4. Data extraction on demand (find analysis code → adapt → export to Excel)

Usage:
  python orchestrator.py              # Run the full pipeline
  python orchestrator.py --interactive  # Skip initial phases, go straight to interactive mode
"""

import argparse
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

os.chdir(Path(__file__).parent)

# ─── Logging Setup ────────────────────────────────────────────────────────────

logger = logging.getLogger("ddanalyze.orchestrator")


def setup_logging(debug: bool = False, log_dir: str = "logs") -> Path:
    Path(log_dir).mkdir(exist_ok=True)
    log_file = Path(log_dir) / f"orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    console_level = logging.DEBUG if debug else logging.INFO

    root = logging.getLogger("ddanalyze")
    root.setLevel(logging.DEBUG)

    # Avoid adding duplicate handlers if logging was already set up
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


# ─── Archive Utilities ────────────────────────────────────────────────────────

_INTERNAL_ERROR_LABELS = {
    "JSON_PARSE_ERROR", "CRITIC_JSON_PARSE_ERROR", "TIMEOUT", "FATAL_ERROR",
    "SYNTH_JSON_PARSE_ERROR",
}


def parse_archive_all(archive_text: str) -> list:
    """Parse full_archive.txt into list of dicts for ALL entries (success + failure)."""
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


def _get_current_iteration(archive_path: str) -> int:
    """Determine the highest iteration number already in the archive."""
    if not Path(archive_path).exists():
        return 0
    text = read_file(archive_path)
    iters = re.findall(r"ITERATION:\s*(\d+)", text)
    return max(int(i) for i in iters) if iters else 0


# ─── Phase Runners ────────────────────────────────────────────────────────────
# These run loop.py, web_research.py, and phase2.py as subprocesses so that
# each module keeps its own logging and state management intact. Config
# overrides are applied by temporarily patching config.yaml.

def _patch_config(overrides: dict) -> dict:
    """Apply temporary overrides to config.yaml. Returns original config for restore."""
    config = yaml.safe_load(read_file("config.yaml"))
    original = dict(config)
    config.update(overrides)
    write_file("config.yaml", yaml.dump(config, default_flow_style=False, allow_unicode=True))
    return original


def _restore_config(original: dict) -> None:
    """Restore config.yaml to its original state."""
    write_file("config.yaml", yaml.dump(original, default_flow_style=False, allow_unicode=True))


def run_data_analysis(n_iterations: int, start_iteration: int = 1) -> bool:
    """Run the data analysis loop for n_iterations starting from start_iteration."""
    logger.info(f"\n{'─' * 60}")
    logger.info(f"PHASE: DATA ANALYSIS — {n_iterations} iterations (starting at iter {start_iteration})")
    logger.info(f"{'─' * 60}")

    original = _patch_config({
        "n_iterations": n_iterations,
        "fresh_start": False,
        "summarize_on_start": False,
    })

    try:
        result = subprocess.run(
            [sys.executable, "loop.py"],
            timeout=n_iterations * 300,  # 5 min per iteration max
        )
        success = result.returncode == 0
        if success:
            logger.info(f"[Data Analysis] Completed {n_iterations} iterations successfully.")
        else:
            logger.warning(f"[Data Analysis] Exited with code {result.returncode}")
        return success
    except subprocess.TimeoutExpired:
        logger.error("[Data Analysis] Timed out")
        return False
    except KeyboardInterrupt:
        logger.info("[Data Analysis] Interrupted by user")
        return False
    finally:
        _restore_config(original)


def run_web_research(n_iterations: int) -> bool:
    """Run the web research loop for n_iterations."""
    logger.info(f"\n{'─' * 60}")
    logger.info(f"PHASE: WEB RESEARCH — {n_iterations} iterations")
    logger.info(f"{'─' * 60}")

    original = _patch_config({
        "web_research_iterations": n_iterations,
        "web_research_fresh_start": False,
    })

    try:
        result = subprocess.run(
            [sys.executable, "web_research.py"],
            timeout=n_iterations * 300,
        )
        success = result.returncode == 0
        if success:
            logger.info(f"[Web Research] Completed {n_iterations} iterations successfully.")
        else:
            logger.warning(f"[Web Research] Exited with code {result.returncode}")
        return success
    except subprocess.TimeoutExpired:
        logger.error("[Web Research] Timed out")
        return False
    except KeyboardInterrupt:
        logger.info("[Web Research] Interrupted by user")
        return False
    finally:
        _restore_config(original)


def run_report_generation() -> bool:
    """Run phase2 report generation."""
    logger.info(f"\n{'─' * 60}")
    logger.info("PHASE: REPORT GENERATION")
    logger.info(f"{'─' * 60}")

    try:
        result = subprocess.run(
            [sys.executable, "phase2.py"],
            timeout=600,  # 10 min max
        )
        success = result.returncode == 0
        if success:
            logger.info("[Report] Generated final_report.md and final_report.docx")
        else:
            logger.warning(f"[Report] Exited with code {result.returncode}")
        return success
    except subprocess.TimeoutExpired:
        logger.error("[Report] Timed out")
        return False
    except KeyboardInterrupt:
        logger.info("[Report] Interrupted by user")
        return False


def run_deloitte_report() -> bool:
    """Run the Deloitte premium HTML report generator."""
    logger.info(f"\n{'─' * 60}")
    logger.info("PHASE: DELOITTE PREMIUM REPORT")
    logger.info(f"{'─' * 60}")

    try:
        result = subprocess.run(
            [sys.executable, "deloitte_report.py"],
            timeout=900,  # 15 min max — multiple LLM calls
        )
        success = result.returncode == 0
        if success:
            logger.info("[Deloitte Report] Generated deloitte_report.html")
        else:
            logger.warning(f"[Deloitte Report] Exited with code {result.returncode}")
        return success
    except subprocess.TimeoutExpired:
        logger.error("[Deloitte Report] Timed out")
        return False
    except KeyboardInterrupt:
        logger.info("[Deloitte Report] Interrupted by user")
        return False


# ─── User Guidance → Additional Iterations ───────────────────────────────────

GUIDANCE_SYSTEM_PROMPT = """You are the orchestrator agent for a financial due diligence analysis system.
The user has reviewed a report and is providing feedback — they want additional analysis, corrections,
or deeper investigation on specific topics.

Your job is to translate the user's feedback into specific guidance that the data analysis loop and/or
web research loop can act on.

You will receive:
- The user's feedback/request
- The current active_context.md (what data analysis has found)
- The current web_research_context.md (what web research has found, if available)

Determine:
1. What additional work is needed (data analysis, web research, or both)
2. How many iterations each should get (1-10)
3. Specific guidance to inject into the knowledge base so the next iteration picks it up

Return a JSON object with:
{
  "data_analysis_needed": true/false,
  "data_analysis_iterations": N,
  "data_analysis_guidance": "Specific instructions to add to Open Questions in active_context.md",
  "web_research_needed": true/false,
  "web_research_iterations": N,
  "web_research_guidance": "Specific instructions to add to Open Questions in web_research_context.md",
  "summary": "One-line summary of what will be done"
}

Rules:
- Be specific in the guidance — translate vague requests into concrete analysis tasks
- If the user wants corrections, frame them as new analyses that will supersede the old ones
- Default to 3-5 iterations unless the user's request is very specific (1-2) or very broad (8-10)
- Always prefer data analysis over web research unless the request is explicitly about external context
"""


def _inject_guidance(context_path: str, section_header: str, guidance: str, source: str) -> None:
    """Inject user guidance into the Open Questions section of a context file."""
    if not Path(context_path).exists():
        return
    content = read_file(context_path)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    injection = f"- [USER GUIDANCE — {timestamp}] **PRIORITY**: {guidance}"

    lines = content.split("\n")
    for i, line in enumerate(lines):
        if line.strip() == section_header:
            lines.insert(i + 1, injection)
            write_file(context_path, "\n".join(lines))
            logger.info(f"  [Guidance] Injected into {context_path}: {guidance[:100]}...")
            return

    logger.warning(f"  [Guidance] Section '{section_header}' not found in {context_path}")


def process_user_feedback(
    client: anthropic.Anthropic,
    model: str,
    user_input: str,
    config: dict,
) -> Optional[dict]:
    """Translate user feedback into actionable guidance for the analysis loops."""
    data_context_path = config["active_context_file"]
    web_context_path = config.get("web_research_context_file", "web_research_context.md")

    data_context = read_file(data_context_path) if Path(data_context_path).exists() else ""
    web_context = read_file(web_context_path) if Path(web_context_path).exists() else ""

    user_msg = f"""## User Feedback
{user_input}

## Current Data Analysis Knowledge Base
{data_context[:8000]}

## Current Web Research Knowledge Base
{web_context[:4000]}"""

    response = call_llm(
        client, GUIDANCE_SYSTEM_PROMPT, user_msg, model, 4000, tag="Guidance"
    )

    plan = parse_json_response(response)
    if plan is None:
        logger.error("[Guidance] Failed to parse guidance plan")
        return None

    return plan


def execute_feedback_plan(plan: dict, config: dict) -> None:
    """Execute the plan generated from user feedback."""
    data_context_path = config["active_context_file"]
    web_context_path = config.get("web_research_context_file", "web_research_context.md")

    # Inject guidance into knowledge bases
    if plan.get("data_analysis_needed") and plan.get("data_analysis_guidance"):
        _inject_guidance(
            data_context_path,
            "## Open Questions / Suggested Next Steps",
            plan["data_analysis_guidance"],
            "user",
        )

    if plan.get("web_research_needed") and plan.get("web_research_guidance"):
        _inject_guidance(
            web_context_path,
            "## Open Questions / Suggested Next Research",
            plan["web_research_guidance"],
            "user",
        )

    # Run additional iterations
    if plan.get("data_analysis_needed"):
        n = plan.get("data_analysis_iterations", 3)
        archive_path = config.get("archive_file", "full_archive.txt")
        current_iter = _get_current_iteration(archive_path)
        run_data_analysis(n, start_iteration=current_iter + 1)

    if plan.get("web_research_needed"):
        n = plan.get("web_research_iterations", 3)
        run_web_research(n)

    # Regenerate report
    logger.info("\n[Feedback] Regenerating report with updated findings...")
    run_report_generation()


# ─── Data Extraction ──────────────────────────────────────────────────────────

EXTRACTION_SYSTEM_PROMPT = """You are a data extraction specialist for a financial due diligence system.
The user wants to extract specific data from analyses that have been run. You have access to the
archive of all successful analyses, including the Python code and output for each.

Your job:
1. Find the most relevant analysis/analyses from the archive that match the user's request
2. Write NEW Python code that:
   a. Loads the same data file (workspace/data.xlsx)
   b. Performs the relevant filtering/transformation the user asked for
   c. Exports the result to an Excel file in workspace/exports/
   d. Prints a summary of what was exported

The code MUST:
- Import pandas and openpyxl
- Load from: df = pd.read_excel("workspace/data.xlsx")
- Save to: workspace/exports/<descriptive_name>.xlsx
- Use pd.ExcelWriter with openpyxl engine for multi-sheet exports when appropriate
- Print the file path and row/column counts after saving
- Handle the specific data transformations the user is asking about
- Be self-contained (no imports from project modules except deloitte_theme if charting)

Return ONLY a JSON object with:
{
  "description": "What this extraction contains",
  "source_iterations": [list of iteration numbers this is based on],
  "filename": "descriptive_name.xlsx",
  "code": "full Python code"
}
No preamble. No markdown fences around the JSON."""


def handle_data_extraction(
    client: anthropic.Anthropic,
    model: str,
    user_request: str,
    config: dict,
) -> bool:
    """Find relevant analysis code, adapt it, and export data to Excel."""
    archive_path = config.get("archive_file", "full_archive.txt")

    if not Path(archive_path).exists():
        logger.error("[Extract] No archive found. Run data analysis first.")
        return False

    archive_text = read_file(archive_path)
    entries = parse_archive_all(archive_text)
    success_entries = [e for e in entries if e.get("status", "").lower() == "success"]

    if not success_entries:
        logger.error("[Extract] No successful analyses in archive.")
        return False

    # Build a condensed view of available analyses for the LLM
    analyses_summary = []
    for e in success_entries:
        analyses_summary.append(
            f"--- Iteration {e.get('iteration', '?')}: {e.get('analysis_type', 'unknown')} ---\n"
            f"Hypothesis: {e.get('hypothesis', '')}\n"
            f"Columns: {e.get('columns_used', '')}\n"
            f"Code:\n{e.get('code', '')}\n"
            f"Output (preview): {e.get('output', '')[:1000]}\n"
        )

    user_msg = f"""## User's Extraction Request
{user_request}

## Available Analyses (from archive)
{chr(10).join(analyses_summary)}

## Data File
Path: workspace/data.xlsx
Export directory: workspace/exports/ (create if needed)

Write Python code that extracts exactly what the user asked for, based on the relevant
analysis code above. Adapt the code — don't just copy it. Focus on producing a clean,
well-structured Excel export that the user can work with."""

    response = call_llm(
        client, EXTRACTION_SYSTEM_PROMPT, user_msg, model, 16000, tag="Extract"
    )

    parsed = parse_json_response(response)
    if parsed is None:
        logger.error("[Extract] Failed to parse extraction plan")
        return False

    description = parsed.get("description", "Data extraction")
    filename = parsed.get("filename", "extraction.xlsx")
    code = parsed.get("code", "")
    source_iters = parsed.get("source_iterations", [])

    logger.info(f"  [Extract] Description: {description}")
    logger.info(f"  [Extract] Source iterations: {source_iters}")
    logger.info(f"  [Extract] Output file: workspace/exports/{filename}")

    # Ensure exports directory exists
    Path("workspace/exports").mkdir(parents=True, exist_ok=True)

    # Write and execute the extraction code
    script_path = "workspace/extraction_script.py"
    write_file(script_path, code)

    logger.info("  [Extract] Running extraction code...")

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            timeout=120,
        )

        if result.stdout:
            logger.info(f"  [Extract] Output:\n{result.stdout}")

        if result.stderr:
            logger.warning(f"  [Extract] Errors:\n{result.stderr}")

            # Retry once if there's an error
            logger.info("  [Extract] Attempting to fix extraction code...")
            retry_msg = f"""The extraction code produced an error. Fix it.

## Original Code
```python
{code}
```

## Error
{result.stderr}

## stdout (if any)
{result.stdout}

Return the same JSON format with corrected code."""

            retry_response = call_llm(
                client, EXTRACTION_SYSTEM_PROMPT, retry_msg, model, 16000, tag="ExtractRetry"
            )
            retry_parsed = parse_json_response(retry_response)
            if retry_parsed and retry_parsed.get("code"):
                write_file(script_path, retry_parsed["code"])
                result = subprocess.run(
                    [sys.executable, script_path],
                    capture_output=True, text=True, encoding="utf-8",
                    errors="replace", env=env, timeout=120,
                )
                if result.stdout:
                    logger.info(f"  [Extract] Retry output:\n{result.stdout}")
                if result.stderr:
                    logger.error(f"  [Extract] Retry still errored:\n{result.stderr}")
                    return False

        # Verify the file was created
        export_path = Path("workspace/exports") / filename
        if export_path.exists():
            size_kb = export_path.stat().st_size / 1024
            logger.info(f"  [Extract] SUCCESS — {export_path} ({size_kb:.1f} KB)")
            return True
        else:
            # Check if any xlsx was created (filename might differ)
            xlsx_files = list(Path("workspace/exports").glob("*.xlsx"))
            if xlsx_files:
                latest = max(xlsx_files, key=lambda p: p.stat().st_mtime)
                size_kb = latest.stat().st_size / 1024
                logger.info(f"  [Extract] SUCCESS — {latest} ({size_kb:.1f} KB)")
                return True
            logger.error(f"  [Extract] No Excel file was created")
            return False

    except subprocess.TimeoutExpired:
        logger.error("[Extract] Extraction timed out")
        return False


# ─── Default Schedule ─────────────────────────────────────────────────────────

DEFAULT_SCHEDULE = [
    # (phase_type, n_iterations)
    ("data_analysis", 5),
    ("web_research", 3),
    ("data_analysis", 5),
    ("web_research", 2),
    ("data_analysis", 5),
    ("report", 0),
]


def parse_schedule(schedule_config) -> list:
    """Parse schedule from config. Accepts list of dicts or uses default."""
    if not schedule_config:
        return DEFAULT_SCHEDULE

    phases = []
    for phase in schedule_config:
        if isinstance(phase, dict):
            ptype = phase.get("type", "data_analysis")
            n = phase.get("iterations", 5)
            phases.append((ptype, n))
        elif isinstance(phase, str):
            phases.append((phase, 0 if phase == "report" else 5))

    return phases


# ─── Interactive Mode ─────────────────────────────────────────────────────────

INTERACTIVE_HELP = """
╔══════════════════════════════════════════════════════════════╗
║                   DDAnalyze Interactive Mode                 ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  Commands:                                                   ║
║    <feedback text>    Provide analysis guidance/corrections   ║
║                       → runs additional iterations + report  ║
║                                                              ║
║    /extract <request> Extract data to Excel                  ║
║                       e.g. /extract revenue by client by     ║
║                            year with all detail rows         ║
║                                                              ║
║    /analyze <N>       Run N more data analysis iterations    ║
║    /research <N>      Run N more web research iterations     ║
║    /report            Regenerate the report                  ║
║    /deloitte          Generate Deloitte premium HTML report  ║
║    /status            Show current state                     ║
║    /help              Show this help message                 ║
║    /quit              Exit interactive mode                  ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"""


def show_status(config: dict) -> None:
    """Show current state of the analysis."""
    archive_path = config.get("archive_file", "full_archive.txt")
    context_path = config["active_context_file"]
    web_context_path = config.get("web_research_context_file", "web_research_context.md")

    print("\n" + "=" * 60)
    print("CURRENT STATUS")
    print("=" * 60)

    # Data analysis status
    if Path(archive_path).exists():
        archive_text = read_file(archive_path)
        entries = parse_archive_all(archive_text)
        success = sum(1 for e in entries if e.get("status", "").lower() == "success")
        failed = sum(1 for e in entries if e.get("status", "").lower() != "success")
        total = _get_current_iteration(archive_path)
        print(f"\nData Analysis: {total} iterations ({success} success, {failed} failed)")

        # Show analysis types done
        types = set()
        for e in entries:
            if e.get("status", "").lower() == "success":
                types.add(e.get("analysis_type", "unknown"))
        if types:
            print(f"  Analysis types: {', '.join(sorted(types))}")
    else:
        print("\nData Analysis: Not started")

    # Web research status
    web_archive_path = config.get("web_research_archive_file", "web_research_archive.txt")
    if Path(web_archive_path).exists():
        web_text = read_file(web_archive_path)
        web_iters = re.findall(r"ITERATION:\s*(\d+)", web_text)
        print(f"\nWeb Research: {len(web_iters)} iterations")
    else:
        print("\nWeb Research: Not started")

    # Report status
    if Path("final_report.md").exists():
        report_size = Path("final_report.md").stat().st_size / 1024
        report_time = datetime.fromtimestamp(
            Path("final_report.md").stat().st_mtime
        ).strftime("%Y-%m-%d %H:%M")
        print(f"\nReport: Generated ({report_size:.1f} KB, last updated {report_time})")
    else:
        print("\nReport: Not generated")

    if Path("final_report.docx").exists():
        docx_size = Path("final_report.docx").stat().st_size / 1024
        print(f"  Word doc: {docx_size:.1f} KB")

    if Path("deloitte_report.html").exists():
        html_size = Path("deloitte_report.html").stat().st_size / 1024
        html_time = datetime.fromtimestamp(
            Path("deloitte_report.html").stat().st_mtime
        ).strftime("%Y-%m-%d %H:%M")
        print(f"\nDeloitte Report: Generated ({html_size:.1f} KB, last updated {html_time})")

    # Exports
    exports_dir = Path("workspace/exports")
    if exports_dir.exists():
        xlsx_files = list(exports_dir.glob("*.xlsx"))
        if xlsx_files:
            print(f"\nExports: {len(xlsx_files)} file(s)")
            for f in sorted(xlsx_files):
                print(f"  - {f.name} ({f.stat().st_size / 1024:.1f} KB)")

    # Graphs
    graphs_dir = Path(config.get("graphs_folder", "workspace/graphs"))
    if graphs_dir.exists():
        png_files = list(graphs_dir.glob("*.png"))
        print(f"\nGraphs: {len(png_files)} chart(s) in {graphs_dir}")

    print("=" * 60 + "\n")


def interactive_loop(client: anthropic.Anthropic, model: str, config: dict) -> None:
    """Interactive feedback loop for user-guided refinements."""
    print(INTERACTIVE_HELP)

    while True:
        try:
            user_input = input("\n📋 DDAnalyze> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting interactive mode.")
            break

        if not user_input:
            continue

        # ── Commands ──────────────────────────────────────────────────────
        if user_input.lower() in ("/quit", "/exit", "/q"):
            print("Exiting interactive mode.")
            break

        elif user_input.lower() == "/help":
            print(INTERACTIVE_HELP)
            continue

        elif user_input.lower() == "/status":
            show_status(config)
            continue

        elif user_input.lower() == "/report":
            run_report_generation()
            print("\nReport regenerated. Check final_report.md and final_report.docx")
            continue

        elif user_input.lower() == "/deloitte":
            run_deloitte_report()
            print("\nDeloitte report generated. Check deloitte_report.html")
            continue

        elif user_input.lower().startswith("/analyze"):
            parts = user_input.split()
            n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 5
            archive_path = config.get("archive_file", "full_archive.txt")
            current_iter = _get_current_iteration(archive_path)
            run_data_analysis(n, start_iteration=current_iter + 1)
            continue

        elif user_input.lower().startswith("/research"):
            parts = user_input.split()
            n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 3
            run_web_research(n)
            continue

        elif user_input.lower().startswith("/extract"):
            request = user_input[len("/extract"):].strip()
            if not request:
                print("Usage: /extract <description of data you want>")
                print("Example: /extract revenue by client by year with all detail rows")
                continue
            print(f"\nExtracting data: {request}")
            success = handle_data_extraction(client, model, request, config)
            if success:
                print("\nExtraction complete. Check workspace/exports/ for the Excel file.")
            else:
                print("\nExtraction failed. Check the log for details.")
            continue

        # ── Free-form feedback → guidance → additional iterations ─────────
        print(f"\nProcessing feedback: {user_input[:100]}...")
        plan = process_user_feedback(client, model, user_input, config)

        if plan is None:
            print("Failed to interpret feedback. Please try rephrasing.")
            continue

        # Show the plan and ask for confirmation
        print(f"\n{'─' * 50}")
        print(f"Plan: {plan.get('summary', 'Additional analysis')}")
        if plan.get("data_analysis_needed"):
            print(f"  → Data analysis: {plan.get('data_analysis_iterations', 0)} iterations")
            print(f"    Guidance: {plan.get('data_analysis_guidance', '')[:120]}")
        if plan.get("web_research_needed"):
            print(f"  → Web research: {plan.get('web_research_iterations', 0)} iterations")
            print(f"    Guidance: {plan.get('web_research_guidance', '')[:120]}")
        print(f"  → Report will be regenerated after analysis")
        print(f"{'─' * 50}")

        try:
            confirm = input("Proceed? [Y/n] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nCancelled.")
            continue

        if confirm in ("n", "no"):
            print("Cancelled.")
            continue

        execute_feedback_plan(plan, config)
        print("\nDone. Check final_report.md / .docx for the updated report.")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="DDAnalyze Orchestrator — coordinate analysis, research, and reporting"
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Skip initial phases and go straight to interactive mode",
    )
    parser.add_argument(
        "--schedule",
        type=str,
        default=None,
        help="Custom schedule as JSON, e.g. "
             '\'[{"type":"data_analysis","iterations":10},{"type":"web_research","iterations":5},'
             '{"type":"report"}]\'',
    )
    parser.add_argument(
        "--no-interactive",
        action="store_true",
        help="Run the schedule and exit without entering interactive mode",
    )
    args = parser.parse_args()

    # Load config
    config = yaml.safe_load(read_file("config.yaml"))
    model = config.get("model", "claude-sonnet-4-6")
    debug_logging = config.get("debug_logging", False)

    log_file = setup_logging(debug=debug_logging)

    client = anthropic.Anthropic(api_key=API_KEY)

    logger.info(f"\n{'═' * 60}")
    logger.info("DDAnalyze ORCHESTRATOR")
    logger.info(f"Model  : {model}")
    logger.info(f"Log    : {log_file}")
    logger.info(f"Mode   : {'Interactive only' if args.interactive else 'Full pipeline'}")
    logger.info(f"{'═' * 60}")

    if not args.interactive:
        # ── Parse and execute schedule ────────────────────────────────────
        if args.schedule:
            try:
                schedule_raw = json.loads(args.schedule)
            except json.JSONDecodeError:
                logger.error("Invalid --schedule JSON. Using default schedule.")
                schedule_raw = None
        else:
            schedule_raw = config.get("orchestrator_schedule", None)

        schedule = parse_schedule(schedule_raw)

        logger.info("\nExecution Schedule:")
        for i, (phase_type, n) in enumerate(schedule, 1):
            if phase_type == "report":
                logger.info(f"  {i}. Report Generation")
            elif phase_type == "deloitte_report":
                logger.info(f"  {i}. Deloitte Premium HTML Report")
            else:
                logger.info(f"  {i}. {phase_type.replace('_', ' ').title()} — {n} iterations")
        logger.info("")

        # Execute each phase
        for phase_type, n_iterations in schedule:
            if phase_type == "data_analysis":
                archive_path = config.get("archive_file", "full_archive.txt")
                current_iter = _get_current_iteration(archive_path)
                run_data_analysis(n_iterations, start_iteration=current_iter + 1)
            elif phase_type == "web_research":
                run_web_research(n_iterations)
            elif phase_type == "report":
                run_report_generation()
            elif phase_type == "deloitte_report":
                run_deloitte_report()
            else:
                logger.warning(f"Unknown phase type: {phase_type}")

        logger.info(f"\n{'═' * 60}")
        logger.info("SCHEDULED PHASES COMPLETE")
        logger.info(f"{'═' * 60}")

    # ── Enter interactive mode ────────────────────────────────────────────
    if not args.no_interactive:
        show_status(config)
        interactive_loop(client, model, config)

    logger.info("\nOrchestrator finished.")


if __name__ == "__main__":
    main()