#!/usr/bin/env python3
"""
DDAnalyze — Unified Web UI

Single entry point that replaces the CLI orchestrator with a browser-based
interface. Users can configure, schedule, launch analysis, monitor progress
in real time, and interactively ask questions or refine the report.

Usage:
    python app.py                  # Start the web server (default port 5000)
    python app.py --port 8080      # Custom port
"""

import argparse
import json
import logging
import os
import queue
import subprocess
import sys
import threading
from typing import Optional
import time
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template, request, Response, send_from_directory

from excel_export import export_iterations_to_excel, generate_databook
from utils import (
    read_file, write_file, load_config, call_llm, parse_json_response,
    parse_archive_all, get_current_iteration, create_client,
)

os.chdir(Path(__file__).parent)

# ─── Logging ──────────────────────────────────────────────────────────────────

Path("logs").mkdir(exist_ok=True)
log_file = Path("logs") / f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

root_logger = logging.getLogger("ddanalyze")
root_logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
root_logger.addHandler(ch)

fh = logging.FileHandler(log_file, encoding="utf-8")
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-7s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
root_logger.addHandler(fh)

logger = logging.getLogger("ddanalyze.app")

# ─── Flask App ────────────────────────────────────────────────────────────────

app = Flask(__name__)

# ─── Global State ─────────────────────────────────────────────────────────────

_config = load_config()
_client = create_client()
_model = _config.get("model", "claude-sonnet-4-6")

# Event bus: SSE subscribers get events pushed here
_event_queues: list[queue.Queue] = []
_event_lock = threading.Lock()

# Pipeline state
_pipeline_state = {
    "running": False,
    "phase": None,       # current phase name
    "progress": "",      # human-readable progress line
    "log": [],           # last N log lines
    "cancelled": False,
}
_pipeline_lock = threading.Lock()


def _broadcast(event_type: str, data: dict) -> None:
    """Push an SSE event to all connected clients."""
    payload = json.dumps({"type": event_type, **data})
    with _event_lock:
        dead = []
        for q in _event_queues:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _event_queues.remove(q)


def _log_and_broadcast(msg: str, level: str = "info") -> None:
    """Log a message and broadcast it to UI clients."""
    logger.info(msg)
    with _pipeline_lock:
        _pipeline_state["log"].append(msg)
        if len(_pipeline_state["log"]) > 500:
            _pipeline_state["log"] = _pipeline_state["log"][-300:]
    _broadcast("log", {"message": msg, "level": level})


# ─── Phase Runners (subprocess) ──────────────────────────────────────────────

def _make_override_env(overrides: dict) -> dict:
    env = os.environ.copy()
    env["DDANALYZE_CONFIG_OVERRIDES"] = json.dumps(overrides)
    return env


def _run_subprocess(label: str, cmd: list, env: dict, timeout: int) -> bool:
    """Run a subprocess, streaming its stdout to the event bus."""
    _log_and_broadcast(f"[{label}] Starting...")
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", env=env,
        )
        for line in iter(proc.stdout.readline, ""):
            line = line.rstrip()
            if line:
                _log_and_broadcast(f"[{label}] {line}")
            # Check cancellation
            with _pipeline_lock:
                if _pipeline_state["cancelled"]:
                    proc.terminate()
                    _log_and_broadcast(f"[{label}] Cancelled by user.")
                    return False
        proc.wait(timeout=timeout)
        success = proc.returncode == 0
        if success:
            _log_and_broadcast(f"[{label}] Completed successfully.")
        else:
            _log_and_broadcast(f"[{label}] Exited with code {proc.returncode}", "error")
        return success
    except subprocess.TimeoutExpired:
        proc.kill()
        _log_and_broadcast(f"[{label}] Timed out.", "error")
        return False
    except Exception as exc:
        _log_and_broadcast(f"[{label}] Error: {exc}", "error")
        return False


def run_data_analysis(n_iterations: int) -> bool:
    env = _make_override_env({"n_iterations": n_iterations, "fresh_start": False, "summarize_on_start": False})
    return _run_subprocess(
        f"Data Analysis ({n_iterations} iters)",
        [sys.executable, "Analysts.py"],
        env, n_iterations * 300,
    )


def run_web_research(n_iterations: int) -> bool:
    env = _make_override_env({"web_research_iterations": n_iterations, "web_research_fresh_start": False})
    return _run_subprocess(
        f"Web Research ({n_iterations} iters)",
        [sys.executable, "web_research.py"],
        env, n_iterations * 300,
    )


def run_report_generation() -> bool:
    return _run_subprocess("Report Generation", [sys.executable, "phase2.py"], os.environ.copy(), 600)


def run_deloitte_report() -> bool:
    return _run_subprocess("Deloitte Report", [sys.executable, "deloitte_report.py"], os.environ.copy(), 900)


def _run_adhoc_analysis(n_iterations: int, description: str) -> bool:
    """Run ad-hoc data analysis with specific guidance injected.
    Uses analysis_source=adhoc so iterations are clearly labeled in the archive."""
    env = _make_override_env({
        "n_iterations": n_iterations,
        "fresh_start": False,
        "summarize_on_start": False,
        "analysis_source": "adhoc",
    })
    return _run_subprocess(
        f"Ad-hoc Analysis: {description[:60]}",
        [sys.executable, "Analysts.py"],
        env, n_iterations * 300,
    )


# ─── Pipeline Execution ──────────────────────────────────────────────────────

def _run_pipeline(schedule: list[tuple[str, int]]) -> None:
    """Execute a full pipeline schedule in a background thread."""
    with _pipeline_lock:
        _pipeline_state["running"] = True
        _pipeline_state["cancelled"] = False
        _pipeline_state["log"] = []

    _broadcast("pipeline", {"status": "started"})
    total_phases = len(schedule)

    for idx, (phase_type, n_iterations) in enumerate(schedule, 1):
        with _pipeline_lock:
            if _pipeline_state["cancelled"]:
                break
            _pipeline_state["phase"] = phase_type
            _pipeline_state["progress"] = f"Phase {idx}/{total_phases}: {phase_type}"

        _broadcast("phase", {"phase": phase_type, "current": idx, "total": total_phases})

        if phase_type == "data_analysis":
            run_data_analysis(n_iterations)
        elif phase_type == "web_research":
            run_web_research(n_iterations)
        elif phase_type == "report":
            run_report_generation()
        elif phase_type == "deloitte_report":
            run_deloitte_report()

    with _pipeline_lock:
        _pipeline_state["running"] = False
        _pipeline_state["phase"] = None
        _pipeline_state["progress"] = "Complete"

    _broadcast("pipeline", {"status": "complete"})
    _log_and_broadcast("Pipeline finished.")


# ─── Chat / Interactive ──────────────────────────────────────────────────────

CHAT_SYSTEM_PROMPT = """You are an expert Financial Due Diligence assistant embedded in the DDAnalyze platform.
You help users understand analysis results, answer questions about findings, and suggest next steps.

You have access to:
- The data analysis knowledge base (active_context.md) — a living summary of all data analyses performed
- The web research knowledge base (web_research_context.md) — external market intelligence
- The full archive of analysis iterations with code, output, and evaluations

When answering:
- Be specific and reference actual findings from the knowledge bases
- Use professional financial language (€ prefix, lowercase magnitude suffixes like €16.7m)
- If the user asks about something not yet analyzed, suggest they run additional iterations
- If the user wants corrections, explain what would change and suggest running more analysis
- Keep answers concise but thorough — executives read these

You can also help with:
- Explaining what specific analyses found
- Comparing results across different analyses
- Identifying gaps in the current analysis
- Suggesting which analyses to run next
"""

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


def _build_context_for_chat() -> str:
    """Assemble the knowledge bases for the chat LLM."""
    parts = []

    ctx_path = _config.get("active_context_file", "active_context.md")
    if Path(ctx_path).exists():
        parts.append(f"## Data Analysis Knowledge Base\n{read_file(ctx_path)}")

    web_ctx_path = _config.get("web_research_context_file", "web_research_context.md")
    if Path(web_ctx_path).exists():
        parts.append(f"## Web Research Knowledge Base\n{read_file(web_ctx_path)}")

    archive_path = _config.get("archive_file", "full_archive.txt")
    if Path(archive_path).exists():
        archive_text = read_file(archive_path)
        entries = parse_archive_all(archive_text)
        success_entries = [e for e in entries if e.get("status", "").lower() == "success"]
        if success_entries:
            summary_lines = []
            for e in success_entries:
                summary_lines.append(
                    f"- Iter {e.get('iteration','?')}: {e.get('analysis_type','unknown')} — "
                    f"{e.get('hypothesis','')}"
                )
            parts.append(f"## Completed Analyses\n" + "\n".join(summary_lines))

    return "\n\n".join(parts) if parts else "No analysis has been performed yet."


def handle_chat_message(message: str, history: list) -> dict:
    """Process a chat message. Returns dict with 'text' and optional 'action'."""

    # Check if this is a command-style message
    msg_lower = message.strip().lower()

    # Direct commands
    if msg_lower.startswith("/analyze"):
        parts = message.split(maxsplit=2)
        n = 5
        description = ""
        if len(parts) > 1 and parts[1].isdigit():
            n = int(parts[1])
            if len(parts) > 2:
                description = parts[2]
        elif len(parts) > 1:
            # No number given, treat rest as description, default 1 iteration
            n = 1
            description = " ".join(parts[1:])

        if description:
            return {
                "text": f"Running {n} focused analysis iteration(s): **{description}**",
                "action": {"type": "run_adhoc", "description": description, "iterations": n},
            }
        return {
            "text": f"Starting {n} additional data analysis iterations...",
            "action": {"type": "run_analysis", "iterations": n},
        }

    if msg_lower.startswith("/research"):
        parts = message.split(maxsplit=2)
        n = 3
        description = ""
        if len(parts) > 1 and parts[1].isdigit():
            n = int(parts[1])
            if len(parts) > 2:
                description = parts[2]
        elif len(parts) > 1:
            n = 1
            description = " ".join(parts[1:])

        if description:
            return {
                "text": f"Running {n} focused web research iteration(s): **{description}**",
                "action": {"type": "run_adhoc_research", "description": description, "iterations": n},
            }
        return {
            "text": f"Starting {n} web research iterations...",
            "action": {"type": "run_research", "iterations": n},
        }

    if msg_lower == "/report" or msg_lower == "/refresh-report":
        return {
            "text": "Refreshing reports (Markdown + Deloitte HTML)...",
            "action": {"type": "refresh_reports"},
        }

    if msg_lower == "/deloitte":
        return {
            "text": "Generating Deloitte premium HTML report...",
            "action": {"type": "run_deloitte"},
        }

    if msg_lower == "/export-all":
        return {
            "text": "Exporting all iterations to Excel...",
            "action": {"type": "export_all"},
        }

    if msg_lower == "/databook":
        return {
            "text": "Generating curated databook...",
            "action": {"type": "databook"},
        }

    # Check if the user wants to trigger more analysis (heuristic)
    action_keywords = [
        "run more", "analyze more", "additional analysis", "dig deeper",
        "investigate", "look into", "research more", "correct the",
        "fix the report", "the report is wrong", "re-analyze", "reanalyze",
    ]
    wants_action = any(kw in msg_lower for kw in action_keywords)

    # Build context and ask the LLM
    context = _build_context_for_chat()

    # Build messages for multi-turn
    messages = []
    for h in history[-10:]:  # last 10 messages for context
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": message})

    system = CHAT_SYSTEM_PROMPT + f"\n\n## Current Analysis Context\n{context[:12000]}"

    if wants_action:
        system += """

The user appears to want additional analysis or corrections. After answering their question,
add a section at the end like:

---
**Suggested action:** [describe what analysis to run]
To proceed, click "Run Additional Analysis" or type a more specific request.
"""

    try:
        t0 = time.time()
        with _client.messages.stream(
            model=_model,
            max_tokens=4000,
            system=system,
            messages=messages,
        ) as stream:
            response = stream.get_final_message()
        elapsed = time.time() - t0
        logger.info(f"  [Chat] Done in {elapsed:.1f}s | tokens in={response.usage.input_tokens:,} out={response.usage.output_tokens:,}")

        text = ""
        for block in response.content:
            if block.type == "text":
                text = block.text
                break

        result = {"text": text}

        # If the user explicitly asks to run something, also process as guidance
        if wants_action:
            guidance_response = _process_guidance(message)
            if guidance_response:
                result["guidance"] = guidance_response

        return result

    except Exception as exc:
        logger.error(f"[Chat] Error: {exc}")
        return {"text": f"Sorry, I encountered an error: {str(exc)}"}


def _process_guidance(user_input: str) -> Optional[dict]:
    """Translate user feedback into an actionable guidance plan."""
    data_context_path = _config.get("active_context_file", "active_context.md")
    web_context_path = _config.get("web_research_context_file", "web_research_context.md")

    data_context = read_file(data_context_path) if Path(data_context_path).exists() else ""
    web_context = read_file(web_context_path) if Path(web_context_path).exists() else ""

    user_msg = f"""## User Feedback
{user_input}

## Current Data Analysis Knowledge Base
{data_context[:8000]}

## Current Web Research Knowledge Base
{web_context[:4000]}"""

    response = call_llm(_client, GUIDANCE_SYSTEM_PROMPT, user_msg, _model, 4000, tag="Guidance")
    return parse_json_response(response)


def _inject_guidance(context_path: str, section_header: str, guidance: str) -> None:
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


def _execute_guidance_plan(plan: dict) -> None:
    """Execute a guidance plan in a background thread."""
    data_context_path = _config.get("active_context_file", "active_context.md")
    web_context_path = _config.get("web_research_context_file", "web_research_context.md")

    if plan.get("data_analysis_needed") and plan.get("data_analysis_guidance"):
        _inject_guidance(data_context_path, "## Open Questions / Suggested Next Steps", plan["data_analysis_guidance"])

    if plan.get("web_research_needed") and plan.get("web_research_guidance"):
        _inject_guidance(web_context_path, "## Open Questions / Suggested Next Research", plan["web_research_guidance"])

    schedule = []
    if plan.get("data_analysis_needed"):
        schedule.append(("data_analysis", plan.get("data_analysis_iterations", 3)))
    if plan.get("web_research_needed"):
        schedule.append(("web_research", plan.get("web_research_iterations", 3)))
    schedule.append(("report", 0))

    _run_pipeline(schedule)


# ─── Status ───────────────────────────────────────────────────────────────────

def _get_status() -> dict:
    """Get current analysis status."""
    status = {}

    archive_path = _config.get("archive_file", "full_archive.txt")
    if Path(archive_path).exists():
        archive_text = read_file(archive_path)
        entries = parse_archive_all(archive_text)
        success = sum(1 for e in entries if e.get("status", "").lower() == "success")
        failed = sum(1 for e in entries if e.get("status", "").lower() != "success")
        total = get_current_iteration(archive_path)
        adhoc_count = sum(1 for e in entries if e.get("source", "").lower() == "adhoc")
        types = sorted(set(
            e.get("analysis_type", "unknown")
            for e in entries if e.get("status", "").lower() == "success"
        ))
        status["data_analysis"] = {
            "total": total, "success": success, "failed": failed,
            "adhoc": adhoc_count, "types": types,
        }
    else:
        status["data_analysis"] = {"total": 0, "success": 0, "failed": 0, "adhoc": 0, "types": []}

    web_archive_path = _config.get("web_research_archive_file", "web_research_archive.txt")
    status["web_research"] = {"total": get_current_iteration(web_archive_path)}

    status["reports"] = {}
    for name, path in [("markdown", "final_report.md"), ("deloitte", "deloitte_report.html")]:
        if Path(path).exists():
            stat = Path(path).stat()
            status["reports"][name] = {
                "size_kb": round(stat.st_size / 1024, 1),
                "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
            }

    graphs_dir = Path(_config.get("graphs_folder", "workspace/graphs"))
    if graphs_dir.exists():
        status["graphs"] = len(list(graphs_dir.glob("*.png")))
    else:
        status["graphs"] = 0

    exports_dir = Path("workspace/exports")
    if exports_dir.exists():
        xlsx_files = list(exports_dir.glob("*.xlsx"))
        status["exports"] = [
            {"name": f.name, "size_kb": round(f.stat().st_size / 1024, 1)}
            for f in sorted(xlsx_files)
        ]
    else:
        status["exports"] = []

    with _pipeline_lock:
        status["pipeline"] = {
            "running": _pipeline_state["running"],
            "phase": _pipeline_state["phase"],
            "progress": _pipeline_state["progress"],
        }

    return status


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    return jsonify(_get_status())


@app.route("/api/config", methods=["GET"])
def api_config_get():
    """Return the current config (non-sensitive parts)."""
    safe_keys = [
        "model", "n_iterations", "max_code_retries", "repetition_mode",
        "web_research_iterations", "data_file", "task_file",
        "orchestrator_schedule",
    ]
    return jsonify({k: _config.get(k) for k in safe_keys if k in _config})


@app.route("/api/config", methods=["POST"])
def api_config_update():
    """Update config values in memory (does not write to disk)."""
    updates = request.json
    allowed = {"n_iterations", "web_research_iterations", "model", "repetition_mode", "max_code_retries"}
    for k, v in updates.items():
        if k in allowed:
            _config[k] = v
    return jsonify({"ok": True})


@app.route("/api/schedule", methods=["POST"])
def api_schedule():
    """Start the analysis pipeline with a given schedule."""
    with _pipeline_lock:
        if _pipeline_state["running"]:
            return jsonify({"error": "Pipeline is already running"}), 409

    data = request.json or {}
    schedule_raw = data.get("schedule")

    if schedule_raw:
        schedule = []
        for phase in schedule_raw:
            schedule.append((phase.get("type", "data_analysis"), phase.get("iterations", 5)))
    else:
        # Use default from config
        schedule_config = _config.get("orchestrator_schedule", [])
        schedule = []
        for phase in schedule_config:
            if isinstance(phase, dict):
                schedule.append((phase.get("type", "data_analysis"), phase.get("iterations", 5)))
        if not schedule:
            schedule = [
                ("data_analysis", 5),
                ("web_research", 3),
                ("data_analysis", 5),
                ("report", 0),
            ]

    thread = threading.Thread(target=_run_pipeline, args=(schedule,), daemon=True)
    thread.start()
    return jsonify({"ok": True, "schedule": [{"type": t, "iterations": n} for t, n in schedule]})


@app.route("/api/cancel", methods=["POST"])
def api_cancel():
    """Cancel the running pipeline."""
    with _pipeline_lock:
        if not _pipeline_state["running"]:
            return jsonify({"error": "No pipeline is running"}), 400
        _pipeline_state["cancelled"] = True
    return jsonify({"ok": True})


@app.route("/api/run", methods=["POST"])
def api_run_phase():
    """Run a single phase (data_analysis, web_research, report, deloitte_report)."""
    with _pipeline_lock:
        if _pipeline_state["running"]:
            return jsonify({"error": "Pipeline is already running"}), 409

    data = request.json or {}
    phase_type = data.get("type", "data_analysis")
    n = data.get("iterations", 5)

    schedule = [(phase_type, n)]
    thread = threading.Thread(target=_run_pipeline, args=(schedule,), daemon=True)
    thread.start()
    return jsonify({"ok": True})


@app.route("/api/adhoc", methods=["POST"])
def api_adhoc():
    """Run an ad-hoc analysis or research with specific user guidance."""
    with _pipeline_lock:
        if _pipeline_state["running"]:
            return jsonify({"error": "Pipeline is already running"}), 409

    data = request.json or {}
    description = data.get("description", "").strip()
    mode = data.get("mode", "analysis")  # "analysis" or "research"
    n = data.get("iterations", 1)

    if not description:
        return jsonify({"error": "No description provided"}), 400

    if mode == "research":
        def _do():
            web_ctx = _config.get("web_research_context_file", "web_research_context.md")
            _inject_guidance(web_ctx, "## Open Questions / Suggested Next Research", description)
            run_web_research(n)
        thread = threading.Thread(target=_do, daemon=True)
        thread.start()
    else:
        def _do():
            data_ctx = _config.get("active_context_file", "active_context.md")
            _inject_guidance(data_ctx, "## Open Questions / Suggested Next Steps", description)
            _run_adhoc_analysis(n, description)
        thread = threading.Thread(target=_do, daemon=True)
        thread.start()

    return jsonify({"ok": True, "mode": mode, "description": description, "iterations": n})


@app.route("/api/chat", methods=["POST"])
def api_chat():
    """Handle a chat message."""
    data = request.json or {}
    message = data.get("message", "").strip()
    history = data.get("history", [])

    if not message:
        return jsonify({"error": "Empty message"}), 400

    result = handle_chat_message(message, history)

    # If there's an action, execute it in the background
    action = result.get("action")
    if action:
        action_type = action.get("type")
        with _pipeline_lock:
            if _pipeline_state["running"]:
                result["text"] += "\n\n*Note: A pipeline is already running. Please wait for it to finish.*"
            else:
                if action_type == "run_analysis":
                    schedule = [("data_analysis", action.get("iterations", 5)), ("report", 0)]
                    thread = threading.Thread(target=_run_pipeline, args=(schedule,), daemon=True)
                    thread.start()
                elif action_type == "run_research":
                    schedule = [("web_research", action.get("iterations", 3))]
                    thread = threading.Thread(target=_run_pipeline, args=(schedule,), daemon=True)
                    thread.start()
                elif action_type == "run_adhoc":
                    desc = action.get("description", "")
                    n = action.get("iterations", 1)
                    def _do_adhoc(desc=desc, n=n):
                        data_ctx_path = _config.get("active_context_file", "active_context.md")
                        _inject_guidance(
                            data_ctx_path,
                            "## Open Questions / Suggested Next Steps",
                            desc,
                        )
                        _run_adhoc_analysis(n, desc)
                    thread = threading.Thread(target=_do_adhoc, daemon=True)
                    thread.start()
                elif action_type == "run_adhoc_research":
                    desc = action.get("description", "")
                    n = action.get("iterations", 1)
                    def _do_adhoc_research(desc=desc, n=n):
                        web_ctx_path = _config.get("web_research_context_file", "web_research_context.md")
                        _inject_guidance(
                            web_ctx_path,
                            "## Open Questions / Suggested Next Research",
                            desc,
                        )
                        run_web_research(n)
                    thread = threading.Thread(target=_do_adhoc_research, daemon=True)
                    thread.start()
                elif action_type == "run_report":
                    thread = threading.Thread(target=_run_pipeline, args=([("report", 0)],), daemon=True)
                    thread.start()
                elif action_type == "refresh_reports":
                    schedule = [("report", 0), ("deloitte_report", 0)]
                    thread = threading.Thread(target=_run_pipeline, args=(schedule,), daemon=True)
                    thread.start()
                elif action_type == "run_deloitte":
                    thread = threading.Thread(target=_run_pipeline, args=([("deloitte_report", 0)],), daemon=True)
                    thread.start()
                elif action_type == "export_all":
                    def _do_export():
                        archive_path = _config.get("archive_file", "full_archive.txt")
                        context_path = _config.get("active_context_file", "active_context.md")
                        graphs_folder = _config.get("graphs_folder", "workspace/graphs")
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_path = f"workspace/exports/iterations_export_{ts}.xlsx"
                        export_iterations_to_excel(archive_path, context_path, output_path, graphs_folder)
                        _broadcast("log", {"message": f"Export complete: {output_path}", "level": "info"})
                    threading.Thread(target=_do_export, daemon=True).start()
                elif action_type == "databook":
                    def _do_databook():
                        archive_path = _config.get("archive_file", "full_archive.txt")
                        context_path = _config.get("active_context_file", "active_context.md")
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_path = f"workspace/exports/databook_{ts}.xlsx"
                        max_tokens = _config.get("databook_max_tokens", 20000)
                        generate_databook(_client, _model, archive_path, context_path, output_path, max_tokens)
                        _broadcast("log", {"message": f"Databook created: {output_path}", "level": "info"})
                    threading.Thread(target=_do_databook, daemon=True).start()

    # If there's guidance, execute it
    guidance = result.get("guidance")
    if guidance:
        with _pipeline_lock:
            if not _pipeline_state["running"]:
                result["text"] += f"\n\n*Suggested action: {guidance.get('summary', 'Additional analysis')}*"
                result["guidance_plan"] = guidance

    return jsonify(result)


@app.route("/api/execute-guidance", methods=["POST"])
def api_execute_guidance():
    """Execute a previously suggested guidance plan."""
    with _pipeline_lock:
        if _pipeline_state["running"]:
            return jsonify({"error": "Pipeline is already running"}), 409

    plan = request.json
    if not plan:
        return jsonify({"error": "No plan provided"}), 400

    thread = threading.Thread(target=_execute_guidance_plan, args=(plan,), daemon=True)
    thread.start()
    return jsonify({"ok": True, "summary": plan.get("summary", "Running additional analysis")})


@app.route("/api/events")
def api_events():
    """SSE endpoint for real-time updates."""
    q = queue.Queue(maxsize=200)
    with _event_lock:
        _event_queues.append(q)

    def stream():
        try:
            # Send initial status
            yield f"data: {json.dumps({'type': 'status', **_get_status()})}\n\n"
            while True:
                try:
                    payload = q.get(timeout=30)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        except GeneratorExit:
            pass
        finally:
            with _event_lock:
                if q in _event_queues:
                    _event_queues.remove(q)

    return Response(stream(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })


@app.route("/api/report/<report_type>")
def api_report(report_type):
    """Serve generated reports."""
    if report_type == "markdown":
        path = "final_report.md"
    elif report_type == "deloitte":
        path = _config.get("deloitte_report_output", "deloitte_report.html")
    else:
        return jsonify({"error": "Unknown report type"}), 404

    if not Path(path).exists():
        return jsonify({"error": "Report not generated yet"}), 404

    content = read_file(path)
    if report_type == "deloitte":
        return Response(content, mimetype="text/html")
    return Response(content, mimetype="text/plain")


@app.route("/api/graphs")
def api_graphs_list():
    """List available graphs."""
    graphs_dir = Path(_config.get("graphs_folder", "workspace/graphs"))
    if not graphs_dir.exists():
        return jsonify([])
    files = sorted(graphs_dir.glob("*.png"), key=lambda p: p.stat().st_mtime, reverse=True)
    return jsonify([f.name for f in files])


@app.route("/graphs/<path:filename>")
def serve_graph(filename):
    graphs_dir = Path(_config.get("graphs_folder", "workspace/graphs")).resolve()
    return send_from_directory(str(graphs_dir), filename)


@app.route("/api/context")
def api_context():
    """Return the current knowledge bases."""
    result = {}
    ctx_path = _config.get("active_context_file", "active_context.md")
    if Path(ctx_path).exists():
        result["data_analysis"] = read_file(ctx_path)
    web_ctx_path = _config.get("web_research_context_file", "web_research_context.md")
    if Path(web_ctx_path).exists():
        result["web_research"] = read_file(web_ctx_path)
    return jsonify(result)


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DDAnalyze Web UI")
    parser.add_argument("--port", type=int, default=5000, help="Port to run on")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to")
    args = parser.parse_args()

    logger.info(f"Starting DDAnalyze Web UI on http://{args.host}:{args.port}")
    logger.info(f"Model: {_model}")
    logger.info(f"Log: {log_file}")

    app.run(host=args.host, port=args.port, debug=False, threaded=True)
