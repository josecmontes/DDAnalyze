#!/usr/bin/env python3
"""
DDAnalyze Orchestrator
Master controller that coordinates the full due diligence pipeline:

  1. Scheduled phases of data analysis (Analysts.py) and web research (web_research.py)
  2. Final markdown report generation (phase2.py)
  3. Deloitte premium HTML report (deloitte_report.py)

Usage:
  python orchestrator.py              # Run the full pipeline
  python orchestrator.py --schedule '[{"type":"data_analysis","iterations":10}]'
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

os.chdir(Path(__file__).parent)

from utils import (
    read_file, load_config, setup_logging, get_current_iteration,
)

logger = logging.getLogger("ddanalyze.orchestrator")


# ─── Phase Runners ────────────────────────────────────────────────────────────


def _make_override_env(overrides: dict) -> dict:
    env = os.environ.copy()
    env["DDANALYZE_CONFIG_OVERRIDES"] = json.dumps(overrides)
    return env


def run_data_analysis(n_iterations: int) -> bool:
    logger.info(f"\n{'─' * 60}")
    logger.info(f"PHASE: DATA ANALYSIS — {n_iterations} iterations")
    logger.info(f"{'─' * 60}")

    env = _make_override_env({
        "n_iterations": n_iterations,
        "fresh_start": False,
        "summarize_on_start": False,
    })

    try:
        result = subprocess.run(
            [sys.executable, "Analysts.py"],
            timeout=n_iterations * 300,
            env=env,
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


def run_web_research(n_iterations: int) -> bool:
    logger.info(f"\n{'─' * 60}")
    logger.info(f"PHASE: WEB RESEARCH — {n_iterations} iterations")
    logger.info(f"{'─' * 60}")

    env = _make_override_env({
        "web_research_iterations": n_iterations,
        "web_research_fresh_start": False,
    })

    try:
        result = subprocess.run(
            [sys.executable, "web_research.py"],
            timeout=n_iterations * 300,
            env=env,
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


def run_report_generation() -> bool:
    logger.info(f"\n{'─' * 60}")
    logger.info("PHASE: REPORT GENERATION")
    logger.info(f"{'─' * 60}")

    try:
        result = subprocess.run(
            [sys.executable, "phase2.py"],
            timeout=600,
        )
        success = result.returncode == 0
        if success:
            logger.info("[Report] Generated final_report.md")
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
    logger.info(f"\n{'─' * 60}")
    logger.info("PHASE: DELOITTE PREMIUM REPORT")
    logger.info(f"{'─' * 60}")

    try:
        result = subprocess.run(
            [sys.executable, "deloitte_report.py"],
            timeout=900,
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


# ─── Schedule Parsing ─────────────────────────────────────────────────────────

DEFAULT_SCHEDULE = [
    ("data_analysis", 5),
    ("web_research", 3),
    ("data_analysis", 5),
    ("web_research", 2),
    ("data_analysis", 5),
    ("report", 0),
]


def parse_schedule(schedule_config) -> list:
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


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="DDAnalyze Orchestrator — coordinate analysis, research, and reporting"
    )
    parser.add_argument(
        "--schedule", type=str, default=None,
        help="Custom schedule as JSON, e.g. "
             '\'[{"type":"data_analysis","iterations":10},{"type":"report"}]\'',
    )
    args = parser.parse_args()

    config = load_config()
    model = config.get("model", "claude-sonnet-4-6")
    debug_logging = config.get("debug_logging", False)

    log_file = setup_logging("orchestrator", debug=debug_logging)

    logger.info(f"\n{'═' * 60}")
    logger.info("DDAnalyze ORCHESTRATOR")
    logger.info(f"Model  : {model}")
    logger.info(f"Log    : {log_file}")
    logger.info(f"{'═' * 60}")

    # Parse and execute schedule
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
        if phase_type in ("report", "deloitte_report"):
            logger.info(f"  {i}. {phase_type.replace('_', ' ').title()}")
        else:
            logger.info(f"  {i}. {phase_type.replace('_', ' ').title()} — {n} iterations")
    logger.info("")

    for phase_type, n_iterations in schedule:
        if phase_type == "data_analysis":
            run_data_analysis(n_iterations)
        elif phase_type == "web_research":
            run_web_research(n_iterations)
        elif phase_type == "report":
            run_report_generation()
        elif phase_type == "deloitte_report":
            run_deloitte_report()
        else:
            logger.warning(f"Unknown phase type: {phase_type}")

    logger.info(f"\n{'═' * 60}")
    logger.info("ORCHESTRATOR COMPLETE")
    logger.info(f"{'═' * 60}")


if __name__ == "__main__":
    main()
