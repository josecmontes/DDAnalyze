"""
DDAnalyze — Shared Utilities

Common functions used across modules: file I/O, logging setup, LLM calls,
JSON parsing, and archive parsing.
"""

import json
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
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

# ─── File Utilities ───────────────────────────────────────────────────────────


def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_file(path: str, content: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def append_file(path: str, content: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(content)


# ─── Config Utilities ────────────────────────────────────────────────────────


def load_config() -> dict:
    """Load config.yaml and apply any orchestrator overrides from env."""
    config = yaml.safe_load(read_file("config.yaml"))
    env_overrides = os.environ.get("DDANALYZE_CONFIG_OVERRIDES")
    if env_overrides:
        config.update(json.loads(env_overrides))
    return config


# ─── Logging Setup ────────────────────────────────────────────────────────────


def setup_logging(module_name: str, debug: bool = False, log_dir: str = "logs") -> Path:
    """
    Configure logging to console + timestamped file.
    Returns the path of the created log file.

    Only adds handlers if the root logger has none, preventing duplicates
    when called from subprocesses.
    """
    Path(log_dir).mkdir(exist_ok=True)
    log_file = Path(log_dir) / f"{module_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
    logger = logging.getLogger("ddanalyze")
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


def call_llm_with_tokens(
    client: anthropic.Anthropic,
    system: str,
    user: str,
    model: str,
    max_tokens: int,
    tag: str = "LLM",
) -> tuple[str, int, int]:
    """Call Claude and return (text, input_tokens, output_tokens)."""
    logger = logging.getLogger("ddanalyze")
    logger.debug(
        f"[{tag}] Sending request | system={len(system):,}ch "
        f"user={len(user):,}ch max_tokens={max_tokens}"
    )
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
    logger.info(
        f"  [{tag}] Done in {elapsed:.1f}s | tokens in={in_tok:,} out={out_tok:,}"
    )

    for block in response.content:
        if block.type == "text":
            return block.text, in_tok, out_tok
    return "", in_tok, out_tok


def parse_json_response(text: str, tag: str = "JSON") -> Optional[dict]:
    """Robustly extract a JSON object from an LLM response."""
    logger = logging.getLogger("ddanalyze")
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

    preview = text[:300].replace("\n", " ")
    logger.warning(f"[{tag}] All JSON parse attempts failed. Response preview: {preview!r}")
    return None


# ─── Archive Utilities ────────────────────────────────────────────────────────

_INTERNAL_ERROR_LABELS = {
    "JSON_PARSE_ERROR", "CRITIC_JSON_PARSE_ERROR", "TIMEOUT", "FATAL_ERROR",
    "SYNTH_JSON_PARSE_ERROR",
}


def parse_archive_all(archive_text: str) -> list:
    """Parse full_archive.txt into list of dicts for ALL non-internal-error entries."""
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
            ("source", r"SOURCE:\s*(.+)"),
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


def parse_archive_success(archive_text: str) -> list:
    """Parse full_archive.txt into list of dicts for SUCCESS entries only."""
    all_entries = parse_archive_all(archive_text)
    return [e for e in all_entries if e.get("status", "").lower() == "success"]


def get_current_iteration(archive_path: str) -> int:
    """Determine the highest iteration number already in the archive."""
    if not Path(archive_path).exists():
        return 0
    text = read_file(archive_path)
    iters = re.findall(r"ITERATION:\s*(\d+)", text)
    return max(int(i) for i in iters) if iters else 0


def create_client() -> anthropic.Anthropic:
    """Create an Anthropic client using the API key from env."""
    return anthropic.Anthropic(api_key=API_KEY)


# ─── Data Registry & Catalog ────────────────────────────────────────────────


def build_data_registry(data_folder: str) -> dict:
    """Walk original/ and processed/ under data_folder, read metadata, merge with existing registry."""
    import pandas as pd

    registry_path = os.path.join(data_folder, "data_registry.json")
    existing = {}
    if Path(registry_path).exists():
        try:
            existing = json.loads(read_file(registry_path))
        except (json.JSONDecodeError, OSError):
            existing = {}

    registry = {}
    for subfolder in ("original", "processed"):
        folder = os.path.join(data_folder, subfolder)
        if not Path(folder).exists():
            continue
        for fpath in sorted(Path(folder).rglob("*")):
            if fpath.is_dir():
                continue
            rel = f"{subfolder}/{fpath.name}"
            entry = existing.get(rel, {})
            entry["type"] = subfolder
            ext = fpath.suffix.lower().lstrip(".")
            entry["format"] = ext
            try:
                if ext in ("xlsx", "xls"):
                    xf = pd.ExcelFile(str(fpath))
                    entry["sheets"] = xf.sheet_names
                    df_head = pd.read_excel(xf, sheet_name=xf.sheet_names[0], nrows=5)
                    entry["columns"] = list(df_head.columns)
                    df_full = pd.read_excel(xf, sheet_name=xf.sheet_names[0])
                    entry["rows"] = len(df_full)
                    xf.close()
                elif ext == "csv":
                    df_head = pd.read_csv(str(fpath), nrows=5)
                    entry["columns"] = list(df_head.columns)
                    # Count rows without loading entire file
                    with open(str(fpath), "r") as f:
                        entry["rows"] = sum(1 for _ in f) - 1  # subtract header
                elif ext == "parquet":
                    df = pd.read_parquet(str(fpath))
                    entry["columns"] = list(df.columns)
                    entry["rows"] = len(df)
                else:
                    entry.setdefault("description", f"Unsupported format: {ext}")
            except Exception as e:
                entry["error"] = str(e)[:200]
            if "description" not in entry:
                if subfolder == "original":
                    entry["description"] = "Auto-scanned. Columns from first sheet." if ext in ("xlsx", "xls") else "Auto-scanned."
                else:
                    entry.setdefault("description", "")
            registry[rel] = entry

    # Preserve agent-provided descriptions from existing registry for processed files
    for key, old_entry in existing.items():
        if key in registry and old_entry.get("description"):
            if not registry[key].get("description"):
                registry[key]["description"] = old_entry["description"]
        if key in registry and old_entry.get("created_by"):
            registry[key].setdefault("created_by", old_entry["created_by"])

    write_file(registry_path, json.dumps(registry, indent=2, ensure_ascii=False))
    return registry


def format_data_catalog(registry: dict, data_folder: str) -> str:
    """Return a markdown catalog of all available data files for injection into the analyst prompt."""
    MAX_COLS_SHOWN = 10

    def _fmt_cols(cols):
        if not cols:
            return ""
        if len(cols) <= MAX_COLS_SHOWN:
            return ", ".join(str(c) for c in cols)
        shown = ", ".join(str(c) for c in cols[:MAX_COLS_SHOWN])
        return f"{shown}, ... and {len(cols) - MAX_COLS_SHOWN} more"

    originals = {k: v for k, v in registry.items() if v.get("type") == "original"}
    processed = {k: v for k, v in registry.items() if v.get("type") == "processed"}

    lines = ["## Available Data"]

    if originals:
        lines.append("\n### Original Files")
        lines.append("| File | Format | Sheets | Rows | Columns |")
        lines.append("|------|--------|--------|------|---------|")
        for fname, meta in originals.items():
            fmt = meta.get("format", "?")
            sheets = ", ".join(meta["sheets"]) if "sheets" in meta else ""
            rows = f"{meta['rows']:,}" if "rows" in meta else "?"
            cols = _fmt_cols(meta.get("columns", []))
            lines.append(f"| {fname} | {fmt} | {sheets} | {rows} | {cols} |")

    if processed:
        lines.append("\n### Processed Files (created by previous analysts)")
        lines.append("| File | Rows | Created By | Description |")
        lines.append("|------|------|------------|-------------|")
        for fname, meta in processed.items():
            rows = f"{meta['rows']:,}" if "rows" in meta else "?"
            created_by = meta.get("created_by", "")
            desc = meta.get("description", "")
            lines.append(f"| {fname} | {rows} | {created_by} | {desc} |")

    lines.append(f"""
### How to load data
- Excel: `df = pd.read_excel("{data_folder}/original/filename.xlsx")`
- CSV: `df = pd.read_csv("{data_folder}/original/filename.csv")`
- Parquet: `df = pd.read_parquet("{data_folder}/processed/filename.parquet")`

You are free to use any combination of files. Processed files are a convenience, not a requirement.

### How to save processed data
    df.to_parquet("{data_folder}/processed/name.parquet", index=False)
    print("DATA_SAVED: processed/name.parquet — Short description")""")

    return "\n".join(lines)
