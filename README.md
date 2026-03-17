# DDAnalyze — Codebase Architecture & User Flow

## Overview

**DDAnalyze** is an autonomous financial due diligence analysis platform. It combines iterative data analysis, web research, and professional report generation using Claude AI. The system is designed to analyze business datasets (Excel/CSV) and produce executive-level insights in multiple formats.

---

## Project Structure

```
DDAnalyze/
├── app.py                      # Flask web UI (primary entry point)
├── orchestrator.py             # CLI pipeline orchestrator
├── Analysts.py                 # Autonomous data analysis loop
├── web_research.py             # Autonomous web research loop
├── phase2.py                   # Markdown report generator
├── deloitte_report.py          # Premium HTML report generator
├── excel_export.py             # Excel export & databook generation
├── deloitte_theme.py           # Matplotlib Deloitte color theme
├── utils.py                    # Shared utilities (LLM calls, I/O, config)
├── config.yaml                 # Central configuration
├── requirements.txt
├── templates/
│   └── index.html              # Single-page web UI
├── workspace/
│   ├── data.xlsx               # Input dataset
│   ├── graphs/                 # Generated chart PNGs
│   ├── exports/                # Excel exports
│   └── analysis_iterXXX.py    # Generated per-iteration scripts
└── docs/
    ├── task.md                 # Business context & questions (user-written)
    ├── active_context.md       # Data analysis knowledge base (auto-generated)
    ├── full_archive.txt        # Complete iteration archive (auto-generated)
    ├── web_research_context.md # Web research knowledge base (auto-generated)
    ├── web_research_archive.txt
    ├── extracted_code.md
    ├── final_report.md
    └── deloitte_report.html
```

---

## Architecture: Modules

### 1. `app.py` — Flask Web UI

The primary entry point for interactive use. Provides a single-page application with real-time log streaming via Server-Sent Events (SSE).

**Key responsibilities:**
- Serve the web UI (`GET /`)
- Manage pipeline state (running, phase, progress, cancelled)
- Launch analysis phases as subprocesses
- Stream live logs to the browser
- Handle interactive chat and guided analysis

**API Routes:**

| Method | Route | Description |
|--------|-------|-------------|
| GET | `/` | Serve web UI |
| GET | `/api/status` | Pipeline status |
| GET | `/api/config` | Get config (non-sensitive) |
| POST | `/api/config` | Update config at runtime |
| POST | `/api/schedule` | Start pipeline with schedule |
| POST | `/api/cancel` | Cancel running pipeline |
| POST | `/api/run` | Run a single phase |
| POST | `/api/adhoc` | Run ad-hoc analysis with guidance |
| POST | `/api/chat` | Interactive chat handler |
| POST | `/api/execute-guidance` | Execute a guidance plan |
| GET | `/api/events` | SSE live event stream |
| GET | `/api/report/<type>` | Serve generated reports |
| GET | `/api/graphs` | List generated graph files |
| GET | `/graphs/<filename>` | Serve graph PNG |
| GET | `/api/context` | Get knowledge bases |

**State management:** A `_pipeline_state` dictionary tracks running/phase/progress/log/cancelled, protected by a threading lock for concurrency safety.

**Launch:**
```bash
python app.py --port 5000 --host 0.0.0.0
```

---

### 2. `orchestrator.py` — CLI Pipeline Orchestrator

Provides a command-line interface for scheduling and running the full pipeline without the web UI.

**Key functions:**
- `parse_schedule()` — Convert config YAML into a list of phase tuples
- `_make_override_env()` — Build subprocess env with config overrides
- `run_data_analysis()` — Launch `Analysts.py`
- `run_web_research()` — Launch `web_research.py`
- `run_report_generation()` — Launch `phase2.py`
- `run_deloitte_report()` — Launch `deloitte_report.py`
- `main()` — Execute phases sequentially per schedule

**Default schedule:**
```
1. Data Analysis    (5 iterations)
2. Web Research     (3 iterations)
3. Data Analysis    (5 iterations)
4. Web Research     (2 iterations)
5. Data Analysis    (5 iterations)
6. Report Generation
```

**Launch:**
```bash
python orchestrator.py
python orchestrator.py --schedule '[{"type":"data_analysis","iterations":10}]'
```

---

### 3. `Analysts.py` — Autonomous Data Analysis Loop

The core analysis engine. Runs an iterative multi-agent loop where Claude acts as analyst, code runner, and critic.

**Per-iteration loop:**
```
1. Analyst   → Plans analysis based on task + knowledge base → writes Python code
2. Runner    → Executes the generated Python script
3. Retrier   → On error, asks Claude to fix the code (up to 3 attempts)
4. Critic    → Evaluates output quality and extracts key findings
5. Archive   → Logs full iteration details to full_archive.txt
6. Updater   → Adds findings to active_context.md
7. Summarizer → Every N iterations, compresses the knowledge base
```

**Agent roles:**
- **Analyst** — Plans what to analyze and generates Python code
- **Critic** — Evaluates analysis quality, extracts findings, flags dead ends
- **Summarizer** — Compresses the knowledge base to prevent bloat

**JSON response schemas:**

*Analyst:*
```json
{
  "hypothesis": "...",
  "analysis_type": "...",
  "columns_used": ["col1", "col2"],
  "code": "import pandas as pd\n..."
}
```

*Critic:*
```json
{
  "status": "success|failure",
  "quality": "high|medium|low",
  "summary": "...",
  "key_findings": ["...", "..."],
  "suggested_followup": "...",
  "error_type": null,
  "dead_ends": ["..."]
}
```

**Key config options:**
| Setting | Default | Description |
|---------|---------|-------------|
| `n_iterations` | 5 | Number of analysis iterations |
| `analyst_max_tokens` | 8192 | Token budget for analyst agent |
| `max_code_retries` | 3 | Code fix retry attempts |
| `repetition_mode` | "soft" | soft/hard/hybrid repetition control |
| `summarizer_every_n` | 5 | Compress context every N iterations |

**Output:**
- `docs/active_context.md` — Growing knowledge base
- `docs/full_archive.txt` — Complete iteration log
- `workspace/analysis_iterXXX.py` — Generated scripts
- `workspace/graphs/*.png` — Charts

---

### 4. `web_research.py` — Autonomous Web Research Loop

Complements data analysis with external market intelligence using Claude's web search tool.

**Per-iteration loop:**
```
1. Researcher  → Plans what to research based on data analysis findings
2. Searcher    → Executes web searches using Claude's web_search_20250305 tool
3. Synthesizer → Evaluates search results and extracts relevant intelligence
4. Archive     → Logs research iteration
5. Updater     → Adds findings to web_research_context.md
6. Summarizer  → Every N iterations, compresses the knowledge base
```

**Agent roles:**
- **Researcher** — Plans research topics and generates search queries
- **Synthesizer** — Evaluates results, connects to data findings, flags dead ends

**JSON response schemas:**

*Researcher:*
```json
{
  "research_topic": "...",
  "hypothesis": "...",
  "search_queries": ["query1", "query2"],
  "connection_to_data": "..."
}
```

*Synthesizer:*
```json
{
  "status": "success|failure",
  "quality": "high|medium|low",
  "summary": "...",
  "key_findings": ["..."],
  "data_connections": ["..."],
  "suggested_followup": "...",
  "dead_ends": ["..."]
}
```

**Key config options:**
| Setting | Description |
|---------|-------------|
| `web_research_iterations` | Number of research iterations |
| `web_research_researcher_max_tokens` | Researcher token budget |
| `web_research_search_max_tokens` | Search execution token budget |
| `web_research_max_tokens` | Synthesizer token budget |
| `web_research_summarizer_every_n` | Summarize every N iterations |

**Output:**
- `docs/web_research_context.md` — Web research knowledge base
- `docs/web_research_archive.txt` — Research iteration log

---

### 5. `phase2.py` — Markdown Report Generator

Transforms the accumulated knowledge bases and archives into a professional structured report.

**Four-stage pipeline:**

```
Stage 1 — Architect
  Reads knowledge bases + archive → designs report outline (JSON blueprint)

Stage 2 — Section Writers (parallel per section)
  For each section: writes detailed markdown narrative with tables and insights

Stage 2b — Executive Summary Writer
  Writes a 1-page summary with 5–7 cross-referenced bullet points

Stage 2c — Glossary Writer
  Compiles definitions for all abbreviations and technical terms

Stage 3 — Assembler
  Stitches all sections into final_report.md
```

**Key config options:**
| Setting | Description |
|---------|-------------|
| `phase2_architect_max_tokens` | Outline generation budget |
| `phase2_section_max_tokens` | Per-section writing budget |
| `phase2_summary_max_tokens` | Executive summary budget |
| `phase2_glossary_max_tokens` | Glossary generation budget |

**Output:**
- `docs/extracted_code.md` — All successful analysis code extracted from archive
- `final_report.md` (in workspace root) — Final structured report

---

### 6. `deloitte_report.py` — Premium HTML Report Generator

Converts the markdown report and raw findings into a polished, self-contained HTML file with Chart.js visualizations.

**Pipeline:**
```
1. Planner   → Designs HTML structure and chart specifications
2. Sections  → Generates HTML content per section
3. Assembler → Builds the final self-contained HTML file
```

**Brand colors:**
| Name | Hex |
|------|-----|
| Green Medium (primary) | `#26890D` |
| Green Dark (headers) | `#046A38` |
| Grey (secondary) | `#404040` |
| Electric Blue (accent) | `#0D8390` |
| Aqua (accent) | `#00ABAB` |

**Features:** Embedded Chart.js charts, responsive layout, print-ready styling, no external dependencies.

**Output:** `docs/deloitte_report.html`

---

### 7. `excel_export.py` — Excel Export & Databook Generator

Exports analysis results to professional Excel workbooks.

**Two modes:**

1. **`export_iterations_to_excel()`** — Quick multi-sheet dump of all iterations with summary table and archive entries.

2. **`generate_databooks()`** — Professional per-iteration databooks with raw data, output, charts, Excel formulas, and Deloitte styling.

**Styling:** Dark green headers, alternating row colors, auto-width columns, frozen panes.

**Output:**
- `workspace/exports/iterations_export_TIMESTAMP.xlsx`
- `workspace/exports/databook_TIMESTAMP.xlsx`

---

### 8. `utils.py` — Shared Utilities

Central utility module used by all other modules.

**Categories:**

*File I/O:*
- `read_file(path)` — Read file contents
- `write_file(path, content)` — Write file (creates parent dirs)
- `append_file(path, content)` — Append to file

*Configuration:*
- `load_config()` — Load `config.yaml` and merge with env-based overrides (`DDANALYZE_CONFIG_OVERRIDES`)

*Logging:*
- `setup_logging()` — Configure rotating console + file logging

*LLM calls:*
- `call_llm(client, model, system, messages, max_tokens)` — Call Claude, return text
- `call_llm_with_tokens(...)` — Call Claude, return text + token counts
- `parse_json_response(text)` — Robustly extract JSON from LLM response (handles markdown fences)
- `create_client()` — Create Anthropic API client

*Archive parsing:*
- `parse_archive_all(path)` — Parse archive, return all entries
- `parse_archive_success(path)` — Return only successful entries
- `get_current_iteration(path)` — Get highest iteration number

---

### 9. `deloitte_theme.py` — Matplotlib Color Theme

Applies consistent Deloitte visual identity to all matplotlib charts generated during analysis.

**Functions:**
- `apply_deloitte_style()` — Apply theme globally to matplotlib
- `style_title(ax)` — Style axis titles
- `deloitte_colors()` — Return ordered color palette list

Analysis scripts import this at the top to ensure all charts use the same palette:
```python
import deloitte_theme
deloitte_theme.apply_deloitte_style()
```

---

## Data Flow

```
┌─────────────────────────────────────────────┐
│  User Inputs                                │
│  • workspace/data.xlsx  (business dataset)  │
│  • docs/task.md         (business context)  │
└──────────────────────┬──────────────────────┘
                       │
          ┌────────────┴────────────┐
          │                         │
     app.py (Web UI)      orchestrator.py (CLI)
          │                         │
          └────────────┬────────────┘
                       │
        ┌──────────────┼──────────────┐
        │                             │
  Analysts.py                 web_research.py
  (Data Analysis)             (Web Research)
        │                             │
        │  Multi-agent loop:          │  Multi-agent loop:
        │  Analyst → Code → Critic    │  Researcher → Search → Synthesizer
        │  → Archive → Context        │  → Archive → Context
        │                             │
  docs/active_context.md      docs/web_research_context.md
  docs/full_archive.txt       docs/web_research_archive.txt
  workspace/graphs/*.png
  workspace/exports/
        │                             │
        └──────────────┬──────────────┘
                       │
          ┌────────────┴────────────┐
          │                         │
      phase2.py              deloitte_report.py
    (MD Report)              (HTML Report)
          │                         │
  final_report.md         docs/deloitte_report.html
  docs/extracted_code.md
          │
    excel_export.py
          │
  workspace/exports/*.xlsx
```

---

## Knowledge Base Structure

### `docs/active_context.md` (Data Analysis)
```markdown
# Active Knowledge Base
## Overarching Goal
## Established Facts
## Analysis Index           ← table of all completed iterations
## What Has Been Tried
## Dead Ends & Closed Paths
## Generated Graphs         ← table of chart files with descriptions
## Open Questions / Suggested Next Steps
```

### `docs/web_research_context.md` (Web Research)
```markdown
# Web Research Knowledge Base
## Overarching Goal
## Key Intelligence
## Research Index            ← table of all research iterations
## What Has Been Researched
## Cross-References to Data Analysis
## Open Questions / Suggested Next Research
## Dead Ends & Closed Paths
```

---

## Archive Format

```
================================================================================
ITERATION: 1
DATE: 2026-03-17 10:30:45
STATUS: success
SOURCE: scheduled
ANALYSIS TYPE: Revenue Trends
HYPOTHESIS: Revenue shows year-over-year growth...
COLUMNS USED: Fecha_Mes, Ventas_Netas, Canal
────────────────────────────────────────────────────────────────────────────────
CODE:
[generated Python code]
────────────────────────────────────────────────────────────────────────────────
OUTPUT:
[analysis output / printed tables / chart paths]
────────────────────────────────────────────────────────────────────────────────
EVALUATION:
Quality: high
Summary: [business-friendly summary]
Key findings:
  - Finding 1
  - Finding 2
Suggested followup: [next investigation]
Confirmed dead ends:
  - Dead end 1
================================================================================
```

---

## Configuration (`config.yaml`)

| Setting | Description |
|---------|-------------|
| `model` | Claude model for data analysis |
| `web_research_model` | Claude model for web research |
| `n_iterations` | Data analysis iterations per run |
| `web_research_iterations` | Web research iterations per run |
| `analyst_max_tokens` | Analyst agent token budget |
| `max_tokens` | Default token budget |
| `max_code_retries` | Retry attempts for failed code |
| `repetition_mode` | `soft` / `hard` / `hybrid` |
| `summarizer_every_n` | Context compression frequency |
| `fresh_start` | If true, clear knowledge bases before run |
| `data_file` | Path to input dataset |
| `graphs_folder` | Output directory for charts |
| `active_context_file` | Data analysis KB path |
| `archive_file` | Archive file path |
| `web_research_context_file` | Web research KB path |
| `orchestrator_schedule` | List of phases with iteration counts |
| `deloitte_report_output` | HTML report output path |

**Runtime config override via environment variable:**
```python
# Set before launching subprocess:
env["DDANALYZE_CONFIG_OVERRIDES"] = json.dumps({
    "n_iterations": 5,
    "fresh_start": False,
    "analysis_source": "adhoc",
})
```

---

## User Flow

### Interactive (Web UI)

```
1. python app.py --port 5000
2. Open http://localhost:5000
3. Left panel: Configure schedule → Start Pipeline
4. Monitor live logs in real time via SSE stream
5. Center panel: Chat with findings:
   - /analyze  → run additional data analysis
   - /research → run web research
   - /report   → generate markdown report
   - /databook → generate Excel databook
   - /export-all → export everything
   - Free text → guided analysis or Q&A
6. Right panel: Browse graphs, download reports, export files
```

### Automated (CLI)

```bash
# Full default pipeline
python orchestrator.py

# Custom schedule
python orchestrator.py --schedule '[
  {"type":"data_analysis","iterations":5},
  {"type":"web_research","iterations":3},
  {"type":"data_analysis","iterations":5},
  {"type":"report"}
]'

# Individual phases
python Analysts.py
python web_research.py
python phase2.py
python deloitte_report.py
```

---

## Key Design Patterns

### 1. Modular Subprocess Architecture
Each phase runs as an independent subprocess. This isolates failures, allows individual phases to be run standalone, and enables concurrent configuration injection via environment variables.

### 2. Evolving Knowledge Bases
Knowledge bases (`active_context.md`, `web_research_context.md`) grow with each iteration. Periodic summarization (every N iterations) compresses them to prevent context window overflow while retaining key insights.

### 3. Complete Audit Trail
The archive files (`full_archive.txt`, `web_research_archive.txt`) preserve the full history of every iteration — including code, output, and evaluation — enabling report generation to reconstruct the entire analytical journey.

### 4. Multi-Agent Collaboration
Each phase uses specialized agents with distinct system prompts:
- **Analyst/Researcher** — Plans and executes
- **Critic/Synthesizer** — Evaluates quality and extracts findings
- **Summarizer** — Compresses knowledge
- **Architect/Writers/Assembler** — Generate final reports

### 5. Guided Refinement
The web UI's chat interface converts free-text user feedback into structured analysis tasks, enabling interactive refinement of the automated pipeline without manual code changes.

### 6. Professional Output
All output — charts (Deloitte matplotlib theme), HTML reports (Chart.js, brand colors, responsive layout), Excel workbooks (styled headers, zebra rows) — follows professional consulting visual standards.

---

## Dependencies

```
anthropic       # Claude API client
python-dotenv   # .env support
pyyaml          # YAML config parsing
pandas          # Data manipulation
openpyxl        # Excel generation
matplotlib      # Charting
seaborn         # Statistical visualization
flask           # Web UI
```

---

## Output Artifacts Summary

| Artifact | Location | Generated By |
|----------|----------|--------------|
| Data analysis knowledge base | `docs/active_context.md` | `Analysts.py` |
| Data analysis archive | `docs/full_archive.txt` | `Analysts.py` |
| Web research knowledge base | `docs/web_research_context.md` | `web_research.py` |
| Web research archive | `docs/web_research_archive.txt` | `web_research.py` |
| Per-iteration analysis scripts | `workspace/analysis_iterXXX.py` | `Analysts.py` |
| Charts | `workspace/graphs/*.png` | Analysis scripts |
| Extracted analysis code | `docs/extracted_code.md` | `phase2.py` |
| Markdown report | `final_report.md` | `phase2.py` |
| Premium HTML report | `docs/deloitte_report.html` | `deloitte_report.py` |
| Excel bulk export | `workspace/exports/iterations_export_*.xlsx` | `excel_export.py` |
| Excel databooks | `workspace/exports/databook_*.xlsx` | `excel_export.py` |
