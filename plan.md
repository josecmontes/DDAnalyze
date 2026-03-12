# Refactor Plan: Iterative Report Generation (Phase 2 — Step B)

## Problem with Current Approach

The current `phase2.py` Step B sends **everything** — full context + all analysis entries — in a single LLM call and asks it to simultaneously organize, select, and write the entire report. This creates:

1. **Cognitive overload** — the model juggles structure AND prose in one pass
2. **Shallow coverage** — with 20K output tokens and 30+ analyses, detail gets sacrificed
3. **No separation of concerns** — organizing data and writing prose are fundamentally different tasks
4. **Wasted context** — when writing about revenue trends, the model still has customer cohort data occupying attention

## Proposed Architecture: 4-Step Pipeline

### Step B.1 — The Architect (Generate Outline & Mapping)

**What**: Send full context + truncated archive to the LLM with ONE goal: produce a structured JSON outline.

**LLM Output** (JSON):
```json
{
  "report_title": "Due Diligence Analysis — [Company Name]",
  "sections": [
    {
      "section_number": 1,
      "title": "Revenue Growth & Trajectory",
      "description": "Year-by-year revenue evolution, growth drivers, CAGR analysis",
      "iterations": [1, 2, 5],
      "graphs": ["revenue_trend.png", "yoy_growth.png"],
      "guidance": "Lead with FY21→LTM23 trajectory, decompose into volume vs price, include CAGR table"
    },
    {
      "section_number": 2,
      "title": "Product Portfolio & Lifecycle Dynamics",
      "iterations": [3, 4, 6, 7],
      "graphs": ["lifecycle_segmentation.png"],
      "guidance": "Cover model count, lifecycle velocity, NE dependency, breakout models"
    }
  ],
  "executive_summary_guidance": "Emphasize the 96% YoY growth, NE dependency risk, top-20 concentration"
}
```

**System prompt**: Focused architect prompt — "You are organizing data, not writing prose. Output valid JSON only."

**Key benefit**: The LLM sees all the data but only has to make organizational decisions — which analyses go where, what order, what emphasis. Much simpler cognitive task.

### Step B.2 — The Filter (Python-Level Sorting)

**What**: Pure Python — no LLM call. Parse the JSON outline and prepare per-section payloads.

```python
for section in outline["sections"]:
    section_entries = [e for e in entries if int(e["iteration"]) in section["iterations"]]
    section_graphs = [g for g in available_graphs if g[1] in section.get("graphs", [])]
    # Build a focused prompt with only these entries + section guidance
```

**Key benefit**: Each section writer gets a clean, minimal payload — only the 2-4 analyses it needs, not all 30.

### Step B.3 — The Writer (Iterative Drafting)

**What**: Loop through the outline. For each section, spin up a **fresh LLM call** with:
- The **overarching context** (active_context.md — always included for grounding)
- Only the **specific analyses** mapped to this section (from B.2)
- The **section guidance** from the Architect
- The **available graphs** for this section
- Information about **previous sections** already written (titles only, to avoid repetition)

**System prompt**: Section-writer prompt — "You are writing ONE section of a business report. Be detailed and thorough. You have been given only the analyses relevant to your section."

**Key benefits**:
- Each call has a **focused context** → higher quality writing
- Each section gets the **full output token budget** → much more detail
- Sections are written **independently** → can be parallelized if needed
- The LLM can give **deep attention** to 2-4 analyses instead of skimming 30

### Step B.4 — The Assembler (Python Stitching)

**What**: Pure Python. Stitch all generated sections together into `final_report.md`.

```python
final_parts = [f"# {outline['report_title']}\n"]
for section, section_md in zip(outline["sections"], section_markdowns):
    final_parts.append(section_md)
final_parts.append(executive_summary_md)  # Written as the last LLM call
write_file("final_report.md", "\n\n".join(final_parts))
```

**Executive Summary**: Written as the **final** LLM call, after all sections exist. This call receives:
- All section titles + key tables (not full text, to stay focused)
- The Architect's executive summary guidance
- The active_context.md established facts

This ensures the summary accurately reflects what was actually written, not what was planned.

## Changes to `phase2.py`

### New Functions
| Function | Purpose |
|----------|---------|
| `build_architect_prompt()` | Build the prompt for B.1 (outline generation) |
| `parse_outline_json()` | Parse + validate the JSON outline from B.1 |
| `filter_entries_for_section()` | B.2 — select entries matching a section's iteration list |
| `build_section_prompt()` | Build focused prompt for one section (B.3) |
| `build_executive_summary_prompt()` | Build prompt for the final exec summary call |
| `generate_section()` | Single LLM call for one section (streaming) |
| `assemble_report()` | B.4 — stitch sections + summary into final markdown |

### New Prompts
| Prompt | Purpose |
|--------|---------|
| `ARCHITECT_SYSTEM_PROMPT` | Instruct LLM to output JSON outline only |
| `SECTION_WRITER_SYSTEM_PROMPT` | Instruct LLM to write one detailed section |
| `EXEC_SUMMARY_SYSTEM_PROMPT` | Instruct LLM to write executive summary from completed sections |

### Modified Functions
| Function | Change |
|----------|--------|
| `main()` | Replace single LLM call with B.1→B.4 pipeline |
| `build_report_prompt()` | Kept but renamed to `_build_full_prompt_legacy()` as fallback |

### Config Additions (`config.yaml`)
```yaml
# Phase 2 report generation
phase2_model: claude-sonnet-4-6          # model for report generation
phase2_architect_max_tokens: 4000        # B.1 outline (JSON, compact)
phase2_section_max_tokens: 8000          # B.3 per-section budget
phase2_summary_max_tokens: 4000          # exec summary budget
```

## Token Economics

**Current**: 1 call × 20K output tokens = 20K tokens for entire report

**Proposed** (assuming 6 sections):
- B.1: 1 call × 4K = 4K (outline)
- B.3: 6 calls × 8K = 48K (sections)
- B.3: 1 call × 4K = 4K (exec summary)
- **Total: 56K output tokens** → ~2.8× more writing budget

Input tokens increase modestly since each section call includes active_context but only a subset of analyses.

## Error Handling

- **B.1 JSON parse failure**: Retry once with explicit "output valid JSON" reinforcement. If still fails, fall back to legacy single-call approach.
- **B.3 section failure**: Log warning, skip section, note in final report. Don't abort entire pipeline.
- **B.4 assembly**: Always produces output even if some sections failed.

## Files Changed

Only `phase2.py` is modified. No changes to `loop.py`, `deloitte_theme.py`, or `config.yaml` structure (just new optional keys).
