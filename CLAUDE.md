# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EU Horizon research project mapping Turkey's Digital (Dijital Dönüşüm) and Green (Yeşil Dönüşüm) transformation skill requirements by scraping Turkish job boards and aligning extracted skills to the ESCO v1.2 taxonomy.

## Running the Pipeline

```bash
# Step 1 — Scrape LinkedIn jobs
python linkedin_scraper.py [--url URL] [--pages N] [--output FILE] [--wait N]

# Step 2 — Extract ESCO skills semantically
python esco_extractor.py

# Step 3 — Analysis & publication-ready visualizations
python analysis_pipeline.py
# Outputs: 13 PNG charts + CSV analysis tables to outputs/analysis/
```

## Running Tests

```bash
pytest test_selenium_scraper.py
```

## Environment Configuration

Copy `.env.example` to `.env`:
- `SEARCH_KEYWORDS` — job search terms (default: "Software Engineer")
- `PAGE_COUNT` — pages to scrape (default: ALL)
- `MAX_JOBS` — cap on jobs collected (default: 0 = unlimited)
- `CHROME_BINARY_PATH` — path to Chrome executable

## Architecture

4-stage pipeline with Pydantic v2 data contracts between each stage:

**Stage 1 — Scraping** (`linkedin_scraper.py`)
- Selenium-based LinkedIn scraper with multi-selector XPath/CSS strategies
- Anti-bot: realistic User-Agents, random delays, headless=False
- Output: `data/*.xlsx` with a `Full Description` column

**Stage 2 — Skill Extraction** (`src/processing/skill_extractor.py`)
- Strips Kariyer.net UI noise, segments descriptions into job-about vs. candidate-criteria
- Extracts skill tokens via regex patterns and NLP
- Output: `data/skill_inventory.xlsx` (Sheet 1: human-readable; Sheet 2: ESCO-ready JSON arrays)

**Stage 3 — ESCO Alignment** (`esco_extractor.py`)
- Semantic extraction via `esco-skill-extractor` (sentence-transformers / all-MiniLM-L6-v2)
- Returns ESCO URIs per description; cross-references against digital/green sub-collection CSVs
- Output: `outputs/linkedin_esco_skills.xlsx` with columns: all_skills, digital_skills, green_skills
- Semantic extraction is the only active ESCO path in this repo

**Stage 4 — Gap Analysis** (`src/analyzer/gap_analyzer.py`)
- Aggregates SkillMatch corpus into `SkillDemandMetric` and `MarketSnapshot`
- Builds co-occurrence matrices + NetworkX graphs for H-TURF forecasting
- `src/analyzer/turkish_concepts.py` applies regex + fuzzy matching for Turkish-specific digital/green transformation concepts

**Data Models** (`src/utils/models.py`): `JobListing → JobDetail → ExtractedSkill → ESCOSkill → SkillMatch → SkillDemandMetric → MarketSnapshot`


## Key Data Files

| File | Contents |
|------|----------|
| `data/*.xlsx` | LinkedIn scraped data (Full Description column) |
| `data/skill_inventory.xlsx` | Extracted skills (human + ESCO JSON) |
| `data/esco/skills_en.csv` | Full ESCO v1.2 taxonomy (8.9 MB) |
| `data/esco/digitalSkillsCollection_en.csv` | ESCO digital skills subset (1284 skills) |
| `data/esco/greenSkillsCollection_en.csv` | ESCO green skills subset (629 skills) |
| `outputs/linkedin_esco_tagged_v4.xlsx` | Input for esco_extractor.py (980 rows, word-boundary tagged) |
| `outputs/linkedin_esco_skills.xlsx` | Final output: Title + Full Description + 3 skill columns |
| `outputs/*.png` | Skill frequency visualizations |

## ESCO Extractor Notes

`esco_extractor.py` uses **semantic matching** via `esco-skill-extractor` (sentence-transformers). It returns ESCO URIs, which are then classified by cross-referencing with `digitalSkillsCollection_en.csv` and `greenSkillsCollection_en.csv`. This is the active ESCO path used in the pipeline.
