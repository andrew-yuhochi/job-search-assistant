# Job Search Assistant

A local Streamlit dashboard that automates job search workflows for Data Science and ML roles. Scrapes LinkedIn, Indeed, and Google Jobs; classifies postings by specialty using Claude Haiku; and drafts CV highlights grounded in your knowledge bank using the Anthropic Citations API.

> Built with [Claude Code](https://claude.ai/code)

## Features

- **Three-source scraping**: LinkedIn, Indeed, and Google Jobs via SerpAPI — parallel fetch with per-source error isolation
- **Duty-based classification**: Claude Haiku 4.5 classifies each posting into your configured specialty types (Data Scientist, ML Engineer, Data Engineer, Data Analyst, or custom)
- **Knowledge bank drafting**: Upload your work history as `.txt`, `.md`, or `.docx`; the Citations API drafts 3-5 grounded CV highlight bullets per posting
- **Hard filters**: Location, salary floor, seniority, company size — unknown values pass through with badges, never silently dropped
- **Deduplication**: Exact-URL and fuzzy title+company matching (token_sort_ratio ≥ 90) across a 30-day window
- **State machine**: New → Reviewed → Applied | Dismissed, with dwell-time and override-rate telemetry
- **Signals dashboard**: Weekly override-rate chart to validate the classifier's commercial premise

## Quick Start

```bash
# 1. Clone and enter the project
git clone https://github.com/andrew-yuhochi/job-search-assistant.git
cd job-search-assistant

# 2. Create a virtual environment and install dependencies
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 3. Copy and fill in your environment variables
cp .env.example .env
# Edit .env with your ANTHROPIC_API_KEY and SERPAPI_API_KEY

# 4. Launch the dashboard
streamlit run src/app.py
```

## Project Structure

```
projects/job-search-assistant/
├── src/
│   ├── app.py              # Streamlit entry point + sidebar
│   ├── models/             # Pydantic domain models
│   ├── services/           # Business logic (dedup, filter, signal, classifier, draft)
│   ├── sources/            # Job source plugin registry (LinkedIn, Indeed, Google)
│   ├── llm/                # LLM client abstraction (Anthropic implementation)
│   ├── processing/         # Normalizer, salary extractor, seniority inferrer, KB chunker
│   ├── pages/              # Streamlit multipage pages
│   └── storage/            # SQLite schema, engine, repository layer
├── config/
│   ├── prompts/            # Editable prompt files (no code restart needed)
│   ├── filter_defaults.yaml
│   └── classifier_types.yaml
├── tests/
│   └── fixtures/           # Fixture JSON and markdown for prototype mode
├── scripts/                # CLI scripts (seed_from_fixtures, etc.)
├── data/                   # Local SQLite DB (gitignored)
└── demos/                  # Demo artifacts per milestone
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | Anthropic API key for Claude Haiku classification and drafting |
| `SERPAPI_API_KEY` | SerpAPI key for Google Jobs scraping |
| `LOG_LEVEL` | Logging level (`DEBUG`, `INFO`, `WARNING`); default `INFO` |
| `DATABASE_PATH` | Path to SQLite DB file; default `data/app.db` |

## Phase

PoC — core engine validation. No auth, no billing, no deployment. Local single-user only.
