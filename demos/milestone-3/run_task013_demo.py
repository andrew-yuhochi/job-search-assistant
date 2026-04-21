"""
Demo script for TASK-013: runs ScrapeRunner.run() programmatically
(no Streamlit UI) and prints a structured pipeline report.

Usage (from the project root with venv active):
    python demos/milestone-3/run_task013_demo.py

Output is written to demos/milestone-3/TASK-013-scrape.txt when redirected:
    python demos/milestone-3/run_task013_demo.py > demos/milestone-3/TASK-013-scrape.txt 2>&1
"""
from __future__ import annotations

import sys
import os

# Ensure the project root is on the path so `src` imports resolve.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import logging
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.WARNING,   # Suppress verbose debug noise in the demo output
    format="%(levelname)s %(name)s: %(message)s",
)

from src.config import settings
from src.runner.scrape_runner import ScrapeConfig, ScrapeRunner
from src.services.filter_service import FilterConfig
from src.sources import GoogleJobsSource, IndeedSource, JobSourceRegistry, LinkedInSource
from src.sources.base import SearchQuery
from src.storage.db import get_engine

SEPARATOR = "-" * 60


def main() -> None:
    # Flush immediately so header appears before any live status lines
    print(SEPARATOR, flush=True)
    print("TASK-013 DEMO — ScrapeRunner Pipeline Run", flush=True)
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}", flush=True)
    print(SEPARATOR, flush=True)

    # --- Wire components ---
    engine = get_engine()

    registry = JobSourceRegistry()
    registry.register(LinkedInSource())
    registry.register(IndeedSource())
    registry.register(GoogleJobsSource())

    # Permissive filter config for demo — show everything that comes back
    filter_config = FilterConfig(
        locations=["Vancouver", "Remote"],
        min_salary_cad=None,   # no salary floor — show all postings
        max_seniority=None,    # no seniority ceiling
        allow_remote=True,
    )

    config = ScrapeConfig(
        filter_config=filter_config,
        user_id="local",
        dedup_window_days=30,
    )

    query = SearchQuery(
        search_term="data scientist",
        location="Vancouver, BC, Canada",
        results_wanted=25,
    )

    status_log: list[str] = []

    def on_status(msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"  [{ts}] {msg}"
        status_log.append(line)
        print(line)

    runner = ScrapeRunner(
        registry=registry,
        engine=engine,
        config=config,
    )

    print("\nQuery:")
    print(f"  search_term : {query.search_term}")
    print(f"  location    : {query.location}")
    print(f"  results_wanted (per source): {query.results_wanted}")
    print(f"  hours_old   : {query.hours_old}")
    print()
    print("Pipeline stages (live):")

    try:
        result = runner.run(query, status_callback=on_status)
    except Exception as exc:
        print(f"\nFATAL: ScrapeRunner raised an exception: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # --- Structured report ---
    print()
    print(SEPARATOR)
    print("PIPELINE REPORT")
    print(SEPARATOR)
    print(f"  Run ID              : {result.run_id}")
    print(f"  Fetched (raw)       : {result.fetched}")
    print(f"  Normalized          : {result.normalized}")
    print(f"  After dedup         : {result.after_dedup}  ({result.duplicate_count} duplicates removed)")
    print(f"  After filter        : {result.after_filter}")
    print(f"  Stored (DB writes)  : {result.stored}")
    print(f"  Classification stubs: {result.classified_stub}")
    print(f"  Elapsed seconds     : {result.elapsed_seconds}")
    print()

    if result.rate_limited_sources:
        print(f"  RATE LIMITED sources: {', '.join(result.rate_limited_sources)}")
    else:
        print("  Rate limit warnings : none")

    if result.errors:
        print("  Source errors:")
        for src, err in result.errors.items():
            print(f"    {src}: {err}")
    else:
        print("  Source errors       : none")

    print(SEPARATOR)
    print("DONE")
    print(SEPARATOR)


if __name__ == "__main__":
    main()
