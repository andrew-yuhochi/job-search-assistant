#!/usr/bin/env python
"""
Replay post-processing pipeline from a saved 01_fetch_raw.json file.

Usage:
    python scripts/replay_pipeline.py logs/run_1/01_fetch_raw.json

Runs all post-processing stages in order:
  2. normalize
  3. hard filter
  4. title filter
  5. cross-source dedup (within-batch)
  6. cross-run dedup (against DB)
  7. enrich (salary + seniority, survivors only)
  8. store

against the saved raw postings. Useful for tuning filter parameters without
re-fetching from the internet.

The DB is NOT cleared before replay — the script creates a new scrape_run row
so results are distinguishable from the original run.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s  %(name)s  %(message)s",
)
logger = logging.getLogger("replay_pipeline")

# ---------------------------------------------------------------------------
# Project imports (after bootstrap)
# ---------------------------------------------------------------------------

from src.models.models import RawJobPosting, SourceName  # noqa: E402
from src.runner.scrape_runner import ScrapeRunner, ScrapeConfig  # noqa: E402
from src.services.filter_service import FilterConfig  # noqa: E402
from src.storage.db import get_engine  # noqa: E402


def load_raw_postings(json_path: Path) -> list[RawJobPosting]:
    """
    Load a saved 01_fetch_raw.json and reconstruct RawJobPosting objects.

    The source field is stored as a string value (e.g. 'linkedin') and must
    be converted back to the SourceName enum.  Any unknown source name that
    is not in the enum is skipped with a warning.

    Returns a list of RawJobPosting objects.
    """
    with open(json_path, encoding="utf-8") as fh:
        records: list[dict] = json.load(fh)

    postings: list[RawJobPosting] = []
    skipped = 0
    for rec in records:
        try:
            source_str = rec.get("source", "")
            try:
                source = SourceName(source_str)
            except ValueError:
                logger.warning(
                    "replay_pipeline: unknown source %r — skipping posting id=%s",
                    source_str,
                    rec.get("id", "?"),
                )
                skipped += 1
                continue

            posting = RawJobPosting(
                id=rec["id"],
                title=rec["title"],
                company=rec["company"],
                location=rec["location"],
                source=source,
                url=rec["url"],
                description=rec.get("description") or "",
                salary_raw=rec.get("salary_raw"),
                salary_min_raw=rec.get("salary_min_raw"),
                salary_max_raw=rec.get("salary_max_raw"),
                salary_currency=rec.get("salary_currency"),
                salary_interval=rec.get("salary_interval"),
                posted_date=rec.get("posted_date") or "",
                search_term=rec.get("search_term"),
            )
            postings.append(posting)
        except Exception as exc:
            logger.warning(
                "replay_pipeline: failed to reconstruct posting id=%s: %s",
                rec.get("id", "?"),
                exc,
            )
            skipped += 1

    if skipped:
        print(f"  [WARN] Skipped {skipped} records due to reconstruction errors")

    return postings


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/replay_pipeline.py <path/to/01_fetch_raw.json>")
        sys.exit(1)

    json_path = Path(sys.argv[1])
    if not json_path.exists():
        print(f"Error: file not found: {json_path}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 1. Load raw postings
    # ------------------------------------------------------------------
    print(f"\nLoaded raw postings from: {json_path}")
    raw_postings = load_raw_postings(json_path)
    print(f"  {len(raw_postings)} postings reconstructed")

    # ------------------------------------------------------------------
    # 2. Initialize DB
    # ------------------------------------------------------------------
    print("\nInitialising database...")
    engine = get_engine()
    print("  DB ready.")

    # ------------------------------------------------------------------
    # 3. Run replay pipeline
    # ------------------------------------------------------------------
    print()
    source_run_dir = json_path.parent  # the directory containing 01_fetch_raw.json
    config = ScrapeConfig(filter_config=FilterConfig())
    runner = ScrapeRunner(registry=None, engine=engine, config=config)  # type: ignore[arg-type]

    result = runner.run_from_raw(
        raw_postings,
        status_callback=lambda msg: print(f"[PIPELINE] {msg}"),
        source_run_dir=source_run_dir,
    )

    # ------------------------------------------------------------------
    # 4. Summary
    # ------------------------------------------------------------------
    dup_removed = result.duplicate_count

    print()
    print("=" * 40)
    print("REPLAY PIPELINE SUMMARY")
    print("=" * 40)
    print(f"Source run:             {source_run_dir / '01_fetch_raw.json'}")
    print(f"Output run:             {result.run_dir}/")
    print(f"Loaded (raw):           {result.fetched}")
    print(f"After normalize:        {result.normalized}")
    print(f"After hard filter:      (see 03_hard_filter.json)")
    print(f"After title filter:     (see 04_title_filter.json)")
    print(f"After cross-src dedup:  (see 05_cross_source_dedup.json)")
    print(f"After cross-run dedup:  {result.after_dedup}  ({dup_removed} total duplicates removed)")
    print(f"Stored to DB:           {result.stored}")
    print(f"Stage logs written to:  {result.run_dir}/")
    print("=" * 40)
    print()

    if result.errors:
        print("Errors:")
        for src, err in result.errors.items():
            print(f"  {src}: {err}")



if __name__ == "__main__":
    main()
