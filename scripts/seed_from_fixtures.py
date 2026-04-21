"""
seed_from_fixtures.py — Load fixture postings into SQLite for Milestone 2 DB-mode validation.

Reads tests/fixtures/jobs_fixtures.json, runs each posting through
Normalizer → DedupService → repository.insert_job / insert_duplicate,
then inserts a stub Classification for every job.

Idempotent: calls repository.delete_all_jobs() before inserting so re-runs
produce the same counts. Per TASK-009 requirements.

Usage:
    python scripts/seed_from_fixtures.py
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure project root is on sys.path so `src.*` imports resolve regardless
# of the working directory the script is invoked from.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.models import RawJobPosting
from src.processing.normalizer import Normalizer
from src.services.dedup import DedupService
from src.storage import repository
from src.storage.db import get_engine

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("seed_from_fixtures")

# ---------------------------------------------------------------------------
# Stub classification constants
# ---------------------------------------------------------------------------

STUB_MODEL = "stub"
STUB_PROMPT_VERSION = "stub-v0"
STUB_REASONING = "stub — replaced in Milestone 4"

# Map expected_specialty → display specialty name (matches specialty_types.name seed data)
SPECIALTY_NAME_MAP: dict[str, str] = {
    "data_scientist": "Data Scientist",
    "ml_engineer":    "ML Engineer",
    "data_engineer":  "Data Engineer",
    "data_analyst":   "Data Analyst",
    "unclassified":   "Unclassified",
}


def _load_fixtures() -> list[dict]:
    """Load raw fixture dicts from tests/fixtures/jobs_fixtures.json."""
    fixtures_path = PROJECT_ROOT / "tests" / "fixtures" / "jobs_fixtures.json"
    logger.info("Loading fixtures from %s", fixtures_path)
    with fixtures_path.open(encoding="utf-8") as f:
        return json.load(f)


def seed(user_id: str = "local") -> None:
    """
    Main seeding procedure.

    1. Wipes existing jobs (+ cascade) for the user.
    2. Normalises each fixture posting.
    3. Runs DedupService against already-inserted canonical jobs.
    4. Inserts job + classification (stub) for every posting.
       Duplicates are inserted to jobs with duplicate_of set, and a row
       goes to the duplicates table; they receive no stub classification
       (mirrors Milestone 4 pipeline intent: only classify canonical posts).
    """
    engine = get_engine()
    normalizer = Normalizer()
    dedup_service = DedupService()

    # ------------------------------------------------------------------
    # Step 1 — wipe
    # ------------------------------------------------------------------
    deleted = repository.delete_all_jobs(engine, user_id=user_id)
    logger.info("Deleted %d existing jobs for user_id='%s'", deleted, user_id)

    # ------------------------------------------------------------------
    # Step 2 — load + parse fixtures
    # ------------------------------------------------------------------
    raw_dicts = _load_fixtures()
    logger.info("Loaded %d fixture entries", len(raw_dicts))

    raws: list[RawJobPosting] = []
    for d in raw_dicts:
        try:
            raws.append(RawJobPosting(**d))
        except Exception as exc:
            logger.error("Skipping malformed fixture entry id=%s: %s", d.get("id"), exc)

    # ------------------------------------------------------------------
    # Step 3 — normalize + dedup + insert
    # ------------------------------------------------------------------
    job_count = 0
    dup_count = 0
    classification_count = 0
    # Running list of canonical jobs seen so far (DedupService window)
    canonical_jobs = []

    for i, raw in enumerate(raws, start=1):
        # Normalize
        try:
            job = normalizer.normalize(raw)
        except Exception as exc:
            logger.error(
                "Normalizer error on fixture id=%s title=%r: %s",
                raw.id, raw.title, exc,
            )
            continue

        logger.debug("Normalized [%02d/%02d] %s — %s", i, len(raws), job.job_id[:12], raw.title)

        # Dedup check against already-inserted canonical postings
        dedup_result = dedup_service.check(job, canonical_jobs)

        if dedup_result.is_duplicate:
            logger.info(
                "  DUPLICATE detected: %r at %r → canonical %s (match_type=%s, score=%s)",
                raw.title, raw.company,
                dedup_result.canonical_job_id[:12] if dedup_result.canonical_job_id else "?",
                dedup_result.match_type,
                dedup_result.match_score,
            )
            # Insert the duplicate posting to jobs (with duplicate_of set) and a duplicates row
            repository.insert_job(engine, job)
            repository.insert_duplicate(
                engine,
                duplicate_job_id=job.job_id,
                canonical_job_id=dedup_result.canonical_job_id,
                match_type=dedup_result.match_type,
                match_score=dedup_result.match_score,
            )
            dup_count += 1
        else:
            # Canonical posting — insert + track for future dedup checks
            repository.insert_job(engine, job)
            canonical_jobs.append(job)
            job_count += 1
            logger.info(
                "  Inserted canonical [%02d] %r at %r",
                job_count, raw.title, raw.company,
            )

            # Stub classification for canonical postings only
            raw_specialty = (raw.expected_specialty or "unclassified").lower()
            specialty_name = SPECIALTY_NAME_MAP.get(raw_specialty, "Unclassified")
            repository.insert_classification(
                engine,
                job_id=job.job_id,
                user_id=user_id,
                specialty_name=specialty_name,
                confidence="low",
                duty_signals=[STUB_REASONING],
                model_name=STUB_MODEL,
                prompt_version=STUB_PROMPT_VERSION,
            )
            classification_count += 1
            logger.debug("    Classification: specialty=%s", specialty_name)

    # ------------------------------------------------------------------
    # Step 4 — summary
    # ------------------------------------------------------------------
    logger.info(
        "Seeding complete: canonical_jobs=%d, duplicates=%d, classifications=%d",
        job_count, dup_count, classification_count,
    )

    # Sanity assertion: canonical + dup should equal total fixtures processed
    total_inserted = job_count + dup_count
    if total_inserted != len(raws):
        logger.warning(
            "Mismatch: processed %d fixtures but inserted %d rows "
            "(check for normalizer errors above)",
            len(raws), total_inserted,
        )


if __name__ == "__main__":
    seed()
