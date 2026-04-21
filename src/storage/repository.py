# Repository layer: all SQL access for the Job Search Assistant.
# Every function uses parameterised queries — no string concatenation into SQL.
# Per TDD §2.4 and TASK-008 requirements. This module is the only place that
# touches the database; services and pages import from here, never raw SQLAlchemy.
#
# NOTE: This file is bootstrapped in TASK-007 with the methods required by
# DedupService (insert_job, insert_duplicate, list_jobs_for_dedup).
# Full CRUD coverage (≥20 functions, ≥12 tests) is completed in TASK-008.

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.models.models import (
    JobPosting,
    JobState,
    SeniorityLevel,
    SourceName,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Job helpers
# ---------------------------------------------------------------------------


def _row_to_job_posting(row) -> JobPosting:
    """Convert a SQLAlchemy Row into a JobPosting Pydantic model."""
    d = dict(row._mapping)
    # Convert stored strings back to enum types
    d["source"] = SourceName(d["source"])
    d["state"] = JobState(d["state"])
    d["seniority"] = SeniorityLevel(d["seniority"]) if d.get("seniority") else SeniorityLevel.unknown
    # Parse timestamp strings if they are strings
    for ts_field in ("fetched_at", "state_updated_at", "posted_at", "url_health_checked_at"):
        val = d.get(ts_field)
        if isinstance(val, str):
            try:
                d[ts_field] = datetime.fromisoformat(val)
            except ValueError:
                d[ts_field] = None
    return JobPosting(**{k: v for k, v in d.items() if k in JobPosting.model_fields})


# ---------------------------------------------------------------------------
# insert_job
# ---------------------------------------------------------------------------


def insert_job(engine: Engine, job: JobPosting) -> None:
    """
    Insert a JobPosting into the jobs table.
    Uses INSERT OR IGNORE so re-inserting the same job_id is a no-op (idempotent).

    Args:
        engine: SQLAlchemy Engine connected to the SQLite database.
        job: The normalized JobPosting to persist.
    """
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO jobs (
                    job_id, user_id, source, source_job_id, url, url_hostname,
                    title, title_normalized, company, company_normalized,
                    location, is_remote, posted_at, description,
                    salary_min_cad, salary_max_cad, salary_source,
                    seniority, company_employees_label, company_size_bucket,
                    duplicate_of, fetched_at, url_health_checked_at,
                    url_is_dead, state, state_updated_at
                ) VALUES (
                    :job_id, :user_id, :source, :source_job_id, :url, :url_hostname,
                    :title, :title_normalized, :company, :company_normalized,
                    :location, :is_remote, :posted_at, :description,
                    :salary_min_cad, :salary_max_cad, :salary_source,
                    :seniority, :company_employees_label, :company_size_bucket,
                    :duplicate_of, :fetched_at, :url_health_checked_at,
                    :url_is_dead, :state, :state_updated_at
                )
                """
            ),
            {
                "job_id": job.job_id,
                "user_id": job.user_id,
                "source": job.source.value,
                "source_job_id": job.source_job_id,
                "url": job.url,
                "url_hostname": job.url_hostname,
                "title": job.title,
                "title_normalized": job.title_normalized,
                "company": job.company,
                "company_normalized": job.company_normalized,
                "location": job.location,
                "is_remote": job.is_remote,
                "posted_at": job.posted_at.isoformat() if job.posted_at else None,
                "description": job.description,
                "salary_min_cad": job.salary_min_cad,
                "salary_max_cad": job.salary_max_cad,
                "salary_source": job.salary_source,
                "seniority": job.seniority.value if job.seniority else "unknown",
                "company_employees_label": job.company_employees_label,
                "company_size_bucket": job.company_size_bucket,
                "duplicate_of": job.duplicate_of,
                "fetched_at": job.fetched_at.isoformat(),
                "url_health_checked_at": (
                    job.url_health_checked_at.isoformat()
                    if job.url_health_checked_at
                    else None
                ),
                "url_is_dead": job.url_is_dead,
                "state": job.state.value,
                "state_updated_at": job.state_updated_at.isoformat(),
            },
        )
        conn.commit()
    logger.debug("insert_job: persisted job_id=%s", job.job_id)


# ---------------------------------------------------------------------------
# update_job_duplicate_of
# ---------------------------------------------------------------------------


def update_job_duplicate_of(
    engine: Engine, job_id: str, canonical_job_id: str
) -> None:
    """
    Set jobs.duplicate_of on a posting that was found to be a duplicate.

    Args:
        engine: SQLAlchemy Engine.
        job_id: The duplicate posting's job_id.
        canonical_job_id: The first-seen (canonical) posting's job_id.
    """
    with engine.connect() as conn:
        conn.execute(
            text(
                "UPDATE jobs SET duplicate_of = :canonical WHERE job_id = :job_id"
            ),
            {"canonical": canonical_job_id, "job_id": job_id},
        )
        conn.commit()
    logger.debug(
        "update_job_duplicate_of: %s → canonical %s", job_id, canonical_job_id
    )


# ---------------------------------------------------------------------------
# insert_duplicate
# ---------------------------------------------------------------------------


def insert_duplicate(
    engine: Engine,
    duplicate_job_id: str,
    canonical_job_id: str,
    match_type: str,
    match_score: Optional[float],
) -> None:
    """
    Insert a row into the duplicates table recording how a duplicate was detected.
    Also sets jobs.duplicate_of on the duplicate posting.

    Args:
        engine: SQLAlchemy Engine.
        duplicate_job_id: The job_id of the posting identified as a duplicate.
        canonical_job_id: The job_id of the first-seen (canonical) posting.
        match_type: 'url_exact' | 'fuzzy_title_company' | 'description_similarity'.
        match_score: Ratio / Jaccard score (None for url_exact).
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO duplicates
                    (duplicate_post_id, canonical_post_id, match_type, match_score, detected_at)
                VALUES
                    (:dup_id, :can_id, :match_type, :match_score, :detected_at)
                """
            ),
            {
                "dup_id": duplicate_job_id,
                "can_id": canonical_job_id,
                "match_type": match_type,
                "match_score": match_score,
                "detected_at": now,
            },
        )
        conn.execute(
            text(
                "UPDATE jobs SET duplicate_of = :canonical WHERE job_id = :job_id"
            ),
            {"canonical": canonical_job_id, "job_id": duplicate_job_id},
        )
        conn.commit()
    logger.debug(
        "insert_duplicate: dup=%s canonical=%s match_type=%s score=%s",
        duplicate_job_id,
        canonical_job_id,
        match_type,
        match_score,
    )


# ---------------------------------------------------------------------------
# list_jobs_for_dedup
# ---------------------------------------------------------------------------


def list_jobs_for_dedup(
    engine: Engine,
    user_id: str,
    within_days: int = 30,
) -> list[JobPosting]:
    """
    Return all canonical (non-duplicate) job postings for a user fetched within
    the last `within_days` days.  DedupService passes these as existing_jobs.

    Args:
        engine: SQLAlchemy Engine.
        user_id: The user whose postings to retrieve.
        within_days: Window size in days (default 30 per TDD §2.3).

    Returns:
        List of JobPosting objects, ordered by fetched_at DESC.
    """
    from datetime import timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(days=within_days)).isoformat()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM jobs
                WHERE user_id = :user_id
                  AND duplicate_of IS NULL
                  AND fetched_at >= :cutoff
                ORDER BY fetched_at DESC
                """
            ),
            {"user_id": user_id, "cutoff": cutoff},
        ).fetchall()
    jobs = []
    for row in rows:
        try:
            jobs.append(_row_to_job_posting(row))
        except Exception as exc:
            logger.warning(
                "list_jobs_for_dedup: skipping malformed row — %s", exc
            )
    return jobs


# ---------------------------------------------------------------------------
# get_job
# ---------------------------------------------------------------------------


def get_job(engine: Engine, job_id: str) -> Optional[JobPosting]:
    """
    Fetch a single job by its primary key.

    Args:
        engine: SQLAlchemy Engine.
        job_id: The sha256-derived job_id.

    Returns:
        JobPosting if found, None otherwise.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM jobs WHERE job_id = :job_id"),
            {"job_id": job_id},
        ).fetchone()
    if row is None:
        return None
    return _row_to_job_posting(row)
