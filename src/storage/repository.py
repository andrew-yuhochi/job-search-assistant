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


# ---------------------------------------------------------------------------
# list_jobs
# ---------------------------------------------------------------------------


def list_jobs(
    engine: Engine,
    user_id: str = "local",
    state: Optional[str] = None,
    specialty_filter: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> list[JobPosting]:
    """
    Return jobs for a user ordered by recency DESC, salary_min_cad DESC NULLS LAST.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope (default 'local').
        state: If provided, filter to this state; 'new' implicitly excludes 'dismissed'.
               Pass None to return all non-dismissed posts.
        specialty_filter: If provided, join classifications and filter by specialty_name.
        limit: Maximum number of rows to return.
        offset: Row offset for pagination.

    Returns:
        List of JobPosting objects.
    """
    conditions = ["j.user_id = :user_id"]
    params: dict = {"user_id": user_id, "limit": limit, "offset": offset}

    if state is not None:
        conditions.append("j.state = :state")
        params["state"] = state
    else:
        # Default: exclude dismissed unless caller explicitly requests state='dismissed'
        conditions.append("j.state != 'dismissed'")

    where_clause = " AND ".join(conditions)

    if specialty_filter:
        params["specialty_filter"] = specialty_filter
        sql = f"""
            SELECT j.* FROM jobs j
            JOIN classifications c ON c.job_id = j.job_id
            WHERE {where_clause}
              AND c.specialty_name = :specialty_filter
            ORDER BY j.posted_at DESC NULLS LAST,
                     j.salary_min_cad DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """
    else:
        sql = f"""
            SELECT j.* FROM jobs j
            WHERE {where_clause}
            ORDER BY j.posted_at DESC NULLS LAST,
                     j.salary_min_cad DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    jobs = []
    for row in rows:
        try:
            jobs.append(_row_to_job_posting(row))
        except Exception as exc:
            logger.warning("list_jobs: skipping malformed row — %s", exc)
    return jobs


# ---------------------------------------------------------------------------
# update_job_state
# ---------------------------------------------------------------------------


def update_job_state(engine: Engine, job_id: str, new_state: str) -> None:
    """
    Update the lifecycle state of a job posting and refresh state_updated_at.

    Args:
        engine: SQLAlchemy Engine.
        job_id: The job to update.
        new_state: One of 'new' | 'reviewed' | 'applied' | 'dismissed'.
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                "UPDATE jobs SET state = :state, state_updated_at = :now "
                "WHERE job_id = :job_id"
            ),
            {"state": new_state, "now": now, "job_id": job_id},
        )
        conn.commit()
    logger.debug("update_job_state: job_id=%s → %s", job_id, new_state)


# ---------------------------------------------------------------------------
# delete_all_jobs
# ---------------------------------------------------------------------------


def delete_all_jobs(engine: Engine, user_id: str = "local") -> int:
    """
    Delete all jobs (and cascade-delete duplicates/classifications) for a user.

    Returns the number of rows deleted from the jobs table.
    Used by seed_from_fixtures.py for idempotent reseeding.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope (default 'local').
    """
    with engine.connect() as conn:
        # Remove dependent rows first (FK constraints, no cascade defined in schema)
        conn.execute(
            text(
                "DELETE FROM duplicates WHERE duplicate_post_id IN "
                "(SELECT job_id FROM jobs WHERE user_id = :user_id) "
                "OR canonical_post_id IN "
                "(SELECT job_id FROM jobs WHERE user_id = :user_id)"
            ),
            {"user_id": user_id},
        )
        conn.execute(
            text(
                "DELETE FROM classifications WHERE user_id = :user_id"
            ),
            {"user_id": user_id},
        )
        conn.execute(
            text(
                "DELETE FROM highlight_drafts WHERE user_id = :user_id"
            ),
            {"user_id": user_id},
        )
        conn.execute(
            text(
                "DELETE FROM signal_events WHERE user_id = :user_id AND job_id IN "
                "(SELECT job_id FROM jobs WHERE user_id = :user_id)"
            ),
            {"user_id": user_id},
        )
        result = conn.execute(
            text("DELETE FROM jobs WHERE user_id = :user_id"),
            {"user_id": user_id},
        )
        conn.commit()
    deleted = result.rowcount
    logger.debug("delete_all_jobs: removed %d rows for user_id=%s", deleted, user_id)
    return deleted


# ---------------------------------------------------------------------------
# list_duplicates
# ---------------------------------------------------------------------------


def list_duplicates(engine: Engine, canonical_job_id: str) -> list[dict]:
    """
    Return all duplicate records pointing to a given canonical job.

    Args:
        engine: SQLAlchemy Engine.
        canonical_job_id: The canonical job's job_id.

    Returns:
        List of dicts with keys: duplicate_id, duplicate_post_id, canonical_post_id,
        match_type, match_score, detected_at.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT * FROM duplicates WHERE canonical_post_id = :can_id "
                "ORDER BY detected_at DESC"
            ),
            {"can_id": canonical_job_id},
        ).fetchall()
    return [dict(row._mapping) for row in rows]


# ---------------------------------------------------------------------------
# Classifications
# ---------------------------------------------------------------------------


def insert_classification(
    engine: Engine,
    job_id: str,
    user_id: str,
    specialty_name: str,
    confidence: str,
    duty_signals: list[str],
    model_name: str,
    prompt_version: str,
) -> None:
    """
    Insert a classification row; INSERT OR IGNORE so re-inserting is a no-op.

    Args:
        engine: SQLAlchemy Engine.
        job_id: FK → jobs.job_id.
        user_id: User scope.
        specialty_name: Matched specialty or 'Unclassified'.
        confidence: 'high' | 'medium' | 'low'.
        duty_signals: Verbatim phrases that drove the classification.
        model_name: LLM model name used.
        prompt_version: Prompt version string.
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO classifications
                    (job_id, user_id, specialty_name, confidence,
                     duty_signals, model_name, prompt_version, classified_at)
                VALUES
                    (:job_id, :user_id, :specialty_name, :confidence,
                     :duty_signals, :model_name, :prompt_version, :classified_at)
                """
            ),
            {
                "job_id": job_id,
                "user_id": user_id,
                "specialty_name": specialty_name,
                "confidence": confidence,
                "duty_signals": json.dumps(duty_signals),
                "model_name": model_name,
                "prompt_version": prompt_version,
                "classified_at": now,
            },
        )
        conn.commit()
    logger.debug(
        "insert_classification: job_id=%s specialty=%s confidence=%s",
        job_id, specialty_name, confidence,
    )


def get_classification(engine: Engine, job_id: str) -> Optional[dict]:
    """
    Fetch the classification row for a given job, or None if not classified.

    Args:
        engine: SQLAlchemy Engine.
        job_id: FK → jobs.job_id.

    Returns:
        Dict with classification fields, or None.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM classifications WHERE job_id = :job_id"),
            {"job_id": job_id},
        ).fetchone()
    if row is None:
        return None
    d = dict(row._mapping)
    if isinstance(d.get("duty_signals"), str):
        try:
            d["duty_signals"] = json.loads(d["duty_signals"])
        except (ValueError, TypeError):
            pass
    return d


def upsert_classification(
    engine: Engine,
    job_id: str,
    user_id: str,
    specialty_name: str,
    confidence: str,
    duty_signals: list[str],
    model_name: str,
    prompt_version: str,
) -> None:
    """
    Insert or replace the classification for a job (one classification per job at PoC).

    Args:
        engine: SQLAlchemy Engine.
        job_id: FK → jobs.job_id.
        user_id: User scope.
        specialty_name: Matched specialty or 'Unclassified'.
        confidence: 'high' | 'medium' | 'low'.
        duty_signals: Verbatim phrases that drove the classification.
        model_name: LLM model name used.
        prompt_version: Prompt version string.
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO classifications
                    (job_id, user_id, specialty_name, confidence,
                     duty_signals, model_name, prompt_version, classified_at)
                VALUES
                    (:job_id, :user_id, :specialty_name, :confidence,
                     :duty_signals, :model_name, :prompt_version, :classified_at)
                ON CONFLICT(job_id) DO UPDATE SET
                    specialty_name  = excluded.specialty_name,
                    confidence      = excluded.confidence,
                    duty_signals    = excluded.duty_signals,
                    model_name      = excluded.model_name,
                    prompt_version  = excluded.prompt_version,
                    classified_at   = excluded.classified_at
                """
            ),
            {
                "job_id": job_id,
                "user_id": user_id,
                "specialty_name": specialty_name,
                "confidence": confidence,
                "duty_signals": json.dumps(duty_signals),
                "model_name": model_name,
                "prompt_version": prompt_version,
                "classified_at": now,
            },
        )
        conn.commit()
    logger.debug(
        "upsert_classification: job_id=%s specialty=%s", job_id, specialty_name
    )


# ---------------------------------------------------------------------------
# Knowledge bank documents
# ---------------------------------------------------------------------------


def insert_kb_document(
    engine: Engine,
    user_id: str,
    filename: str,
    full_text: str,
    word_count: int,
) -> int:
    """
    Insert a knowledge bank document and return its new kb_doc_id.

    Marks all prior documents for this user as inactive (is_active=0) so
    only the newest upload is active.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope.
        filename: Original filename of the uploaded document.
        full_text: Parsed full text of the document.
        word_count: Token/word count for quick display.

    Returns:
        The newly assigned kb_doc_id (INTEGER PRIMARY KEY).
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        # Deactivate previous documents
        conn.execute(
            text(
                "UPDATE knowledge_bank_documents SET is_active = 0 "
                "WHERE user_id = :user_id"
            ),
            {"user_id": user_id},
        )
        conn.execute(
            text(
                """
                INSERT INTO knowledge_bank_documents
                    (user_id, filename, full_text, word_count, uploaded_at, is_active)
                VALUES
                    (:user_id, :filename, :full_text, :word_count, :uploaded_at, 1)
                """
            ),
            {
                "user_id": user_id,
                "filename": filename,
                "full_text": full_text,
                "word_count": word_count,
                "uploaded_at": now,
            },
        )
        row = conn.execute(text("SELECT last_insert_rowid()")).fetchone()
        conn.commit()
    kb_doc_id = row[0]
    logger.debug(
        "insert_kb_document: kb_doc_id=%d filename=%s", kb_doc_id, filename
    )
    return kb_doc_id


def get_kb_document(engine: Engine, kb_doc_id: int) -> Optional[dict]:
    """
    Fetch a knowledge bank document by its primary key.

    Args:
        engine: SQLAlchemy Engine.
        kb_doc_id: The integer primary key.

    Returns:
        Dict with document fields, or None if not found.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT * FROM knowledge_bank_documents WHERE kb_doc_id = :kb_doc_id"
            ),
            {"kb_doc_id": kb_doc_id},
        ).fetchone()
    return dict(row._mapping) if row is not None else None


def list_kb_documents(engine: Engine, user_id: str = "local") -> list[dict]:
    """
    Return all knowledge bank documents for a user, newest first.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope.

    Returns:
        List of dicts with document fields.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT * FROM knowledge_bank_documents "
                "WHERE user_id = :user_id ORDER BY uploaded_at DESC"
            ),
            {"user_id": user_id},
        ).fetchall()
    return [dict(row._mapping) for row in rows]


def delete_kb_document(engine: Engine, kb_doc_id: int) -> bool:
    """
    Delete a knowledge bank document and its associated chunks.

    Args:
        engine: SQLAlchemy Engine.
        kb_doc_id: The integer primary key.

    Returns:
        True if a row was deleted, False if kb_doc_id was not found.
    """
    with engine.connect() as conn:
        conn.execute(
            text(
                "DELETE FROM knowledge_bank_chunks WHERE kb_doc_id = :kb_doc_id"
            ),
            {"kb_doc_id": kb_doc_id},
        )
        result = conn.execute(
            text(
                "DELETE FROM knowledge_bank_documents WHERE kb_doc_id = :kb_doc_id"
            ),
            {"kb_doc_id": kb_doc_id},
        )
        conn.commit()
    logger.debug("delete_kb_document: kb_doc_id=%d rows_deleted=%d", kb_doc_id, result.rowcount)
    return result.rowcount > 0


# ---------------------------------------------------------------------------
# Highlight drafts
# ---------------------------------------------------------------------------


def insert_highlight_draft(
    engine: Engine,
    job_id: str,
    user_id: str,
    bullets_json: str,
    model_name: str,
    prompt_version: str,
    persisted_reason: str = "applied",
) -> int:
    """
    Insert a highlight draft and return its new draft_id.

    Args:
        engine: SQLAlchemy Engine.
        job_id: FK → jobs.job_id.
        user_id: User scope.
        bullets_json: JSON-serialised list of bullet strings.
        model_name: LLM model name used.
        prompt_version: Prompt version string.
        persisted_reason: 'applied' | 'manual_save'.

    Returns:
        The newly assigned draft_id.
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO highlight_drafts
                    (job_id, user_id, bullets_json, model_name,
                     prompt_version, generated_at, persisted_reason)
                VALUES
                    (:job_id, :user_id, :bullets_json, :model_name,
                     :prompt_version, :generated_at, :persisted_reason)
                """
            ),
            {
                "job_id": job_id,
                "user_id": user_id,
                "bullets_json": bullets_json,
                "model_name": model_name,
                "prompt_version": prompt_version,
                "generated_at": now,
                "persisted_reason": persisted_reason,
            },
        )
        row = conn.execute(text("SELECT last_insert_rowid()")).fetchone()
        conn.commit()
    draft_id = row[0]
    logger.debug("insert_highlight_draft: draft_id=%d job_id=%s", draft_id, job_id)
    return draft_id


def get_highlight_draft(engine: Engine, job_id: str) -> Optional[dict]:
    """
    Fetch the most recent highlight draft for a job, or None if none exists.

    Args:
        engine: SQLAlchemy Engine.
        job_id: FK → jobs.job_id.

    Returns:
        Dict with draft fields (bullets_json as raw string), or None.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT * FROM highlight_drafts WHERE job_id = :job_id "
                "ORDER BY generated_at DESC LIMIT 1"
            ),
            {"job_id": job_id},
        ).fetchone()
    return dict(row._mapping) if row is not None else None


def upsert_highlight_draft(
    engine: Engine,
    job_id: str,
    user_id: str,
    bullets_json: str,
    model_name: str,
    prompt_version: str,
    persisted_reason: str = "manual_save",
) -> int:
    """
    Delete any existing highlight draft for the job and insert a fresh one.

    Args:
        engine: SQLAlchemy Engine.
        job_id: FK → jobs.job_id.
        user_id: User scope.
        bullets_json: JSON-serialised list of bullet strings.
        model_name: LLM model name used.
        prompt_version: Prompt version string.
        persisted_reason: 'applied' | 'manual_save'.

    Returns:
        The newly assigned draft_id.
    """
    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM highlight_drafts WHERE job_id = :job_id"),
            {"job_id": job_id},
        )
        conn.commit()
    return insert_highlight_draft(
        engine, job_id, user_id, bullets_json, model_name, prompt_version, persisted_reason
    )


# ---------------------------------------------------------------------------
# Signal events
# ---------------------------------------------------------------------------


def insert_signal(
    engine: Engine,
    user_id: str,
    event_type: str,
    job_id: Optional[str] = None,
    from_state: Optional[str] = None,
    to_state: Optional[str] = None,
    specialty_name: Optional[str] = None,
    classification_confidence: Optional[str] = None,
    dwell_ms: Optional[int] = None,
    override_reason: Optional[str] = None,
) -> int:
    """
    Insert a signal event row and return its event_id.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope.
        event_type: 'state_change' | 'detail_view_open' | 'detail_view_close' | 'override_inferred'.
        job_id: FK → jobs.job_id (optional for session-level events).
        from_state: Prior state (for state_change events).
        to_state: New state (for state_change events).
        specialty_name: Classification specialty at time of event.
        classification_confidence: Confidence level at time of event.
        dwell_ms: Milliseconds spent viewing the detail pane (for detail_view_close).
        override_reason: 'quick_dismiss_lt_15s' | 'acted_on_unclassified' | None.

    Returns:
        The newly assigned event_id.
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO signal_events
                    (user_id, job_id, event_type, from_state, to_state,
                     specialty_name, classification_confidence, dwell_ms,
                     override_reason, recorded_at)
                VALUES
                    (:user_id, :job_id, :event_type, :from_state, :to_state,
                     :specialty_name, :classification_confidence, :dwell_ms,
                     :override_reason, :recorded_at)
                """
            ),
            {
                "user_id": user_id,
                "job_id": job_id,
                "event_type": event_type,
                "from_state": from_state,
                "to_state": to_state,
                "specialty_name": specialty_name,
                "classification_confidence": classification_confidence,
                "dwell_ms": dwell_ms,
                "override_reason": override_reason,
                "recorded_at": now,
            },
        )
        row = conn.execute(text("SELECT last_insert_rowid()")).fetchone()
        conn.commit()
    event_id = row[0]
    logger.debug("insert_signal: event_id=%d event_type=%s job_id=%s", event_id, event_type, job_id)
    return event_id


def list_signals(
    engine: Engine,
    user_id: str = "local",
    job_id: Optional[str] = None,
    window_days: Optional[int] = None,
) -> list[dict]:
    """
    Return signal events for a user, optionally filtered by job or time window.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope.
        job_id: If provided, restrict to events for this job.
        window_days: If provided, restrict to events in the last N days.

    Returns:
        List of event dicts ordered by recorded_at DESC.
    """
    from datetime import timedelta

    conditions = ["user_id = :user_id"]
    params: dict = {"user_id": user_id}

    if job_id is not None:
        conditions.append("job_id = :job_id")
        params["job_id"] = job_id

    if window_days is not None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        conditions.append("recorded_at >= :cutoff")
        params["cutoff"] = cutoff

    where = " AND ".join(conditions)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT * FROM signal_events WHERE {where} ORDER BY recorded_at DESC"
            ),
            params,
        ).fetchall()
    return [dict(row._mapping) for row in rows]


# ---------------------------------------------------------------------------
# User settings
# ---------------------------------------------------------------------------


def get_settings(engine: Engine, user_id: str = "local") -> Optional[dict]:
    """
    Fetch the settings row for a user, or None if no settings row exists.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope.

    Returns:
        Dict with settings fields, or None.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM user_settings WHERE user_id = :user_id"),
            {"user_id": user_id},
        ).fetchone()
    return dict(row._mapping) if row is not None else None


def upsert_settings(
    engine: Engine,
    user_id: str,
    location_preference: str,
    salary_floor_cad: Optional[int],
    excluded_seniority_levels: list[str],
) -> None:
    """
    Insert or update the settings row for a user.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope.
        location_preference: 'vancouver' | 'remote_friendly' | 'both'.
        salary_floor_cad: Minimum acceptable salary in CAD, or None for no floor.
        excluded_seniority_levels: JSON-serialisable list of seniority strings to exclude.
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO user_settings
                    (user_id, location_preference, salary_floor_cad,
                     excluded_seniority_levels, updated_at)
                VALUES
                    (:user_id, :location_preference, :salary_floor_cad,
                     :excluded_seniority_levels, :updated_at)
                ON CONFLICT(user_id) DO UPDATE SET
                    location_preference       = excluded.location_preference,
                    salary_floor_cad          = excluded.salary_floor_cad,
                    excluded_seniority_levels = excluded.excluded_seniority_levels,
                    updated_at                = excluded.updated_at
                """
            ),
            {
                "user_id": user_id,
                "location_preference": location_preference,
                "salary_floor_cad": salary_floor_cad,
                "excluded_seniority_levels": json.dumps(excluded_seniority_levels),
                "updated_at": now,
            },
        )
        conn.commit()
    logger.debug("upsert_settings: user_id=%s location=%s", user_id, location_preference)


# ---------------------------------------------------------------------------
# Specialty types
# ---------------------------------------------------------------------------


def list_specialty_types(
    engine: Engine,
    user_id: str = "local",
    enabled_only: bool = True,
) -> list[dict]:
    """
    Return specialty type rows for a user, optionally restricted to enabled ones.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope.
        enabled_only: If True, filter to rows where enabled = 1.

    Returns:
        List of dicts ordered by tier ASC, name ASC.
    """
    params: dict = {"user_id": user_id}
    extra = "AND enabled = 1" if enabled_only else ""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT * FROM specialty_types WHERE user_id = :user_id {extra} "
                f"ORDER BY tier ASC, name ASC"
            ),
            params,
        ).fetchall()
    return [dict(row._mapping) for row in rows]


def insert_specialty_type(
    engine: Engine,
    user_id: str,
    name: str,
    description: Optional[str],
    duty_signals: list[str],
    tier: int,
    enabled: bool,
    source: str,
) -> int:
    """
    Insert a specialty type row and return its specialty_id.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope.
        name: Display name e.g. 'Data Scientist'.
        description: Optional description for prompt context.
        duty_signals: Example duty phrases.
        tier: 1 | 2 | 3.
        enabled: Whether the type is active.
        source: 'seed' | 'config' | 'proposed'.

    Returns:
        The newly assigned specialty_id.
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO specialty_types
                    (user_id, name, description, duty_signals, tier, enabled, source, created_at)
                VALUES
                    (:user_id, :name, :description, :duty_signals, :tier, :enabled, :source, :created_at)
                """
            ),
            {
                "user_id": user_id,
                "name": name,
                "description": description,
                "duty_signals": json.dumps(duty_signals),
                "tier": tier,
                "enabled": 1 if enabled else 0,
                "source": source,
                "created_at": now,
            },
        )
        row = conn.execute(text("SELECT last_insert_rowid()")).fetchone()
        conn.commit()
    return row[0]


def update_specialty_type_enabled(
    engine: Engine,
    specialty_id: int,
    enabled: bool,
) -> None:
    """
    Toggle the enabled flag on a specialty type.

    Args:
        engine: SQLAlchemy Engine.
        specialty_id: The integer primary key of the specialty type.
        enabled: True to enable, False to disable.
    """
    with engine.connect() as conn:
        conn.execute(
            text(
                "UPDATE specialty_types SET enabled = :enabled "
                "WHERE specialty_id = :specialty_id"
            ),
            {"enabled": 1 if enabled else 0, "specialty_id": specialty_id},
        )
        conn.commit()
    logger.debug(
        "update_specialty_type_enabled: specialty_id=%d enabled=%s",
        specialty_id, enabled,
    )


# ---------------------------------------------------------------------------
# Scrape runs
# ---------------------------------------------------------------------------


def insert_scrape_run(
    engine: Engine,
    user_id: str,
    source_results_json: str = "{}",
) -> int:
    """
    Insert a new scrape run record and return its run_id.

    Args:
        engine: SQLAlchemy Engine.
        user_id: User scope.
        source_results_json: JSON string e.g. '{"linkedin": {"status": "pending"}}'.

    Returns:
        The newly assigned run_id.
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO scrape_runs
                    (user_id, started_at, source_results_json)
                VALUES
                    (:user_id, :started_at, :source_results_json)
                """
            ),
            {
                "user_id": user_id,
                "started_at": now,
                "source_results_json": source_results_json,
            },
        )
        row = conn.execute(text("SELECT last_insert_rowid()")).fetchone()
        conn.commit()
    run_id = row[0]
    logger.debug("insert_scrape_run: run_id=%d", run_id)
    return run_id


def update_scrape_run_finished(
    engine: Engine,
    run_id: int,
    source_results_json: str,
    total_fetched: int,
    total_after_filters: int,
    total_duplicates: int,
    error_log: Optional[str] = None,
) -> None:
    """
    Mark a scrape run as finished with final counts and result JSON.

    Args:
        engine: SQLAlchemy Engine.
        run_id: The run_id returned by insert_scrape_run.
        source_results_json: Final JSON result per source.
        total_fetched: Total postings fetched before dedup/filters.
        total_after_filters: Postings remaining after hard filters.
        total_duplicates: Postings flagged as duplicates.
        error_log: Optional error text to store.
    """
    now = datetime.now(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                UPDATE scrape_runs SET
                    finished_at           = :finished_at,
                    source_results_json   = :source_results_json,
                    total_fetched         = :total_fetched,
                    total_after_filters   = :total_after_filters,
                    total_duplicates      = :total_duplicates,
                    error_log             = :error_log
                WHERE run_id = :run_id
                """
            ),
            {
                "finished_at": now,
                "source_results_json": source_results_json,
                "total_fetched": total_fetched,
                "total_after_filters": total_after_filters,
                "total_duplicates": total_duplicates,
                "error_log": error_log,
                "run_id": run_id,
            },
        )
        conn.commit()
    logger.debug(
        "update_scrape_run_finished: run_id=%d total_fetched=%d", run_id, total_fetched
    )
