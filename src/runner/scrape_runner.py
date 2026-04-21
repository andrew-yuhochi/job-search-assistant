"""
ScrapeRunner: end-to-end scrape pipeline orchestrator.

Stages (in order):
  1. fetch_all       — JobSourceRegistry dispatches to all sources in parallel
  2. normalize       — Normalizer maps RawJobPosting → JobPosting
  3. extract         — SalaryExtractor & SeniorityInferrer enrich salary/seniority fields
  4. dedup           — DedupService checks against existing DB rows + within-run URL set
  5. filter          — FilterService applies hard filters (salary floor, seniority, location)
  6. store           — repository.insert_job persists canonical jobs; insert_duplicate for dups
  7. classify_stub   — insert_classification with specialty='Unclassified' (TASK-014 upgrades this)
  8. scrape_run      — update_scrape_run_finished records final counts

Per TDD §2 data flow diagram and TASK-013.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

from sqlalchemy.engine import Engine

from src.models.models import JobPosting, SeniorityLevel
from src.processing.normalizer import Normalizer
from src.processing.salary import SalaryExtractor
from src.processing.seniority import SeniorityInferrer
from src.services.dedup import DedupService
from src.services.filter_service import FilterConfig, FilterService
from src.sources.base import SearchQuery
from src.sources.registry import JobSourceRegistry
from src.storage import repository

logger = logging.getLogger(__name__)

# Stub values written to classifications until ClassifierService is implemented in TASK-014.
_STUB_MODEL_NAME = "stub"
_STUB_PROMPT_VERSION = "v0-unclassified"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass
class ScrapeConfig:
    """
    Runtime configuration for a single scrape run.

    Attributes:
        filter_config:  FilterService configuration (salary floor, location, seniority, size).
        user_id:        Multi-tenant user scope (default 'local').
        dedup_window_days: How many days back to look for cross-run duplicates (default 30).
    """
    filter_config: FilterConfig = field(default_factory=FilterConfig)
    user_id: str = "local"
    dedup_window_days: int = 30


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass
class ScrapeRunResult:
    """
    Structured result returned from ScrapeRunner.run().

    Counts represent postings at each pipeline stage. rate_limited_sources
    lists source names that returned HTTP 429; errors maps source name → error
    message for sources that failed with a non-rate-limit error.
    """
    run_id: int
    fetched: int
    normalized: int
    after_dedup: int          # canonical postings (not duplicates)
    duplicate_count: int      # within-run + cross-run duplicates detected
    after_filter: int         # postings that passed hard filters
    stored: int               # rows successfully written to jobs table
    classified_stub: int      # classifications stub rows written
    rate_limited_sources: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)
    elapsed_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class ScrapeRunner:
    """
    Orchestrates the full scrape pipeline from source fetch to DB storage.

    Designed for synchronous execution (PoC) — no background workers.
    status_callback is called after each major stage so the UI can display
    live progress via st.session_state.scrape_status.

    All dependencies are injected so the runner is testable in isolation.
    """

    def __init__(
        self,
        registry: JobSourceRegistry,
        engine: Engine,
        filter_service: Optional[FilterService] = None,
        config: Optional[ScrapeConfig] = None,
    ) -> None:
        self._registry = registry
        self._engine = engine
        self._filter_service = filter_service or FilterService()
        self._config = config or ScrapeConfig()
        self._normalizer = Normalizer()
        self._salary_extractor = SalaryExtractor()
        self._seniority_inferrer = SeniorityInferrer()
        self._dedup_service = DedupService()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        query: SearchQuery,
        status_callback: Optional[Callable[[str], None]] = None,
    ) -> ScrapeRunResult:
        """
        Execute the full pipeline for a given query.

        Args:
            query:           The search query dispatched to all registered sources.
            status_callback: Optional callable(message: str) invoked after each stage.
                             The UI wires this to st.session_state.scrape_status.

        Returns:
            ScrapeRunResult with per-stage counts and elapsed time.
        """
        t_start = time.monotonic()

        def _status(msg: str) -> None:
            logger.info("ScrapeRunner: %s", msg)
            if status_callback:
                status_callback(msg)

        # --- Open scrape_run row ---
        run_id = repository.insert_scrape_run(
            self._engine,
            user_id=self._config.user_id,
            source_results_json="{}",
        )
        _status(f"Scrape run #{run_id} started")

        rate_limited: list[str] = []
        source_errors: dict[str, str] = {}

        # ----------------------------------------------------------------
        # Stage 1: Fetch all sources
        # ----------------------------------------------------------------
        _status("Fetching postings from all sources…")
        fetch_results = self._registry.fetch_all(query)

        raw_postings = []
        source_summary: dict[str, dict] = {}
        for source_name, result in fetch_results.items():
            if result.status == "ok":
                raw_postings.extend(result.postings)
                source_summary[source_name] = {
                    "status": "ok",
                    "count": result.count,
                }
                _status(f"  {source_name}: {result.count} raw postings")
            elif result.status == "rate_limited":
                rate_limited.append(source_name)
                source_summary[source_name] = {
                    "status": "rate_limited",
                    "error": result.error,
                }
                _status(f"  {source_name}: RATE LIMITED — {result.error}")
            else:
                source_errors[source_name] = result.error or "(unknown error)"
                source_summary[source_name] = {
                    "status": "error",
                    "error": result.error,
                }
                _status(f"  {source_name}: ERROR — {result.error}")

        fetched_count = len(raw_postings)
        _status(f"Fetched {fetched_count} total raw postings")

        # ----------------------------------------------------------------
        # Stage 2: Normalize
        # ----------------------------------------------------------------
        _status("Normalizing postings…")
        normalized: list[JobPosting] = []
        for raw in raw_postings:
            try:
                job = self._normalizer.normalize(raw)
                normalized.append(job)
            except Exception as exc:
                logger.warning(
                    "ScrapeRunner: normalize failed for url=%s: %s",
                    getattr(raw, "url", "?"),
                    exc,
                )
        normalized_count = len(normalized)
        _status(f"Normalized {normalized_count} postings")

        # ----------------------------------------------------------------
        # Stage 3: Salary extraction + seniority enrichment
        # ----------------------------------------------------------------
        _status("Extracting salary and seniority signals…")
        for job in normalized:
            try:
                sal_min, sal_max, sal_source = self._salary_extractor.extract(
                    job.description
                )
                if sal_min is not None and job.salary_min_cad is None:
                    job.salary_min_cad = int(sal_min)
                if sal_max is not None and job.salary_max_cad is None:
                    job.salary_max_cad = int(sal_max)
                if sal_source != "unknown" and job.salary_source is None:
                    job.salary_source = sal_source
            except Exception as exc:
                logger.warning("ScrapeRunner: salary extraction failed: %s", exc)

            try:
                # SeniorityInferrer already ran in normalizer via title keyword scan;
                # here we re-run with job_level field if available (richer signal).
                job_level = getattr(job, "job_level", None)
                if job_level and job.seniority == SeniorityLevel.unknown:
                    job.seniority = self._seniority_inferrer.infer(
                        job.title, job_level=job_level
                    )
            except Exception as exc:
                logger.warning("ScrapeRunner: seniority inference failed: %s", exc)

        # ----------------------------------------------------------------
        # Stage 4: Dedup
        # ----------------------------------------------------------------
        _status("Running deduplication…")
        # Cross-run: load existing canonical jobs from DB (last N days)
        existing_jobs: list[JobPosting] = repository.list_jobs_for_dedup(
            self._engine,
            user_id=self._config.user_id,
            within_days=self._config.dedup_window_days,
        )

        canonical_jobs: list[JobPosting] = []
        dup_count = 0
        # Within-run URL dedup set — catches same URL from two sources in this batch
        seen_urls: set[str] = {j.url for j in existing_jobs}

        # Pending duplicates: (dup_job, canonical_job_id, match_type, match_score)
        # We collect them here and insert AFTER the canonical jobs are in the DB,
        # so the FK constraint on jobs.duplicate_of is satisfied.
        pending_duplicates: list[tuple[JobPosting, str, str, float | None]] = []

        for job in normalized:
            # Within-run URL check first (cheap)
            if job.url in seen_urls:
                logger.debug(
                    "ScrapeRunner: within-run URL dup skipped: %s", job.url
                )
                dup_count += 1
                # No canonical_job_id available for pure within-run dups — skip recording.
                continue

            # Cross-run dedup via DedupService
            dedup_result = self._dedup_service.check(job, existing_jobs)
            if dedup_result.is_duplicate:
                dup_count += 1
                job.duplicate_of = dedup_result.canonical_job_id
                pending_duplicates.append((
                    job,
                    dedup_result.canonical_job_id,
                    dedup_result.match_type or "unknown",
                    dedup_result.match_score,
                ))
            else:
                # Canonical — add to seen set and candidate list for filtering
                seen_urls.add(job.url)
                existing_jobs.append(job)   # keep dedup set fresh within loop
                canonical_jobs.append(job)

        _status(
            f"Dedup complete: {len(canonical_jobs)} canonical, {dup_count} duplicates"
        )

        # ----------------------------------------------------------------
        # Stage 5: Filter
        # ----------------------------------------------------------------
        _status("Applying hard filters…")
        filter_result = self._filter_service.apply(
            canonical_jobs, self._config.filter_config
        )
        kept_jobs = filter_result.kept
        _status(
            f"Filter: {filter_result.kept_count} kept, "
            f"{filter_result.excluded_count} excluded"
        )

        # ----------------------------------------------------------------
        # Stage 6 + 7: Store canonical kept jobs + duplicates + classification stubs
        #
        # Order matters: canonical jobs must be persisted before duplicate rows
        # that reference them via the duplicate_of FK.
        # ----------------------------------------------------------------
        _status(f"Storing {len(kept_jobs)} postings to database…")
        stored_count = 0
        classified_stub_count = 0
        stored_job_ids: set[str] = set()

        for job in kept_jobs:
            try:
                repository.insert_job(self._engine, job)
                stored_count += 1
                stored_job_ids.add(job.job_id)
            except Exception as exc:
                logger.warning(
                    "ScrapeRunner: failed to store job_id=%s: %s", job.job_id, exc
                )
                continue

            # Write classification stub (TASK-014 replaces this with real classifier)
            try:
                repository.insert_classification(
                    self._engine,
                    job_id=job.job_id,
                    user_id=self._config.user_id,
                    specialty_name="Unclassified",
                    confidence="low",
                    duty_signals=[],
                    model_name=_STUB_MODEL_NAME,
                    prompt_version=_STUB_PROMPT_VERSION,
                )
                classified_stub_count += 1
            except Exception as exc:
                logger.warning(
                    "ScrapeRunner: failed to write classification stub for job_id=%s: %s",
                    job.job_id, exc,
                )

        # Now persist duplicates — canonical jobs are guaranteed to be in the DB
        for dup_job, canonical_job_id, match_type, match_score in pending_duplicates:
            try:
                repository.insert_job(self._engine, dup_job)
                repository.insert_duplicate(
                    self._engine,
                    duplicate_job_id=dup_job.job_id,
                    canonical_job_id=canonical_job_id,
                    match_type=match_type,
                    match_score=match_score,
                )
            except Exception as exc:
                logger.warning(
                    "ScrapeRunner: failed to persist duplicate job_id=%s: %s",
                    dup_job.job_id, exc,
                )

        _status(f"Stored {stored_count} jobs, wrote {classified_stub_count} classification stubs")

        # ----------------------------------------------------------------
        # Stage 8: Update scrape_run row with final counts + error log
        # ----------------------------------------------------------------
        elapsed = time.monotonic() - t_start
        error_log: Optional[str] = None
        if source_errors:
            error_log = json.dumps(source_errors)

        repository.update_scrape_run_finished(
            self._engine,
            run_id=run_id,
            source_results_json=json.dumps(source_summary),
            total_fetched=fetched_count,
            total_after_filters=filter_result.kept_count,
            total_duplicates=dup_count,
            error_log=error_log,
        )

        result = ScrapeRunResult(
            run_id=run_id,
            fetched=fetched_count,
            normalized=normalized_count,
            after_dedup=len(canonical_jobs),
            duplicate_count=dup_count,
            after_filter=filter_result.kept_count,
            stored=stored_count,
            classified_stub=classified_stub_count,
            rate_limited_sources=rate_limited,
            errors=source_errors,
            elapsed_seconds=round(elapsed, 2),
        )

        _status(
            f"Run #{run_id} complete — {stored_count} new jobs stored in "
            f"{elapsed:.1f}s"
        )
        logger.info(
            "ScrapeRunner.run complete: run_id=%d fetched=%d normalized=%d "
            "canonical=%d dups=%d filtered_kept=%d stored=%d elapsed=%.2fs",
            run_id, fetched_count, normalized_count, len(canonical_jobs),
            dup_count, filter_result.kept_count, stored_count, elapsed,
        )
        return result
