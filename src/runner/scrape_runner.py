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
from pathlib import Path
from typing import Any, Callable, Optional

import yaml
from sqlalchemy.engine import Engine

from src.models.models import JobPosting, RawJobPosting, SeniorityLevel
from src.processing.normalizer import Normalizer
from src.processing.salary import SalaryExtractor
from src.processing.seniority import SeniorityInferrer
from src.services.dedup import DedupService
from src.services.filter_service import (
    FilterConfig,
    FilterService,
    _TITLE_ALLOWLIST,
    _TITLE_DENYLIST,
    title_passes,
)
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
        dedup_window_days: How many days back to look for cross-run duplicates (default 90).
    """
    filter_config: FilterConfig = field(default_factory=FilterConfig)
    user_id: str = "local"
    dedup_window_days: int = 90


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
        # Load scrape config and apply dedup_window_days override from yaml
        self._scrape_cfg = self._load_scrape_config()
        self._config.dedup_window_days = (
            self._scrape_cfg.get("dedup", {}).get("window_days", 90)
        )

    # ------------------------------------------------------------------
    # Config loader
    # ------------------------------------------------------------------

    @staticmethod
    def _load_scrape_config() -> dict[str, Any]:
        """
        Load config/scrape_config.yaml relative to the project root.

        Returns the parsed YAML dict, or a dict with defaults if the file is
        missing or unreadable.  Never raises — config loading must not crash
        the pipeline.
        """
        _defaults: dict[str, Any] = {
            "search": {
                "terms": ["data scientist", "machine learning engineer", "applied scientist", "data analyst"],
                "locations": ["Vancouver, BC, Canada", "Canada"],
                "results_wanted_per_term_location": 30,
                "hours_old": 72,
            },
            "dedup": {"window_days": 90},
            "logging": {"run_logs_dir": "logs"},
        }
        # Resolve path relative to this file's directory (src/runner/) → project root
        project_root = Path(__file__).resolve().parent.parent.parent
        config_path = project_root / "config" / "scrape_config.yaml"
        try:
            with open(config_path, "r", encoding="utf-8") as fh:
                loaded = yaml.safe_load(fh) or {}
            logger.info("ScrapeRunner: loaded scrape config from %s", config_path)
            return loaded
        except FileNotFoundError:
            logger.warning(
                "ScrapeRunner: %s not found — using defaults", config_path
            )
            return _defaults
        except Exception as exc:
            logger.warning(
                "ScrapeRunner: failed to load scrape config: %s — using defaults", exc
            )
            return _defaults

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _write_stage_log(self, run_id: int, stage_file: str, data: Any) -> None:
        """
        Write a JSON diagnostic log file for a pipeline stage.

        Files are written to logs/run_{run_id}/{stage_file}.  Failures are
        swallowed and logged as warnings — stage logs must never crash the pipeline.
        """
        try:
            log_dir = (
                Path(self._scrape_cfg.get("logging", {}).get("run_logs_dir", "logs"))
                / f"run_{run_id}"
            )
            log_dir.mkdir(parents=True, exist_ok=True)
            with open(log_dir / stage_file, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as exc:
            logger.warning(
                "ScrapeRunner: failed to write stage log %s: %s", stage_file, exc
            )

    def _fetch_from_sources(
        self,
        term_location_pairs: list[tuple[str, str]],
        hours_old: int,
        results_wanted_per_pair: int,
        query: SearchQuery,
    ) -> tuple[list[RawJobPosting], dict[str, dict]]:
        """
        Fetch from all registered sources using fetch_multi where available.

        For sources that implement fetch_multi(), calls it with the config-driven
        term_location_pairs.  Falls back to fetch(query) for sources that do not.

        If the registry does not expose _sources directly (e.g. in tests where
        the registry is a mock), falls back to the legacy fetch_all() path so
        that existing test mocks continue to work.

        Returns:
            (all_raw_postings, source_summary_dict)
        """
        from src.sources.base import RateLimitError  # avoid circular at module level

        # -- Try direct _sources access (real registry) --
        sources_dict: dict | None = None
        try:
            sources_dict = self._registry._sources
        except AttributeError:
            sources_dict = None

        if sources_dict is None:
            # Fallback: legacy fetch_all() path (used in tests with mock registries)
            fetch_results = self._registry.fetch_all(query)
            raw_postings: list[RawJobPosting] = []
            source_summary: dict[str, dict] = {}
            for source_name, result in fetch_results.items():
                if result.status == "ok":
                    raw_postings.extend(result.postings)
                    source_summary[source_name] = {
                        "status": "ok",
                        "count": result.count,
                    }
                elif result.status == "rate_limited":
                    source_summary[source_name] = {
                        "status": "rate_limited",
                        "error": result.error,
                        "count": 0,
                    }
                else:
                    source_summary[source_name] = {
                        "status": "error",
                        "error": result.error,
                        "count": 0,
                    }
            return raw_postings, source_summary

        # -- Direct source dispatch with fetch_multi support --
        raw_postings = []
        source_summary = {}

        for source_name, source in sources_dict.items():
            if not source.is_available():
                logger.warning(
                    "ScrapeRunner: source '%s' is not available — skipping", source_name
                )
                source_summary[source_name] = {
                    "status": "error",
                    "error": f"Source '{source_name}' not available",
                    "count": 0,
                }
                continue

            try:
                if hasattr(source, "fetch_multi"):
                    postings = source.fetch_multi(
                        term_location_pairs, hours_old, results_wanted_per_pair
                    )
                else:
                    postings = source.fetch(query)

                raw_postings.extend(postings)
                source_summary[source_name] = {
                    "status": "ok",
                    "count": len(postings),
                }
                logger.info(
                    "ScrapeRunner: %s → %d postings", source_name, len(postings)
                )
            except RateLimitError as exc:
                source_summary[source_name] = {
                    "status": "rate_limited",
                    "error": str(exc),
                    "count": 0,
                }
                logger.warning(
                    "ScrapeRunner: rate limit on '%s': %s", source_name, exc
                )
            except Exception as exc:
                source_summary[source_name] = {
                    "status": "error",
                    "error": str(exc),
                    "count": 0,
                }
                logger.error(
                    "ScrapeRunner: error from '%s': %s", source_name, exc,
                    exc_info=True,
                )

        return raw_postings, source_summary

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

        # ----------------------------------------------------------------
        # Stage 1: Fetch all sources (config-driven multi-term fetch)
        # ----------------------------------------------------------------
        _status("Fetching postings from all sources…")

        search_cfg = self._scrape_cfg.get("search", {})
        terms: list[str] = search_cfg.get("terms", [query.search_term])
        locations: list[str] = search_cfg.get("locations", [query.location])
        results_per_pair: int = search_cfg.get("results_wanted_per_term_location", 30)
        hours_old: int = search_cfg.get("hours_old", query.hours_old or 72)

        term_location_pairs = [
            (term, location) for term in terms for location in locations
        ]

        raw_postings, source_summary = self._fetch_from_sources(
            term_location_pairs, hours_old, results_per_pair, query
        )

        rate_limited: list[str] = []
        source_errors: dict[str, str] = {}
        for src_name, info in source_summary.items():
            if info["status"] == "rate_limited":
                rate_limited.append(src_name)
                _status(f"  {src_name}: RATE LIMITED — {info.get('error', '')}")
            elif info["status"] == "error":
                source_errors[src_name] = info.get("error", "(unknown error)")
                _status(f"  {src_name}: ERROR — {info.get('error', '')}")
            else:
                _status(f"  {src_name}: {info['count']} raw postings")

        fetched_count = len(raw_postings)
        _status(f"Fetched {fetched_count} total raw postings")

        # Stage 1 diagnostic log
        self._write_stage_log(
            run_id,
            "01_fetch_raw.json",
            [
                {
                    "id": p.id,
                    "title": p.title,
                    "company": p.company,
                    "location": p.location,
                    "url": p.url,
                    "source": p.source.value,
                }
                for p in raw_postings
            ],
        )

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

        # Stage 2 diagnostic log
        self._write_stage_log(
            run_id,
            "02_normalized.json",
            [
                {
                    "job_id": j.job_id,
                    "title": j.title,
                    "company": j.company,
                    "location": j.location,
                    "salary_min_cad": j.salary_min_cad,
                    "salary_max_cad": j.salary_max_cad,
                    "seniority": j.seniority.value,
                    "source": j.source.value,
                }
                for j in normalized
            ],
        )

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

        # Stage 3 diagnostic log (post-enrichment)
        self._write_stage_log(
            run_id,
            "03_enriched.json",
            [
                {
                    "job_id": j.job_id,
                    "title": j.title,
                    "company": j.company,
                    "location": j.location,
                    "salary_min_cad": j.salary_min_cad,
                    "salary_max_cad": j.salary_max_cad,
                    "seniority": j.seniority.value,
                    "source": j.source.value,
                }
                for j in normalized
            ],
        )

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

        # Stage 4 diagnostic log
        _today = datetime.now(timezone.utc)
        dedup_log = []
        for job in normalized:
            if job.duplicate_of:
                dedup_log.append({
                    "job_id": job.job_id,
                    "title": job.title,
                    "decision": "duplicate",
                    "matched_job_id": job.duplicate_of,
                })
            elif job in canonical_jobs:
                dedup_log.append({
                    "job_id": job.job_id,
                    "title": job.title,
                    "decision": "canonical",
                    "matched_job_id": None,
                })
            else:
                dedup_log.append({
                    "job_id": job.job_id,
                    "title": job.title,
                    "decision": "within_run_dup",
                    "matched_job_id": None,
                })
        self._write_stage_log(run_id, "04_dedup.json", dedup_log)

        # ----------------------------------------------------------------
        # Stage 4.5: Title relevance filter
        # ----------------------------------------------------------------
        _status("Applying title relevance filter…")
        title_kept: list[JobPosting] = []
        title_filter_log: list[dict] = []
        for job in canonical_jobs:
            passed = title_passes(job.title)
            if _TITLE_ALLOWLIST.search(job.title):
                rule = "allowlist"
            elif not passed:
                rule = "denylist"
            else:
                rule = "unknown_pass"
            title_filter_log.append({
                "job_id": job.job_id,
                "title": job.title,
                "passed": passed,
                "rule": rule,
            })
            if passed:
                title_kept.append(job)
        canonical_jobs = title_kept
        _status(
            f"Title filter: {len(canonical_jobs)} kept, "
            f"{len(title_filter_log) - len(canonical_jobs)} dropped"
        )
        self._write_stage_log(run_id, "05_title_filter.json", title_filter_log)

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

        # Stage 5 (hard filter) diagnostic log
        hard_filter_log = (
            [
                {"job_id": j.job_id, "title": j.title, "passed": True, "reason": None}
                for j in filter_result.kept
            ]
            + [
                {"job_id": p.job_id, "title": p.title, "passed": False, "reason": r}
                for p, r in filter_result.excluded
            ]
        )
        self._write_stage_log(run_id, "06_hard_filter.json", hard_filter_log)

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

        # Stage 6 (stored) diagnostic log
        self._write_stage_log(run_id, "07_stored.json", list(stored_job_ids))

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
