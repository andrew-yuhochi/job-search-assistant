"""
ScrapeRunner: end-to-end scrape pipeline orchestrator.

Stages (in order):
  1. fetch_all          — JobSourceRegistry dispatches to all sources in parallel
  2. normalize          — Normalizer maps RawJobPosting → JobPosting
  3. hard_filter        — FilterService applies hard filters (salary floor, seniority, location)
  4. title_filter       — Allowlist/denylist regex + Haiku for unknowns
  5. cross_source_dedup — Fuzzy fingerprint dedup within the current batch (same job on LinkedIn + Indeed)
  6. cross_run_dedup    — DedupService fuzzy match against existing DB rows + within-run URL set
  7. enrich             — SalaryExtractor & SeniorityInferrer (only runs on survivors of stages 3–5)
  8. store              — repository.insert_job persists canonical jobs; insert_duplicate for dups
  9. classify_stub      — insert_classification with specialty='Unclassified' (TASK-014 upgrades this)
  10. scrape_run        — update_scrape_run_finished records final counts

Cheap operations (hard filter, title filter, cross-source dedup) run before the expensive
cross-run DB dedup and enrichment, so the LLM/API calls only process survivors.

Run directory naming: logs/run_{YYYYMMDD_HHMMSS}/ — timestamp-based, never overwritten.
run_from_raw() always writes to a fresh directory; it never touches the source run's directory.

Per TDD §2 data flow diagram and TASK-M4-001.
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import yaml
from rapidfuzz import fuzz
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
    Structured result returned from ScrapeRunner.run() and run_from_raw().

    Counts represent postings at each pipeline stage. rate_limited_sources
    lists source names that returned HTTP 429; errors maps source name → error
    message for sources that failed with a non-rate-limit error.

    run_dir is the timestamp-named directory where stage logs were written
    (e.g. logs/run_20260422_143052/).  Always set; never None after a successful run.
    """
    run_id: int
    fetched: int
    normalized: int
    after_dedup: int          # canonical postings (not duplicates)
    duplicate_count: int      # within-run + cross-run duplicates detected
    after_filter: int         # postings that passed hard filters
    stored: int               # rows successfully written to jobs table
    classified_stub: int      # classifications stub rows written
    run_dir: Optional[Path] = None   # timestamp-named stage-log directory
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

    def _make_run_dir(self) -> Path:
        """
        Create and return a fresh, timestamp-named run directory.

        Format: logs/run_{YYYYMMDD_HHMMSS}/

        Microseconds are used to break ties when two runs start within the same
        wall-clock second (common in tests).  exist_ok=False is intentional —
        if by extreme coincidence the directory already exists, we raise rather
        than silently sharing it with another run.
        """
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        base = Path(self._scrape_cfg.get("logging", {}).get("run_logs_dir", "logs"))
        run_dir = base / f"run_{ts}"
        run_dir.mkdir(parents=True, exist_ok=False)
        return run_dir

    def _write_stage_log(self, run_dir: Path, stage_file: str, data: Any) -> None:
        """
        Write a JSON diagnostic log file for a pipeline stage.

        Files are written to run_dir/{stage_file}.  run_dir must have been
        created by _make_run_dir() before the first call.  Failures are swallowed
        and logged as warnings — stage logs must never crash the pipeline.
        """
        try:
            with open(run_dir / stage_file, "w") as f:
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
    # Cross-source dedup helper (Stage 5)
    # ------------------------------------------------------------------

    # Pre-compiled pattern for stripping punctuation from fingerprints.
    _PUNCT_RE = re.compile(r"[^\w\s]")

    @classmethod
    def _fingerprint(cls, title: str, company: str) -> str:
        """
        Compute a normalised fingerprint for cross-source dedup.

        Algorithm: lowercase → strip punctuation → collapse whitespace, then
        join title and company with a pipe separator.
        """
        def _norm(text: str) -> str:
            text = text.lower()
            text = cls._PUNCT_RE.sub("", text)
            return " ".join(text.split())

        return f"{_norm(title)}|{_norm(company)}"

    @staticmethod
    def _cross_source_dedup(
        postings: list["JobPosting"],
        threshold: int = 85,
    ) -> tuple[list["JobPosting"], list[dict]]:
        """
        Deduplicate postings within a single batch across different sources.

        Detects the same job posted on LinkedIn AND Indeed (different URLs, same role).
        For each duplicate cluster, the posting with the longest description is kept;
        others are marked as within-run duplicates.

        Args:
            postings:  Postings that survived hard filter + title filter.
            threshold: Levenshtein ratio threshold (rapidfuzz, 0–100). Default 85.

        Returns:
            (kept, log) where kept is the surviving postings and log is a list of
            per-posting dicts with keys: job_id, title, company, decision, matched_job_id,
            match_score.
        """
        if not postings:
            return [], []

        # Build fingerprint list once
        fps = [
            ScrapeRunner._fingerprint(p.title, p.company) for p in postings
        ]

        # cluster_of[i] = index of the representative (kept) posting for posting i.
        # Initialise: each posting is its own cluster representative.
        cluster_of: list[int] = list(range(len(postings)))

        def _find(i: int) -> int:
            """Find the root representative of posting i (path-compressed union-find)."""
            while cluster_of[i] != i:
                cluster_of[i] = cluster_of[cluster_of[i]]
                i = cluster_of[i]
            return i

        def _union(i: int, j: int) -> None:
            """Union two clusters, keeping the one with the longer description as root."""
            ri, rj = _find(i), _find(j)
            if ri == rj:
                return
            # Keep the posting with the longer description as the canonical root
            len_i = len(postings[ri].description or "")
            len_j = len(postings[rj].description or "")
            if len_i >= len_j:
                cluster_of[rj] = ri
            else:
                cluster_of[ri] = rj

        # Compare all pairs (O(n²) — acceptable at PoC batch sizes)
        match_scores: dict[tuple[int, int], float] = {}
        for i in range(len(postings)):
            for j in range(i + 1, len(postings)):
                score = fuzz.ratio(fps[i], fps[j])
                if score >= threshold:
                    match_scores[(i, j)] = score
                    _union(i, j)

        # Determine kept vs. duplicate for each posting
        kept: list[JobPosting] = []
        log: list[dict] = []
        seen_roots: set[int] = set()

        for i, posting in enumerate(postings):
            root = _find(i)
            if root not in seen_roots:
                # This posting IS the cluster representative — keep it
                seen_roots.add(root)
                kept.append(posting)
                log.append({
                    "job_id": posting.job_id,
                    "title": posting.title,
                    "company": posting.company,
                    "decision": "kept",
                    "matched_job_id": None,
                    "match_score": None,
                })
            else:
                # Duplicate of the cluster representative
                canonical = postings[root]
                # Find the match score between i and root (may have been set indirectly)
                pair = (min(i, root), max(i, root))
                score = match_scores.get(pair)
                log.append({
                    "job_id": posting.job_id,
                    "title": posting.title,
                    "company": posting.company,
                    "decision": "duplicate",
                    "matched_job_id": canonical.job_id,
                    "match_score": round(score / 100.0, 4) if score is not None else None,
                })
                logger.debug(
                    "Cross-source dedup: %s marked as duplicate of %s (score=%s)",
                    posting.job_id,
                    canonical.job_id,
                    score,
                )

        return kept, log

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

        # --- Create a fresh, timestamp-named run directory for stage logs ---
        run_dir = self._make_run_dir()

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

        # Stage 1 diagnostic log — all RawJobPosting fields for complete replay input.
        # Safety assertion: 01_fetch_raw.json must not already exist in this directory.
        # With timestamp-named directories this should never trigger; the assertion is
        # a last-resort guard against accidental overwrites.
        _raw_log_path = run_dir / "01_fetch_raw.json"
        if _raw_log_path.exists():
            raise RuntimeError(
                f"01_fetch_raw.json already exists in {run_dir} — "
                "this should never happen with timestamp-named run directories."
            )
        self._write_stage_log(
            run_dir,
            "01_fetch_raw.json",
            [
                {
                    "id": p.id,
                    "title": p.title,
                    "company": p.company,
                    "location": p.location,
                    "source": p.source.value,
                    "url": p.url,
                    "search_term": getattr(p, "search_term", None),
                    "description": getattr(p, "description", None),
                    "salary_raw": getattr(p, "salary_raw", None),
                    "salary_min_raw": getattr(p, "salary_min_raw", None),
                    "salary_max_raw": getattr(p, "salary_max_raw", None),
                    "salary_currency": getattr(p, "salary_currency", None),
                    "salary_interval": getattr(p, "salary_interval", None),
                    "posted_date": getattr(p, "posted_date", None),
                }
                for p in raw_postings
            ],
        )

        # ----------------------------------------------------------------
        # Stage 2: Normalize
        # ----------------------------------------------------------------
        _status("Normalizing postings…")
        normalized: list[JobPosting] = []
        malformed_title_count = 0
        for raw in raw_postings:
            try:
                job = self._normalizer.normalize(raw)
                if job is None:
                    # Fix 5: malformed title detected — skip this record
                    malformed_title_count += 1
                    continue
                normalized.append(job)
            except Exception as exc:
                logger.warning(
                    "ScrapeRunner: normalize failed for url=%s: %s",
                    getattr(raw, "url", "?"),
                    exc,
                )
        normalized_count = len(normalized)
        if malformed_title_count:
            _status(
                f"Normalized {normalized_count} postings "
                f"({malformed_title_count} dropped: malformed title)"
            )
        else:
            _status(f"Normalized {normalized_count} postings")

        # Stage 2 diagnostic log — Fix 2: include search_term, url, posted_at, description
        self._write_stage_log(
            run_dir,
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
                    "url": j.url,
                    "posted_at": j.posted_at.isoformat() if j.posted_at else None,
                    "description": j.description,
                    "search_term": j.search_term,
                }
                for j in normalized
            ],
        )

        # ----------------------------------------------------------------
        # Stage 3: Hard filter (cheap — runs before enrichment and dedup)
        # ----------------------------------------------------------------
        _status("Applying hard filters…")
        filter_result = self._filter_service.apply(
            normalized, self._config.filter_config
        )
        hard_filter_survivors = filter_result.kept
        _status(
            f"Hard filter: {filter_result.kept_count} kept, "
            f"{filter_result.excluded_count} excluded"
        )

        # Stage 3 diagnostic log
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
        self._write_stage_log(run_dir, "03_hard_filter.json", hard_filter_log)

        # ----------------------------------------------------------------
        # Stage 4: Title relevance filter (cheap — runs before enrichment and dedup)
        # ----------------------------------------------------------------
        _status("Applying title relevance filter…")
        title_kept: list[JobPosting] = []
        title_filter_log: list[dict] = []
        for job in hard_filter_survivors:
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
        title_filter_survivors = title_kept
        _status(
            f"Title filter: {len(title_filter_survivors)} kept, "
            f"{len(title_filter_log) - len(title_filter_survivors)} dropped"
        )
        self._write_stage_log(run_dir, "04_title_filter.json", title_filter_log)

        # ----------------------------------------------------------------
        # Stage 5: Cross-source dedup (NEW — catches same job on LinkedIn + Indeed)
        # ----------------------------------------------------------------
        _status("Running cross-source deduplication…")
        cross_source_survivors, cross_source_log = self._cross_source_dedup(
            title_filter_survivors
        )
        cross_source_dup_count = len(title_filter_survivors) - len(cross_source_survivors)
        _status(
            f"Cross-source dedup: {len(cross_source_survivors)} kept, "
            f"{cross_source_dup_count} within-batch duplicates removed"
        )
        self._write_stage_log(run_dir, "05_cross_source_dedup.json", cross_source_log)

        # ----------------------------------------------------------------
        # Stage 6: Cross-run dedup (against existing DB rows)
        # ----------------------------------------------------------------
        _status("Running cross-run deduplication…")
        # Load existing canonical jobs from DB (last N days)
        existing_jobs: list[JobPosting] = repository.list_jobs_for_dedup(
            self._engine,
            user_id=self._config.user_id,
            within_days=self._config.dedup_window_days,
        )

        canonical_jobs: list[JobPosting] = []
        dup_count = cross_source_dup_count  # start from cross-source dups already detected
        # Within-run URL dedup set — catches same URL from two sources in this batch
        seen_urls: set[str] = {j.url for j in existing_jobs}

        # Pending duplicates: (dup_job, canonical_job_id, match_type, match_score)
        # We collect them here and insert AFTER the canonical jobs are in the DB,
        # so the FK constraint on jobs.duplicate_of is satisfied.
        pending_duplicates: list[tuple[JobPosting, str, str, float | None]] = []

        for job in cross_source_survivors:
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
                # Canonical — add to seen set and candidate list
                seen_urls.add(job.url)
                existing_jobs.append(job)   # keep dedup set fresh within loop
                canonical_jobs.append(job)

        _status(
            f"Cross-run dedup complete: {len(canonical_jobs)} canonical, "
            f"{dup_count - cross_source_dup_count} cross-run duplicates"
        )

        # Stage 6 diagnostic log
        cross_run_dedup_log = []
        for job in cross_source_survivors:
            if job.duplicate_of:
                cross_run_dedup_log.append({
                    "job_id": job.job_id,
                    "title": job.title,
                    "decision": "duplicate",
                    "matched_job_id": job.duplicate_of,
                })
            elif job in canonical_jobs:
                cross_run_dedup_log.append({
                    "job_id": job.job_id,
                    "title": job.title,
                    "decision": "canonical",
                    "matched_job_id": None,
                })
            else:
                cross_run_dedup_log.append({
                    "job_id": job.job_id,
                    "title": job.title,
                    "decision": "within_run_dup",
                    "matched_job_id": None,
                })
        self._write_stage_log(run_dir, "06_cross_run_dedup.json", cross_run_dedup_log)

        # ----------------------------------------------------------------
        # Stage 7: Salary extraction + seniority enrichment
        #          (only runs on canonical survivors — expensive, so deferred)
        # ----------------------------------------------------------------
        _status("Extracting salary and seniority signals…")
        for job in canonical_jobs:
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

        # Stage 7 diagnostic log (post-enrichment, survivors only)
        self._write_stage_log(
            run_dir,
            "07_enriched.json",
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
                for j in canonical_jobs
            ],
        )

        kept_jobs = canonical_jobs

        # ----------------------------------------------------------------
        # Stage 8: Store canonical kept jobs + duplicates + classification stubs
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

        # Stage 8 (stored) diagnostic log
        self._write_stage_log(run_dir, "08_stored.json", list(stored_job_ids))

        # ----------------------------------------------------------------
        # Stage 9: Update scrape_run row with final counts + error log
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
            total_after_filters=len(kept_jobs),
            total_duplicates=dup_count,
            error_log=error_log,
        )

        result = ScrapeRunResult(
            run_id=run_id,
            fetched=fetched_count,
            normalized=normalized_count,
            after_dedup=len(canonical_jobs),
            duplicate_count=dup_count,
            after_filter=len(kept_jobs),
            stored=stored_count,
            classified_stub=classified_stub_count,
            run_dir=run_dir,
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
            "canonical=%d dups=%d stored=%d elapsed=%.2fs",
            run_id, fetched_count, normalized_count, len(canonical_jobs),
            dup_count, stored_count, elapsed,
        )
        return result

    def run_from_raw(
        self,
        raw_postings: list[RawJobPosting],
        status_callback: Optional[Callable[[str], None]] = None,
        source_run_dir: Optional[Path] = None,
    ) -> ScrapeRunResult:
        """
        Execute all post-processing pipeline stages starting from pre-loaded
        raw postings — skipping the live fetch entirely.

        Intended for replay runs where a saved 01_fetch_raw.json is loaded and
        re-processed with the current filter / dedup configuration.  A new
        scrape_run DB row is created so replay results are distinguishable from
        the original run.

        This method NEVER writes 01_fetch_raw.json — the source data already
        exists in source_run_dir and must not be modified.  Stage logs start
        from 02_normalized.json.  The output is always written to a fresh
        timestamp-named run directory, never back into source_run_dir.

        Args:
            raw_postings:    Pre-loaded list of RawJobPosting objects.
            status_callback: Optional callable(message: str) for progress updates.
            source_run_dir:  Path to the original run directory that was replayed.
                             When set, provenance is logged to replay_provenance.json
                             in the new run directory.

        Returns:
            ScrapeRunResult with per-stage counts, identical structure to run().
        """
        t_start = time.monotonic()

        def _status(msg: str) -> None:
            logger.info("ScrapeRunner(replay): %s", msg)
            if status_callback:
                status_callback(msg)

        # --- Create a fresh, timestamp-named run directory for stage logs ---
        # This is NEVER the same as source_run_dir.
        run_dir = self._make_run_dir()

        # --- Open scrape_run row ---
        run_id = repository.insert_scrape_run(
            self._engine,
            user_id=self._config.user_id,
            source_results_json="{}",
        )
        _status(f"Replay run #{run_id} started with {len(raw_postings)} loaded postings")

        fetched_count = len(raw_postings)

        # Write provenance metadata so the output directory is self-documenting.
        # 01_fetch_raw.json is intentionally NOT written here — the source data
        # lives in source_run_dir and must not be duplicated or overwritten.
        provenance = {
            "replay": True,
            "source_run_dir": str(source_run_dir) if source_run_dir else None,
            "source_fetch_raw": (
                str(source_run_dir / "01_fetch_raw.json") if source_run_dir else None
            ),
            "output_run_dir": str(run_dir),
            "replayed_at": datetime.now(timezone.utc).isoformat(),
            "raw_posting_count": fetched_count,
        }
        self._write_stage_log(run_dir, "replay_provenance.json", provenance)
        logger.info(
            "ScrapeRunner(replay): source=%s output=%s",
            source_run_dir,
            run_dir,
        )

        # ----------------------------------------------------------------
        # Stage 2: Normalize
        # ----------------------------------------------------------------
        _status("Normalizing postings…")
        normalized: list[JobPosting] = []
        malformed_title_count = 0
        for raw in raw_postings:
            try:
                job = self._normalizer.normalize(raw)
                if job is None:
                    # Fix 5: malformed title detected — skip this record
                    malformed_title_count += 1
                    continue
                normalized.append(job)
            except Exception as exc:
                logger.warning(
                    "ScrapeRunner(replay): normalize failed for url=%s: %s",
                    getattr(raw, "url", "?"),
                    exc,
                )
        normalized_count = len(normalized)
        if malformed_title_count:
            _status(
                f"Normalized {normalized_count} postings "
                f"({malformed_title_count} dropped: malformed title)"
            )
        else:
            _status(f"Normalized {normalized_count} postings")

        # Stage 2 diagnostic log — Fix 2: include search_term, url, posted_at, description
        self._write_stage_log(
            run_dir,
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
                    "url": j.url,
                    "posted_at": j.posted_at.isoformat() if j.posted_at else None,
                    "description": j.description,
                    "search_term": j.search_term,
                }
                for j in normalized
            ],
        )

        # ----------------------------------------------------------------
        # Stage 3: Hard filter (cheap — runs before enrichment and dedup)
        # ----------------------------------------------------------------
        _status("Applying hard filters…")
        filter_result = self._filter_service.apply(
            normalized, self._config.filter_config
        )
        hard_filter_survivors = filter_result.kept
        _status(
            f"Hard filter: {filter_result.kept_count} kept, "
            f"{filter_result.excluded_count} excluded"
        )

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
        self._write_stage_log(run_dir, "03_hard_filter.json", hard_filter_log)

        # ----------------------------------------------------------------
        # Stage 4: Title relevance filter (cheap — runs before enrichment and dedup)
        # ----------------------------------------------------------------
        _status("Applying title relevance filter…")
        title_kept: list[JobPosting] = []
        title_filter_log: list[dict] = []
        for job in hard_filter_survivors:
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
        title_filter_survivors = title_kept
        _status(
            f"Title filter: {len(title_filter_survivors)} kept, "
            f"{len(title_filter_log) - len(title_filter_survivors)} dropped"
        )
        self._write_stage_log(run_dir, "04_title_filter.json", title_filter_log)

        # ----------------------------------------------------------------
        # Stage 5: Cross-source dedup (NEW — catches same job on LinkedIn + Indeed)
        # ----------------------------------------------------------------
        _status("Running cross-source deduplication…")
        cross_source_survivors, cross_source_log = self._cross_source_dedup(
            title_filter_survivors
        )
        cross_source_dup_count = len(title_filter_survivors) - len(cross_source_survivors)
        _status(
            f"Cross-source dedup: {len(cross_source_survivors)} kept, "
            f"{cross_source_dup_count} within-batch duplicates removed"
        )
        self._write_stage_log(run_dir, "05_cross_source_dedup.json", cross_source_log)

        # ----------------------------------------------------------------
        # Stage 6: Cross-run dedup (against existing DB rows)
        # ----------------------------------------------------------------
        _status("Running cross-run deduplication…")
        existing_jobs: list[JobPosting] = repository.list_jobs_for_dedup(
            self._engine,
            user_id=self._config.user_id,
            within_days=self._config.dedup_window_days,
        )

        canonical_jobs: list[JobPosting] = []
        dup_count = cross_source_dup_count
        seen_urls: set[str] = {j.url for j in existing_jobs}
        pending_duplicates: list[tuple[JobPosting, str, str, float | None]] = []

        for job in cross_source_survivors:
            if job.url in seen_urls:
                logger.debug(
                    "ScrapeRunner(replay): within-run URL dup skipped: %s", job.url
                )
                dup_count += 1
                continue

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
                seen_urls.add(job.url)
                existing_jobs.append(job)
                canonical_jobs.append(job)

        _status(
            f"Cross-run dedup complete: {len(canonical_jobs)} canonical, "
            f"{dup_count - cross_source_dup_count} cross-run duplicates"
        )

        cross_run_dedup_log = []
        for job in cross_source_survivors:
            if job.duplicate_of:
                cross_run_dedup_log.append({
                    "job_id": job.job_id,
                    "title": job.title,
                    "decision": "duplicate",
                    "matched_job_id": job.duplicate_of,
                })
            elif job in canonical_jobs:
                cross_run_dedup_log.append({
                    "job_id": job.job_id,
                    "title": job.title,
                    "decision": "canonical",
                    "matched_job_id": None,
                })
            else:
                cross_run_dedup_log.append({
                    "job_id": job.job_id,
                    "title": job.title,
                    "decision": "within_run_dup",
                    "matched_job_id": None,
                })
        self._write_stage_log(run_dir, "06_cross_run_dedup.json", cross_run_dedup_log)

        # ----------------------------------------------------------------
        # Stage 7: Salary extraction + seniority enrichment
        #          (only runs on canonical survivors — expensive, so deferred)
        # ----------------------------------------------------------------
        _status("Extracting salary and seniority signals…")
        for job in canonical_jobs:
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
                logger.warning("ScrapeRunner(replay): salary extraction failed: %s", exc)

            try:
                job_level = getattr(job, "job_level", None)
                if job_level and job.seniority == SeniorityLevel.unknown:
                    job.seniority = self._seniority_inferrer.infer(
                        job.title, job_level=job_level
                    )
            except Exception as exc:
                logger.warning("ScrapeRunner(replay): seniority inference failed: %s", exc)

        self._write_stage_log(
            run_dir,
            "07_enriched.json",
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
                for j in canonical_jobs
            ],
        )

        kept_jobs = canonical_jobs

        # ----------------------------------------------------------------
        # Stage 8: Store canonical jobs + duplicates + classification stubs
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
                    "ScrapeRunner(replay): failed to store job_id=%s: %s", job.job_id, exc
                )
                continue

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
                    "ScrapeRunner(replay): failed to write classification stub for job_id=%s: %s",
                    job.job_id, exc,
                )

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
                    "ScrapeRunner(replay): failed to persist duplicate job_id=%s: %s",
                    dup_job.job_id, exc,
                )

        _status(f"Stored {stored_count} jobs, wrote {classified_stub_count} classification stubs")
        self._write_stage_log(run_dir, "08_stored.json", list(stored_job_ids))

        # ----------------------------------------------------------------
        # Stage 9: Update scrape_run row
        # ----------------------------------------------------------------
        elapsed = time.monotonic() - t_start
        repository.update_scrape_run_finished(
            self._engine,
            run_id=run_id,
            source_results_json='{"replay": {"status": "ok", "count": ' + str(fetched_count) + '}}',
            total_fetched=fetched_count,
            total_after_filters=len(kept_jobs),
            total_duplicates=dup_count,
            error_log=None,
        )

        result = ScrapeRunResult(
            run_id=run_id,
            fetched=fetched_count,
            normalized=normalized_count,
            after_dedup=len(canonical_jobs),
            duplicate_count=dup_count,
            after_filter=len(kept_jobs),
            stored=stored_count,
            classified_stub=classified_stub_count,
            run_dir=run_dir,
            rate_limited_sources=[],
            errors={},
            elapsed_seconds=round(elapsed, 2),
        )

        _status(
            f"Replay run #{run_id} complete — {stored_count} new jobs stored in "
            f"{elapsed:.1f}s"
        )
        logger.info(
            "ScrapeRunner.run_from_raw complete: run_id=%d fetched=%d normalized=%d "
            "canonical=%d dups=%d stored=%d elapsed=%.2fs",
            run_id, fetched_count, normalized_count, len(canonical_jobs),
            dup_count, stored_count, elapsed,
        )
        return result
