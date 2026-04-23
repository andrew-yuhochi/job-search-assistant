"""
Tests for ScrapeRunner (TASK-013).

Covers:
  - rate-limit handling (source surfaces as rate_limited; other sources continue)
  - within-run URL dedup logic
  - cross-run dedup via DedupService
  - scrape_runs row creation and final-count update
  - classification stub insertion (specialty='Unclassified')
  - progress callback invocation
  - filter integration (excluded jobs do not reach storage)
  - error source handling (source errors are recorded, others continue)

Run:
    SKIP_LIVE=1 pytest tests/test_runner.py -v
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Callable, Optional
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy import create_engine, text

from src.models.models import JobPosting, JobState, SeniorityLevel, SourceName
from src.runner.scrape_runner import ScrapeConfig, ScrapeRunner, ScrapeRunResult
from src.services.filter_service import FilterConfig, FilterResult, FilterService
from src.sources.base import FetchResult, RateLimitError, SearchQuery
from src.sources.registry import JobSourceRegistry
from src.storage import repository

# ---------------------------------------------------------------------------
# In-memory SQLite engine for isolation
# ---------------------------------------------------------------------------

_SCHEMA_SQL = (
    __import__("pathlib").Path(__file__).parent.parent
    / "src" / "storage" / "schema.sql"
)


def _make_engine():
    """Create a fresh in-memory SQLite engine with the full schema applied."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    schema = _SCHEMA_SQL.read_text(encoding="utf-8")
    with engine.connect() as conn:
        conn.connection.executescript(schema)
        conn.commit()
    # Seed the local user required for FK constraints
    with engine.connect() as conn:
        conn.execute(
            text("INSERT OR IGNORE INTO users (user_id, created_at) VALUES ('local', :ts)"),
            {"ts": datetime.now(timezone.utc).isoformat()},
        )
        conn.commit()
    return engine


# ---------------------------------------------------------------------------
# Sample data helpers
# ---------------------------------------------------------------------------

def _make_job_posting(
    *,
    job_id: str = "job-001",
    title: str = "Data Scientist",
    company: str = "ACME Corp",
    source: SourceName = SourceName.linkedin,
    url: str = "https://www.linkedin.com/jobs/view/001",
    description: str = "Looking for a data scientist with ML experience.",
    location: str = "Vancouver, BC",
    salary_min_cad: Optional[int] = None,
    salary_max_cad: Optional[int] = None,
    seniority: SeniorityLevel = SeniorityLevel.mid,
    duplicate_of: Optional[str] = None,
) -> JobPosting:
    return JobPosting(
        job_id=job_id,
        user_id="local",
        source=source,
        url=url,
        url_hostname="www.linkedin.com",
        title=title,
        title_normalized=title.lower(),
        company=company,
        company_normalized=company.lower(),
        location=location,
        is_remote=False,
        description=description,
        salary_min_cad=salary_min_cad,
        salary_max_cad=salary_max_cad,
        seniority=seniority,
        duplicate_of=duplicate_of,
    )


def _make_raw_posting(
    *,
    job_id: str = "raw-001",
    title: str = "Data Scientist",
    company: str = "ACME Corp",
    url: str = "https://www.linkedin.com/jobs/view/001",
    source: SourceName = SourceName.linkedin,
    description: str = "Looking for a data scientist with ML experience.",
):
    from src.models.models import RawJobPosting
    return RawJobPosting(
        id=job_id,
        title=title,
        company=company,
        location="Vancouver, BC",
        source=source,
        url=url,
        description=description,
        posted_date="2026-04-21",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def engine():
    return _make_engine()


@pytest.fixture()
def query():
    return SearchQuery(search_term="data scientist", location="Vancouver, BC, Canada")


# ---------------------------------------------------------------------------
# Helper: build a ScrapeRunner backed by a fully-mocked registry and
# real FilterService + real engine.
# ---------------------------------------------------------------------------

def _build_runner(
    engine,
    fetch_results: dict[str, FetchResult],
    normalized_jobs: list[JobPosting],
    *,
    existing_db_jobs: list[JobPosting] | None = None,
    filter_config: FilterConfig | None = None,
    dedup_results: dict[str, bool] | None = None,
) -> tuple[ScrapeRunner, list[str]]:
    """
    Build a ScrapeRunner with mocked registry and normalizer.
    Returns (runner, progress_messages).
    """
    # Mock registry
    mock_registry = MagicMock(spec=JobSourceRegistry)
    mock_registry.fetch_all.return_value = fetch_results

    # Mock normalizer to return pre-built JobPosting objects
    # (one per raw posting in order)
    call_counter = {"n": 0}

    def _normalize_side_effect(raw):
        idx = call_counter["n"]
        call_counter["n"] += 1
        return normalized_jobs[idx] if idx < len(normalized_jobs) else normalized_jobs[-1]

    # Mock dedup so every job is canonical by default (no cross-run matches)
    from src.services.dedup import DedupResult, DedupService
    mock_dedup = MagicMock(spec=DedupService)
    if dedup_results:
        # dedup_results maps job_id → is_duplicate
        def _check_side_effect(job, existing):
            is_dup = dedup_results.get(job.job_id, False)
            if is_dup:
                canonical_id = existing[0].job_id if existing else "canonical-0"
                return DedupResult(
                    is_duplicate=True,
                    canonical_job_id=canonical_id,
                    match_type="fuzzy_title_company",
                    match_score=92.0,
                )
            return DedupResult(is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None)
        mock_dedup.check.side_effect = _check_side_effect
    else:
        from src.services.dedup import DedupResult
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None
        )

    runner = ScrapeRunner(
        registry=mock_registry,
        engine=engine,
        filter_service=FilterService(config=filter_config or FilterConfig()),
        config=ScrapeConfig(user_id="local"),
    )
    # Patch normalizer and dedup at instance level
    runner._normalizer = MagicMock()
    runner._normalizer.normalize.side_effect = _normalize_side_effect
    runner._dedup_service = mock_dedup
    # Patch salary/seniority extractors to no-ops
    runner._salary_extractor = MagicMock()
    runner._salary_extractor.extract.return_value = (None, None, "unknown")
    runner._seniority_inferrer = MagicMock()

    # Patch list_jobs_for_dedup to return existing_db_jobs
    with patch(
        "src.runner.scrape_runner.repository.list_jobs_for_dedup",
        return_value=existing_db_jobs or [],
    ):
        progress: list[str] = []
        result = runner.run(query=SearchQuery("data scientist"), status_callback=progress.append)

    return result, progress


# ===========================================================================
# Test 1: scrape_runs row is created and populated with correct counts
# ===========================================================================

def test_scrape_run_row_created_and_populated(engine):
    """
    ScrapeRunner.run() must insert a scrape_runs row and update it with
    final counts (total_fetched, total_after_filters, total_duplicates,
    finished_at).
    """
    job = _make_job_posting(job_id="job-a", url="https://www.linkedin.com/jobs/view/a")
    raw = _make_raw_posting(job_id="raw-a", url="https://www.linkedin.com/jobs/view/a")

    fetch_results = {
        "linkedin": FetchResult(source_name="linkedin", postings=[raw], status="ok"),
    }

    with patch("src.runner.scrape_runner.repository.list_jobs_for_dedup", return_value=[]):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        from src.services.dedup import DedupResult
        mock_dedup = MagicMock()
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None
        )

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            config=ScrapeConfig(user_id="local"),
        )
        runner._normalizer = MagicMock()
        runner._normalizer.normalize.return_value = job
        runner._dedup_service = mock_dedup
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        result = runner.run(SearchQuery("data scientist"))

    # Verify result object
    assert result.run_id >= 1
    assert result.fetched == 1
    assert result.stored == 1
    assert result.classified_stub == 1
    assert result.elapsed_seconds >= 0.0

    # Verify scrape_runs table
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM scrape_runs WHERE run_id = :rid"),
            {"rid": result.run_id},
        ).fetchone()

    assert row is not None, "scrape_runs row missing"
    d = dict(row._mapping)
    assert d["total_fetched"] == 1
    assert d["total_after_filters"] == 1
    assert d["total_duplicates"] == 0
    assert d["finished_at"] is not None, "finished_at not populated"


# ===========================================================================
# Test 2: rate-limited source is recorded; other sources continue
# ===========================================================================

def test_rate_limited_source_does_not_abort_run(engine):
    """
    When LinkedIn returns RateLimitError, the run continues and other
    sources' results are processed.  The result must list LinkedIn in
    rate_limited_sources, and stored count reflects the non-rate-limited data.
    """
    job_indeed = _make_job_posting(
        job_id="job-i", source=SourceName.indeed,
        url="https://www.indeed.com/jobs?jk=aabbcc",
    )
    raw_indeed = _make_raw_posting(
        job_id="raw-i", source=SourceName.indeed,
        url="https://www.indeed.com/jobs?jk=aabbcc",
    )

    fetch_results = {
        "linkedin": FetchResult(
            source_name="linkedin",
            postings=[],
            status="rate_limited",
            error="HTTP 429",
        ),
        "indeed": FetchResult(
            source_name="indeed",
            postings=[raw_indeed],
            status="ok",
        ),
    }

    with patch("src.runner.scrape_runner.repository.list_jobs_for_dedup", return_value=[]):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        from src.services.dedup import DedupResult
        mock_dedup = MagicMock()
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None
        )

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            config=ScrapeConfig(user_id="local"),
        )
        runner._normalizer = MagicMock()
        runner._normalizer.normalize.return_value = job_indeed
        runner._dedup_service = mock_dedup
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        result = runner.run(SearchQuery("data scientist"))

    assert "linkedin" in result.rate_limited_sources
    assert result.stored == 1   # Indeed posting stored
    assert result.fetched == 1  # Only Indeed raw posting counted


# ===========================================================================
# Test 3: within-run URL dedup removes duplicate postings from same batch
# ===========================================================================

def test_within_run_url_dedup(engine):
    """
    Two raw postings with the same URL in a single fetch result must yield
    only 1 canonical job stored.  The duplicate count must be ≥ 1.
    """
    same_url = "https://www.linkedin.com/jobs/view/999"
    raw1 = _make_raw_posting(job_id="raw-1", url=same_url)
    raw2 = _make_raw_posting(job_id="raw-2", url=same_url)

    job1 = _make_job_posting(job_id="job-1", url=same_url)
    job2 = _make_job_posting(job_id="job-2", url=same_url)

    fetch_results = {
        "linkedin": FetchResult(
            source_name="linkedin",
            postings=[raw1, raw2],
            status="ok",
        ),
    }

    normalize_calls = [job1, job2]
    idx = {"n": 0}

    def _norm(raw):
        j = normalize_calls[idx["n"]]
        idx["n"] += 1
        return j

    with patch("src.runner.scrape_runner.repository.list_jobs_for_dedup", return_value=[]):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        from src.services.dedup import DedupResult
        mock_dedup = MagicMock()
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None
        )

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            config=ScrapeConfig(user_id="local"),
        )
        runner._normalizer = MagicMock()
        runner._normalizer.normalize.side_effect = _norm
        runner._dedup_service = mock_dedup
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        result = runner.run(SearchQuery("data scientist"))

    # Only 1 canonical should be stored; 1 within-run dup detected
    assert result.stored == 1
    assert result.duplicate_count >= 1


# ===========================================================================
# Test 4: cross-run dedup via DedupService flags duplicates
# ===========================================================================

def test_cross_run_dedup_via_dedup_service(engine):
    """
    When DedupService.check() flags a posting as a duplicate, it must not
    be stored as a canonical job.  duplicate_count increments correctly.
    """
    canonical_url = "https://www.linkedin.com/jobs/view/canonical"
    dup_url = "https://www.linkedin.com/jobs/view/dup-fuzzy"

    canonical_job = _make_job_posting(job_id="job-canonical", url=canonical_url)
    dup_job = _make_job_posting(job_id="job-dup", url=dup_url)

    raw_dup = _make_raw_posting(job_id="raw-dup", url=dup_url)

    fetch_results = {
        "linkedin": FetchResult(
            source_name="linkedin",
            postings=[raw_dup],
            status="ok",
        ),
    }

    from src.services.dedup import DedupResult

    with patch(
        "src.runner.scrape_runner.repository.list_jobs_for_dedup",
        return_value=[canonical_job],  # simulate existing canonical in DB
    ):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        mock_dedup = MagicMock()
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=True,
            canonical_job_id="job-canonical",
            match_type="fuzzy_title_company",
            match_score=95.0,
        )

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            config=ScrapeConfig(user_id="local"),
        )
        runner._normalizer = MagicMock()
        runner._normalizer.normalize.return_value = dup_job
        runner._dedup_service = mock_dedup
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        result = runner.run(SearchQuery("data scientist"))

    assert result.stored == 0            # dup is not stored as canonical
    assert result.duplicate_count >= 1   # flagged as duplicate


# ===========================================================================
# Test 5: classification stub rows are written with specialty='Unclassified'
# ===========================================================================

def test_classification_stub_inserted_for_each_stored_job(engine):
    """
    For each job that passes filters and is stored, a classifications row
    with specialty_name='Unclassified' must be written.

    Each job must have a distinct title+company fingerprint so the cross-source
    dedup stage (Stage 5) does not merge them into a single cluster.
    """
    distinct_titles = ["Data Scientist", "Machine Learning Engineer", "Applied Scientist"]
    distinct_companies = ["Alpha Corp", "Beta Inc", "Gamma Ltd"]
    jobs = [
        _make_job_posting(
            job_id=f"job-{i}",
            url=f"https://www.linkedin.com/jobs/view/{i}",
            title=distinct_titles[i],
            company=distinct_companies[i],
        )
        for i in range(3)
    ]
    raws = [
        _make_raw_posting(job_id=f"raw-{i}", url=f"https://www.linkedin.com/jobs/view/{i}")
        for i in range(3)
    ]

    fetch_results = {
        "linkedin": FetchResult(source_name="linkedin", postings=raws, status="ok"),
    }

    idx = {"n": 0}

    def _norm(raw):
        j = jobs[idx["n"]]
        idx["n"] += 1
        return j

    from src.services.dedup import DedupResult

    with patch("src.runner.scrape_runner.repository.list_jobs_for_dedup", return_value=[]):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        mock_dedup = MagicMock()
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None
        )

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            config=ScrapeConfig(user_id="local"),
        )
        runner._normalizer = MagicMock()
        runner._normalizer.normalize.side_effect = _norm
        runner._dedup_service = mock_dedup
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        result = runner.run(SearchQuery("data scientist"))

    assert result.classified_stub == 3

    # Verify DB rows
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT specialty_name FROM classifications WHERE user_id = 'local'")
        ).fetchall()

    specialties = [r[0] for r in rows]
    assert len(specialties) == 3
    assert all(s == "Unclassified" for s in specialties), (
        f"Expected all 'Unclassified', got: {specialties}"
    )


# ===========================================================================
# Test 6: progress callback is called at key stages
# ===========================================================================

def test_progress_callback_called_at_each_stage(engine):
    """
    status_callback must be invoked with meaningful messages at each
    major pipeline stage (fetch, normalize, dedup, filter, store, complete).
    """
    job = _make_job_posting(job_id="job-cb", url="https://www.linkedin.com/jobs/view/cb")
    raw = _make_raw_posting(job_id="raw-cb", url="https://www.linkedin.com/jobs/view/cb")

    fetch_results = {
        "linkedin": FetchResult(source_name="linkedin", postings=[raw], status="ok"),
    }

    from src.services.dedup import DedupResult

    messages: list[str] = []

    with patch("src.runner.scrape_runner.repository.list_jobs_for_dedup", return_value=[]):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        mock_dedup = MagicMock()
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None
        )

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            config=ScrapeConfig(user_id="local"),
        )
        runner._normalizer = MagicMock()
        runner._normalizer.normalize.return_value = job
        runner._dedup_service = mock_dedup
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        runner.run(SearchQuery("data scientist"), status_callback=messages.append)

    joined = " | ".join(messages).lower()
    assert "started" in joined or "run #" in joined, "Missing 'started' message"
    assert "fetch" in joined, "Missing fetch stage message"
    assert "normaliz" in joined, "Missing normalize stage message"
    assert "dedup" in joined, "Missing dedup stage message"
    assert "filter" in joined, "Missing filter stage message"
    assert "stor" in joined, "Missing storage stage message"
    assert "complete" in joined or "done" in joined, "Missing completion message"


# ===========================================================================
# Test 7: filtered-out jobs are not stored
# ===========================================================================

def test_filtered_jobs_are_not_stored(engine):
    """
    Jobs that fail hard filters (e.g. salary below floor) must not appear
    in the jobs table.  stored count is 0; after_filter is 0.
    """
    # Job has known salary below the floor
    job = _make_job_posting(
        job_id="job-cheap",
        url="https://www.linkedin.com/jobs/view/cheap",
        salary_min_cad=50_000,  # below $120K floor
        salary_max_cad=70_000,
    )
    raw = _make_raw_posting(job_id="raw-cheap", url="https://www.linkedin.com/jobs/view/cheap")

    fetch_results = {
        "linkedin": FetchResult(source_name="linkedin", postings=[raw], status="ok"),
    }

    # FilterConfig with $120K salary floor
    filter_config = FilterConfig(
        min_salary_cad=120_000.0,
        locations=["Vancouver"],
        allow_remote=True,
    )

    from src.services.dedup import DedupResult

    with patch("src.runner.scrape_runner.repository.list_jobs_for_dedup", return_value=[]):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        mock_dedup = MagicMock()
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None
        )

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            filter_service=FilterService(),
            config=ScrapeConfig(user_id="local", filter_config=filter_config),
        )
        runner._normalizer = MagicMock()
        runner._normalizer.normalize.return_value = job
        runner._dedup_service = mock_dedup
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        result = runner.run(SearchQuery("data scientist"))

    assert result.stored == 0
    assert result.after_filter == 0

    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM jobs WHERE job_id = 'job-cheap'")
        ).scalar()
    assert count == 0, "Filtered-out job should not be in DB"


# ===========================================================================
# Test 8: error source is recorded in result; other sources still run
# ===========================================================================

def test_error_source_recorded_run_continues(engine):
    """
    When a source returns status='error', the error is captured in
    result.errors and the run_id is still populated. Other sources continue.
    """
    job = _make_job_posting(job_id="job-google-ok", url="https://www.google.com/jobs/001",
                             source=SourceName.google)
    raw = _make_raw_posting(job_id="raw-google", url="https://www.google.com/jobs/001",
                             source=SourceName.google)

    fetch_results = {
        "linkedin": FetchResult(
            source_name="linkedin",
            postings=[],
            status="error",
            error="Connection timeout",
        ),
        "google": FetchResult(
            source_name="google",
            postings=[raw],
            status="ok",
        ),
    }

    from src.services.dedup import DedupResult

    with patch("src.runner.scrape_runner.repository.list_jobs_for_dedup", return_value=[]):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        mock_dedup = MagicMock()
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None
        )

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            config=ScrapeConfig(user_id="local"),
        )
        runner._normalizer = MagicMock()
        runner._normalizer.normalize.return_value = job
        runner._dedup_service = mock_dedup
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        result = runner.run(SearchQuery("data scientist"))

    assert "linkedin" in result.errors
    assert result.run_id >= 1
    assert result.stored == 1  # Google posting stored


# ===========================================================================
# Test 9: source_results_json in scrape_runs reflects per-source status
# ===========================================================================

def test_scrape_run_source_results_json_populated(engine):
    """
    After run() completes, the scrape_runs row's source_results_json must
    be a valid JSON object containing an entry for each dispatched source.
    """
    job = _make_job_posting(job_id="job-j", url="https://www.linkedin.com/jobs/view/j")
    raw = _make_raw_posting(job_id="raw-j", url="https://www.linkedin.com/jobs/view/j")

    fetch_results = {
        "linkedin": FetchResult(source_name="linkedin", postings=[raw], status="ok"),
        "indeed": FetchResult(source_name="indeed", postings=[], status="ok"),
    }

    from src.services.dedup import DedupResult

    with patch("src.runner.scrape_runner.repository.list_jobs_for_dedup", return_value=[]):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        mock_dedup = MagicMock()
        mock_dedup.check.return_value = DedupResult(
            is_duplicate=False, canonical_job_id=None, match_type=None, match_score=None
        )

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            config=ScrapeConfig(user_id="local"),
        )
        runner._normalizer = MagicMock()
        runner._normalizer.normalize.return_value = job
        runner._dedup_service = mock_dedup
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        result = runner.run(SearchQuery("data scientist"))

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT source_results_json FROM scrape_runs WHERE run_id = :rid"),
            {"rid": result.run_id},
        ).fetchone()

    payload = json.loads(row[0])
    assert "linkedin" in payload, "linkedin missing from source_results_json"
    assert "indeed" in payload, "indeed missing from source_results_json"
    assert payload["linkedin"]["status"] == "ok"
    assert payload["indeed"]["status"] == "ok"


# ===========================================================================
# Test 10: ScrapeRunResult.elapsed_seconds is a non-negative float
# ===========================================================================

def test_elapsed_seconds_is_non_negative(engine):
    """
    The elapsed_seconds field of ScrapeRunResult must be a non-negative float.
    """
    fetch_results = {
        "linkedin": FetchResult(source_name="linkedin", postings=[], status="ok"),
    }

    from src.services.dedup import DedupResult

    with patch("src.runner.scrape_runner.repository.list_jobs_for_dedup", return_value=[]):
        mock_registry = MagicMock(spec=JobSourceRegistry)
        mock_registry.fetch_all.return_value = fetch_results

        runner = ScrapeRunner(
            registry=mock_registry,
            engine=engine,
            config=ScrapeConfig(user_id="local"),
        )
        runner._normalizer = MagicMock()
        runner._dedup_service = MagicMock()
        runner._salary_extractor = MagicMock()
        runner._salary_extractor.extract.return_value = (None, None, "unknown")
        runner._seniority_inferrer = MagicMock()

        result = runner.run(SearchQuery("data scientist"))

    assert isinstance(result.elapsed_seconds, float)
    assert result.elapsed_seconds >= 0.0
