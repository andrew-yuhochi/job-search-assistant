"""
Tests for the JobSource plugin layer: base classes, three concrete sources,
and JobSourceRegistry.  Per TASK-011 Acceptance Criteria:
  - ≥6 tests with mocked responses
  - 1 live smoke test skippable via SKIP_LIVE=1

Run mocked suite:
    SKIP_LIVE=1 pytest tests/test_sources.py -v

Run all including live smoke test (requires SERPAPI_API_KEY in env):
    pytest tests/test_sources.py -v
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import MagicMock, patch

import pandas as pd

from src.models import RawJobPosting, SourceName
from src.sources.base import FetchResult, JobSource, RateLimitError, SearchQuery
from src.sources.google_jobs import GoogleJobsSource, _serpapi_job_to_raw
from src.sources.indeed import IndeedSource
from src.sources.linkedin import LinkedInSource, _row_to_raw
from src.sources.registry import JobSourceRegistry


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

SKIP_LIVE = os.environ.get("SKIP_LIVE", "0") == "1"


def _make_df_row(
    *,
    id: str = "job123",
    title: str = "Data Scientist",
    company: str = "ACME Corp",
    location: str = "Vancouver, BC, Canada",
    description: str = "Looking for a data scientist.",
    job_url: str = "https://www.linkedin.com/jobs/view/job123",
    date_posted: str = "2026-04-21",
    min_amount=None,
    max_amount=None,
    currency: str = "CAD",
) -> pd.Series:
    """Return a minimal jobspy-shaped pandas Series for testing."""
    return pd.Series(
        {
            "id": id,
            "title": title,
            "company": company,
            "location": location,
            "description": description,
            "job_url": job_url,
            "job_url_direct": None,
            "date_posted": date_posted,
            "min_amount": min_amount,
            "max_amount": max_amount,
            "currency": currency,
            "is_remote": False,
            "job_level": "mid senior level",
        }
    )


def _make_serpapi_job(
    *,
    title: str = "ML Engineer",
    company_name: str = "TechCo",
    location: str = "Vancouver, BC",
    job_id: str = "abc123",
) -> dict:
    """Return a minimal SerpAPI Google Jobs result dict for testing."""
    return {
        "title": title,
        "company_name": company_name,
        "location": location,
        "job_id": job_id,
        "description": "Join our ML team.",
        "job_highlights": [
            {"title": "Responsibilities", "items": ["Build ML models", "Deploy pipelines"]}
        ],
        "related_links": [{"text": "Apply", "link": "https://techco.com/careers/abc123"}],
        "detected_extensions": {"posted_at": "3 days ago", "salary": "$120K/yr"},
    }


# ---------------------------------------------------------------------------
# Test 1 — RateLimitError carries the source name
# ---------------------------------------------------------------------------


def test_rate_limit_error_carries_source_name():
    exc = RateLimitError("linkedin")
    assert exc.source_name == "linkedin"
    assert "linkedin" in str(exc)


# ---------------------------------------------------------------------------
# Test 2 — _row_to_raw converts a DataFrame row to RawJobPosting (LinkedIn)
# ---------------------------------------------------------------------------


def test_row_to_raw_linkedin():
    row = _make_df_row(id="789", title="Senior DS", company="BigCo")
    posting = _row_to_raw(row, SourceName.linkedin)
    assert isinstance(posting, RawJobPosting)
    assert posting.source == SourceName.linkedin
    assert posting.title == "Senior DS"
    assert posting.company == "BigCo"
    assert posting.id == "789"


# ---------------------------------------------------------------------------
# Test 3 — _row_to_raw converts a DataFrame row to RawJobPosting (Indeed)
# ---------------------------------------------------------------------------


def test_row_to_raw_indeed():
    row = _make_df_row(id="456", title="Data Analyst", company="LocalCo")
    posting = _row_to_raw(row, SourceName.indeed)
    assert posting.source == SourceName.indeed
    assert posting.title == "Data Analyst"


# ---------------------------------------------------------------------------
# Test 4 — _serpapi_job_to_raw converts SerpAPI result to RawJobPosting
# ---------------------------------------------------------------------------


def test_serpapi_job_to_raw():
    raw_job = _make_serpapi_job(title="ML Engineer", company_name="TechCo")
    posting = _serpapi_job_to_raw(raw_job)
    assert isinstance(posting, RawJobPosting)
    assert posting.source == SourceName.google
    assert posting.title == "ML Engineer"
    assert posting.company == "TechCo"
    assert posting.salary_raw == "$120K/yr"
    assert "Build ML models" in posting.description


# ---------------------------------------------------------------------------
# Test 5 — LinkedInSource raises RateLimitError on 429
# ---------------------------------------------------------------------------


def test_linkedin_raises_rate_limit_error_on_429():
    source = LinkedInSource()
    # linkedin.py imports scrape_jobs at module level; patch it there
    with patch("src.sources.linkedin.scrape_jobs", side_effect=Exception("HTTP 429 too many requests")):
        with pytest.raises(RateLimitError) as exc_info:
            source.fetch(SearchQuery(search_term="data scientist", results_wanted=5))
    assert exc_info.value.source_name == "linkedin"


# ---------------------------------------------------------------------------
# Test 6 — LinkedInSource returns empty list on empty DataFrame
# ---------------------------------------------------------------------------


def test_linkedin_empty_dataframe_returns_empty_list():
    source = LinkedInSource()
    empty_df = pd.DataFrame()
    # linkedin.py imports scrape_jobs at module level; patch it there
    with patch("src.sources.linkedin.scrape_jobs", return_value=empty_df):
        result = source.fetch(SearchQuery(search_term="data scientist", results_wanted=5))
    assert result == []


# ---------------------------------------------------------------------------
# Test 7 — IndeedSource raises RateLimitError on 429
# ---------------------------------------------------------------------------


def test_indeed_raises_rate_limit_error_on_429():
    source = IndeedSource()
    # indeed.py re-imports scrape_jobs from linkedin at module level; patch indeed's reference
    with patch("src.sources.indeed.scrape_jobs", side_effect=Exception("rate limit exceeded 429")):
        with pytest.raises(RateLimitError) as exc_info:
            source.fetch(SearchQuery(search_term="data scientist", results_wanted=5))
    assert exc_info.value.source_name == "indeed"


# ---------------------------------------------------------------------------
# Test 8 — GoogleJobsSource returns empty list when API returns no results
# ---------------------------------------------------------------------------


def test_google_jobs_empty_results():
    source = GoogleJobsSource(api_key="test_key")
    mock_search_instance = MagicMock()
    mock_search_instance.get_dict.return_value = {"jobs_results": []}

    # google_jobs.py imports GoogleSearch at module level; patch it there
    with patch("src.sources.google_jobs.GoogleSearch", return_value=mock_search_instance):
        result = source.fetch(SearchQuery(search_term="data scientist", results_wanted=5))
    assert result == []


# ---------------------------------------------------------------------------
# Test 9 — GoogleJobsSource raises RateLimitError on quota error
# ---------------------------------------------------------------------------


def test_google_jobs_raises_rate_limit_error_on_quota():
    source = GoogleJobsSource(api_key="test_key")
    mock_search_instance = MagicMock()
    mock_search_instance.get_dict.return_value = {
        "error": "Your plan does not allow quota: plan limit reached"
    }

    # google_jobs.py imports GoogleSearch at module level; patch it there
    with patch("src.sources.google_jobs.GoogleSearch", return_value=mock_search_instance):
        with pytest.raises(RateLimitError) as exc_info:
            source.fetch(SearchQuery(search_term="data scientist", results_wanted=5))
    assert exc_info.value.source_name == "google"


# ---------------------------------------------------------------------------
# Test 10 — GoogleJobsSource.is_available() returns False when key missing
# ---------------------------------------------------------------------------


def test_google_jobs_is_available_false_without_key():
    source = GoogleJobsSource(api_key=None)
    # Make sure env var is not set for this test
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("SERPAPI_API_KEY", None)
        source2 = GoogleJobsSource(api_key=None)
        assert source2.is_available() is False


# ---------------------------------------------------------------------------
# Test 11 — JobSourceRegistry.fetch_all() returns FetchResult per source
# ---------------------------------------------------------------------------


def test_registry_fetch_all_returns_fetch_result_per_source():
    registry = JobSourceRegistry()

    # Build two mock sources
    mock_source_a = MagicMock(spec=JobSource)
    mock_source_a.name = "mock_a"
    mock_source_a.is_available.return_value = True
    mock_source_a.fetch.return_value = [
        RawJobPosting(
            id="a1", title="DS", company="Co", location="Vancouver",
            source=SourceName.linkedin, url="https://example.com/a1",
            description="desc", posted_date="2026-04-21",
        )
    ]

    mock_source_b = MagicMock(spec=JobSource)
    mock_source_b.name = "mock_b"
    mock_source_b.is_available.return_value = True
    mock_source_b.fetch.return_value = []

    registry.register(mock_source_a)
    registry.register(mock_source_b)

    query = SearchQuery(search_term="data scientist")
    results = registry.fetch_all(query)

    assert "mock_a" in results
    assert "mock_b" in results
    assert isinstance(results["mock_a"], FetchResult)
    assert results["mock_a"].status == "ok"
    assert results["mock_a"].count == 1
    assert results["mock_b"].status == "ok"
    assert results["mock_b"].count == 0


# ---------------------------------------------------------------------------
# Test 12 — Registry isolates one source failure; others succeed
# ---------------------------------------------------------------------------


def test_registry_isolates_source_failure():
    registry = JobSourceRegistry()

    failing_source = MagicMock(spec=JobSource)
    failing_source.name = "failing"
    failing_source.is_available.return_value = True
    failing_source.fetch.side_effect = RuntimeError("scraper exploded")

    ok_source = MagicMock(spec=JobSource)
    ok_source.name = "ok_source"
    ok_source.is_available.return_value = True
    ok_source.fetch.return_value = []

    registry.register(failing_source)
    registry.register(ok_source)

    results = registry.fetch_all(SearchQuery(search_term="data scientist"))

    assert results["failing"].status == "error"
    assert "scraper exploded" in results["failing"].error
    assert results["ok_source"].status == "ok"


# ---------------------------------------------------------------------------
# Test 13 — Registry records unavailable sources as error, skips fetch()
# ---------------------------------------------------------------------------


def test_registry_skips_unavailable_source():
    registry = JobSourceRegistry()

    unavailable = MagicMock(spec=JobSource)
    unavailable.name = "no_key"
    unavailable.is_available.return_value = False

    registry.register(unavailable)
    results = registry.fetch_all(SearchQuery(search_term="data scientist"))

    assert results["no_key"].status == "error"
    unavailable.fetch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 14 — Registry captures RateLimitError as rate_limited status
# ---------------------------------------------------------------------------


def test_registry_captures_rate_limit_as_status():
    registry = JobSourceRegistry()

    rl_source = MagicMock(spec=JobSource)
    rl_source.name = "rl_source"
    rl_source.is_available.return_value = True
    rl_source.fetch.side_effect = RateLimitError("rl_source")

    registry.register(rl_source)
    results = registry.fetch_all(SearchQuery(search_term="data scientist"))

    assert results["rl_source"].status == "rate_limited"


# ---------------------------------------------------------------------------
# Live smoke test — skippable via SKIP_LIVE=1
# ---------------------------------------------------------------------------


@pytest.mark.skipif(SKIP_LIVE, reason="SKIP_LIVE=1 — skipping live sources test")
def test_live_smoke_all_sources_vancouver_ds():
    """
    Live smoke test: hit all three sources with a real Vancouver DS query.
    Passes as long as each source returns a FetchResult without crashing.
    Empty results are acceptable (rate limits / no postings are handled).

    Skip with: SKIP_LIVE=1 pytest tests/test_sources.py
    """
    registry = JobSourceRegistry()
    registry.register(LinkedInSource())
    registry.register(IndeedSource())
    registry.register(GoogleJobsSource())  # needs SERPAPI_API_KEY

    query = SearchQuery(
        search_term="data scientist",
        location="Vancouver, BC, Canada",
        results_wanted=5,
        hours_old=72,
    )
    results = registry.fetch_all(query)

    for source_name, result in results.items():
        assert isinstance(result, FetchResult), f"{source_name}: not a FetchResult"
        assert result.status in ("ok", "rate_limited", "error"), \
            f"{source_name}: unexpected status {result.status!r}"
        print(f"  {source_name}: status={result.status} count={result.count} error={result.error}")
