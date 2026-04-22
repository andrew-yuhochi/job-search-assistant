"""
Tests for title_passes() and the Canada location pass-through.

Covers TASK-M4-000 acceptance criteria:
  - title_passes("QA Engineer") == False
  - title_passes("Data Scientist") == True
  - title_passes("Engineer II") == True (unknown title → passes to Haiku)
  - Allowlist wins over denylist when both match
  - Postings with location="Canada" pass the FilterService location filter
"""
from __future__ import annotations

import pytest

from src.models.models import JobPosting, SeniorityLevel, SourceName
from src.services.filter_service import FilterConfig, FilterService, title_passes


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def make_job(
    job_id: str = "tf-001",
    title: str = "Data Scientist",
    location: str = "Vancouver, BC",
    salary_min: int | None = None,
    salary_max: int | None = None,
) -> JobPosting:
    """Minimal JobPosting factory for filter tests."""
    return JobPosting(
        job_id=job_id,
        user_id="local",
        source=SourceName.linkedin,
        url=f"https://www.linkedin.com/jobs/view/{job_id}",
        url_hostname="www.linkedin.com",
        title=title,
        title_normalized=title.lower(),
        company="Test Corp",
        company_normalized="test corp",
        location=location,
        description="Test job description.",
        salary_min_cad=salary_min,
        salary_max_cad=salary_max,
        seniority=SeniorityLevel.unknown,
    )


# Fixture for make_job to allow pytest fixture-style injection in the Canada test.
@pytest.fixture
def make_job_fixture():
    return make_job


# ---------------------------------------------------------------------------
# title_passes() tests (≥7 cases)
# ---------------------------------------------------------------------------


def test_title_passes_allowlist():
    """'Senior Data Scientist' matches allowlist → True."""
    assert title_passes("Senior Data Scientist") is True


def test_title_passes_ml_engineer():
    """'Machine Learning Engineer' matches allowlist → True."""
    assert title_passes("Machine Learning Engineer") is True


def test_title_passes_data_analyst():
    """'Data Analyst' matches allowlist → True."""
    assert title_passes("Data Analyst") is True


def test_title_passes_denylist_qa():
    """'QA Engineer' matches denylist (\\bqa\\b) → False."""
    assert title_passes("QA Engineer") is False


def test_title_passes_denylist_backend():
    """'Backend Engineer' matches denylist → False."""
    assert title_passes("Backend Engineer") is False


def test_title_passes_unknown_passes():
    """'Engineer II' matches neither list → True (pass to Haiku)."""
    assert title_passes("Engineer II") is True


def test_title_passes_allowlist_wins_over_denylist():
    """'Data Scientist QA Lead' — allowlist match wins over denylist."""
    assert title_passes("Data Scientist QA Lead") is True


# Extra coverage for other allowlist/denylist patterns

def test_title_passes_nlp_engineer():
    assert title_passes("NLP Engineer") is True


def test_title_passes_devops_excluded():
    assert title_passes("DevOps Engineer") is False


def test_title_passes_marketing_excluded():
    assert title_passes("Marketing Manager") is False


def test_title_passes_applied_scientist():
    assert title_passes("Applied Scientist II") is True


def test_title_passes_empty_string_unknown_pass():
    """Empty title → no pattern match → True (ambiguous, passes to Haiku)."""
    assert title_passes("") is True


# ---------------------------------------------------------------------------
# Canada location pass-through test
# ---------------------------------------------------------------------------


def test_location_canada_passes(make_job_fixture):
    """Posting with location='Canada' should pass the location filter."""
    job = make_job_fixture(job_id="canada-001", location="Canada")
    config = FilterConfig()  # default metro_locations active
    result = FilterService().apply([job], config)
    assert result.kept_count == 1, (
        f"Expected 1 kept but got {result.kept_count}; "
        f"excluded: {[r for _, r in result.excluded]}"
    )


def test_location_canada_remote_passes(make_job_fixture):
    """Posting with location='Canada (Remote)' should also pass."""
    job = make_job_fixture(job_id="canada-002", location="Canada (Remote)")
    config = FilterConfig()
    result = FilterService().apply([job], config)
    assert result.kept_count == 1


def test_location_canada_with_province_passes(make_job_fixture):
    """Posting with location='Ontario, Canada' should pass."""
    job = make_job_fixture(job_id="canada-003", location="Ontario, Canada")
    config = FilterConfig()
    result = FilterService().apply([job], config)
    assert result.kept_count == 1
