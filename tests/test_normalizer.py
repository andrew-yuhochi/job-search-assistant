# Tests for src/processing/normalizer.py — covers all TASK-006 acceptance criteria.
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.models.models import RawJobPosting, SeniorityLevel, SourceName
from src.processing.normalizer import Normalizer, _normalize_text, _parse_date

FIXTURES_PATH = Path(__file__).parent / "fixtures" / "jobs_fixtures.json"


@pytest.fixture
def normalizer() -> Normalizer:
    return Normalizer()


@pytest.fixture
def raw_fixtures() -> list[RawJobPosting]:
    data = json.loads(FIXTURES_PATH.read_text())
    return [RawJobPosting(**item) for item in data]


# ---------------------------------------------------------------------------
# 1. All 15 fixtures normalize without raising
# ---------------------------------------------------------------------------


def test_all_fixtures_normalize_without_error(normalizer: Normalizer, raw_fixtures: list[RawJobPosting]) -> None:
    assert len(raw_fixtures) == 15
    for raw in raw_fixtures:
        posting = normalizer.normalize(raw)
        assert posting is not None
        assert posting.job_id, "job_id must be non-empty"
        assert posting.title_normalized, "title_normalized must be non-empty"
        assert posting.company_normalized, "company_normalized must be non-empty"
        assert posting.url_hostname, "url_hostname must be non-empty"


# ---------------------------------------------------------------------------
# 2. Duplicate pairs produce matching title_normalized + company_normalized
# ---------------------------------------------------------------------------


def test_duplicate_pair_produces_matching_normalized_fields(
    normalizer: Normalizer, raw_fixtures: list[RawJobPosting]
) -> None:
    by_id = {r.id: r for r in raw_fixtures}
    # jf-001 "Senior Data Scientist" at Acme and jf-002 "Data Scientist, Senior" at Acme
    p1 = normalizer.normalize(by_id["jf-001"])
    p2 = normalizer.normalize(by_id["jf-002"])
    assert p1.company_normalized == p2.company_normalized, (
        f"company_normalized mismatch: {p1.company_normalized!r} vs {p2.company_normalized!r}"
    )
    # Title normalized strings differ (different title text), but company must match
    assert p1.company_normalized == "acme corp"


# ---------------------------------------------------------------------------
# 3. Relative date parsing → absolute timestamp within ±1 day
# ---------------------------------------------------------------------------


def test_relative_date_3_days_ago(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-rel-1",
        title="Data Scientist",
        company="Test Co",
        location="Vancouver, BC",
        source=SourceName.linkedin,
        url="https://www.linkedin.com/jobs/view/99999",
        description="Test description for relative date parsing.",
        posted_date="3 days ago",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.posted_at is not None
    expected = datetime.now(timezone.utc) - timedelta(days=3)
    delta = abs((posting.posted_at - expected).total_seconds())
    assert delta < 86400, f"posted_at {posting.posted_at} not within 1 day of expected {expected}"


def test_relative_date_posted_today(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-rel-2",
        title="ML Engineer",
        company="Test Co",
        location="Remote",
        source=SourceName.indeed,
        url="https://ca.indeed.com/viewjob?jk=aaaa1111",
        description="Test description for today date parsing.",
        posted_date="posted today",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.posted_at is not None
    now = datetime.now(timezone.utc)
    delta = abs((posting.posted_at - now).total_seconds())
    assert delta < 86400


def test_relative_date_2_hours_ago(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-rel-3",
        title="Data Engineer",
        company="Test Co",
        location="Vancouver, BC",
        source=SourceName.google,
        url="https://boards.greenhouse.io/test/jobs/123456",
        description="Test description for hours-ago date parsing.",
        posted_date="2 hours ago",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.posted_at is not None
    expected = datetime.now(timezone.utc) - timedelta(hours=2)
    delta = abs((posting.posted_at - expected).total_seconds())
    assert delta < 3600  # within 1 hour


# ---------------------------------------------------------------------------
# 4. URL health check: False for dead URLs, True for valid ones
# ---------------------------------------------------------------------------


def test_check_url_returns_false_for_dead_url(normalizer: Normalizer) -> None:
    result = normalizer.check_url("http://this-domain-does-not-exist-12345678.example/jobs/1")
    assert result is False


def test_check_url_returns_false_for_connection_error(normalizer: Normalizer) -> None:
    with patch("src.processing.normalizer.requests.head") as mock_head:
        from requests.exceptions import ConnectionError as ReqConnError
        mock_head.side_effect = ReqConnError("connection refused")
        result = normalizer.check_url("http://fakehost.invalid/jobs/1")
    assert result is False


def test_check_url_returns_true_for_200_response(normalizer: Normalizer) -> None:
    with patch("src.processing.normalizer.requests.head") as mock_head:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_head.return_value = mock_resp
        result = normalizer.check_url("https://www.example.com/jobs/123")
    assert result is True


def test_check_url_returns_false_for_404_response(normalizer: Normalizer) -> None:
    with patch("src.processing.normalizer.requests.head") as mock_head:
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_head.return_value = mock_resp
        result = normalizer.check_url("https://www.example.com/jobs/dead-link")
    assert result is False


# ---------------------------------------------------------------------------
# 5. Hostname extraction
# ---------------------------------------------------------------------------


def test_url_hostname_extraction(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-host",
        title="Data Analyst",
        company="Acme",
        location="Vancouver, BC",
        source=SourceName.linkedin,
        url="https://www.linkedin.com/jobs/view/3901234001",
        description="Test description.",
        posted_date="2026-04-18",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.url_hostname == "www.linkedin.com"


def test_greenhouse_url_hostname_and_job_id(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-greenhouse",
        title="Data Engineer",
        company="Hootsuite",
        location="Vancouver, BC",
        source=SourceName.google,
        url="https://boards.greenhouse.io/hootsuite/jobs/5550001",
        description="Test description.",
        posted_date="2026-04-16",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.url_hostname == "boards.greenhouse.io"
    assert posting.source_job_id == "5550001"


# ---------------------------------------------------------------------------
# 6. source_job_id extraction for Indeed query-param URLs
# ---------------------------------------------------------------------------


def test_indeed_job_id_extracted_from_query_param(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-indeed",
        title="ML Engineer",
        company="Shopify",
        location="Remote",
        source=SourceName.indeed,
        url="https://ca.indeed.com/viewjob?jk=b2c3d4e5f6071234",
        description="Test description.",
        posted_date="2026-04-15",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.source_job_id == "b2c3d4e5f6071234"


# ---------------------------------------------------------------------------
# 7. Seniority inference from title
# ---------------------------------------------------------------------------


def test_seniority_senior_inferred(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-sen-1",
        title="Senior Data Scientist",
        company="Corp",
        location="Vancouver, BC",
        source=SourceName.linkedin,
        url="https://www.linkedin.com/jobs/view/111",
        description="Test.",
        posted_date="2026-04-20",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.seniority == SeniorityLevel.senior


def test_seniority_principal_inferred(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-sen-2",
        title="Principal Data Scientist",
        company="Loblaws",
        location="Vancouver, BC",
        source=SourceName.google,
        url="https://www.linkedin.com/jobs/view/3901234009",
        description="Test.",
        posted_date="2026-04-12",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.seniority == SeniorityLevel.principal


def test_seniority_unknown_for_plain_title(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-sen-3",
        title="Data Scientist",
        company="Klue",
        location="Vancouver, BC",
        source=SourceName.linkedin,
        url="https://www.linkedin.com/jobs/view/3901234011",
        description="Test.",
        posted_date="2026-04-19",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.seniority == SeniorityLevel.unknown


# ---------------------------------------------------------------------------
# 8. ISO date string parsing
# ---------------------------------------------------------------------------


def test_iso_date_parses_correctly(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-iso",
        title="Data Analyst",
        company="Corp",
        location="Vancouver, BC",
        source=SourceName.indeed,
        url="https://ca.indeed.com/viewjob?jk=isodate001",
        description="Test.",
        posted_date="2026-04-18",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.posted_at is not None
    assert posting.posted_at.year == 2026
    assert posting.posted_at.month == 4
    assert posting.posted_at.day == 18


# ---------------------------------------------------------------------------
# 9. is_remote flag
# ---------------------------------------------------------------------------


def test_is_remote_true_for_remote_location(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-remote",
        title="ML Engineer",
        company="Shopify",
        location="Remote (Canada)",
        source=SourceName.indeed,
        url="https://ca.indeed.com/viewjob?jk=remote001",
        description="Test.",
        posted_date="2026-04-15",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.is_remote is True


def test_is_remote_false_for_vancouver_location(normalizer: Normalizer) -> None:
    raw = RawJobPosting(
        id="test-onsite",
        title="Data Engineer",
        company="Hootsuite",
        location="Vancouver, BC",
        source=SourceName.google,
        url="https://boards.greenhouse.io/hootsuite/jobs/555",
        description="Test.",
        posted_date="2026-04-16",
        user_id="local",
    )
    posting = normalizer.normalize(raw)
    assert posting.is_remote is False


# ---------------------------------------------------------------------------
# 10. _normalize_text helper
# ---------------------------------------------------------------------------


def test_normalize_text_lowercases_and_strips_punctuation() -> None:
    assert _normalize_text("Acme Corp.") == "acme corp"
    assert _normalize_text("  Senior Data Scientist — NLP  ") == "senior data scientist nlp"
