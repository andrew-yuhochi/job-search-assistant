"""
Unit tests for src/models/models.py — TASK-003.

Tests cover:
  - Valid construction of every model
  - Invalid construction (field validation errors)
  - Enum value checks
  - Cross-field validators (SalaryRange.max >= min; KnowledgeBankChunk.char_end > char_start)
  - Fixture round-trip: all 15 fixture entries parse as RawJobPosting without error

Run:
    pytest tests/unit/test_models.py -v
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.models.models import (
    ConfidenceLevel,
    HighlightDraft,
    JobPosting,
    JobState,
    KnowledgeBank,
    KnowledgeBankChunk,
    LocationPreference,
    NormalizedJobPosting,
    RawJobPosting,
    SalaryRange,
    SeniorityLevel,
    SignalEvent,
    SourceName,
    SpecialtyTier,
    SpecialtyType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOBS_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "jobs_fixtures.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_raw_job(**overrides) -> dict:
    base = {
        "id": "jf-test",
        "title": "Data Scientist",
        "company": "Acme",
        "location": "Vancouver, BC",
        "source": "linkedin",
        "url": "https://www.linkedin.com/jobs/view/1234",
        "description": "Build and deploy ML models.",
        "posted_date": "2026-04-20",
        "user_id": "local",
    }
    base.update(overrides)
    return base


def _minimal_job_posting(**overrides) -> dict:
    base = {
        "job_id": "abc123",
        "user_id": "local",
        "source": "linkedin",
        "url": "https://www.linkedin.com/jobs/view/1234",
        "url_hostname": "www.linkedin.com",
        "title": "Data Scientist",
        "title_normalized": "data scientist",
        "company": "Acme",
        "company_normalized": "acme",
        "description": "Build and deploy ML models.",
    }
    base.update(overrides)
    return base


# ===========================================================================
# 1. Enum value checks
# ===========================================================================


class TestEnumValues:
    def test_job_state_values(self):
        assert set(s.value for s in JobState) == {"new", "reviewed", "applied", "dismissed"}

    def test_source_name_values(self):
        assert set(s.value for s in SourceName) == {"linkedin", "indeed", "google"}

    def test_specialty_tier_values(self):
        assert SpecialtyTier.tier1.value == 1
        assert SpecialtyTier.tier2.value == 2
        assert SpecialtyTier.tier3.value == 3

    def test_seniority_level_values(self):
        expected = {"junior", "mid", "senior", "principal", "staff", "director", "vp", "csuite", "unknown"}
        assert set(s.value for s in SeniorityLevel) == expected

    def test_confidence_level_values(self):
        assert set(c.value for c in ConfidenceLevel) == {"high", "medium", "low"}

    def test_location_preference_values(self):
        assert set(lp.value for lp in LocationPreference) == {"vancouver", "remote_friendly", "both"}

    def test_job_state_is_string_enum(self):
        """JobState values must coerce to str for SQLite storage."""
        assert isinstance(JobState.new.value, str)

    def test_source_name_is_string_enum(self):
        assert isinstance(SourceName.linkedin.value, str)


# ===========================================================================
# 2. SalaryRange — valid and invalid construction
# ===========================================================================


class TestSalaryRange:
    def test_valid_construction(self):
        s = SalaryRange(min_cad=100_000, max_cad=130_000, source="regex")
        assert s.min_cad == 100_000
        assert s.max_cad == 130_000
        assert s.source == "regex"

    def test_equal_min_max_is_valid(self):
        s = SalaryRange(min_cad=120_000, max_cad=120_000, source="source_field")
        assert s.min_cad == s.max_cad

    def test_max_less_than_min_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            SalaryRange(min_cad=150_000, max_cad=100_000, source="regex")
        assert "max_cad" in str(exc_info.value)

    def test_negative_min_raises(self):
        with pytest.raises(ValidationError):
            SalaryRange(min_cad=-1, max_cad=100_000, source="regex")

    def test_source_field_roundtrip(self):
        for src in ("regex", "llm", "source_field"):
            s = SalaryRange(min_cad=80_000, max_cad=100_000, source=src)
            assert s.source == src


# ===========================================================================
# 3. RawJobPosting — valid and invalid construction
# ===========================================================================


class TestRawJobPosting:
    def test_valid_minimal(self):
        data = _minimal_raw_job()
        raw = RawJobPosting(**data)
        assert raw.title == "Data Scientist"
        assert raw.source == SourceName.linkedin
        assert raw.user_id == "local"

    def test_salary_raw_nullable(self):
        raw = RawJobPosting(**_minimal_raw_job(salary_raw=None))
        assert raw.salary_raw is None

    def test_salary_raw_string(self):
        raw = RawJobPosting(**_minimal_raw_job(salary_raw="$130,000 – $155,000 CAD"))
        assert "130" in raw.salary_raw

    def test_user_id_defaults_to_local(self):
        data = _minimal_raw_job()
        del data["user_id"]
        raw = RawJobPosting(**data)
        assert raw.user_id == "local"

    def test_invalid_source_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            RawJobPosting(**_minimal_raw_job(source="monster"))
        assert "source" in str(exc_info.value).lower()

    def test_google_is_valid_source(self):
        """'google' (not 'google_jobs') is the correct SourceName for Google Jobs."""
        raw = RawJobPosting(**_minimal_raw_job(source="google"))
        assert raw.source == SourceName.google

    def test_empty_url_raises(self):
        with pytest.raises(ValidationError):
            RawJobPosting(**_minimal_raw_job(url=""))

    def test_empty_description_raises(self):
        with pytest.raises(ValidationError):
            RawJobPosting(**_minimal_raw_job(description=""))

    def test_expected_specialty_is_optional(self):
        # present
        raw = RawJobPosting(**_minimal_raw_job(expected_specialty="data_scientist"))
        assert raw.expected_specialty == "data_scientist"
        # absent
        raw2 = RawJobPosting(**_minimal_raw_job())
        assert raw2.expected_specialty is None

    def test_indeed_source(self):
        raw = RawJobPosting(**_minimal_raw_job(source="indeed"))
        assert raw.source == SourceName.indeed


# ===========================================================================
# 4. RawJobPosting fixture round-trip
# ===========================================================================


class TestFixtureRoundTrip:
    def test_all_15_fixtures_parse(self):
        """Every entry in jobs_fixtures.json must parse as RawJobPosting without error.
        TASK-009: fixture count is 17 (15 canonical + 2 duplicates)."""
        with open(JOBS_FIXTURE, encoding="utf-8") as f:
            raw_entries = json.load(f)
        assert len(raw_entries) == 17, f"Expected 17, got {len(raw_entries)}"
        for entry in raw_entries:
            raw = RawJobPosting(**entry)
            assert raw.id is not None
            assert raw.source in list(SourceName)

    def test_fixture_sources_are_valid_enum_values(self):
        with open(JOBS_FIXTURE, encoding="utf-8") as f:
            raw_entries = json.load(f)
        valid = {s.value for s in SourceName}
        for entry in raw_entries:
            assert entry["source"] in valid, (
                f"Fixture id={entry['id']}: source={entry['source']!r} not in {valid}"
            )

    def test_fixture_user_ids_all_local(self):
        with open(JOBS_FIXTURE, encoding="utf-8") as f:
            raw_entries = json.load(f)
        for entry in raw_entries:
            raw = RawJobPosting(**entry)
            assert raw.user_id == "local"


# ===========================================================================
# 5. JobPosting — valid and invalid construction
# ===========================================================================


class TestJobPosting:
    def test_valid_construction(self):
        jp = JobPosting(**_minimal_job_posting())
        assert jp.state == JobState.new
        assert jp.user_id == "local"
        assert jp.seniority == SeniorityLevel.unknown

    def test_default_state_is_new(self):
        jp = JobPosting(**_minimal_job_posting())
        assert jp.state == JobState.new

    def test_state_applied(self):
        jp = JobPosting(**_minimal_job_posting(state="applied"))
        assert jp.state == JobState.applied

    def test_invalid_state_raises(self):
        with pytest.raises(ValidationError):
            JobPosting(**_minimal_job_posting(state="pending"))

    def test_duplicate_of_nullable(self):
        jp = JobPosting(**_minimal_job_posting(duplicate_of=None))
        assert jp.duplicate_of is None

    def test_duplicate_of_set(self):
        jp = JobPosting(**_minimal_job_posting(duplicate_of="parent-job-id"))
        assert jp.duplicate_of == "parent-job-id"

    def test_salary_fields_nullable(self):
        jp = JobPosting(**_minimal_job_posting())
        assert jp.salary_min_cad is None
        assert jp.salary_max_cad is None

    def test_salary_fields_set(self):
        jp = JobPosting(**_minimal_job_posting(salary_min_cad=120_000, salary_max_cad=150_000))
        assert jp.salary_min_cad == 120_000
        assert jp.salary_max_cad == 150_000


# ===========================================================================
# 6. KnowledgeBankChunk — valid and invalid
# ===========================================================================


class TestKnowledgeBankChunk:
    def _valid(self, **overrides) -> dict:
        base = {
            "source_path": "knowledge_bank_fixture.md",
            "section_heading": "## TU — Data Science",
            "char_start": 0,
            "char_end": 500,
            "content": "Led development of customer churn prediction model. " * 5,
        }
        base.update(overrides)
        return base

    def test_valid_construction(self):
        chunk = KnowledgeBankChunk(**self._valid())
        assert chunk.char_end > chunk.char_start

    def test_char_end_equals_start_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            KnowledgeBankChunk(**self._valid(char_start=100, char_end=100))
        assert "char_end" in str(exc_info.value)

    def test_char_end_less_than_start_raises(self):
        with pytest.raises(ValidationError):
            KnowledgeBankChunk(**self._valid(char_start=200, char_end=100))

    def test_empty_content_raises(self):
        with pytest.raises(ValidationError):
            KnowledgeBankChunk(**self._valid(content=""))

    def test_order_index_defaults_to_zero(self):
        chunk = KnowledgeBankChunk(**self._valid())
        assert chunk.order_index == 0

    def test_negative_char_start_raises(self):
        with pytest.raises(ValidationError):
            KnowledgeBankChunk(**self._valid(char_start=-1, char_end=100))


# ===========================================================================
# 7. KnowledgeBank
# ===========================================================================


class TestKnowledgeBank:
    def test_valid_construction(self):
        kb = KnowledgeBank(
            user_id="local",
            file_path="resume.md",
            full_text="Content here.",
            word_count=2,
        )
        assert kb.is_active is True
        assert kb.user_id == "local"
        assert kb.chunks == []

    def test_user_id_defaults_to_local(self):
        kb = KnowledgeBank(file_path="f.md", full_text="text", word_count=1)
        assert kb.user_id == "local"

    def test_with_chunks(self):
        chunk = KnowledgeBankChunk(
            source_path="f.md",
            section_heading="## Section 1",
            char_start=0,
            char_end=50,
            content="Fifty characters of meaningful knowledge bank text.",
        )
        kb = KnowledgeBank(file_path="f.md", full_text="x" * 50, word_count=5, chunks=[chunk])
        assert len(kb.chunks) == 1


# ===========================================================================
# 8. HighlightDraft
# ===========================================================================


class TestHighlightDraft:
    def test_valid_construction(self):
        draft = HighlightDraft(
            job_posting_id="job-123",
            bullets=["Developed a customer churn model achieving 85% AUC."],
        )
        assert draft.user_id == "local"
        assert draft.persisted_reason == "applied"

    def test_user_id_defaults_to_local(self):
        draft = HighlightDraft(
            job_posting_id="j1",
            bullets=["Led feature engineering for LTV model."],
        )
        assert draft.user_id == "local"

    def test_empty_bullets_raises(self):
        with pytest.raises(ValidationError):
            HighlightDraft(job_posting_id="j1", bullets=[])

    def test_citations_default_empty(self):
        draft = HighlightDraft(
            job_posting_id="j1",
            bullets=["Reduced model latency by 40%."],
        )
        assert draft.citations == []


# ===========================================================================
# 9. SpecialtyType
# ===========================================================================


class TestSpecialtyType:
    def test_default_source_is_seed(self):
        st = SpecialtyType(name="Data Scientist", tier=SpecialtyTier.tier1)
        assert st.source == "seed"
        assert st.enabled is True

    def test_valid_source_seed(self):
        st = SpecialtyType(name="Data Scientist", source="seed")
        assert st.source == "seed"

    def test_valid_source_config(self):
        st = SpecialtyType(name="Analytics Engineer", tier=SpecialtyTier.tier2, source="config")
        assert st.source == "config"

    def test_valid_source_proposed(self):
        st = SpecialtyType(name="MLOps Engineer", tier=SpecialtyTier.tier3, source="proposed")
        assert st.source == "proposed"

    def test_invalid_source_system_raises(self):
        """'system' was the old value — must now be rejected."""
        with pytest.raises(ValidationError) as exc_info:
            SpecialtyType(name="X", source="system")
        assert "source" in str(exc_info.value)

    def test_invalid_source_user_raises(self):
        """'user' was the old value — must now be rejected."""
        with pytest.raises(ValidationError) as exc_info:
            SpecialtyType(name="X", source="user")
        assert "source" in str(exc_info.value)

    def test_invalid_source_arbitrary_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            SpecialtyType(name="X", source="manual")
        assert "source" in str(exc_info.value)

    def test_empty_name_raises(self):
        with pytest.raises(ValidationError):
            SpecialtyType(name="")

    def test_duty_signals_default_empty(self):
        st = SpecialtyType(name="ML Engineer")
        assert st.duty_signals == []

    def test_tier1_is_default(self):
        st = SpecialtyType(name="Data Engineer")
        assert st.tier == SpecialtyTier.tier1

    def test_user_id_defaults_to_local(self):
        st = SpecialtyType(name="Data Analyst")
        assert st.user_id == "local"


# ===========================================================================
# 10. SignalEvent
# ===========================================================================


class TestSignalEvent:
    def test_valid_state_change(self):
        ev = SignalEvent(
            event_type="state_change",
            job_posting_id="job-001",
            from_state=JobState.new,
            to_state=JobState.reviewed,
        )
        assert ev.user_id == "local"
        assert ev.from_state == JobState.new

    def test_valid_detail_view_close(self):
        ev = SignalEvent(
            event_type="detail_view_close",
            job_posting_id="job-001",
            dwell_ms=8500,
        )
        assert ev.dwell_ms == 8500

    def test_invalid_event_type_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            SignalEvent(event_type="click", job_posting_id="j1")
        assert "event_type" in str(exc_info.value)

    def test_negative_dwell_ms_raises(self):
        with pytest.raises(ValidationError):
            SignalEvent(event_type="detail_view_close", dwell_ms=-1)

    def test_user_id_defaults_to_local(self):
        ev = SignalEvent(event_type="detail_view_open", job_posting_id="j1")
        assert ev.user_id == "local"

    def test_job_posting_id_optional(self):
        ev = SignalEvent(event_type="state_change")
        assert ev.job_posting_id is None

    def test_override_reason_set(self):
        ev = SignalEvent(
            event_type="override_inferred",
            job_posting_id="j1",
            override_reason="quick_dismiss_lt_15s",
        )
        assert ev.override_reason == "quick_dismiss_lt_15s"


# ===========================================================================
# 11. NormalizedJobPosting
# ===========================================================================


class TestNormalizedJobPosting:
    def test_valid_construction(self):
        njp = NormalizedJobPosting(
            id="jf-001",
            source=SourceName.linkedin,
            url="https://www.linkedin.com/jobs/view/001",
            title="Senior Data Scientist",
            company="Acme Corp",
            location="Vancouver, BC",
            description="Build ML models.",
            title_normalized="senior data scientist",
            company_normalized="acme corp",
            url_hostname="www.linkedin.com",
        )
        assert njp.seniority == SeniorityLevel.unknown
        assert njp.salary_range is None

    def test_user_id_defaults_to_local(self):
        njp = NormalizedJobPosting(
            id="x",
            source="linkedin",
            url="https://example.com",
            title="T",
            company="C",
            location="L",
            description="D",
            title_normalized="t",
            company_normalized="c",
            url_hostname="example.com",
        )
        assert njp.user_id == "local"
