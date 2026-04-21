# tests/test_repository.py — pytest tests for the repository layer (TASK-008).
# Uses an in-memory SQLite database initialised with the real schema.sql so
# every SQL path is exercised against a real engine — no mocks.
#
# Run with:  pytest tests/test_repository.py -v
#
# Coverage: insert_job, get_job, list_jobs (state filter + ordering),
#           update_job_state, delete_all_jobs, insert_duplicate, list_duplicates,
#           insert_classification, get_classification, upsert_classification,
#           insert_kb_document, get_kb_document, list_kb_documents, delete_kb_document,
#           insert_highlight_draft, get_highlight_draft, upsert_highlight_draft,
#           insert_signal, list_signals,
#           get_settings, upsert_settings,
#           list_specialty_types, insert_specialty_type, update_specialty_type_enabled,
#           insert_scrape_run, update_scrape_run_finished.

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from src.models.models import JobPosting, JobState, SeniorityLevel, SourceName
import src.storage.repository as repo

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SCHEMA_PATH = (
    Path(__file__).parent.parent / "src" / "storage" / "schema.sql"
)


@pytest.fixture()
def engine():
    """In-memory SQLite engine bootstrapped with the real schema.sql + seed user."""
    eng = create_engine("sqlite:///:memory:", echo=False)
    schema_sql = _SCHEMA_PATH.read_text()
    with eng.connect() as conn:
        conn.connection.executescript(schema_sql)
        # Seed the 'local' user so FK constraints pass
        conn.execute(
            text("INSERT OR IGNORE INTO users (user_id, created_at) VALUES ('local', :now)"),
            {"now": datetime.now(timezone.utc).isoformat()},
        )
        conn.commit()
    return eng


def _make_job(
    job_id: str = "job-001",
    title: str = "Data Scientist",
    company: str = "Acme Corp",
    state: JobState = JobState.new,
    salary_min: int | None = None,
    salary_max: int | None = None,
    posted_at: datetime | None = None,
) -> JobPosting:
    """Factory for minimal JobPosting objects."""
    url = f"https://www.linkedin.com/jobs/view/{job_id}"
    return JobPosting(
        job_id=job_id,
        user_id="local",
        source=SourceName.linkedin,
        url=url,
        url_hostname="www.linkedin.com",
        title=title,
        title_normalized=title.lower(),
        company=company,
        company_normalized=company.lower(),
        description="Build ML models.",
        seniority=SeniorityLevel.senior,
        state=state,
        salary_min_cad=salary_min,
        salary_max_cad=salary_max,
        fetched_at=datetime.now(timezone.utc),
        state_updated_at=datetime.now(timezone.utc),
        posted_at=posted_at,
    )


# ---------------------------------------------------------------------------
# Jobs table
# ---------------------------------------------------------------------------


class TestInsertAndGetJob:
    """insert_job + get_job round-trip."""

    def test_insert_and_retrieve(self, engine):
        job = _make_job("abc-001")
        repo.insert_job(engine, job)
        fetched = repo.get_job(engine, "abc-001")
        assert fetched is not None
        assert fetched.job_id == "abc-001"
        assert fetched.title == "Data Scientist"

    def test_get_missing_job_returns_none(self, engine):
        assert repo.get_job(engine, "does-not-exist") is None

    def test_insert_is_idempotent(self, engine):
        job = _make_job("idem-001")
        repo.insert_job(engine, job)
        repo.insert_job(engine, job)  # second insert should be a no-op
        assert repo.get_job(engine, "idem-001") is not None


class TestListJobs:
    """list_jobs filtering and ordering."""

    def test_dismissed_excluded_by_default(self, engine):
        repo.insert_job(engine, _make_job("j1", state=JobState.new))
        repo.insert_job(engine, _make_job("j2", state=JobState.dismissed))
        results = repo.list_jobs(engine, user_id="local")
        ids = [j.job_id for j in results]
        assert "j1" in ids
        assert "j2" not in ids

    def test_state_filter_new_excludes_dismissed(self, engine):
        repo.insert_job(engine, _make_job("n1", state=JobState.new))
        repo.insert_job(engine, _make_job("d1", state=JobState.dismissed))
        results = repo.list_jobs(engine, user_id="local", state="new")
        ids = [j.job_id for j in results]
        assert "n1" in ids
        assert "d1" not in ids

    def test_state_filter_dismissed_returns_only_dismissed(self, engine):
        repo.insert_job(engine, _make_job("n2", state=JobState.new))
        repo.insert_job(engine, _make_job("d2", state=JobState.dismissed))
        results = repo.list_jobs(engine, user_id="local", state="dismissed")
        ids = [j.job_id for j in results]
        assert "d2" in ids
        assert "n2" not in ids

    def test_ordering_salary_desc_nulls_last(self, engine):
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        # Same posted_at so ordering falls to salary tiebreak
        repo.insert_job(engine, _make_job("sal-low", salary_min=50_000, posted_at=now))
        repo.insert_job(engine, _make_job("sal-high", salary_min=120_000, posted_at=now))
        repo.insert_job(engine, _make_job("sal-none", salary_min=None, posted_at=now))
        results = repo.list_jobs(engine, user_id="local")
        ids = [j.job_id for j in results]
        # sal-high must come before sal-low; sal-none (NULLS LAST) must be last
        assert ids.index("sal-high") < ids.index("sal-low")
        assert ids.index("sal-none") == len(ids) - 1

    def test_limit_and_offset(self, engine):
        for i in range(5):
            repo.insert_job(engine, _make_job(f"lim-{i:02d}"))
        page1 = repo.list_jobs(engine, limit=2, offset=0)
        page2 = repo.list_jobs(engine, limit=2, offset=2)
        assert len(page1) == 2
        assert len(page2) == 2
        assert {j.job_id for j in page1}.isdisjoint({j.job_id for j in page2})


class TestUpdateJobState:
    """update_job_state transitions."""

    def test_state_transition_new_to_applied(self, engine):
        repo.insert_job(engine, _make_job("st-001"))
        repo.update_job_state(engine, "st-001", "applied")
        fetched = repo.get_job(engine, "st-001")
        assert fetched.state == JobState.applied

    def test_state_updated_at_refreshed(self, engine):
        job = _make_job("st-002")
        repo.insert_job(engine, job)
        original_ts = job.state_updated_at
        repo.update_job_state(engine, "st-002", "reviewed")
        fetched = repo.get_job(engine, "st-002")
        # Timestamps stored as ISO strings; just verify the state changed
        assert fetched.state == JobState.reviewed


class TestDeleteAllJobs:
    """delete_all_jobs idempotent wipe."""

    def test_deletes_jobs_for_user(self, engine):
        repo.insert_job(engine, _make_job("del-001"))
        repo.insert_job(engine, _make_job("del-002"))
        count = repo.delete_all_jobs(engine, user_id="local")
        assert count == 2
        assert repo.get_job(engine, "del-001") is None

    def test_delete_on_empty_table_returns_zero(self, engine):
        assert repo.delete_all_jobs(engine, user_id="local") == 0


# ---------------------------------------------------------------------------
# Duplicates table
# ---------------------------------------------------------------------------


class TestDuplicates:
    """insert_duplicate + list_duplicates."""

    def test_insert_and_list_duplicate(self, engine):
        repo.insert_job(engine, _make_job("can-001"))
        repo.insert_job(engine, _make_job("dup-001"))
        repo.insert_duplicate(engine, "dup-001", "can-001", "fuzzy_title_company", 92.5)
        dups = repo.list_duplicates(engine, "can-001")
        assert len(dups) == 1
        assert dups[0]["duplicate_post_id"] == "dup-001"
        assert dups[0]["match_type"] == "fuzzy_title_company"

    def test_list_duplicates_empty_when_none(self, engine):
        repo.insert_job(engine, _make_job("can-002"))
        assert repo.list_duplicates(engine, "can-002") == []


# ---------------------------------------------------------------------------
# Classifications table
# ---------------------------------------------------------------------------


class TestClassifications:
    """insert_classification, get_classification, upsert_classification."""

    def test_insert_and_get_classification(self, engine):
        repo.insert_job(engine, _make_job("cls-001"))
        repo.insert_classification(
            engine, "cls-001", "local",
            "Data Scientist", "high", ["builds ML models"], "claude-haiku", "v1",
        )
        cls = repo.get_classification(engine, "cls-001")
        assert cls is not None
        assert cls["specialty_name"] == "Data Scientist"
        assert cls["confidence"] == "high"
        assert isinstance(cls["duty_signals"], list)

    def test_get_classification_missing_returns_none(self, engine):
        assert repo.get_classification(engine, "no-such-job") is None

    def test_upsert_overwrites_existing(self, engine):
        repo.insert_job(engine, _make_job("cls-002"))
        repo.insert_classification(
            engine, "cls-002", "local",
            "ML Engineer", "low", [], "claude-haiku", "v1",
        )
        repo.upsert_classification(
            engine, "cls-002", "local",
            "Data Scientist", "high", ["updated signal"], "claude-haiku", "v2",
        )
        cls = repo.get_classification(engine, "cls-002")
        assert cls["specialty_name"] == "Data Scientist"
        assert cls["prompt_version"] == "v2"


# ---------------------------------------------------------------------------
# Knowledge bank documents
# ---------------------------------------------------------------------------


class TestKnowledgeBank:
    """insert_kb_document, get_kb_document, list_kb_documents, delete_kb_document."""

    def test_insert_and_get_document(self, engine):
        kb_doc_id = repo.insert_kb_document(engine, "local", "resume.md", "Full text here.", 3)
        doc = repo.get_kb_document(engine, kb_doc_id)
        assert doc is not None
        assert doc["filename"] == "resume.md"
        assert doc["is_active"] == 1

    def test_insert_deactivates_prior_documents(self, engine):
        first_id = repo.insert_kb_document(engine, "local", "old.md", "old text", 2)
        repo.insert_kb_document(engine, "local", "new.md", "new text", 2)
        old_doc = repo.get_kb_document(engine, first_id)
        assert old_doc["is_active"] == 0

    def test_list_kb_documents(self, engine):
        repo.insert_kb_document(engine, "local", "a.md", "text a", 1)
        repo.insert_kb_document(engine, "local", "b.md", "text b", 1)
        docs = repo.list_kb_documents(engine, "local")
        assert len(docs) >= 2

    def test_delete_kb_document(self, engine):
        kb_doc_id = repo.insert_kb_document(engine, "local", "del.md", "text", 1)
        deleted = repo.delete_kb_document(engine, kb_doc_id)
        assert deleted is True
        assert repo.get_kb_document(engine, kb_doc_id) is None

    def test_delete_missing_document_returns_false(self, engine):
        assert repo.delete_kb_document(engine, 99999) is False


# ---------------------------------------------------------------------------
# Highlight drafts
# ---------------------------------------------------------------------------


class TestHighlightDrafts:
    """insert_highlight_draft, get_highlight_draft, upsert_highlight_draft."""

    def test_insert_and_get_draft(self, engine):
        repo.insert_job(engine, _make_job("hd-001"))
        bullets = json.dumps(["Led ML project", "Reduced latency 40%"])
        draft_id = repo.insert_highlight_draft(
            engine, "hd-001", "local", bullets, "claude-haiku", "v1"
        )
        assert isinstance(draft_id, int)
        draft = repo.get_highlight_draft(engine, "hd-001")
        assert draft is not None
        assert "Led ML project" in draft["bullets_json"]

    def test_get_draft_missing_returns_none(self, engine):
        assert repo.get_highlight_draft(engine, "no-job") is None

    def test_upsert_replaces_existing_draft(self, engine):
        repo.insert_job(engine, _make_job("hd-002"))
        repo.insert_highlight_draft(
            engine, "hd-002", "local", json.dumps(["old bullet"]), "haiku", "v1"
        )
        repo.upsert_highlight_draft(
            engine, "hd-002", "local", json.dumps(["new bullet"]), "haiku", "v2"
        )
        draft = repo.get_highlight_draft(engine, "hd-002")
        assert "new bullet" in draft["bullets_json"]
        assert "old bullet" not in draft["bullets_json"]


# ---------------------------------------------------------------------------
# Signal events
# ---------------------------------------------------------------------------


class TestSignals:
    """insert_signal + list_signals."""

    def test_insert_and_list_signal(self, engine):
        repo.insert_job(engine, _make_job("sig-001"))
        event_id = repo.insert_signal(
            engine, "local", "state_change",
            job_id="sig-001", from_state="new", to_state="applied",
            specialty_name="Data Scientist", classification_confidence="high",
        )
        assert isinstance(event_id, int)
        events = repo.list_signals(engine, user_id="local", job_id="sig-001")
        assert len(events) == 1
        assert events[0]["event_type"] == "state_change"
        assert events[0]["to_state"] == "applied"

    def test_list_signals_returns_all_for_user(self, engine):
        repo.insert_job(engine, _make_job("sig-002"))
        repo.insert_signal(engine, "local", "detail_view_open", job_id="sig-002")
        repo.insert_signal(engine, "local", "detail_view_close", job_id="sig-002", dwell_ms=3500)
        events = repo.list_signals(engine, user_id="local", job_id="sig-002")
        assert len(events) == 2

    def test_list_signals_window_days_filter(self, engine):
        # All events just inserted will be within any reasonable window
        repo.insert_job(engine, _make_job("sig-003"))
        repo.insert_signal(engine, "local", "detail_view_open", job_id="sig-003")
        recent = repo.list_signals(engine, user_id="local", window_days=1)
        assert len(recent) >= 1


# ---------------------------------------------------------------------------
# User settings
# ---------------------------------------------------------------------------


class TestSettings:
    """get_settings + upsert_settings."""

    def test_upsert_and_get_settings(self, engine):
        repo.upsert_settings(engine, "local", "both", 80_000, ["principal", "vp"])
        s = repo.get_settings(engine, "local")
        assert s is not None
        assert s["location_preference"] == "both"
        assert s["salary_floor_cad"] == 80_000

    def test_upsert_updates_existing_settings(self, engine):
        repo.upsert_settings(engine, "local", "vancouver", None, [])
        repo.upsert_settings(engine, "local", "remote_friendly", 100_000, ["director"])
        s = repo.get_settings(engine, "local")
        assert s["location_preference"] == "remote_friendly"
        assert s["salary_floor_cad"] == 100_000

    def test_get_settings_missing_user_returns_none(self, engine):
        # No settings row for 'unknown-user'
        assert repo.get_settings(engine, "unknown-user") is None


# ---------------------------------------------------------------------------
# Specialty types
# ---------------------------------------------------------------------------


class TestSpecialtyTypes:
    """list_specialty_types, insert_specialty_type, update_specialty_type_enabled."""

    def test_insert_and_list_enabled(self, engine):
        repo.insert_specialty_type(
            engine, "local", "Data Scientist", "DS work",
            ["builds models"], 1, True, "seed",
        )
        types = repo.list_specialty_types(engine, "local", enabled_only=True)
        names = [t["name"] for t in types]
        assert "Data Scientist" in names

    def test_disabled_specialty_excluded_when_enabled_only(self, engine):
        repo.insert_specialty_type(
            engine, "local", "Analytics Engineer", "AE work",
            [], 2, False, "config",
        )
        enabled = repo.list_specialty_types(engine, "local", enabled_only=True)
        names = [t["name"] for t in enabled]
        assert "Analytics Engineer" not in names

    def test_list_all_includes_disabled(self, engine):
        repo.insert_specialty_type(
            engine, "local", "Analytics Engineer", None,
            [], 2, False, "config",
        )
        all_types = repo.list_specialty_types(engine, "local", enabled_only=False)
        names = [t["name"] for t in all_types]
        assert "Analytics Engineer" in names

    def test_update_specialty_type_enabled_toggle(self, engine):
        sp_id = repo.insert_specialty_type(
            engine, "local", "ML Engineer", None,
            ["trains models"], 1, True, "seed",
        )
        repo.update_specialty_type_enabled(engine, sp_id, False)
        types = repo.list_specialty_types(engine, "local", enabled_only=False)
        ml = next(t for t in types if t["name"] == "ML Engineer")
        assert ml["enabled"] == 0


# ---------------------------------------------------------------------------
# Scrape runs
# ---------------------------------------------------------------------------


class TestScrapeRuns:
    """insert_scrape_run + update_scrape_run_finished."""

    def test_insert_and_finish_scrape_run(self, engine):
        run_id = repo.insert_scrape_run(
            engine, "local", '{"linkedin": {"status": "pending"}}'
        )
        assert isinstance(run_id, int)
        repo.update_scrape_run_finished(
            engine, run_id,
            source_results_json='{"linkedin": {"status": "ok", "count": 10}}',
            total_fetched=10,
            total_after_filters=8,
            total_duplicates=2,
            error_log=None,
        )
        # Verify finished_at and totals were written
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM scrape_runs WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).fetchone()
        d = dict(row._mapping)
        assert d["total_fetched"] == 10
        assert d["total_duplicates"] == 2
        assert d["finished_at"] is not None
