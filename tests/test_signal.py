# tests/test_signal.py — pytest tests for SignalService and signal_events wiring (TASK-010).
# Uses an in-memory SQLite DB bootstrapped with the real schema.sql (same pattern as
# test_repository.py — no mocks, real SQL).
#
# Run with:  pytest tests/test_signal.py -v

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from src.models.models import JobPosting, JobState, SeniorityLevel, SourceName
from src.services.signal_service import SignalService
import src.storage.repository as repo

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SCHEMA_PATH = Path(__file__).parent.parent / "src" / "storage" / "schema.sql"


@pytest.fixture()
def engine():
    """In-memory SQLite engine with schema.sql + seeded local user."""
    eng = create_engine("sqlite:///:memory:", echo=False)
    schema_sql = _SCHEMA_PATH.read_text()
    with eng.connect() as conn:
        conn.connection.executescript(schema_sql)
        conn.execute(
            text("INSERT OR IGNORE INTO users (user_id, created_at) VALUES ('local', :now)"),
            {"now": datetime.now(timezone.utc).isoformat()},
        )
        conn.commit()
    return eng


def _make_job(job_id: str = "job-001", state: JobState = JobState.new) -> JobPosting:
    url = f"https://www.linkedin.com/jobs/view/{job_id}"
    return JobPosting(
        job_id=job_id,
        user_id="local",
        source=SourceName.linkedin,
        url=url,
        url_hostname="www.linkedin.com",
        title="Data Scientist",
        title_normalized="data scientist",
        company="Acme Corp",
        company_normalized="acme corp",
        description="Build models.",
        seniority=SeniorityLevel.senior,
        state=state,
    )


# ---------------------------------------------------------------------------
# TEST 1: SignalService.record writes a row with correct event_type
# ---------------------------------------------------------------------------

def test_record_writes_signal_event(engine):
    """SignalService.record inserts a row into signal_events."""
    job = _make_job("job-001")
    repo.insert_job(engine, job)

    event_id = SignalService.record(
        engine=engine,
        job_id="job-001",
        event_type="detail_view_open",
    )

    assert isinstance(event_id, int) and event_id > 0
    rows = repo.list_signals(engine, user_id="local", job_id="job-001")
    assert len(rows) == 1
    assert rows[0]["event_type"] == "detail_view_open"
    assert rows[0]["job_id"] == "job-001"


# ---------------------------------------------------------------------------
# TEST 2: record_state_change stores from_state / to_state
# ---------------------------------------------------------------------------

def test_record_state_change_stores_states(engine):
    """record_state_change writes from_state and to_state correctly."""
    job = _make_job("job-002")
    repo.insert_job(engine, job)

    SignalService.record_state_change(
        engine=engine,
        job_id="job-002",
        from_state="new",
        to_state="reviewed",
        specialty_name="Data Scientist",
        classification_confidence="high",
    )

    rows = repo.list_signals(engine, user_id="local", job_id="job-002")
    assert len(rows) == 1
    r = rows[0]
    assert r["event_type"] == "state_change"
    assert r["from_state"] == "new"
    assert r["to_state"] == "reviewed"
    assert r["specialty_name"] == "Data Scientist"
    assert r["classification_confidence"] == "high"
    assert r["override_reason"] is None  # reviewed is not an override


# ---------------------------------------------------------------------------
# TEST 3: mark_dismissed writes mark_dismissed + state_change signal events
# ---------------------------------------------------------------------------

def test_mark_dismissed_writes_two_events(engine):
    """Dismissing a job writes both mark_dismissed and state_change events."""
    job = _make_job("job-003", state=JobState.reviewed)
    repo.insert_job(engine, job)

    # Simulate what 1_Feed.py does on Dismiss button press
    repo.update_job_state(engine, "job-003", "dismissed")
    SignalService.record(
        engine=engine,
        job_id="job-003",
        event_type="mark_dismissed",
        specialty_name="ML Engineer",
    )
    SignalService.record_state_change(
        engine=engine,
        job_id="job-003",
        from_state="reviewed",
        to_state="dismissed",
        specialty_name="ML Engineer",
    )

    rows = repo.list_signals(engine, user_id="local", job_id="job-003")
    event_types = {r["event_type"] for r in rows}
    assert "mark_dismissed" in event_types
    assert "state_change" in event_types
    assert len(rows) == 2

    # Job state in DB should now be dismissed
    updated_job = repo.get_job(engine, "job-003")
    assert updated_job.state == JobState.dismissed


# ---------------------------------------------------------------------------
# TEST 4: mark_applied writes mark_applied + state_change, job stays in DB
# ---------------------------------------------------------------------------

def test_mark_applied_writes_events_and_job_stays(engine):
    """Applying keeps the job row in the DB (state=applied), writes signal events."""
    job = _make_job("job-004", state=JobState.reviewed)
    repo.insert_job(engine, job)

    repo.update_job_state(engine, "job-004", "applied")
    SignalService.record(
        engine=engine,
        job_id="job-004",
        event_type="mark_applied",
        specialty_name="Data Engineer",
        classification_confidence="medium",
    )
    SignalService.record_state_change(
        engine=engine,
        job_id="job-004",
        from_state="reviewed",
        to_state="applied",
        specialty_name="Data Engineer",
        classification_confidence="medium",
    )

    updated_job = repo.get_job(engine, "job-004")
    assert updated_job.state == JobState.applied

    rows = repo.list_signals(engine, user_id="local", job_id="job-004")
    event_types = {r["event_type"] for r in rows}
    assert "mark_applied" in event_types
    assert "state_change" in event_types


# ---------------------------------------------------------------------------
# TEST 5: un_dismiss resets to 'new' and writes two signal events
# ---------------------------------------------------------------------------

def test_un_dismiss_resets_state_and_writes_events(engine):
    """Un-dismiss transitions state to 'new' and writes state_change + un_dismiss events."""
    job = _make_job("job-005", state=JobState.dismissed)
    repo.insert_job(engine, job)

    # Simulate what 3_Dismissed.py does on Un-dismiss button press
    repo.update_job_state(engine, "job-005", "new")
    SignalService.record_state_change(
        engine=engine,
        job_id="job-005",
        from_state="dismissed",
        to_state="new",
        specialty_name="Data Analyst",
    )
    SignalService.record(
        engine=engine,
        job_id="job-005",
        event_type="un_dismiss",
        specialty_name="Data Analyst",
    )

    updated_job = repo.get_job(engine, "job-005")
    assert updated_job.state == JobState.new

    rows = repo.list_signals(engine, user_id="local", job_id="job-005")
    event_types = {r["event_type"] for r in rows}
    assert "state_change" in event_types
    assert "un_dismiss" in event_types


# ---------------------------------------------------------------------------
# TEST 6: detail_view_close stores dwell_ms ≥ 0
# ---------------------------------------------------------------------------

def test_detail_view_close_stores_dwell_ms(engine):
    """detail_view_close event correctly persists a non-negative dwell_ms."""
    job = _make_job("job-006")
    repo.insert_job(engine, job)

    SignalService.record(
        engine=engine,
        job_id="job-006",
        event_type="detail_view_open",
    )
    SignalService.record(
        engine=engine,
        job_id="job-006",
        event_type="detail_view_close",
        dwell_ms=4200,
    )

    rows = repo.list_signals(engine, user_id="local", job_id="job-006")
    close_events = [r for r in rows if r["event_type"] == "detail_view_close"]
    assert len(close_events) == 1
    assert close_events[0]["dwell_ms"] == 4200
    assert close_events[0]["dwell_ms"] >= 0


# ---------------------------------------------------------------------------
# TEST 7: override_reason is 'acted_on_unclassified' when specialty is None
# ---------------------------------------------------------------------------

def test_override_reason_set_for_unclassified_dismiss(engine):
    """record_state_change infers 'acted_on_unclassified' when specialty is None and to_state='dismissed'."""
    job = _make_job("job-007")
    repo.insert_job(engine, job)

    SignalService.record_state_change(
        engine=engine,
        job_id="job-007",
        from_state="reviewed",
        to_state="dismissed",
        specialty_name=None,  # unclassified
    )

    rows = repo.list_signals(engine, user_id="local", job_id="job-007")
    assert len(rows) == 1
    assert rows[0]["override_reason"] == "acted_on_unclassified"


# ---------------------------------------------------------------------------
# TEST 8: list_signals returns all events ordered by recorded_at DESC
# ---------------------------------------------------------------------------

def test_list_signals_returns_all_events_for_job(engine):
    """list_signals returns all events for a job_id, newest first."""
    job = _make_job("job-008")
    repo.insert_job(engine, job)

    SignalService.record(engine=engine, job_id="job-008", event_type="detail_view_open")
    SignalService.record(engine=engine, job_id="job-008", event_type="detail_view_close", dwell_ms=1200)
    SignalService.record_state_change(
        engine=engine,
        job_id="job-008",
        from_state="new",
        to_state="reviewed",
    )

    rows = repo.list_signals(engine, user_id="local", job_id="job-008")
    assert len(rows) == 3
    # Ordered by recorded_at DESC — most recent first
    assert rows[0]["event_type"] == "state_change"
    assert rows[1]["event_type"] == "detail_view_close"
    assert rows[2]["event_type"] == "detail_view_open"
