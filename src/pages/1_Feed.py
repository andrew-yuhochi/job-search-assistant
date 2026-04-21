# Job Feed page — the primary surface of the Job Search Assistant.
# TASK-009: Prototype mode removed. All data served from SQLite via repository.
# In DB mode, loads JobPosting objects via repository.list_jobs() and
# classification dicts via repository.get_classification().
# Per UX-SPEC.md §UI Component Guide and TASK-005.
# BL-001: scrollable description (height=200), BL-005: clickable card header,
# BL-006: applied/dismissed hidden from feed, BL-007: sort order,
# BL-008: standardised salary display.
# TASK-010: SignalService wiring — auto-reviewed on detail open, Mark Applied /
# Mark Dismissed write signal events, dwell time tracked in session_state.

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import streamlit as st

# Ensure src/ is importable when page is loaded directly by Streamlit
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.models.models import JobPosting
from src.services.signal_service import SignalService
from src.storage import repository
from src.storage.db import get_engine

# ---------------------------------------------------------------------------
# Specialty chip colours (inline HTML badges)
# ---------------------------------------------------------------------------

# Keys match specialty_name values from the classifications table.
SPECIALTY_COLORS: dict[str, tuple[str, str]] = {
    "Data Scientist": ("#1565C0", "#fff"),
    "ML Engineer":    ("#6A1B9A", "#fff"),
    "Data Engineer":  ("#E65100", "#fff"),
    "Data Analyst":   ("#00695C", "#fff"),
    "Unclassified":   ("#424242", "#fff"),
}

SPECIALTY_OPTIONS = ["All", "Data Scientist", "ML Engineer", "Data Engineer", "Data Analyst", "Unclassified"]

# ---------------------------------------------------------------------------
# State badge HTML
# ---------------------------------------------------------------------------

STATE_BADGE: dict[str, str] = {
    "new":      '<span style="background:#1E88E5;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem">New</span>',
    "reviewed": '<span style="background:#9E9E9E;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem">Reviewed</span>',
    "applied":  '<span style="background:#43A047;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem">Applied</span>',
    "dismissed":'<span style="background:#E53935;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem">Dismissed</span>',
}

SOURCE_LABEL: dict[str, str] = {
    "linkedin": "(LinkedIn)",
    "indeed":   "(Indeed)",
    "google":   "(Google)",
}


def _badge_html(text: str, bg: str, fg: str) -> str:
    return (
        f'<span style="background:{bg};color:{fg};padding:2px 8px;'
        f'border-radius:4px;font-size:0.75rem">{text}</span>'
    )


# ---------------------------------------------------------------------------
# BL-008: Salary formatting
# ---------------------------------------------------------------------------

def format_salary(min_cad: int | None, max_cad: int | None) -> str:
    """Return a human-readable salary string from parsed min/max CAD values.

    Examples:
        (130000, 155000) -> "$130K‑$155K CAD"
        (105000, 105000) -> "$105K CAD"
        (None, None)     -> "Salary unknown"
    """
    _NBHYPHEN = "\u2011"  # non-breaking hyphen

    if min_cad is None and max_cad is None:
        return "Salary unknown"

    lo_k = round((min_cad or 0) / 1_000)
    hi_k = round((max_cad or min_cad or 0) / 1_000)

    if lo_k == hi_k:
        return f"${lo_k}K CAD"
    return f"${lo_k}K{_NBHYPHEN}${hi_k}K CAD"


def format_salary_raw(salary_raw: str | None) -> str:
    """Fallback: parse salary from a raw string (used when DB salary fields are null)."""
    _NBHYPHEN = "\u2011"

    if not salary_raw:
        return "Salary unknown"

    raw = salary_raw.replace(",", "")
    token_re = re.compile(r"(\d+(?:\.\d+)?)\s*([Kk]?)")
    numbers: list[float] = []
    for m in token_re.finditer(raw):
        val = float(m.group(1))
        if m.group(2):
            val *= 1_000
        numbers.append(val)

    if not numbers:
        return salary_raw

    lo_k = round(min(numbers) / 1_000)
    hi_k = round(max(numbers) / 1_000)

    if lo_k == hi_k:
        return f"${lo_k}K CAD"
    return f"${lo_k}K{_NBHYPHEN}${hi_k}K CAD"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_db_engine():
    """Return the shared SQLAlchemy engine (cached per session)."""
    return get_engine()


def _get_jobs() -> list[JobPosting]:
    """Return all non-dismissed jobs from the DB ordered by recency + salary."""
    engine = _get_db_engine()
    return repository.list_jobs(engine, user_id="local")


def _get_classification(job_id: str) -> dict | None:
    """Fetch the classification dict for a job, or None if not classified."""
    engine = _get_db_engine()
    return repository.get_classification(engine, job_id)


def _list_duplicates(canonical_job_id: str) -> list[dict]:
    """Return duplicate records for a canonical job."""
    engine = _get_db_engine()
    return repository.list_duplicates(engine, canonical_job_id)


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

def _specialty_label(job_id: str) -> str:
    """Look up the specialty name from the DB; default to 'Unclassified'."""
    clf = _get_classification(job_id)
    if clf:
        return clf.get("specialty_name", "Unclassified")
    return "Unclassified"


def _current_state(job: JobPosting) -> str:
    """Return the job's current state, preferring in-session overrides."""
    return st.session_state.job_states.get(job.job_id, job.state.value)


def _render_card(job: JobPosting) -> None:
    """Render a single job card inside st.container(border=True).

    Layout:
      Row 1: [title as plain bold text (4) | state badge (1)]
      Row 2: specialty chip — left-aligned small tag, colour per type
      Row 3: description snippet — first 120 chars, muted
      Row 4: salary badge | size badge | source label
      Row 5: right-aligned "View ›" button
    """
    job_id = job.job_id
    specialty = _specialty_label(job_id)
    state = _current_state(job)
    is_duplicate = job.duplicate_of is not None

    chip_bg, chip_fg = SPECIALTY_COLORS.get(specialty, ("#424242", "#fff"))

    with st.container(border=True):
        # Row 1: title + state badge
        col_title, col_badge = st.columns([4, 1])
        with col_title:
            st.markdown(
                f"<span style='font-size:1.2em;font-weight:700'>{job.title}</span>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<span style='color:#aaa;font-size:1.05em'>{job.company}</span>",
                unsafe_allow_html=True,
            )
        with col_badge:
            st.markdown(
                f"<div style='text-align:right;padding-top:6px'>"
                f"{STATE_BADGE.get(state, STATE_BADGE['new'])}</div>",
                unsafe_allow_html=True,
            )

        # Row 2: specialty chip
        st.markdown(
            f"<span style='background:{chip_bg};padding:2px 8px;border-radius:4px;"
            f"font-size:0.8em;color:{chip_fg}'>🏷 {specialty}</span>",
            unsafe_allow_html=True,
        )

        # Row 3: description snippet
        summary = job.description or ""
        truncated = (summary[:120] + "...") if len(summary) > 120 else summary
        st.caption(truncated)

        # Row 4: salary + size + source badges
        salary_str = format_salary(job.salary_min_cad, job.salary_max_cad)
        if salary_str == "Salary unknown":
            salary_badge = (
                "<span style='background:#2d2d2d;color:#aaa;padding:3px 8px;"
                "border-radius:4px;font-size:0.85em;font-style:normal'>Salary unknown</span>"
            )
        else:
            salary_badge = (
                f"<span style='background:#1a3a1a;color:#4caf50;padding:3px 8px;"
                f"border-radius:4px;font-size:0.85em;font-style:normal'>{salary_str}</span>"
            )

        size_label = job.company_employees_label
        if size_label:
            size_badge = (
                f"<span style='background:#2d2d2d;color:#ccc;padding:3px 8px;"
                f"border-radius:4px;font-size:0.85em;font-style:normal'>{size_label}</span>"
            )
        else:
            size_badge = (
                "<span style='background:#2d2d2d;color:#ccc;padding:3px 8px;"
                "border-radius:4px;font-size:0.85em;font-style:normal'>Size unknown</span>"
            )

        source_text = SOURCE_LABEL.get(job.source.value, "")
        st.html(f"{salary_badge} &nbsp; {size_badge} &nbsp; <small style='color:#888'>{source_text}</small>")

        # Duplicate flag
        if is_duplicate:
            st.html(
                f"<span style='background:#FFF3CD;color:#856404;padding:2px 8px;"
                f"border-radius:4px;font-size:0.75rem'>&#9888; Duplicate of "
                f"{str(job.duplicate_of)[:12]}</span>"
            )

        # Row 5: View button
        if st.button("View ›", key=f"select_{job_id}", use_container_width=True):
            # Close dwell on the previously selected card (if any) before switching
            prev_selected = st.session_state.get("selected_job_id")
            if prev_selected and prev_selected != job_id:
                prev_specialty = _specialty_label(prev_selected)
                _close_dwell(_get_db_engine(), prev_selected, prev_specialty)
            st.session_state.selected_job_id = job_id
            st.rerun()


def _close_dwell(engine, job_id: str, specialty: str) -> None:
    """Write a detail_view_close signal with dwell_ms if the pane was previously open.

    Called whenever a different card is opened (or a state button is pressed) so we
    capture the time the previous card was visible before the rerun.
    """
    open_key = f"open_time_{job_id}"
    open_ts = st.session_state.get(open_key)
    if open_ts is not None:
        dwell_ms = max(0, int((time.time() - open_ts) * 1000))
        engine = _get_db_engine()
        SignalService.record(
            engine=engine,
            job_id=job_id,
            event_type="detail_view_close",
            dwell_ms=dwell_ms,
            specialty_name=specialty,
        )
        del st.session_state[open_key]
        guard_key = f"dv_open_written_{job_id}"
        if guard_key in st.session_state:
            del st.session_state[guard_key]


def _render_detail(job: JobPosting) -> None:
    """Render the detail pane for the selected job."""
    job_id = job.job_id
    specialty = _specialty_label(job_id)
    state = _current_state(job)
    clf = _get_classification(job_id)

    engine = _get_db_engine()

    # --- TASK-010: auto-transition new → reviewed on detail open ---
    if state == "new":
        repository.update_job_state(engine, job_id, "reviewed")
        st.session_state.job_states[job_id] = "reviewed"
        state = "reviewed"
        SignalService.record_state_change(
            engine=engine,
            job_id=job_id,
            from_state="new",
            to_state="reviewed",
            specialty_name=specialty,
            classification_confidence=(clf or {}).get("confidence"),
        )

    # --- TASK-010: dwell-time tracking — record open timestamp ---
    open_key = f"open_time_{job_id}"
    guard_key = f"dv_open_written_{job_id}"
    if open_key not in st.session_state:
        st.session_state[open_key] = time.time()
    if guard_key not in st.session_state:
        st.session_state[guard_key] = True
        SignalService.record(
            engine=engine,
            job_id=job_id,
            event_type="detail_view_open",
            specialty_name=specialty,
            classification_confidence=(clf or {}).get("confidence"),
        )

    spec_bg, spec_fg = SPECIALTY_COLORS.get(specialty, ("#424242", "#fff"))

    # Header
    st.subheader(job.title)
    st.markdown(
        f"<span style='color:#aaa;font-size:0.9em'>{job.company}</span>"
        f" &nbsp;·&nbsp; {job.location or 'N/A'}",
        unsafe_allow_html=True,
    )
    posted_str = job.posted_at.strftime("%Y-%m-%d") if job.posted_at else "N/A"
    source_text = SOURCE_LABEL.get(job.source.value, job.source.value)
    st.caption(f"Source: {source_text} &nbsp;·&nbsp; Posted: {posted_str}")

    st.divider()

    # Specialty + salary badges
    salary_str = format_salary(job.salary_min_cad, job.salary_max_cad)
    if salary_str == "Salary unknown":
        salary_badge = (
            "<span style='background:#2d2d2d;color:#aaa;padding:3px 8px;"
            "border-radius:4px;font-size:0.85em;font-style:normal'>Salary unknown</span>"
        )
    else:
        salary_badge = (
            f"<span style='background:#1a3a1a;color:#4caf50;padding:3px 8px;"
            f"border-radius:4px;font-size:0.85em;font-style:normal'>{salary_str}</span>"
        )
    spec_badge = _badge_html(specialty, spec_bg, spec_fg)
    st.html(f"{spec_badge} &nbsp; {salary_badge}")

    # Classification confidence (if available)
    if clf:
        confidence = clf.get("confidence", "low")
        st.caption(f"Classification confidence: {confidence}")

    # Duplicate notice
    if job.duplicate_of:
        st.info(
            f"This post appears to duplicate canonical job **{str(job.duplicate_of)[:12]}**. "
            "It may be the same role posted on a different job board."
        )

    st.divider()

    # Full description — collapsible expander
    with st.expander("📄 Job Description", expanded=True):
        st.markdown(job.description or "")

    st.divider()

    # Highlight draft placeholder
    st.subheader("CV Highlight Draft")
    st.info("Highlight draft will appear here after knowledge bank is uploaded.")

    st.divider()

    # Action buttons
    st.subheader("Actions")
    col_reviewed, col_apply, col_dismiss, col_open = st.columns(4)

    with col_reviewed:
        if st.button("Mark Reviewed", key=f"reviewed_{job_id}", use_container_width=True):
            prev_state = _current_state(job)
            repository.update_job_state(engine, job_id, "reviewed")
            st.session_state.job_states[job_id] = "reviewed"
            SignalService.record_state_change(
                engine=engine,
                job_id=job_id,
                from_state=prev_state,
                to_state="reviewed",
                specialty_name=specialty,
                classification_confidence=(clf or {}).get("confidence"),
            )
            st.toast(f"Marked as Reviewed: {job.title}")
            st.rerun()

    with col_apply:
        if st.button("Apply", key=f"apply_{job_id}", type="primary", use_container_width=True):
            prev_state = _current_state(job)
            # Write dwell_close before navigating away
            _close_dwell(engine, job_id, specialty)
            repository.update_job_state(engine, job_id, "applied")
            st.session_state.job_states[job_id] = "applied"
            SignalService.record(
                engine=engine,
                job_id=job_id,
                event_type="mark_applied",
                specialty_name=specialty,
                classification_confidence=(clf or {}).get("confidence"),
            )
            SignalService.record_state_change(
                engine=engine,
                job_id=job_id,
                from_state=prev_state,
                to_state="applied",
                specialty_name=specialty,
                classification_confidence=(clf or {}).get("confidence"),
            )
            st.session_state.selected_job_id = None
            st.session_state.pending_toast = {
                "msg": "Saved to Applied. Head to the Applied tab to view your draft.",
                "icon": "✅",
            }
            st.rerun()

    with col_dismiss:
        if st.button("Dismiss", key=f"dismiss_{job_id}", use_container_width=True):
            prev_state = _current_state(job)
            # Write dwell_close before navigating away
            _close_dwell(engine, job_id, specialty)
            repository.update_job_state(engine, job_id, "dismissed")
            st.session_state.job_states[job_id] = "dismissed"
            SignalService.record(
                engine=engine,
                job_id=job_id,
                event_type="mark_dismissed",
                specialty_name=specialty,
                classification_confidence=(clf or {}).get("confidence"),
            )
            SignalService.record_state_change(
                engine=engine,
                job_id=job_id,
                from_state=prev_state,
                to_state="dismissed",
                specialty_name=specialty,
                classification_confidence=(clf or {}).get("confidence"),
            )
            st.session_state.selected_job_id = None
            st.session_state.pending_toast = {
                "msg": "Post dismissed. You can restore it from the Dismissed tab.",
                "icon": "🗑",
            }
            st.rerun()

    with col_open:
        st.link_button("Open Post ↗", url=job.url or "#", use_container_width=True)


# ---------------------------------------------------------------------------
# Page layout
# ---------------------------------------------------------------------------

st.title("Job Feed")

jobs = _get_jobs()

if not jobs:
    st.info("No jobs yet — run the scraper first, or seed from fixtures with `python scripts/seed_from_fixtures.py`.")
    st.stop()

# Render any pending toast queued before the last rerun
if st.session_state.pending_toast:
    st.toast(st.session_state.pending_toast["msg"], icon=st.session_state.pending_toast["icon"])
    st.session_state.pending_toast = None

# Specialty filter chips
try:
    selected_specialty = st.pills(
        label="Specialty",
        options=SPECIALTY_OPTIONS,
        default="All",
        label_visibility="collapsed",
    )
except AttributeError:
    # Fallback for older Streamlit builds without st.pills
    selected_specialty = st.radio(
        "Filter by specialty",
        options=SPECIALTY_OPTIONS,
        horizontal=True,
        label_visibility="collapsed",
    )

# BL-006: count applied / dismissed for sidebar badge and exclude from feed.
job_states = st.session_state.job_states
applied_count = sum(1 for j in jobs if job_states.get(j.job_id, j.state.value) == "applied")
dismissed_count = sum(1 for j in jobs if job_states.get(j.job_id, j.state.value) == "dismissed")

with st.sidebar:
    if applied_count:
        st.caption(f"Applied ({applied_count})")
    if dismissed_count:
        st.caption(f"Dismissed ({dismissed_count})")

# Filter by specialty
if selected_specialty and selected_specialty != "All":
    specialty_jobs = [j for j in jobs if _specialty_label(j.job_id) == selected_specialty]
else:
    specialty_jobs = jobs

# BL-006: exclude applied/dismissed from feed
filtered_jobs = [
    j for j in specialty_jobs
    if job_states.get(j.job_id, j.state.value) not in ("applied", "dismissed")
]

# BL-007: sort by status priority, then recency DESC, then salary DESC (null = 0)
_STATUS_PRIORITY: dict[str, int] = {"new": 0, "reviewed": 1, "applied": 2, "dismissed": 3}


def _salary_sort_key(job: JobPosting) -> int:
    return job.salary_min_cad or 0


def _posted_sort_key(job: JobPosting) -> int:
    if job.posted_at:
        return int(job.posted_at.strftime("%Y%m%d"))
    return 0


filtered_jobs.sort(
    key=lambda j: (
        _STATUS_PRIORITY.get(job_states.get(j.job_id, j.state.value), 0),
        -_posted_sort_key(j),
        -_salary_sort_key(j),
    )
)

# Two-pane layout: [2, 3] per UX-SPEC §1
left_col, right_col = st.columns([2, 3])

with left_col:
    if not filtered_jobs:
        st.caption(
            f"No {selected_specialty} posts in this run. "
            "Try selecting a different specialty or 'All'."
        )
    else:
        for job in filtered_jobs:
            _render_card(job)

with right_col:
    selected_id = st.session_state.get("selected_job_id")
    if selected_id:
        # Find in the full (unfiltered) jobs list so detail pane works for applied/dismissed
        match = next((j for j in jobs if j.job_id == selected_id), None)
        if match:
            _render_detail(match)
        else:
            st.info("Select a post from the list to view details.")
    else:
        st.info("Select a post from the list to view details.")
