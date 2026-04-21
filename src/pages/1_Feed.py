# Job Feed page — the primary surface of the Job Search Assistant.
# In prototype mode (MODE=prototype), loads from tests/fixtures/jobs_fixtures.json.
# In normal mode, shows a placeholder until the scraper pipeline is wired (Milestone 2+).
# Per UX-SPEC.md §UI Component Guide and TASK-005.

from __future__ import annotations

import json
import os
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Prototype mode detection
# ---------------------------------------------------------------------------

PROTOTYPE_MODE = os.environ.get("MODE", "").lower() == "prototype"

# ---------------------------------------------------------------------------
# Duplicate detection for prototype mode
# Pairs: jf-002 duplicates jf-001 (fuzzy), jf-010 duplicates jf-009 (exact URL)
# ---------------------------------------------------------------------------

PROTOTYPE_DUPLICATE_MAP: dict[str, str] = {
    "jf-002": "jf-001",
    "jf-010": "jf-009",
}

# ---------------------------------------------------------------------------
# Specialty mapping: fixture expected_specialty → display label
# ---------------------------------------------------------------------------

SPECIALTY_LABEL: dict[str, str] = {
    "data_scientist": "Data Scientist",
    "ml_engineer": "ML Engineer",
    "data_engineer": "Data Engineer",
    "data_analyst": "Data Analyst",
    "unclassified": "Unclassified",
}

SPECIALTY_OPTIONS = ["All", "Data Scientist", "ML Engineer", "Data Engineer", "Data Analyst", "Unclassified"]

# ---------------------------------------------------------------------------
# Specialty chip colours (inline HTML badges)
# ---------------------------------------------------------------------------

SPECIALTY_COLOR: dict[str, tuple[str, str]] = {
    "Data Scientist":  ("#E3F2FD", "#1565C0"),
    "ML Engineer":     ("#F3E5F5", "#6A1B9A"),
    "Data Engineer":   ("#E8F5E9", "#2E7D32"),
    "Data Analyst":    ("#FFF8E1", "#F57F17"),
    "Unclassified":    ("#F5F5F5", "#616161"),
}

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
# Data loading
# ---------------------------------------------------------------------------

def _load_fixtures() -> list[dict]:
    """Load fixture postings from tests/fixtures/jobs_fixtures.json."""
    fixtures_path = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "jobs_fixtures.json"
    with fixtures_path.open(encoding="utf-8") as f:
        return json.load(f)


def _get_jobs() -> list[dict]:
    """Return list of job dicts for display."""
    if PROTOTYPE_MODE:
        return _load_fixtures()
    return []


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

def _specialty_label(job: dict) -> str:
    raw = job.get("expected_specialty", "unclassified") or "unclassified"
    return SPECIALTY_LABEL.get(raw, "Unclassified")


def _current_state(job: dict) -> str:
    return st.session_state.job_states.get(job["id"], "new")


def _render_card(job: dict) -> None:
    """Render a single job card inside st.container(border=True)."""
    job_id = job["id"]
    specialty = _specialty_label(job)
    state = _current_state(job)
    is_duplicate = job_id in PROTOTYPE_DUPLICATE_MAP

    with st.container(border=True):
        # Row 1: company (bold) + state badge
        col_company, col_state = st.columns([3, 1])
        with col_company:
            st.markdown(f"**{job['company']}**")
        with col_state:
            st.markdown(STATE_BADGE.get(state, STATE_BADGE["new"]), unsafe_allow_html=True)

        # Row 2: title + specialty chip
        spec_bg, spec_fg = SPECIALTY_COLOR.get(specialty, ("#F5F5F5", "#424242"))
        st.markdown(
            f"{job['title']} &nbsp; {_badge_html(specialty, spec_bg, spec_fg)}",
            unsafe_allow_html=True,
        )

        # Row 3: duty summary (first 120 chars of description)
        summary = job.get("description", "")
        truncated = (summary[:120] + "...") if len(summary) > 120 else summary
        st.caption(truncated)

        # Row 4: salary badge + company size badge + source
        salary_raw = job.get("salary_raw")
        if salary_raw:
            salary_html = _badge_html(salary_raw, "#E8F5E9", "#2E7D32")
        else:
            salary_html = _badge_html("Salary unknown", "#F5F5F5", "#757575")

        # company_employees_label not in fixtures — always "Size unknown"
        size_label = job.get("company_employees_label")
        if size_label:
            size_html = _badge_html(size_label, "#F5F5F5", "#424242")
        else:
            size_html = _badge_html("Size unknown", "#FFF9C4", "#F57F17")

        source_text = SOURCE_LABEL.get(job.get("source", ""), "")
        st.markdown(
            f"{salary_html} &nbsp; {size_html} &nbsp; <small>{source_text}</small>",
            unsafe_allow_html=True,
        )

        # Row 5: duplicate flag
        if is_duplicate:
            canonical_id = PROTOTYPE_DUPLICATE_MAP[job_id]
            st.markdown(
                f'<span style="background:#FFF3CD;color:#856404;padding:2px 8px;'
                f'border-radius:4px;font-size:0.75rem">&#9888; Duplicate of {canonical_id}</span>',
                unsafe_allow_html=True,
            )

        # Click target
        if st.button("View →", key=f"card_{job_id}", use_container_width=True):
            st.session_state.selected_job_id = job_id
            st.rerun()


def _render_detail(job: dict) -> None:
    """Render the detail pane for the selected job."""
    job_id = job["id"]
    specialty = _specialty_label(job)
    state = _current_state(job)
    is_duplicate = job_id in PROTOTYPE_DUPLICATE_MAP

    # Header
    st.subheader(job["title"])
    st.markdown(f"**{job['company']}** &nbsp;·&nbsp; {job.get('location', 'N/A')}")
    source_text = SOURCE_LABEL.get(job.get("source", ""), job.get("source", ""))
    st.caption(f"Source: {source_text} &nbsp;·&nbsp; Posted: {job.get('posted_date', 'N/A')}")

    st.divider()

    # Specialty + salary badges
    spec_bg, spec_fg = SPECIALTY_COLOR.get(specialty, ("#F5F5F5", "#424242"))
    salary_raw = job.get("salary_raw")
    salary_html = (
        _badge_html(salary_raw, "#E8F5E9", "#2E7D32")
        if salary_raw
        else _badge_html("Salary unknown", "#F5F5F5", "#757575")
    )
    st.markdown(
        f"{_badge_html(specialty, spec_bg, spec_fg)} &nbsp; {salary_html}",
        unsafe_allow_html=True,
    )

    # Duplicate notice
    if is_duplicate:
        canonical_id = PROTOTYPE_DUPLICATE_MAP[job_id]
        st.info(f"This post appears to duplicate **{canonical_id}**. It may be the same role posted on a different job board.")

    st.divider()

    # Full description
    st.subheader("Job Description")
    st.text_area(
        label="Description",
        value=job.get("description", ""),
        height=250,
        disabled=True,
        label_visibility="collapsed",
    )

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
            if PROTOTYPE_MODE:
                st.session_state.job_states[job_id] = "reviewed"
                st.toast(f"Marked as Reviewed: {job['title']}")
                st.rerun()

    with col_apply:
        if st.button("Apply", key=f"apply_{job_id}", type="primary", use_container_width=True):
            if PROTOTYPE_MODE:
                st.session_state.job_states[job_id] = "applied"
                st.toast(f"Marked as Applied: {job['title']}")
                st.rerun()

    with col_dismiss:
        if st.button("Dismiss", key=f"dismiss_{job_id}", use_container_width=True):
            if PROTOTYPE_MODE:
                st.session_state.job_states[job_id] = "dismissed"
                st.toast(f"Dismissed: {job['title']}")
                st.rerun()

    with col_open:
        st.link_button("Open Post ↗", url=job.get("url", "#"), use_container_width=True)


# ---------------------------------------------------------------------------
# Page layout
# ---------------------------------------------------------------------------

st.title("Job Feed")

if PROTOTYPE_MODE:
    st.caption("Prototype mode — loading from fixture data")

jobs = _get_jobs()

if not PROTOTYPE_MODE and not jobs:
    st.info("No jobs yet — run the scraper first.")
    st.stop()

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

# Filter jobs by selected specialty
if selected_specialty and selected_specialty != "All":
    filtered_jobs = [j for j in jobs if _specialty_label(j) == selected_specialty]
else:
    filtered_jobs = jobs

# Two-pane layout: [2, 3] per UX-SPEC §1
left_col, right_col = st.columns([2, 3])

with left_col:
    if not filtered_jobs:
        st.caption(f"No {selected_specialty} posts in this run. Try selecting a different specialty or 'All'.")
    else:
        for job in filtered_jobs:
            _render_card(job)

with right_col:
    selected_id = st.session_state.get("selected_job_id")
    if selected_id:
        # Find the selected job
        match = next((j for j in jobs if j["id"] == selected_id), None)
        if match:
            _render_detail(match)
        else:
            st.info("Selected job not found. It may have been filtered out.")
    else:
        st.info("← Select a job from the list to view details.")
