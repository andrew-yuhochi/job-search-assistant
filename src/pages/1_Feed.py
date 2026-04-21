# Job Feed page — the primary surface of the Job Search Assistant.
# In prototype mode (MODE=prototype), loads from tests/fixtures/jobs_fixtures.json.
# In normal mode, shows a placeholder until the scraper pipeline is wired (Milestone 2+).
# Per UX-SPEC.md §UI Component Guide and TASK-005.
# BL-001: scrollable description (height=200), BL-005: clickable card header,
# BL-006: applied/dismissed hidden from feed, BL-007: sort order,
# BL-008: standardised salary display.

from __future__ import annotations

import json
import os
import re
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
# BL-008: Salary formatting
# ---------------------------------------------------------------------------

def format_salary(salary_raw: str | None) -> str:
    """Return a human-readable salary string.

    Examples:
        "$120,000 - $150,000" -> "$120K–$150K CAD"
        "100K"                -> "$100K CAD"
        None / ""             -> "Salary unknown"
    """
    if not salary_raw:
        return "Salary unknown"

    # Match each number optionally followed by K/k, e.g. "120,000", "100K", "150k"
    raw = salary_raw.replace(",", "")
    token_re = re.compile(r"(\d+(?:\.\d+)?)\s*([Kk]?)")
    numbers: list[float] = []
    for m in token_re.finditer(raw):
        val = float(m.group(1))
        if m.group(2):        # K/k suffix — value is already in thousands
            val *= 1_000
        numbers.append(val)

    if not numbers:
        return salary_raw  # couldn't parse — return raw as-is

    lo_k = round(min(numbers) / 1_000)
    hi_k = round(max(numbers) / 1_000)

    if lo_k == hi_k:
        return f"${lo_k}K CAD"
    return f"${lo_k}K–${hi_k}K CAD"


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
        # BL-005: Full-width button as the click target — shows title @ company.
        # This replaces the separate "View →" button and makes the whole card header clickable.
        if st.button(
            f"{job['title']} @ {job['company']}",
            key=f"card_{job_id}",
            use_container_width=True,
        ):
            st.session_state.selected_job_id = job_id
            st.rerun()

        # Row 1: state badge + specialty chip
        col_state, col_spec = st.columns([1, 2])
        with col_state:
            st.markdown(STATE_BADGE.get(state, STATE_BADGE["new"]), unsafe_allow_html=True)
        with col_spec:
            spec_bg, spec_fg = SPECIALTY_COLOR.get(specialty, ("#F5F5F5", "#424242"))
            st.markdown(_badge_html(specialty, spec_bg, spec_fg), unsafe_allow_html=True)

        # Row 2: duty summary (first 120 chars of description)
        summary = job.get("description", "")
        truncated = (summary[:120] + "...") if len(summary) > 120 else summary
        st.caption(truncated)

        # Row 3: salary badge + company size badge + source  (BL-008: format_salary)
        salary_str = format_salary(job.get("salary_raw"))
        if salary_str == "Salary unknown":
            salary_html = _badge_html(salary_str, "#F5F5F5", "#757575")
        else:
            salary_html = _badge_html(salary_str, "#E8F5E9", "#2E7D32")

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

        # Row 4: duplicate flag
        if is_duplicate:
            canonical_id = PROTOTYPE_DUPLICATE_MAP[job_id]
            st.markdown(
                f'<span style="background:#FFF3CD;color:#856404;padding:2px 8px;'
                f'border-radius:4px;font-size:0.75rem">&#9888; Duplicate of {canonical_id}</span>',
                unsafe_allow_html=True,
            )


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

    # Specialty + salary badges  (BL-008: format_salary)
    spec_bg, spec_fg = SPECIALTY_COLOR.get(specialty, ("#F5F5F5", "#424242"))
    salary_str = format_salary(job.get("salary_raw"))
    salary_html = (
        _badge_html(salary_str, "#E8F5E9", "#2E7D32")
        if salary_str != "Salary unknown"
        else _badge_html(salary_str, "#F5F5F5", "#757575")
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
    # BL-001: fixed height so the text area scrolls internally, not the page.
    st.text_area(
        label="Description",
        value=job.get("description", ""),
        height=200,
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

# BL-006: count applied / dismissed for sidebar badge and exclude from feed.
job_states = st.session_state.job_states
applied_count = sum(1 for j in jobs if job_states.get(j["id"]) == "applied")
dismissed_count = sum(1 for j in jobs if job_states.get(j["id"]) == "dismissed")

with st.sidebar:
    if applied_count:
        st.caption(f"Applied ({applied_count})")
    if dismissed_count:
        st.caption(f"Dismissed ({dismissed_count})")

# Filter jobs by selected specialty, then exclude applied/dismissed.
if selected_specialty and selected_specialty != "All":
    specialty_jobs = [j for j in jobs if _specialty_label(j) == selected_specialty]
else:
    specialty_jobs = jobs

filtered_jobs = [
    j for j in specialty_jobs
    if job_states.get(j["id"]) not in ("applied", "dismissed")
]

# BL-007: Sort — New first (0), Reviewed (1), Applied (2), Dismissed (3),
# then posted_date descending, then salary descending (null salary = 0).
_STATUS_PRIORITY: dict[str, int] = {"new": 0, "reviewed": 1, "applied": 2, "dismissed": 3}


def _salary_int(salary_raw: str | None) -> int:
    """Extract the minimum salary as an integer for sort purposes (null → 0)."""
    if not salary_raw:
        return 0
    nums = re.findall(r"\d+", salary_raw.replace(",", ""))
    if not nums:
        return 0
    val = int(nums[0])
    if "k" in salary_raw.lower():
        val *= 1_000
    return val


def _posted_int(posted_date: str | None) -> int:
    """Convert ISO date string to integer (YYYYMMDD) for numeric negation in sort."""
    if not posted_date:
        return 0
    return int(posted_date.replace("-", ""))


filtered_jobs.sort(
    key=lambda j: (
        _STATUS_PRIORITY.get(job_states.get(j["id"], "new"), 0),
        -_posted_int(j.get("posted_date")),
        -_salary_int(j.get("salary_raw")),
    )
)

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
