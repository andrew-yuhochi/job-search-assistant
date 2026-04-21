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

# Fix 3: per-specialty colors for card chips and detail badges.
# Keys match expected_specialty values from fixtures.
SPECIALTY_COLORS: dict[str, tuple[str, str]] = {
    "data_scientist": ("#1565C0", "#fff"),
    "ml_engineer":    ("#6A1B9A", "#fff"),
    "data_engineer":  ("#E65100", "#fff"),
    "data_analyst":   ("#00695C", "#fff"),
    "unclassified":   ("#424242", "#fff"),
}

# Legacy label-keyed dict used by _render_detail (kept for backward compat).
SPECIALTY_COLOR: dict[str, tuple[str, str]] = {
    "Data Scientist":  ("#1565C0", "#fff"),
    "ML Engineer":     ("#6A1B9A", "#fff"),
    "Data Engineer":   ("#E65100", "#fff"),
    "Data Analyst":    ("#00695C", "#fff"),
    "Unclassified":    ("#424242", "#fff"),
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
    """Return a human-readable salary string safe for HTML rendering.

    Fix 7: use non-breaking hyphen (U+2011) for ranges so Streamlit's
    markdown parser never treats the dash as italic markup, and prefix
    both bounds with $ so the output is always "$NNNk\u2011$MMMk CAD".

    Examples:
        "$120,000 - $150,000" -> "$120K\u2011$150K CAD"
        "100K"                -> "$100K CAD"
        None / ""             -> "Salary unknown"
    """
    _NBHYPHEN = "\u2011"  # non-breaking hyphen — not a markdown token

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
    return f"${lo_k}K{_NBHYPHEN}${hi_k}K CAD"


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
    """Render a single job card inside st.container(border=True).

    Layout (Fix 1, 2, 3):
      Row 1: [title as plain bold text (4) | state badge (1)]
      Row 2: specialty chip — left-aligned small tag, color per type
      Row 3: description snippet — first 120 chars, muted
      Row 4: salary badge | size badge | source label
      Row 5: right-aligned "View ›" button
    """
    job_id = job["id"]
    raw_specialty = (job.get("expected_specialty") or "unclassified").lower()
    specialty = _specialty_label(job)
    state = _current_state(job)
    is_duplicate = job_id in PROTOTYPE_DUPLICATE_MAP

    with st.container(border=True):
        # Row 1: Fix 1 — plain bold title (non-interactive) + state badge
        col_title, col_badge = st.columns([4, 1])
        with col_title:
            st.markdown(f"<span style='font-size:1.2em;font-weight:700'>{job['title']}</span>", unsafe_allow_html=True)
            st.markdown(f"<span style='color:#aaa;font-size:1.05em'>{job['company']}</span>", unsafe_allow_html=True)
        with col_badge:
            st.markdown(
                f"<div style='text-align:right;padding-top:6px'>"
                f"{STATE_BADGE.get(state, STATE_BADGE['new'])}</div>",
                unsafe_allow_html=True,
            )

        # Row 2: specialty chip — Fix 3: color per expected_specialty value
        chip_bg, chip_fg = SPECIALTY_COLORS.get(raw_specialty, ("#424242", "#fff"))
        st.markdown(
            f"<span style='background:{chip_bg};padding:2px 8px;border-radius:4px;"
            f"font-size:0.8em;color:{chip_fg}'>🏷 {specialty}</span>",
            unsafe_allow_html=True,
        )

        # Row 3: description snippet — first 120 chars
        summary = job.get("description", "")
        truncated = (summary[:120] + "...") if len(summary) > 120 else summary
        st.caption(truncated)

        # Row 4: salary badge + company size badge + source  (BL-008: format_salary)
        # Fix 3: use st.html() for all badge content to bypass markdown processing entirely.
        salary_str = format_salary(job.get("salary_raw"))
        if salary_str == "Salary unknown":
            salary_badge = f"<span style='background:#2d2d2d;color:#aaa;padding:3px 8px;border-radius:4px;font-size:0.85em;font-style:normal'>Salary unknown</span>"
        else:
            salary_badge = f"<span style='background:#1a3a1a;color:#4caf50;padding:3px 8px;border-radius:4px;font-size:0.85em;font-style:normal'>{salary_str}</span>"

        # company_employees_label not in fixtures — always "Size unknown"
        size_label = job.get("company_employees_label")
        if size_label:
            size_badge = f"<span style='background:#2d2d2d;color:#ccc;padding:3px 8px;border-radius:4px;font-size:0.85em;font-style:normal'>{size_label}</span>"
        else:
            size_badge = f"<span style='background:#2d2d2d;color:#ccc;padding:3px 8px;border-radius:4px;font-size:0.85em;font-style:normal'>Size unknown</span>"

        source_text = SOURCE_LABEL.get(job.get("source", ""), "")
        st.html(f"{salary_badge} &nbsp; {size_badge} &nbsp; <small style='color:#888'>{source_text}</small>")

        # Duplicate flag — Fix 3: st.html() to prevent markdown processing
        if is_duplicate:
            canonical_id = PROTOTYPE_DUPLICATE_MAP[job_id]
            st.html(
                f"<span style='background:#FFF3CD;color:#856404;padding:2px 8px;"
                f"border-radius:4px;font-size:0.75rem'>&#9888; Duplicate of {canonical_id}</span>"
            )

        # Row 5: Fix 1 — small right-aligned "View ›" button
        _, col_view = st.columns([4, 1])
        with col_view:
            if st.button("View ›", key=f"select_{job_id}", type="secondary"):
                st.session_state.selected_job_id = job_id
                st.rerun()


def _render_detail(job: dict) -> None:
    """Render the detail pane for the selected job."""
    job_id = job["id"]
    specialty = _specialty_label(job)
    state = _current_state(job)
    is_duplicate = job_id in PROTOTYPE_DUPLICATE_MAP

    # Header — Fix 1: title on one line, company muted below
    st.subheader(job["title"])
    st.markdown(
        f"<span style='color:#aaa;font-size:0.9em'>{job['company']}</span>"
        f" &nbsp;·&nbsp; {job.get('location', 'N/A')}",
        unsafe_allow_html=True,
    )
    source_text = SOURCE_LABEL.get(job.get("source", ""), job.get("source", ""))
    st.caption(f"Source: {source_text} &nbsp;·&nbsp; Posted: {job.get('posted_date', 'N/A')}")

    st.divider()

    # Specialty + salary badges  (BL-008: format_salary)
    # Fix 3: use st.html() for salary to bypass markdown processing entirely.
    spec_bg, spec_fg = SPECIALTY_COLOR.get(specialty, ("#F5F5F5", "#424242"))
    salary_str = format_salary(job.get("salary_raw"))
    if salary_str == "Salary unknown":
        salary_badge = f"<span style='background:#2d2d2d;color:#aaa;padding:3px 8px;border-radius:4px;font-size:0.85em;font-style:normal'>Salary unknown</span>"
    else:
        salary_badge = f"<span style='background:#1a3a1a;color:#4caf50;padding:3px 8px;border-radius:4px;font-size:0.85em;font-style:normal'>{salary_str}</span>"
    spec_badge = _badge_html(specialty, spec_bg, spec_fg)
    st.html(f"{spec_badge} &nbsp; {salary_badge}")

    # Duplicate notice
    if is_duplicate:
        canonical_id = PROTOTYPE_DUPLICATE_MAP[job_id]
        st.info(f"This post appears to duplicate **{canonical_id}**. It may be the same role posted on a different job board.")

    st.divider()

    # Full description — Fix 5: collapsible expander with markdown rendering.
    # Replaces the fixed-height text_area so the user can collapse when done reading.
    with st.expander("📄 Job Description", expanded=True):
        st.markdown(job.get("description", ""))

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
                # Fix 6: state → clear selection → toast → rerun (toast must precede rerun)
                st.session_state.job_states[job_id] = "applied"
                st.session_state.selected_job_id = None
                st.session_state.pending_toast = {"msg": "✅ Saved to Applied. Head to the Applied tab to view your draft.", "icon": "✅"}
                st.rerun()

    with col_dismiss:
        if st.button("Dismiss", key=f"dismiss_{job_id}", use_container_width=True):
            if PROTOTYPE_MODE:
                # Fix 6: state → clear selection → toast → rerun (toast must precede rerun)
                st.session_state.job_states[job_id] = "dismissed"
                st.session_state.selected_job_id = None
                st.session_state.pending_toast = {"msg": "🗑 Post dismissed. You can restore it from the Dismissed tab.", "icon": "🗑"}
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

# Fix 2: render any pending toast that was queued before the last rerun.
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
            # Fix 6: job may have been applied/dismissed and removed from feed
            st.info("👈 Select a post from the list to view details.")
    else:
        # Fix 6: default empty state
        st.info("👈 Select a post from the list to view details.")
