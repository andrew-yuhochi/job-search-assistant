# _card.py — Shared job card rendering helper used by Feed, Applied, and Dismissed pages.
# Extracts the card layout from 1_Feed.py so it can be imported without duplicating HTML.
# Per Fix 4 (Dismissed) and Fix 3 (Applied) requirements from Milestone 2 validation.

from __future__ import annotations

import re
import sys
from pathlib import Path

import streamlit as st

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.models.models import JobPosting

# ---------------------------------------------------------------------------
# Specialty chip colours
# ---------------------------------------------------------------------------

SPECIALTY_COLORS: dict[str, tuple[str, str]] = {
    "Data Scientist": ("#1565C0", "#fff"),
    "ML Engineer":    ("#6A1B9A", "#fff"),
    "Data Engineer":  ("#E65100", "#fff"),
    "Data Analyst":   ("#00695C", "#fff"),
    "Unclassified":   ("#424242", "#fff"),
}

SOURCE_LABEL: dict[str, str] = {
    "linkedin": "(LinkedIn)",
    "indeed":   "(Indeed)",
    "google":   "(Google)",
}

STATE_BADGE: dict[str, str] = {
    "new":      '<span style="background:#1E88E5;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem">New</span>',
    "reviewed": '<span style="background:#9E9E9E;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem">Reviewed</span>',
    "applied":  '<span style="background:#43A047;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem">Applied</span>',
    "dismissed": '<span style="background:#E53935;color:#fff;padding:2px 8px;border-radius:4px;font-size:0.75rem">Dismissed</span>',
}

_NBHYPHEN = "\u2011"  # non-breaking hyphen


# ---------------------------------------------------------------------------
# Salary formatting
# ---------------------------------------------------------------------------

def format_salary(min_cad: int | None, max_cad: int | None) -> str:
    """Return a human-readable salary string from parsed min/max CAD values."""
    if min_cad is None and max_cad is None:
        return "Salary unknown"
    lo_k = round((min_cad or 0) / 1_000)
    hi_k = round((max_cad or min_cad or 0) / 1_000)
    if lo_k == hi_k:
        return f"${lo_k}K CAD"
    return f"${lo_k}K{_NBHYPHEN}${hi_k}K CAD"


def _salary_badge_html(salary_str: str) -> str:
    if salary_str == "Salary unknown":
        return (
            "<span style='background:#2d2d2d;color:#aaa;padding:3px 8px;"
            "border-radius:4px;font-size:0.85em;font-style:normal'>Salary unknown</span>"
        )
    return (
        f"<span style='background:#1a3a1a;color:#4caf50;padding:3px 8px;"
        f"border-radius:4px;font-size:0.85em;font-style:normal'>{salary_str}</span>"
    )


def _size_badge_html(size_label: str | None) -> str:
    label = size_label or "Size unknown"
    return (
        f"<span style='background:#2d2d2d;color:#ccc;padding:3px 8px;"
        f"border-radius:4px;font-size:0.85em;font-style:normal'>{label}</span>"
    )


# ---------------------------------------------------------------------------
# Full card rendering (shared)
# ---------------------------------------------------------------------------

def render_full_card(
    job: JobPosting,
    specialty: str,
    state: str,
    canonical_label: str | None = None,
    extra_buttons: list | None = None,
) -> None:
    """Render a complete job card.

    Args:
        job: The JobPosting to render.
        specialty: Display specialty name (e.g. 'Data Scientist').
        state: Current lifecycle state string.
        canonical_label: If not None, show a duplicate warning with this label
                         (should be '<title> at <company>' of the canonical post).
        extra_buttons: Optional list of callables that receive no args; called
                       inside st.columns after the card body, for action buttons.
                       Each callable is responsible for rendering one button column.
    """
    chip_bg, chip_fg = SPECIALTY_COLORS.get(specialty, ("#424242", "#fff"))
    source_text = SOURCE_LABEL.get(job.source.value, "")

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

        # Row 2: location + posted date
        location_str = job.location or "N/A"
        posted_str = job.posted_at.strftime("%Y-%m-%d") if job.posted_at else "N/A"
        st.caption(f"{location_str}  ·  Posted: {posted_str}")

        # Row 3: specialty chip
        st.markdown(
            f"<span style='background:{chip_bg};padding:2px 8px;border-radius:4px;"
            f"font-size:0.8em;color:{chip_fg}'>🏷 {specialty}</span>",
            unsafe_allow_html=True,
        )

        # Row 4: description snippet
        summary = job.description or ""
        truncated = (summary[:120] + "...") if len(summary) > 120 else summary
        st.caption(truncated)

        # Row 5: salary + size + source badges
        salary_str = format_salary(job.salary_min_cad, job.salary_max_cad)
        salary_badge = _salary_badge_html(salary_str)
        size_badge = _size_badge_html(job.company_employees_label)
        st.html(
            f"{salary_badge} &nbsp; {size_badge} &nbsp; "
            f"<small style='color:#888'>{source_text}</small>"
        )

        # Duplicate flag
        if canonical_label is not None:
            st.html(
                f"<span style='background:#FFF3CD;color:#856404;padding:2px 8px;"
                f"border-radius:4px;font-size:0.75rem'>&#9888; Duplicate of: "
                f"{canonical_label}</span>"
            )

        # Extra buttons (action row)
        if extra_buttons:
            cols = st.columns(len(extra_buttons))
            for col, btn_fn in zip(cols, extra_buttons):
                with col:
                    btn_fn()
