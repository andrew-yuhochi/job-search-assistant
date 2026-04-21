# Main entry point for the Job Search Assistant Streamlit app.
# Initialises the database engine, sets page config, and provides
# the sidebar navigation and "Run Scraper" button.
# Per TDD §2.5 and TASK-005.
# TASK-009: Prototype mode removed — DB mode is the only mode.
# TASK-011: JobSourceRegistry wired at startup.
# TASK-013: ScrapeRunner wired to [Run Scraper] button with live status strip.

from __future__ import annotations

import json
import logging

import streamlit as st

from src.config import settings
from src.runner.scrape_runner import ScrapeConfig, ScrapeRunner
from src.services.filter_service import FilterConfig
from src.sources import (
    GoogleJobsSource,
    IndeedSource,
    JobSourceRegistry,
    LinkedInSource,
)
from src.sources.base import SearchQuery
from src.storage.db import get_engine
from src.storage.repository import get_settings

logger = logging.getLogger(__name__)

# Must be the very first Streamlit call in the entry-point file.
st.set_page_config(
    page_title="Job Search Assistant",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialise the database (idempotent — no-op if already bootstrapped).
engine = get_engine()

# ---------------------------------------------------------------------------
# Source registry — populated once at startup, reused across requests.
# TASK-011: registered via plugin pattern — no "if source == X" branching.
# ---------------------------------------------------------------------------

if "source_registry" not in st.session_state:
    _registry = JobSourceRegistry()
    _registry.register(LinkedInSource())
    _registry.register(IndeedSource())
    _registry.register(GoogleJobsSource())
    st.session_state.source_registry = _registry

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

if "selected_job_id" not in st.session_state:
    st.session_state.selected_job_id = None

if "job_states" not in st.session_state:
    st.session_state.job_states = {}  # job_id -> state string

if "pending_toast" not in st.session_state:
    st.session_state.pending_toast = None  # {"msg": str, "icon": str} or None

if "scrape_running" not in st.session_state:
    st.session_state.scrape_running = False

if "scrape_status" not in st.session_state:
    st.session_state.scrape_status = ""

if "last_scrape_result" not in st.session_state:
    st.session_state.last_scrape_result = None  # ScrapeRunResult | None


# ---------------------------------------------------------------------------
# Helper: build ScrapeConfig from user_settings in the DB
# ---------------------------------------------------------------------------


def _build_scrape_config() -> tuple[ScrapeConfig, SearchQuery]:
    """Read user_settings from DB and produce a ScrapeConfig + SearchQuery."""
    user_id = "local"
    db_settings = get_settings(engine, user_id=user_id)

    # Build filter config from stored settings
    location_pref = (db_settings or {}).get("location_preference", "both")
    salary_floor = (db_settings or {}).get("salary_floor_cad")
    excluded_seniority_raw = (db_settings or {}).get("excluded_seniority_levels", "[]")
    try:
        excluded_seniority = json.loads(excluded_seniority_raw)
    except (ValueError, TypeError):
        excluded_seniority = []

    # Map location_preference to filter locations list
    if location_pref == "vancouver":
        locations = ["Vancouver"]
        allow_remote = False
    elif location_pref == "remote_friendly":
        locations = ["Remote"]
        allow_remote = True
    else:  # "both"
        locations = ["Vancouver", "Remote"]
        allow_remote = True

    # Use first excluded seniority as max (simplified for PoC)
    max_seniority: str | None = excluded_seniority[0] if excluded_seniority else None

    filter_config = FilterConfig(
        locations=locations,
        min_salary_cad=float(salary_floor) if salary_floor else None,
        max_seniority=max_seniority,
        allow_remote=allow_remote,
    )

    scrape_config = ScrapeConfig(
        filter_config=filter_config,
        user_id=user_id,
    )

    # Build search query — hard-coded title for PoC; location from preference
    loc_str = "Vancouver, BC, Canada" if location_pref in ("vancouver", "both") else "Canada"
    query = SearchQuery(
        search_term="data scientist",
        location=loc_str,
        results_wanted=25,
    )

    return scrape_config, query


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("Job Search Assistant")

    st.page_link("pages/1_Feed.py", label="Job Feed", icon="📋")
    st.page_link("pages/2_Applied.py", label="Applied", icon="✅")
    st.page_link("pages/3_Dismissed.py", label="Dismissed", icon="🚫")
    st.page_link("pages/4_Knowledge_Bank.py", label="Knowledge Bank", icon="📚")
    st.page_link("pages/5_Settings.py", label="Settings", icon="⚙️")
    st.page_link("pages/6_Signals.py", label="Signals", icon="📊")

    st.divider()

    # Status strip — shown when a run is in progress or just finished
    status_placeholder = st.empty()
    if st.session_state.scrape_status:
        status_placeholder.info(st.session_state.scrape_status)

    # [Run Scraper] button — disabled during an active run
    if st.button(
        "Run Scraper",
        type="primary",
        use_container_width=True,
        disabled=st.session_state.scrape_running,
    ):
        st.session_state.scrape_running = True
        st.session_state.scrape_status = "Starting scrape run…"
        st.session_state.last_scrape_result = None
        status_placeholder.info(st.session_state.scrape_status)

        def _on_status(msg: str) -> None:
            st.session_state.scrape_status = msg
            status_placeholder.info(msg)

        try:
            scrape_config, query = _build_scrape_config()
            runner = ScrapeRunner(
                registry=st.session_state.source_registry,
                engine=engine,
                config=scrape_config,
            )
            result = runner.run(query, status_callback=_on_status)
            st.session_state.last_scrape_result = result

            # Final status
            final_msg = (
                f"Run #{result.run_id} done — "
                f"{result.stored} new jobs in {result.elapsed_seconds:.1f}s"
            )
            st.session_state.scrape_status = final_msg
            status_placeholder.success(final_msg)

        except Exception as exc:
            err_msg = f"Scrape run failed: {exc}"
            logger.error("app.py: scrape run raised an exception", exc_info=True)
            st.session_state.scrape_status = err_msg
            status_placeholder.error(err_msg)
        finally:
            st.session_state.scrape_running = False

    # Rate-limit warning banners — one per rate-limited source from the last run
    result = st.session_state.last_scrape_result
    if result and result.rate_limited_sources:
        for src in result.rate_limited_sources:
            st.warning(
                f"{src.capitalize()} returned HTTP 429 (rate limited). "
                f"Other sources continued normally."
            )
            if st.button(f"Retry {src.capitalize()}", key=f"retry_{src}"):
                # Trigger a single-source re-run (reuses the same button flow)
                # For PoC we simply note this is a future enhancement.
                st.info(
                    f"Single-source retry for {src} is not yet implemented. "
                    f"Click [Run Scraper] to retry all sources."
                )


# ---------------------------------------------------------------------------
# Main page body
# ---------------------------------------------------------------------------

st.header("Job Search Assistant")
st.markdown("Navigate to **Job Feed** in the sidebar to browse jobs.")

# Show a brief run summary if one just finished
if st.session_state.last_scrape_result:
    r = st.session_state.last_scrape_result
    st.info(
        f"Last scrape run #{r.run_id}: "
        f"fetched={r.fetched} | "
        f"normalized={r.normalized} | "
        f"dedup_removed={r.duplicate_count} | "
        f"filter_kept={r.after_filter} | "
        f"stored={r.stored} | "
        f"elapsed={r.elapsed_seconds}s"
    )
