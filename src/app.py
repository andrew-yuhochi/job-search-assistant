# Main entry point for the Job Search Assistant Streamlit app.
# Initialises the database engine, sets page config, and provides
# the sidebar navigation and "Run Scraper" button stub.
# Per TDD §2.5 and TASK-005.
# TASK-009: Prototype mode removed — DB mode is the only mode.
# TASK-011: JobSourceRegistry wired at startup.

from __future__ import annotations

import streamlit as st

from src.sources import (
    GoogleJobsSource,
    IndeedSource,
    JobSourceRegistry,
    LinkedInSource,
)
from src.storage.db import get_engine

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

    if st.button("Run Scraper", type="primary", use_container_width=True):
        st.info("Scraper integration coming in Milestone 3.")

# ---------------------------------------------------------------------------
# Main page body
# ---------------------------------------------------------------------------

st.header("Job Search Assistant")
st.markdown("Navigate to **Job Feed** in the sidebar to browse jobs.")
