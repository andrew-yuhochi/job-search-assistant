# Main entry point for the Job Search Assistant Streamlit app.
# Initialises the database engine, sets page config, and provides
# the sidebar navigation and "Run Scraper" button stub.
# Per TDD §2.5 and TASK-005.

from __future__ import annotations

import os

import streamlit as st

# Prototype mode: load fixtures instead of DB when MODE=prototype
PROTOTYPE_MODE = os.environ.get("MODE", "").lower() == "prototype"

# Must be the very first Streamlit call in the entry-point file.
st.set_page_config(
    page_title="Job Search Assistant",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialise the database (idempotent — no-op if already bootstrapped).
# Skipped in prototype mode to avoid DB dependency during Stage 1 prototype.
if not PROTOTYPE_MODE:
    from src.storage.db import get_engine
    engine = get_engine()

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

if "selected_job_id" not in st.session_state:
    st.session_state.selected_job_id = None

if "job_states" not in st.session_state:
    st.session_state.job_states = {}  # job_id -> state string

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("Job Search Assistant")

    st.page_link("src/pages/1_Feed.py", label="Job Feed", icon="📋")
    st.page_link("src/pages/2_Applied.py", label="Applied", icon="✅")
    st.page_link("src/pages/3_Dismissed.py", label="Dismissed", icon="🚫")
    st.page_link("src/pages/4_Knowledge_Bank.py", label="Knowledge Bank", icon="📚")
    st.page_link("src/pages/5_Settings.py", label="Settings", icon="⚙️")
    st.page_link("src/pages/6_Signals.py", label="Signals", icon="📊")

    st.divider()

    if PROTOTYPE_MODE:
        st.caption("Prototype mode — scraper disabled")
        st.button("Run Scraper", disabled=True, use_container_width=True)
    else:
        if st.button("Run Scraper", type="primary", use_container_width=True):
            st.info("Scraper integration coming in Milestone 3.")

# ---------------------------------------------------------------------------
# Main page body — redirect hint
# ---------------------------------------------------------------------------

st.header("Job Search Assistant")
if PROTOTYPE_MODE:
    st.info("Running in **prototype mode** — fixtures loaded from `tests/fixtures/jobs_fixtures.json`.")
    st.markdown("Navigate to **Job Feed** in the sidebar to see the prototype dashboard.")
else:
    st.markdown("Navigate to **Job Feed** in the sidebar to browse jobs.")
