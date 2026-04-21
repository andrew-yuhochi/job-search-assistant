# Dismissed page — lists jobs the user has dismissed and allows un-dismissal.
# TASK-010: Implements repository.list_jobs(state='dismissed') + Un-dismiss button.
# Un-dismiss: update_job_state(new) + SignalService.record(un_dismiss) → card removed from list.

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.services.signal_service import SignalService
from src.storage import repository
from src.storage.db import get_engine


@st.cache_resource
def _get_db_engine():
    return get_engine()


def _get_specialty(job_id: str) -> str:
    engine = _get_db_engine()
    clf = repository.get_classification(engine, job_id)
    if clf:
        return clf.get("specialty_name", "Unclassified")
    return "Unclassified"


# ---------------------------------------------------------------------------
# Page layout
# ---------------------------------------------------------------------------

st.title("Dismissed")
st.caption("Posts you have dismissed. Click Un-dismiss to restore a post to the feed.")

engine = _get_db_engine()
dismissed_jobs = repository.list_jobs(engine, user_id="local", state="dismissed")

if not dismissed_jobs:
    st.divider()
    st.caption("No dismissed posts.")
    st.stop()

st.divider()

for job in dismissed_jobs:
    job_id = job.job_id
    specialty = _get_specialty(job_id)

    with st.container(border=True):
        col_info, col_btn = st.columns([5, 1])
        with col_info:
            st.markdown(
                f"**{job.title}** &nbsp; "
                f"<span style='color:#aaa'>{job.company}</span>",
                unsafe_allow_html=True,
            )
            st.caption(f"{job.location or 'N/A'} · {specialty}")
        with col_btn:
            if st.button("Un-dismiss", key=f"undismiss_{job_id}", use_container_width=True):
                repository.update_job_state(engine, job_id, "new")
                # Write state_change signal
                SignalService.record_state_change(
                    engine=engine,
                    job_id=job_id,
                    from_state="dismissed",
                    to_state="new",
                    specialty_name=specialty,
                    classification_confidence=None,
                )
                # Write explicit un_dismiss event
                SignalService.record(
                    engine=engine,
                    job_id=job_id,
                    event_type="un_dismiss",
                    specialty_name=specialty,
                )
                st.session_state.pop(f"job_states", None)
                st.toast(f"Restored: {job.title}")
                st.rerun()
