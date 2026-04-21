# Applied page — lists jobs the user has marked as applied.
# Shows full card layout (title, company, salary badge, specialty chip, location,
# duty summary, posted date) via the shared render_full_card helper from _card.py.
# Un-apply button restores the job to state=new in the main feed.
# Fix 3: replaces the empty-state stub from the Milestone 2 validation round.

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.pages._card import render_full_card
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

st.title("Applied")
st.caption("Posts you have marked as applied.")

engine = _get_db_engine()
applied_jobs = repository.list_jobs(engine, user_id="local", state="applied")

if not applied_jobs:
    st.divider()
    st.info("No applied posts yet.")
    st.stop()

st.divider()

for job in applied_jobs:
    job_id = job.job_id
    specialty = _get_specialty(job_id)

    def _make_unapply_btn(jid: str, spec: str):
        def _btn():
            if st.button("Un-apply", key=f"unapply_{jid}", use_container_width=True):
                repository.update_job_state(engine, jid, "new")
                SignalService.record_state_change(
                    engine=engine,
                    job_id=jid,
                    from_state="applied",
                    to_state="new",
                    specialty_name=spec,
                    classification_confidence=None,
                )
                SignalService.record(
                    engine=engine,
                    job_id=jid,
                    event_type="un_apply",
                    specialty_name=spec,
                )
                st.session_state.pop("job_states", None)
                st.toast(f"Restored: {job.title}")
                st.rerun()
        return _btn

    render_full_card(
        job=job,
        specialty=specialty,
        state="applied",
        canonical_label=None,
        extra_buttons=[_make_unapply_btn(job_id, specialty)],
    )
