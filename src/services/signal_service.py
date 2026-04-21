# SignalService — records commercial-signal events for the Job Search Assistant.
# Wraps repository.insert_signal() with business logic: state-change events carry
# from/to state; detail_view_close events carry dwell_ms in metadata.
# Per TDD §2.3 (Analytical / Business Logic layer) and TASK-010.

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.engine import Engine

from src.storage import repository

logger = logging.getLogger(__name__)


class SignalService:
    """
    Records signal events for the commercial-signal instrument.

    All methods are static so callers do not need to instantiate the service —
    usage: ``SignalService.record(engine, job_id, "state_change", ...)``.

    Supported event_type values (per TDD §10):
        state_change       — job state machine transition
        detail_view_open   — user opened the detail pane for a job
        detail_view_close  — user closed (or navigated away from) the detail pane
        mark_applied       — user clicked [Mark Applied]
        mark_dismissed     — user clicked [Mark Dismissed]
        un_dismiss         — user clicked [Un-dismiss] on the Dismissed page
    """

    @staticmethod
    def record(
        engine: Engine,
        job_id: str,
        event_type: str,
        user_id: str = "local",
        from_state: Optional[str] = None,
        to_state: Optional[str] = None,
        specialty_name: Optional[str] = None,
        classification_confidence: Optional[str] = None,
        dwell_ms: Optional[int] = None,
        override_reason: Optional[str] = None,
    ) -> int:
        """
        Insert a signal_events row via repository.insert_signal() and return the event_id.

        Args:
            engine: SQLAlchemy Engine.
            job_id: FK → jobs.job_id for the job this event relates to.
            event_type: One of the supported event type strings listed in the class docstring.
            user_id: User scope; defaults to 'local'.
            from_state: Prior job state (set for state_change events).
            to_state: New job state (set for state_change events).
            specialty_name: Specialty from classifications row at event time (optional).
            classification_confidence: Confidence from classifications at event time (optional).
            dwell_ms: Milliseconds the detail pane was open (set for detail_view_close events).
            override_reason: 'quick_dismiss_lt_15s' | 'acted_on_unclassified' | None.

        Returns:
            The newly assigned event_id.
        """
        logger.debug(
            "SignalService.record: job_id=%s event_type=%s from=%s to=%s dwell_ms=%s",
            job_id, event_type, from_state, to_state, dwell_ms,
        )
        return repository.insert_signal(
            engine=engine,
            user_id=user_id,
            event_type=event_type,
            job_id=job_id,
            from_state=from_state,
            to_state=to_state,
            specialty_name=specialty_name,
            classification_confidence=classification_confidence,
            dwell_ms=dwell_ms,
            override_reason=override_reason,
        )

    @staticmethod
    def record_state_change(
        engine: Engine,
        job_id: str,
        from_state: str,
        to_state: str,
        user_id: str = "local",
        specialty_name: Optional[str] = None,
        classification_confidence: Optional[str] = None,
    ) -> int:
        """
        Convenience wrapper: record a state_change event.

        Automatically infers override_reason:
        - 'acted_on_unclassified' when specialty_name is None or 'Unclassified'
          and to_state is 'applied' or 'dismissed'.

        Returns:
            The newly assigned event_id.
        """
        override_reason: Optional[str] = None
        if (
            to_state in ("applied", "dismissed")
            and (specialty_name is None or specialty_name == "Unclassified")
        ):
            override_reason = "acted_on_unclassified"

        return SignalService.record(
            engine=engine,
            job_id=job_id,
            event_type="state_change",
            user_id=user_id,
            from_state=from_state,
            to_state=to_state,
            specialty_name=specialty_name,
            classification_confidence=classification_confidence,
            override_reason=override_reason,
        )
