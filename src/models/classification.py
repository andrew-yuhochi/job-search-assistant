"""
Classification model for the Job Search Assistant.

Stores the output of ClassifierService (TASK-014): the specialty assignment for
a given job posting, along with the evidence signals and confidence level that
produced the classification.
"""
from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from src.models.models import ConfidenceLevel, SpecialtyType  # noqa: F401 — re-exported for callers


class Classification(BaseModel):
    """
    Result of classifying a job posting against a SpecialtyType. TDD §2.4 classifications table.
    Created by ClassifierService and stored via ClassificationRepository.
    """
    job_posting_id: str
    specialty_name: str  # matches SpecialtyType.name
    confidence: ConfidenceLevel
    duty_signals: list[str] = Field(default_factory=list)  # verbatim quotes from posting
    prompt_version: str = "v1"
    user_id: str = "local"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
