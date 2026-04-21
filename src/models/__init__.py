"""
src/models — Pydantic domain models for the Job Search Assistant.

All models are defined in models.py (and classification.py) and re-exported here:
    from src.models import RawJobPosting, JobPosting, JobState, Classification, ...
"""

from src.models.classification import Classification
from src.models.models import (
    # Enums
    ConfidenceLevel,
    JobState,
    LocationPreference,
    SeniorityLevel,
    SourceName,
    SpecialtyTier,
    # Value objects
    SalaryRange,
    # Core posting models
    RawJobPosting,
    NormalizedJobPosting,
    JobPosting,
    # Knowledge bank models
    KnowledgeBankChunk,
    KnowledgeBank,
    # Highlight draft
    HighlightDraft,
    # Specialty type
    SpecialtyType,
    # Signal analytics
    SignalEvent,
)

__all__ = [
    # Classification model
    "Classification",
    # Enums
    "ConfidenceLevel",
    "JobState",
    "LocationPreference",
    "SeniorityLevel",
    "SourceName",
    "SpecialtyTier",
    # Value objects
    "SalaryRange",
    # Core posting models
    "RawJobPosting",
    "NormalizedJobPosting",
    "JobPosting",
    # Knowledge bank models
    "KnowledgeBankChunk",
    "KnowledgeBank",
    # Highlight draft
    "HighlightDraft",
    # Specialty type
    "SpecialtyType",
    # Signal analytics
    "SignalEvent",
]
