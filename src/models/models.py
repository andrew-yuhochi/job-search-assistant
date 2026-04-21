"""
Pydantic domain models for the Job Search Assistant.

All models are defined in this single module and exported from src/models/__init__.py.
Each model docstring references the TDD section that defines its structure (TDD §2.4 schema
and §2.1 data ingestion layer contracts).

Usage:
    from src.models import RawJobPosting, JobPosting, JobState, SourceName, ...
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class JobState(str, Enum):
    """
    Lifecycle state for a job posting. TDD §2.4 schema — jobs.state column.
    Transitions: new → reviewed → applied | dismissed.
    """
    new = "new"
    reviewed = "reviewed"
    applied = "applied"
    dismissed = "dismissed"


class SourceName(str, Enum):
    """
    Registered job source names. TDD §2.1 JobSource.name contract.
    Matches the three concrete source implementations at PoC.
    Note: Google Jobs source is registered as 'google', not 'google_jobs',
    to align with fixture data and SerpAPI client naming.
    """
    linkedin = "linkedin"
    indeed = "indeed"
    google = "google"


class SpecialtyTier(int, Enum):
    """
    Specialty type tier. TDD §2.4 specialty_types.tier column.
    1 = hardcoded seed, 2 = configurable, 3 = user-proposed.
    """
    tier1 = 1
    tier2 = 2
    tier3 = 3


class SeniorityLevel(str, Enum):
    """
    Seniority levels inferred from job title or source field. TDD §2.2 SeniorityInferrer.
    """
    junior = "junior"
    mid = "mid"
    senior = "senior"
    principal = "principal"
    staff = "staff"
    director = "director"
    vp = "vp"
    csuite = "csuite"
    unknown = "unknown"


class ConfidenceLevel(str, Enum):
    """
    Classifier confidence level. TDD §2.4 classifications.confidence column.
    """
    high = "high"
    medium = "medium"
    low = "low"


class LocationPreference(str, Enum):
    """
    User location filter preference. TDD §2.4 user_settings.location_preference column.
    """
    vancouver = "vancouver"
    remote_friendly = "remote_friendly"
    both = "both"


# ---------------------------------------------------------------------------
# Salary helper
# ---------------------------------------------------------------------------


class SalaryRange(BaseModel):
    """
    Parsed salary range in CAD. Produced by SalaryExtractor. TDD §2.2.
    All values are in integer CAD (thousands not used — raw dollar amounts).
    """
    min_cad: int = Field(..., ge=0, description="Minimum salary in CAD")
    max_cad: int = Field(..., ge=0, description="Maximum salary in CAD")
    source: str = Field(..., description="'regex' | 'llm' | 'source_field'")

    @field_validator("max_cad")
    @classmethod
    def max_gte_min(cls, v: int, info) -> int:
        min_val = info.data.get("min_cad")
        if min_val is not None and v < min_val:
            raise ValueError(f"max_cad ({v}) must be >= min_cad ({min_val})")
        return v


# ---------------------------------------------------------------------------
# Raw ingestion model — mirrors fixture schema exactly
# ---------------------------------------------------------------------------


class RawJobPosting(BaseModel):
    """
    Raw scraper output before any normalization. TDD §2.1 / §2.2.
    Field names and types match tests/fixtures/jobs_fixtures.json exactly,
    including the expected_specialty field added for TASK-009 seeding.

    This model is the interface contract between the JobSource layer and
    the Normalizer. All fields are kept as-scraped; normalization happens
    in Normalizer.normalize() → JobPosting.
    """
    id: str = Field(..., description="Fixture or source-assigned ID")
    title: str
    company: str
    location: str
    source: SourceName
    url: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    salary_raw: Optional[str] = Field(None, description="Raw salary string as scraped; None if not listed")
    posted_date: str = Field(..., description="Date string as returned by source (YYYY-MM-DD or relative)")
    user_id: str = Field(default="local")
    expected_specialty: Optional[str] = Field(
        None,
        description="Ground-truth label used for fixture seeding (TASK-009). "
                    "Not present in live-scraped data.",
    )


# ---------------------------------------------------------------------------
# Normalized model — post-normalization with parsed salary, seniority, location type
# ---------------------------------------------------------------------------


class NormalizedJobPosting(BaseModel):
    """
    Output of Normalizer.normalize(raw: RawJobPosting). TDD §2.2.
    Contains all canonical fields derived from the raw posting:
    - Parsed salary range (or None)
    - Inferred seniority level
    - Location type flag (is_remote)
    - Normalized title / company strings
    - Extracted source_job_id and url_hostname

    extra='allow' permits FilterService to attach `badge_flags` dynamically.
    """
    model_config = ConfigDict(extra="allow")
    # Identity
    id: str
    user_id: str = "local"

    # Raw passthrough
    source: SourceName
    url: str
    title: str
    company: str
    location: str
    description: str

    # Normalized / derived fields
    title_normalized: str = Field(..., description="Lowercase, punctuation-stripped, whitespace-collapsed title")
    company_normalized: str = Field(..., description="Lowercase, punctuation-stripped company name")
    url_hostname: str = Field(..., description="Hostname extracted from url (e.g. 'www.linkedin.com')")
    source_job_id: Optional[str] = Field(None, description="Job ID extracted from URL via source-specific regex")
    posted_at: Optional[datetime] = Field(None, description="Parsed absolute datetime from posted_date string")
    is_remote: Optional[bool] = None

    # Salary
    salary_range: Optional[SalaryRange] = None
    salary_source: Optional[str] = Field(None, description="'regex' | 'llm' | 'source_field'")

    # Seniority
    seniority: SeniorityLevel = SeniorityLevel.unknown

    # Company size
    company_employees_label: Optional[str] = None
    company_size_bucket: Optional[str] = Field(
        None,
        description="'micro' | 'small' | 'medium' | 'large' | 'unknown'",
    )


# ---------------------------------------------------------------------------
# Full DB-backed model
# ---------------------------------------------------------------------------


class JobPosting(BaseModel):
    """
    Full DB-backed model for a job posting. TDD §2.4 jobs table.
    Produced by Normalizer and stored via Repository. Carries multi-tenant
    user_id (default 'local') and lifecycle state.

    Dedup fields (duplicate_of, url_is_dead) are populated by DedupService
    and the URL health check respectively.

    extra='allow' permits FilterService to attach `badge_flags` dynamically
    (e.g. 'Salary unknown', 'Size unknown') without requiring a schema change.
    """
    model_config = ConfigDict(extra="allow")

    # Primary key
    job_id: str = Field(..., description="sha256(source + url) — deterministic dedup key")
    user_id: str = "local"

    # Source metadata
    source: SourceName
    source_job_id: Optional[str] = None
    url: str
    url_hostname: str

    # Content
    title: str
    title_normalized: str
    company: str
    company_normalized: str
    location: Optional[str] = None
    is_remote: Optional[bool] = None
    posted_at: Optional[datetime] = None
    description: str

    # Salary (populated by SalaryExtractor)
    salary_min_cad: Optional[int] = None
    salary_max_cad: Optional[int] = None
    salary_source: Optional[str] = None

    # Seniority (populated by SeniorityInferrer)
    seniority: SeniorityLevel = SeniorityLevel.unknown

    # Company size (from scraper)
    company_employees_label: Optional[str] = None
    company_size_bucket: Optional[str] = None

    # Dedup
    duplicate_of: Optional[str] = Field(None, description="job_id of canonical posting; None = this is canonical")

    # Timestamps
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    url_health_checked_at: Optional[datetime] = None
    url_is_dead: Optional[bool] = None
    state_updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Lifecycle state
    state: JobState = JobState.new


# ---------------------------------------------------------------------------
# Knowledge bank models
# ---------------------------------------------------------------------------


class KnowledgeBankChunk(BaseModel):
    """
    A single section chunk of a knowledge bank document. TDD §2.4 knowledge_bank_chunks table.
    Character offsets (char_start / char_end) index into the parent document's full_text
    and are used by the Citations API to ground highlight draft bullets.
    """
    chunk_id: Optional[int] = None
    kb_doc_id: Optional[int] = None
    source_path: str = Field(..., description="Filename or path of the source document")
    section_heading: str = Field(..., description="Title of the section (e.g., '## TU — Data Science')")
    char_start: int = Field(..., ge=0, description="Start character index in the document full_text")
    char_end: int = Field(..., ge=0, description="End character index (exclusive) in the document full_text")
    content: str = Field(..., min_length=1, description="Verbatim text of this chunk")
    order_index: int = Field(default=0, ge=0)

    @field_validator("char_end")
    @classmethod
    def end_gt_start(cls, v: int, info) -> int:
        start = info.data.get("char_start")
        if start is not None and v <= start:
            raise ValueError(f"char_end ({v}) must be > char_start ({start})")
        return v


class KnowledgeBank(BaseModel):
    """
    Document-level knowledge bank model. TDD §2.4 knowledge_bank_documents table.
    Contains the full parsed document plus its chunks for citation alignment.
    """
    kb_doc_id: Optional[int] = None
    user_id: str = "local"
    file_path: str = Field(..., description="Original filename or upload path")
    full_text: str = Field(..., min_length=1)
    word_count: int = Field(..., ge=0)
    uploaded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_active: bool = True
    chunks: list[KnowledgeBankChunk] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Highlight draft
# ---------------------------------------------------------------------------


class HighlightDraft(BaseModel):
    """
    CV highlight draft produced by DraftService via Citations API. TDD §2.4 highlight_drafts table.
    Each bullet is grounded by one or more source chunk references.
    Persisted only on 'Mark Applied' at PoC (persisted_reason='applied').
    """
    draft_id: Optional[int] = None
    job_posting_id: str = Field(..., description="FK → jobs.job_id")
    user_id: str = "local"
    bullets: list[str] = Field(
        ...,
        min_length=1,
        description="List of drafted CV bullet strings",
    )
    citations: list[str] = Field(
        default_factory=list,
        description="Source chunk references parallel to bullets (section titles or chunk IDs)",
    )
    model_name: str = Field(default="claude-haiku-4-5-20251001")
    prompt_version: str = Field(default="v1")
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    persisted_reason: str = Field(
        default="applied",
        description="'applied' | 'manual_save' — reason the draft was persisted",
    )


# ---------------------------------------------------------------------------
# Specialty type
# ---------------------------------------------------------------------------


class SpecialtyType(BaseModel):
    """
    A job specialty classification type. TDD §2.4 specialty_types table.
    Tier 1 = seed (hardcoded), Tier 2 = config, Tier 3 = user-proposed via LLM.
    source field distinguishes how the type was added:
      'seed'     = system-defined at bootstrap
      'config'   = defined in config file (e.g., Tier 2 Analytics Engineer)
      'proposed' = proposed by SpecialtyTypeProposer and accepted by user
    """
    specialty_id: Optional[int] = None
    user_id: str = "local"
    name: str = Field(..., min_length=1, description="Display name e.g. 'Data Scientist'")
    description: Optional[str] = None
    duty_signals: list[str] = Field(default_factory=list, description="Example duty phrases for prompt context")
    tier: SpecialtyTier = SpecialtyTier.tier1
    enabled: bool = True
    source: str = Field(
        default="seed",
        description="'seed' | 'config' | 'proposed'",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("source")
    @classmethod
    def source_valid(cls, v: str) -> str:
        allowed = {"seed", "config", "proposed"}
        if v not in allowed:
            raise ValueError(f"source must be one of {allowed!r}, got {v!r}")
        return v


# ---------------------------------------------------------------------------
# Signal event
# ---------------------------------------------------------------------------


class SignalEvent(BaseModel):
    """
    Analytics event for the commercial-signal instrument. TDD §2.4 signal_events table / §10.
    Captures state transitions, detail view dwell time, and inferred override events.
    Used by SignalService.override_rate() to compute the PoC commercial signal.
    """
    event_id: Optional[int] = None
    event_type: str = Field(
        ...,
        description="'state_change' | 'detail_view_open' | 'detail_view_close' | 'override_inferred'",
    )
    job_posting_id: Optional[str] = Field(None, description="FK → jobs.job_id; None for session-level events")
    user_id: str = "local"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # State-change specifics
    from_state: Optional[JobState] = None
    to_state: Optional[JobState] = None

    # Classification context (copied at event time)
    specialty_name: Optional[str] = None
    classification_confidence: Optional[ConfidenceLevel] = None

    # Dwell-time (detail_view_close)
    dwell_ms: Optional[int] = Field(None, ge=0)

    # Override inference
    override_reason: Optional[str] = Field(
        None,
        description="'quick_dismiss_lt_15s' | 'acted_on_unclassified' | null",
    )

    @field_validator("event_type")
    @classmethod
    def event_type_valid(cls, v: str) -> str:
        allowed = {"state_change", "detail_view_open", "detail_view_close", "override_inferred"}
        if v not in allowed:
            raise ValueError(f"event_type must be one of {allowed!r}, got {v!r}")
        return v
