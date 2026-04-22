"""
FilterService: applies hard filters to job postings per PRD §6.

Critical design rule (non-negotiable):
  - Salary UNKNOWN → PASS with badge "Salary unknown"
  - Company size UNKNOWN → PASS with badge "Size unknown"
  - Only exclude when the KNOWN value fails the filter.

FilterConfig controls which filters are active; any field set to None disables
that filter. FilterResult separates kept postings from excluded ones, with
the exclusion reason attached for UI display and demo artifacts.

Called by ScrapeRunner (TASK-013) after Normalizer/SalaryExtractor/SeniorityInferrer
and before storage write.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Union

from src.models.models import JobPosting, NormalizedJobPosting, SeniorityLevel

logger = logging.getLogger(__name__)

# Seniority levels that are ordered for comparison (lower index = more junior)
_SENIORITY_ORDER: list[SeniorityLevel] = [
    SeniorityLevel.junior,
    SeniorityLevel.mid,
    SeniorityLevel.senior,
    SeniorityLevel.staff,
    SeniorityLevel.principal,
    SeniorityLevel.director,
    SeniorityLevel.vp,
    SeniorityLevel.csuite,
]

# Indeed company_employees_label values that count as "1 to 10" micro-startups
# Add any variant Indeed returns for very small companies.
_MICRO_SIZE_LABELS: frozenset[str] = frozenset({
    "1 to 10",
    "1-10",
    "1 to 10 employees",
    "fewer than 10",
    "under 10",
    "1-10 employees",
})

# Default metro Vancouver municipalities (case-insensitive substring match).
# Loaded from config/filter_defaults.yaml at runtime; this constant is a
# fallback for direct FilterService construction without a config file.
_DEFAULT_METRO_LOCATIONS: tuple[str, ...] = (
    "vancouver",
    "burnaby",
    "richmond",
    "surrey",
    "coquitlam",
    "north vancouver",
    "west vancouver",
    "new westminster",
    "delta",
    "langley",
    "maple ridge",
    "port moody",
    "port coquitlam",
    "pitt meadows",
    "white rock",
    "abbotsford",
)

_REMOTE_KEYWORDS: frozenset[str] = frozenset({
    "remote",
    "work from home",
    "wfh",
    "distributed",
    "anywhere",
})

# US city/state patterns that disqualify a "remote" posting as non-Canadian.
# Matched case-insensitively against the full location string.
_NON_CANADIAN_PATTERNS: tuple[str, ...] = (
    ", tx",
    ", ny",
    ", wa",
    ", or",
    ", il",
    ", ma",
    ", co",
    ", fl",
    ", ga",
    ", nc",
    ", nj",
    ", oh",
    ", mi",
    ", mn",
    ", az",
    ", va",
    ", pa",
    ", tn",
    ", mo",
    ", in",
    "united states",
    ", usa",
    " usa",
    ", u.s.",
    "austin",
    "new york, ny",
    "san francisco",
    "seattle, wa",
    "chicago, il",
    "boston, ma",
)

AnyPosting = Union[JobPosting, NormalizedJobPosting]


# ---------------------------------------------------------------------------
# Config and result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class FilterConfig:
    """
    Configuration for a FilterService.apply() call.

    All fields default to None (disabled). Set a field to activate that filter.

    Attributes:
        locations:              Legacy field — if set alongside metro_locations=None,
                                treated as a simple substring list for backward
                                compatibility. Prefer metro_locations for new callers.
        metro_locations:        If set, a posting passes the location filter when its
                                location contains any of these strings (case-insensitive
                                substring).  Defaults to _DEFAULT_METRO_LOCATIONS (all
                                Metro Vancouver municipalities).  Set to None to disable
                                the location filter entirely.
        min_salary_cad:         Exclude postings whose KNOWN salary max is below this
                                threshold. Postings with unknown salary always pass.
        max_seniority:          Exclude postings at or above this seniority level.
                                E.g. "senior" excludes senior, staff, principal, director, vp, csuite.
                                Postings with unknown seniority always pass.
        company_size_exclude:   Exact Indeed company_employees_label strings to exclude.
                                Postings with unknown/missing size always pass.
        allow_remote:           When True, postings whose location contains a remote
                                keyword OR whose is_remote flag is True will pass the
                                location filter — provided they do not have a known
                                non-Canadian location.
    """
    # Legacy simple substring list (kept for backward-compatible test construction).
    locations: list[str] | None = None
    # Metro Vancouver municipality list. When set (even to []), this takes precedence
    # over `locations`.  When None, the location filter is disabled.
    metro_locations: list[str] | None = field(default_factory=lambda: list(_DEFAULT_METRO_LOCATIONS))
    min_salary_cad: float | None = None
    max_seniority: str | None = None
    company_size_exclude: list[str] | None = None
    allow_remote: bool = True


@dataclass
class FilterResult:
    """
    Output of FilterService.apply().

    Attributes:
        kept:       Postings that passed all active filters.
                    Each kept posting may carry badge labels in the
                    `badge_flags` attribute (added dynamically).
        excluded:   List of (posting, reason) pairs for rejected postings.
    """
    kept: list[AnyPosting] = field(default_factory=list)
    excluded: list[tuple[AnyPosting, str]] = field(default_factory=list)

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    @property
    def excluded_count(self) -> int:
        return len(self.excluded)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _get_location(posting: AnyPosting) -> str:
    return (posting.location or "").lower()


def _get_salary_min(posting: AnyPosting) -> int | None:
    if isinstance(posting, JobPosting):
        return posting.salary_min_cad
    # NormalizedJobPosting stores salary in salary_range
    if hasattr(posting, "salary_range") and posting.salary_range is not None:
        return posting.salary_range.min_cad
    return None


def _get_salary_max(posting: AnyPosting) -> int | None:
    if isinstance(posting, JobPosting):
        return posting.salary_max_cad
    if hasattr(posting, "salary_range") and posting.salary_range is not None:
        return posting.salary_range.max_cad
    return None


def _get_seniority(posting: AnyPosting) -> SeniorityLevel:
    return posting.seniority


def _get_company_size_label(posting: AnyPosting) -> str | None:
    return getattr(posting, "company_employees_label", None)


def _seniority_rank(level: SeniorityLevel) -> int:
    """Return ordinal rank of seniority level. Higher = more senior."""
    try:
        return _SENIORITY_ORDER.index(level)
    except ValueError:
        return -1  # unknown → not ranked


def _is_remote_posting(posting: AnyPosting) -> bool:
    """Return True if the posting is remote based on is_remote flag or location text."""
    if getattr(posting, "is_remote", None) is True:
        return True
    loc = _get_location(posting)
    return any(kw in loc for kw in _REMOTE_KEYWORDS)


def _is_non_canadian(loc_lower: str) -> bool:
    """Return True if the location string explicitly names a non-Canadian place."""
    return any(pattern in loc_lower for pattern in _NON_CANADIAN_PATTERNS)


def _location_matches(posting: AnyPosting, locations: list[str]) -> bool:
    """Return True if the posting location contains any of the filter strings."""
    loc = _get_location(posting)
    for filter_loc in locations:
        if filter_loc.lower() in loc:
            return True
    return False


def _check_location(posting: AnyPosting, config: "FilterConfig") -> str | None:
    """
    Apply the expanded location filter.

    Returns None (pass) or an exclusion reason string (fail).

    Logic:
    1. Determine the effective municipality list (metro_locations takes priority
       over legacy `locations`).
    2. If no location filter is active, pass unconditionally.
    3. A posting passes if:
       a. Its location contains any metro municipality, OR
       b. allow_remote=True AND (is_remote flag OR location contains a remote
          keyword) AND the location does NOT contain a non-Canadian pattern.
    4. Everything else is excluded.
    """
    # Resolve which location list to use
    if config.metro_locations is not None:
        active_list = config.metro_locations
    elif config.locations is not None:
        active_list = config.locations
    else:
        # Both None — location filter disabled
        return None

    loc = _get_location(posting)
    loc_display = posting.location or "(no location)"

    # Pass if location matches a metro municipality
    for municipality in active_list:
        if municipality.lower() in loc:
            return None  # pass

    # Pass if remote-allowed AND posting is remote AND Canadian context
    if config.allow_remote and _is_remote_posting(posting):
        if _is_non_canadian(loc):
            return (
                f"location: {loc_display} not in metro Vancouver or remote-CA"
                f" (non-Canadian location)"
            )
        return None  # pass — remote with Canadian/ambiguous location

    return f"location: {loc_display} not in metro Vancouver or remote-CA"


# ---------------------------------------------------------------------------
# FilterService
# ---------------------------------------------------------------------------


class FilterService:
    """
    Applies hard filters to a list of job postings.

    Design invariants (per PRD §6 and TASK-012 context):
    - Salary unknown  → ALWAYS passes the salary filter; badge "Salary unknown" added
    - Size unknown    → ALWAYS passes the size filter; badge "Size unknown" added
    - Location unknown → treated as pass for location filter (avoids silent exclusion)
    - Seniority unknown → ALWAYS passes the seniority filter

    Only exclude when the KNOWN value violates the configured threshold.
    """

    def apply(
        self,
        postings: list[AnyPosting],
        config: FilterConfig,
    ) -> FilterResult:
        """
        Filter postings according to config.

        Adds a `badge_flags` set attribute to each kept posting to communicate
        "Salary unknown" / "Size unknown" badges to the UI layer. This attribute
        is dynamically added to the posting object (no schema change needed).

        Args:
            postings:   List of JobPosting or NormalizedJobPosting objects.
            config:     Active filter configuration.

        Returns:
            FilterResult with kept and excluded lists.
        """
        result = FilterResult()

        for posting in postings:
            badges: set[str] = set()
            exclusion_reason: str | None = None

            # --- Location filter ---
            if exclusion_reason is None:
                exclusion_reason = _check_location(posting, config)

            # --- Seniority filter ---
            if config.max_seniority is not None and exclusion_reason is None:
                seniority = _get_seniority(posting)
                if seniority == SeniorityLevel.unknown:
                    # Unknown seniority always passes
                    pass
                else:
                    max_rank = _seniority_rank(
                        SeniorityLevel(config.max_seniority)
                        if config.max_seniority in SeniorityLevel.__members__
                        else SeniorityLevel.unknown
                    )
                    posting_rank = _seniority_rank(seniority)
                    if posting_rank >= 0 and max_rank >= 0 and posting_rank >= max_rank:
                        exclusion_reason = f"Seniority '{seniority.value}' meets or exceeds max '{config.max_seniority}'"

            # --- Salary filter ---
            if config.min_salary_cad is not None and exclusion_reason is None:
                sal_min = _get_salary_min(posting)
                sal_max = _get_salary_max(posting)
                salary_known = sal_min is not None or sal_max is not None

                if not salary_known:
                    # Unknown salary → pass with badge
                    badges.add("Salary unknown")
                else:
                    # Use the higher of the two values for floor comparison
                    # (if a posting shows $90K-$110K, we check max ≥ floor)
                    best_salary = max(v for v in (sal_min, sal_max) if v is not None)
                    if best_salary < config.min_salary_cad:
                        exclusion_reason = (
                            f"Salary ${best_salary:,.0f} below floor ${config.min_salary_cad:,.0f}"
                        )

            # --- Company size filter ---
            if config.company_size_exclude is not None and exclusion_reason is None:
                size_label = _get_company_size_label(posting)

                if not size_label:
                    # Unknown/missing size → pass with badge
                    badges.add("Size unknown")
                else:
                    normalized_label = size_label.strip().lower()
                    exclude_set = {s.strip().lower() for s in config.company_size_exclude}
                    if normalized_label in exclude_set:
                        exclusion_reason = f"Company size '{size_label}' is in exclude list"

            # --- Outcome ---
            if exclusion_reason:
                logger.debug(
                    "Posting excluded by filter",
                    extra={"title": posting.title, "reason": exclusion_reason},
                )
                result.excluded.append((posting, exclusion_reason))
            else:
                # Attach badge_flags dynamically for UI layer consumption
                posting.badge_flags = badges  # type: ignore[attr-defined]
                result.kept.append(posting)

        logger.info(
            "FilterService.apply complete",
            extra={"kept": result.kept_count, "excluded": result.excluded_count},
        )
        return result
