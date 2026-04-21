"""
SeniorityInferrer: keyword-based seniority classification from job titles.

Maps job titles to SeniorityLevel enum values. Falls back to 'unknown'
when no recognized keywords are found. Used by FilterService to enforce
seniority hard filters per PRD §6. Called after Normalizer in the pipeline.
"""
from __future__ import annotations

import logging
import re

from src.models.models import SeniorityLevel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ordered keyword → level mapping.
# More specific patterns come first to avoid false positives
# (e.g., "Staff Engineer" must not match "senior" before "staff").
# ---------------------------------------------------------------------------
_TITLE_KEYWORDS: list[tuple[re.Pattern, SeniorityLevel]] = [
    # Exec-level
    (re.compile(r"\bcto\b|\bceo\b|\bcoo\b|\bcso\b|\bc-suite\b|\bchief\b", re.IGNORECASE), SeniorityLevel.csuite),
    # VP
    (re.compile(r"\bvp\b|\bvice[\s-]president\b", re.IGNORECASE), SeniorityLevel.vp),
    # Director
    (re.compile(r"\bdirector\b", re.IGNORECASE), SeniorityLevel.director),
    # Principal
    (re.compile(r"\bprincipal\b", re.IGNORECASE), SeniorityLevel.principal),
    # Staff
    (re.compile(r"\bstaff\b", re.IGNORECASE), SeniorityLevel.staff),
    # Manager / Lead (treated as senior for filter purposes — many orgs consider these senior+)
    (re.compile(r"\bmanager\b|\blead\b|\bhead\s+of\b|\btech\s+lead\b", re.IGNORECASE), SeniorityLevel.director),
    # Senior
    (re.compile(r"\bsenior\b|\bsr\.?\b", re.IGNORECASE), SeniorityLevel.senior),
    # Junior / Entry
    (re.compile(r"\bjunior\b|\bjr\.?\b|\bentry[\s-]level\b|\bassociate\b|\bintern\b", re.IGNORECASE), SeniorityLevel.junior),
    # Mid-level
    (re.compile(r"\bmid[\s-]level\b|\bintermediate\b|\bii\b|\b2\b", re.IGNORECASE), SeniorityLevel.mid),
]

# JobSpy job_level string → SeniorityLevel
_JOBSPY_LEVEL_MAP: dict[str, SeniorityLevel] = {
    "internship": SeniorityLevel.junior,
    "entry level": SeniorityLevel.junior,
    "associate": SeniorityLevel.junior,
    "mid-senior level": SeniorityLevel.senior,
    "senior": SeniorityLevel.senior,
    "senior level": SeniorityLevel.senior,
    "director": SeniorityLevel.director,
    "executive": SeniorityLevel.vp,
    "not applicable": SeniorityLevel.unknown,
}


class SeniorityInferrer:
    """
    Infers seniority level from a job title string.

    Algorithm:
    1. If a JobSpy `job_level` field is provided, map it via _JOBSPY_LEVEL_MAP.
    2. Otherwise, scan the title for keyword patterns in priority order.
    3. Return SeniorityLevel.unknown when no signal is found.

    Returns one of the SeniorityLevel enum values. Never raises.
    """

    def infer(self, title: str, job_level: str | None = None) -> SeniorityLevel:
        """
        Infer seniority from title (and optionally JobSpy's job_level field).

        Args:
            title:      Raw or normalized job title.
            job_level:  Optional structured level from JobSpy/LinkedIn
                        (e.g. "Mid-Senior level", "Director").

        Returns:
            SeniorityLevel enum value.
        """
        # 1. Try structured field first (more reliable than keyword regex)
        if job_level:
            mapped = _JOBSPY_LEVEL_MAP.get(job_level.lower().strip())
            if mapped is not None:
                logger.debug("Seniority from job_level field", extra={"job_level": job_level, "level": mapped})
                return mapped

        # 2. Keyword scan on title
        if not title:
            return SeniorityLevel.unknown

        for pattern, level in _TITLE_KEYWORDS:
            if pattern.search(title):
                logger.debug("Seniority from title keyword", extra={"title": title, "level": level})
                return level

        return SeniorityLevel.unknown
