# DedupService: three-stage duplicate detection for job postings.
# Called by scrape_runner.py (TASK-011) and seed_from_fixtures.py (TASK-009),
# after Normalizer and before Classifier. Writes to jobs.duplicate_of and
# the duplicates table via repository.insert_duplicate().
# Per TDD §2.3 and TASK-007 requirements.

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from rapidfuzz import fuzz

from src.models.models import JobPosting

logger = logging.getLogger(__name__)


@dataclass
class DedupResult:
    """
    Result of DedupService.check() for a single new posting.
    If is_duplicate is True, canonical_job_id identifies the first-seen post.
    match_type is one of 'url_exact', 'fuzzy_title_company', 'description_similarity'.
    match_score is None for url_exact; the ratio / Jaccard score for the others.
    """
    is_duplicate: bool
    canonical_job_id: str | None
    match_type: str | None
    match_score: float | None


# Pre-compiled regex to tokenize description text for Jaccard similarity.
# Splits on whitespace and any non-word characters.
_TOKEN_SPLIT = re.compile(r"[^\w]+")


def _tokenize(text: str) -> set[str]:
    """Return a set of lowercase tokens from text, splitting on whitespace and punctuation."""
    raw = _TOKEN_SPLIT.split(text.lower())
    return {t for t in raw if t}


def _jaccard(a: set[str], b: set[str]) -> float:
    """Compute Jaccard similarity |A ∩ B| / |A ∪ B|. Returns 0.0 if both sets are empty."""
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


class DedupService:
    """
    Three-stage duplicate detection for job postings. TDD §2.3.

    Stage 1 — exact URL match: if a posting with the same URL already exists in
    existing_jobs, it is a duplicate. match_type='url_exact', match_score=None.

    Stage 2 — fuzzy title+company: rapidfuzz token_sort_ratio on the concatenation
    of title_normalized and company_normalized. Score ≥ 90 → duplicate.
    match_type='fuzzy_title_company', match_score=<ratio/100>.

    Stage 3 — description similarity: if Stage 2 score is 70–89 (inclusive), also
    require Jaccard similarity on description token sets ≥ 0.5. Both signals must
    fire to flag. match_type='description_similarity', match_score=<jaccard>.

    The caller is responsible for supplying only postings within the last 30 days
    as existing_jobs (the filtering window per TDD §2.3).
    """

    # Tunable thresholds (configuration over hardcoding, per CLAUDE.md)
    FUZZY_EXACT_THRESHOLD: float = 90.0   # Stage 2: score ≥ this → duplicate
    FUZZY_AMBIGUOUS_LOW: float = 70.0     # Stage 3: lower bound of ambiguous band
    JACCARD_THRESHOLD: float = 0.5        # Stage 3: Jaccard ≥ this (combined with stage 2)

    def check(
        self,
        new_job: JobPosting,
        existing_jobs: list[JobPosting],
    ) -> DedupResult:
        """
        Check whether new_job is a duplicate of any posting in existing_jobs.

        Returns a DedupResult describing whether a duplicate was found and which
        posting is the canonical version.  The caller is responsible for persisting
        the result (insert_job / insert_duplicate via repository).

        Args:
            new_job: The newly normalised posting to check.
            existing_jobs: Candidate postings (caller filters to the 30-day window).

        Returns:
            DedupResult — is_duplicate=False if no match found.
        """
        # ------------------------------------------------------------------
        # Stage 1: Exact URL match
        # ------------------------------------------------------------------
        for existing in existing_jobs:
            if existing.url == new_job.url:
                logger.debug(
                    "Stage-1 URL exact match: new=%s canonical=%s",
                    new_job.job_id,
                    existing.job_id,
                )
                return DedupResult(
                    is_duplicate=True,
                    canonical_job_id=existing.job_id,
                    match_type="url_exact",
                    match_score=None,
                )

        # ------------------------------------------------------------------
        # Stage 2 + 3: Fuzzy title+company and optional Jaccard description
        # ------------------------------------------------------------------
        new_text = f"{new_job.title_normalized} {new_job.company_normalized}"
        new_tokens = _tokenize(new_job.description)

        best_score: float = 0.0
        best_match: JobPosting | None = None

        for existing in existing_jobs:
            existing_text = (
                f"{existing.title_normalized} {existing.company_normalized}"
            )
            score = fuzz.token_sort_ratio(new_text, existing_text)
            if score > best_score:
                best_score = score
                best_match = existing

        if best_match is None:
            return DedupResult(
                is_duplicate=False,
                canonical_job_id=None,
                match_type=None,
                match_score=None,
            )

        if best_score >= self.FUZZY_EXACT_THRESHOLD:
            logger.debug(
                "Stage-2 fuzzy match (score=%.1f): new=%s canonical=%s",
                best_score,
                new_job.job_id,
                best_match.job_id,
            )
            return DedupResult(
                is_duplicate=True,
                canonical_job_id=best_match.job_id,
                match_type="fuzzy_title_company",
                match_score=round(best_score / 100.0, 4),
            )

        if best_score >= self.FUZZY_AMBIGUOUS_LOW:
            # Ambiguous band — require corroborating Jaccard evidence
            existing_tokens = _tokenize(best_match.description)
            jaccard = _jaccard(new_tokens, existing_tokens)
            if jaccard >= self.JACCARD_THRESHOLD:
                logger.debug(
                    "Stage-3 description similarity (fuzzy=%.1f, jaccard=%.4f): "
                    "new=%s canonical=%s",
                    best_score,
                    jaccard,
                    new_job.job_id,
                    best_match.job_id,
                )
                return DedupResult(
                    is_duplicate=True,
                    canonical_job_id=best_match.job_id,
                    match_type="description_similarity",
                    match_score=round(jaccard, 4),
                )
            else:
                logger.debug(
                    "Stage-3 rejected (fuzzy=%.1f, jaccard=%.4f < %.1f): "
                    "new=%s is NOT a duplicate",
                    best_score,
                    jaccard,
                    self.JACCARD_THRESHOLD,
                    new_job.job_id,
                )

        return DedupResult(
            is_duplicate=False,
            canonical_job_id=None,
            match_type=None,
            match_score=None,
        )
