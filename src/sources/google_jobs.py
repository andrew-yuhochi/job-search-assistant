"""
Google Jobs source using the SerpAPI google-search-results library.

Wraps the GoogleSearch client from serpapi and converts results into
list[RawJobPosting].  Requires SERPAPI_API_KEY in the environment.

Free-tier limits: 250 searches/month, 50/hour.  Quota exhaustion surfaces
as RateLimitError.

Per TDD §2.1 and DATA-SOURCES.md Source 3.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Optional

from src.models import RawJobPosting, SourceName
from src.sources.base import JobSource, RateLimitError, SearchQuery

try:
    from serpapi import GoogleSearch  # type: ignore[import]
except ImportError:
    GoogleSearch = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

_API_KEY_ENV = "SERPAPI_API_KEY"
_MAX_RESULTS = 50  # SerpAPI free tier: cap at 50 per query to conserve quota


class GoogleJobsSource(JobSource):
    """
    Concrete JobSource for Google Jobs via SerpAPI.

    Requires SERPAPI_API_KEY to be set in the environment.
    is_available() returns False when the key is missing so the registry
    can skip this source gracefully rather than raising.
    """

    name = "google"

    def __init__(self, api_key: Optional[str] = None) -> None:
        self._api_key = api_key or os.environ.get(_API_KEY_ENV)

    def is_available(self) -> bool:
        """Return True only if the API key is present."""
        return bool(self._api_key)

    def fetch(self, query: SearchQuery) -> list[RawJobPosting]:
        """
        Fetch Google Jobs postings via SerpAPI and return RawJobPosting list.

        Raises RateLimitError when SerpAPI signals quota exhaustion.
        Raises RuntimeError when SERPAPI_API_KEY is not set.
        """
        if not self._api_key:
            raise RuntimeError(
                f"{_API_KEY_ENV} is not set. Google Jobs source is unavailable."
            )

        if GoogleSearch is None:
            raise ImportError(
                "google-search-results is not installed. Add it to requirements.txt."
            )

        results_wanted = min(query.results_wanted, _MAX_RESULTS)
        logger.info(
            "GoogleJobsSource.fetch: search_term=%r location=%r results_wanted=%d",
            query.search_term,
            query.location,
            results_wanted,
        )

        # SerpAPI Google Jobs engine parameters
        params: dict[str, Any] = {
            "engine": "google_jobs",
            "q": query.search_term,
            "location": query.location,
            "api_key": self._api_key,
            "gl": "ca",
            "hl": "en",
            "num": min(results_wanted, 10),  # SerpAPI Google Jobs returns up to 10 per page
        }

        try:
            search = GoogleSearch(params)
            results = search.get_dict()
        except Exception as exc:
            exc_str = str(exc).lower()
            if (
                "rate limit" in exc_str
                or "quota" in exc_str
                or "429" in exc_str
                or "plan" in exc_str  # "Your plan does not allow..."
            ):
                raise RateLimitError(self.name) from exc
            raise

        # SerpAPI returns error info in results dict on failure
        if "error" in results:
            error_msg = results["error"]
            if (
                "rate" in error_msg.lower()
                or "quota" in error_msg.lower()
                or "plan" in error_msg.lower()
                or "limit" in error_msg.lower()
            ):
                raise RateLimitError(self.name, error_msg)
            logger.warning("GoogleJobsSource: API error — %s", error_msg)
            return []

        jobs_results: list[dict] = results.get("jobs_results", [])
        logger.info(
            "GoogleJobsSource: received %d raw results from SerpAPI",
            len(jobs_results),
        )

        postings: list[RawJobPosting] = []
        for job in jobs_results[:results_wanted]:
            try:
                posting = _serpapi_job_to_raw(job)
                postings.append(posting)
            except Exception as exc:
                logger.warning(
                    "GoogleJobsSource: skipping malformed result title=%r — %s",
                    job.get("title"),
                    exc,
                )

        logger.info("GoogleJobsSource: converted %d postings", len(postings))
        return postings

    def fetch_multi(
        self,
        term_location_pairs: list[tuple[str, str]],
        hours_old: int,
        results_wanted_per_pair: int,
    ) -> list[RawJobPosting]:
        """
        Fetch Google Jobs postings for multiple (search_term, location) pairs.

        Combines all unique terms into a single OR query per location to minimise
        SerpAPI quota usage.  Runs one query per unique location (typically 2).
        Deduplicates by job_url if present, else by job_id (first occurrence wins).

        Args:
            term_location_pairs:      List of (search_term, location) tuples.
            hours_old:                Unused for Google Jobs (chips param removed).
            results_wanted_per_pair:  Max results requested per SerpAPI call.

        Returns:
            Deduplicated list of RawJobPosting across all location queries.
        """
        if not self._api_key:
            raise RuntimeError(
                f"{_API_KEY_ENV} is not set. Google Jobs source is unavailable."
            )
        if GoogleSearch is None:
            raise ImportError(
                "google-search-results is not installed. Add it to requirements.txt."
            )

        # Build unique terms and locations from the pairs
        unique_terms: list[str] = []
        seen_terms: set[str] = set()
        unique_locations: list[str] = []
        seen_locs: set[str] = set()
        for term, location in term_location_pairs:
            if term not in seen_terms:
                unique_terms.append(term)
                seen_terms.add(term)
            if location not in seen_locs:
                unique_locations.append(location)
                seen_locs.add(location)

        # Combine all terms into a single OR query
        combined_query = " OR ".join(f'"{t}"' for t in unique_terms)
        logger.info(
            "GoogleJobsSource.fetch_multi: combined query=%r, locations=%r",
            combined_query, unique_locations,
        )

        all_postings: list[RawJobPosting] = []
        seen_ids: set[str] = set()  # dedup by url or job_id

        for location in unique_locations:
            logger.info(
                "GoogleJobsSource: fetching combined query for location=%r", location
            )
            params: dict[str, Any] = {
                "engine": "google_jobs",
                "q": combined_query,
                "location": location,
                "api_key": self._api_key,
                "gl": "ca",
                "hl": "en",
                "num": min(results_wanted_per_pair, 10),
            }

            try:
                search = GoogleSearch(params)
                results = search.get_dict()
            except Exception as exc:
                exc_str = str(exc).lower()
                if (
                    "rate limit" in exc_str
                    or "quota" in exc_str
                    or "429" in exc_str
                    or "plan" in exc_str
                ):
                    raise RateLimitError(self.name) from exc
                logger.warning(
                    "GoogleJobsSource.fetch_multi: error for location=%r: %s",
                    location, exc,
                )
                continue

            if "error" in results:
                error_msg = results["error"]
                if any(k in error_msg.lower() for k in ("rate", "quota", "plan", "limit")):
                    raise RateLimitError(self.name, error_msg)
                logger.warning(
                    "GoogleJobsSource.fetch_multi: API error for location=%r — %s",
                    location, error_msg,
                )
                continue

            jobs_results: list[dict] = results.get("jobs_results", [])
            logger.info(
                "GoogleJobsSource.fetch_multi: %d raw results for location=%r",
                len(jobs_results), location,
            )

            for job in jobs_results[:results_wanted_per_pair]:
                try:
                    posting = _serpapi_job_to_raw(job)
                    # Dedup: prefer URL, fall back to job_id
                    dedup_key = posting.url if posting.url else posting.id
                    if dedup_key and dedup_key in seen_ids:
                        continue
                    if dedup_key:
                        seen_ids.add(dedup_key)
                    all_postings.append(posting)
                except Exception as exc:
                    logger.warning(
                        "GoogleJobsSource.fetch_multi: skipping malformed result title=%r — %s",
                        job.get("title"), exc,
                    )

        logger.info(
            "GoogleJobsSource.fetch_multi: %d unique postings across %d locations",
            len(all_postings), len(unique_locations),
        )
        return all_postings


# ---------------------------------------------------------------------------
# Description formatting helpers
# ---------------------------------------------------------------------------

# Sentence-boundary words that often start bullet-like items in job descriptions.
# We insert a paragraph break before them when they follow ". " at the start of
# what looks like a new sentence.
_BULLET_STARTERS = re.compile(
    r"\.\s+(?="
    r"(?:You |We |Our |The |This |Must |Will |Should |Can |Are |Have |"
    r"Responsibilities|Requirements|Qualifications|Benefits|About|"
    r"Experience|Skills|Education|What|How|Why|Work|Join|Build|Lead|"
    r"Design|Develop|Manage|Analyze|Create|Ensure|Support|Collaborate|"
    r"Drive|Own|Define|Partner|Report|Provide|Help|Make|Use|Apply|"
    r"[A-Z][a-z])"
    r")",
    re.MULTILINE,
)

# Collapse excessive whitespace and carriage returns.
_EXCESS_WHITESPACE = re.compile(r"\r|\t|[ ]{2,}")


def _format_description(text: str) -> str:
    """
    Lightweight formatting pass for SerpAPI Google Jobs descriptions.

    These come back as dense unformatted paragraphs.  We add paragraph breaks
    at sentence boundaries before common bullet-starter words, and strip noise
    whitespace.  No LLM or NLP — pure regex.
    """
    if not text:
        return text
    # 1. Normalise carriage returns and excessive spaces.
    text = _EXCESS_WHITESPACE.sub(" ", text)
    # 2. Insert paragraph breaks at sentence boundaries before bullet-like words.
    text = _BULLET_STARTERS.sub(".\n\n", text)
    # 3. Strip leading/trailing whitespace from each line.
    lines = [line.strip() for line in text.splitlines()]
    text = "\n".join(lines)
    # 4. Collapse more than two consecutive newlines.
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ---------------------------------------------------------------------------
# SerpAPI result → RawJobPosting helper
# ---------------------------------------------------------------------------


def _serpapi_job_to_raw(job: dict) -> RawJobPosting:
    """
    Convert a single SerpAPI Google Jobs result dict into a RawJobPosting.

    SerpAPI Google Jobs fields reference:
    https://serpapi.com/google-jobs-api
    """
    title = job.get("title", "").strip() or "Untitled"
    company = job.get("company_name", "").strip() or "Unknown Company"
    location = job.get("location", "").strip()

    # Build a stable unique ID from company + title + detected_extensions
    detected = job.get("detected_extensions", {})
    posted_at_raw: str = detected.get("posted_at", "") or ""

    # Use the job_id field if present (SerpAPI sometimes includes it)
    job_id: str = job.get("job_id", "")
    if not job_id:
        import hashlib
        job_id = hashlib.md5(f"{company}|{title}|{location}".encode()).hexdigest()[:16]

    # Extract URL — SerpAPI puts the apply link in related_links
    related_links: list[dict] = job.get("related_links", [])
    url = ""
    for link in related_links:
        href = link.get("link", "")
        if href.startswith("http"):
            url = href
            break
    # Fallback: construct a Google Jobs search URL
    if not url:
        from urllib.parse import quote
        url = f"https://www.google.com/search?q={quote(title + ' ' + company)}&ibp=htl;jobs"

    # Description: jobspy highlights > description > empty
    highlights: list[dict] = job.get("job_highlights", [])
    description_parts: list[str] = []
    for highlight in highlights:
        items: list[str] = highlight.get("items", [])
        if items:
            description_parts.extend(items)
    raw_description = "\n".join(description_parts) or job.get("description", "") or ""
    description = _format_description(raw_description)

    # Salary
    salary_raw: Optional[str] = detected.get("salary")

    return RawJobPosting(
        id=job_id,
        title=title,
        company=company,
        location=location,
        source=SourceName.google,
        url=url,
        description=description,
        salary_raw=salary_raw,
        posted_date=posted_at_raw,
    )
