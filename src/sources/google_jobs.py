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
            "num": min(results_wanted, 10),  # SerpAPI Google Jobs returns up to 10 per page
        }
        if query.hours_old:
            # Map hours_old to SerpAPI's chips parameter: past 24h / 3d / week
            if query.hours_old <= 24:
                params["chips"] = "date_posted:today"
            elif query.hours_old <= 72:
                params["chips"] = "date_posted:3days"
            elif query.hours_old <= 168:
                params["chips"] = "date_posted:week"

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
    description = "\n".join(description_parts) or job.get("description", "") or ""

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
