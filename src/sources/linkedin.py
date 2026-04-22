"""
LinkedIn job source using python-jobspy.

Wraps jobspy.scrape_jobs(site_name=["linkedin"], ...) and converts the
returned pandas DataFrame into a list[RawJobPosting].  Caps results_wanted
at 250 (page-10 ceiling per DATA-SOURCES.md) and raises RateLimitError on
HTTP 429 or any JobSpy exception that indicates rate-limiting.

Per TDD §2.1 and DATA-SOURCES.md Source 1.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from src.models import RawJobPosting, SourceName
from src.sources.base import FetchResult, JobSource, RateLimitError, SearchQuery

try:
    from jobspy import scrape_jobs  # type: ignore[import]
except ImportError:
    scrape_jobs = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_MAX_RESULTS = 250  # hard ceiling per LinkedIn guest-API page limit


class LinkedInSource(JobSource):
    """
    Concrete JobSource for LinkedIn via python-jobspy.

    No authentication required — uses LinkedIn's public guest API.
    Raises RateLimitError when jobspy surfaces an HTTP 429 or equivalent.
    """

    name = "linkedin"

    def is_available(self) -> bool:
        """LinkedIn needs no API key — always consider it potentially available."""
        return True

    def fetch(self, query: SearchQuery) -> list[RawJobPosting]:
        """
        Scrape LinkedIn for query and return normalised RawJobPosting list.

        Caps results_wanted at _MAX_RESULTS.  Catches jobspy / requests
        exceptions and re-raises as RateLimitError when the signal is 429.
        """
        if scrape_jobs is None:
            raise ImportError(
                "python-jobspy is not installed. Add it to requirements.txt."
            )

        results_wanted = min(query.results_wanted, _MAX_RESULTS)
        logger.info(
            "LinkedInSource.fetch: search_term=%r location=%r results_wanted=%d",
            query.search_term,
            query.location,
            results_wanted,
        )

        try:
            df = scrape_jobs(
                site_name=["linkedin"],
                search_term=query.search_term,
                location=query.location,
                results_wanted=results_wanted,
                hours_old=query.hours_old,
                linkedin_fetch_description=True,  # fetches full description by visiting each posting URL
            )
        except Exception as exc:
            exc_str = str(exc).lower()
            if "429" in exc_str or "rate limit" in exc_str or "too many" in exc_str:
                raise RateLimitError(self.name) from exc
            raise

        return self._df_to_postings(df)

    def fetch_multi(
        self,
        term_location_pairs: list[tuple[str, str]],
        hours_old: int,
        results_wanted_per_pair: int,
    ) -> list[RawJobPosting]:
        """
        Fetch LinkedIn postings for multiple (search_term, location) pairs.

        Loops over all pairs, calls scrape_jobs() once per pair with a 2-second
        delay between calls to respect LinkedIn guest-API rate limits.  All
        results are merged and deduplicated by job_url (first occurrence wins).

        Args:
            term_location_pairs:      List of (search_term, location) tuples.
            hours_old:                Hours window for freshness filter.
            results_wanted_per_pair:  Max results requested per scrape_jobs() call.

        Returns:
            Deduplicated list of RawJobPosting across all pairs.
        """
        if scrape_jobs is None:
            raise ImportError(
                "python-jobspy is not installed. Add it to requirements.txt."
            )

        all_postings: list[RawJobPosting] = []
        seen_urls: set[str] = set()

        for idx, (term, location) in enumerate(term_location_pairs):
            logger.info(
                "LinkedInSource: fetching term=%r location=%r", term, location
            )
            if idx > 0:
                time.sleep(2)

            try:
                df = scrape_jobs(
                    site_name=["linkedin"],
                    search_term=term,
                    location=location,
                    results_wanted=min(results_wanted_per_pair, _MAX_RESULTS),
                    hours_old=hours_old,
                    linkedin_fetch_description=True,
                )
            except Exception as exc:
                exc_str = str(exc).lower()
                if "429" in exc_str or "rate limit" in exc_str or "too many" in exc_str:
                    logger.warning(
                        "LinkedInSource.fetch_multi: rate limit on term=%r location=%r",
                        term, location,
                    )
                    raise RateLimitError(self.name) from exc
                logger.warning(
                    "LinkedInSource.fetch_multi: error on term=%r location=%r: %s",
                    term, location, exc,
                )
                continue

            batch = self._df_to_postings(df)
            for posting in batch:
                if posting.url and posting.url in seen_urls:
                    continue
                if posting.url:
                    seen_urls.add(posting.url)
                all_postings.append(posting)

        logger.info(
            "LinkedInSource.fetch_multi: %d unique postings across %d pairs",
            len(all_postings), len(term_location_pairs),
        )
        return all_postings

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _df_to_postings(df) -> list[RawJobPosting]:
        """Convert a jobspy DataFrame to a list of RawJobPosting models."""
        if df is None or df.empty:
            logger.info("LinkedInSource: empty DataFrame returned by jobspy")
            return []

        postings: list[RawJobPosting] = []
        for _, row in df.iterrows():
            try:
                posting = _row_to_raw(row, SourceName.linkedin)
                postings.append(posting)
            except Exception as exc:
                logger.warning(
                    "LinkedInSource: skipping malformed row id=%s — %s",
                    row.get("id"),
                    exc,
                )
        logger.info("LinkedInSource: converted %d postings", len(postings))
        return postings


# ---------------------------------------------------------------------------
# Shared DataFrame → RawJobPosting helper (used by both LinkedIn and Indeed)
# ---------------------------------------------------------------------------


def _row_to_raw(row, source: SourceName) -> RawJobPosting:
    """
    Convert a single jobspy DataFrame row into a RawJobPosting.

    jobspy uses a unified schema for LinkedIn and Indeed so the same
    helper works for both.  Missing optional columns default to None.
    """
    def _str(val) -> Optional[str]:
        if val is None or (hasattr(val, "__class__") and val.__class__.__name__ == "float"):
            import math
            if val is None:
                return None
            try:
                if math.isnan(float(val)):
                    return None
            except (ValueError, TypeError):
                pass
        s = str(val).strip() if val is not None else None
        return s if s else None

    import math

    def _safe_str(val) -> Optional[str]:
        if val is None:
            return None
        try:
            if isinstance(val, float) and math.isnan(val):
                return None
        except (ValueError, TypeError):
            pass
        s = str(val).strip()
        return s if s else None

    job_id = _safe_str(row.get("id")) or ""
    url = _safe_str(row.get("job_url")) or _safe_str(row.get("job_url_direct")) or ""
    title = _safe_str(row.get("title")) or "Untitled"
    company = _safe_str(row.get("company")) or "Unknown Company"
    location = _safe_str(row.get("location")) or ""
    # Some LinkedIn guest-API results have no description; use placeholder so
    # we retain the posting for dedup and filtering rather than dropping it.
    description = _safe_str(row.get("description")) or "(description not available)"

    # Structured salary fields from jobspy — pass through for SalaryExtractor.
    def _safe_float(val) -> Optional[float]:
        if val is None:
            return None
        try:
            f = float(val)
            return None if math.isnan(f) else f
        except (ValueError, TypeError):
            return None

    min_amount = _safe_float(row.get("min_amount"))
    max_amount = _safe_float(row.get("max_amount"))
    currency_str = _safe_str(row.get("currency"))
    interval_str = _safe_str(row.get("interval"))

    # Also build a salary_raw string as before (kept for legacy regex path).
    if min_amount is not None:
        currency_label = currency_str or ""
        salary_raw = f"{min_amount}–{max_amount} {currency_label}".strip("–").strip()
    else:
        salary_raw = None

    posted_date = _safe_str(row.get("date_posted")) or ""

    return RawJobPosting(
        id=job_id,
        title=title,
        company=company,
        location=location,
        source=source,
        url=url,
        description=description,
        salary_raw=salary_raw,
        posted_date=posted_date,
        salary_min_raw=min_amount,
        salary_max_raw=max_amount,
        salary_currency=currency_str,
        salary_interval=interval_str,
    )
