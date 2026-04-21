"""
Indeed job source using python-jobspy.

Wraps jobspy.scrape_jobs(site_name=["indeed"], ...) and converts the
returned pandas DataFrame into a list[RawJobPosting].  Indeed is the most
reliable jobspy source at PoC — no meaningful rate limiting according to
the jobspy README.

Per TDD §2.1 and DATA-SOURCES.md Source 2.
"""
from __future__ import annotations

import logging

from src.models import RawJobPosting, SourceName
from src.sources.base import JobSource, RateLimitError, SearchQuery
from src.sources.linkedin import _row_to_raw, scrape_jobs  # shared jobspy helpers

logger = logging.getLogger(__name__)

_MAX_RESULTS = 100  # conservative ceiling; Indeed rarely hits rate limits


class IndeedSource(JobSource):
    """
    Concrete JobSource for Indeed via python-jobspy.

    No authentication required — uses Indeed's public scraping interface.
    """

    name = "indeed"

    def is_available(self) -> bool:
        """Indeed needs no API key — always consider it potentially available."""
        return True

    def fetch(self, query: SearchQuery) -> list[RawJobPosting]:
        """
        Scrape Indeed for query and return normalised RawJobPosting list.

        Raises RateLimitError if jobspy signals a 429 / rate-limit condition.
        """
        if scrape_jobs is None:
            raise ImportError(
                "python-jobspy is not installed. Add it to requirements.txt."
            )

        results_wanted = min(query.results_wanted, _MAX_RESULTS)
        logger.info(
            "IndeedSource.fetch: search_term=%r location=%r results_wanted=%d",
            query.search_term,
            query.location,
            results_wanted,
        )

        try:
            df = scrape_jobs(
                site_name=["indeed"],
                search_term=query.search_term,
                location=query.location,
                results_wanted=results_wanted,
                hours_old=query.hours_old,
            )
        except Exception as exc:
            exc_str = str(exc).lower()
            if "429" in exc_str or "rate limit" in exc_str or "too many" in exc_str:
                raise RateLimitError(self.name) from exc
            raise

        if df is None or df.empty:
            logger.info("IndeedSource: empty DataFrame returned by jobspy")
            return []

        postings: list[RawJobPosting] = []
        for _, row in df.iterrows():
            try:
                posting = _row_to_raw(row, SourceName.indeed)
                postings.append(posting)
            except Exception as exc:
                logger.warning(
                    "IndeedSource: skipping malformed row id=%s — %s",
                    row.get("id"),
                    exc,
                )

        logger.info("IndeedSource: converted %d postings", len(postings))
        return postings
