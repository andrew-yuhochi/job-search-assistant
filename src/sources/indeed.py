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
                country_indeed="Canada",
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

    def fetch_multi(
        self,
        term_location_pairs: list[tuple[str, str]],
        hours_old: int,
        results_wanted_per_pair: int,
    ) -> list[RawJobPosting]:
        """
        Fetch Indeed postings for multiple (search_term, location) pairs.

        Loops over all pairs, calls scrape_jobs() once per pair with
        country_indeed="Canada" (fix for the 0-result bug).  All results
        are merged and deduplicated by job_url (first occurrence wins).
        No sleep between calls — Indeed does not require it.

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

        for term, location in term_location_pairs:
            logger.info(
                "IndeedSource: fetching term=%r location=%r", term, location
            )
            try:
                df = scrape_jobs(
                    site_name=["indeed"],
                    search_term=term,
                    location=location,
                    results_wanted=min(results_wanted_per_pair, _MAX_RESULTS),
                    hours_old=hours_old,
                    country_indeed="Canada",
                )
            except Exception as exc:
                exc_str = str(exc).lower()
                if "429" in exc_str or "rate limit" in exc_str or "too many" in exc_str:
                    logger.warning(
                        "IndeedSource.fetch_multi: rate limit on term=%r location=%r",
                        term, location,
                    )
                    raise RateLimitError(self.name) from exc
                logger.warning(
                    "IndeedSource.fetch_multi: error on term=%r location=%r: %s",
                    term, location, exc,
                )
                continue

            if df is None or df.empty:
                logger.info(
                    "IndeedSource.fetch_multi: empty result for term=%r location=%r",
                    term, location,
                )
                continue

            for _, row in df.iterrows():
                try:
                    posting = _row_to_raw(row, SourceName.indeed)
                    if posting.url and posting.url in seen_urls:
                        continue
                    if posting.url:
                        seen_urls.add(posting.url)
                    all_postings.append(posting)
                except Exception as exc:
                    logger.warning(
                        "IndeedSource.fetch_multi: skipping malformed row id=%s — %s",
                        row.get("id"), exc,
                    )

        logger.info(
            "IndeedSource.fetch_multi: %d unique postings across %d pairs",
            len(all_postings), len(term_location_pairs),
        )
        return all_postings
