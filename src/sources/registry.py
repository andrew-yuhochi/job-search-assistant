"""
JobSourceRegistry — plugin registry for all concrete JobSource implementations.

Per TDD §2.1: every job source is registered once (at app startup) and
dispatched through this registry.  No `if source == "X"` branching exists
anywhere outside this module.

Usage:
    registry = JobSourceRegistry()
    registry.register(LinkedInSource())
    registry.register(IndeedSource())
    registry.register(GoogleJobsSource())

    results: dict[str, FetchResult] = registry.fetch_all(query)
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from src.sources.base import FetchResult, JobSource, RateLimitError, SearchQuery

logger = logging.getLogger(__name__)


class JobSourceRegistry:
    """
    Central registry for JobSource plugins.

    fetch_all() dispatches queries to every registered source in parallel
    via ThreadPoolExecutor and returns a dict[source_name, FetchResult].
    Individual source failures (including RateLimitError) are caught and
    materialised as FetchResult(status='rate_limited'|'error') — they never
    crash the whole scrape run.
    """

    def __init__(self, max_workers: int = 4) -> None:
        self._sources: dict[str, JobSource] = {}
        self._max_workers = max_workers

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, source: JobSource) -> None:
        """
        Register a JobSource implementation.

        The source's `name` attribute is used as the registry key.
        Registering a source with a duplicate name overwrites the previous.
        """
        if source.name in self._sources:
            logger.warning(
                "JobSourceRegistry: overwriting existing source '%s'", source.name
            )
        self._sources[source.name] = source
        logger.debug("JobSourceRegistry: registered source '%s'", source.name)

    @property
    def source_names(self) -> list[str]:
        """Return names of all registered sources."""
        return list(self._sources.keys())

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def fetch_all(
        self,
        query: SearchQuery,
        source_names: Optional[list[str]] = None,
    ) -> dict[str, FetchResult]:
        """
        Fetch postings from all (or a subset of) registered sources in parallel.

        Args:
            query: The search query to dispatch to every source.
            source_names: Optional list of source names to restrict the fetch.
                          Defaults to all registered sources.

        Returns:
            dict[source_name, FetchResult] — one entry per dispatched source.
            Sources that are unavailable (is_available() == False) are
            recorded as FetchResult(status='error') without calling fetch().
        """
        targets = self._resolve_targets(source_names)
        if not targets:
            logger.warning("JobSourceRegistry.fetch_all: no sources to dispatch")
            return {}

        results: dict[str, FetchResult] = {}

        with ThreadPoolExecutor(max_workers=min(self._max_workers, len(targets))) as pool:
            futures = {
                pool.submit(self._fetch_one, source, query): source.name
                for source in targets
            }
            for future in as_completed(futures):
                source_name = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    # Should not reach here — _fetch_one catches all exceptions
                    logger.error(
                        "JobSourceRegistry: unexpected error from future for '%s': %s",
                        source_name,
                        exc,
                    )
                    result = FetchResult(
                        source_name=source_name, status="error", error=str(exc)
                    )
                results[source_name] = result

        self._log_summary(results)
        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_targets(
        self, source_names: Optional[list[str]]
    ) -> list[JobSource]:
        """Return the subset of registered sources to dispatch."""
        if source_names is None:
            return list(self._sources.values())
        targets = []
        for name in source_names:
            if name in self._sources:
                targets.append(self._sources[name])
            else:
                logger.warning(
                    "JobSourceRegistry: unknown source '%s' in source_names filter",
                    name,
                )
        return targets

    def _fetch_one(self, source: JobSource, query: SearchQuery) -> FetchResult:
        """
        Dispatch fetch() to a single source.

        Catches RateLimitError as 'rate_limited' and all other exceptions as
        'error'.  Never raises — always returns a FetchResult.
        """
        if not source.is_available():
            logger.warning(
                "JobSourceRegistry: source '%s' is not available — skipping",
                source.name,
            )
            return FetchResult(
                source_name=source.name,
                status="error",
                error=f"Source '{source.name}' is not available (missing credentials or health check failed)",
            )

        try:
            postings = source.fetch(query)
            return FetchResult(
                source_name=source.name,
                postings=postings,
                status="ok",
            )
        except RateLimitError as exc:
            logger.warning(
                "JobSourceRegistry: rate limit hit for '%s': %s",
                source.name,
                exc,
            )
            return FetchResult(
                source_name=source.name,
                status="rate_limited",
                error=str(exc),
            )
        except Exception as exc:
            logger.error(
                "JobSourceRegistry: error fetching from '%s': %s",
                source.name,
                exc,
                exc_info=True,
            )
            return FetchResult(
                source_name=source.name,
                status="error",
                error=str(exc),
            )

    @staticmethod
    def _log_summary(results: dict[str, FetchResult]) -> None:
        for name, result in results.items():
            if result.status == "ok":
                logger.info(
                    "JobSourceRegistry: %s → %d postings [ok]",
                    name,
                    result.count,
                )
            else:
                logger.warning(
                    "JobSourceRegistry: %s → %s: %s",
                    name,
                    result.status,
                    result.error or "(no details)",
                )
