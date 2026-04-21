"""
Abstract base classes and shared data types for the job-source plugin layer.

This module defines the interface that every concrete JobSource must implement,
plus the RawJobPosting and FetchResult types exchanged between the ingestion
layer and the rest of the pipeline.

Per TDD §2.1: no `if source == "X"` branching is permitted outside this package.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal, Optional

from src.models import RawJobPosting

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class RateLimitError(Exception):
    """
    Raised when a job source signals it is being rate-limited.

    This is the only exception that callers in the services layer should
    catch and handle specially (surface a yellow warning banner).  All other
    source errors propagate as regular exceptions and are caught by
    fetch_all() to populate FetchResult.status = 'error'.
    """

    def __init__(self, source_name: str, message: str = "") -> None:
        self.source_name = source_name
        super().__init__(
            message or f"Rate limit hit for source '{source_name}'"
        )


# ---------------------------------------------------------------------------
# Search query
# ---------------------------------------------------------------------------


@dataclass
class SearchQuery:
    """
    Immutable search specification passed to every JobSource.fetch().

    All fields have sensible defaults so callers can construct a minimal
    query (just search_term) for smoke-tests and prototypes.
    """

    search_term: str
    location: str = "Vancouver, BC, Canada"
    results_wanted: int = 25  # per source, not total
    hours_old: int = 72       # only postings published within this window
    remote_only: bool = False


# ---------------------------------------------------------------------------
# Fetch result envelope
# ---------------------------------------------------------------------------


FetchStatus = Literal["ok", "rate_limited", "error"]


@dataclass
class FetchResult:
    """
    Return type of JobSourceRegistry.fetch_all() for a single source.

    The registry catches all exceptions from individual sources and
    materialises them as FetchResult(status='error') so that one failing
    source never crashes the whole scrape run.
    """

    source_name: str
    postings: list[RawJobPosting] = field(default_factory=list)
    status: FetchStatus = "ok"
    error: Optional[str] = None

    @property
    def count(self) -> int:
        return len(self.postings)


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------


class JobSource(ABC):
    """
    Plugin interface for job-posting sources.  TDD §2.1.

    Every concrete implementation must:
    1. Set `name` to a stable lower-case identifier (matches SourceName enum).
    2. Implement fetch() returning a list[RawJobPosting].
    3. Implement is_available() returning False if the source is temporarily
       unhealthy (missing API key, network unreachable).
    4. Raise RateLimitError — not a pandas / HTTP exception — when rate-limited.
    """

    name: str  # overridden as a class attribute in each subclass

    @abstractmethod
    def fetch(self, query: SearchQuery) -> list[RawJobPosting]:
        """
        Fetch postings matching query.

        Must raise RateLimitError when the source signals HTTP 429 or an
        equivalent quota-exceeded response.  All other errors may propagate
        as regular exceptions; they will be caught by the registry.
        """
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """
        Return True if the source is ready to serve requests.

        Called by the registry before dispatching fetch(); unavailable sources
        are skipped and recorded as FetchResult(status='error').
        """
        ...
