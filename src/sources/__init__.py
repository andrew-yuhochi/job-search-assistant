# Sources package — job-posting plugin registry.
# Public re-exports for convenience; importers can also use the submodules directly.

from src.sources.base import FetchResult, FetchStatus, JobSource, RateLimitError, SearchQuery
from src.sources.google_jobs import GoogleJobsSource
from src.sources.indeed import IndeedSource
from src.sources.linkedin import LinkedInSource
from src.sources.registry import JobSourceRegistry

__all__ = [
    "JobSource",
    "RateLimitError",
    "FetchResult",
    "FetchStatus",
    "SearchQuery",
    "LinkedInSource",
    "IndeedSource",
    "GoogleJobsSource",
    "JobSourceRegistry",
]
