# Normalizer: cleans RawJobPosting into canonical JobPosting fields.
# Called by scrape_runner.py (TASK-011) and seed_from_fixtures.py (TASK-009).
import hashlib
import logging
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests

from src.models.models import JobPosting, RawJobPosting, SeniorityLevel

logger = logging.getLogger(__name__)

_RELATIVE_DATE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"(\d+)\s+second", re.IGNORECASE), "seconds"),
    (re.compile(r"(\d+)\s+minute", re.IGNORECASE), "minutes"),
    (re.compile(r"(\d+)\s+hour", re.IGNORECASE), "hours"),
    (re.compile(r"(\d+)\s+day", re.IGNORECASE), "days"),
    (re.compile(r"(\d+)\s+week", re.IGNORECASE), "weeks"),
    (re.compile(r"(\d+)\s+month", re.IGNORECASE), "months"),
]

_TODAY_PATTERN = re.compile(r"\btoday\b|\bjust posted\b|\btoday's date\b", re.IGNORECASE)
_YESTERDAY_PATTERN = re.compile(r"\byesterday\b", re.IGNORECASE)

_JOB_ID_PATTERN = re.compile(r"[/=]([a-zA-Z0-9_-]{4,})\??$")

_NORMALIZE_STRIP = re.compile(r"[^\w\s]")
_WHITESPACE_COLLAPSE = re.compile(r"\s+")

_SENIORITY_KEYWORDS: list[tuple[re.Pattern, SeniorityLevel]] = [
    (re.compile(r"\bjunior\b|\bjr\.?\b|\bentry[\s-]level\b", re.IGNORECASE), SeniorityLevel.junior),
    (re.compile(r"\bstaff\b", re.IGNORECASE), SeniorityLevel.staff),
    (re.compile(r"\bprincipal\b", re.IGNORECASE), SeniorityLevel.principal),
    (re.compile(r"\bdirector\b", re.IGNORECASE), SeniorityLevel.director),
    (re.compile(r"\bvp\b|\bvice\s+president\b", re.IGNORECASE), SeniorityLevel.vp),
    (re.compile(r"\bcto\b|\bceo\b|\bcoo\b|\bcso\b|\bc-suite\b", re.IGNORECASE), SeniorityLevel.csuite),
    (re.compile(r"\bsenior\b|\bsr\.?\b", re.IGNORECASE), SeniorityLevel.senior),
    (re.compile(r"\bmid[\s-]level\b|\bintermediate\b", re.IGNORECASE), SeniorityLevel.mid),
]


def _normalize_text(text: str) -> str:
    stripped = _NORMALIZE_STRIP.sub(" ", text.lower())
    return _WHITESPACE_COLLAPSE.sub(" ", stripped).strip()


def _parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None

    now = datetime.now(timezone.utc)

    if _TODAY_PATTERN.search(date_str):
        return now.replace(hour=0, minute=0, second=0, microsecond=0)

    if _YESTERDAY_PATTERN.search(date_str):
        return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    for pattern, unit in _RELATIVE_DATE_PATTERNS:
        match = pattern.search(date_str)
        if match:
            n = int(match.group(1))
            if unit == "seconds":
                return now - timedelta(seconds=n)
            elif unit == "minutes":
                return now - timedelta(minutes=n)
            elif unit == "hours":
                return now - timedelta(hours=n)
            elif unit == "days":
                return now - timedelta(days=n)
            elif unit == "weeks":
                return now - timedelta(weeks=n)
            elif unit == "months":
                return now - timedelta(days=n * 30)

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    logger.warning("Could not parse date string", extra={"date_str": date_str})
    return None


def _extract_job_id(url: str) -> str | None:
    try:
        parsed = urlparse(url)
    except Exception:
        return None

    # Query param 'jk' is the Indeed job ID convention
    from urllib.parse import parse_qs
    qs = parse_qs(parsed.query)
    for key in ("jk", "jobId", "job_id", "id"):
        if key in qs and qs[key]:
            return qs[key][0]

    # Fall back to last alphanumeric path segment
    match = _JOB_ID_PATTERN.search(parsed.path + ("?" + parsed.query if not parsed.query else ""))
    if match:
        return match.group(1)

    # Last path segment
    path_parts = [p for p in parsed.path.rstrip("/").split("/") if p]
    if path_parts:
        last = path_parts[-1]
        if re.match(r"^[a-zA-Z0-9_-]{3,}$", last):
            return last

    return None


def _infer_seniority(title: str) -> SeniorityLevel:
    for pattern, level in _SENIORITY_KEYWORDS:
        if pattern.search(title):
            return level
    return SeniorityLevel.unknown


_SALARY_TOKEN = re.compile(r"(\d[\d,]*(?:\.\d+)?)\s*([Kk]?)")


def _parse_salary_raw(salary_raw: str | None) -> tuple[int | None, int | None]:
    """Extract (min_cad, max_cad) integers from a raw salary string.

    Returns (None, None) when the string is absent or no numbers are found.
    Examples:
        "$130,000 – $155,000 CAD" → (130000, 155000)
        "130K to 155K CAD"        → (130000, 155000)
        None                       → (None, None)
    """
    if not salary_raw:
        return None, None

    cleaned = salary_raw.replace(",", "")
    numbers: list[float] = []
    for m in _SALARY_TOKEN.finditer(cleaned):
        val = float(m.group(1))
        if m.group(2).upper() == "K":
            val *= 1_000
        if val >= 1_000:  # ignore noise like "2 years" → 2
            numbers.append(val)

    if not numbers:
        return None, None

    lo = round(min(numbers))
    hi = round(max(numbers))
    return lo, hi


def _make_job_id(source: str, url: str) -> str:
    return hashlib.sha256(f"{source}:{url}".encode()).hexdigest()


class Normalizer:
    """
    Transforms a RawJobPosting into a canonical JobPosting.
    Pure logic — no database access, no side effects beyond HTTP HEAD for URL checks.
    """

    def normalize(self, raw: RawJobPosting) -> JobPosting:
        hostname = ""
        try:
            hostname = urlparse(raw.url).netloc
        except Exception:
            logger.warning("Failed to parse URL hostname", extra={"url": raw.url})

        title_normalized = _normalize_text(raw.title)
        company_normalized = _normalize_text(raw.company)
        source_job_id = _extract_job_id(raw.url)
        posted_at = _parse_date(raw.posted_date)
        seniority = _infer_seniority(raw.title)

        is_remote: bool | None = None
        if raw.location:
            loc_lower = raw.location.lower()
            if "remote" in loc_lower:
                is_remote = True
            elif any(city in loc_lower for city in ("vancouver", "burnaby", "richmond", "surrey")):
                is_remote = False

        salary_min, salary_max = _parse_salary_raw(raw.salary_raw)
        salary_source = "regex" if salary_min is not None else None

        return JobPosting(
            job_id=_make_job_id(raw.source.value, raw.url),
            user_id=raw.user_id,
            source=raw.source,
            source_job_id=source_job_id,
            url=raw.url,
            url_hostname=hostname,
            title=raw.title,
            title_normalized=title_normalized,
            company=raw.company,
            company_normalized=company_normalized,
            location=raw.location,
            is_remote=is_remote,
            posted_at=posted_at,
            description=raw.description,
            seniority=seniority,
            salary_min_cad=salary_min,
            salary_max_cad=salary_max,
            salary_source=salary_source,
        )

    def check_url(self, url: str) -> bool:
        try:
            response = requests.head(url, timeout=5, allow_redirects=True)
            return response.status_code < 400
        except requests.RequestException as exc:
            logger.debug("URL health check failed", extra={"url": url, "error": str(exc)})
            return False
