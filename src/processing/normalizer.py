# Normalizer: cleans RawJobPosting into canonical JobPosting fields.
# Called by scrape_runner.py (TASK-011) and seed_from_fixtures.py (TASK-009).
# TASK-M4-002: expanded seniority keywords, location standardization,
#              title pre-filter for malformed Google results.
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
    # C-suite / exec must appear before vp and director to avoid partial matches
    (re.compile(r"\bcto\b|\bceo\b|\bcoo\b|\bcso\b|\bc-suite\b|\bchief\b", re.IGNORECASE), SeniorityLevel.csuite),
    (re.compile(r"\bvp\b|\bvice\s+president\b", re.IGNORECASE), SeniorityLevel.vp),
    (re.compile(r"\bdirector\b", re.IGNORECASE), SeniorityLevel.director),
    (re.compile(r"\bprincipal\b", re.IGNORECASE), SeniorityLevel.principal),
    (re.compile(r"\bstaff\b", re.IGNORECASE), SeniorityLevel.staff),
    # "lead" as a standalone word — "Technical Lead", "Lead ML Engineer"
    # Uses negative lookahead to avoid matching "leadership" or "leading".
    (re.compile(r"\blead(?!er|ing|s\b)", re.IGNORECASE), SeniorityLevel.senior),
    (re.compile(r"\bsenior\b|\bsr\.?\b", re.IGNORECASE), SeniorityLevel.senior),
    (re.compile(r"\bmanager\b", re.IGNORECASE), SeniorityLevel.mid),
    (re.compile(r"\bmid[\s-]level\b|\bintermediate\b", re.IGNORECASE), SeniorityLevel.mid),
    # junior / entry-level keywords — last so senior variants above take priority
    (re.compile(r"\bjunior\b|\bjr\.?\b|\bentry[\s-]?level\b|\bgraduate\b|\bintern\b", re.IGNORECASE), SeniorityLevel.junior),
]


# ---------------------------------------------------------------------------
# Fix 4: Location standardization
# ---------------------------------------------------------------------------

# Strip trailing country-code suffix appended by jobspy (e.g. ", CA" or ", CA,")
_CA_SUFFIX_RE = re.compile(r",\s*CA,?\s*$", re.IGNORECASE)

# Province abbreviation → full name (word-boundary match on the abbreviation)
_PROVINCE_EXPANSIONS: list[tuple[re.Pattern, str]] = [
    (re.compile(r",\s*\bBC\b", re.IGNORECASE), ", British Columbia"),
    (re.compile(r",\s*\bON\b", re.IGNORECASE), ", Ontario"),
    (re.compile(r",\s*\bAB\b", re.IGNORECASE), ", Alberta"),
    (re.compile(r",\s*\bQC\b", re.IGNORECASE), ", Quebec"),
    (re.compile(r",\s*\bMB\b", re.IGNORECASE), ", Manitoba"),
    (re.compile(r",\s*\bSK\b", re.IGNORECASE), ", Saskatchewan"),
    (re.compile(r",\s*\bNS\b", re.IGNORECASE), ", Nova Scotia"),
    (re.compile(r",\s*\bNB\b", re.IGNORECASE), ", New Brunswick"),
    (re.compile(r",\s*\bNL\b", re.IGNORECASE), ", Newfoundland and Labrador"),
    (re.compile(r",\s*\bPE\b", re.IGNORECASE), ", Prince Edward Island"),
    (re.compile(r",\s*\bNT\b", re.IGNORECASE), ", Northwest Territories"),
    (re.compile(r",\s*\bYT\b", re.IGNORECASE), ", Yukon"),
    (re.compile(r",\s*\bNU\b", re.IGNORECASE), ", Nunavut"),
]

# Known Canadian province/territory full names used to detect whether to append ", Canada"
_CANADIAN_PROVINCES = {
    "british columbia", "ontario", "alberta", "quebec", "manitoba",
    "saskatchewan", "nova scotia", "new brunswick",
    "newfoundland and labrador", "prince edward island",
    "northwest territories", "yukon", "nunavut",
}


def _standardize_location(location: str | None) -> str | None:
    """
    Normalize a raw location string:
    1. Strip trailing ', CA' or ', CA,' country-code suffixes added by jobspy.
    2. Expand province abbreviations to full names.
    3. Append ', Canada' if a known province name is present and string doesn't
       already end with 'Canada'.
    """
    if not location:
        return location

    loc = location.strip()

    # Step 1: strip trailing ", CA" country-code suffix
    loc = _CA_SUFFIX_RE.sub("", loc).strip().rstrip(",").strip()

    # Step 2: expand province abbreviations
    for pattern, replacement in _PROVINCE_EXPANSIONS:
        loc = pattern.sub(replacement, loc)

    # Step 3: append ", Canada" if a province is present and it's not already there
    if not re.search(r"\bCanada\b", loc, re.IGNORECASE):
        loc_lower = loc.lower()
        for province in _CANADIAN_PROVINCES:
            if province in loc_lower:
                loc = loc.rstrip() + ", Canada"
                break

    return loc


# ---------------------------------------------------------------------------
# Fix 5: Title pre-filter for malformed Google results
# ---------------------------------------------------------------------------

# Matches "Role Title - Apply" or "Role Title - Apply Now" at end of string
_TITLE_APPLY_RE = re.compile(r"\s+-\s+Apply(?:\s+Now)?\s*$", re.IGNORECASE)

# Matches embedded location like " - Vancouver, BC" or " - Toronto, Ontario" in title
_TITLE_EMBEDDED_LOCATION_RE = re.compile(
    r"\s+-\s+[A-Z][a-zA-Z\s]+,\s+(?:"
    r"BC|ON|AB|QC|MB|SK|NS|NB|NL|PE|NT|YT|NU"
    r"|British Columbia|Ontario|Alberta|Quebec|Manitoba|Saskatchewan"
    r"|Nova Scotia|New Brunswick|Newfoundland|Prince Edward Island"
    r"|Northwest Territories|Yukon|Nunavut"
    r")",
    re.IGNORECASE,
)


def _is_malformed_title(title: str) -> bool:
    """Return True if the title is a known malformed Google Jobs artifact."""
    if _TITLE_APPLY_RE.search(title):
        return True
    if _TITLE_EMBEDDED_LOCATION_RE.search(title):
        return True
    return False


# ---------------------------------------------------------------------------
# Existing helpers
# ---------------------------------------------------------------------------


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

    def normalize(self, raw: RawJobPosting) -> JobPosting | None:
        """
        Normalize a RawJobPosting into a canonical JobPosting.

        Returns None (and logs a debug message) when Fix 5 detects a malformed
        title (e.g. Google Jobs artifacts like "Role - Apply Now" or
        "Role - Vancouver, BC").  The caller (ScrapeRunner) must skip None results.
        """
        # Fix 5: drop malformed Google Jobs titles before any further processing
        if _is_malformed_title(raw.title):
            logger.debug(
                "Normalizer: dropping malformed title: %r (url=%s)", raw.title, raw.url
            )
            return None

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

        # Fix 4: standardize location string
        location = _standardize_location(raw.location)

        is_remote: bool | None = None
        if location:
            loc_lower = location.lower()
            if "remote" in loc_lower:
                is_remote = True
            elif any(city in loc_lower for city in ("vancouver", "burnaby", "richmond", "surrey")):
                is_remote = False

        from src.processing.salary import SalaryExtractor
        _extractor = SalaryExtractor()
        # Prefer structured jobspy fields; fall back to regex on salary_raw / description.
        salary_text = raw.salary_raw or raw.description
        salary_min, salary_max, salary_source_str = _extractor.extract(
            salary_text,
            salary_min_raw=getattr(raw, "salary_min_raw", None),
            salary_max_raw=getattr(raw, "salary_max_raw", None),
            salary_currency=getattr(raw, "salary_currency", None),
            salary_interval=getattr(raw, "salary_interval", None),
        )
        salary_source = salary_source_str if salary_min is not None else None

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
            location=location,
            is_remote=is_remote,
            posted_at=posted_at,
            # Fix 2: carry key fields through from RawJobPosting
            description=raw.description,
            search_term=getattr(raw, "search_term", None),
            seniority=seniority,
            # Round to int — SalaryExtractor returns float; JobPosting.salary_min/max_cad are int
            salary_min_cad=round(salary_min) if salary_min is not None else None,
            salary_max_cad=round(salary_max) if salary_max is not None else None,
            salary_source=salary_source,
        )

    def check_url(self, url: str) -> bool:
        try:
            response = requests.head(url, timeout=5, allow_redirects=True)
            return response.status_code < 400
        except requests.RequestException as exc:
            logger.debug("URL health check failed", extra={"url": url, "error": str(exc)})
            return False
