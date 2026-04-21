"""
SalaryExtractor: regex-based extraction of salary ranges from Canadian job postings.

Handles formats: "$120K", "$120,000", "$90K–$110K", "$45/hr", "120000 CAD",
hourly-to-annual conversion (×2080). Returns (None, None, "unknown") when no
salary found — never raises. Called by ScrapeRunner (TASK-013) after Normalizer.
"""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hourly conversion constant
# ---------------------------------------------------------------------------
HOURS_PER_YEAR = 2080  # 40 hrs/wk × 52 wks

# ---------------------------------------------------------------------------
# Regex patterns — ordered from most specific to least specific
# ---------------------------------------------------------------------------

# Matches: "$120K–$150K", "$90,000-$110,000", "120K to 150K", etc.
_RANGE_PATTERN = re.compile(
    r"\$?\s*(\d[\d,]*(?:\.\d+)?)\s*[Kk]?"
    r"\s*(?:–|-|to|—)\s*"
    r"\$?\s*(\d[\d,]*(?:\.\d+)?)\s*[Kk]?",
    re.IGNORECASE,
)

# Matches hourly rates: "$45/hr", "$45/hour", "45 per hour"
_HOURLY_PATTERN = re.compile(
    r"\$?\s*(\d[\d,]*(?:\.\d+)?)\s*(?:/\s*h(?:r|our)|per\s+hour)",
    re.IGNORECASE,
)

# Matches single annual figure: "$120K", "$120,000", "120000 CAD", "120K CAD"
_SINGLE_PATTERN = re.compile(
    r"\$?\s*(\d[\d,]*(?:\.\d+)?)\s*[Kk]?\s*(?:CAD|USD|per\s+year|/\s*yr|annually)?",
    re.IGNORECASE,
)


def _parse_amount(raw: str) -> float:
    """Convert a raw number string like '120,000' or '120K' to a float."""
    cleaned = raw.replace(",", "").strip()
    val = float(cleaned)
    # K suffix already removed from raw by the caller (captured group 1 of regex)
    return val


def _is_plausible_annual(value: float) -> bool:
    """Reject clearly non-salary numbers (e.g. years of experience, percentages)."""
    return 15_000 <= value <= 1_000_000


class SalaryExtractor:
    """
    Extracts salary range from free-text job postings.

    Returns (min_cad, max_cad, source) where source is 'regex' on success
    or 'unknown' when no salary information is found.

    All extraction is regex-only at PoC. LLM fallback is deferred to
    Milestone 4 per TASK-012 implementation notes.
    """

    def extract(self, text: str) -> tuple[float | None, float | None, str]:
        """
        Parse salary from text.

        Returns:
            (min_cad, max_cad, source) where source = 'regex' | 'unknown'
            (None, None, 'unknown') when no salary is found.
        """
        if not text:
            return None, None, "unknown"

        # --- 1. Try range pattern first (most informative) ---
        for m in _RANGE_PATTERN.finditer(text):
            lo_raw = m.group(1)
            hi_raw = m.group(2)
            try:
                lo = _parse_amount(lo_raw)
                hi = _parse_amount(hi_raw)
            except ValueError:
                continue

            # Detect K suffix from surrounding match text
            match_text = m.group(0)
            if re.search(r"\d\s*[Kk]\b", match_text.split("–")[0].split("-")[0]):
                lo *= 1_000
            if re.search(r"\d\s*[Kk]\b", match_text.split("–")[-1].split("-")[-1].split("to")[-1]):
                hi *= 1_000

            if not _is_plausible_annual(lo) and not _is_plausible_annual(hi):
                continue

            # Normalise so min ≤ max
            lo, hi = min(lo, hi), max(lo, hi)
            if _is_plausible_annual(lo) or _is_plausible_annual(hi):
                logger.debug("Salary extracted via range pattern", extra={"min": lo, "max": hi})
                return lo, hi, "regex"

        # --- 2. Try hourly pattern ---
        for m in _HOURLY_PATTERN.finditer(text):
            try:
                rate = _parse_amount(m.group(1))
            except ValueError:
                continue
            if 10 <= rate <= 500:  # plausible hourly range
                annual = round(rate * HOURS_PER_YEAR)
                logger.debug("Salary extracted via hourly pattern", extra={"rate": rate, "annual": annual})
                return float(annual), float(annual), "regex"

        # --- 3. Try single annual figure ---
        best: float | None = None
        for m in _SINGLE_PATTERN.finditer(text):
            raw_num = m.group(1)
            match_suffix = m.group(0)
            try:
                val = _parse_amount(raw_num)
            except ValueError:
                continue

            if re.search(r"\d\s*[Kk]\b", match_suffix):
                val *= 1_000

            if _is_plausible_annual(val):
                if best is None or val > best:
                    best = val

        if best is not None:
            logger.debug("Salary extracted via single pattern", extra={"value": best})
            return best, best, "regex"

        return None, None, "unknown"
