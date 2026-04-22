"""
SalaryExtractor: structured-field-first extraction of salary ranges from Canadian job postings.

Priority order:
  1. Structured fields from jobspy (salary_min_raw / salary_max_raw + salary_interval).
  2. Regex on salary_raw string (already-formatted string from the scraper).
  3. Regex on free-text description.

Handles formats: "$120K", "$120,000", "$90K–$110K", "$45/hr", "120000 CAD",
hourly-to-annual conversion (×2080). Returns (None, None, "unknown") when no
salary found — never raises. Called by Normalizer (TASK-013).
"""
from __future__ import annotations

import logging
import re
from typing import Optional

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
    Extracts salary range from job postings.

    Tries structured jobspy fields first (most accurate), then regex on the
    formatted salary_raw string, then regex on the description text.

    Returns (min_cad, max_cad, source) where source is:
      'source_field' — structured salary from jobspy (LinkedIn / Indeed)
      'regex'        — extracted via regex from salary_raw or description
      'unknown'      — no salary information found

    All extraction at PoC. LLM fallback deferred to Milestone 4.
    """

    _USD_TO_CAD = 1.36  # approximate conversion rate; good enough for PoC filtering

    def extract(
        self,
        text: str,
        *,
        salary_min_raw: Optional[float] = None,
        salary_max_raw: Optional[float] = None,
        salary_currency: Optional[str] = None,
        salary_interval: Optional[str] = None,
    ) -> tuple[float | None, float | None, str]:
        """
        Parse salary.

        Parameters
        ----------
        text:
            Free-text description (used as regex fallback).
        salary_min_raw, salary_max_raw:
            Structured amounts from jobspy (may be None).
        salary_currency:
            ISO currency code from jobspy (e.g. 'CAD', 'USD').
        salary_interval:
            'yearly' | 'hourly' | 'monthly' from jobspy.

        Returns
        -------
        (min_cad, max_cad, source) or (None, None, 'unknown').
        """
        # --- 0. Structured fields — highest priority ---
        if salary_min_raw is not None:
            lo = salary_min_raw
            hi = salary_max_raw if salary_max_raw is not None else salary_min_raw

            # Annualise if hourly
            if salary_interval and salary_interval.lower() == "hourly":
                lo = round(lo * HOURS_PER_YEAR)
                hi = round(hi * HOURS_PER_YEAR)
            elif salary_interval and salary_interval.lower() == "monthly":
                lo = round(lo * 12)
                hi = round(hi * 12)
            else:
                lo = round(lo)
                hi = round(hi)

            # Currency conversion (USD → CAD); other currencies kept as-is.
            currency_upper = (salary_currency or "CAD").upper()
            if currency_upper == "USD":
                lo = round(lo * self._USD_TO_CAD)
                hi = round(hi * self._USD_TO_CAD)

            lo, hi = min(lo, hi), max(lo, hi)
            if _is_plausible_annual(lo) or _is_plausible_annual(hi):
                logger.debug(
                    "Salary extracted via source_field",
                    extra={"min": lo, "max": hi, "interval": salary_interval, "currency": salary_currency},
                )
                return float(lo), float(hi), "source_field"

        # --- Regex fallback on description text ---
        return self._extract_from_text(text)

    def _extract_from_text(self, text: str) -> tuple[float | None, float | None, str]:
        """Regex extraction from free-text (salary_raw string or description)."""
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
