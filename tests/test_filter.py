"""
Tests for SalaryExtractor, SeniorityInferrer, and FilterService.

Covers all acceptance criteria from TASK-012:
  - ≥10 salary extraction cases (common Canadian formats)
  - SeniorityInferrer correctly classifies "Principal Data Scientist"
  - FilterService location, seniority, salary floor, and company size filters
  - CRITICAL: salary-unknown and size-unknown postings always pass filters
    (regression anchors — these must never be removed)

Run: pytest tests/test_filter.py -v
"""
from __future__ import annotations

import pytest

from src.models.models import JobPosting, SeniorityLevel, SourceName
from src.processing.salary import SalaryExtractor
from src.processing.seniority import SeniorityInferrer
from src.services.filter_service import FilterConfig, FilterResult, FilterService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_posting(
    job_id: str = "test-001",
    title: str = "Data Scientist",
    company: str = "Acme Corp",
    location: str = "Vancouver, BC",
    salary_min: int | None = None,
    salary_max: int | None = None,
    seniority: SeniorityLevel = SeniorityLevel.unknown,
    company_employees_label: str | None = None,
    source: SourceName = SourceName.linkedin,
    is_remote: bool | None = None,
) -> JobPosting:
    return JobPosting(
        job_id=job_id,
        user_id="local",
        source=source,
        url=f"https://www.linkedin.com/jobs/view/{job_id}",
        url_hostname="www.linkedin.com",
        title=title,
        title_normalized=title.lower(),
        company=company,
        company_normalized=company.lower(),
        location=location,
        is_remote=is_remote,
        description="A great data science role.",
        salary_min_cad=salary_min,
        salary_max_cad=salary_max,
        seniority=seniority,
        company_employees_label=company_employees_label,
    )


# ===========================================================================
# SalaryExtractor tests (≥10 cases required)
# ===========================================================================

class TestSalaryExtractor:
    extractor = SalaryExtractor()

    def test_dollar_k_shorthand(self):
        min_, max_, src = self.extractor.extract("Salary: $120K per year")
        assert src == "regex"
        assert min_ == pytest.approx(120_000, abs=1)

    def test_dollar_full_with_comma(self):
        min_, max_, src = self.extractor.extract("Compensation: $120,000 CAD")
        assert src == "regex"
        assert min_ == pytest.approx(120_000, abs=1)

    def test_range_k_shorthand(self):
        min_, max_, src = self.extractor.extract("Pay: $90K–$110K")
        assert src == "regex"
        assert min_ == pytest.approx(90_000, abs=1)
        assert max_ == pytest.approx(110_000, abs=1)

    def test_range_full_numbers(self):
        min_, max_, src = self.extractor.extract("Range: $90,000-$110,000")
        assert src == "regex"
        assert min_ == pytest.approx(90_000, abs=1)
        assert max_ == pytest.approx(110_000, abs=1)

    def test_range_to_keyword(self):
        min_, max_, src = self.extractor.extract("We offer 80K to 100K CAD annually")
        assert src == "regex"
        assert min_ == pytest.approx(80_000, abs=1)
        assert max_ == pytest.approx(100_000, abs=1)

    def test_hourly_rate_converted(self):
        min_, max_, src = self.extractor.extract("$45/hr, full-time position")
        assert src == "regex"
        # 45 × 2080 = 93,600
        assert min_ == pytest.approx(93_600, abs=10)

    def test_hourly_per_hour(self):
        min_, max_, src = self.extractor.extract("Rate: $55 per hour")
        assert src == "regex"
        assert min_ == pytest.approx(55 * 2080, abs=10)

    def test_cad_suffix(self):
        min_, max_, src = self.extractor.extract("120000 CAD total compensation")
        assert src == "regex"
        assert min_ == pytest.approx(120_000, abs=1)

    def test_no_salary_returns_unknown(self):
        min_, max_, src = self.extractor.extract("Great opportunity with competitive pay")
        assert src == "unknown"
        assert min_ is None
        assert max_ is None

    def test_empty_string_returns_unknown(self):
        min_, max_, src = self.extractor.extract("")
        assert src == "unknown"
        assert min_ is None
        assert max_ is None

    def test_large_k_range(self):
        min_, max_, src = self.extractor.extract("$150K–$200K annually")
        assert src == "regex"
        assert min_ == pytest.approx(150_000, abs=1)
        assert max_ == pytest.approx(200_000, abs=1)

    def test_noise_numbers_ignored(self):
        """Numbers like '5 years' or '80%' should not produce a salary."""
        min_, max_, src = self.extractor.extract("5 years experience required, 80% remote")
        assert src == "unknown"

    def test_low_salary_rejected(self):
        """Salary under $15K should be rejected as non-plausible annual."""
        min_, max_, src = self.extractor.extract("$5,000 signing bonus")
        # A $5000 value is not plausible as an annual salary — should be skipped
        # (extractor only keeps values ≥ 15,000)
        # This test validates the _is_plausible_annual guard
        assert src in ("unknown", "regex")  # regex may or may not fire depending on context
        # If regex fires, min must be ≥ 15000
        if src == "regex":
            assert min_ >= 15_000


# ===========================================================================
# SeniorityInferrer tests
# ===========================================================================

class TestSeniorityInferrer:
    inferrer = SeniorityInferrer()

    def test_principal_data_scientist(self):
        level = self.inferrer.infer("Principal Data Scientist")
        assert level == SeniorityLevel.principal

    def test_senior_ml_engineer(self):
        level = self.inferrer.infer("Senior ML Engineer")
        assert level == SeniorityLevel.senior

    def test_junior_analyst(self):
        level = self.inferrer.infer("Junior Data Analyst")
        assert level == SeniorityLevel.junior

    def test_director(self):
        level = self.inferrer.infer("Director of Data Science")
        assert level == SeniorityLevel.director

    def test_staff_engineer(self):
        level = self.inferrer.infer("Staff Data Engineer")
        assert level == SeniorityLevel.staff

    def test_vp(self):
        level = self.inferrer.infer("VP of Engineering")
        assert level == SeniorityLevel.vp

    def test_unknown_title(self):
        level = self.inferrer.infer("Data Scientist")
        assert level == SeniorityLevel.unknown

    def test_jobspy_level_override(self):
        level = self.inferrer.infer("Data Scientist", job_level="Mid-Senior level")
        assert level == SeniorityLevel.senior

    def test_sr_abbreviation(self):
        level = self.inferrer.infer("Sr. Data Scientist")
        assert level == SeniorityLevel.senior

    def test_jr_abbreviation(self):
        level = self.inferrer.infer("Jr. ML Engineer")
        assert level == SeniorityLevel.junior


# ===========================================================================
# FilterService tests
# ===========================================================================

class TestFilterServiceLocation:
    svc = FilterService()

    def test_metro_vancouver_excludes_toronto(self):
        """Default metro_locations config excludes 'Toronto, ON' posting."""
        posting = make_posting(job_id="t1", location="Toronto, ON")
        config = FilterConfig()  # uses default metro_locations
        result = self.svc.apply([posting], config)
        assert result.excluded_count == 1
        assert result.kept_count == 0
        assert "Toronto, ON" in result.excluded[0][1]

    def test_metro_vancouver_keeps_vancouver(self):
        posting = make_posting(job_id="t2", location="Vancouver, BC, Canada")
        config = FilterConfig()
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_burnaby_passes(self):
        """Burnaby is a metro Vancouver municipality and should pass."""
        posting = make_posting(job_id="t4", location="Burnaby, BC")
        config = FilterConfig()
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_surrey_passes(self):
        """Surrey is a metro Vancouver municipality and should pass."""
        posting = make_posting(job_id="t5", location="Surrey, BC, Canada")
        config = FilterConfig()
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_remote_canadian_passes(self):
        """Remote postings with no explicit location pass as remote-CA."""
        posting = make_posting(job_id="t6", location="Remote", is_remote=True)
        config = FilterConfig(allow_remote=True)
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_remote_canada_explicit_passes(self):
        """'Canada Remote' location string passes as remote-CA."""
        posting = make_posting(job_id="t7", location="Canada Remote")
        config = FilterConfig(allow_remote=True)
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_remote_non_canadian_excluded(self):
        """Remote posting with 'Austin, TX' excluded even when is_remote=True."""
        posting = make_posting(job_id="t8", location="Austin, TX", is_remote=True)
        config = FilterConfig(allow_remote=True)
        result = self.svc.apply([posting], config)
        assert result.excluded_count == 1
        assert "Austin, TX" in result.excluded[0][1]

    def test_remote_new_york_excluded(self):
        """Remote posting with 'New York, NY' excluded."""
        posting = make_posting(job_id="t9", location="New York, NY", is_remote=True)
        config = FilterConfig(allow_remote=True)
        result = self.svc.apply([posting], config)
        assert result.excluded_count == 1

    def test_remote_flag_true_location_blank_passes(self):
        """is_remote=True with blank location treated as remote-CA (pass)."""
        posting = make_posting(job_id="t10", location="", is_remote=True)
        config = FilterConfig(allow_remote=True)
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_remote_passes_location_filter_legacy(self):
        """Backward-compat: Remote postings pass even with legacy locations list."""
        posting = make_posting(job_id="t3", location="Remote", is_remote=True)
        config = FilterConfig(metro_locations=["vancouver"], allow_remote=True)
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_location_filter_disabled_when_metro_locations_none(self):
        """metro_locations=None disables the location filter entirely."""
        posting = make_posting(job_id="t11", location="Toronto, ON")
        config = FilterConfig(metro_locations=None, locations=None)
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1


class TestFilterServiceSeniority:
    svc = FilterService()

    def test_principal_excluded(self):
        """Principal Data Scientist is excluded when max_seniority='principal'."""
        posting = make_posting(
            job_id="s1",
            title="Principal Data Scientist",
            seniority=SeniorityLevel.principal,
        )
        config = FilterConfig(max_seniority="principal")
        result = self.svc.apply([posting], config)
        assert result.excluded_count == 1
        assert "principal" in result.excluded[0][1].lower()

    def test_senior_passes_when_max_is_staff(self):
        posting = make_posting(
            job_id="s2",
            seniority=SeniorityLevel.senior,
        )
        config = FilterConfig(max_seniority="staff")
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_unknown_seniority_always_passes(self):
        """Unknown seniority always passes the seniority filter."""
        posting = make_posting(job_id="s3", seniority=SeniorityLevel.unknown)
        config = FilterConfig(max_seniority="senior")
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_director_excluded(self):
        posting = make_posting(job_id="s4", seniority=SeniorityLevel.director)
        config = FilterConfig(max_seniority="principal")
        result = self.svc.apply([posting], config)
        assert result.excluded_count == 1


class TestFilterServiceSalary:
    svc = FilterService()

    def test_salary_below_floor_excluded(self):
        """$90K posting excluded when floor is $120K."""
        posting = make_posting(job_id="sal1", salary_min=90_000, salary_max=90_000)
        config = FilterConfig(min_salary_cad=120_000)
        result = self.svc.apply([posting], config)
        assert result.excluded_count == 1
        assert "90,000" in result.excluded[0][1]

    def test_salary_above_floor_kept(self):
        posting = make_posting(job_id="sal2", salary_min=130_000, salary_max=150_000)
        config = FilterConfig(min_salary_cad=120_000)
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_unknown_salary_passes_with_badge(self):
        """Posting with no salary information passes with 'Salary unknown' badge."""
        posting = make_posting(job_id="sal3", salary_min=None, salary_max=None)
        config = FilterConfig(min_salary_cad=120_000)
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1
        kept = result.kept[0]
        assert hasattr(kept, "badge_flags")
        assert "Salary unknown" in kept.badge_flags  # type: ignore[attr-defined]

    def test_range_max_used_for_floor(self):
        """A $90K–$130K range passes $120K floor because max ≥ floor."""
        posting = make_posting(job_id="sal4", salary_min=90_000, salary_max=130_000)
        config = FilterConfig(min_salary_cad=120_000)
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1


class TestFilterServiceCompanySize:
    svc = FilterService()

    def test_indeed_micro_size_excluded(self):
        """Indeed '1 to 10' company size is excluded when in exclude list."""
        posting = make_posting(
            job_id="sz1",
            company_employees_label="1 to 10",
            source=SourceName.indeed,
        )
        config = FilterConfig(company_size_exclude=["1 to 10"])
        result = self.svc.apply([posting], config)
        assert result.excluded_count == 1

    def test_linkedin_unknown_size_passes_with_badge(self):
        """LinkedIn posting with no size label passes with 'Size unknown' badge."""
        posting = make_posting(
            job_id="sz2",
            company_employees_label=None,
            source=SourceName.linkedin,
        )
        config = FilterConfig(company_size_exclude=["1 to 10"])
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1
        kept = result.kept[0]
        assert hasattr(kept, "badge_flags")
        assert "Size unknown" in kept.badge_flags  # type: ignore[attr-defined]

    def test_large_company_not_excluded(self):
        posting = make_posting(
            job_id="sz3",
            company_employees_label="1,001 to 5,000",
        )
        config = FilterConfig(company_size_exclude=["1 to 10"])
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1

    def test_no_size_filter_passes_all(self):
        """When company_size_exclude is None, no size filtering occurs."""
        posting = make_posting(job_id="sz4", company_employees_label="1 to 10")
        config = FilterConfig(company_size_exclude=None)
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1


class TestFilterServiceCombined:
    svc = FilterService()

    def test_empty_list_returns_empty(self):
        config = FilterConfig(min_salary_cad=120_000)
        result = self.svc.apply([], config)
        assert result.kept_count == 0
        assert result.excluded_count == 0

    def test_no_location_config_passes_all_locations(self):
        """metro_locations=None disables location filter → all postings pass."""
        postings = [
            make_posting("c1", location="Toronto", salary_min=50_000, salary_max=50_000),
            make_posting("c2", location="Remote"),
        ]
        config = FilterConfig(metro_locations=None, locations=None)  # location filter off
        result = self.svc.apply(postings, config)
        assert result.kept_count == 2
        assert result.excluded_count == 0

    def test_both_salary_and_size_unknown_pass(self):
        """Posting with unknown salary AND unknown size passes both filters with both badges."""
        posting = make_posting(
            job_id="c3",
            salary_min=None,
            salary_max=None,
            company_employees_label=None,
        )
        config = FilterConfig(min_salary_cad=120_000, company_size_exclude=["1 to 10"])
        result = self.svc.apply([posting], config)
        assert result.kept_count == 1
        kept = result.kept[0]
        assert "Salary unknown" in kept.badge_flags  # type: ignore[attr-defined]
        assert "Size unknown" in kept.badge_flags  # type: ignore[attr-defined]
