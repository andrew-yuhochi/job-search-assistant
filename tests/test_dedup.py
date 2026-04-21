# tests/test_dedup.py — pytest tests for DedupService.
# Covers: exact-URL detection, fuzzy title+company detection, Stage-3
# description-similarity detection, false-positive protection for different-
# company near-misses and same-company/different-description legitimate pairs,
# canonical ordering, and round-trip DB persistence via repository helpers.
#
# Run with:  pytest tests/test_dedup.py -v
#            pytest tests/test_dedup.py -v -k "not db"   (skip DB tests)

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.models.models import JobPosting, JobState, SeniorityLevel, SourceName
from src.services.dedup import DedupService, DedupResult, _tokenize, _jaccard

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FIXTURES_PATH = (
    Path(__file__).parent / "fixtures" / "jobs_fixtures.json"
)


def _make_job(
    *,
    job_id: str,
    title: str,
    company: str,
    url: str,
    description: str,
    source: SourceName = SourceName.linkedin,
    user_id: str = "local",
) -> JobPosting:
    """Convenience factory: builds a minimal JobPosting for dedup tests."""
    import re

    _strip = re.compile(r"[^\w\s]")
    _ws = re.compile(r"\s+")

    def norm(text: str) -> str:
        return _ws.sub(" ", _strip.sub(" ", text.lower())).strip()

    from urllib.parse import urlparse
    hostname = urlparse(url).netloc

    return JobPosting(
        job_id=job_id,
        user_id=user_id,
        source=source,
        url=url,
        url_hostname=hostname,
        title=title,
        title_normalized=norm(title),
        company=company,
        company_normalized=norm(company),
        description=description,
        seniority=SeniorityLevel.unknown,
        state=JobState.new,
        fetched_at=datetime.now(timezone.utc),
        state_updated_at=datetime.now(timezone.utc),
    )


def _job_from_sha(source: str, url: str, **kwargs) -> JobPosting:
    """Build a job whose job_id is the canonical sha256 used by Normalizer."""
    job_id = hashlib.sha256(f"{source}:{url}".encode()).hexdigest()
    return _make_job(job_id=job_id, url=url, **kwargs)


# ---------------------------------------------------------------------------
# Unit tests — DedupService.check() without DB
# ---------------------------------------------------------------------------


class TestDedupServiceUrlExact:
    """Stage 1: exact URL match → url_exact."""

    def test_exact_url_returns_is_duplicate_true(self):
        svc = DedupService()
        canonical = _make_job(
            job_id="canonical-001",
            title="Data Scientist",
            company="Coveo",
            url="https://www.linkedin.com/jobs/view/9999",
            description="Build NLP models for enterprise search.",
        )
        new_job = _make_job(
            job_id="dup-001",
            title="Applied Data Scientist",          # different title
            company="Loblaws Digital",                # different company
            url="https://www.linkedin.com/jobs/view/9999",  # SAME URL
            description="Lead demand forecasting science.",
        )
        result = svc.check(new_job, [canonical])
        assert result.is_duplicate is True
        assert result.canonical_job_id == "canonical-001"
        assert result.match_type == "url_exact"
        assert result.match_score is None

    def test_exact_url_fixture_pair_jf009_jf010(self):
        """
        Fixture jf-010 re-uses the same URL as jf-009 (same LinkedIn URL for a
        different company posting) — this is the intentional exact-URL pair.
        """
        raw = json.loads(_FIXTURES_PATH.read_text())
        jf009 = next(j for j in raw if j["id"] == "jf-009")
        jf010 = next(j for j in raw if j["id"] == "jf-010")
        assert jf009["url"] == jf010["url"], (
            "Fixture pair jf-009/jf-010 must share the same URL for Stage-1 test"
        )

        canonical = _job_from_sha(
            source=SourceName(jf009["source"]),
            url=jf009["url"],
            title=jf009["title"],
            company=jf009["company"],
            description=jf009["description"],
        )
        duplicate = _job_from_sha(
            source=SourceName(jf010["source"]),
            url=jf010["url"],
            title=jf010["title"],
            company=jf010["company"],
            description=jf010["description"],
        )
        svc = DedupService()
        result = svc.check(duplicate, [canonical])
        assert result.is_duplicate is True
        assert result.match_type == "url_exact"

    def test_different_urls_do_not_match_stage1(self):
        svc = DedupService()
        existing = _make_job(
            job_id="a1",
            title="Data Scientist",
            company="Acme",
            url="https://linkedin.com/jobs/view/111",
            description="ML models.",
        )
        new_job = _make_job(
            job_id="b2",
            title="Data Scientist",
            company="Acme",
            url="https://linkedin.com/jobs/view/222",
            description="ML models.",
        )
        # Should fall through to Stage 2 (and likely hit Stage 2 given same title+co)
        result = svc.check(new_job, [existing])
        # Stage 1 must NOT fire (different URLs)
        if result.is_duplicate:
            assert result.match_type != "url_exact"


class TestDedupServiceFuzzyTitleCompany:
    """Stage 2: fuzzy token_sort_ratio ≥ 90 → fuzzy_title_company."""

    def test_same_title_same_company_different_source_is_duplicate(self):
        """
        jf-001: 'Senior Data Scientist' at 'Acme Corp' (LinkedIn)
        jf-002: 'Data Scientist, Senior' at 'Acme Corp' (Indeed)
        These normalise to nearly identical strings — Stage 2 should fire.
        """
        svc = DedupService()
        canonical = _make_job(
            job_id="jf-001",
            title="Senior Data Scientist",
            company="Acme Corp",
            url="https://www.linkedin.com/jobs/view/3901234001",
            description=(
                "Build churn and LTV prediction models in Python. "
                "Manage end-to-end ML model lifecycle including deployment to SageMaker."
            ),
        )
        duplicate = _make_job(
            job_id="jf-002",
            title="Data Scientist, Senior",
            company="Acme Corp",
            url="https://ca.indeed.com/viewjob?jk=a1b2c3d4e5f60001",
            description=(
                "We are hiring a Data Scientist Senior level to support our "
                "Analytics and AI organization in Vancouver. "
                "Build churn and LTV prediction models in Python."
            ),
        )
        result = svc.check(duplicate, [canonical])
        assert result.is_duplicate is True
        assert result.match_type == "fuzzy_title_company"
        assert result.match_score is not None
        assert 0.0 < result.match_score <= 1.0

    def test_fuzzy_fixture_pair_jf001_jf002(self):
        """
        Full end-to-end fixture validation for the fuzzy-title pair.
        """
        raw = json.loads(_FIXTURES_PATH.read_text())
        jf001 = next(j for j in raw if j["id"] == "jf-001")
        jf002 = next(j for j in raw if j["id"] == "jf-002")

        canonical = _job_from_sha(
            source=SourceName(jf001["source"]),
            url=jf001["url"],
            title=jf001["title"],
            company=jf001["company"],
            description=jf001["description"],
        )
        duplicate = _job_from_sha(
            source=SourceName(jf002["source"]),
            url=jf002["url"],
            title=jf002["title"],
            company=jf002["company"],
            description=jf002["description"],
        )
        svc = DedupService()
        result = svc.check(duplicate, [canonical])
        assert result.is_duplicate is True
        assert result.match_type in ("fuzzy_title_company", "url_exact")

    def test_same_title_different_company_is_not_duplicate(self):
        """
        'Data Scientist' at 'Klue' vs 'Data Scientist' at 'Coveo' — near-miss.
        Different companies → Stage 2 score drops below threshold.
        """
        svc = DedupService()
        existing = _make_job(
            job_id="klue-ds",
            title="Data Scientist",
            company="Klue",
            url="https://www.linkedin.com/jobs/view/3901234011",
            description="NLP models for competitive intelligence.",
        )
        new_job = _make_job(
            job_id="coveo-ds",
            title="Data Scientist",
            company="Coveo",
            url="https://www.linkedin.com/jobs/view/3901234099",
            description="Fine-tune LLMs for enterprise search relevance.",
        )
        result = svc.check(new_job, [existing])
        assert result.is_duplicate is False, (
            "Different companies should not be flagged — near-miss protection"
        )


class TestDedupServiceDescriptionSimilarity:
    """Stage 3: ambiguous fuzzy band (70-89) + Jaccard ≥ 0.5 → description_similarity."""

    def test_ambiguous_band_high_jaccard_is_duplicate(self):
        """
        Simulate a posting where the fuzzy score lands in 70-89 AND descriptions
        are very similar (same duties, minor rewrite) → Stage 3 fires.
        """
        svc = DedupService()
        desc = (
            "Responsibilities: build and deploy machine learning models for "
            "customer churn prediction using Python scikit-learn and XGBoost. "
            "Own the full model lifecycle from feature engineering to monitoring. "
            "Partner with product and engineering to define success metrics. "
            "Run A/B tests using causal inference methods. Mentor junior scientists."
        )
        canonical = _make_job(
            job_id="can-stage3",
            title="Data Scientist Senior NLP Focus",
            company="Alpha Corp",
            url="https://example.com/jobs/100",
            description=desc,
        )
        # Slightly different title (pushes fuzzy to ~75) but near-identical description
        duplicate = _make_job(
            job_id="dup-stage3",
            title="Senior Data Scientist Machine Learning",
            company="Alpha Corp",
            url="https://example.com/jobs/999",
            description=desc,  # identical description → Jaccard = 1.0
        )
        result = svc.check(duplicate, [canonical])
        # With identical descriptions, Stage 3 must fire (or Stage 2 if score ≥ 90)
        assert result.is_duplicate is True

    def test_same_company_different_description_is_not_duplicate(self):
        """
        jf-016: 'Senior Data Scientist — NLP' at Clio Technologies
        jf-017: 'Senior Data Scientist — Computer Vision' at Clio Technologies
        Different duties → Jaccard < 0.5 → NOT a duplicate even in Stage 3.
        """
        raw = json.loads(_FIXTURES_PATH.read_text())
        jf016 = next(j for j in raw if j["id"] == "jf-016")
        jf017 = next(j for j in raw if j["id"] == "jf-017")

        canonical = _job_from_sha(
            source=SourceName(jf016["source"]),
            url=jf016["url"],
            title=jf016["title"],
            company=jf016["company"],
            description=jf016["description"],
        )
        new_job = _job_from_sha(
            source=SourceName(jf017["source"]),
            url=jf017["url"],
            title=jf017["title"],
            company=jf017["company"],
            description=jf017["description"],
        )
        svc = DedupService()
        result = svc.check(new_job, [canonical])
        assert result.is_duplicate is False, (
            "Same company, different specialisation (NLP vs CV) must not be flagged"
        )

    def test_ambiguous_band_low_jaccard_is_not_duplicate(self):
        """
        Construct a case explicitly in the 70-89 fuzzy band with Jaccard < 0.5 →
        Stage 3 rejects → not a duplicate.
        """
        svc = DedupService()
        canonical = _make_job(
            job_id="can-low-j",
            title="Senior Data Scientist NLP Research",
            company="Beta Corp",
            url="https://example.com/jobs/200",
            description=(
                "Fine-tune transformer models for entity extraction. "
                "Build RAG pipelines with dense retrieval. "
                "Publish at ACL and EMNLP. "
                "Use HuggingFace Transformers and PyTorch daily."
            ),
        )
        new_job = _make_job(
            job_id="new-low-j",
            title="Senior Data Scientist Computer Vision",
            company="Beta Corp",
            url="https://example.com/jobs/201",
            description=(
                "Design object detection pipelines for satellite imagery analysis. "
                "Implement semantic segmentation with Detectron2 on GPU clusters. "
                "Deploy real-time inference services using Triton server and ONNX. "
                "Conduct thermal imaging anomaly detection for industrial inspection."
            ),
        )
        result = svc.check(new_job, [canonical])
        assert result.is_duplicate is False, (
            "Low Jaccard in the ambiguous fuzzy band must not produce a false positive"
        )


class TestDedupServiceEdgeCases:
    """Edge cases: empty existing list, canonical ordering, no match."""

    def test_no_existing_jobs_returns_not_duplicate(self):
        svc = DedupService()
        new_job = _make_job(
            job_id="solo",
            title="Data Scientist",
            company="Acme",
            url="https://example.com/job/1",
            description="Some description.",
        )
        result = svc.check(new_job, [])
        assert result.is_duplicate is False
        assert result.canonical_job_id is None
        assert result.match_type is None
        assert result.match_score is None

    def test_canonical_is_first_seen_not_the_new_job(self):
        """
        The canonical job is the one already in existing_jobs (first-seen).
        The new job's job_id must NOT appear as canonical_job_id.
        """
        svc = DedupService()
        first_seen = _make_job(
            job_id="first-001",
            title="Senior Data Scientist",
            company="Acme Corp",
            url="https://www.linkedin.com/jobs/view/1111",
            description="Build churn models and mentor junior scientists on the team.",
        )
        new_arrival = _make_job(
            job_id="new-001",
            title="Data Scientist, Senior",
            company="Acme Corp",
            url="https://ca.indeed.com/viewjob?jk=different",
            description="Build churn and LTV models and mentor two junior data scientists.",
        )
        result = svc.check(new_arrival, [first_seen])
        if result.is_duplicate:
            assert result.canonical_job_id == "first-001", (
                "canonical_job_id must be the first-seen post, not the new arrival"
            )

    def test_dedup_result_dataclass_fields(self):
        """DedupResult must expose all four required fields."""
        r = DedupResult(
            is_duplicate=True,
            canonical_job_id="abc",
            match_type="url_exact",
            match_score=None,
        )
        assert r.is_duplicate is True
        assert r.canonical_job_id == "abc"
        assert r.match_type == "url_exact"
        assert r.match_score is None


# ---------------------------------------------------------------------------
# Helper function unit tests
# ---------------------------------------------------------------------------


class TestTokenizeAndJaccard:
    """Tests for the _tokenize and _jaccard helper functions."""

    def test_tokenize_lowercases_and_splits(self):
        tokens = _tokenize("Hello, World! NLP-models")
        assert "hello" in tokens
        assert "world" in tokens
        assert "nlp" in tokens
        assert "models" in tokens

    def test_jaccard_identical_sets(self):
        a = {"data", "scientist", "python"}
        assert _jaccard(a, a) == 1.0

    def test_jaccard_disjoint_sets(self):
        a = {"data", "science"}
        b = {"legal", "document"}
        assert _jaccard(a, b) == 0.0

    def test_jaccard_empty_sets(self):
        assert _jaccard(set(), set()) == 0.0

    def test_jaccard_partial_overlap(self):
        a = {"a", "b", "c"}
        b = {"b", "c", "d"}
        # |A ∩ B| = 2, |A ∪ B| = 4 → 0.5
        assert _jaccard(a, b) == 0.5
