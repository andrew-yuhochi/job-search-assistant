"""
Standalone fixture validation tests for TASK-002.

No Pydantic models yet (TASK-003 not implemented). Uses Python's json module
and basic assertions to validate schema, content rules, and demo artifact.

Acceptance criteria tested:
  AC-1: jobs_fixtures.json has exactly 15 entries
  AC-2: Each entry validates against the RawJobPosting field contract
  AC-3: An intentional duplicate pair is detectable by fuzzy title+company matching
  AC-4: knowledge_bank_fixture.md has >=3 ## sections and >=1 ### subsection per employer
  AC-5: demos/milestone-1/TASK-002-fixtures.json matches jobs_fixtures.json
"""

import json
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Fixture paths (absolute so they survive any cwd reset)
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # projects/job-search-assistant
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"
JOBS_FIXTURE = FIXTURES_DIR / "jobs_fixtures.json"
KB_FIXTURE = FIXTURES_DIR / "knowledge_bank_fixture.md"
DEMO_ARTIFACT = PROJECT_ROOT / "demos" / "milestone-1" / "TASK-002-fixtures.json"

# ---------------------------------------------------------------------------
# RawJobPosting field contract (mirrors TDD §2.4 / TASK-003 design)
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = {
    "id": str,
    "title": str,
    "company": str,
    "location": str,
    "source": str,
    "url": str,
    "description": str,
    "posted_date": str,
    "user_id": str,
    "expected_specialty": str,
    # salary_raw is nullable (str | None)
}

NULLABLE_FIELDS = {"salary_raw"}

VALID_SOURCES = {"linkedin", "indeed", "google"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_jobs() -> list[dict]:
    with open(JOBS_FIXTURE, encoding="utf-8") as f:
        return json.load(f)


def load_demo() -> list[dict]:
    with open(DEMO_ARTIFACT, encoding="utf-8") as f:
        return json.load(f)


def _normalize_title_company(title: str, company: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace — mirrors Normalizer logic."""
    combined = f"{title} {company}".lower()
    combined = re.sub(r"[^\w\s]", " ", combined)
    combined = re.sub(r"\s+", " ", combined).strip()
    return combined


def _token_sort(s: str) -> str:
    """Sort tokens for fuzzy duplicate detection (mirrors rapidfuzz token_sort_ratio input)."""
    return " ".join(sorted(s.split()))


# ---------------------------------------------------------------------------
# AC-1: exactly 17 entries (TASK-009: expanded from 15 to 17)
# 15 canonical postings + 2 intentional duplicate postings
# ---------------------------------------------------------------------------

class TestJobsFixtureCount:
    def test_exactly_15_entries(self):
        jobs = load_jobs()
        assert len(jobs) == 17, (
            f"Expected 17 entries in jobs_fixtures.json (15 canonical + 2 duplicates), got {len(jobs)}"
        )


# ---------------------------------------------------------------------------
# AC-2: each entry validates against the RawJobPosting field contract
# ---------------------------------------------------------------------------

class TestRawJobPostingSchema:
    def test_all_required_fields_present(self):
        jobs = load_jobs()
        missing = []
        for job in jobs:
            for field in REQUIRED_FIELDS:
                if field not in job and field not in NULLABLE_FIELDS:
                    missing.append(f"id={job.get('id','?')}: missing field '{field}'")
        assert not missing, "Required fields missing:\n" + "\n".join(missing)

    def test_all_required_fields_correct_type(self):
        jobs = load_jobs()
        wrong = []
        for job in jobs:
            for field, expected_type in REQUIRED_FIELDS.items():
                value = job.get(field)
                if field in NULLABLE_FIELDS:
                    # nullable: None is allowed, otherwise must be str
                    if value is not None and not isinstance(value, expected_type):
                        wrong.append(
                            f"id={job.get('id','?')}: '{field}' expected {expected_type.__name__} "
                            f"or None, got {type(value).__name__}"
                        )
                else:
                    if not isinstance(value, expected_type):
                        wrong.append(
                            f"id={job.get('id','?')}: '{field}' expected {expected_type.__name__}, "
                            f"got {type(value).__name__} (value={value!r})"
                        )
        assert not wrong, "Type errors:\n" + "\n".join(wrong)

    def test_salary_raw_is_str_or_none(self):
        jobs = load_jobs()
        bad = [
            f"id={j.get('id','?')}: salary_raw={j['salary_raw']!r}"
            for j in jobs
            if "salary_raw" not in j or (
                j["salary_raw"] is not None and not isinstance(j["salary_raw"], str)
            )
        ]
        assert not bad, "salary_raw must be str or None:\n" + "\n".join(bad)

    def test_source_values_are_valid(self):
        jobs = load_jobs()
        bad = [
            f"id={j.get('id','?')}: source={j.get('source')!r}"
            for j in jobs
            if j.get("source") not in VALID_SOURCES
        ]
        assert not bad, (
            f"source must be one of {VALID_SOURCES}:\n" + "\n".join(bad)
        )

    def test_id_values_are_unique(self):
        jobs = load_jobs()
        ids = [j.get("id") for j in jobs]
        duplicates = [i for i in ids if ids.count(i) > 1]
        assert not duplicates, (
            f"Duplicate 'id' values found (ids must be unique): {set(duplicates)}"
        )

    def test_url_values_are_non_empty_strings(self):
        jobs = load_jobs()
        bad = [
            f"id={j.get('id','?')}: url={j.get('url')!r}"
            for j in jobs
            if not isinstance(j.get("url"), str) or not j.get("url").strip()
        ]
        assert not bad, "url must be a non-empty string:\n" + "\n".join(bad)

    def test_user_id_is_local_for_all_entries(self):
        """All fixture entries should carry user_id='local' (single-tenant PoC default)."""
        jobs = load_jobs()
        bad = [
            f"id={j.get('id','?')}: user_id={j.get('user_id')!r}"
            for j in jobs
            if j.get("user_id") != "local"
        ]
        assert not bad, "user_id must be 'local' for all fixture entries:\n" + "\n".join(bad)

    def test_description_is_non_empty(self):
        jobs = load_jobs()
        bad = [
            f"id={j.get('id','?')}"
            for j in jobs
            if not isinstance(j.get("description"), str) or len(j.get("description", "").strip()) == 0
        ]
        assert not bad, "description must be a non-empty string:\n" + "\n".join(bad)

    def test_no_extra_unexpected_top_level_keys(self):
        """Warn if entries have keys outside the known RawJobPosting field set."""
        known_fields = set(REQUIRED_FIELDS) | NULLABLE_FIELDS
        jobs = load_jobs()
        extra = []
        for job in jobs:
            unexpected = set(job.keys()) - known_fields
            if unexpected:
                extra.append(f"id={job.get('id','?')}: unexpected keys {unexpected}")
        # This is a warning-level check; fail so the developer is aware
        assert not extra, (
            "Entries contain keys outside the RawJobPosting contract. "
            "Either add them to the schema or remove them:\n" + "\n".join(extra)
        )


# ---------------------------------------------------------------------------
# AC-3: intentional duplicate pair detectable by fuzzy title+company
# ---------------------------------------------------------------------------

class TestDuplicatePair:
    def test_at_least_one_duplicate_pair_detected_by_token_sort(self):
        """
        TASK-002 requires 2 duplicate pairs.
        This test checks that at least one pair is detectable by sorted-token
        equality on normalized (title + company) — the same logic DedupService
        will use with token_sort_ratio >= 90.
        """
        jobs = load_jobs()
        normalized = [
            (j["id"], _token_sort(_normalize_title_company(j["title"], j["company"])))
            for j in jobs
        ]
        pairs_found = []
        seen = {}
        for job_id, key in normalized:
            if key in seen:
                pairs_found.append((seen[key], job_id))
            else:
                seen[key] = job_id

        assert len(pairs_found) >= 1, (
            "No duplicate pair detected via token-sort on normalized (title+company). "
            "TASK-002 requires at least one pair that fuzzy dedup can catch. "
            "Current normalized keys:\n"
            + "\n".join(f"  {jid}: {key}" for jid, key in normalized)
        )

    def test_duplicate_pair_is_same_company(self):
        """Both entries in a duplicate pair should share the same company name (case-insensitive)."""
        jobs = load_jobs()
        company_map = {j["id"]: j["company"].lower().strip() for j in jobs}

        # Build candidate pairs by checking for shared company + similar title
        title_map = {j["id"]: j["title"].lower() for j in jobs}

        pairs_with_same_company = []
        ids = list(company_map.keys())
        for i in range(len(ids)):
            for k in range(i + 1, len(ids)):
                id_a, id_b = ids[i], ids[k]
                if company_map[id_a] == company_map[id_b]:
                    pairs_with_same_company.append((id_a, id_b))

        assert len(pairs_with_same_company) >= 1, (
            "No two entries share the same company. "
            "A duplicate pair (same company, slightly different title) is required by TASK-002."
        )

    def test_exact_url_duplicate_pair_detected(self):
        """
        TASK-002 requires a second duplicate pair detectable by exact URL match.
        Two entries from different sources must share the same url value.
        This mirrors the DedupService exact-URL dedup path.
        """
        jobs = load_jobs()
        url_to_ids: dict[str, list[str]] = {}
        for job in jobs:
            url = job.get("url", "")
            url_to_ids.setdefault(url, []).append(job["id"])

        exact_url_pairs = [
            ids for url, ids in url_to_ids.items() if len(ids) >= 2
        ]

        assert len(exact_url_pairs) >= 1, (
            "No exact-URL duplicate pair found. TASK-002 requires at least one pair "
            "where two entries (different sources) share the same url value.\n"
            "URL counts:\n"
            + "\n".join(f"  {url}: {ids}" for url, ids in url_to_ids.items())
        )

        # The entries in the pair must come from different sources
        for ids in exact_url_pairs:
            sources = [
                next(j["source"] for j in jobs if j["id"] == jid)
                for jid in ids
            ]
            assert len(set(sources)) > 1, (
                f"Exact-URL duplicate pair {ids} has identical sources {sources}. "
                "The pair must represent entries from different sources (e.g. linkedin vs google)."
            )


# ---------------------------------------------------------------------------
# expected_specialty field coverage
# ---------------------------------------------------------------------------

VALID_SPECIALTY_VALUES = {
    "data_scientist",
    "ml_engineer",
    "data_engineer",
    "data_analyst",
    "unclassified",
}


class TestExpectedSpecialty:
    def test_all_entries_have_expected_specialty(self):
        """Every fixture entry must carry an expected_specialty field (W-003)."""
        jobs = load_jobs()
        missing = [
            f"id={j.get('id','?')}"
            for j in jobs
            if "expected_specialty" not in j
        ]
        assert not missing, (
            "The following entries are missing the 'expected_specialty' field:\n"
            + "\n".join(missing)
        )

    def test_expected_specialty_values_are_valid(self):
        """expected_specialty must be one of the known classifier output labels."""
        jobs = load_jobs()
        bad = [
            f"id={j.get('id','?')}: expected_specialty={j.get('expected_specialty')!r}"
            for j in jobs
            if j.get("expected_specialty") not in VALID_SPECIALTY_VALUES
        ]
        assert not bad, (
            f"expected_specialty must be one of {VALID_SPECIALTY_VALUES}:\n"
            + "\n".join(bad)
        )

    def test_expected_specialty_covers_all_role_types(self):
        """
        The fixture must exercise every classifier output label at least once
        so that specialty-classification tests are meaningful.
        """
        jobs = load_jobs()
        present = {j.get("expected_specialty") for j in jobs}
        missing_labels = VALID_SPECIALTY_VALUES - present
        assert not missing_labels, (
            f"The following specialty labels have no fixture entry: {missing_labels}. "
            "Add entries to ensure full classifier coverage."
        )


# ---------------------------------------------------------------------------
# AC-4: knowledge_bank_fixture.md structure
# ---------------------------------------------------------------------------

class TestKnowledgeBankFixture:
    def _load_lines(self) -> list[str]:
        with open(KB_FIXTURE, encoding="utf-8") as f:
            return f.readlines()

    def test_at_least_3_h2_sections(self):
        lines = self._load_lines()
        h2_sections = [l.strip() for l in lines if re.match(r"^## [^#]", l)]
        assert len(h2_sections) >= 3, (
            f"knowledge_bank_fixture.md must have at least 3 '##' sections, "
            f"found {len(h2_sections)}: {h2_sections}"
        )

    def test_at_least_one_h3_per_h2_section(self):
        """
        For every ## employer section, there must be at least one ### subsection
        before the next ## heading (or end of file).
        """
        lines = self._load_lines()

        sections: list[tuple[str, list[str]]] = []
        current_h2 = None
        current_children: list[str] = []

        for line in lines:
            if re.match(r"^## [^#]", line):
                if current_h2 is not None:
                    sections.append((current_h2, current_children))
                current_h2 = line.strip()
                current_children = []
            elif re.match(r"^### [^#]", line) and current_h2 is not None:
                current_children.append(line.strip())

        if current_h2 is not None:
            sections.append((current_h2, current_children))

        failing = [h2 for h2, children in sections if len(children) < 1]
        assert not failing, (
            "The following ## sections have no ### subsections:\n"
            + "\n".join(f"  {h2}" for h2 in failing)
        )

    def test_word_count_is_substantial(self):
        """The fixture should be ~1,500 words per TASK-002 description."""
        with open(KB_FIXTURE, encoding="utf-8") as f:
            content = f.read()
        words = len(content.split())
        assert words >= 1000, (
            f"knowledge_bank_fixture.md has only {words} words; "
            "TASK-002 description calls for ~1,500 words."
        )


# ---------------------------------------------------------------------------
# AC-5: demo artifact matches jobs_fixtures.json
# ---------------------------------------------------------------------------

class TestDemoArtifact:
    def test_demo_artifact_exists(self):
        assert DEMO_ARTIFACT.exists(), (
            f"Demo artifact not found at {DEMO_ARTIFACT}. "
            "TASK-002 requires a copy at demos/milestone-1/TASK-002-fixtures.json."
        )

    def test_demo_artifact_is_valid_json(self):
        if not DEMO_ARTIFACT.exists():
            return  # caught by test_demo_artifact_exists
        with open(DEMO_ARTIFACT, encoding="utf-8") as f:
            data = json.load(f)
        assert isinstance(data, list), "TASK-002-fixtures.json must be a JSON array"

    def test_demo_artifact_matches_fixtures(self):
        if not DEMO_ARTIFACT.exists():
            return  # caught by test_demo_artifact_exists
        jobs = load_jobs()
        demo = load_demo()
        assert jobs == demo, (
            "demos/milestone-1/TASK-002-fixtures.json does not match "
            "tests/fixtures/jobs_fixtures.json. "
            "The demo artifact must be an exact copy."
        )
