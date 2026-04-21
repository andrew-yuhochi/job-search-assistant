# Integration tests for TASK-004: SQLite schema + first-run bootstrap
# Validates all four acceptance criteria using a temporary DB file
# (never touches data/app.db to avoid polluting the dev environment).
#
# Run: pytest tests/integration/test_db.py -v

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EXPECTED_TABLES = {
    "users",
    "user_settings",
    "specialty_types",
    "jobs",
    "classifications",
    "duplicates",
    "knowledge_bank_documents",
    "knowledge_bank_chunks",
    "highlight_drafts",
    "signal_events",
    "scrape_runs",
}

TIER1_NAMES = {"data_scientist", "ml_engineer", "data_engineer", "data_analyst"}
TIER2_NAMES = {"analytics_engineer"}


def _get_tables(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        ).fetchall()
    return {r[0] for r in rows}


def _pragma(engine: Engine, pragma: str) -> str:
    with engine.connect() as conn:
        row = conn.execute(text(f"PRAGMA {pragma}")).fetchone()
    return str(row[0]) if row else ""


def _get_specialty_types(engine: Engine) -> list[tuple]:
    """Return (name, tier, enabled) rows ordered by tier, name."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT name, tier, enabled FROM specialty_types ORDER BY tier, name")
        ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


# ---------------------------------------------------------------------------
# Fixture: isolated engine using a temp file
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_engine(tmp_path, monkeypatch):
    """
    Patch settings.database_path to a temp file, call get_engine(),
    and yield the resulting engine. The temp directory is cleaned up
    automatically by pytest after each test.
    """
    db_file = tmp_path / "test_app.db"

    # Patch the settings object so get_engine() creates the DB at our temp path.
    # Import after patching so the module-level settings object picks up the change.
    import src.config as config_mod
    monkeypatch.setattr(config_mod.settings, "database_path", db_file)

    from src.storage.db import get_engine
    engine = get_engine()
    yield engine
    engine.dispose()


# ---------------------------------------------------------------------------
# AC-1: get_engine() creates the DB with all 11 tables
# ---------------------------------------------------------------------------

class TestAllTablesCreated:
    def test_all_11_tables_exist(self, tmp_engine):
        """AC-1: schema.sql must create exactly the 11 domain tables."""
        tables = _get_tables(tmp_engine)
        missing = EXPECTED_TABLES - tables
        assert not missing, f"Missing tables: {missing}"

    def test_no_unexpected_tables(self, tmp_engine):
        """No extra tables beyond the 11 specified in TDD §2.4."""
        tables = _get_tables(tmp_engine)
        extra = tables - EXPECTED_TABLES
        assert not extra, f"Unexpected tables: {extra}"

    def test_db_file_is_created(self, tmp_path, monkeypatch):
        """AC-1: the DB file must be written to disk at the configured path."""
        db_file = tmp_path / "subdir" / "app.db"
        import src.config as config_mod
        monkeypatch.setattr(config_mod.settings, "database_path", db_file)

        from src.storage.db import get_engine
        engine = get_engine()
        engine.dispose()

        assert db_file.exists(), "DB file was not created at the configured path"


# ---------------------------------------------------------------------------
# AC-2: PRAGMAs are set correctly
# ---------------------------------------------------------------------------

class TestPragmas:
    def test_journal_mode_is_wal(self, tmp_engine):
        """AC-2: PRAGMA journal_mode must return 'wal'."""
        mode = _pragma(tmp_engine, "journal_mode")
        assert mode == "wal", f"Expected 'wal', got '{mode}'"

    def test_foreign_keys_enabled(self, tmp_engine):
        """AC-2: PRAGMA foreign_keys must return '1'."""
        fk = _pragma(tmp_engine, "foreign_keys")
        assert fk == "1", f"Expected '1', got '{fk}'"

    def test_foreign_key_constraint_enforced(self, tmp_engine):
        """AC-2: FK enforcement must actually reject a bad reference."""
        with tmp_engine.connect() as conn:
            with pytest.raises(Exception):
                conn.execute(
                    text(
                        "INSERT INTO jobs "
                        "(job_id, user_id, source, url, url_hostname, title, title_normalized, "
                        "company, company_normalized, description, fetched_at, state, state_updated_at) "
                        "VALUES ('bad', 'nonexistent_user', 'linkedin', 'http://x.com', 'x.com', "
                        "'T', 't', 'C', 'c', 'd', '2024-01-01', 'new', '2024-01-01')"
                    )
                )
                conn.commit()


# ---------------------------------------------------------------------------
# AC-3: specialty_types seed rows — 4 Tier 1 enabled + 1 Tier 2 disabled
# ---------------------------------------------------------------------------

class TestSpecialtyTypesSeed:
    def test_exactly_5_specialty_rows(self, tmp_engine):
        """AC-3: classifier_types.yaml defines 5 rows; all must be inserted."""
        rows = _get_specialty_types(tmp_engine)
        assert len(rows) == 5, f"Expected 5 specialty_types rows, got {len(rows)}: {rows}"

    def test_4_tier1_rows_are_enabled(self, tmp_engine):
        """AC-3: 4 Tier 1 rows must all have enabled=1."""
        rows = _get_specialty_types(tmp_engine)
        tier1 = [(name, tier, enabled) for name, tier, enabled in rows if tier == 1]
        assert len(tier1) == 4, f"Expected 4 Tier 1 rows, got {len(tier1)}: {tier1}"
        disabled_tier1 = [r for r in tier1 if not r[2]]
        assert not disabled_tier1, f"Tier 1 rows must all be enabled; disabled: {disabled_tier1}"

    def test_tier1_names_match_spec(self, tmp_engine):
        """AC-3: the 4 Tier 1 names must match exactly what classifier_types.yaml defines."""
        rows = _get_specialty_types(tmp_engine)
        actual_tier1 = {name for name, tier, _ in rows if tier == 1}
        assert actual_tier1 == TIER1_NAMES, (
            f"Tier 1 name mismatch. Expected {TIER1_NAMES}, got {actual_tier1}"
        )

    def test_1_tier2_row_is_disabled(self, tmp_engine):
        """AC-3: analytics_engineer (Tier 2) must exist and be disabled."""
        rows = _get_specialty_types(tmp_engine)
        tier2 = [(name, tier, enabled) for name, tier, enabled in rows if tier == 2]
        assert len(tier2) == 1, f"Expected 1 Tier 2 row, got {len(tier2)}: {tier2}"
        name, tier, enabled = tier2[0]
        assert name == "analytics_engineer", f"Expected 'analytics_engineer', got '{name}'"
        assert not enabled, "analytics_engineer (Tier 2) must be disabled on first run"

    def test_local_user_row_seeded(self, tmp_engine):
        """AC-3 (prerequisite): 'local' user must be seeded for FK integrity."""
        with tmp_engine.connect() as conn:
            row = conn.execute(
                text("SELECT user_id FROM users WHERE user_id = 'local'")
            ).fetchone()
        assert row is not None, "Seed step must create the 'local' user row"

    def test_user_settings_row_seeded(self, tmp_engine):
        """AC-3 (prerequisite): default user_settings must be seeded for 'local' user."""
        with tmp_engine.connect() as conn:
            row = conn.execute(
                text("SELECT location_preference, salary_floor_cad FROM user_settings WHERE user_id = 'local'")
            ).fetchone()
        assert row is not None, "Seed step must create the user_settings row"
        loc_pref, salary_floor = row[0], row[1]
        # filter_defaults.yaml sets allowed: [Vancouver BC, Remote] → 'both'
        assert loc_pref == "both", f"Expected location_preference='both', got '{loc_pref}'"
        assert salary_floor == 120000, f"Expected salary_floor_cad=120000, got {salary_floor}"


# ---------------------------------------------------------------------------
# AC-4: idempotency — calling get_engine() twice must not duplicate seed rows
# ---------------------------------------------------------------------------

class TestIdempotency:
    def test_double_call_no_duplicate_users(self, tmp_path, monkeypatch):
        """AC-4: second get_engine() call must not insert a second 'local' user."""
        db_file = tmp_path / "idem.db"
        import src.config as config_mod
        monkeypatch.setattr(config_mod.settings, "database_path", db_file)

        from src.storage.db import get_engine
        e1 = get_engine()
        e2 = get_engine()

        with e2.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM users WHERE user_id = 'local'")
            ).scalar()
        e1.dispose()
        e2.dispose()
        assert count == 1, f"Expected 1 local user row after two get_engine() calls, got {count}"

    def test_double_call_no_duplicate_specialty_types(self, tmp_path, monkeypatch):
        """AC-4: second get_engine() call must not duplicate specialty_type rows."""
        db_file = tmp_path / "idem2.db"
        import src.config as config_mod
        monkeypatch.setattr(config_mod.settings, "database_path", db_file)

        from src.storage.db import get_engine
        e1 = get_engine()
        e2 = get_engine()

        with e2.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM specialty_types WHERE user_id = 'local'")
            ).scalar()
        e1.dispose()
        e2.dispose()
        assert count == 5, (
            f"Expected exactly 5 specialty_type rows after two get_engine() calls, got {count}"
        )

    def test_double_call_no_duplicate_user_settings(self, tmp_path, monkeypatch):
        """AC-4: second get_engine() call must not duplicate user_settings rows."""
        db_file = tmp_path / "idem3.db"
        import src.config as config_mod
        monkeypatch.setattr(config_mod.settings, "database_path", db_file)

        from src.storage.db import get_engine
        e1 = get_engine()
        e2 = get_engine()

        with e2.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM user_settings WHERE user_id = 'local'")
            ).scalar()
        e1.dispose()
        e2.dispose()
        assert count == 1, (
            f"Expected 1 user_settings row after two get_engine() calls, got {count}"
        )

    def test_double_call_no_error(self, tmp_path, monkeypatch):
        """AC-4: second get_engine() call must not raise any exception."""
        db_file = tmp_path / "idem4.db"
        import src.config as config_mod
        monkeypatch.setattr(config_mod.settings, "database_path", db_file)

        from src.storage.db import get_engine
        e1 = get_engine()
        e1.dispose()
        # This must not raise
        e2 = get_engine()
        e2.dispose()

    def test_pragmas_preserved_on_second_call(self, tmp_path, monkeypatch):
        """AC-4: WAL and foreign_keys pragmas must still be set after second open."""
        db_file = tmp_path / "idem5.db"
        import src.config as config_mod
        monkeypatch.setattr(config_mod.settings, "database_path", db_file)

        from src.storage.db import get_engine
        e1 = get_engine()
        e1.dispose()
        e2 = get_engine()

        assert _pragma(e2, "journal_mode") == "wal"
        assert _pragma(e2, "foreign_keys") == "1"
        e2.dispose()
