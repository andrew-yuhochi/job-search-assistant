# Database bootstrap for the Job Search Assistant.
# Creates the SQLite DB at settings.database_path, applies WAL/FK PRAGMAs,
# runs schema.sql idempotently, and seeds specialty_types + local user on first run.
# Per TDD §2.4 and TASK-004 requirements.

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import settings

logger = logging.getLogger(__name__)

# Resolve paths relative to this file's location so the module works from any cwd.
_STORAGE_DIR = Path(__file__).parent
_SCHEMA_SQL = _STORAGE_DIR / "schema.sql"
_CONFIG_DIR = _STORAGE_DIR.parent.parent / "config"
_CLASSIFIER_TYPES_YAML = _CONFIG_DIR / "classifier_types.yaml"
_FILTER_DEFAULTS_YAML = _CONFIG_DIR / "filter_defaults.yaml"


def get_engine() -> Engine:
    """
    Create (or open) the SQLite database, set PRAGMAs, apply the schema,
    and seed the local user + default specialty types on first run.

    Returns a SQLAlchemy Engine configured for single-user local use.
    Calling this multiple times on an existing DB is fully idempotent.
    """
    db_path = Path(settings.database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False, "timeout": 5},
    )

    with engine.connect() as conn:
        # --- PRAGMAs ---
        conn.execute(text("PRAGMA journal_mode=WAL"))
        conn.execute(text("PRAGMA busy_timeout=5000"))
        conn.execute(text("PRAGMA foreign_keys=ON"))

        # --- Schema (idempotent via IF NOT EXISTS) ---
        schema_sql = _SCHEMA_SQL.read_text(encoding="utf-8")
        conn.executescript = None  # SQLAlchemy connection — use raw DBAPI
        raw_conn = conn.connection
        raw_conn.executescript(schema_sql)

        conn.commit()

    # --- Seed data (all idempotent) ---
    _seed_local_user(engine)
    _seed_user_settings(engine)
    _seed_specialty_types(engine)

    logger.info("Database ready at %s", db_path)
    return engine


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_local_user(engine: Engine) -> None:
    """Insert the single 'local' user row if it doesn't exist yet."""
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT user_id FROM users WHERE user_id = 'local'")
        ).fetchone()
        if existing is None:
            conn.execute(
                text("INSERT INTO users (user_id, created_at) VALUES ('local', :ts)"),
                {"ts": datetime.now(timezone.utc).isoformat()},
            )
            conn.commit()
            logger.info("Seeded local user row")


def _seed_user_settings(engine: Engine) -> None:
    """Insert default user_settings row from filter_defaults.yaml if not present."""
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT user_id FROM user_settings WHERE user_id = 'local'")
        ).fetchone()
        if existing is not None:
            return

    defaults = _load_yaml(_FILTER_DEFAULTS_YAML)
    seniority_excluded = defaults.get("seniority", {}).get("excluded_keywords", [])
    salary_floor = defaults.get("salary", {}).get("floor_cad")
    location_cfg = defaults.get("location", {})

    # Support both old schema (allowed: [...]) and new schema (metro_locations: [...])
    allowed = location_cfg.get("allowed", [])
    metro_locations = location_cfg.get("metro_locations", [])
    remote_keywords = location_cfg.get("remote_keywords", [])

    # Map to LocationPreference enum value.
    # New config: metro_locations present + remote_keywords present → 'both'
    if metro_locations and remote_keywords:
        location_pref = "both"
    elif "Remote" in allowed and "Vancouver, BC" in allowed:
        location_pref = "both"
    elif "Remote" in allowed:
        location_pref = "remote_friendly"
    else:
        location_pref = "vancouver"

    with engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO user_settings "
                "(user_id, location_preference, salary_floor_cad, excluded_seniority_levels, updated_at) "
                "VALUES ('local', :loc, :floor, :excl, :ts)"
            ),
            {
                "loc": location_pref,
                "floor": salary_floor,
                "excl": json.dumps(seniority_excluded),
                "ts": datetime.now(timezone.utc).isoformat(),
            },
        )
        conn.commit()
        logger.info("Seeded default user_settings row")


def _seed_specialty_types(engine: Engine) -> None:
    """Seed specialty_types from classifier_types.yaml; skip rows that already exist."""
    types_cfg = _load_yaml(_CLASSIFIER_TYPES_YAML)
    specialty_types = types_cfg.get("specialty_types", [])

    # Map tier string to integer
    tier_map = {"tier1": 1, "tier2": 2, "tier3": 3}

    with engine.connect() as conn:
        for st in specialty_types:
            name = st["name"]
            tier_str = st.get("tier", "tier1")
            tier = tier_map.get(tier_str, int(tier_str.replace("tier", "")))
            enabled = 1 if st.get("enabled", True) else 0

            # Determine source: tier1 = 'seed', tier2 = 'config', tier3 = 'proposed'
            if tier == 1:
                source = "seed"
            elif tier == 2:
                source = "config"
            else:
                source = "proposed"

            existing = conn.execute(
                text(
                    "SELECT specialty_id FROM specialty_types "
                    "WHERE user_id = 'local' AND name = :name"
                ),
                {"name": name},
            ).fetchone()

            if existing is None:
                conn.execute(
                    text(
                        "INSERT INTO specialty_types "
                        "(user_id, name, description, duty_signals, tier, enabled, source, created_at) "
                        "VALUES ('local', :name, NULL, '[]', :tier, :enabled, :source, :ts)"
                    ),
                    {
                        "name": name,
                        "tier": tier,
                        "enabled": enabled,
                        "source": source,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    },
                )
                logger.debug("Seeded specialty_type: %s (tier=%s)", name, tier)

        conn.commit()
        logger.info("Specialty types seed complete (%d types configured)", len(specialty_types))


# ---------------------------------------------------------------------------
# YAML loader helper
# ---------------------------------------------------------------------------


def _load_yaml(path: Path) -> dict:
    """Load a YAML file and return its contents as a dict."""
    if not path.exists():
        logger.warning("Config file not found: %s — returning empty dict", path)
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
