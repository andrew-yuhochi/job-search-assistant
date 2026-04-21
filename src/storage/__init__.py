# Storage package — exposes get_engine for app startup and service imports.

from src.storage.db import get_engine

__all__ = ["get_engine"]
