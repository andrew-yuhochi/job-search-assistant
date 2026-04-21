# Application configuration loaded from environment variables and .env file.
# All settings are centralised here; import `settings` from this module.

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    anthropic_api_key: str = ""
    serpapi_api_key: str = ""
    log_level: str = "INFO"
    database_path: Path = Path("data/app.db")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
