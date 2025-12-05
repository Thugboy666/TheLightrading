import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

class Settings:
    PROJECT_NAME: str = "TheLight24 v7"
    ENV: str = os.getenv("THELIGHT_ENV", "dev")

    # API
    API_HOST: str = os.getenv("THELIGHT_API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("THELIGHT_API_PORT", "8090"))

    # LLM endpoint (Aspire o remoto)
    LLM_BASE_URL: str = os.getenv("THELIGHT_LLM_BASE_URL", "http://127.0.0.1:8081")
    LLM_COMPLETION_URL: str = os.getenv(
        "THELIGHT_LLM_COMPLETION_URL", f"{LLM_BASE_URL}/completion"
    )
    LLM_MODEL: str = os.getenv("THELIGHT_LLM_MODEL", "local-7b")

    # Database
    DB_PATH: Path = BASE_DIR / "data" / "db" / "thelight_universe.db"

    # Logging
    LOG_PATH: Path = BASE_DIR / "data" / "logs" / "system.log"

settings = Settings()
