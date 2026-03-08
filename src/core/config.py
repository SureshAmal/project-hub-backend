from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Union, Any
from pydantic import field_validator
from sqlalchemy.engine import make_url


def _normalize_postgres_url(value: Any, *, async_mode: bool) -> Any:
    if not isinstance(value, str) or not value.strip():
        return value

    try:
        url = make_url(value.strip())
    except Exception:
        return value

    if async_mode:
        async_driver_map = {
            "postgres": "postgresql+asyncpg",
            "postgresql": "postgresql+asyncpg",
            "postgres+psycopg": "postgresql+asyncpg",
            "postgres+psycopg2": "postgresql+asyncpg",
            "postgres+pg8000": "postgresql+asyncpg",
            "postgresql+psycopg": "postgresql+asyncpg",
            "postgresql+psycopg2": "postgresql+asyncpg",
            "postgresql+pg8000": "postgresql+asyncpg",
            "postgres+asyncpg": "postgresql+asyncpg",
        }
        drivername = async_driver_map.get(url.drivername)
        if drivername:
            url = url.set(drivername=drivername)
    else:
        sync_driver_map = {
            "postgres": "postgresql+psycopg2",
            "postgresql": "postgresql+psycopg2",
            "postgres+asyncpg": "postgresql+psycopg2",
            "postgresql+asyncpg": "postgresql+psycopg2",
            "postgres+psycopg": "postgresql+psycopg2",
            "postgresql+psycopg": "postgresql+psycopg2",
        }
        drivername = sync_driver_map.get(url.drivername)
        if drivername:
            url = url.set(drivername=drivername)

    if "channel_binding" in url.query:
        url = url.difference_update_query(["channel_binding"])

    return url.render_as_string(hide_password=False)

class Settings(BaseSettings):
    """
    Core application settings powered by Pydantic.
    Loads variables from `.env` file or environment.
    """
    PROJECT_NAME: str = "Project Hub API"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api"

    # Database
    DATABASE_URL: str = ""
    ALEMBIC_DATABASE_URL: Union[str, None] = None

    # Security
    SECRET_KEY: str = ""
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 7 days

    # External APIs
    GITHUB_TOKEN: Union[str, None] = None
    CEREBRAS_API_KEY: Union[str, None] = None
    GEMINI_API_KEY: Union[str, None] = None

    # CORS
    BACKEND_CORS_ORIGINS: Union[List[str], str] = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
    ]
    FRONTEND_URL: Union[str, None] = None
    BACKEND_CORS_ORIGIN_REGEX: Union[str, None] = None

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    @field_validator("FRONTEND_URL", "BACKEND_CORS_ORIGIN_REGEX", mode="before")
    def normalize_optional_url_like_values(cls, v: Any) -> Any:
        if not isinstance(v, str):
            return v

        normalized = v.strip()
        return normalized or None

    @field_validator("DATABASE_URL", mode="before")
    def normalize_database_url(cls, v: Any) -> Any:
        return _normalize_postgres_url(v, async_mode=True)

    @field_validator("ALEMBIC_DATABASE_URL", mode="before")
    def normalize_alembic_database_url(cls, v: Any) -> Any:
        return _normalize_postgres_url(v, async_mode=False)

    @property
    def async_database_url(self) -> str:
        return str(self.DATABASE_URL)

    @property
    def alembic_database_url(self) -> str:
        if self.ALEMBIC_DATABASE_URL:
            return str(self.ALEMBIC_DATABASE_URL)
        return str(_normalize_postgres_url(self.DATABASE_URL, async_mode=False))

    @property
    def cors_origins(self) -> List[str]:
        raw_origins = self.BACKEND_CORS_ORIGINS
        origins = raw_origins if isinstance(raw_origins, list) else [raw_origins]
        normalized = [origin.rstrip("/") for origin in origins if isinstance(origin, str) and origin.strip()]

        if self.FRONTEND_URL:
            normalized.append(self.FRONTEND_URL.rstrip("/"))

        # Preserve order while removing duplicates.
        return list(dict.fromkeys(normalized))

    model_config = SettingsConfigDict(
        env_file=(".env.example", ".env"), 
        env_file_encoding="utf-8", 
        case_sensitive=True
    )


settings = Settings()
