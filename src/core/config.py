from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Union, Any
from pydantic import field_validator
from sqlalchemy.engine import make_url

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

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    @field_validator("DATABASE_URL", mode="before")
    def normalize_database_url(cls, v: Any) -> Any:
        if not isinstance(v, str) or not v.strip():
            return v

        try:
            url = make_url(v.strip())
        except Exception:
            return v

        sync_postgres_drivers = {
            "postgres",
            "postgresql",
            "postgres+psycopg",
            "postgres+psycopg2",
            "postgres+pg8000",
            "postgresql+psycopg",
            "postgresql+psycopg2",
            "postgresql+pg8000",
        }

        if url.drivername in sync_postgres_drivers:
            url = url.set(drivername="postgresql+asyncpg")
        elif url.drivername == "postgres+asyncpg":
            url = url.set(drivername="postgresql+asyncpg")

        if url.drivername == "postgresql+asyncpg" and "channel_binding" in url.query:
            url = url.difference_update_query(["channel_binding"])

        return url.render_as_string(hide_password=False)

    model_config = SettingsConfigDict(
        env_file=(".env.example", ".env"), 
        env_file_encoding="utf-8", 
        case_sensitive=True
    )


settings = Settings()
