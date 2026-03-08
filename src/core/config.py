from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Union, Any
from pydantic import field_validator

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
    model_config = SettingsConfigDict(
        env_file=(".env.example", ".env"), 
        env_file_encoding="utf-8", 
        case_sensitive=True
    )


settings = Settings()
