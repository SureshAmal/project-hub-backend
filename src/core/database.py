from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.engine import make_url
from src.core.config import settings


def _build_async_engine_config() -> tuple[str, dict[str, str]]:
    url = make_url(settings.async_database_url)
    connect_args: dict[str, str] = {}

    ssl_value = url.query.get("ssl") or url.query.get("sslmode")
    if isinstance(ssl_value, (list, tuple)):
        ssl_value = ssl_value[0] if ssl_value else None
    if ssl_value:
        connect_args["ssl"] = str(ssl_value)

    query_keys_to_remove = [key for key in ("ssl", "sslmode", "channel_binding") if key in url.query]
    if query_keys_to_remove:
        url = url.difference_update_query(query_keys_to_remove)

    return url.render_as_string(hide_password=False), connect_args


async_database_url, async_connect_args = _build_async_engine_config()

# Async engine for PostgreSQL
engine = create_async_engine(
    async_database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    connect_args=async_connect_args,
)

# Async session factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency to yield an async database session per request.
    Closes the session after the request is finished.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
