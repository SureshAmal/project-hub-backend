import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from src.main import app
from src.core.database import get_db
from src.models.base import Base

# Use an in-memory SQLite database for tests (using aiosqlite, we'll configure a slightly different engine)
# However PostgreSQL syntax in schemas (ARRAY) makes SQLite difficult.
# Assuming tests run against a test postgres container or we mock it. For simplicity, we'll mock the get_db dependency.

@pytest_asyncio.fixture()
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
