import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_read_main(client: AsyncClient):
    response = await client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to Project Hub API!"}

@pytest.mark.asyncio
async def test_openapi_schema_generated(client: AsyncClient):
    response = await client.get("/api/openapi.json")
    assert response.status_code == 200
    data = response.json()
    assert "openapi" in data
    assert data["info"]["title"] == "Project Hub API"
    # Ensure generated OpenAPI has the routes we built
    assert "/api/auth/register" in data["paths"]
    assert "/api/projects/" in data["paths"]
