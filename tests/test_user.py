import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_user_routes_exist_and_require_auth(client: AsyncClient):
    # Check that the routes return 401 Unauthorized for unauthenticated users,
    # which proves they are wired up in the FastAPI app router.
    
    endpoints = [
        ("GET", "/api/user/progress"),
        ("GET", "/api/user/github-progress"),
        ("GET", "/api/user/activity"),
        ("GET", "/api/user/bookmarks"),
        ("GET", "/api/user/profile-stats"),
        ("PUT", "/api/user/profile"),
        ("POST", "/api/user/profile/image"),
    ]
    
    for method, url in endpoints:
        if method == "GET":
            response = await client.get(url)
        elif method == "PUT":
            response = await client.put(url, json={})
        elif method == "POST":
            # Just send empty to see if it hits the auth barrier
            response = await client.post(url, json={})
            
        assert response.status_code == 401, f"Expected 401 for {method} {url}, got {response.status_code}"

@pytest.mark.asyncio
async def test_openapi_has_user_routes(client: AsyncClient):
    response = await client.get("/api/openapi.json")
    assert response.status_code == 200
    data = response.json()
    
    paths = data.get("paths", {})
    assert "/api/user/profile" in paths
    assert "/api/user/profile/image" in paths
    assert "/api/user/bookmarks" in paths
    assert "/api/user/activity" in paths
    assert "/api/user/profile-stats" in paths
