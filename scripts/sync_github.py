import asyncio
import httpx
import logging
import sys
from pathlib import Path
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

# Add the project root to sys.path before importing from src
basedir = Path(__file__).resolve().parent.parent
sys.path.append(str(basedir))

from src.core.database import AsyncSessionLocal
from src.models.project import GitHubProject, Domain

# Setup simple logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Languages/Domains we want to seed
LANGUAGES = [
    "Python", "TypeScript", "JavaScript", "Go", "Rust", 
    "Java", "C++", "C#", "Ruby", "PHP"
]

# Provide a GitHub personal access token here to avoid tight rate limits
GITHUB_TOKEN = None

# Base Headers
HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "ProjectHub-Sync-Bot"
}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"token {GITHUB_TOKEN}"

async def fetch_repos_for_language(client: httpx.AsyncClient, language: str, limit: int = 15):
    """Fetch popular repositories for a specific programming language."""
    url = f"https://api.github.com/search/repositories?q=language:{language}&sort=stars&order=desc&per_page={limit}"
    logger.info(f"Fetching {limit} top {language} repositories...")
    
    response = await client.get(url, headers=HEADERS)
    
    if response.status_code != 200:
        logger.error(f"Failed to fetch for {language}: {response.status_code} - {response.text}")
        return []
        
    data = response.json()
    return data.get("items", [])

async def get_or_create_domain(db: AsyncSession, name: str) -> str:
    """Find a domain by name, or create it if it doesn't exist. Returns the domain ID."""
    from re import sub
    slug = sub(r'[^a-z0-9]', '', name.lower())
    
    result = await db.execute(select(Domain).where(Domain.slug == slug))
    domain = result.scalars().first()
    
    if domain:
        return domain.id
        
    domain = Domain(
        name=name,
        slug=slug,
        description=f"Curated projects for {name} development."
    )
    db.add(domain)
    await db.commit()
    await db.refresh(domain)
    logger.info(f"Created new domain: {name} ({slug})")
    
    return domain.id

async def sync_github_data():
    logger.info("Starting GitHub Projects sync process...")
    
    async with AsyncSessionLocal() as db:
        async with httpx.AsyncClient() as client:
            total_inserted = 0
            total_updated = 0
            
            for language in LANGUAGES:
                repos = await fetch_repos_for_language(client, language)
                if not repos:
                    continue
                    
                # Ensure domain exists
                domain_id = await get_or_create_domain(db, language)
                
                for repo in repos:
                    # Check if project already exists by slug (which we'll derive from full_name)
                    slug = repo["full_name"].lower().replace("/", "-")
                    
                    result = await db.execute(select(GitHubProject).where(GitHubProject.slug == slug))
                    existing_project = result.scalars().first()
                    
                    # Extract topics safely
                    topics = repo.get("topics", [])
                    if not isinstance(topics, list):
                        topics = []
                        
                    # Calculate a simulated generic difficulty based on stars/size (just for demo purposes)
                    size = repo.get("size", 0)
                    if size > 100000:
                        difficulty = "HARD"
                    elif size > 10000:
                        difficulty = "MEDIUM"
                    else:
                        difficulty = "EASY"
                        
                    # Fallback live URL from homepage if available
                    live_url = repo.get("homepage")
                    if live_url and not str(live_url).startswith("http"):
                        live_url = f"https://{live_url}"
                        
                    if existing_project:
                        # Update fields
                        existing_project.stars = repo.get("stargazers_count", existing_project.stars)
                        existing_project.forks = repo.get("forks_count", existing_project.forks)
                        existing_project.description = repo.get("description") or "No description provided."
                        existing_project.last_updated = datetime.now(timezone.utc)
                        total_updated += 1
                    else:
                        # Create new project
                        new_project = GitHubProject(
                            title=repo["name"],
                            slug=slug,
                            description=repo.get("description") or "No description provided.",
                            repo_url=repo["html_url"],
                            repo_owner=repo["owner"]["login"],
                            repo_name=repo["name"],
                            default_branch=repo.get("default_branch", "main"),
                            domain_id=domain_id,
                            stars=repo.get("stargazers_count", 0),
                            forks=repo.get("forks_count", 0),
                            language=repo.get("language"),
                            difficulty=difficulty,
                            topics=topics,
                            live_url=live_url,
                            author=repo["owner"]["login"],
                            last_updated=datetime.now(timezone.utc),
                        )
                        db.add(new_project)
                        total_inserted += 1
                        
                # Commit after each language chunk
                await db.commit()
                
                # Small delay to avoid API rate limits, especially if not using a token
                if not GITHUB_TOKEN:
                    await asyncio.sleep(2)
                    
            logger.info(f"Sync complete! Inserted: {total_inserted}, Updated: {total_updated}")

if __name__ == "__main__":
    # We must add src to path if running this as a script directly
    import sys
    from pathlib import Path
    basedir = Path(__file__).resolve().parent.parent
    sys.path.append(str(basedir))
        
    asyncio.run(sync_github_data())
