"""
Routes for GitHub projects with pagination support.
The frontend expects responses in {projects: [], pagination: {}} format.
"""
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, func, not_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from src.core.database import get_db
from src.core.config import settings
from src.models.project import GitHubProject, Domain
from src.schemas.project import (
    GitHubProjectListResponse, GitHubProjectResponse
)
from src.services.ai_service import generate_project_details
from src.services.github_project_scraper import sync_regular_project_from_github_project
import httpx

router = APIRouter()

SUPPORTED_DOMAIN_SLUGS = {
    "web-development",
    "artificial-intelligence",
    "machine-learning",
    "data-science",
    "cybersecurity",
}

CURATION_EXCLUSION_PATTERNS = [
    "%awesome%",
    "%free-programming-books%",
    "%free programming books%",
    "%tutorial%",
    "%course%",
    "%boilerplate%",
    "%template%",
    "%cheatsheet%",
    "%cookbook%",
    "%reference%",
    "%roadmap%",
    "%snippets%",
    "%ebook%",
    "%books%",
]


def apply_curation_filters(stmt, count_stmt=None):
    excluded = [
        or_(
            GitHubProject.title.ilike(pattern),
            GitHubProject.description.ilike(pattern),
            GitHubProject.slug.ilike(pattern),
        )
        for pattern in CURATION_EXCLUSION_PATTERNS
    ]

    domain_filter = Domain.slug.in_(SUPPORTED_DOMAIN_SLUGS)
    base_filters = [
        GitHubProject.is_active.is_(True),
        GitHubProject.project_type == "PROJECT",
        not_(or_(*excluded)),
        domain_filter,
    ]

    stmt = stmt.join(Domain).where(and_(*base_filters))
    if count_stmt is not None:
        count_stmt = count_stmt.join(Domain).where(and_(*base_filters))
        return stmt, count_stmt
    return stmt


def resolve_sort_column(sort_by: Optional[str]):
    sort_mapping = {
        "createdAt": GitHubProject.created_at,
        "created_at": GitHubProject.created_at,
        "lastUpdated": GitHubProject.last_updated,
        "last_updated": GitHubProject.last_updated,
        "downloadCount": GitHubProject.download_count,
        "download_count": GitHubProject.download_count,
        "stars": GitHubProject.stars,
        "forks": GitHubProject.forks,
        "language": GitHubProject.language,
        "difficulty": GitHubProject.difficulty,
    }
    return sort_mapping.get(sort_by or "stars", GitHubProject.stars)


def apply_difficulty_filter(stmt, count_stmt, difficulty: Optional[str]):
    if not difficulty:
        return stmt, count_stmt

    normalized = difficulty.upper()
    if normalized == "ADVANCED":
        difficulty_values = ["HARD", "ADVANCED", "EXPERT"]
        stmt = stmt.where(GitHubProject.difficulty.in_(difficulty_values))
        count_stmt = count_stmt.where(GitHubProject.difficulty.in_(difficulty_values))
        return stmt, count_stmt

    stmt = stmt.where(GitHubProject.difficulty == normalized)
    count_stmt = count_stmt.where(GitHubProject.difficulty == normalized)
    return stmt, count_stmt

async def proxy_github_search(
    db: AsyncSession,
    search: Optional[str],
    domain_id: Optional[str],
    difficulty: Optional[str],
    page: int,
    limit: int
):
    """Proxy query to GitHub API, upsert results, and return them. Returns (projects, total_count) or (None, 0) on failure/rate limit."""
    query_parts = []
    if search:
        query_parts.append(search.replace(" ", "+"))
        
    domain_name = None
    if domain_id:
        domain_result = await db.execute(select(Domain).where(Domain.id == domain_id))
        domain = domain_result.scalars().first()
        if domain:
            domain_name = domain.name
            query_parts.append(f"language:{domain.name}")
            
    if not query_parts:
        query_parts.append("stars:>500")
        
    gh_query = "+".join(query_parts)
    
    # GitHub search API max limit: 1000 total accessible results
    if page * limit > 1000:
        page = max(1, 1000 // limit)
        
    url = f"https://api.github.com/search/repositories?q={gh_query}&sort=stars&order=desc&page={page}&per_page={limit}"
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "ProjectHub-Sync-Bot"
    }
    if settings.GITHUB_TOKEN:
        headers["Authorization"] = f"token {settings.GITHUB_TOKEN}"
        
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                total_count = min(data.get("total_count", 0), 1000)
                
                # Ensure we have a domain to assign to
                fallback_domain_result = await db.execute(select(Domain).where(Domain.slug == "dynamic"))
                fallback_domain = fallback_domain_result.scalars().first()
                if not fallback_domain:
                    fallback_domain = Domain(name="Dynamic Search", slug="dynamic", description="Dynamically cached projects")
                    db.add(fallback_domain)
                    await db.flush()
                
                domain_id_to_use = domain_id if domain_id else fallback_domain.id
                
                saved_projects = []
                for repo in items:
                    slug = repo["full_name"].lower().replace("/", "-")
                    result = await db.execute(
                        select(GitHubProject)
                        .options(selectinload(GitHubProject.domain))
                        .where(GitHubProject.slug == slug)
                    )
                    existing = result.scalars().first()
                    
                    diff = "MEDIUM"
                    size = repo.get("size", 0)
                    if size > 100000: diff = "HARD"
                    elif size < 10000: diff = "EASY"
                    
                    live_url = repo.get("homepage")
                    if live_url and not str(live_url).startswith("http"):
                        live_url = f"https://{live_url}"
                        
                    topics = repo.get("topics", [])
                    library_keywords = {"library", "framework", "sdk", "api-wrapper", "toolkit", "plugin", "module", "package", "backend-framework", "frontend-framework"}
                    project_type = "LIBRARY" if any(kw in topics for kw in library_keywords) else "PROJECT"
                        
                    if not existing:
                        existing = GitHubProject(
                            title=repo["name"],
                            slug=slug,
                            description=repo.get("description") or "No description provided.",
                            repo_url=repo["html_url"],
                            repo_owner=repo["owner"]["login"],
                            repo_name=repo["name"],
                            default_branch=repo.get("default_branch", "main"),
                            domain_id=domain_id_to_use,
                            stars=repo.get("stargazers_count", 0),
                            forks=repo.get("forks_count", 0),
                            language=repo.get("language") or domain_name,
                            difficulty=diff,
                            topics=topics,
                            project_type=project_type,
                            live_url=live_url,
                            author=repo["owner"]["login"],
                            last_updated=datetime.now(timezone.utc)
                        )
                        db.add(existing)
                        await db.flush()
                    else:
                        existing.title = repo["name"]
                        existing.description = repo.get("description") or existing.description
                        existing.repo_url = repo["html_url"]
                        existing.repo_owner = repo["owner"]["login"]
                        existing.repo_name = repo["name"]
                        existing.default_branch = repo.get("default_branch", existing.default_branch)
                        existing.domain_id = domain_id_to_use
                        existing.language = repo.get("language") or domain_name
                        existing.difficulty = diff
                        existing.topics = topics
                        existing.live_url = live_url
                        existing.author = repo["owner"]["login"]
                        existing.last_updated = datetime.now(timezone.utc)
                        existing.stars = repo.get("stargazers_count", existing.stars)
                        existing.forks = repo.get("forks_count", existing.forks)
                        existing.project_type = project_type

                    await sync_regular_project_from_github_project(db, existing)
                        
                    saved_projects.append(existing)
                    
                await db.commit()
                for p in saved_projects:
                    await db.refresh(p, attribute_names=["domain"])
                
                if difficulty:
                    saved_projects = [p for p in saved_projects if p.difficulty == difficulty.upper()]
                    
                return saved_projects, total_count
        except Exception as e:
            print(f"GitHub Proxy Error: {e}")
            pass
            
    return None, 0

@router.get("/languages", response_model=list[str])
async def get_github_project_languages(db: AsyncSession = Depends(get_db)):
    """
    Get all unique programming languages from cached GitHub projects.
    """
    result = await db.execute(
        select(GitHubProject.language)
        .where(GitHubProject.language != None)
        .distinct()
    )
    languages = result.scalars().all()
    return sorted(list(set(lang for lang in languages if lang)))


@router.get("", response_model=GitHubProjectListResponse)
async def get_github_projects(
    page: int = Query(1, ge=1),
    limit: int = Query(24, ge=1, le=100),
    domainId: Optional[str] = None,
    difficulty: Optional[str] = None,
    search: Optional[str] = None,
    sortBy: Optional[str] = "created_at",
    order: Optional[str] = "desc",
    qaStatus: Optional[str] = None,
    projectType: Optional[str] = None,
    live: bool = Query(False, description="Set true to query GitHub live instead of the curated local catalog."),
    db: AsyncSession = Depends(get_db),
):
    """
    Get paginated list of GitHub projects with optional filtering.
    Returns {projects: [], pagination: {total, page, limit, totalPages}}.
    """
    # Default to the curated local catalog. Live GitHub proxy is opt-in only because
    # raw GitHub search returns unwanted entries like awesome lists, books, and tutorials.
    if live and not qaStatus:
        gh_projects, gh_total = await proxy_github_search(
            db=db, search=search, domain_id=domainId, difficulty=difficulty, page=page, limit=limit
        )
        if gh_projects is not None:
            if projectType:
                gh_projects = [p for p in gh_projects if p.project_type == projectType.upper()]
                # Adjust total, though GitHub's total might be higher, we only have this page's filtered count easily available
                # In a real app we'd pass projectType into proxy_github_search, but doing it in-memory here for the current page works for now
                
            total_pages = (gh_total + limit - 1) // limit if gh_total > 0 else 0
            return {
                "projects": gh_projects,
                "pagination": {
                    "total": gh_total,
                    "page": page,
                    "limit": limit,
                    "total_pages": total_pages,
                }
            }

    # === 2. Fallback to Local DB (Rate Limit or QA Status Filter) ===
    stmt = select(GitHubProject).options(selectinload(GitHubProject.domain))
    count_stmt = select(func.count()).select_from(GitHubProject)
    stmt, count_stmt = apply_curation_filters(stmt, count_stmt)

    if domainId:
        stmt = stmt.where(GitHubProject.domain_id == domainId)
        count_stmt = count_stmt.where(GitHubProject.domain_id == domainId)

    stmt, count_stmt = apply_difficulty_filter(stmt, count_stmt, difficulty)

    if qaStatus:
        stmt = stmt.where(GitHubProject.qa_status == qaStatus)
        count_stmt = count_stmt.where(GitHubProject.qa_status == qaStatus)

    if projectType:
        stmt = stmt.where(GitHubProject.project_type == projectType.upper())
        count_stmt = count_stmt.where(GitHubProject.project_type == projectType.upper())

    if search:
        search_term = f"%{search}%"
        search_filter = (
            GitHubProject.title.ilike(search_term)
            | GitHubProject.description.ilike(search_term)
        )
        stmt = stmt.where(search_filter)
        count_stmt = count_stmt.where(search_filter)

    total_result = await db.execute(count_stmt)
    total = total_result.scalar() or 0

    # Apply sorting
    sort_column = resolve_sort_column(sortBy)
    if order == "asc":
        stmt = stmt.order_by(sort_column.asc())
    else:
        stmt = stmt.order_by(sort_column.desc())

    # Apply pagination
    offset = (page - 1) * limit
    stmt = stmt.offset(offset).limit(limit)

    result = await db.execute(stmt)
    projects = result.scalars().all()

    total_pages = (total + limit - 1) // limit if total > 0 else 0

    return {
        "projects": projects,
        "pagination": {
            "total": total,
            "page": page,
            "limit": limit,
            "totalPages": total_pages,
        },
    }


@router.get("/search")
async def search_github_projects(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    live: bool = Query(False, description="Set true to query GitHub live instead of the curated local catalog."),
    db: AsyncSession = Depends(get_db),
):
    """
    Search GitHub projects by title or description.
    Returns {projects: [], pagination: {}}.
    """
    search_term = f"%{q}%"
    search_filter = (
        GitHubProject.title.ilike(search_term)
        | GitHubProject.description.ilike(search_term)
    )

    if live:
        gh_projects, gh_total = await proxy_github_search(
            db=db, search=q, domain_id=None, difficulty=None, page=page, limit=limit
        )
        if gh_projects is not None:
            total_pages = (gh_total + limit - 1) // limit if gh_total > 0 else 0
            return {
                "projects": gh_projects,
                "pagination": {
                    "total": gh_total,
                    "page": page,
                    "limit": limit,
                    "totalPages": total_pages,
                }
            }

    # Fallback to Local Search
    count_stmt = select(func.count()).select_from(GitHubProject)
    stmt = select(GitHubProject).options(selectinload(GitHubProject.domain))
    stmt, count_stmt = apply_curation_filters(stmt, count_stmt)
    count_stmt = count_stmt.where(search_filter)
    total_result = await db.execute(count_stmt)
    total = total_result.scalar() or 0

    offset = (page - 1) * limit
    stmt = stmt.where(search_filter).offset(offset).limit(limit)
    result = await db.execute(stmt)
    projects = result.scalars().all()

    total_pages = (total + limit - 1) // limit if total > 0 else 0

    return {
        "projects": projects,
        "pagination": {
            "total": total,
            "page": page,
            "limit": limit,
            "totalPages": total_pages,
        },
    }


@router.get("/domain/{domain_slug}", response_model=GitHubProjectListResponse)
async def get_github_projects_by_domain(
    domain_slug: str,
    page: int = Query(1, ge=1),
    limit: int = Query(200, ge=1, le=500),
    difficulty: Optional[str] = None,
    language: Optional[str] = None,
    search: Optional[str] = None,
    sortBy: Optional[str] = "stars",
    order: Optional[str] = "desc",
    db: AsyncSession = Depends(get_db),
):
    """
    Get GitHub projects filtered by domain slug.
    """
    # First find the domain
    domain_result = await db.execute(select(Domain).where(Domain.slug == domain_slug))
    domain = domain_result.scalars().first()
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
    if domain.slug not in SUPPORTED_DOMAIN_SLUGS:
        raise HTTPException(status_code=404, detail="Domain not supported")

    count_stmt = select(func.count()).select_from(GitHubProject)
    stmt = select(GitHubProject).options(selectinload(GitHubProject.domain))
    stmt, count_stmt = apply_curation_filters(stmt, count_stmt)
    stmt = stmt.where(GitHubProject.domain_id == domain.id)
    count_stmt = count_stmt.where(GitHubProject.domain_id == domain.id)

    stmt, count_stmt = apply_difficulty_filter(stmt, count_stmt, difficulty)

    if language:
        stmt = stmt.where(GitHubProject.language == language)
        count_stmt = count_stmt.where(GitHubProject.language == language)

    if search:
        search_term = f"%{search}%"
        search_filter = (
            GitHubProject.title.ilike(search_term)
            | GitHubProject.description.ilike(search_term)
        )
        stmt = stmt.where(search_filter)
        count_stmt = count_stmt.where(search_filter)

    total_result = await db.execute(count_stmt)
    total = total_result.scalar() or 0

    offset = (page - 1) * limit
    sort_column = resolve_sort_column(sortBy)
    if order == "asc":
        stmt = stmt.order_by(sort_column.asc())
    else:
        stmt = stmt.order_by(sort_column.desc())

    stmt = stmt.offset(offset).limit(limit)
    result = await db.execute(stmt)
    projects = result.scalars().all()

    total_pages = (total + limit - 1) // limit if total > 0 else 0

    return {
        "projects": projects,
        "pagination": {
            "total": total,
            "page": page,
            "limit": limit,
            "totalPages": total_pages,
        },
    }


import time

# Very simple in-memory rate limiter for AI queries
# In a real production app, use Redis.
ai_rate_limit = {
    "tokens": 10,
    "last_refill": time.time()
}

def check_ai_rate_limit() -> bool:
    global ai_rate_limit
    now = time.time()
    
    # Refill tokens (10 tokens per 60 seconds)
    if now - ai_rate_limit["last_refill"] > 60:
        ai_rate_limit["tokens"] = 10
        ai_rate_limit["last_refill"] = now
        
    if ai_rate_limit["tokens"] > 0:
        ai_rate_limit["tokens"] -= 1
        return True
    return False

@router.get("/{project_id}", response_model=GitHubProjectResponse)
async def get_github_project_by_id(
    project_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get a single GitHub project by its ID. 
    Lazily generates AI details if they are missing.
    """
    result = await db.execute(
        select(GitHubProject)
        .options(selectinload(GitHubProject.domain))
        .where(GitHubProject.id == project_id)
    )
    project = result.scalars().first()

    if not project:
        raise HTTPException(status_code=404, detail="GitHub project not found")
        
    # Lazy AI Generation: Only generate rich info when the user OPENS the project page.
    if not project.case_study or not project.problem_statement:
        # Check Rate Limit to prevent abuse/high costs
        if check_ai_rate_limit():
            ai_details = await generate_project_details(
                title=project.title,
                description=project.description,
                language=project.language,
                topics=project.topics
            )
            if ai_details:
                project.case_study = ai_details.get("case_study")
                project.problem_statement = ai_details.get("problem_statement")
                project.solution_description = ai_details.get("solution_description")
                project.prerequisites = ai_details.get("prerequisites", [])
                project.deliverables = ai_details.get("deliverables", [])
                
                if ai_details.get("sub_domain"):
                    project.sub_domain = ai_details.get("sub_domain")
                if ai_details.get("difficulty") in ["EASY", "MEDIUM", "ADVANCED", "EXPERT"]:
                    project.difficulty = ai_details.get("difficulty")
                if ai_details.get("estimated_min_time"):
                    try: project.estimated_min_time = int(ai_details.get("estimated_min_time"))
                    except: pass
                if ai_details.get("estimated_max_time"):
                    try: project.estimated_max_time = int(ai_details.get("estimated_max_time"))
                    except: pass
                if ai_details.get("language"):
                    project.language = ai_details.get("language")
                
                await db.commit()
                await db.refresh(project)
        else:
            print(f"AI Rate limit reached for generating project details {project.title}")

    return project


@router.post("/{project_id}/review")
async def review_github_project(
    project_id: str,
    data: dict,
    db: AsyncSession = Depends(get_db),
):
    """
    Submit a QA review for a GitHub project.
    """
    result = await db.execute(
        select(GitHubProject).where(GitHubProject.id == project_id)
    )
    project = result.scalars().first()

    if not project:
        raise HTTPException(status_code=404, detail="GitHub project not found")

    if "qaStatus" in data:
        project.qa_status = data["qaStatus"]
    if "qaFeedback" in data:
        project.qa_feedback = data["qaFeedback"]
    if "reviewedBy" in data:
        project.reviewed_by = data["reviewedBy"]

    from datetime import datetime, timezone
    project.reviewed_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(project)
    return project
