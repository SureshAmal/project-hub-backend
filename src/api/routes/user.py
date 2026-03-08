import os
import shutil
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query, UploadFile, File, HTTPException, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import List

from src.core.database import get_db
from src.core.config import settings
from src.api.dependencies import get_current_user
from src.models.project import Project, GitHubProject
from src.models.user import User
from src.models.tracking import ProjectProgress, GitHubProjectProgress, Bookmark, Notification
from src.schemas.tracking import ProjectProgressResponse, GitHubProjectProgressResponse, ProjectProgressBase, GitHubProjectProgressBase

router = APIRouter()

@router.get("/progress", response_model=List[ProjectProgressResponse])
async def get_user_progress(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get the authenticated user's standard project progress.
    """
    stmt = (
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(ProjectProgress.user_id == current_user.id)
    )
    result = await db.execute(stmt)
    return result.scalars().all()


@router.get("/progress/{project_id}", response_model=ProjectProgressResponse)
async def get_single_project_progress(
    project_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(
            ProjectProgress.user_id == current_user.id,
            ProjectProgress.project_id == project_id,
        )
    )
    result = await db.execute(stmt)
    progress = result.scalar_one_or_none()

    if not progress:
        progress = ProjectProgress(
            user_id=current_user.id,
            project_id=project_id,
            status="NOT_STARTED",
            time_spent=0,
            is_running=False,
        )
        db.add(progress)
        await db.commit()

    refreshed = await db.execute(
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(ProjectProgress.user_id == current_user.id, ProjectProgress.project_id == project_id)
    )
    return refreshed.scalar_one()


@router.put("/progress/{project_id}", response_model=ProjectProgressResponse)
async def update_project_progress(
    project_id: str,
    data: ProjectProgressBase,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(
            ProjectProgress.user_id == current_user.id,
            ProjectProgress.project_id == project_id,
        )
    )
    result = await db.execute(stmt)
    progress = result.scalar_one_or_none()

    if not progress:
        progress = ProjectProgress(user_id=current_user.id, project_id=project_id)
        db.add(progress)

    progress.status = data.status
    if data.time_spent is not None:
        progress.time_spent = data.time_spent
    if data.is_running is not None:
        progress.is_running = data.is_running
    if data.notes is not None:
        progress.notes = data.notes

    now = datetime.now(timezone.utc)
    if data.is_running is True:
        progress.last_timer_start = now
    elif data.is_running is False:
        progress.last_timer_start = None

    if data.status == "IN_PROGRESS" and not progress.started_at:
        progress.started_at = now
    if data.status == "COMPLETED":
        progress.completed_at = progress.completed_at or now
        progress.is_running = False
        progress.last_timer_start = None
    elif data.status != "COMPLETED":
        progress.completed_at = None

    await db.commit()

    refreshed = await db.execute(
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(ProjectProgress.user_id == current_user.id, ProjectProgress.project_id == project_id)
    )
    return refreshed.scalar_one()

@router.get("/github-progress", response_model=List[GitHubProjectProgressResponse])
async def get_github_progress(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get the authenticated user's GitHub project progress.
    """
    stmt = (
        select(GitHubProjectProgress)
        .options(selectinload(GitHubProjectProgress.github_project).selectinload(GitHubProject.domain))
        .where(GitHubProjectProgress.user_id == current_user.id)
    )
    result = await db.execute(stmt)
    return result.scalars().all()


@router.get("/github-progress/{project_id}", response_model=GitHubProjectProgressResponse)
async def get_single_github_progress(
    project_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get the authenticated user's progress for a specific GitHub project.
    Creates a NOT_STARTED record if none exists.
    """
    stmt = select(GitHubProjectProgress).options(
        selectinload(GitHubProjectProgress.github_project).selectinload(GitHubProject.domain)
    ).where(
        GitHubProjectProgress.user_id == current_user.id,
        GitHubProjectProgress.github_project_id == project_id,
    )
    result = await db.execute(stmt)
    progress = result.scalar_one_or_none()

    if not progress:
        progress = GitHubProjectProgress(
            user_id=current_user.id,
            github_project_id=project_id,
            status="NOT_STARTED",
            time_spent=0,
        )
        db.add(progress)
        await db.commit()
        await db.refresh(progress)

    refreshed = await db.execute(
        select(GitHubProjectProgress).options(
            selectinload(GitHubProjectProgress.github_project).selectinload(GitHubProject.domain)
        ).where(GitHubProjectProgress.id == progress.id)
    )
    return refreshed.scalar_one()


@router.put("/github-progress/{project_id}", response_model=GitHubProjectProgressResponse)
async def update_github_progress(
    project_id: str,
    data: GitHubProjectProgressBase,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Update the authenticated user's progress for a specific GitHub project.
    Creates a new record if none exists. Auto-manages started_at/completed_at timestamps.
    """
    stmt = select(GitHubProjectProgress).options(
        selectinload(GitHubProjectProgress.github_project).selectinload(GitHubProject.domain)
    ).where(
        GitHubProjectProgress.user_id == current_user.id,
        GitHubProjectProgress.github_project_id == project_id,
    )
    result = await db.execute(stmt)
    progress = result.scalar_one_or_none()

    if not progress:
        progress = GitHubProjectProgress(
            user_id=current_user.id,
            github_project_id=project_id,
        )
        db.add(progress)

    # Update fields from request body
    progress.status = data.status
    if data.time_spent is not None:
        progress.time_spent = data.time_spent
    if data.notes is not None:
        progress.notes = data.notes
    if data.checklist is not None:
        progress.checklist = data.checklist

    # Auto-manage timestamps
    if data.status == "IN_PROGRESS" and not progress.started_at:
        progress.started_at = datetime.now(timezone.utc)
    if data.status == "COMPLETED" and not progress.completed_at:
        progress.completed_at = datetime.now(timezone.utc)

    await db.commit()

    refreshed = await db.execute(
        select(GitHubProjectProgress).options(
            selectinload(GitHubProjectProgress.github_project).selectinload(GitHubProject.domain)
        ).where(GitHubProjectProgress.id == progress.id)
    )
    return refreshed.scalar_one()

@router.put("/profile")
async def update_profile(
    data: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Update the authenticated user's profile information.
    """
    if "firstName" in data:
        current_user.first_name = data["firstName"]
    if "lastName" in data:
        current_user.last_name = data["lastName"]
    if "headline" in data:
        current_user.headline = data["headline"]
    if "bio" in data:
        current_user.bio = data["bio"]
    if "location" in data:
        current_user.location = data["location"]
    if "githubUrl" in data:
        current_user.github_url = data["githubUrl"]
    if "portfolioUrl" in data:
        current_user.portfolio_url = data["portfolioUrl"]

    await db.commit()
    await db.refresh(current_user)
    
    return current_user

@router.post("/profile/image")
async def upload_profile_image(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Upload a new profile image.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded")
        
    file_ext = file.filename.split(".")[-1]
    new_filename = f"{current_user.id}.{file_ext}"
    file_path = f"uploads/profiles/{new_filename}"
    
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    # Make sure this matches the static mounting path in main.py
    image_url = f"{settings.API_V1_STR.replace('/api', '')}/uploads/profiles/{new_filename}"
    current_user.profile_image = image_url
    await db.commit()
    await db.refresh(current_user)
    
    return {"message": "Profile image updated successfully", "profileImage": current_user.profile_image}

@router.get("/profile-stats")
async def get_profile_stats(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get aggregated profile statistics.
    """
    # Count bookmarks
    bm_stmt = select(Bookmark).where(Bookmark.user_id == current_user.id)
    bm_result = await db.execute(bm_stmt)
    bookmarks_count = len(bm_result.scalars().all())
    
    # Count normal completed projects
    prog_stmt = select(ProjectProgress).where(
        ProjectProgress.user_id == current_user.id,
        ProjectProgress.status == "COMPLETED"
    )
    prog_result = await db.execute(prog_stmt)
    completed_projects = len(prog_result.scalars().all())

    # Count GH completed projects
    gh_prog_stmt = select(GitHubProjectProgress).where(
        GitHubProjectProgress.user_id == current_user.id,
        GitHubProjectProgress.status == "COMPLETED"
    )
    gh_prog_result = await db.execute(gh_prog_stmt)
    gh_completed_projects = len(gh_prog_result.scalars().all())
    
    return {
        "projects_completed": completed_projects + gh_completed_projects,
        "bookmarks_count": bookmarks_count,
        "xp": current_user.points,
        "contributions": 0 # Default for now based on activities
    }

@router.get("/activity")
async def get_user_activity(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get the authenticated user's recent activity feed (mocked/simplified for now).
    """
    stmt = select(Notification).where(Notification.user_id == current_user.id).order_by(Notification.created_at.desc()).limit(10)
    result = await db.execute(stmt)
    notifications = result.scalars().all()
    return notifications

@router.get("/bookmarks")
async def get_user_bookmarks(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all bookmarks for the authenticated user with nested project details.
    """
    stmt = (
        select(Bookmark)
        .options(
            selectinload(Bookmark.project).selectinload(Project.domain),
            selectinload(Bookmark.github_project).selectinload(GitHubProject.domain),
        )
        .where(Bookmark.user_id == current_user.id)
        .order_by(Bookmark.created_at.desc())
    )
    result = await db.execute(stmt)
    bookmarks = result.scalars().all()
    
    # We need to map the snake_case relationship (github_project) to camelCase (githubProject) 
    # for the frontend before returning, or rely on FastAPI's model dumping if configured.
    # Since we are returning raw dicts/objects, let's format it.
    formatted_bookmarks = []
    for b in bookmarks:
        formatted_bookmarks.append({
            "id": b.id,
            "project": b.project,
            "githubProject": b.github_project,
            "createdAt": b.created_at
        })
        
    return formatted_bookmarks

@router.get("/bookmarks/{project_id}/check")
async def check_bookmark(
    project_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Check if a specific project is bookmarked.
    Checks both GitHub projects and standard projects.
    """
    stmt = select(Bookmark).where(
        Bookmark.user_id == current_user.id,
        (Bookmark.project_id == project_id) | (Bookmark.github_project_id == project_id)
    )
    result = await db.execute(stmt)
    bookmark = result.scalars().first()
    return {"bookmarked": bookmark is not None}

@router.post("/bookmarks/batch-check")
async def batch_check_bookmarks(
    data: dict, 
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Check bookmark status for multiple projects at once.
    Expects {"projectIds": ["id1", "id2"]}
    """
    project_ids = data.get("projectIds", [])
    if not project_ids:
        return {"bookmarks": {}}
        
    stmt = select(Bookmark).where(
        Bookmark.user_id == current_user.id,
        (Bookmark.project_id.in_(project_ids)) | (Bookmark.github_project_id.in_(project_ids))
    )
    result = await db.execute(stmt)
    bookmarks = result.scalars().all()
    
    bookmark_map = {}
    for b in bookmarks:
        if b.project_id:
            bookmark_map[b.project_id] = True
        if b.github_project_id:
            bookmark_map[b.github_project_id] = True
            
    # Include un-bookmarked ones as false
    return {"bookmarks": {pid: bookmark_map.get(pid, False) for pid in project_ids}}

@router.post("/bookmarks/{project_id}")
async def toggle_bookmark(
    project_id: str,
    data: dict = Body(default={}), # Allows specifying type, e.g {"type": "github"} or {"type": "standard"}
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Toggle bookmark status for a project.
    """
    project_type = data.get("type")

    if project_type:
        is_github = project_type.lower() == "github"
    else:
        regular_result = await db.execute(
            select(Project.id).where(Project.id == project_id, Project.deleted_at.is_(None))
        )
        is_github = regular_result.scalar_one_or_none() is None
    
    stmt = select(Bookmark).where(
        Bookmark.user_id == current_user.id,
        Bookmark.github_project_id == project_id if is_github else Bookmark.project_id == project_id
    )
    result = await db.execute(stmt)
    existing_bookmark = result.scalars().first()

    if existing_bookmark:
        await db.delete(existing_bookmark)
        await db.commit()
        return {"bookmarked": False}
    else:
        new_bookmark = Bookmark(user_id=current_user.id)
        if is_github:
            new_bookmark.github_project_id = project_id
        else:
            new_bookmark.project_id = project_id
            
        db.add(new_bookmark)
        await db.commit()
        return {"bookmarked": True}
