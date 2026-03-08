from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from src.api.dependencies import get_current_user
from src.core.database import get_db
from src.models.project import Project
from src.models.user import User
from src.schemas.project import ProjectCreate, ProjectResponse, ProjectUpdate

router = APIRouter()


def apply_project_defaults(payload: ProjectCreate | ProjectUpdate) -> dict:
    data = payload.model_dump(exclude_unset=True, by_alias=False)

    if data.get("case_study") is None and data.get("industry_context"):
        data["case_study"] = data["industry_context"]
    if data.get("industry_context") is None and data.get("case_study"):
        data["industry_context"] = data["case_study"]

    if data.get("solution_description") is None and data.get("scope"):
        data["solution_description"] = data["scope"]
    if data.get("scope") is None and data.get("solution_description"):
        data["scope"] = data["solution_description"]

    if "problem_statement" in data and data["problem_statement"] is None:
        data["problem_statement"] = ""
    if "industry_context" in data and data["industry_context"] is None:
        data["industry_context"] = ""
    if "scope" in data and data["scope"] is None:
        data["scope"] = ""
    return data


async def get_project_or_404(db: AsyncSession, project_id: str) -> Project:
    result = await db.execute(
        select(Project)
        .options(selectinload(Project.domain))
        .where(Project.id == project_id, Project.deleted_at.is_(None))
    )
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
    return project


def ensure_can_edit(project: Project, current_user: User) -> None:
    if project.created_by_id != current_user.id and current_user.role != "ADMIN":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not allowed")


@router.get("/", response_model=List[ProjectResponse])
async def get_projects(
    domainId: Optional[str] = Query(None),
    difficulty: Optional[str] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(Project)
        .options(selectinload(Project.domain))
        .where(Project.deleted_at.is_(None))
        .order_by(Project.created_at.desc())
    )
    
    if domainId:
        stmt = stmt.where(Project.domain_id == domainId)
        
    if difficulty:
        stmt = stmt.where(Project.difficulty == difficulty.upper())
        
    if search:
        search_term = f"%{search}%"
        stmt = stmt.where(
            or_(
                Project.title.ilike(search_term),
                Project.problem_statement.ilike(search_term),
                Project.case_study.ilike(search_term),
                Project.scope.ilike(search_term),
            )
        )
        
    result = await db.execute(stmt)
    return result.scalars().all()


@router.get("/domain/{domain_id}", response_model=List[ProjectResponse])
async def get_projects_by_domain(domain_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Project)
        .options(selectinload(Project.domain))
        .where(Project.domain_id == domain_id, Project.deleted_at.is_(None))
        .order_by(Project.created_at.desc())
    )
    return result.scalars().all()


@router.get("/search", response_model=List[ProjectResponse])
async def search_projects(
    q: str = Query(..., min_length=2),
    db: AsyncSession = Depends(get_db)
):
    search_term = f"%{q}%"
    stmt = (
        select(Project)
        .options(selectinload(Project.domain))
        .where(
            Project.deleted_at.is_(None),
            or_(
                Project.title.ilike(search_term),
                Project.problem_statement.ilike(search_term),
                Project.case_study.ilike(search_term),
                Project.scope.ilike(search_term),
            ),
        )
        .order_by(Project.created_at.desc())
        .limit(50)
    )
    
    result = await db.execute(stmt)
    return result.scalars().all()


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project_by_id(project_id: str, db: AsyncSession = Depends(get_db)):
    return await get_project_or_404(db, project_id)


@router.post("/", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
async def create_project(
    payload: ProjectCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    data = apply_project_defaults(payload)
    data["created_by_id"] = current_user.id

    if current_user.role != "ADMIN":
        data["is_published"] = False

    project = Project(**data)
    db.add(project)
    await db.commit()
    await db.refresh(project)
    return await get_project_or_404(db, project.id)


@router.put("/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: str,
    payload: ProjectUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    project = await get_project_or_404(db, project_id)
    ensure_can_edit(project, current_user)

    data = apply_project_defaults(payload)
    if current_user.role != "ADMIN":
        data.pop("is_published", None)

    for field, value in data.items():
        setattr(project, field, value)

    await db.commit()
    await db.refresh(project)
    return await get_project_or_404(db, project.id)


@router.delete("/{project_id}")
async def delete_project(
    project_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    project = await get_project_or_404(db, project_id)
    ensure_can_edit(project, current_user)
    await db.delete(project)
    await db.commit()
    return {"deleted": True}
