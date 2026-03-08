"""Routes for workspace features (timer, active sessions, etc.)."""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from src.api.dependencies import get_current_user
from src.core.database import get_db
from src.models.project import Project
from src.models.tracking import ProjectProgress
from src.models.user import User

router = APIRouter()


class StartTimerRequest(BaseModel):
    projectId: str
    notes: Optional[str] = None


class StopTimerRequest(BaseModel):
    projectId: Optional[str] = None
    notes: Optional[str] = None


class UpdateTimerNotesRequest(BaseModel):
    projectId: Optional[str] = None
    notes: Optional[str] = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _elapsed_seconds(started_at: Optional[datetime], now: datetime) -> int:
    started_at = _as_utc(started_at)
    if started_at is None:
        return 0
    return max(0, int((now - started_at).total_seconds()))


def _stop_progress(progress: ProjectProgress, now: datetime, notes: Optional[str] = None) -> None:
    progress.time_spent = int(progress.time_spent or 0) + _elapsed_seconds(progress.last_timer_start, now)
    progress.is_running = False
    progress.last_timer_start = None
    progress.updated_at = now
    if notes is not None:
        progress.notes = notes


def _serialize_session(progress: Optional[ProjectProgress]) -> Optional[dict]:
    if not progress:
        return None

    project = getattr(progress, "project", None)
    if not project:
        return None

    now = _utc_now()
    started_at = progress.last_timer_start or progress.started_at or progress.updated_at
    base_duration = int(progress.time_spent or 0)
    live_duration = base_duration + (
        _elapsed_seconds(progress.last_timer_start, now) if progress.is_running else 0
    )

    return {
        "id": progress.id,
        "userId": progress.user_id,
        "projectId": progress.project_id,
        "startTime": started_at,
        "endTime": None if progress.is_running else progress.updated_at,
        "duration": live_duration,
        "baseDuration": base_duration,
        "isActive": progress.is_running,
        "notes": progress.notes,
        "lastUpdatedAt": progress.updated_at,
        "project": {
            "id": getattr(project, "id", None),
            "title": getattr(project, "title", None),
            "domain": getattr(getattr(project, "domain", None), "name", None),
        },
    }


async def _get_progress(
    db: AsyncSession,
    user_id: str,
    project_id: Optional[str] = None,
    running_only: bool = False,
) -> Optional[ProjectProgress]:
    query = (
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(ProjectProgress.user_id == user_id)
    )

    if project_id is not None:
        query = query.where(ProjectProgress.project_id == project_id)

    if running_only:
        query = query.where(ProjectProgress.is_running.is_(True))

    result = await db.execute(query.order_by(ProjectProgress.updated_at.desc()).limit(1))
    return result.scalar_one_or_none()


@router.get("/timer/active")
async def get_active_timer(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    progress = await _get_progress(db, current_user.id, running_only=True)
    return {"session": _serialize_session(progress)}


@router.post("/timer/start")
async def start_timer(
    payload: StartTimerRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    project_result = await db.execute(select(Project).where(Project.id == payload.projectId))
    project = project_result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    now = _utc_now()
    active_progress = await _get_progress(db, current_user.id, running_only=True)

    if active_progress and active_progress.project_id != payload.projectId:
        _stop_progress(active_progress, now)

    progress = await _get_progress(db, current_user.id, project_id=payload.projectId)

    if progress is None:
        progress = ProjectProgress(
            user_id=current_user.id,
            project_id=payload.projectId,
            started_at=now,
            updated_at=now,
            time_spent=0,
            is_running=True,
            last_timer_start=now,
            notes=payload.notes,
        )
        db.add(progress)
    else:
        if progress.is_running:
            if payload.notes is not None:
                progress.notes = payload.notes
                progress.updated_at = now
                await db.commit()
                progress = await _get_progress(db, current_user.id, project_id=payload.projectId)
            return {"session": _serialize_session(progress)}

        progress.is_running = True
        progress.last_timer_start = now
        progress.updated_at = now
        progress.started_at = progress.started_at or now
        if payload.notes is not None:
            progress.notes = payload.notes

    await db.commit()
    progress = await _get_progress(db, current_user.id, project_id=payload.projectId)
    return {"session": _serialize_session(progress)}


@router.post("/timer/stop")
async def stop_timer(
    payload: Optional[StopTimerRequest] = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    project_id = payload.projectId if payload else None
    notes = payload.notes if payload else None

    progress = await _get_progress(
        db,
        current_user.id,
        project_id=project_id,
        running_only=True,
    )
    if not progress:
        return {"session": None}

    _stop_progress(progress, _utc_now(), notes)
    await db.commit()

    progress = await _get_progress(db, current_user.id, project_id=progress.project_id)
    return {"session": _serialize_session(progress)}


@router.patch("/timer/notes")
async def update_timer_notes(
    payload: UpdateTimerNotesRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    progress = await _get_progress(
        db,
        current_user.id,
        project_id=payload.projectId,
        running_only=payload.projectId is None,
    )
    if not progress:
        raise HTTPException(status_code=404, detail="Timer session not found")

    progress.notes = payload.notes
    progress.updated_at = _utc_now()

    await db.commit()
    progress = await _get_progress(db, current_user.id, project_id=progress.project_id)
    return {"session": _serialize_session(progress)}


@router.get("/timer/history")
async def get_timer_history(
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(
            ProjectProgress.user_id == current_user.id,
            or_(
                ProjectProgress.started_at.is_not(None),
                ProjectProgress.time_spent > 0,
                ProjectProgress.is_running.is_(True),
            ),
        )
        .order_by(ProjectProgress.updated_at.desc())
        .limit(limit)
    )

    sessions = [
        session
        for session in (_serialize_session(progress) for progress in result.scalars().all())
        if session is not None
    ]
    return {"sessions": sessions}


@router.get("/timer/projects/{project_id}/history")
async def get_project_timer_history(
    project_id: str,
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(
            ProjectProgress.user_id == current_user.id,
            ProjectProgress.project_id == project_id,
            or_(
                ProjectProgress.started_at.is_not(None),
                ProjectProgress.time_spent > 0,
                ProjectProgress.is_running.is_(True),
            ),
        )
        .order_by(ProjectProgress.updated_at.desc())
        .limit(limit)
    )

    sessions = [
        session
        for session in (_serialize_session(progress) for progress in result.scalars().all())
        if session is not None
    ]
    return {"sessions": sessions}
