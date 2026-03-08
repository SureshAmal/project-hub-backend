"""Routes for analytics summaries."""

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from src.api.dependencies import get_current_user
from src.core.database import get_db
from src.models.project import Project
from src.models.tracking import ProjectProgress
from src.models.user import User

router = APIRouter()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _session_duration(progress: ProjectProgress, now: datetime) -> int:
    duration = int(progress.time_spent or 0)
    if progress.is_running and progress.last_timer_start:
        started_at = _as_utc(progress.last_timer_start)
        if started_at:
            duration += max(0, int((now - started_at).total_seconds()))
    return duration


def _serialize_activity(progress: ProjectProgress, now: datetime) -> Optional[dict]:
    project = getattr(progress, "project", None)
    if not project:
        return None

    return {
        "id": progress.id,
        "projectId": progress.project_id,
        "projectTitle": getattr(project, "title", None),
        "domain": getattr(getattr(project, "domain", None), "name", None),
        "duration": _session_duration(progress, now),
        "isActive": progress.is_running,
        "updatedAt": progress.updated_at,
        "notes": progress.notes,
    }


def _serialize_dashboard(
    current_user: User,
    entries: Sequence[ProjectProgress],
    now: datetime,
) -> dict:
    total_time_spent = 0
    weekly_time_spent = 0
    in_progress = 0
    completed = 0
    not_started = 0

    week_cutoff = now - timedelta(days=7)

    for progress in entries:
        duration = _session_duration(progress, now)
        total_time_spent += duration

        updated_at = _as_utc(progress.updated_at)
        if updated_at and updated_at >= week_cutoff:
            weekly_time_spent += duration

        if progress.status == "IN_PROGRESS":
            in_progress += 1
        elif progress.status == "COMPLETED":
            completed += 1
        elif progress.status == "NOT_STARTED":
            not_started += 1

    total_projects = len(entries)
    completion_rate = int((completed / total_projects) * 100) if total_projects else 0

    return {
        "user": {
            "totalTimeSpent": total_time_spent,
            "totalTimeSpentHours": total_time_spent // 3600,
            "weeklyTimeSpent": weekly_time_spent,
            "currentStreak": int(current_user.current_streak or 0),
            "longestStreak": int(current_user.longest_streak or 0),
            "points": int(current_user.points or 0),
            "level": int(current_user.level or 1),
        },
        "projects": {
            "total": total_projects,
            "inProgress": in_progress,
            "completed": completed,
            "notStarted": not_started,
            "completionRate": completion_rate,
        },
        "achievements": {
            "total": 0,
        },
    }


@router.get("/summary")
async def get_analytics_summary(
    recent_limit: int = Query(5, ge=1, le=20),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(ProjectProgress.user_id == current_user.id)
        .order_by(ProjectProgress.updated_at.desc())
    )
    entries = result.scalars().all()
    now = _utc_now()

    tracked_projects = set()
    active_timers = 0
    total_time_spent = 0
    domain_counter: Counter[str] = Counter()
    recent_activity = []

    for progress in entries:
        project = getattr(progress, "project", None)
        if not project:
            continue

        tracked_projects.add(progress.project_id)
        if progress.is_running:
            active_timers += 1

        duration = _session_duration(progress, now)
        total_time_spent += duration

        domain_name = getattr(getattr(project, "domain", None), "name", None)
        if domain_name:
            domain_counter[domain_name] += duration or 1

        if len(recent_activity) < recent_limit:
            activity = _serialize_activity(progress, now)
            if activity is not None:
                recent_activity.append(activity)

    top_domains = [
        {"domain": domain, "timeSpent": seconds}
        for domain, seconds in domain_counter.most_common(5)
    ]

    return {
        "overview": {
            "trackedProjects": len(tracked_projects),
            "totalSessions": len(entries),
            "activeTimers": active_timers,
            "totalTimeSpent": total_time_spent,
            "averageSessionDuration": total_time_spent // len(entries) if entries else 0,
        },
        "topDomains": top_domains,
        "recentActivity": recent_activity,
    }


@router.get("/dashboard")
async def get_dashboard_analytics(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(ProjectProgress.user_id == current_user.id)
        .order_by(ProjectProgress.updated_at.desc())
    )
    entries = result.scalars().all()
    now = _utc_now()

    summary = await get_analytics_summary(recent_limit=5, current_user=current_user, db=db)
    compatibility = _serialize_dashboard(current_user, entries, now)

    project_result = await db.execute(select(Project))
    total_projects = len(project_result.scalars().all())

    return {
        **summary,
        **compatibility,
        "catalog": {
            "totalProjects": total_projects,
        },
    }


@router.post("/update-streak")
async def update_user_streak(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    now = _utc_now()
    today = now.date()
    last_active = _as_utc(current_user.last_active_date)

    if last_active and last_active.date() == today:
        current_streak = int(current_user.current_streak or 1)
    elif last_active and last_active.date() == (today - timedelta(days=1)):
        current_streak = int(current_user.current_streak or 0) + 1
    else:
        current_streak = 1

    current_user.current_streak = current_streak
    current_user.longest_streak = max(int(current_user.longest_streak or 0), current_streak)
    current_user.last_active_date = now

    await db.commit()

    return {
        "message": "Streak updated",
        "currentStreak": current_user.current_streak,
        "longestStreak": current_user.longest_streak,
    }


@router.get("/leaderboard")
async def get_leaderboard(
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(User)
        .order_by(
            desc(User.points),
            desc(User.total_time_spent),
            desc(User.current_streak),
            User.created_at.asc(),
        )
        .limit(limit)
    )
    users = result.scalars().all()

    return [
        {
            "id": user.id,
            "email": user.email,
            "firstName": user.first_name or "",
            "lastName": user.last_name or "",
            "currentStreak": int(user.current_streak or 0),
            "longestStreak": int(user.longest_streak or 0),
            "totalMinutes": int((user.total_time_spent or 0) // 60),
            "points": int(user.points or 0),
            "level": int(user.level or 1),
        }
        for user in users
    ]


@router.get("/time-tracking")
async def get_time_tracking(
    days: int = Query(7, ge=1, le=90),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ProjectProgress)
        .where(ProjectProgress.user_id == current_user.id)
        .order_by(ProjectProgress.updated_at.desc())
    )
    entries = result.scalars().all()
    now = _utc_now()

    buckets: dict[str, int] = {}
    for offset in range(days - 1, -1, -1):
        day = (now - timedelta(days=offset)).date().isoformat()
        buckets[day] = 0

    for progress in entries:
        updated_at = _as_utc(progress.updated_at)
        if not updated_at:
            continue
        day_key = updated_at.date().isoformat()
        if day_key in buckets:
            buckets[day_key] += _session_duration(progress, now)

    return {
        "days": [{"date": day, "timeSpent": seconds} for day, seconds in buckets.items()]
    }


@router.get("/progress-insights")
async def get_progress_insights(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    summary = await get_analytics_summary(recent_limit=3, current_user=current_user, db=db)
    top_domains = summary.get("topDomains", [])
    overview = summary.get("overview", {})

    insights = []
    if overview.get("activeTimers", 0):
        insights.append("You currently have an active timer running—keep the momentum going.")
    if top_domains:
        insights.append(f"Your strongest recent focus area is {top_domains[0]['domain']}.")
    if overview.get("trackedProjects", 0) == 0:
        insights.append("Start your first project to unlock deeper analytics and recommendations.")
    elif overview.get("averageSessionDuration", 0) < 1800:
        insights.append("Try longer focused sessions to build deeper project momentum.")
    else:
        insights.append("Your session length is healthy—maintain consistency for faster progress.")

    return {"insights": insights}
