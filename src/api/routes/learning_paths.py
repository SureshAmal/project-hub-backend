"""Routes for learning path recommendations."""

from collections import Counter

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import Sequence

from src.api.dependencies import get_current_user
from src.core.database import get_db
from src.models.project import Project
from src.models.tracking import ProjectProgress
from src.models.user import User

router = APIRouter()


def _serialize_project(project: Project, reason: str) -> dict:
    return {
        "id": getattr(project, "id", None),
        "title": getattr(project, "title", None),
        "domain": getattr(getattr(project, "domain", None), "name", None),
        "reason": reason,
    }


def _path_id(domain_name: str) -> str:
    return f"path-{domain_name.lower().replace(' ', '-')}"


def _serialize_path(domain_name: str, projects: Sequence[Project]) -> dict:
    skills: list[str] = []
    seen_skills = set()

    for project in projects:
        for skill in getattr(project, "skill_focus", []) or []:
            if skill and skill not in seen_skills:
                seen_skills.add(skill)
                skills.append(skill)

    difficulty_order = {"EASY": 1, "MEDIUM": 2, "HARD": 3, "ADVANCED": 4, "EXPERT": 5}
    highest_difficulty = "MEDIUM"
    highest_rank = 0

    estimated_time = 0
    for project in projects:
        difficulty = getattr(project, "difficulty", "MEDIUM") or "MEDIUM"
        rank = difficulty_order.get(difficulty, 2)
        if rank > highest_rank:
            highest_rank = rank
            highest_difficulty = difficulty
        estimated_time += int(getattr(project, "max_time", 0) or getattr(project, "min_time", 0) or 0)

    return {
        "id": _path_id(domain_name),
        "title": f"{domain_name} Learning Path",
        "description": f"Build practical {domain_name.lower()} skills through a guided sequence of real projects.",
        "difficulty": highest_difficulty,
        "domains": [domain_name],
        "projectIds": [getattr(project, "id", None) for project in projects if getattr(project, "id", None)],
        "estimatedTime": estimated_time or len(projects) * 8,
        "skills": skills[:8],
    }


def _build_paths(projects: Sequence[Project], preferred_domains: set[str] | None = None, limit: int | None = None) -> list[dict]:
    grouped: dict[str, list[Project]] = {}

    for project in projects:
        domain_name = getattr(getattr(project, "domain", None), "name", None)
        if not domain_name:
            continue
        grouped.setdefault(domain_name, []).append(project)

    preferred_domains = preferred_domains or set()

    ordered_domains = sorted(
        grouped.keys(),
        key=lambda domain: (0 if domain in preferred_domains else 1, domain.lower()),
    )

    paths = [_serialize_path(domain, grouped[domain][:6]) for domain in ordered_domains if grouped[domain]]
    return paths[:limit] if limit is not None else paths


@router.get("")
@router.get("/")
async def get_learning_paths(db: AsyncSession = Depends(get_db)):
    project_result = await db.execute(
        select(Project)
        .options(selectinload(Project.domain))
        .order_by(Project.created_at.desc())
        .limit(300)
    )
    all_projects = project_result.scalars().all()
    return {"paths": _build_paths(all_projects, limit=12)}


@router.get("/recommendations")
async def get_learning_path_recommendations(
    limit: int = Query(8, ge=1, le=20),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    progress_result = await db.execute(
        select(ProjectProgress)
        .options(selectinload(ProjectProgress.project).selectinload(Project.domain))
        .where(ProjectProgress.user_id == current_user.id)
        .order_by(ProjectProgress.updated_at.desc())
    )
    progress_entries = progress_result.scalars().all()

    tracked_project_ids = set()
    domain_counter: Counter[str] = Counter()
    continue_learning = []
    completed_projects = 0
    tracked_skills: Counter[str] = Counter()

    for progress in progress_entries:
        project = getattr(progress, "project", None)
        if not project:
            continue

        tracked_project_ids.add(progress.project_id)
        domain_name = getattr(getattr(project, "domain", None), "name", None)
        if domain_name:
            domain_counter[domain_name] += 1

        if progress.status == "COMPLETED":
            completed_projects += 1

        for skill in getattr(project, "skill_focus", []) or []:
            if skill:
                tracked_skills[skill] += 1

        if len(continue_learning) < 3:
            continue_learning.append(
                {
                    "projectId": progress.project_id,
                    "title": getattr(project, "title", None),
                    "domain": domain_name,
                    "isActive": progress.is_running,
                    "timeSpent": int(progress.time_spent or 0),
                    "updatedAt": progress.updated_at,
                }
            )

    focus_domains = [
        {"domain": domain, "activityCount": count}
        for domain, count in domain_counter.most_common(3)
    ]
    preferred_domains = {item["domain"] for item in focus_domains}

    project_result = await db.execute(
        select(Project)
        .options(selectinload(Project.domain))
        .order_by(Project.id.desc())
        .limit(200)
    )
    all_projects = project_result.scalars().all()

    preferred = []
    fallback = []

    for project in all_projects:
        if getattr(project, "id", None) in tracked_project_ids:
            continue

        domain_name = getattr(getattr(project, "domain", None), "name", None)
        item = _serialize_project(
            project,
            "Matches your recent activity"
            if domain_name in preferred_domains
            else "Recommended to broaden your project mix",
        )

        if domain_name in preferred_domains:
            preferred.append(item)
        else:
            fallback.append(item)

    recommendations = (preferred + fallback)[:limit]

    if not recommendations:
        recommendations = [
            _serialize_project(project, "Starter recommendation")
            for project in all_projects[:limit]
        ]

    recommended_paths = _build_paths(all_projects, preferred_domains=preferred_domains, limit=limit)

    completed_ratio = completed_projects / len(progress_entries) if progress_entries else 0
    if completed_ratio >= 0.7:
        recommended_difficulty = "HARD"
    elif completed_ratio >= 0.3:
        recommended_difficulty = "MEDIUM"
    else:
        recommended_difficulty = "EASY"

    return {
        "focusDomains": focus_domains,
        "continueLearning": continue_learning,
        "projectRecommendations": recommendations,
        "recommendations": recommended_paths,
        "insights": {
            "completedProjects": completed_projects,
            "recommendedDifficulty": recommended_difficulty,
            "topDomain": focus_domains[0]["domain"] if focus_domains else "",
            "topSkills": [skill for skill, _count in tracked_skills.most_common(5)],
        },
    }


@router.get("/focus")
async def get_learning_focus(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    recommendations = await get_learning_path_recommendations(
        limit=6,
        current_user=current_user,
        db=db,
    )
    return {
        "focusDomains": recommendations["focusDomains"],
        "continueLearning": recommendations["continueLearning"],
    }
