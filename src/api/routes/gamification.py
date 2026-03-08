"""
Routes for gamification features (achievements, leaderboards, etc.).
"""
from fastapi import APIRouter, Depends

from src.api.dependencies import get_current_user
from src.models.user import User

router = APIRouter()


@router.get("/achievements/all")
async def get_all_achievements():
    """
    Get all available achievements system-wide.
    Provides generic data that does not require authentication.
    """
    return {"achievements": []}

@router.get("/achievements/user")
async def get_user_achievements(
    current_user: User = Depends(get_current_user),
):
    """
    Get achievements earned by the authenticated user.
    """
    return {
        "achievements": [],
        "progress": {
            "totalHours": 0,
            "completedProjects": 0,
            "currentStreak": getattr(current_user, "current_streak", 0),
            "points": 0
        }
    }
