from fastapi import APIRouter
from src.api.routes import auth, domains, projects, user, notifications, github_projects, workspace, gamification, analytics, learning_paths, social

api_router = APIRouter()

api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(domains.router, prefix="/domains", tags=["domains"])
api_router.include_router(projects.router, prefix="/projects", tags=["projects"])
api_router.include_router(github_projects.router, prefix="/github-projects", tags=["github-projects"])
api_router.include_router(user.router, prefix="/user", tags=["user"])
api_router.include_router(gamification.router, prefix="/gamification", tags=["gamification"])
api_router.include_router(notifications.router, prefix="/notifications", tags=["notifications"])
api_router.include_router(social.router, prefix="/social", tags=["social"])
api_router.include_router(workspace.router, prefix="/workspace", tags=["workspace"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
api_router.include_router(learning_paths.router, prefix="/learning-paths", tags=["learning-paths"])
