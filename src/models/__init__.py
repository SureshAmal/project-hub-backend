from sqlalchemy.orm import configure_mappers
from src.models.base import Base # Base must be first
from src.models.user import User
from src.models.project import Domain, Project, GitHubProject
from src.models.social import Comment, Like
from src.models.tracking import Notification, ProjectProgress, GitHubProjectProgress

# Explicitly configure all mappers to resolve string-based relationships (e.g. Mapped["Notification"]) 
# across circular dependencies before Uvicorn starts serving traffic.
configure_mappers()

# Make sure all models are exported here for Alembic and SQLAlchemy to detect them properly
__all__ = [
    "Base", 
    "User", 
    "Domain", 
    "Project", 
    "GitHubProject", 
    "Comment",
    "Like",
    "Notification", 
    "ProjectProgress", 
    "GitHubProjectProgress"
]