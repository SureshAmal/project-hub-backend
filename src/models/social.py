import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base


def get_utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Comment(Base):
    __tablename__ = "comments"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column("userId", ForeignKey("users.id", ondelete="CASCADE"), index=True)
    project_id: Mapped[Optional[str]] = mapped_column("projectId", ForeignKey("projects.id", ondelete="CASCADE"), index=True)
    github_project_id: Mapped[Optional[str]] = mapped_column("githubProjectId", ForeignKey("github_projects.id", ondelete="CASCADE"), index=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    parent_id: Mapped[Optional[str]] = mapped_column("parentId", ForeignKey("comments.id", ondelete="CASCADE"), index=True)
    upvotes: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column("createdAt", DateTime(timezone=True), default=get_utc_now)
    updated_at: Mapped[datetime] = mapped_column("updatedAt", DateTime(timezone=True), default=get_utc_now, onupdate=get_utc_now)

    user: Mapped[object] = relationship("User", back_populates="comments")
    project: Mapped[object] = relationship("Project", back_populates="comments")
    github_project: Mapped[object] = relationship("GitHubProject", back_populates="comments")
    parent: Mapped[object] = relationship("Comment", remote_side="Comment.id", back_populates="replies")
    replies: Mapped[list] = relationship("Comment", back_populates="parent", cascade="all, delete-orphan")
    likes: Mapped[list] = relationship("Like", back_populates="comment", cascade="all, delete-orphan")


class Like(Base):
    __tablename__ = "likes"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column("userId", ForeignKey("users.id", ondelete="CASCADE"), index=True)
    project_id: Mapped[Optional[str]] = mapped_column("projectId", ForeignKey("projects.id", ondelete="CASCADE"), index=True)
    github_project_id: Mapped[Optional[str]] = mapped_column("githubProjectId", ForeignKey("github_projects.id", ondelete="CASCADE"), index=True)
    comment_id: Mapped[Optional[str]] = mapped_column("commentId", ForeignKey("comments.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column("createdAt", DateTime(timezone=True), default=get_utc_now)

    user: Mapped[object] = relationship("User", back_populates="likes")
    project: Mapped[object] = relationship("Project", back_populates="likes")
    github_project: Mapped[object] = relationship("GitHubProject", back_populates="likes")
    comment: Mapped[object] = relationship("Comment", back_populates="likes")