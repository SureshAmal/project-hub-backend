import uuid
from datetime import datetime, timezone
from typing import Optional, List

from sqlalchemy import String, Boolean, DateTime, Text, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import ARRAY, ENUM

from src.models.base import Base



def get_utc_now() -> datetime:
    return datetime.now(timezone.utc)

class Domain(Base):
    __tablename__ = "domains"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    slug: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column("createdAt", DateTime(timezone=True), default=get_utc_now)
    updated_at: Mapped[datetime] = mapped_column("updatedAt", DateTime(timezone=True), default=get_utc_now, onupdate=get_utc_now)

    github_projects: Mapped[List["GitHubProject"]] = relationship("GitHubProject", back_populates="domain", cascade="all, delete")
    projects: Mapped[List["Project"]] = relationship("Project", back_populates="domain", cascade="all, delete")

class Project(Base):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    title: Mapped[str] = mapped_column(String, nullable=False)
    created_by_id: Mapped[Optional[str]] = mapped_column("createdById", ForeignKey("users.id", ondelete="SET NULL"))
    domain_id: Mapped[str] = mapped_column("domainId", ForeignKey("domains.id", ondelete="CASCADE"), index=True)
    sub_domain: Mapped[Optional[str]] = mapped_column("subDomain", String)
    difficulty: Mapped[str] = mapped_column(ENUM("EASY", "MEDIUM", "HARD", "ADVANCED", "EXPERT", name="Difficulty", create_type=False), default="MEDIUM", index=True)
    min_time: Mapped[int] = mapped_column("minTime", Integer, default=10)
    max_time: Mapped[int] = mapped_column("maxTime", Integer, default=20)
    skill_focus: Mapped[List[str]] = mapped_column("skillFocus", ARRAY(String), default=list)

    case_study: Mapped[Optional[str]] = mapped_column("caseStudy", Text)
    problem_statement: Mapped[str] = mapped_column("problemStatement", Text, nullable=False, default="")
    solution_description: Mapped[Optional[str]] = mapped_column("solutionDescription", Text)
    tech_stack: Mapped[List[str]] = mapped_column("techStack", ARRAY(String), default=list)
    supposed_deadline: Mapped[Optional[str]] = mapped_column("supposedDeadline", String)

    screenshots: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    initialization_guide: Mapped[Optional[str]] = mapped_column("initializationGuide", Text)

    industry_context: Mapped[str] = mapped_column("industryContext", Text, nullable=False, default="")
    scope: Mapped[str] = mapped_column(Text, nullable=False, default="")
    prerequisites: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    deliverables: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    requirements: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    requirements_text: Mapped[Optional[str]] = mapped_column("requirements_text", Text)
    advanced_extensions: Mapped[Optional[str]] = mapped_column("advancedExtensions", Text)
    evaluation_criteria: Mapped[Optional[str]] = mapped_column("evaluationCriteria", Text)

    is_published: Mapped[bool] = mapped_column("isPublished", Boolean, default=False, index=True)
    deleted_at: Mapped[Optional[datetime]] = mapped_column("deletedAt", DateTime(timezone=True))

    domain: Mapped["Domain"] = relationship("Domain", back_populates="projects")
    created_by: Mapped[Optional[object]] = relationship("User", back_populates="created_projects")
    
    progress: Mapped[list] = relationship("ProjectProgress", back_populates="project", cascade="all, delete-orphan")
    bookmarks: Mapped[list] = relationship("Bookmark", back_populates="project", cascade="all, delete-orphan")
    comments: Mapped[list] = relationship("Comment", back_populates="project", cascade="all, delete-orphan")
    likes: Mapped[list] = relationship("Like", back_populates="project", cascade="all, delete-orphan")
    created_at: Mapped[datetime] = mapped_column("createdAt", DateTime(timezone=True), default=get_utc_now)
    updated_at: Mapped[datetime] = mapped_column("updatedAt", DateTime(timezone=True), default=get_utc_now, onupdate=get_utc_now)

class GitHubProject(Base):
    __tablename__ = "github_projects"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    title: Mapped[str] = mapped_column(String, nullable=False)
    slug: Mapped[Optional[str]] = mapped_column(String, unique=True, index=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    repo_url: Mapped[str] = mapped_column("repoUrl", String, nullable=False)
    repo_owner: Mapped[Optional[str]] = mapped_column("repoOwner", String)
    repo_name: Mapped[Optional[str]] = mapped_column("repoName", String)
    default_branch: Mapped[str] = mapped_column("defaultBranch", String, default="main")
    download_url: Mapped[str] = mapped_column("downloadUrl", String, default="")
    live_url: Mapped[Optional[str]] = mapped_column("liveUrl", String)
    download_count: Mapped[int] = mapped_column("downloadCount", Integer, default=0)
    
    domain_id: Mapped[str] = mapped_column("domainId", ForeignKey("domains.id", ondelete="CASCADE"), index=True)
    domain: Mapped["Domain"] = relationship("Domain", back_populates="github_projects")
    
    stars: Mapped[int] = mapped_column(Integer, default=0, index=True)
    forks: Mapped[int] = mapped_column(Integer, default=0)
    language: Mapped[Optional[str]] = mapped_column(String, index=True)
    tech_stack: Mapped[List[str]] = mapped_column("techStack", ARRAY(String), default=list)
    difficulty: Mapped[str] = mapped_column(ENUM("EASY", "MEDIUM", "HARD", "ADVANCED", "EXPERT", name="Difficulty", create_type=False), default="MEDIUM", index=True)
    topics: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    project_type: Mapped[str] = mapped_column(String, default="PROJECT", index=True)
    last_updated: Mapped[Optional[datetime]] = mapped_column("lastUpdated", DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column("isActive", Boolean, default=True, index=True)

    author: Mapped[str] = mapped_column(String, default="Project Hub")
    introduction: Mapped[Optional[str]] = mapped_column(Text)
    implementation: Mapped[Optional[str]] = mapped_column(Text)
    technical_skills: Mapped[List[str]] = mapped_column("technicalSkills", ARRAY(String), default=list)
    tools_used: Mapped[List[str]] = mapped_column("toolsUsed", ARRAY(String), default=list)
    concepts_used: Mapped[List[str]] = mapped_column("conceptsUsed", ARRAY(String), default=list)

    sub_domain: Mapped[Optional[str]] = mapped_column(String)
    case_study: Mapped[Optional[str]] = mapped_column("caseStudy", Text)
    problem_statement: Mapped[Optional[str]] = mapped_column("problemStatement", Text)
    solution_description: Mapped[Optional[str]] = mapped_column("solutionDescription", Text)
    prerequisites: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    prerequisites_text: Mapped[Optional[str]] = mapped_column(Text)
    
    estimated_min_time: Mapped[int] = mapped_column(Integer, default=10)
    estimated_max_time: Mapped[int] = mapped_column(Integer, default=40)
    deliverables: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    supposed_deadline: Mapped[Optional[str]] = mapped_column("supposedDeadline", String)
    optional_extensions: Mapped[Optional[str]] = mapped_column(Text)
    requirements: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    requirements_text: Mapped[Optional[str]] = mapped_column(Text)
    evaluation_criteria: Mapped[Optional[str]] = mapped_column(Text)

    qa_status: Mapped[str] = mapped_column(String, default="PENDING")
    qa_feedback: Mapped[Optional[str]] = mapped_column(Text)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    reviewed_by: Mapped[Optional[str]] = mapped_column(String)

    created_at: Mapped[datetime] = mapped_column("createdAt", DateTime(timezone=True), default=get_utc_now)
    updated_at: Mapped[datetime] = mapped_column("updatedAt", DateTime(timezone=True), default=get_utc_now, onupdate=get_utc_now)

    progress: Mapped[list] = relationship("GitHubProjectProgress", back_populates="github_project", cascade="all, delete-orphan")
    bookmarks: Mapped[list] = relationship("Bookmark", back_populates="github_project", cascade="all, delete-orphan")
    comments: Mapped[list] = relationship("Comment", back_populates="github_project", cascade="all, delete-orphan")
    likes: Mapped[list] = relationship("Like", back_populates="github_project", cascade="all, delete-orphan")
