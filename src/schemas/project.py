from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel
from typing import Optional, List
from datetime import datetime

class DomainBase(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)
    name: str
    slug: str
    description: Optional[str] = None

class DomainResponse(DomainBase):
    id: str
    created_at: datetime
    updated_at: datetime


class ProjectBase(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)

    title: str
    domain_id: str
    sub_domain: Optional[str] = None
    difficulty: str = "MEDIUM"
    min_time: int = 10
    max_time: int = 20
    skill_focus: List[str] = Field(default_factory=list)

    case_study: Optional[str] = None
    problem_statement: str = ""
    solution_description: Optional[str] = None
    tech_stack: List[str] = Field(default_factory=list)
    supposed_deadline: Optional[str] = None

    screenshots: List[str] = Field(default_factory=list)
    initialization_guide: Optional[str] = None

    industry_context: str = ""
    scope: str = ""
    prerequisites: List[str] = Field(default_factory=list)
    deliverables: List[str] = Field(default_factory=list)
    requirements: List[str] = Field(default_factory=list)
    requirements_text: Optional[str] = None
    advanced_extensions: Optional[str] = None
    evaluation_criteria: Optional[str] = None

    is_published: bool = False


class ProjectCreate(ProjectBase):
    pass


class ProjectUpdate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    title: Optional[str] = None
    domain_id: Optional[str] = None
    sub_domain: Optional[str] = None
    difficulty: Optional[str] = None
    min_time: Optional[int] = None
    max_time: Optional[int] = None
    skill_focus: Optional[List[str]] = None

    case_study: Optional[str] = None
    problem_statement: Optional[str] = None
    solution_description: Optional[str] = None
    tech_stack: Optional[List[str]] = None
    supposed_deadline: Optional[str] = None

    screenshots: Optional[List[str]] = None
    initialization_guide: Optional[str] = None

    industry_context: Optional[str] = None
    scope: Optional[str] = None
    prerequisites: Optional[List[str]] = None
    deliverables: Optional[List[str]] = None
    requirements: Optional[List[str]] = None
    requirements_text: Optional[str] = None
    advanced_extensions: Optional[str] = None
    evaluation_criteria: Optional[str] = None

    is_published: Optional[bool] = None


class ProjectResponse(ProjectBase):
    id: str
    created_by_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    domain: Optional[DomainResponse] = None

class GitHubProjectBase(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)
    title: str
    slug: Optional[str] = None
    description: str
    repo_url: str
    repo_owner: Optional[str] = None
    repo_name: Optional[str] = None
    default_branch: str = "main"
    download_url: str = ""
    live_url: Optional[str] = None
    project_type: str = "PROJECT"
    
    stars: int = 0
    forks: int = 0
    language: Optional[str] = None
    tech_stack: List[str] = []
    difficulty: str = "MEDIUM"
    topics: List[str] = []
    
    author: str = "Project Hub"
    introduction: Optional[str] = None
    implementation: Optional[str] = None
    technical_skills: List[str] = []
    tools_used: List[str] = []
    concepts_used: List[str] = []

    sub_domain: Optional[str] = None
    case_study: Optional[str] = None
    problem_statement: Optional[str] = None
    solution_description: Optional[str] = None
    prerequisites: List[str] = []
    prerequisites_text: Optional[str] = None
    
    estimated_min_time: int = 10
    estimated_max_time: int = 40
    deliverables: List[str] = []
    supposed_deadline: Optional[str] = None
    optional_extensions: Optional[str] = None
    requirements: List[str] = []
    requirements_text: Optional[str] = None
    evaluation_criteria: Optional[str] = None

class GitHubProjectResponse(GitHubProjectBase):
    id: str
    domain_id: str
    download_count: int
    is_active: bool
    last_updated: Optional[datetime] = None
    qa_status: str
    qa_feedback: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    reviewed_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    domain: Optional[DomainResponse] = None

class PaginationSchema(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    total: int
    page: int
    limit: int
    total_pages: int

class GitHubProjectListResponse(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    projects: List[GitHubProjectResponse]
    pagination: PaginationSchema
