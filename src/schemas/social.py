from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class SocialUserResponse(BaseModel):
    id: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    profile_image: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)


class CommentCreateRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=5000)
    parent_id: Optional[str] = None

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class CommentResponse(BaseModel):
    id: str
    user_id: str
    project_id: Optional[str] = None
    github_project_id: Optional[str] = None
    text: str
    parent_id: Optional[str] = None
    upvotes: int
    created_at: datetime
    updated_at: datetime
    user: SocialUserResponse

    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)


class CommentListResponse(BaseModel):
    comments: list[CommentResponse]
    total: int
    page: int
    limit: int
    total_pages: int

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class LikeStatusResponse(BaseModel):
    liked: bool

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class LikeCountResponse(BaseModel):
    count: int

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class ToggleLikeResponse(BaseModel):
    liked: bool
    count: int

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class UpvoteCommentResponse(BaseModel):
    upvoted: bool
    count: int

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)