from pydantic import BaseModel, EmailStr, ConfigDict
from pydantic.alias_generators import to_camel
from typing import Optional
from datetime import datetime

class UserBase(BaseModel):
    """Base schema for user fields"""
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, populate_by_name=True)
    email: EmailStr
    name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    image: Optional[str] = None
    profile_image: Optional[str] = None
    bio: Optional[str] = None
    headline: Optional[str] = None
    location: Optional[str] = None
    github_url: Optional[str] = None
    portfolio_url: Optional[str] = None
    role: str = "STUDENT"

class UserCreate(UserBase):
    """Schema for basic Email/Password Registration"""
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserUpdate(BaseModel):
    """Schema for updating User profile"""
    name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    bio: Optional[str] = None
    headline: Optional[str] = None
    location: Optional[str] = None
    github_url: Optional[str] = None
    portfolio_url: Optional[str] = None

class UserInDB(UserBase):
    """Schema representing user from Database"""
    id: str
    is_verified: bool
    email_verified: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    
    total_time_spent: int
    current_streak: int
    longest_streak: int
    points: int
    level: int
    last_active_date: Optional[datetime] = None

class UserResponse(UserInDB):
    """Client-facing user response schema"""
    pass

class Token(BaseModel):
    """JWT Token schema"""
    access_token: str
    token_type: str = "bearer"
