from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import jwt
from pydantic import ValidationError

from src.core.config import settings
from src.core.database import get_db
from src.models.user import User

security = HTTPBearer()

async def get_current_user(
    db: AsyncSession = Depends(get_db),
    auth: HTTPAuthorizationCredentials = Depends(security)
) -> User:
    """
    Dependency to get the currently authenticated user from a JWT token.
    Throws a 401 if invalid or user not found.
    """
    token = auth.credentials
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        user_id: str = payload.get("sub") or payload.get("id")
        if user_id is None:
            print("JWT Decode Error: No user ID found in payload")
            raise credentials_exception
    except Exception as e:
        print(f"JWT Verification Exception: {e}")
        raise credentials_exception
    
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    
    if user is None:
        print(f"User Not Found in DB: {user_id}")
        raise credentials_exception
        
    return user
