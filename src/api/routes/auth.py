from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi.security import OAuth2PasswordRequestForm

from src.core.database import get_db
from src.core.security import verify_password, get_password_hash, create_access_token
from src.core.config import settings
from src.models.user import User
from src.schemas.user import UserCreate, UserResponse, Token, UserLogin
from src.api.dependencies import get_current_user

router = APIRouter()

@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register_user(user_in: UserCreate, db: AsyncSession = Depends(get_db)):
    """
    Register a new user email and password.
    """
    # Check if user exists
    result = await db.execute(select(User).where(User.email == user_in.email))
    if result.scalars().first():
        raise HTTPException(status_code=400, detail="Email already registered")
        
    # Create new user
    db_user = User(
        email=user_in.email,
        name=user_in.name,
        first_name=user_in.first_name,
        last_name=user_in.last_name,
        password_hash=get_password_hash(user_in.password),
        role="STUDENT"
    )
    
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user

@router.post("/login")
async def login(login_data: UserLogin, db: AsyncSession = Depends(get_db)):
    """
    JSON token login, get an access token for future requests.
    """
    result = await db.execute(select(User).where(User.email == login_data.email))
    user = result.scalars().first()
    
    if not user or not user.password_hash:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    if not verify_password(login_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    # Generate tokens
    access_token = create_access_token(subject=user.id)
    from datetime import timedelta
    refresh_token = create_access_token(
        subject=user.id, 
        expires_delta=timedelta(days=30)
    )
    return {
        "accessToken": access_token, 
        "refreshToken": refresh_token, 
        "tokenType": "bearer"
    }

@router.get("/me", response_model=UserResponse)
async def read_users_me(current_user: User = Depends(get_current_user)):
    """
    Returns the currently authenticated user based on JWT.
    """
    return current_user

@router.post("/refresh")
async def refresh_token(body: dict, db: AsyncSession = Depends(get_db)):
    """
    Refresh an access token using a refresh token.
    For now, decodes the existing access token or accepts a dummy refresh token
    and issues a new access token if the user exists.
    """
    refresh_token_value = body.get("refreshToken")
    if not refresh_token_value:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No refresh token provided",
        )

    # Try to decode if it's a real JWT, otherwise look up via stored token
    try:
        import jwt as pyjwt
        payload = pyjwt.decode(
            refresh_token_value,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )
        user_id = payload.get("sub")
    except Exception:
        # If the refresh token is the dummy one, we can't refresh
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        )

    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token payload",
        )

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )

    new_access_token = create_access_token(subject=user.id)
    return {"accessToken": new_access_token}

@router.post("/logout")
async def logout():
    """
    Logout endpoint. Clears server-side token state if needed.
    Currently stateless JWT, so no server action required.
    """
    return {"message": "Logged out successfully"}

