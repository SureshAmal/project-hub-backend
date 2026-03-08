from datetime import datetime, timedelta, timezone
import jwt
import bcrypt
from src.core.config import settings

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a hashed one using bcrypt."""
    password_byte_enc = plain_password.encode('utf-8')
    hashed_password_bytes = hashed_password.encode('utf-8')
    try:
        return bcrypt.checkpw(password_byte_enc, hashed_password_bytes)
    except ValueError:
        return False

def get_password_hash(password: str) -> str:
    """Generates a bcrypt hash of the provided password."""
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed_password_bytes = bcrypt.hashpw(password=pwd_bytes, salt=salt)
    return hashed_password_bytes.decode('utf-8')

def create_access_token(subject: str | int, expires_delta: timedelta | None = None) -> str:
    """
    Creates a JWT access token for authentication.
    :param subject: The user ID to encode in the token.
    :param expires_delta: Optional expiration delta. Uses default config if not provided.
    """
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt
