"""
JWT Authentication Middleware for FastAPI
Uses shared JWT_SECRET with backend for token verification
"""
from functools import wraps
from typing import Optional, Callable
from fastapi import HTTPException, status, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
import os
from config import settings

security = HTTPBearer(auto_error=False)


async def verify_rent_backfill_auth(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> dict:
    """
    For /rent-estimation/backfill: accept either X-API-Key (if RENT_BACKFILL_API_KEY is set)
    or JWT Bearer. Use X-API-Key when you don't have a JWT (e.g. Postman, scripts).
    """
    api_key = getattr(settings, "RENT_BACKFILL_API_KEY", None) or os.getenv("RENT_BACKFILL_API_KEY") or ""
    api_key = (api_key or "").strip()
    if api_key and request.headers.get("X-API-Key") == api_key:
        return {"_api_key": True}

    if not credentials:
        detail = "Authorization: Bearer <JWT> required."
        if api_key:
            detail = "Use Authorization: Bearer <JWT> or X-API-Key: <key>."
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = credentials.credentials
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def verify_token(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """
    Verify JWT token from Authorization header
    Returns the decoded token payload if valid
    """
    # Get JWT secret from config (shared with backend)
    jwt_secret = settings.JWT_SECRET
    
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    token = credentials.credentials
    
    try:
        # Verify and decode the token
        payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_auth(func: Callable):
    """
    Decorator to require JWT authentication on a route
    Usage:
        @app.get("/protected")
        @require_auth
        async def protected_route(request: Request, token_payload: dict = Depends(verify_token)):
            user_id = token_payload.get("userId")
            ...
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # The verify_token dependency will be handled by FastAPI's dependency injection
        # This decorator is mainly for documentation/consistency
        return await func(*args, **kwargs)
    return wrapper

