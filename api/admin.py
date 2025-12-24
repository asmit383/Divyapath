from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from jose import jwt, JWTError

from core.security import verify_password, create_access_token
from core.config import settings
from db.mysql import get_admin_by_username

router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{settings.API_V1_STR}/admin/login")

class Token(BaseModel):
    access_token: str
    token_type: str

class AdminUser(BaseModel):
    username: str
    id: int | str

async def get_current_admin(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = get_admin_by_username(username)
    if user is None:
        raise credentials_exception
        
    return AdminUser(username=user["username"], id=user["id"])

@router.post("/login", response_model=Token)
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    """
    Admin Login Endpoint. 
    Exchange username/password for a JWT access token.
    """
    user = get_admin_by_username(form_data.username)
    
    if not user or not verify_password(form_data.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = create_access_token(subject=user["username"])
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me", response_model=AdminUser)
async def read_users_me(current_user: Annotated[AdminUser, Depends(get_current_admin)]):
    """
    Protected Endpoint: Get current logged-in admin details.
    """
    return current_user
