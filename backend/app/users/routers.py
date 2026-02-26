from app.database import get_db
from app.users.auth import AuthService
from app.users.dependencies import get_current_user
from app.users.models import User
from app.users.schemas import SUserAuth, SUserResponse
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/auth", tags=["Auth"])


def set_tokens_cookies(response: Response, tokens: dict):
    response.set_cookie(
        key="flats_access_token",
        value=tokens["access_token"],
        httponly=True,
        secure=False,
        samesite="lax",
    )
    response.set_cookie(
        key="flats_refresh_token",
        value=tokens["refresh_token"],
        httponly=True,
        secure=False,
        samesite="lax",
    )


@router.post("/register", response_model=SUserResponse)
async def register(data: SUserAuth, db: AsyncSession = Depends(get_db)):
    auth_service = AuthService(db)
    return await auth_service.register_user(username=data.username, password=data.password)


@router.post("/login")
async def login(response: Response, data: SUserAuth, db: AsyncSession = Depends(get_db)):
    auth_service = AuthService(db)
    tokens = await auth_service.authenticate_user(username=data.username, password=data.password)
    set_tokens_cookies(response, tokens)
    return {"message": "Успешный вход", "user": data.username}


@router.post("/refresh")
async def refresh(request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    refresh_token = request.cookies.get("flats_refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Refresh токен отсутствует")

    auth_service = AuthService(db)
    new_tokens = await auth_service.refresh_tokens(refresh_token)
    set_tokens_cookies(response, new_tokens)
    return {"message": "Токены обновлены"}


@router.post("/logout")
async def logout(response: Response):
    response.delete_cookie("flats_access_token")
    response.delete_cookie("flats_refresh_token")
    return {"message": "Вы вышли из системы"}


@router.get("/me")
async def read_users_me(current_user: User = Depends(get_current_user)):
    return {"id": current_user.id, "username": current_user.username, "role": current_user.role}
