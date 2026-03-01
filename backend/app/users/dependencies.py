from datetime import UTC, datetime

from app.core.config import settings
from app.database import get_db
from app.users.models import User, UserRole
from app.users.repository import UserRepository
from fastapi import Depends, HTTPException, Request, status
from jose import JWTError, jwt
from sqlalchemy.ext.asyncio import AsyncSession


def get_token_from_cookie(request: Request) -> str:
    token = request.cookies.get("flats_access_token")
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Токен отсутствует",
        )
    return token


async def get_current_user(
    token: str = Depends(get_token_from_cookie),
    db: AsyncSession = Depends(get_db),
) -> User:
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Неверный токен") from None

    exp = payload.get("exp")
    if not exp or datetime.fromtimestamp(exp, tz=UTC) < datetime.now(UTC):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Токен истёк")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Неверный токен (no sub)")

    user_repo = UserRepository(db)
    user = await user_repo.find_one_or_none(id=int(user_id))

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Пользователь не найден")

    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Аккаунт забанен")

    return user


class RoleChecker:
    def __init__(self, allowed_roles: list[UserRole]):
        self.allowed_roles = allowed_roles

    def __call__(self, user: User = Depends(get_current_user)):
        if user.role not in self.allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="У вас недостаточно прав для выполнения этого действия"
            )
        return user


# Инстансы для использования в роутах
allow_admin = RoleChecker([UserRole.ADMIN])
allow_user_and_admin = RoleChecker([UserRole.USER, UserRole.ADMIN])
