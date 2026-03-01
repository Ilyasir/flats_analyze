from datetime import UTC, datetime, timedelta

from app.core.config import settings
from app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    get_password_hash,
    verify_password,
)
from app.users.models import RefreshToken, User, UserRole
from app.users.repository import UserRepository
from fastapi import HTTPException, status
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession


class AuthService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.user_repo = UserRepository(session)

    async def _generate_and_save_tokens(self, user: User):
        """Внутренний метод для генерации и сохранения токенов в БД"""
        access_token = create_access_token(data={"sub": str(user.id), "role": user.role.value})
        refresh_token = create_refresh_token(data={"sub": str(user.id)})

        # удаляем старый токен и сохраняем новый
        await self.session.execute(delete(RefreshToken).where(RefreshToken.user_id == user.id))

        new_refresh = RefreshToken(
            user_id=user.id,
            token=refresh_token,
            expires_at=datetime.now(UTC) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS),
        )
        self.session.add(new_refresh)
        await self.session.commit()

        return {"access_token": access_token, "refresh_token": refresh_token}

    async def register_user(self, username: str, password: str):
        existing_user = await self.user_repo.find_one_or_none(username=username)
        if existing_user:
            raise HTTPException(status_code=400, detail="Пользователь уже существует")

        hashed_password = get_password_hash(password)

        new_user = await self.user_repo.add(username=username, hashed_password=hashed_password, role=UserRole.USER)
        await self.session.commit()
        return new_user

    async def authenticate_user(self, username: str, password: str):
        user = await self.user_repo.find_one_or_none(username=username)
        if not user or not verify_password(password, user.hashed_password):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Неверное имя пользователя или пароль")

        return await self._generate_and_save_tokens(user)

    async def refresh_tokens(self, refresh_token: str):
        payload = decode_token(refresh_token)
        if not payload or payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Неверный refresh токен")

        query = select(RefreshToken).where(RefreshToken.token == refresh_token)
        result = await self.session.execute(query)
        token_db = result.scalar_one_or_none()

        if not token_db:
            raise HTTPException(status_code=401, detail="Refresh токен не найден")

        if token_db.expires_at < datetime.now(UTC):
            raise HTTPException(status_code=401, detail="Refresh токен истек")

        user = await self.user_repo.find_one_or_none(id=token_db.user_id)
        if not user:
            raise HTTPException(status_code=401, detail="Пользователь не найден")

        return await self._generate_and_save_tokens(user)
