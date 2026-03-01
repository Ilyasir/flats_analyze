from app.database import get_db
from app.users.dependencies import allow_admin, get_current_user
from app.users.models import User, UserRole
from app.users.repository import UserRepository
from app.users.schemas import SUserResponse
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/admin", tags=["Admin"], dependencies=[Depends(allow_admin)])


@router.get("/users/{user_id}", response_model=SUserResponse)
async def get_user(user_id: int, db: AsyncSession = Depends(get_db)):
    user_repo = UserRepository(db)
    user = await user_repo.find_one_or_none(id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return user


@router.get("/users", response_model=list[SUserResponse])
async def get_all_users(db: AsyncSession = Depends(get_db)):
    user_repo = UserRepository(db)
    return await user_repo.find_all()


@router.patch("/users/{user_id}/role")
async def update_user_role(
    user_id: int,
    new_role: UserRole,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):

    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="Нельзя поменять роль у себя!")

    user_repo = UserRepository(db)
    user = await user_repo.find_one_or_none(id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    await user_repo.update({"id": user_id}, role=new_role)
    return {"message": f"Роль пользователя {user.username} (id: {user_id}) изменена на {new_role.value}"}


@router.patch("/users/{user_id}/status")
async def toggle_user_status(
    user_id: int,
    is_active: bool,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="Нельзя забанить самого себя!")

    user_repo = UserRepository(db)
    user = await user_repo.find_one_or_none(id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    await user_repo.update({"id": user_id}, is_active=is_active)
    status_text = "разбанен" if is_active else "забанен"
    return {"message": f"Пользователь {user.username} {status_text}"}


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),  # текущий админ
):
    # чтобы самого себя нельзя было удалить
    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="Нельзя удалить себя!")

    user_repo = UserRepository(db)
    user = await user_repo.find_one_or_none(id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    await user_repo.delete(id=user_id)
    return {"message": f"Пользователь {user.username} (id: {user_id}) удален"}
