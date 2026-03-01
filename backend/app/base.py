from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession


class BaseRepository:
    model = None

    def __init__(self, session: AsyncSession):
        self.session = session

    async def find_all(self, **filter_by):
        query = select(self.model).filter_by(**filter_by)
        result = await self.session.execute(query)
        return result.scalars().all()

    async def find_one_or_none(self, **filter_by):
        query = select(self.model).filter_by(**filter_by)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def add(self, **data):
        query = insert(self.model).values(**data).returning(self.model)
        result = await self.session.execute(query)
        await self.session.commit()
        return result.scalar_one()

    async def update(self, filter_by: dict, **data):
        query = update(self.model).filter_by(**filter_by).values(**data).returning(self.model)
        result = await self.session.execute(query)
        await self.session.commit()
        return result.scalar_one_or_none()

    async def delete(self, **filter_by):
        query = delete(self.model).filter_by(**filter_by)
        await self.session.execute(query)
        await self.session.commit()
