from app.base import BaseRepository
from app.flats.models import Flat, OkrugName
from sqlalchemy import func, select


class FlatRepository(BaseRepository):
    model = Flat

    def _get_filters(
        self,
        min_price: int | None = None,
        max_price: int | None = None,
        okrug: OkrugName | None = None,
        rooms_count: int | None = None,
    ):
        filters = [self.model.is_active]

        if min_price:
            filters.append(self.model.price >= min_price)
        if max_price:
            filters.append(self.model.price <= max_price)
        if okrug:
            filters.append(self.model.okrug == okrug)
        if rooms_count is not None:
            filters.append(self.model.rooms_count == rooms_count)

        return filters

    async def find_filtered(self, limit: int, offset: int, **filters_data):
        filters = self._get_filters(**filters_data)

        query = select(self.model).where(*filters).limit(limit).offset(offset).order_by(self.model.id)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_count(self, **filters_data):
        filters = self._get_filters(**filters_data)

        query = select(func.count()).select_from(self.model).where(*filters)

        result = await self.session.execute(query)
        return result.scalar() or 0
