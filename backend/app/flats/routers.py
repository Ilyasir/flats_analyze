from app.database import get_db
from app.flats.models import OkrugName
from app.flats.repository import FlatRepository
from app.flats.schemas import SFlatList, SFlatResponse
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/flats", tags=["Flats"])


@router.get("/{flat_id}", response_model=SFlatResponse)
async def get_flat(flat_id: int, db: AsyncSession = Depends(get_db)):
    flat_repo = FlatRepository(db)
    flat = await flat_repo.find_one_or_none(id=flat_id)

    if not flat:
        raise HTTPException(status_code=404, detail="Квартира не найдена")

    return flat


@router.get("", response_model=SFlatList)
async def get_flats(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    min_price: int | None = None,
    max_price: int | None = None,
    okrug: OkrugName | None = None,
    rooms: int | None = Query(None, ge=0, le=6),
    order_by: str | None = Query(None),
):
    repo = FlatRepository(db)

    filter_params = {"min_price": min_price, "max_price": max_price, "okrug": okrug, "rooms_count": rooms}

    items = await repo.find_filtered(limit=limit, offset=offset, **filter_params)
    total = await repo.get_count(**filter_params)

    return {"total": total, "limit": limit, "offset": offset, "items": items, "pages": (total + limit - 1) // limit}
