from app.flats.models import OkrugName, TransportType
from pydantic import BaseModel, ConfigDict


class SFlatResponse(BaseModel):
    id: int
    title: str
    price: int
    area: float
    rooms_count: int
    floor: int
    total_floors: int
    address: str
    okrug: OkrugName
    district: str | None
    metro_name: str | None
    metro_min: int | None
    metro_type: TransportType | None
    link: str
    is_active: bool

    model_config = ConfigDict(from_attributes=True)


class SFlatList(BaseModel):
    total: int
    limit: int
    offset: int
    pages: int
    items: list[SFlatResponse]
