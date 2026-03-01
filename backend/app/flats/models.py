from datetime import datetime
from enum import Enum

from app.database import Base
from sqlalchemy import BigInteger, Boolean, DateTime, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import ENUM as PG_ENUM
from sqlalchemy.orm import Mapped, mapped_column


class TransportType(Enum):
    walk = "walk"
    transport = "transport"


class OkrugName(Enum):
    NAO = "НАО"
    TAO = "ТАО"
    CAO = "ЦАО"
    SAO = "САО"
    UAO = "ЮАО"
    ZAO = "ЗАО"
    VAO = "ВАО"
    UZAO = "ЮЗАО"
    UVAO = "ЮВАО"
    SZAO = "СЗАО"
    SVAO = "СВАО"
    ZelAO = "ЗелАО"


class Flat(Base):
    __tablename__ = "history_flats"
    __table_args__ = {"schema": "gold"}

    id: Mapped[int] = mapped_column(primary_key=True)
    flat_hash: Mapped[str] = mapped_column(nullable=False)
    link: Mapped[str] = mapped_column(nullable=False)
    title: Mapped[str] = mapped_column(String(100), nullable=False)
    price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_apartament: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_studio: Mapped[bool] = mapped_column(Boolean, nullable=False)
    area: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    rooms_count: Mapped[int] = mapped_column(Integer, nullable=False)
    floor: Mapped[int] = mapped_column(Integer, nullable=False)
    total_floors: Mapped[int] = mapped_column(Integer, nullable=False)

    # Геоданные
    is_new_moscow: Mapped[bool | None] = mapped_column(Boolean)
    address: Mapped[str] = mapped_column(nullable=False)
    city: Mapped[str] = mapped_column(String(100), nullable=False)
    okrug: Mapped[OkrugName] = mapped_column(
        PG_ENUM(
            OkrugName,
            name="okrug_name",
            schema="gold",
            create_type=False,
            values_callable=lambda obj: [e.value for e in obj],
        )
    )
    district: Mapped[str | None] = mapped_column(String(100))

    # Метро
    metro_name: Mapped[str | None] = mapped_column(String(100))
    metro_min: Mapped[int | None] = mapped_column(Integer)
    metro_type: Mapped[TransportType | None] = mapped_column(
        PG_ENUM(TransportType, name="transport_type", schema="gold", create_type=False)
    )

    # SCD2
    effective_from: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    effective_to: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
