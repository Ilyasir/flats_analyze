import pytest
from httpx._client import AsyncClient


@pytest.mark.asyncio
async def test_get_flats_unauthorized(ac: AsyncClient):
    response = await ac.get("/flats")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert isinstance(data["items"], list)


@pytest.mark.asyncio
async def test_admin_stats_protected(ac: AsyncClient):
    response = await ac.get("/admin/users")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_predict_validation_error(ac: AsyncClient):
    bad_payload = {"area": "очень мало", "rooms_count": 2}
    response = await ac.post("/flats/predict", json=bad_payload)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_full_predict_flow(ac: AsyncClient):
    payload = {
        "area": 55.5,
        "rooms_count": 2,
        "floor": 5,
        "total_floors": 12,
        "metro_min": 10,
        "is_apartament": False,
        "is_studio": False,
        "is_new_moscow": False,
        "okrug": "ЮАО",
        "district": "Даниловский",
    }

    response = await ac.post("/flats/predict", json=payload)

    assert response.status_code == 200
    res_data = response.json()
    assert "total_price" in res_data
    assert "price_per_meter" in res_data
    assert res_data["total_price"] > 0
