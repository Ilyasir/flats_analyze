import asyncio

import pytest
from app.main import app
from app.ml.service import s3_service  # Импортируем сервис
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="session", autouse=True)
async def load_test_model():
    # autouse=True значит, что модель загрузится один раз перед всеми тестами
    if s3_service.model is None:
        await s3_service.load_model()


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def ac():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
