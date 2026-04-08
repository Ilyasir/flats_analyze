import pytest
from app.ml.service import s3_service


@pytest.mark.asyncio
async def test_ml_prediction_format():

    features = [
        "False",
        "False",
        60.0,
        2,
        3,
        10,
        "False",
        "False",
        "False",
        "ЮАО",
        "Даниловский",
        10,
        0.3,
        20.0,
        "False",
    ]

    if s3_service.model is None:
        pytest.skip("Не удалось загрузить модель из S3 для теста")

    prediction = s3_service.predict([features])
    assert prediction is not None
    assert prediction[0] > 0
