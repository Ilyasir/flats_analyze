import os

import boto3
from botocore.client import Config
from parser.core.logger import setup_logger

logger = setup_logger()

ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
SECRET_KEY = os.getenv("S3_SECRET_KEY")
ENDPOINT = os.getenv("S3_ENDPOINT_URL")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "raw")
REGION_NAME = os.getenv("S3_REGION_NAME", "ru-central1")

s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION_NAME,
    config=Config(signature_version="s3v4"),
)


def upload_file_to_s3(file_path: str, object_name: str) -> bool:
    """Загружает файл в S3. True при успешной загрузке, иначе False"""
    try:
        s3_client.upload_file(file_path, BUCKET_NAME, object_name)
        return True
    except Exception as e:
        logger.error(f"Ошибка загрузки {file_path} в S3: {e}")
        return False
