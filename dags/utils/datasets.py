from airflow.datasets import Dataset

# Датасеты для разных слоев
RAW_DATASET_SALES_FLATS = Dataset("s3://raw/sales/flats_data")
SILVER_DATASET_SALES_FLATS = Dataset("s3://silver/sales/flats_data")
GOLD_DATASET_HISTORY = Dataset("pg://postgres_dwh/gold/history_flats")
