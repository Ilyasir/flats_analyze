import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from utils.datasets import GOLD_DATASET_HISTORY
from utils.duckdb import connect_duckdb_to_pg, connect_duckdb_to_s3

OWNER = "ilyas"
DAG_ID = "ml_train_price_model"

SHORT_DESCRIPTION = "Пайплайн обучения catboost модели для оценки стоимости квадратного метра"

LONG_DESCRIPTION = """
### ML Training Pipeline

#### Таски:
**Prepare Training Dataset**:
    - Использует duckdb.
    - Извлекает данные из DWH `gold.history_flats`.
    - Использует оконные функции для дедубликации и выбора актуальных цен квартир (даже снятых с публикации).
    - Собирает и создает признаки.
    - Сохраняет датасет в S3 в формате parquet.
**Train Model**:
    - Запускает Docker-контейнер с CatBoost.
    - Обучает регрессию на скачанном из S3 датасете.
    - Загружает готовую модель (`.cbm`) обратно в S3.
    - Пишет логи и метрики в Airflow.
"""

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def get_ml_dataset_from_pg_to_s3(**context):
    """Берет данные из gold слоя и подготавливает датасет с новыми признаками для обучения, и в S3 грузит"""
    dt = context["data_interval_end"].in_timezone("Europe/Moscow")
    dataset_s3_key = f"s3://ml-data/datasets/dataset_{dt.format('YYYY-MM-DD')}.parquet"
    model_s3_key = f"s3://ml-data/models/model_{dt.format('YYYY-MM-DD')}.cbm"

    con = duckdb.connect()
    connect_duckdb_to_s3(con, "s3_conn")
    connect_duckdb_to_pg(con, "pg_conn")
    try:
        logging.info(f"💻 Начинаю подготовку признаков и загрузку датасета в {dataset_s3_key}")
        con.execute(
            f"""
            COPY (
                WITH base_table as (
                    select
                        -- самые свежие записи, убираем дубли со старыми ценами
                        row_number() OVER(
                            partition by flat_hash
                            order by effective_to desc
                        ) as row_num,
                        round((price / area)) as price_per_meter,
                        is_apartament,
                        is_studio,
                        area::DOUBLE as area,
                        rooms_count,
                        floor,
                        total_floors,
                        (floor = 1) as is_first_floor,
                        (floor = total_floors) as is_last_floor,
                        is_new_moscow,
                        okrug,
                        district,
                        -- нормализация времени до метро
                        CASE
                            WHEN metro_type = 'walk' THEN metro_min
                            ELSE metro_min * 5 -- примерно, умнножаем на 5 для трансопрта
                        END as metro_min,
                        flat_hash
                    from flats_db.gold.history_flats
                    where metro_min is not null
                )
                select
                    * EXCLUDE (row_num, flat_hash),
                    -- доп. признаки для модели 
                    (floor::DOUBLE / total_floors::DOUBLE) as rel_floor,
                    (area / (rooms_count + 1)) as area_per_room,
                    (total_floors > 18) as is_high_rise
                from base_table
                where row_num = 1
                -- чтобы порядок в датасете был стабильный, иначе метрики скачут на тех же данных
                order by flat_hash
            ) TO '{dataset_s3_key}' (FORMAT PARQUET);
            """
        )
    finally:
        con.close()
    logging.info(f"✅ Датасет подготовлен и загружен в {dataset_s3_key}")

    return {
        "dataset_s3_key": dataset_s3_key,
        "model_s3_key": model_s3_key,
    }


with DAG(
    dag_id=DAG_ID,
    schedule=[GOLD_DATASET_HISTORY],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ml", "gold", "s3"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    prepare_training_dataset = PythonOperator(
        task_id="prepare_training_dataset",
        python_callable=get_ml_dataset_from_pg_to_s3,
    )

    # обучение модели в контейнере с catboost
    train_model = DockerOperator(
        task_id="train_model",
        image="catboost_train:latest",
        container_name="catboost_train_container",
        api_version="auto",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="data_network",
        mount_tmp_dir=False,
        tty=True,
        mem_limit="1g",  # для catboost 1 хватит
        environment={
            # для доступа к S3 из контейнера
            "S3_ACCESS_KEY": "{{ conn.s3_conn.login }}",
            "S3_SECRET_KEY": "{{ conn.s3_conn.password }}",
            "S3_ENDPOINT_URL": "{{ conn.s3_conn.extra_dejson.endpoint_url }}",
            "S3_REGION_NAME": "{{ conn.s3_conn.extra_dejson.region_name }}",
            "S3_BUCKET_NAME": "ml-data",
            # берем ключи из xcoms прошлой таски, передаем в конейнер, потом он скачает датасет и загрузит модель
            "DATASET_S3_KEY": "{{ task_instance.xcom_pull(task_ids='prepare_training_dataset')['dataset_s3_key'] }}",
            "MODEL_S3_KEY": "{{ task_instance.xcom_pull(task_ids='prepare_training_dataset')['model_s3_key'] }}",
        },
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> prepare_training_dataset >> train_model >> end
