import logging

import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.log.secrets_masker import mask_secret
from utils.datasets import SILVER_DATASET_CIAN_FLATS
from utils.duckdb import get_duckdb_s3_connection

OWNER = "ilyas"
DAG_ID = "gold_from_s3_to_pg"

LAYER_SOURCE = "silver"
LAYER_TARGET = "gold"

SHORT_DESCRIPTION = ""

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def load_silver_data_from_s3_to_pg(**context) -> None:
    """Копипаст данных из S3 в stage таблицу postgres, с помощью duckdb"""
    dt = context["data_interval_start"].in_timezone("Europe/Moscow")
    silver_s3_key = (
        f"s3://{LAYER_SOURCE}/cian/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.parquet"
    )

    pg_conn = BaseHook.get_connection("pg_conn")
    mask_secret(pg_conn.login)
    mask_secret(pg_conn.password)
    con = get_duckdb_s3_connection("s3_conn")

    try:
        logging.info(f"💻 Загружаю данные из {silver_s3_key} в stage таблицу")
        con.execute(
            f"""
            LOAD postgres;
            CREATE SECRET IF NOT EXISTS dwh_postgres (
                TYPE postgres,
                HOST '{pg_conn.host}',
                PORT {pg_conn.port},
                DATABASE '{pg_conn.schema}',
                USER '{pg_conn.login}',
                PASSWORD '{pg_conn.password}'
            );

            ATTACH '' AS flats_db (TYPE postgres, SECRET dwh_postgres);

            TRUNCATE TABLE flats_db.gold.stage_flats;
            
            INSERT INTO flats_db.gold.stage_flats (
                flat_id, link, title, price, is_apartament, is_studio, area, 
                rooms_count, floor, total_floors, is_new_moscow, address, 
                city, okrug, district, metro_name, metro_min, metro_type, 
                parsed_at
            )
            SELECT 
                id, link, title, price, is_apartament, is_studio, area, 
                rooms_count, floor, total_floors, is_new_moscow, address, 
                city, okrug::flats_db.gold.okrug_name, district,
                metro_name, metro_min, metro_type::flats_db.gold.transport_type, parsed_at
            FROM read_parquet('{silver_s3_key}');
            """
        )

    finally:
        con.close()
    logging.info("✅ Успешно загружено в stage таблицу")


with DAG(
    dag_id=DAG_ID,
    schedule=[SILVER_DATASET_CIAN_FLATS],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["pg", "gold"],
    description=SHORT_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    load_from_s3_to_pg_stage = PythonOperator(
        task_id="load_from_s3_to_pg_stage", python_callable=load_silver_data_from_s3_to_pg
    )

    merge_from_stage_to_history = SQLExecuteQueryOperator(
        task_id="merge_from_stage_to_history",
        conn_id="pg_conn",
        autocommit=False,
        sql="sql/stage_to_history_scd2.sql",
        show_return_value_in_logs=True,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> load_from_s3_to_pg_stage >> merge_from_stage_to_history >> end
