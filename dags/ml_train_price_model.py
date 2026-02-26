import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from utils.duckdb import connect_duckdb_to_pg, connect_duckdb_to_s3

OWNER = "ilyas"
DAG_ID = "ml_train_price_model"

SHORT_DESCRIPTION = ""

LONG_DESCRIPTION = """
"""


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def get_ml_dataset_from_pg_to_s3(**context):
    dt = context["data_interval_end"].in_timezone("Europe/Moscow")
    dataset_s3_key = f"s3://ml-data/datasets/dataset_{dt.format('YYYY-MM-DD')}.parquet"

    con = duckdb.connect()
    connect_duckdb_to_s3(con, "s3_conn")
    connect_duckdb_to_pg(con, "pg_conn")
    try:
        logging.info(f"Начинаю загрузку датасета из pg в {dataset_s3_key}")
        con.execute(
            f"""
            COPY (
                WITH base_table as (
                    select
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
                        CASE
                            WHEN metro_type = 'walk' THEN metro_min
                            ELSE metro_min * 5
                        END as metro_min
                    from flats_db.gold.history_flats
                    where metro_min is not null
                )
                select
                    * EXCLUDE (row_num),
                    (floor::DOUBLE / total_floors::DOUBLE) as rel_floor,
                    (area / (rooms_count + 1)) as area_per_room,
                    (total_floors > 18) as is_high_rise
                from base_table
                where row_num = 1
            ) TO '{dataset_s3_key}' (FORMAT PARQUET);
            """
        )
    finally:
        con.close()


with DAG(
    dag_id=DAG_ID,
    schedule=None,
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

    end = EmptyOperator(
        task_id="end",
    )

    start >> prepare_training_dataset >> end
