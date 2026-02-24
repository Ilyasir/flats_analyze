import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.datasets import GOLD_DATASET_HISTORY

OWNER = "ilyas"
DAG_ID = "gold_mart_district_history"

SHORT_DESCRIPTION = "Расчет исторической динамики цен по районам"

LONG_DESCRIPTION = """
Этот DAG Формирует временные ряды для анализа изменения цен по районам
рассчитывает состояние рынка на конкретную дату (`{{ ds }}`).
Он заглядывает в таблицу с SCD2 (`history_flats`) и выбирает те записи, которые были активны в указанный день.

Идемпотентность. Используется `DELETE` по `report_date` перед вставкой.
Это позволяет перезапускать даг за любую прошлую дату, дублей не будет
"""


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


with DAG(
    dag_id=DAG_ID,
    schedule=[GOLD_DATASET_HISTORY],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["marts", "gold", "pg"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )
    # удаляем данные за этот день, если они уже были (для перезапусков)
    clear_mart = SQLExecuteQueryOperator(
        task_id="clear_mart",
        conn_id="pg_conn",
        sql="""
            DELETE FROM gold.dm_district_history
            WHERE report_date = '{{ ds }}'::DATE;  
            """,
    )

    build_dm_district_history = SQLExecuteQueryOperator(
        task_id="build_dm_district_history",
        conn_id="pg_conn",
        autocommit=True,
        sql="""
            INSERT INTO gold.dm_district_history (
                report_date, okrug, district, total_flats, 
                avg_price_per_meter, median_price_per_meter
            )
            select
                '{{ ds }}'::DATE as report_date,
                okrug,
                district,
                count(*) as total_flats,
                ROUND(avg(price / area))::BIGINT as avg_price_per_meter,
                round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price / area))::BIGINT as median_price_per_meter
            from gold.history_flats as hf
            -- Выбираем записи, которые были "живы" на дату расчета ({{ ds }})
            where '{{ ds }}'::DATE >= effective_from::DATE 
            AND '{{ ds }}'::DATE < effective_to::DATE
            group by okrug, district
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> clear_mart >> build_dm_district_history >> end
