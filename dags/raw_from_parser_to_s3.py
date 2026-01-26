import logging
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException

OWNER = "ilyas"
DAG_ID = "raw_from_parser_to_s3"

LAYER = "raw"
SOURCE = "cian"

SHORT_DESCRIPTION = "DAG запускает контейнер парсера для сбора объявлений о квартирах и сохраняет результат в S3 в формате .jsonl"

default_args = {
    'owner': OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    'retries': 2,
    "retry_delay": pendulum.duration(hours=1),
}


def get_s3_key(source, dt) -> str:
    year = dt.year
    month = dt.strftime('%m')
    day = dt.strftime('%d')
    
    return f"{source}/year={year}/month={month}/day={day}/flats.jsonl"


def check_file_in_s3(data_interval_start):
    dt = data_interval_start.in_timezone('Europe/Moscow')

    s3_key = get_s3_key(SOURCE, dt)
    bucket_name = LAYER
    
    hook = S3Hook(aws_conn_id='s3_conn')
    
    if not hook.check_for_key(s3_key, bucket_name):
        logging.error(f"❌ Файл {s3_key} не найден в бакете {bucket_name}!")
        raise AirflowFailException("Файл не найден!")

    file_obj = hook.get_key(s3_key, bucket_name)
    file_size = file_obj.content_length
    
    if file_size < 1000000:
        logging.warning(f"⚠️ Файл {s3_key} найден, но он маленький ({file_size} байт)!")
        raise AirflowFailException(f"Размер файла слишком мал!")
    
    logging.info(f"✅ Проверка пройдена! Файл найден, размер: {round(file_size / 1000000, 1)} MB.")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 1 * * *",
    default_args=default_args,
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    run_parser = DockerOperator(
        task_id='run_flats_parser',
        image='flats-parser:1.0',
        api_version='auto',
        auto_remove='success',
        docker_url="unix://var/run/docker.sock",
        network_mode="flats_analyze_default",
        tty=True,
        mem_limit='4g',
        shm_size='2g',
        environment={
            'MINIO_ACCESS_KEY': "{{ conn.s3_conn.login }}", # login из Connections
            'MINIO_SECRET_KEY': "{{ conn.s3_conn.password }}", # password из Connections
            'MINIO_ENDPOINT_URL': 'http://minio:9000',
            'MINIO_BUCKET_NAME': LAYER,
            'TZ': 'Europe/Moscow',
            'EXECUTION_DATE': "{{ data_interval_start.in_timezone('Europe/Moscow').format('YYYY-MM-DD') }}",
        }
    )

    check_data = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_file_in_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> run_parser >> check_data >> end