import duckdb
from airflow.hooks.base import BaseHook
from airflow.utils.log.secrets_masker import mask_secret


def connect_duckdb_to_s3(con: duckdb.DuckDBPyConnection, conn_id: str = "s3_conn") -> duckdb.DuckDBPyConnection:
    """Получение подключения к S3 для duckdb через Airflow Connection"""
    s3_conn = BaseHook.get_connection(conn_id)
    # маскируем ключи, чтобы в логах аирфлоу их не было видно
    mask_secret(s3_conn.login)
    mask_secret(s3_conn.password)
    # параметры подключения
    access_key = s3_conn.login
    secret_key = s3_conn.password
    endpoint_with_protocol = s3_conn.extra_dejson.get("endpoint_url", "")
    url_style = s3_conn.extra_dejson.get("addressing_style", "path")
    region = s3_conn.extra_dejson.get("region_name", "ru-central1")

    # определяем нужно ли использовать SSL по протоколу в endpoint
    use_ssl = "true" if endpoint_with_protocol.startswith("https") else "false"
    # убираем протокол, duckdb сам подставляет его
    endpoint = endpoint_with_protocol.replace("http://", "").replace("https://", "")

    # указываем папку с расширениями для duckdb и загружаем httpfs для работы с S3
    con.execute("SET extension_directory = '/opt/airflow/duckdb_extensions';")
    con.execute("LOAD httpfs;")

    con.execute(f"""
        SET TimeZone = 'Europe/Moscow';
        SET s3_url_style = '{url_style}';
        SET s3_endpoint = '{endpoint}';
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_region = '{region}';
        SET s3_use_ssl = {use_ssl};
    """)
    return con


def connect_duckdb_to_pg(con: duckdb.DuckDBPyConnection, conn_id: str = "pg_conn") -> duckdb.DuckDBPyConnection:
    """Подключение расширения postgres и создание секрета внутри duckdb"""
    pg_conn = BaseHook.get_connection(conn_id)
    mask_secret(pg_conn.login)
    mask_secret(pg_conn.password)

    con.execute(f"""
        LOAD postgres;
        CREATE SECRET IF NOT EXISTS dwh_postgres (
            TYPE postgres,
            HOST '{pg_conn.host}',
            PORT {pg_conn.port},
            DATABASE '{pg_conn.schema}',
            USER '{pg_conn.login}',
            PASSWORD '{pg_conn.password}'
        );
    """)
    con.execute("ATTACH '' AS flats_db (TYPE postgres, SECRET dwh_postgres);")
    return con
