from pathlib import Path

from jinja2 import Template

# dags/sql/
sql_dir = Path(__file__).parent.parent / "sql"


def load_sql(file_name: str, **params) -> str:
    """Загружает SQL запрос из папки sql_dir и подставляет параметры"""
    file_path = sql_dir / file_name  # формируем полный путь к SQL файлу внутри папки
    # чекаем, есть ли файл
    if not file_path.exists():
        raise FileNotFoundError(f"SQL файл не найден: {file_path}")
    sql_template = file_path.read_text(encoding="utf-8")  # читаем как строку
    # подставляем через jinja
    return Template(sql_template).render(**params)
