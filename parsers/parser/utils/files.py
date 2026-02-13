import json


async def save_to_file_object(item: dict, file_object) -> None:
    """Сохраняет словарь в файл в формате JSONL (в нем одна строка - это один json объект)"""
    line = json.dumps(item, ensure_ascii=False) + "\n"
    await file_object.write(line)
