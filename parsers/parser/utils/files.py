import json


async def save_to_file_object(item: dict, file_object) -> None:
    line = json.dumps(item, ensure_ascii=False) + "\n"
    await file_object.write(line)
