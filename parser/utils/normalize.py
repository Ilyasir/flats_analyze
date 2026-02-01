import re

def extract_cian_id(url: str) -> int | None:
    """Извлекает ID из ссылки и возвращает его как целое число (int)."""
    if not url:
        return None
    
    match = re.search(r'/(\d{7,15})', url)
    
    if match:
        return int(match.group(1))
    
    return None


def extract_area(title: str) -> float:
    match = re.search(r'([\d,.]+)\s*м²', title)
    if match:
        area_str = match.group(1).replace(',', '.')
        return float(area_str)
    return 0.0


def extract_floor(title: str) -> int:
    match = re.search(r'(\d+)/\d+\s*этаж', title)
    if match:
        return int(match.group(1))
    return 0


def extract_total_floors(title: str) -> int:
    match = re.search(r'\d+/(\d+)\s*этаж', title)
    if match:
        return int(match.group(1))
    return 0