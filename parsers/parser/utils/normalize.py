import re

def _extract_digits(text: str | None) -> str:
    """Извлекает и возвращает все цифры из строки"""
    if not text:
        return ""
    return re.sub(r"\D", "", text)

def price_to_int(text: str | None) -> int | None:
    digits = _extract_digits(text)
    return int(digits) if digits else None

def area_to_float(area_str: str | None) -> float | None:
    if not area_str:
        return None

    cleaned = area_str.replace("\xa0", "").replace("м²", "").replace("м", "").replace(",", ".").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None

def underground_minutes_to_int(minutes_str: str | None) -> int | None:
    digits = _extract_digits(minutes_str)
    return int(digits) if digits else None

def year_built_to_int(year_str: str | None) -> int | None:
    if not year_str:
        return None
    # год. 4 цифры подряд, начинающиеся на 18, 19 или 20
    match = re.search(r"\b(18|19|20)\d{2}\b", year_str)
    return int(match.group()) if match else None

def floors_str_to_tuple(floors_str: str | None) -> tuple[int | None, int | None]:
    if not floors_str:
        return None, None

    text = floors_str.replace("\xa0", " ").lower()

    # если "5 из 10" или "5/10"
    match = re.search(r"(\d+)\s*(?:из|/)\s*(\d+)", text)
    if match:
        return int(match.group(1)), int(match.group(2))

    # и если просто "5" без всех этажей
    digits = _extract_digits(text)
    return (int(digits), None) if digits else (None, None)


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


def extract_rooms(title_text: str) -> int:
    """Извлекает количество комнат из заголовка. Студия = 0."""
    if not title_text:
        return -1 # Или None, если данных нет
        
    title_low = title_text.lower()
    
    if "студия" in title_low:
        return 0
    
    match = re.search(r'(\d+)-комн', title_low)
    if match:
        return int(match.group(1))
        
    first_part = title_low.split(',')[0]
    digit_match = re.search(r'(\d+)', first_part)
    if digit_match:
        return int(digit_match.group(1))
        
    return -1


def extract_metro_name(metro_info: str) -> str | None:
    """Выделяет название метро (все до первой цифры)."""
    if not metro_info:
        return None
    match = re.search(r'^([^0-9]+)', metro_info)
    if match:
        return match.group(1).strip()
    return metro_info.strip()

def extract_metro_minutes(metro_info: str) -> int | None:
    """Выделяет количество минут (первое число в строке)."""
    if not metro_info:
        return None
    match = re.search(r'(\d+)', metro_info)
    if match:
        return int(match.group(1))
    return None

def extract_metro_transport(metro_info: str) -> str:
    """Определяет тип транспорта (пешком или на транспорте)."""
    if not metro_info:
        return "не указано"
    info_low = metro_info.lower()
    if "пешком" in info_low:
        return "пешком"
    elif "транспорт" in info_low or "машин" in info_low:
        return "на транспорте"
    return "неизвестно"