from bs4 import BeautifulSoup
from playwright.async_api import Page
from utils.normalize import ( 
    price_to_int,
    area_to_float,
    underground_minutes_to_int,
    year_built_to_int,
    floors_str_to_tuple
)

def get_title(soup: BeautifulSoup) -> str | None:
    item = soup.find("div", {"data-name": "OfferTitleNew"})
    if item:
        title_h1 = item.find("h1")
        return title_h1.get_text(strip=True) if title_h1 else None
    return None


def get_price(soup: BeautifulSoup) -> int | None:
    item = soup.find("div", {"data-testid": "price-amount"})
    return price_to_int(item.get_text()) if item else None


def get_total_area(soup: BeautifulSoup) -> float | None:
    items = soup.find_all("div", {"data-name": "OfferSummaryInfoItem"})
    for item in items:
        if "Общая площадь" in item.get_text():
            val = item.find_all("p")
            if len(val) > 1:
                return area_to_float(val[1].get_text())
    return None


def get_living_area(soup: BeautifulSoup) -> float | None:
    items = soup.find_all("div", {"data-name": "OfferSummaryInfoItem"})
    for item in items:
        if "Жилая площадь" in item.get_text():
            val = item.find_all("p")
            if len(val) > 1:
                return area_to_float(val[1].get_text())
    return None


def get_floors(soup: BeautifulSoup) -> tuple[int | None, int | None]:
    items = soup.find_all("div", {"data-name": "ObjectFactoidsItem"})
    
    for item in items:
        if "Этаж" in item.get_text():
            floors_str = item.get_text(separator=" ", strip=True)
            return floors_str_to_tuple(floors_str)
            
    return (None, None)


def get_address(soup: BeautifulSoup) -> str | None:
    container = soup.find("div", {"data-name": "AddressContainer"})
    
    if not container:
        return None
    
    address_text = container.get_text(separator="", strip=True)
    clean_address = address_text.replace("На карте", "").replace(",", ", ").strip()
    
    return clean_address


def get_repair(soup: BeautifulSoup) -> str | None:
    items = soup.find_all("div", {"data-name": "OfferSummaryInfoItem"})
    
    for item in items:
        if "Ремонт" in item.get_text():
            val = item.find_all("p")
            if len(val) > 1:
                return val[1].get_text(strip=True)
                
    return None


def get_description(soup: BeautifulSoup) -> str:
    container = soup.find("div", {"data-id": "content"})
    
    if not container:
        return ""
    
    desc_span = container.find("span")
    
    if desc_span:
        return desc_span.get_text(separator="\n", strip=True)
        
    return ""


def get_year_built(soup: BeautifulSoup) -> int | None:
    items = soup.find_all("div", {"data-name": ["ObjectFactoidsItem", "OfferSummaryInfoItem"]})

    for item in items:
        if "Год постройки" in item.get_text():
            values = item.find_all(["span", "p"])
            if len(values) > 1:
                year_text = values[1].get_text(strip=True)
                return year_built_to_int(year_text)
    
    return None


def get_building_type(soup: BeautifulSoup) -> str | None:
    items = soup.find_all("div", {"data-name": "OfferSummaryInfoItem"})

    for item in items:
        if "Тип дома" in item.get_text():
            values = item.find_all("p")
            if len(values) > 1:
                return values[1].get_text(strip=True)
                
    return None


def get_underground(soup: BeautifulSoup) -> dict | None:
    ul = soup.find("ul", {"data-name": "UndergroundList"})
    
    if not ul:
        return None

    item = ul.find("li", {"data-name": "UndergroundItem"})
    
    if not item:
        return None

    try:

        name_tag = item.find("a")
        name = name_tag.get_text(strip=True) if name_tag else "Неизвестно"
        
        spans = item.find_all("span")
        minutes_text = ""
        for s in spans:
            if "мин" in s.get_text():
                minutes_text = s.get_text()
                break
        
        return {
            "name": name, 
            "minutes": underground_minutes_to_int(minutes_text)
        }
    except Exception:
        return None


async def parse_flat_page(page: Page, link: str, rooms: int) -> dict:
    """Парсит страницу квартиры и возвращает словарь с данными."""
    content = await page.content()
    soup = BeautifulSoup(content, 'html.parser')

    floors_tuple = get_floors(soup)
    floor, floors_total = floors_tuple

    result = {
        "link": link,
        "title": get_title(soup),
        "price": get_price(soup),
        "rooms": rooms,
        "total_area": get_total_area(soup),
        "living_area": get_living_area(soup),
        "address": get_address(soup),
        "floor": floor,
        "floors_total": floors_total,
        "description": get_description(soup),
        "repair": get_repair(soup),
        "year_built": get_year_built(soup),
        "building_type": get_building_type(soup),
        "underground": get_underground(soup)
    }

    return result