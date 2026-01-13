import os
from pathlib import Path


CONCURRENT_TASKS: int = 7  # максимальное количество одновременных задач парсинга(вкладок браузера)
MAX_PAGES_TO_PARSE: int = 1 # максимальное количество страниц с объявлениями для парсинга в каждой категории
PAGE_TIMEOUT: int = 40000 # время ожидания загрузки страницы (в миллисекундах)
MIN_SLEEP: float = 2.0 # минимальная задержка между запросами к страницам (в секундах)
MAX_SLEEP: float = 5.0 # максимальная задержка между запросами к страницам (в секундах)

CAPCHA_BLOCK_TEXT: str = "text=Кажется, у вас включён VPN" # текст, по которому определяется блокировка/капча

# ссылки на категории квартир ( 0 - студии, 1 - однушки, 2 - двушки, и т.д.)
URLS: list[str] = [
   "https://www.cian.ru/cat.php?deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&object_type%5B0%5D=1&offer_type=flat&region=1&room9=1&sort=creation_date_desc",

   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=12200000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room1=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=15000000&minprice=12200000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room1=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=20000000&minprice=15000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room1=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&minprice=20000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room1=1&sort=creation_date_desc",

    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=15000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=18000000&minprice=15000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=22000000&minprice=18000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=27000000&minprice=22000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=36000000&minprice=27000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&minprice=36000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room2=1&sort=creation_date_desc",

   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=20000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room3=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=25000000&minprice=20000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room3=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=33000000&minprice=25000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room3=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=44000000&minprice=33000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room3=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=67000000&minprice=44000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room3=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&minprice=67000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room3=1&sort=creation_date_desc",

   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&maxprice=75000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room4=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?currency=2&deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&minprice=75000000&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room4=1&sort=creation_date_desc",
    
   "https://www.cian.ru/cat.php?deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room5=1&sort=creation_date_desc",
   "https://www.cian.ru/cat.php?deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&include_new_moscow=0&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room6=1&sort=creation_date_desc"
]

HEADLESS: bool = True # режим запуска браузера (безголовый или с интерфейсом)
# Заголовки для маскировки под реальный браузер
DEFAULT_HEADERS: dict[str, str] = {
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}
# Список User-Agent для рандомизации запросов
USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; rv:115.0) Gecko/20100101 Firefox/115.0",

    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 15.1; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:128.0) Gecko/20100101 Firefox/128.0",
    
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (X11; Linux i686; rv:130.0) Gecko/20100101 Firefox/130.0",
    
    "Mozilla/5.0 (X11; Debian; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
]


BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
# Папка для данных
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)