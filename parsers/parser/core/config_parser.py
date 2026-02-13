CONCURRENT_TASKS: int = 3  # макс. колво одновременных открытых вкладок браузера
MAX_PAGES_TO_PARSE: int = 60  # макс. колво страниц для парсинга с каждого URL (всего старниц на один URL примерно 50)
HEADLESS: bool = True  # режим запуска браузера (безголовый или с интерфейсом)
CAPCHA_BLOCK_TEXT: str = "text=Кажется, у вас включён VPN"  # текст, по которому определяется блокировка/капча

# Список URL для парсинга (сплит по комнатности и ценам)
URLS: list[str] = [
    # Студии
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=11000000&object_type%5B0%5D=1&offer_type=flat&region=1&room9=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=11000000&object_type%5B0%5D=1&offer_type=flat&region=1&room9=1&sort=creation_date_desc",
    # 1 комн.
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=11500000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=13000000&minprice=11500000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=14700000&minprice=13000000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=17000000&minprice=14700000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=20000000&minprice=17000000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=26000000&minprice=20000000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=26000000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    # 2 комн.
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=13500000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=15500000&minprice=13500000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=17000000&minprice=15500000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=19000000&minprice=17000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=21500000&minprice=19000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=24000000&minprice=21500000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=28000000&minprice=24000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=33000000&minprice=28000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=40000000&minprice=33000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=55000000&minprice=40000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=55000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    # 3 комн.
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=18500000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=23000000&minprice=18500000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=27000000&minprice=23000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=33000000&minprice=27000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=40000000&minprice=33000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=48500000&minprice=40000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=62000000&minprice=48500000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=100000000&minprice=62000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=100000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    # 4 комн.
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=55000000&object_type%5B0%5D=1&offer_type=flat&region=1&room4=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=120000000&minprice=55000000&object_type%5B0%5D=1&offer_type=flat&region=1&room4=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=120000000&object_type%5B0%5D=1&offer_type=flat&region=1&room4=1&sort=creation_date_desc",
    # 5 комн.
    "https://www.cian.ru/cat.php?deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&object_type%5B0%5D=1&offer_type=flat&region=1&room5=1&sort=creation_date_desc",
    # 6 комн.
    "https://www.cian.ru/cat.php?deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&object_type%5B0%5D=1&offer_type=flat&region=1&room6=1&sort=creation_date_desc",
]

# Список User-Agent для рандомизации при парсинге и маскировки под реальный браузер
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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
]

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
