import asyncio
import random
from playwright.async_api import Page
from parser.core import config_parser
from parser.core.logger import setup_logger
from parser.utils.normalize import extract_cian_id
from collections import namedtuple

Flat_data = namedtuple("Flat_data", ["cian_id", "link", "rooms"])

logger = setup_logger()


async def block_heavy_resources(page: Page):
    """Отключает загрузку тяжелых ресурсов(картинок и стилей) для экономии трафика и скорости."""
    def route_handler(route):

        if route.request.resource_type in ["image", "font", "media"]:
            return route.abort()
        return route.continue_()

    await page.route("**/*", route_handler)


async def collect_flats_main_data_from_url(
        browser,
        url: str,
        rooms: int,
        flats_main_data: list,
        semaphore: asyncio.Semaphore
    ):
    """Собирает основные данные (ссылки) по квартирам с указанной страницы категории."""
    async with semaphore:
        context = await browser.new_context(
                user_agent=random.choice(config_parser.USER_AGENTS),
                extra_http_headers=config_parser.DEFAULT_HEADERS
            )
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=config_parser.PAGE_TIMEOUT)
            
            for page_num in range(config_parser.MAX_PAGES_TO_PARSE):
                cards = page.locator("article:has([data-mark='OfferTitle'])")
                total_cards = await cards.count()
                # Собирает ссылки с текущей страницы
                for i in range(total_cards):
                    card = cards.nth(i)
                    link = await card.locator("a[href]").first.get_attribute("href")
                    cian_id = extract_cian_id(link)

                    new_flat_data = Flat_data(cian_id=cian_id, link=link, rooms=rooms)
                    flats_main_data.append(new_flat_data)

                logger.info(f"Категория {url} стр. {page_num + 1} - квартир: {total_cards}")
                # переход на следующую страницу, если есть, нажать кнопку "Дальше"
                if page_num + 1 < config_parser.MAX_PAGES_TO_PARSE:
                    next_button = page.locator("nav[data-name='Pagination']").locator("a").filter(has_text="Дальше")
                    if await next_button.count() > 0:
                        await next_button.click()
                        await page.wait_for_load_state("domcontentloaded")
                        await asyncio.sleep(random.uniform(1.0, 2.0))
                    else:
                        break
        finally:
            logger.info(f"Завершен сбор квартир с категории {url}")
            await context.close()


async def collect_main_flats_data(urls_data: list[dict], browser) -> list[Flat_data]:
    """Проходит по всем категориям и собирает основные данные по квартирам."""
    semaphore = asyncio.Semaphore(5)
    flats_main_data = []
    
    tasks = [
        collect_flats_main_data_from_url(browser, item["url"], item["rooms"], flats_main_data, semaphore) 
        for item in urls_data
    ]

    await asyncio.gather(*tasks)

    unique_flats_main_data = list(set(flats_main_data))
    logger.info(f"Сбор завершен. Уникальных квартир: {len(unique_flats_main_data)}")
    return unique_flats_main_data