import asyncio
import random
from playwright.async_api import Page
from core.logger import setup_logger
from core import config

logger = setup_logger()


async def block_heavy_resources(page: Page):
    """Отключает загрузку тяжелых ресурсов(картинок и стилей) для экономии трафика и скорости."""
    def route_handler(route):

        if route.request.resource_type in ["image", "font", "media", "stylesheet"]:
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
                user_agent=random.choice(config.USER_AGENTS),
                extra_http_headers=config.DEFAULT_HEADERS
            )
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=config.PAGE_TIMEOUT)
            
            for page_num in range(config.MAX_PAGES_TO_PARSE):
                cards = page.locator("article:has([data-mark='OfferTitle'])")
                total_cards = await cards.count()
                # Собирает ссылки с текущей страницы
                for i in range(total_cards):
                    card = cards.nth(i)
                    link = await card.locator("a[href]").first.get_attribute("href")
                    flats_main_data.append((link, rooms))

                logger.info(f"Категория {url} стр. {page_num + 1} - квартир: {total_cards}")
                # переход на следующую страницу, если есть, нажать кнопку "Дальше"
                if page_num + 1 < config.MAX_PAGES_TO_PARSE:
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


async def collect_main_flats_data(urls_data: list[dict], browser) -> list[tuple[str, int]]:
    """Проходит по всем категориям и собирает основные данные по квартирам."""
    semaphore = asyncio.Semaphore(5)
    flats_main_data = []
    
    tasks = [
        collect_flats_main_data_from_url(browser, item["url"], item["rooms"], flats_main_data, semaphore) 
        for item in urls_data
    ]

    await asyncio.gather(*tasks)

    unique_flats = list(set(flats_main_data))
    logger.info(f"Сбор завершен. Уникальных квартир: {len(unique_flats)}")
    return unique_flats