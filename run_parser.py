import asyncio
import time
import random
from playwright.async_api import async_playwright
from core import config
from parsers.cian import parse_flat_page
from core.logger import setup_logger
from utils.browser import block_heavy_resources, collect_main_flats_data
from utils.files import save_to_jsonl

logger = setup_logger()
semaphore = asyncio.Semaphore(config.CONCURRENT_TASKS)


async def scrape_flat_page(browser, tuple_main_data) -> dict | None:
    """Парсит страницу квартиры и возвращает данные в виде словаря."""
    async with semaphore:
        cian_id, link, rooms = tuple_main_data.cian_id, tuple_main_data.link, tuple_main_data.rooms

        context = await browser.new_context(
            user_agent=random.choice(config.USER_AGENTS),
            extra_http_headers=config.DEFAULT_HEADERS
        )
        page = await context.new_page()
        await block_heavy_resources(page)

        try:
            # небольшая случайная задержка перед загрузкой страницы
            await asyncio.sleep(random.uniform(config.MIN_SLEEP, config.MAX_SLEEP))
            await page.goto(link, wait_until="domcontentloaded", timeout=30000)

            if await page.locator(config.CAPCHA_BLOCK_TEXT).count() > 0: # проверить на капчу/блокировку
                logger.warning(f"Обнаружена блокировка/капча на: {link}")
                return None

            flat = await asyncio.wait_for(
                parse_flat_page(page, cian_id, link, rooms),
                timeout=15)
            
            # проверить наличие обязательных полей (цена и заголовок)
            if not flat.get("price") or not flat.get("title"):
                logger.warning(f"Пропускаю объявление {link}: отсутствует цена или заголовок")
                return None
            
            return flat

        except Exception as e:
            logger.error(f"Ошибка при парсинге {link}: {e}")
            return None
        
        finally:
            await page.close()
            await context.close()


async def track_progress(tasks: list, filename: str) -> int:
    """Отслеживает прогресс выполнения задач и сохраняет результаты в файл."""
    parsed_count = 0
    failed_count = 0
    total = len(tasks)

    for coro in asyncio.as_completed(tasks):
        result = await coro
        
        if result:
            parsed_count += 1
            await save_to_jsonl(result, filename)
        else:
            failed_count += 1

        logger.info(
            f"Прогресс: {parsed_count + failed_count}/{total} | OK: {parsed_count} | FAIL: {failed_count}")
    return parsed_count


async def main():
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=config.HEADLESS)

        logger.info("Начинаю сбор ссылок...")
        flats_data = await collect_main_flats_data(config.URLS, browser)  # собрать основную инфу о квартирах

        if not flats_data:
            logger.error("Ссылки не найдены!")
            await browser.close()
            return 0

        tasks = [
            scrape_flat_page(browser, tuple_main_data)
            for tuple_main_data in flats_data
        ]

        output_file = config.DATA_DIR / f"data_{int(time.time())}.jsonl"
        count = await track_progress(tasks, output_file)

        await browser.close()
        return count, output_file


if __name__ == "__main__":
    start_time = time.time()
    try:
        count, file_path = asyncio.run(main())
        duration = time.time() - start_time
        logger.info(f"Процесс завершен. Спарсено: {count} за {duration:.2f} сек. Файл: {file_path}")
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")