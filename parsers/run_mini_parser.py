import os
import asyncio
import random
import time
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from parser.core import config_parser
from parser.utils.files import save_to_jsonl
from parser.utils.normalize import extract_cian_id
from parser.utils.browser import block_heavy_resources
from parser.core.logger import setup_logger

logger = setup_logger()


async def collect_flats_from_url(browser, flat_ids: set, url: str, filename):
    context = await browser.new_context(
        user_agent=random.choice(config_parser.USER_AGENTS),
        extra_http_headers=config_parser.DEFAULT_HEADERS
    )
    page = await context.new_page()
    await block_heavy_resources(page)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        
        for page_num in range(60):
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            cards = soup.find_all("article", {"data-name": "CardComponent"}) 
            cards_count = len(cards)

            logger.info(f"🔎 Обрабатываю страницу {page_num + 1}. Квартир - {cards_count} \nURL: {url} ")
            for card in cards:
                try:
                    link_el = card.find("a", href=True)
                    if not link_el: continue
                    link = link_el['href']
                    cian_id = extract_cian_id(link)

                    if cian_id in flat_ids: continue
                    
                    price_el = card.find("span", {"data-mark": "MainPrice"})
                    price_text = price_el.get_text() if price_el else None

                    title_el = card.find("span", {"data-mark": "OfferSubtitle"}) or \
                               card.find("span", {"data-mark": "OfferTitle"})
                    title = title_el.get_text() if title_el else None

                    geo_labels = card.find_all("a", {"data-name": "GeoLabel"})
                    all_geo_texts = [g.get_text() for g in geo_labels]
                    address = ", ".join(all_geo_texts)

                    metro_container = card.find("div", {"data-name": "SpecialGeo"})
                    metro = None
                    if metro_container:
                        metro = metro_container.get_text()

                    flat_data = {
                        "id": cian_id,
                        "link": link,
                        "title": title,
                        "price": price_text,
                        "address": address,
                        "metro": metro,
                        "parsed_at": datetime.now().isoformat()
                    }

                    await save_to_jsonl(flat_data, filename)
                    flat_ids.add(cian_id)

                except Exception as e:
                    logger.error(f"❌ Ошибка при парсинге URL: {url}. На странице: {page_num + 1}. {e}")
                    continue

            if page_num + 1 < 60:
                try:
                    next_button = page.locator("nav[data-name='Pagination'] a").filter(has_text="Дальше")
                    if await next_button.count() > 0:
                        await next_button.click()
                        await page.wait_for_load_state("domcontentloaded")
                        await asyncio.sleep(random.uniform(1.5, 2.5))
                    else:
                        break
                except Exception:
                    logger.warning(f"⚠️ Не нашел кнопку 'Дальше' на странице {page_num + 1}")
                    break
    finally:
        logger.info(f"✅ Завершен сбор с URL: {url}")
        await context.close()


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=config_parser.HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage=false", 
            ]
        )

        semaphore = asyncio.Semaphore(3)
        flat_ids = set()

        date_str = datetime.now().strftime("%Y-%m-%d")

        final_local = f"data/flats_{date_str}.jsonl"
        temp_local = f"data/flats_{date_str}_temp.jsonl"

        # Очищаем временный файл перед стартом
        if os.path.exists(temp_local):
            os.remove(temp_local)

        async def sem_task(url):
            async with semaphore:
                # задержка между стартами потоков
                await asyncio.sleep(random.uniform(1, 4)) 
                return await collect_flats_from_url(
                    browser,
                    flat_ids, 
                    url,
                    temp_local
                )

        tasks = [sem_task(u) for u in config_parser.URLS]
        await asyncio.gather(*tasks, return_exceptions=True)

        if os.path.exists(temp_local):
            os.replace(temp_local, final_local)
            logger.info(f"Данные обновлены в {final_local}")

        await browser.close()
        return len(flat_ids)

if __name__ == "__main__":
    start_time = time.time()
    flats_count = asyncio.run(main())
    duration = time.time() - start_time
    logger.info(f"Спарсено за {duration}. Квартир: {flats_count}")