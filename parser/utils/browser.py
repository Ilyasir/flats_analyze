import random
import asyncio
from playwright.async_api import Page

async def block_heavy_resources(page: Page):
    """Отключает загрузку тяжелых ресурсов(картинки, стилей) для экономии трафика и скорости."""
    def route_handler(route):

        if route.request.resource_type in ["image", "font", "media"]:
            return route.abort()
        return route.continue_()

    await page.route("**/*", route_handler)


async def click_next_page(page, selector: str, wait_range=(1.5, 2.5)) -> bool:
    """Пытается найти кнопку пагинации и кликнуть по ней."""
    try:
        next_button = page.locator(selector).filter(has_text="Дальше")
        if await next_button.count() > 0:
            await next_button.click()
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(random.uniform(*wait_range))
            return True
        return False
    except Exception as e:
        return False