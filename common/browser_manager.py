# common/browser_manager.py
from playwright.async_api import Playwright
from .anti_detection import create_stealth_context
import asyncio

async def get_browser(playwright: Playwright, headless: bool = True, args: list = None):
    """
    Return a launched browser instance (Playwright Browser).
    Keep args minimal; callers should close the browser when done.
    """
    args = args or ["--no-sandbox"]
    browser = await playwright.chromium.launch(headless=headless, args=args)
    return browser

async def get_stealth_page(browser, *, locale="en-US"):
    """
    Create a stealth context and return a page bound to it.
    Caller is responsible for closing the browser/context when done.
    """
    context = await create_stealth_context(browser, locale=locale)
    page = await context.new_page()
    return page
