# common/browser_manager.py

from playwright.async_api import Playwright, Browser, Page
from .anti_detection import get_stealth_launch_args, get_stealth_context_options

async def get_browser(playwright: Playwright, headless: bool = True) -> Browser:
    """
    Launches and returns a stealthy browser instance.
    """
    launch_args = get_stealth_launch_args()
    
    browser = await playwright.chromium.launch(
        headless=headless,
        args=launch_args['args']
    )
    return browser

async def get_stealth_page(browser: Browser) -> Page:
    """
    Creates and returns a new page within a stealthy browser context.
    """
    context_options = get_stealth_context_options()
    
    page = await browser.new_page(**context_options)
    
    # You could add more sophisticated stealth techniques here,
    # like running a script to hide more browser properties.
    
    return page