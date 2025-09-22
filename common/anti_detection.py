# common/anti_detection.py
import asyncio
import random
from playwright.async_api import TimeoutError as PlaywrightTimeout

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
]


async def goto_resilient(page, url: str, retries: int = 3, timeout: int = 30000):
    """
    Robust navigation helper:
      - retries on timeout/errors
      - small randomized sleeps to mimic human behaviour
    """
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await asyncio.sleep(random.uniform(1.2, 3.0))
            return
        except PlaywrightTimeout:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"⚠️ Timeout navigating {url}. Retrying in {wait}s... ({attempt+1}/{retries})")
                await asyncio.sleep(wait + random.uniform(0, 1))
            else:
                raise
        except Exception as e:
            if attempt < retries - 1:
                print(f"⚠️ Navigation error ({e}) for {url}. Retrying... ({attempt+1}/{retries})")
                await asyncio.sleep(1.5 + random.uniform(0, 1))
            else:
                raise


async def create_stealth_context(browser, *, locale="en-US"):
    """
    Create a stealth browser context with:
      - randomized UA
      - randomized viewport
      - timezone and extra headers
      - small navigator spoofing
    Returns the Playwright context (call .new_page() on it).
    """
    user_agent = random.choice(DEFAULT_USER_AGENTS)
    width = random.randint(1200, 1400)
    height = random.randint(700, 900)
    timezone = random.choice(["America/Los_Angeles", "Europe/London", "Asia/Kolkata", "America/New_York"])

    context = await browser.new_context(
        user_agent=user_agent,
        viewport={"width": width, "height": height},
        locale=locale,
        timezone_id=timezone,
        java_script_enabled=True,
        accept_downloads=False,
    )

    await context.set_extra_http_headers({
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
    })

    # Spoof some navigator properties
    await context.add_init_script(
        """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        """
    )

    print(f"[stealth] UA={user_agent[:80]}... viewport={width}x{height} tz={timezone}")
    return context
