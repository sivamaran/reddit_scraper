# scraper_types/reddit_scraper_visible_text.py (FINAL CORRECTED VERSION)
import re, json, time, asyncio
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from playwright.async_api import Page, TimeoutError

# --- All your helper functions and the detailed extractor function remain here ---
# This part of your code is correct and does not need to change.
# ... (all your _norm, _dedupe_keep_order, extract_visible_text_from_reddit_page, etc. functions)


# -------------------------------------------------------------------------
# --- 🔄 THE NEW REFACTORED MAIN FUNCTION (REPLACES THE OLD ONE) ---
# -------------------------------------------------------------------------
async def scrape_reddit_visible_text_seq(urls: List[str], page: Page) -> List[Dict]:
    """
    Sequentially scrapes a list of Reddit URLs using a PRE-CONFIGURED page object.
    """
    results = []
    for url in urls:
        item = {"platform": "reddit", "reddit_link": url, "scraped_at": int(time.time())}
        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.wait_for_selector('shreddit-post', timeout=20000)

            try:
                close_button = page.locator('button[aria-label="Close"], [aria-label="Close dialog"]').first
                if await close_button.is_visible(timeout=5000):
                    await close_button.click()
            except (TimeoutError, Exception):
                pass

            extracted = await extract_visible_text_from_reddit_page(page)
            item.update(extracted)
        except Exception as e:
            item["error"] = str(e)
        results.append(item)
    return results