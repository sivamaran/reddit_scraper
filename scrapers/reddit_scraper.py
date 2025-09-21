# reddit_scraper.py (FINAL REFACTORED VERSION)
import asyncio
import json
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Optional

# --- ADDED: Import Playwright and your browser manager ---
from playwright.async_api import async_playwright

def _setup_path():
    project_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(project_root))

_setup_path()

# ---- ADDED: Import your new browser manager ----
from common.browser_manager import get_browser, get_stealth_page

# ---- platform scrapers (using the function names from the refactored files) ----
from scraper_types.reddit_scraper_meta import scrape_reddit_posts_async
from scraper_types.reddit_scraper_visible_text import scrape_reddit_visible_text_seq

# ---- db + schema utils ----
from common.db_utils import get_db, process_and_store, SCHEMA


def _merge_results(meta_results: List[Dict], visual_results: List[Dict]) -> List[Dict]:
    """
    Merge meta + visible results; visual wins on key collisions.
    Uses 'reddit_link' as the unique key.
    """
    merged: Dict[str, Dict] = {}
    for item in meta_results + visual_results:
        url = item.get("reddit_link")
        if not url:
            continue
        if url not in merged:
            merged[url] = {}
        merged[url].update(item)
    return list(merged.values())


# ---- main function is MODIFIED to manage the browser ----
async def main(
    urls: List[str],
    *,
    headless: bool = True,
    db=None,
    schema: Optional[Dict] = None,
    alias: Optional[Dict[str, list]] = None,
    write_path: Optional[str] = None,
) -> List[Dict]:
    """
    1) Creates ONE stealth browser and pages via browser_manager
    2) Runs both Reddit scrapers concurrently with these pages
    3) Merges and processes results
    """
    print(f"--- Starting combined Reddit scrape for {len(urls)} URLs ---")

    async with async_playwright() as p:
        browser = await get_browser(p, headless=headless)
        try:
            # Create two separate pages from the same stealth browser
            meta_page = await get_stealth_page(browser)
            visual_page = await get_stealth_page(browser)

            # CHANGED: Pass the page object to each scraper task. The 'headless' arg is removed.
            meta_task = asyncio.create_task(
                scrape_reddit_posts_async(urls, page=meta_page)
            )
            visual_task = asyncio.create_task(
                scrape_reddit_visible_text_seq(urls, page=visual_page)
            )

            meta_results, visual_results = await asyncio.gather(meta_task, visual_task)
        finally:
            # Ensure the single browser instance is always closed
            if browser:
                await browser.close()

    # The rest of the function for merging and storing data is unchanged
    print("\n--- Merging Reddit results ---")
    combined_results = _merge_results(meta_results, visual_results)

    if schema is not None and db is not None:
        print("\n--- Filtering to flat schema + inserting into MongoDB ---")
        filtered = process_and_store(
            db=db,
            data=combined_results,
            platform="reddit",
            schema_obj=schema,
            alias=alias or {},
            write_path=write_path,
        )
        return filtered

    return combined_results

# The `if __name__ == "__main__":` block is unchanged and works as intended
if __name__ == "__main__":
    # ... (this part of your code does not need to change)
    parser = argparse.ArgumentParser(description="Run the main combined Reddit scraper.")
    # ... (all your argparse and asyncio.run logic remains the same)