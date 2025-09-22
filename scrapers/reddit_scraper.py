# scrapers/reddit_scraper.py
from typing import List, Dict, Any
from collections import defaultdict
from playwright.async_api import async_playwright
from common.browser_manager import get_browser, get_stealth_page
from scraper_types.reddit_scraper_meta import scrape_reddit_posts_async
from scraper_types.reddit_scraper_visible_text import scrape_reddit_visible_text_seq

def _merge_records(meta_list: List[Dict[str, Any]], vis_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_url: Dict[str, Dict[str, Any]] = defaultdict(dict)

    def _merge_one(rec: Dict[str, Any]):
        url = rec.get("reddit_link") or rec.get("url")
        if not url:
            return
        if url not in by_url:
            by_url[url] = {}
        for k, v in rec.items():
            if k == "reddit_link":
                by_url[url]["reddit_link"] = v
                continue
            if isinstance(v, list):
                base = by_url[url].get(k) or []
                seen = set(base)
                for item in v:
                    if item not in seen:
                        base.append(item)
                        seen.add(item)
                by_url[url][k] = base
            else:
                if not by_url[url].get(k) and v not in (None, "", []):
                    by_url[url][k] = v

    for rec in meta_list or []:
        _merge_one(rec)
    for rec in vis_list or []:
        _merge_one(rec)

    for url, rec in list(by_url.items()):
        if (rec.get("title") or rec.get("content")) and "error" in rec:
            rec.pop("error", None)

    return list(by_url.values())

def _to_schema(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "url": raw.get("reddit_link", ""),
        "platform": "reddit",
        "content_type": "post",
        "source": "web-scraper",
        "profile": {
            "username": raw.get("author") or "",
            "full_name": "",
            "bio": ""
        },
        "post": {
            "title": raw.get("title") or "",
            "body": raw.get("content") or "",
            "subreddit": raw.get("subreddit") or ""
        },
        "engagement": {
            "num_comments": raw.get("comments_num"),
            "num_upvotes": raw.get("upvotes_num")
        },
        "contact_info": {
            "emails": raw.get("emails") or [],
            "phones": raw.get("phones") or []
        },
        "external_links": raw.get("external_links") or [],
        "posted": raw.get("posted")
    }

async def main(urls: List[str], headless: bool = True) -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        browser = await get_browser(p, headless=headless)
        # get_stealth_page returns a page bound to a stealth context
        page = await get_stealth_page(browser)
        try:
            meta_results = await scrape_reddit_posts_async(urls, page)
        finally:
            # close browser to free resources
            try:
                await browser.close()
            except Exception:
                pass

    visual_results = scrape_reddit_visible_text_seq(urls)
    merged = _merge_records(meta_results, visual_results)
    schema_docs = [_to_schema(m) for m in merged]
    return schema_docs
