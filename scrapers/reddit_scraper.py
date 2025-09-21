# scrapers/reddit_scraper.py
# Runs BOTH internal scrapers, merges, maps to schema, and RETURNS results (no DB here).

from typing import List, Dict, Any
from collections import defaultdict
from playwright.async_api import async_playwright

from scraper_types.reddit_scraper_meta import scrape_reddit_posts_async
from scraper_types.reddit_scraper_visible_text import scrape_reddit_visible_text_seq


# ---------- Merge helpers ----------
def _merge_records(meta_list: List[Dict[str, Any]], vis_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge meta + visible-text results by reddit_link.
    Preference: keep non-empty fields from whichever scraper has them; union lists.
    """
    by_url: Dict[str, Dict[str, Any]] = defaultdict(dict)

    def _merge_one(rec: Dict[str, Any]) -> None:
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

    # If we have usable content, drop stale error flags
    for url, rec in by_url.items():
        if (rec.get("title") or rec.get("content")) and "error" in rec:
            rec.pop("error", None)

    return list(by_url.values())


# ---------- Schema mapping ----------
def _to_schema(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map merged raw record into your unified schema (aligned with the Twitter schema you used).
    """
    url = raw.get("reddit_link", "") or raw.get("url", "")
    title = (raw.get("title") or "").strip()
    body = (raw.get("content") or "").strip()
    subreddit = (raw.get("subreddit") or "").strip()
    author = (raw.get("author") or "").strip()
    posted = raw.get("posted")
    upvotes = raw.get("upvotes_num")
    comments = raw.get("comments_num")
    emails = raw.get("emails") or []
    phones = raw.get("phones") or []
    external_links = raw.get("external_links") or []

    schema = {
        "url": url,
        "platform": "reddit",
        "content_type": "post",
        "source": "web-scraper",

        "profile": {
            "username": author,
            "full_name": "",
            "bio": ""
        },

        "post": {
            "title": title,
            "body": body,
            "subreddit": subreddit
        },

        "engagement": {
            "num_comments": comments if isinstance(comments, int) else None,
            "num_upvotes": upvotes if isinstance(upvotes, int) else None
        },

        "contact_info": {
            "emails": emails,
            "phones": phones
        },

        "external_links": external_links,
        "posted": posted
    }

    if not (title or body):
        schema["error"] = raw.get("error", "Failed to extract")

    return schema


# ---------- Public entrypoint (NO DB) ----------
async def main(urls: List[str], headless: bool = True) -> List[Dict[str, Any]]:
    """
    Manager for Reddit scraping:
      1) Run meta scraper (Playwright).
      2) Run visible-text scraper (requests/BS4).
      3) Merge results.
      4) Map to unified schema.
      5) Return the schema list.  (No DB here)
    """
    # 1) Meta (Playwright)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        page = await browser.new_page(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ))
        meta_results = await scrape_reddit_posts_async(urls, page)
        await browser.close()

    # 2) Visible text (requests/BS4)
    visual_results = scrape_reddit_visible_text_seq(urls)

    # 3) Merge
    merged = _merge_records(meta_results, visual_results)

    # 4) Schema
    schema_docs = [_to_schema(m) for m in merged]

    # 5) Return (no DB)
    return schema_docs
