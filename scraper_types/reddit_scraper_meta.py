# scraper_types/reddit_scraper_meta.py (FINAL COMPLETE VERSION)

import os
import re
import asyncio
from typing import List, Dict, Optional
from urllib.parse import urlparse
from playwright.async_api import TimeoutError as PWTimeout, Page

# -----------------------------------------------------------------
# --- ✅ HELPER FUNCTIONS AND SELECTORS (ALL RESTORED) ---
# -----------------------------------------------------------------

# -- Utilities --
def _is_reddit(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
        return ("reddit.com" in host) or ("redd.it" in host)
    except Exception:
        return False

def _dedupe(seq: List[str]) -> List[str]:
    seen, out = set(), []
    for s in seq:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _compact_to_int(s: Optional[str]) -> Optional[int]:
    if not s: return None
    t = s.strip().lower().replace(",", "")
    m = re.match(r"^(\d+(?:\.\d+)?)([km])?$", t)
    if not m:
        digits = re.sub(r"[^\d]", "", t)
        return int(digits) if digits else None
    num = float(m.group(1))
    suf = m.group(2)
    if suf == "k": num *= 1_000
    elif suf == "m": num *= 1_000_000
    return int(num)

def _contacts(text: Optional[str]) -> Dict[str, List[str]]:
    if not text: return {"emails": [], "phones": []}
    emails = list({m.group(0) for m in re.finditer(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)})
    phones = list({m.group(0) for m in re.finditer(r"\+?\d[\d\s().\-]{8,}\d", text)})
    return {"emails": emails, "phones": phones}

def _external_links(hrefs: List[str]) -> List[str]:
    out = [h for h in hrefs if h and h.startswith("http") and "reddit.com" not in h and "redd.it" not in h]
    return _dedupe(out)[:20]

# -- Playwright helpers --
async def _goto(page: Page, url: str):
    await page.goto(url, wait_until="domcontentloaded", timeout=35000)
    try:
        await page.wait_for_load_state("networkidle", timeout=8000)
    except Exception:
        pass

async def _first_text(page: Page, selectors: List[str], timeout_ms: int = 6000) -> Optional[str]:
    for sel in selectors:
        if not sel: continue
        try:
            el = await page.wait_for_selector(sel, timeout=timeout_ms, state="attached")
            txt = (await el.text_content() or "").strip()
            if txt: return txt
        except Exception:
            pass
    return None

async def _all_texts(page: Page, selector: str, limit: int = 25, timeout_ms: int = 5000) -> List[str]:
    try:
        await page.wait_for_selector(selector, timeout=timeout_ms, state="attached")
    except Exception:
        return []
    nodes = await page.query_selector_all(selector)
    out: List[str] = []
    for el in nodes[:limit]:
        try:
            t = (await el.text_content() or "").strip()
            if t: out.append(t)
        except Exception:
            pass
    return out

# -- Selectors --
TITLE_SEL = ["h1", "div[data-test-id='post-content'] h1"]
SUBREDDIT_SEL = ["a[data-click-id='subreddit']", "a[href*='/r/']"]
AUTHOR_SEL = ["a[href*='/user/']", "a[data-click-id='user']"]
TIME_SEL = ["a[data-click-id='timestamp']", "time"]
CONTENT_PARA_SEL = ["div[data-test-id='post-content'] [data-click-id='text'] p"]
UPVOTES_CANDIDATE_SEL = ["[id^='vote-arrows-'] ~ div"]
COMMENTS_COUNT_SEL = ["a[data-click-id='comments']", "span[data-click-id='comments']"]

# -- Extractor --
async def _extract_post(page: Page, url: str) -> Dict:
    title = await _first_text(page, TITLE_SEL)
    subreddit = None
    sub_text = await _first_text(page, SUBREDDIT_SEL)
    if sub_text and sub_text.startswith("r/"):
        subreddit = sub_text
    author = await _first_text(page, AUTHOR_SEL)
    timestamp_text = await _first_text(page, TIME_SEL)
    paras = await _all_texts(page, CONTENT_PARA_SEL[0], limit=80)
    content = "\n".join(paras) if paras else None
    upvotes_text = await _first_text(page, UPVOTES_CANDIDATE_SEL, timeout_ms=1000)
    upvotes_num = _compact_to_int(upvotes_text)
    comments_text = await _first_text(page, COMMENTS_COUNT_SEL)
    comments_num = _compact_to_int(re.search(r"[\d,.]+", comments_text or "").group(0)) if comments_text else None
    href_nodes = await page.query_selector_all("a[href]")
    hrefs = [await a.get_attribute("href") for a in href_nodes[:80] if await a.get_attribute("href")]
    external_links = _external_links(hrefs)
    text_blob = " ".join(filter(None, [title, content]))
    contacts = _contacts(text_blob)
    result = {
        "platform": "reddit", "reddit_link": url, "title": title, "subreddit": subreddit,
        "author": author, "posted": timestamp_text, "content": content,
        "upvotes": upvotes_text, "upvotes_num": upvotes_num, "comments": comments_text,
        "comments_num": comments_num, "external_links": external_links, "emails": contacts["emails"],
        "phones": contacts["phones"], "scraped_at": int(time.time())
    }
    if not (title or content):
        result["error"] = "Failed to extract"
    return result

# -------------------------------------------------------------------------
# --- 🔄 THE REFACTORED MAIN FUNCTION ---
# -------------------------------------------------------------------------
async def scrape_reddit_posts_async(urls: List[str], page: Page) -> List[Dict]:
    """
    Scrapes a list of Reddit post URLs using a PRE-CONFIGURED page object.
    (Simple version without network blocking to avoid errors).
    """
    norm = _dedupe([u.strip() for u in urls if u and _is_reddit(u)])
    results: List[Dict] = []
    
    try:
        for link in norm:
            try:
                await _goto(page, link)
            except PWTimeout:
                results.append({"platform": "reddit", "reddit_link": link, "error": "Navigation timeout"})
                continue
            results.append(await _extract_post(page, link))
    except Exception as e:
        print(f"A critical error occurred during scraping: {e}")
    
    return results