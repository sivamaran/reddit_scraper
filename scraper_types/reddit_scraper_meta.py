# scraper_types/reddit_scraper_meta.py
import re
import time
from typing import List, Dict, Optional
from playwright.async_api import TimeoutError as PWTimeout, Page
from common.anti_detection import goto_resilient

def _dedupe(seq: List[str]) -> List[str]:
    seen, out = set(), []
    for s in seq:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _compact_to_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
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
    if not text:
        return {"emails": [], "phones": []}
    emails = list({m.group(0) for m in re.finditer(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)})
    phones = list({m.group(0) for m in re.finditer(r"\+?\d[\d\s().\-]{8,}\d", text)})
    return {"emails": emails, "phones": phones}

def _external_links(hrefs: List[str]) -> List[str]:
    return [h for h in hrefs if h and h.startswith("http") and "reddit.com" not in h]

async def _first_text(page: Page, selectors: List[str], timeout_ms: int = 6000) -> Optional[str]:
    for sel in selectors:
        try:
            el = await page.wait_for_selector(sel, timeout=timeout_ms, state="attached")
            txt = (await el.text_content() or "").strip()
            if txt:
                return txt
        except Exception:
            continue
    return None

async def _all_texts(page: Page, selectors: List[str], limit: int = 50) -> List[str]:
    out = []
    for sel in selectors:
        try:
            nodes = await page.query_selector_all(sel)
            for el in nodes[:limit]:
                t = (await el.text_content() or "").strip()
                if t:
                    out.append(t)
            if out:
                break
        except Exception:
            continue
    return out

async def _extract_post(page: Page, url: str) -> Dict:
    TITLE_SEL = [
        "h1[data-test-id='post-title']",
        "h1._eYtD2XCVieq6emjKBH3m",
        "h1"
    ]
    SUBREDDIT_SEL = ["a[data-testid='subreddit-name']", "a[data-click-id='subreddit']", "a[href*='/r/']"]
    AUTHOR_SEL = ["a[data-testid='post_author_link']", "a[data-click-id='user']", "a[href*='/user/']"]
    TIME_SEL = ["a[data-click-id='timestamp']", "time"]
    CONTENT_SEL = ["div[data-test-id='post-content'] p", "div._1qeIAgB0cPwnLhDF9XSiJM p"]
    UPVOTE_SEL = ["div._1rZYMD_4xY3gRcSS3p8ODO", "[id^='vote-arrows-'] ~ div"]
    COMMENTS_SEL = ["span.FHCV02u6Cp2zYL0fhQPsO", "a[data-click-id='comments']"]

    title = await _first_text(page, TITLE_SEL)
    subreddit = await _first_text(page, SUBREDDIT_SEL)
    author = await _first_text(page, AUTHOR_SEL)
    timestamp_text = await _first_text(page, TIME_SEL)

    content_lines = await _all_texts(page, CONTENT_SEL, limit=80)
    content = "\n".join(content_lines) if content_lines else None

    upvotes_text = await _first_text(page, UPVOTE_SEL, timeout_ms=2000)
    upvotes_num = _compact_to_int(upvotes_text)

    comments_text = await _first_text(page, COMMENTS_SEL)
    comments_num = None
    if comments_text:
        m = re.search(r"[\d,.]+", comments_text)
        if m:
            comments_num = _compact_to_int(m.group(0))

    href_nodes = await page.query_selector_all("a[href]")
    hrefs = []
    for a in href_nodes[:100]:
        try:
            href = await a.get_attribute("href")
            if href:
                hrefs.append(href)
        except Exception:
            pass
    external_links = _external_links(hrefs)

    text_blob = " ".join(filter(None, [title, content]))
    contacts = _contacts(text_blob)

    result = {
        "platform": "reddit",
        "reddit_link": url,
        "title": title,
        "subreddit": subreddit,
        "author": author,
        "posted": timestamp_text,
        "content": content,
        "upvotes": upvotes_text,
        "upvotes_num": upvotes_num,
        "comments": comments_text,
        "comments_num": comments_num,
        "external_links": external_links,
        "emails": contacts["emails"],
        "phones": contacts["phones"],
        "scraped_at": int(time.time())
    }

    if not (title or content):
        result["error"] = "Failed to extract"

    return result

async def scrape_reddit_posts_async(urls: List[str], page: Page) -> List[Dict]:
    """
    Scrape list of reddit post URLs using provided Playwright page.
    Uses goto_resilient for navigation.
    """
    norm = _dedupe([u.strip() for u in urls if u])
    results: List[Dict] = []
    for link in norm:
        try:
            # resilient navigation
            await goto_resilient(page, link, retries=3, timeout=35000)
            rec = await _extract_post(page, link)
            # if failed, don't crash; keep record and let manager decide fallback
            results.append(rec)
        except PWTimeout:
            results.append({"platform": "reddit", "reddit_link": link, "error": "Navigation timeout"})
        except Exception as e:
            results.append({"platform": "reddit", "reddit_link": link, "error": str(e)})
    return results
