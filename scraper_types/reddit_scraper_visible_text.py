# scraper_types/reddit_scraper_visible_text.py (UPDATED)

import re
import time
import requests
from bs4 import BeautifulSoup
from typing import List, Dict
from urllib.parse import urlparse, urlunparse

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def _dedupe(seq: List[str]) -> List[str]:
    seen, out = set(), []
    for s in seq:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _compact_to_int(s: str):
    if not s:
        return None
    s = s.strip().lower().replace(",", "")
    m = re.match(r"^(\d+(?:\.\d+)?)([km])?$", s)
    if not m:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else None
    num = float(m.group(1))
    suf = m.group(2)
    if suf == "k":
        num *= 1_000
    elif suf == "m":
        num *= 1_000_000
    return int(num)

def _contacts(text: str) -> Dict[str, List[str]]:
    emails = list({m.group(0) for m in re.finditer(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")})
    phones = list({m.group(0) for m in re.finditer(r"\+?\d[\d\s().\-]{8,}\d", text or "")})
    return {"emails": emails, "phones": phones}

def _external_links(hrefs: List[str]) -> List[str]:
    return [h for h in hrefs if h and h.startswith("http") and "reddit.com" not in h]

def _normalize_url(url: str) -> str:
    """Force fallback to old.reddit.com"""
    u = urlparse(url)
    if "reddit.com" in u.netloc and not u.netloc.startswith("old."):
        return urlunparse((u.scheme or "https", "old.reddit.com", u.path, u.params, u.query, u.fragment))
    return url

# ---------------------------------------------------------
# Core Extractor
# ---------------------------------------------------------

def _extract_from_html(html: str, url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")

    # --- Title ---
    title = None
    for sel in [
        "h1[data-test-id='post-content-title']",
        "h1._eYtD2XCVieq6emjKBH3m",
        "h1"
    ]:
        node = soup.select_one(sel)
        if node and node.get_text(strip=True):
            title = node.get_text(strip=True)
            break

    # --- Subreddit ---
    subreddit = None
    for sel in [
        "a[data-testid='subreddit-name']",
        "a[data-click-id='subreddit']",
        "a[href*='/r/']"
    ]:
        node = soup.select_one(sel)
        if node and node.get_text(strip=True).startswith("r/"):
            subreddit = node.get_text(strip=True)
            break

    # --- Author ---
    author = None
    for sel in [
        "a[data-testid='post_author_link']",
        "a[data-click-id='user']",
        "a[href*='/user/']"
    ]:
        node = soup.select_one(sel)
        if node and node.get_text(strip=True):
            author = node.get_text(strip=True)
            break

    # --- Content ---
    paras = []
    for sel in [
        "div[data-test-id='post-content'] p",
        "div._1qeIAgB0cPwnLhDF9XSiJM p"
    ]:
        for node in soup.select(sel):
            txt = node.get_text(strip=True)
            if txt:
                paras.append(txt)
        if paras:
            break
    content = "\n".join(paras) if paras else None

    # --- Votes ---
    upvotes_text = None
    for sel in ["div._1rZYMD_4xY3gRcSS3p8ODO"]:
        node = soup.select_one(sel)
        if node and node.get_text(strip=True):
            upvotes_text = node.get_text(strip=True)
            break
    upvotes_num = _compact_to_int(upvotes_text)

    # --- Comments ---
    comments_text = None
    for sel in [
        "span.FHCV02u6Cp2zYL0fhQPsO",
        "a[data-click-id='comments']"
    ]:
        node = soup.select_one(sel)
        if node and node.get_text(strip=True):
            comments_text = node.get_text(strip=True)
            break
    comments_num = None
    if comments_text:
        m = re.search(r"[\d,.]+", comments_text)
        if m:
            comments_num = _compact_to_int(m.group(0))

    # --- External links ---
    hrefs = [a["href"] for a in soup.select("a[href]")[:100] if a.get("href")]
    external_links = _external_links(hrefs)

    # --- Contacts ---
    text_blob = " ".join(filter(None, [title, content]))
    contacts = _contacts(text_blob)

    result = {
        "platform": "reddit",
        "reddit_link": url,
        "title": title,
        "subreddit": subreddit,
        "author": author,
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

# ---------------------------------------------------------
# Public function
# ---------------------------------------------------------

def scrape_reddit_visible_text_seq(urls: List[str]) -> List[Dict]:
    """Scrape Reddit posts sequentially using requests + BeautifulSoup."""
    results = []
    norm = _dedupe([u.strip() for u in urls if u])
    headers = {"User-Agent": "Mozilla/5.0 (compatible; RedditScraper/1.0)"}

    for link in norm:
        try:
            resp = requests.get(link, headers=headers, timeout=20)
            if resp.status_code != 200 or "reddit" not in resp.url:
                # fallback to old reddit
                old_url = _normalize_url(link)
                resp = requests.get(old_url, headers=headers, timeout=20)

            record = _extract_from_html(resp.text, link)
            results.append(record)

            print(f"[OK] Scraped: {link} → title={record.get('title')}")
        except Exception as e:
            results.append({"platform": "reddit", "reddit_link": link, "error": str(e)})
            print(f"[ERR] {link} → {e}")

    return results
