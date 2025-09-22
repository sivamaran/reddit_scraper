# scraper_types/reddit_scraper_visible_text.py
import re
import time
import requests
from bs4 import BeautifulSoup
from typing import List, Dict

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
    if suf == "k": num *= 1_000
    elif suf == "m": num *= 1_000_000
    return int(num)

def _normalize_to_old(url: str) -> str:
    from urllib.parse import urlparse, urlunparse
    u = urlparse(url)
    if "reddit.com" in u.netloc and not u.netloc.startswith("old."):
        return urlunparse((u.scheme or "https", "old.reddit.com", u.path, u.params, u.query, u.fragment))
    return url

def scrape_reddit_visible_text_seq(urls: List[str]) -> List[Dict]:
    """
    Simple sequential extractor using requests + BeautifulSoup.
    Returns list of dicts with same base fields as meta extractor.
    """
    results = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    for link in [u.strip() for u in urls if u and u.strip()]:
        try:
            resp = requests.get(link, headers=headers, timeout=20)
            # if redirected to non-reddit or blocked, try old.reddit
            if resp.status_code != 200 or "reddit" not in resp.url:
                old = _normalize_to_old(link)
                resp = requests.get(old, headers=headers, timeout=20)

            soup = BeautifulSoup(resp.text, "html.parser")

            # Title
            title = None
            for sel in [
                "h1[data-test-id='post-title']",
                "h1._eYtD2XCVieq6emjKBH3m",
                "h1"
            ]:
                node = soup.select_one(sel)
                if node and node.get_text(strip=True):
                    title = node.get_text(strip=True)
                    break

            # Author
            author = None
            node = soup.select_one("a[data-testid='post_author_link']") or soup.select_one("a[data-click-id='user']")
            if node:
                author = node.get_text(strip=True)

            # Subreddit
            subreddit = None
            node = soup.select_one("a[data-testid='subreddit-name']") or soup.select_one("a[data-click-id='subreddit']")
            if node:
                subreddit = node.get_text(strip=True)

            # Content paragraphs
            paras = []
            for sel in ["div[data-test-id='post-content'] p", "div._1qeIAgB0cPwnLhDF9XSiJM p"]:
                for p in soup.select(sel):
                    t = p.get_text(strip=True)
                    if t:
                        paras.append(t)
                if paras:
                    break
            content = "\n".join(paras) if paras else None

            # Upvotes
            upvotes_text = None
            node = soup.select_one("div._1rZYMD_4xY3gRcSS3p8ODO")
            if node:
                upvotes_text = node.get_text(strip=True)
            upvotes_num = _compact_to_int(upvotes_text)

            # Comments
            comments_text = None
            node = soup.select_one("span.FHCV02u6Cp2zYL0fhQPsO") or soup.select_one("a[data-click-id='comments']")
            if node:
                comments_text = node.get_text(strip=True)
            comments_num = _compact_to_int(comments_text) if comments_text else None

            hrefs = [a.get("href") for a in soup.select("a[href]")[:100] if a.get("href")]
            external_links = [h for h in hrefs if h and h.startswith("http") and "reddit.com" not in h and "redd.it" not in h]

            result = {
                "platform": "reddit",
                "reddit_link": link,
                "title": title,
                "subreddit": subreddit,
                "author": author,
                "content": content,
                "upvotes": upvotes_text,
                "upvotes_num": upvotes_num,
                "comments": comments_text,
                "comments_num": comments_num,
                "external_links": external_links,
                "emails": [],
                "phones": [],
                "scraped_at": int(time.time())
            }

            if not (title or content):
                result["error"] = "Failed to extract"

            results.append(result)
            print(f"[OK] Scraped: {link} → title={bool(title)} author={bool(author)}")
        except Exception as e:
            results.append({"platform": "reddit", "reddit_link": link, "error": str(e)})
            print(f"[ERR] {link} → {e}")

    return results
