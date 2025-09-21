# tests/reddit_test.py
# Reads URLs, calls scrapers.reddit_scraper.main (returns schema), upserts to Mongo, writes JSON.

import asyncio
import json
import sys
from pathlib import Path

def _setup_path():
    project_root = Path(__file__).resolve().parent.parent  # project root
    sys.path.insert(0, str(project_root))

_setup_path()

from scrapers.reddit_scraper import main as run_reddit_scraper
from common.db_utils import get_db, PLATFORM_COLLECTION
from pymongo import UpdateOne


async def run_test():
    print("--- Starting Reddit Test ---")

    tests_dir = Path(__file__).resolve().parent
    urls_file_path = tests_dir / "reddit_urls.txt"
    output_file_path = tests_dir / "reddit_output.json"

    try:
        with open(urls_file_path, "r", encoding="utf-8") as f:
            urls_to_scrape = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"ERROR: The input file was not found at '{urls_file_path}'")
        return

    if not urls_to_scrape:
        print("No URLs found in 'reddit_urls.txt'. Test aborted.")
        return

    # 🔹 Call the main (returns schema docs)
    schema_results = await run_reddit_scraper(urls_to_scrape, headless=True)

    # 🔹 Write JSON for inspection
    with open(output_file_path, "w", encoding="utf-8") as f:
        json.dump(schema_results, f, indent=2, ensure_ascii=False)
    print(f"[OK] Wrote schema results to: {output_file_path}")

    # 🔹 Upsert into MongoDB here (NOT in main)
    db = get_db()
    coll_name = PLATFORM_COLLECTION.get("reddit", "reddit_leads")
    col = db[coll_name]
    try:
        col.create_index("url", unique=True)
    except Exception:
        pass

    ops = []
    for doc in schema_results:
        url = doc.get("url")
        if not url:
            continue
        ops.append(UpdateOne({"url": url}, {"$set": doc}, upsert=True))

    if ops:
        bulk = col.bulk_write(ops, ordered=False)
        print("[Mongo] matched:", bulk.matched_count,
              "modified:", bulk.modified_count,
              "upserted:", len(bulk.upserted_ids) if bulk.upserted_ids else 0)
    else:
        print("[Mongo] No valid docs to upsert.")

    print("--- Test Complete ---")

if __name__ == "__main__":
    asyncio.run(run_test())
