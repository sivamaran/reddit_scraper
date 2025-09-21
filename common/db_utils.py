# scraper_types/db_utils.py
import os
import json
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from pymongo import MongoClient, ASCENDING, UpdateOne
from dotenv import load_dotenv

Json = Union[Dict[str, Any], List[Dict[str, Any]]]

# ---------------- MongoDB ----------------
def get_db():
    """
    Initialize and return a MongoDB database connection.
    Uses .env MONGO_URI or defaults to localhost.
    """
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/leadgen")
    client = MongoClient(mongo_uri)
    db = client.get_default_database() or client["leadgen"]
    return db

# platform -> collection
PLATFORM_COLLECTION = {
    "twitter": "twitter_leads",
    "quora": "quora_leads",
    "reddit": "reddit_leads",
    # extend here (instagram, linkedin...) when needed
}

def _ensure_indexes_for(db, collection_name: str):
    # use non-unique index by default; switch to unique if you dedupe on url
    db[collection_name].create_index([("url", ASCENDING)], unique=False)

def add_leads(db, data: Json, platform: str) -> Dict[str, Any]:
    """
    Upsert many leads into the right collection by platform.
    - data: dict or list[dict]
    """
    platform_key = platform.strip().lower()
    collection = PLATFORM_COLLECTION.get(platform_key)
    if not collection:
        raise ValueError(f"Unknown platform '{platform}'. Supported: {list(PLATFORM_COLLECTION.keys())}")

    items: List[Dict[str, Any]] = data if isinstance(data, list) else [data]
    _ensure_indexes_for(db, collection)

    ops: List[UpdateOne] = []
    skipped, errors = 0, []
    for i, d in enumerate(items):
        if not isinstance(d, dict):
            skipped += 1
            errors.append(f"Item {i}: not a dict")
            continue

        # Find a URL-like field; alias map later guarantees 'url' exists after filtering
        url = d.get("url") or d.get("twitter_link") or d.get("quora_link") or d.get("reddit_link")
        if not url:
            skipped += 1
            errors.append(f"Item {i}: missing 'url'")
            continue

        d.setdefault("platform", platform_key)
        d.setdefault("scraped_at", datetime.utcnow())

        ops.append(UpdateOne({"url": url}, {"$set": d}, upsert=True))

    inserted_or_upserted = 0
    if ops:
        res = db[collection].bulk_write(ops, ordered=False)
        inserted_or_upserted = (res.upserted_count or 0) + (res.modified_count or 0)

    return {
        "platform": platform_key,
        "collection": collection,
        "total": len(items),
        "inserted_or_upserted": inserted_or_upserted,
        "skipped": skipped,
        "errors": errors,
    }

# ---------------- Schema filtering (flat KV) ----------------
def filter_by_schema(
    data: Dict[str, Any],
    schema_obj: Dict[str, Any],
    *,
    fill_missing: bool = True,
    alias: Optional[Dict[str, List[str]]] = None
) -> Dict[str, Any]:
    """
    Return a new dict restricted to keys in `schema_obj`.
    - alias: mapping of output_key -> list of possible input keys (merge & dedupe).
    """
    if alias is None:
        alias = {}

    out: Dict[str, Any] = {}
    for out_key in schema_obj.keys():
        in_keys = alias.get(out_key, [out_key])
        vals = []
        for k in in_keys:
            if k in data and data[k] is not None:
                vals.append(data[k])

        # dedupe while preserving order
        uniq = []
        for v in vals:
            if v not in uniq:
                uniq.append(v)

        if uniq:
            out[out_key] = uniq[0] if len(uniq) == 1 else uniq
        elif fill_missing:
            out[out_key] = None
    return out

# ---------------- One-call pipeline ----------------
def process_and_store(
    db,
    data: Json,
    platform: str,
    schema_obj: Dict[str, Any],
    *,
    alias: Optional[Dict[str, List[str]]] = None,
    fill_missing: bool = True,
    write_path: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    1) filter to schema (with alias),
    2) insert into Mongo (by platform),
    3) optionally write JSON to disk,
    4) return the filtered list.
    """
    items = data if isinstance(data, list) else [data]
    filtered = [
        filter_by_schema(item, schema_obj, fill_missing=fill_missing, alias=alias or {})
        for item in items
        if isinstance(item, dict)
    ]

    add_leads(db, filtered, platform=platform)

    if write_path:
        with open(write_path, "w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)

    return filtered

#-------------------------------------
# ---------------- Default Schema Template ----------------
# in common/db_utils.py

SCHEMA = {
  "url": "",
  "platform": "twitter",
  "content_type": "",
  "source": "twitter-scraper",
  "profile": {
    "username": "",
    "full_name": "",
    "bio": "",
    "location": "",
    "job_title": "",
    "employee_count": ""
  },
  "contact": {
    "emails": [],
    "phone_numbers": [],
    "address": "",
    "websites": [],
    "social_media_handles": {
      "instagram": "",
      "twitter": "",
      "facebook": "",
      "linkedin": "",
      "youtube": "",
      "tiktok": "",
      "other": []
    },
    "bio_links": []
  },
  "content": {
    "caption": "",
    "upload_date": "",
    "channel_name": "",
    "author_name": ""
  },
  "metadata": {
    "scraped_at": "",
    "data_quality_score": ""
  },
  "industry": "",
  "revenue": "",
  "lead_category": "",
  "lead_sub_category": "",
  "company_name": "",
  "company_type": "",
  "decision_makers": "",
  "bdr": "AKG",
  "product_interests": "",
  "timeline": "",
  "interest_level": "",
  "icp_identifier": ""
}

# common/db_utils.py

import json
from datetime import datetime
from pymongo import MongoClient

def save_to_mongo(json_list, db_name="leadgen", collection_name="map_leads"):
    """
    Save a list of schema-shaped JSON docs into MongoDB.
    Defaults: db_name='leadgen', collection_name='map_leads'.
    """
    if not json_list:
        print(f"⚠️ No data to save into {collection_name}")
        return []

    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]

    try:
        collection.insert_many(json_list)
        print(f"✅ Saved {len(json_list)} records into MongoDB: {db_name}.{collection_name}")
    except Exception as e:
        print(f"⚠️ Failed saving into MongoDB: {e}")

    return json_list


def save_to_json(json_list, file_path="output.json"):
    """
    Save a list of schema-shaped JSON docs into a local JSON file.
    Includes a timestamp for when the file was written.
    """
    if not json_list:
        print("⚠️ No data to save to JSON file")
        return []

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "saved_at": datetime.utcnow().isoformat(),
                    "records": json_list,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )
        print(f"💾 Wrote {len(json_list)} records to {file_path}")
    except Exception as e:
        print(f"⚠️ Failed writing JSON file: {e}")

    return json_list
