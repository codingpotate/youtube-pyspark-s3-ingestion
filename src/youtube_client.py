import json
import os
from datetime import datetime
from typing import Dict, Any
import requests

BASE_URL = "https://www.googleapis.com/youtube/v3"

def fetch_trending(api_key: str, region_code: str = "US", max_results: int = 50) -> Dict[str, Any]:
    url = (
        f"{BASE_URL}/videos?part=snippet,statistics,contentDetails"
        f"&chart=mostPopular&maxResults={max_results}&regionCode={region_code}&key={api_key}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def dump_raw(data: Dict[str, Any], folder: str = "data/raw", prefix: str = "trending") -> str:
    os.makedirs(folder, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(folder, f"{prefix}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path
