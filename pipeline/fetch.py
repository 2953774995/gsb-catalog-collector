"""抓取层：目前只有最朴素的一次性 GET。"""
from __future__ import annotations

import json
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8765"
TIMEOUT = 10


def get_json(path: str) -> dict:
    url = BASE + path
    with urllib.request.urlopen(url, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode())


def list_page(cursor: int, limit: int = 10) -> dict:
    return get_json(f"/api/catalog?cursor={cursor}&limit={limit}")


def item_detail(sku: str) -> dict:
    return get_json(f"/api/item/{sku}")
