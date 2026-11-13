"""本地模拟的供应商门户（供离线联调/复现使用）。

启动：python3 mock_portal.py --port 8765
接口：
  GET /api/catalog?cursor=<int>&limit=<int>   → {"items":[...],"next_cursor":<int|null>}
  GET /api/item/<sku>                          → 明细

真实环境的三类特征在这里都是确定性的、可复现的：
  1. 限流：1 秒内超过 5 次请求 → 429 + Retry-After: 1
  2. 抖动：第 6 的倍数次请求 → 503（模拟上游偶发故障）
  3. 脏数据：跨页重复上架同一 sku；部分明细返回非法 JSON
"""
from __future__ import annotations

import argparse
import json
import random
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

TOTAL_SKUS = 120
PAGE_SIZE_DEFAULT = 10
RATE_LIMIT = 5          # 每秒允许的请求数
JITTER_EVERY = 6        # 每 6 次请求返回一次 503

_state = {"count": 0, "window_start": 0.0, "window_count": 0}


def _catalog(limit: int = PAGE_SIZE_DEFAULT) -> list[dict]:
    rnd = random.Random(20260101)
    items = []
    for i in range(1, TOTAL_SKUS + 1):
        items.append({
            "sku": f"SKU-{i:04d}",
            "name": f"item-{i:04d}",
            "price": round(rnd.uniform(5, 500), 2),
            "currency": "CNY",
            "stock": rnd.randint(0, 200),
            "updated_at": f"2026-03-{(i % 28) + 1:02d}T0{i % 9}:00:00Z",
        })
    # 跨页重复：把前 8 条再挂到末尾，模拟上游重复上架
    items = items + items[:8]
    return items


CATALOG = _catalog()


def _bad_json(sku: str) -> bool:
    return int(sku.split("-")[1]) % 17 == 0


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):  # 静音
        pass

    def _send(self, code: int, body: bytes, ctype="application/json", extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _throttled(self) -> bool:
        import time
        now = time.time()
        if now - _state["window_start"] >= 1.0:
            _state["window_start"] = now
            _state["window_count"] = 0
        _state["window_count"] += 1
        return _state["window_count"] > RATE_LIMIT

    def do_GET(self):
        _state["count"] += 1
        if _state["count"] % JITTER_EVERY == 0:
            self._send(503, b'{"error":"upstream busy"}')
            return
        if self._throttled():
            self._send(429, b'{"error":"rate limited"}', extra={"Retry-After": "1"})
            return

        u = urlparse(self.path)
        if u.path == "/api/catalog":
            q = parse_qs(u.query)
            cursor = int((q.get("cursor") or ["0"])[0])
            limit = int((q.get("limit") or [PAGE_SIZE_DEFAULT])[0])
            page = CATALOG[cursor:cursor + limit]
            nxt = cursor + limit
            body = {"items": page, "next_cursor": nxt if nxt < len(CATALOG) else None}
            self._send(200, json.dumps(body).encode())
            return

        if u.path.startswith("/api/item/"):
            sku = u.path.rsplit("/", 1)[-1]
            if _bad_json(sku):
                self._send(200, b'{"sku": "SKU-0000", "na')  # 截断的非法 JSON
                return
            one = next((x for x in CATALOG if x["sku"] == sku), None)
            if not one:
                self._send(404, b'{"error":"not found"}')
                return
            self._send(200, json.dumps(one).encode())
            return

        self._send(404, b'{"error":"no route"}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"mock portal on http://127.0.0.1:{args.port}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
