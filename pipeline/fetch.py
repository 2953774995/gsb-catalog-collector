"""抓取层：全局限速 + 指数退避重试（仅标准库，线程安全）。

- RateLimiter：滑动窗口限速，任意 window 秒内放行不超过 max_calls 个请求，
  多线程共享一个实例即可把整体速率压在上游限额以内。
- fetch_json：429/5xx/网络错误指数退避重试；429 优先尊重响应头 Retry-After。
- 不可重试的失败（4xx、JSON 解析失败、重试耗尽）抛 FetchError，
  由上层归入 dead_letter 并做失败分类统计。
"""
from __future__ import annotations

import collections
import email.utils
import http.client
import json
import random
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

BASE = "http://127.0.0.1:8765"
TIMEOUT = 10
MAX_RETRIES = 8
BACKOFF_BASE = 0.5   # 首次退避秒数，之后按 2**attempt 指数增长
BACKOFF_CAP = 10.0   # 单次退避上限


class FetchError(Exception):
    """不可重试或重试耗尽的抓取失败。category 用于失败分类统计。"""

    def __init__(self, category, message, status=None):
        super().__init__(message)
        self.category = category
        self.status = status


class RateLimiter:
    """滑动窗口限速器：任意 period 秒内放行不超过 max_calls 个请求。线程安全。"""

    def __init__(self, max_calls=5, period=1.05):
        self.max_calls = max_calls
        self.period = period
        self._lock = threading.Lock()
        self._calls = collections.deque()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                while self._calls and now - self._calls[0] >= self.period:
                    self._calls.popleft()
                if len(self._calls) < self.max_calls:
                    self._calls.append(now)
                    return
                wait = self.period - (now - self._calls[0])
            time.sleep(max(wait, 0.01))


class Stats:
    """跨线程共享的请求/重试计数器。"""

    def __init__(self):
        self._lock = threading.Lock()
        self.requests = 0
        self.retries = 0

    def request(self):
        with self._lock:
            self.requests += 1

    def retry(self):
        with self._lock:
            self.retries += 1


def _parse_retry_after(value):
    """Retry-After 可能是秒数或 HTTP 日期，返回等待秒数；解析不了返回 None。"""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        dt = email.utils.parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return None


def _backoff(attempt):
    """指数退避 + 抖动，封顶 BACKOFF_CAP。"""
    return min(BACKOFF_CAP, BACKOFF_BASE * (2 ** attempt)) + random.uniform(0, 0.3)


def fetch_json(path, limiter=None, stats=None, base=None):
    """GET 一个 JSON 接口，带限速与重试。失败抛 FetchError。"""
    url = (base or BASE) + path
    attempt = 0
    while True:
        if limiter is not None:
            limiter.acquire()
        if stats is not None:
            stats.request()
        try:
            with urllib.request.urlopen(url, timeout=TIMEOUT) as resp:
                body = resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.status == 429:
                # 限流：必须尊重 Retry-After，没有才退回到指数退避
                wait = _parse_retry_after(e.headers.get("Retry-After"))
                if wait is None:
                    wait = _backoff(attempt)
            elif 500 <= e.status < 600:
                wait = _backoff(attempt)
            else:
                raise FetchError("http_%d" % e.status,
                                 "GET %s -> HTTP %d" % (path, e.status), e.status)
            attempt += 1
            if attempt > MAX_RETRIES:
                raise FetchError("exhausted_retries",
                                 "GET %s gave up after %d retries (HTTP %d)"
                                 % (path, MAX_RETRIES, e.status), e.status)
            if stats is not None:
                stats.retry()
            time.sleep(wait)
            continue
        except (urllib.error.URLError, TimeoutError, OSError,
                http.client.HTTPException) as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise FetchError("network_error",
                                 "GET %s network error: %s" % (path, e))
            if stats is not None:
                stats.retry()
            time.sleep(_backoff(attempt))
            continue
        try:
            return json.loads(body)
        except json.JSONDecodeError as e:
            # 截断/非法 JSON 属于数据问题，重试无意义，交给 dead_letter
            raise FetchError("invalid_json",
                             "GET %s invalid JSON: %s" % (path, e))


def list_page(cursor, limit=10, limiter=None, stats=None, base=None):
    return fetch_json("/api/catalog?cursor=%d&limit=%d" % (cursor, limit),
                      limiter=limiter, stats=stats, base=base)


def item_detail(sku, limiter=None, stats=None, base=None):
    return fetch_json("/api/item/%s" % sku,
                      limiter=limiter, stats=stats, base=base)
