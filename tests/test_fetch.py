import json
import os
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import fetch  # noqa: E402


class RateLimiterTest(unittest.TestCase):
    def test_window_not_exceeded(self):
        limiter = fetch.RateLimiter(max_calls=3, period=0.4)
        t0 = time.monotonic()
        for _ in range(7):
            limiter.acquire()
        elapsed = time.monotonic() - t0
        # 7 次调用、窗口 3 次/0.4s：至少要到第 3 个窗口，耗时 >= 0.8s
        self.assertGreaterEqual(elapsed, 0.79)


class _FlakyHandler(BaseHTTPRequestHandler):
    """前 2 次 503，第 3 次 429+Retry-After，第 4 次才给正常 JSON。"""

    hits = 0
    retry_after_seen_at = None

    def log_message(self, *a):
        pass

    def do_GET(self):
        type(self).hits += 1
        n = type(self).hits
        if n <= 2:
            body = b'{"error":"busy"}'
            self.send_response(503)
        elif n == 3:
            body = b'{"error":"limited"}'
            self.send_response(429)
            self.send_header("Retry-After", "1")
            type(self).retry_after_seen_at = time.monotonic()
        else:
            body = json.dumps({"ok": True}).encode()
            self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class RetryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _FlakyHandler.hits = 0
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), _FlakyHandler)
        cls.port = cls.srv.server_address[1]
        cls.thread = threading.Thread(target=cls.srv.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def test_retries_and_retry_after(self):
        fetch.BACKOFF_BASE = 0.05  # 测试提速
        try:
            t0 = time.monotonic()
            out = fetch.fetch_json("/x", base="http://127.0.0.1:%d" % self.port)
            elapsed = time.monotonic() - t0
            self.assertEqual(out, {"ok": True})
            self.assertEqual(_FlakyHandler.hits, 4)
            # 429 的 Retry-After: 1 必须被尊重：总耗时至少 ~1s
            self.assertGreaterEqual(elapsed, 0.95)
        finally:
            fetch.BACKOFF_BASE = 0.5

    def test_invalid_json_raises_fetch_error(self):
        class _Bad(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                body = b'{"sku": "X", "na'
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        srv = ThreadingHTTPServer(("127.0.0.1", 0), _Bad)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            with self.assertRaises(fetch.FetchError) as cm:
                fetch.fetch_json("/x",
                                 base="http://127.0.0.1:%d" % srv.server_address[1])
            self.assertEqual(cm.exception.category, "invalid_json")
        finally:
            srv.shutdown()

    def test_parse_retry_after(self):
        self.assertEqual(fetch._parse_retry_after("2"), 2.0)
        self.assertIsNone(fetch._parse_retry_after(None))
        self.assertIsNone(fetch._parse_retry_after("garbage"))


if __name__ == "__main__":
    unittest.main()
