import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import store  # noqa: E402


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.conn = store.connect(":memory:")

    def test_save_and_count(self):
        n = store.save_page(self.conn, [{
            "sku": "SKU-0001", "name": "a", "price": 1.0,
            "currency": "CNY", "stock": 3, "updated_at": "2026-03-01T00:00:00Z",
        }], 0)
        self.assertEqual(n, 1)
        self.assertEqual(store.count(self.conn), 1)

    def test_duplicate_sku_raises(self):
        row = [{
            "sku": "SKU-0001", "name": "a", "price": 1.0,
            "currency": "CNY", "stock": 3, "updated_at": "2026-03-01T00:00:00Z",
        }]
        store.save_page(self.conn, row, 0)
        with self.assertRaises(Exception):
            store.save_page(self.conn, row, 1)


if __name__ == "__main__":
    unittest.main()
