import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import store  # noqa: E402


def _row(sku="SKU-0001", updated_at="2026-03-01T00:00:00Z", price=1.0):
    return {"sku": sku, "name": "a", "price": price, "currency": "CNY",
            "stock": 3, "updated_at": updated_at}


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.conn = store.connect(":memory:")

    def test_upsert_and_count(self):
        ins, upd, drop = store.upsert_items(self.conn, [_row()], 0)
        self.assertEqual((ins, upd, drop), (1, 0, 0))
        self.assertEqual(store.count(self.conn), 1)

    def test_duplicate_sku_keeps_latest(self):
        store.upsert_items(self.conn, [_row(updated_at="2026-03-05T00:00:00Z")], 0)
        # 更旧的重复上架：丢弃，不抛主键冲突
        ins, upd, drop = store.upsert_items(
            self.conn, [_row(updated_at="2026-03-01T00:00:00Z", price=9.9)], 1)
        self.assertEqual((ins, upd, drop), (0, 0, 1))
        row = self.conn.execute(
            "SELECT updated_at, price FROM items WHERE sku='SKU-0001'").fetchone()
        self.assertEqual(row, ("2026-03-05T00:00:00Z", 1.0))
        # 更新的数据：覆盖
        ins, upd, drop = store.upsert_items(
            self.conn, [_row(updated_at="2026-03-09T00:00:00Z", price=9.9)], 2)
        self.assertEqual((ins, upd, drop), (0, 1, 0))
        row = self.conn.execute(
            "SELECT updated_at, price FROM items WHERE sku='SKU-0001'").fetchone()
        self.assertEqual(row, ("2026-03-09T00:00:00Z", 9.9))
        self.assertEqual(store.count(self.conn), 1)

    def test_dead_letter_upsert(self):
        store.add_dead_letter(self.conn, "run-1", "SKU-0017", "detail",
                              "invalid_json", "truncated")
        store.add_dead_letter(self.conn, "run-2", "SKU-0017", "detail",
                              "invalid_json", "truncated again")
        self.assertEqual(store.count_dead_letter(self.conn), 1)
        reason = self.conn.execute(
            "SELECT reason FROM dead_letter WHERE sku='SKU-0017'").fetchone()[0]
        self.assertEqual(reason, "invalid_json")

    def test_checkpoint_roundtrip(self):
        self.assertIsNone(store.get_checkpoint(self.conn, "next_cursor"))
        store.set_checkpoint(self.conn, "next_cursor", 40)
        self.assertEqual(store.get_checkpoint(self.conn, "next_cursor"), "40")
        store.set_checkpoint(self.conn, "next_cursor", None)
        self.assertIsNone(store.get_checkpoint(self.conn, "next_cursor"))


if __name__ == "__main__":
    unittest.main()
