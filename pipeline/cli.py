"""命令行入口：从 cursor=0 一路抓到没有下一页。"""
from __future__ import annotations

import argparse
import sys

from pipeline import fetch, store


def run(db_path: str, page_size: int, max_pages: int = 200) -> int:
    conn = store.connect(db_path)
    cursor, batch_no, pages = 0, 0, 0
    while cursor is not None and pages < max_pages:
        page = fetch.list_page(cursor, page_size)
        items = page.get("items") or []
        store.save_page(conn, items, batch_no)
        batch_no += 1
        pages += 1
        print(f"page={pages} cursor={cursor} got={len(items)} total={store.count(conn)}")
        cursor = page.get("next_cursor")
    return store.count(conn)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="catalog.db")
    ap.add_argument("--page-size", type=int, default=10)
    args = ap.parse_args(argv)
    total = run(args.db, args.page_size)
    print(f"done, rows={total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
