"""落库层：单表 items，主键 sku，直接 INSERT。"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    sku        TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    price      REAL NOT NULL,
    currency   TEXT NOT NULL,
    stock      INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    batch_no   INTEGER
);
"""


def connect(path: str = "catalog.db") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


def save_page(conn: sqlite3.Connection, items: list[dict], batch_no: int) -> int:
    rows = [
        (i["sku"], i["name"], i["price"], i["currency"], i["stock"], i["updated_at"], batch_no)
        for i in items
    ]
    conn.executemany(
        "INSERT INTO items(sku,name,price,currency,stock,updated_at,batch_no) "
        "VALUES(?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return len(rows)


def count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
