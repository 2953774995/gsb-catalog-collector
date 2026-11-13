"""落库层：items 按 updated_at 去重 upsert；dead_letter 隔离脏数据；checkpoint 支持断点续传。"""
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

CREATE TABLE IF NOT EXISTS dead_letter (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT NOT NULL,
    sku        TEXT,
    stage      TEXT NOT NULL,          -- 出错环节：detail / page / record
    reason     TEXT NOT NULL,          -- 失败分类：invalid_json / http_404 / ...
    message    TEXT,
    payload    TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(sku, stage)                 -- 断点重跑时同一失败只留一条
);

CREATE TABLE IF NOT EXISTS checkpoint (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def connect(path: str = "catalog.db") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


def upsert_items(conn: sqlite3.Connection, items: list[dict], batch_no: int):
    """按 sku 去重入库：只保留 updated_at 最新的一条，绝不抛主键冲突。

    返回 (inserted, updated, discarded)：
    - inserted  新 sku 入库数
    - updated   已有 sku 被更新的数（新数据 updated_at 更新）
    - discarded 因 updated_at 不新于库内记录而被丢弃的数（跨页重复上架）
    """
    inserted = updated = discarded = 0
    with conn:  # 单事务，要么整页落库要么不落，配合 checkpoint 保证不重不漏
        for i in items:
            row = conn.execute(
                "SELECT updated_at FROM items WHERE sku = ?", (i["sku"],)
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO items(sku,name,price,currency,stock,updated_at,batch_no) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (i["sku"], i["name"], i["price"], i["currency"],
                     i["stock"], i["updated_at"], batch_no),
                )
                inserted += 1
            elif i["updated_at"] > row[0]:
                conn.execute(
                    "UPDATE items SET name=?, price=?, currency=?, stock=?, "
                    "updated_at=?, batch_no=? WHERE sku=?",
                    (i["name"], i["price"], i["currency"], i["stock"],
                     i["updated_at"], batch_no, i["sku"]),
                )
                updated += 1
            else:
                discarded += 1
    return inserted, updated, discarded


def add_dead_letter(conn: sqlite3.Connection, run_id: str, sku, stage: str,
                    reason: str, message: str, payload=None) -> None:
    """记录一条失败数据；同一 (sku, stage) 重跑时只刷新不堆叠。"""
    with conn:
        conn.execute(
            "INSERT INTO dead_letter(run_id, sku, stage, reason, message, payload) "
            "VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(sku, stage) DO UPDATE SET "
            "run_id=excluded.run_id, reason=excluded.reason, "
            "message=excluded.message, payload=excluded.payload, "
            "created_at=datetime('now')",
            (run_id, sku, stage, reason, message, payload),
        )


def get_checkpoint(conn: sqlite3.Connection, key: str, default=None):
    row = conn.execute(
        "SELECT value FROM checkpoint WHERE key = ?", (key,)
    ).fetchone()
    return row[0] if row else default


def set_checkpoint(conn: sqlite3.Connection, key: str, value) -> None:
    """value 为 None 表示清除（一轮完整跑完后复位，下一轮从头开始）。"""
    with conn:
        if value is None:
            conn.execute("DELETE FROM checkpoint WHERE key = ?", (key,))
        else:
            conn.execute(
                "INSERT INTO checkpoint(key, value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )


def count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]


def count_dead_letter(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM dead_letter").fetchone()[0]
