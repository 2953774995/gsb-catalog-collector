"""命令行入口：限速并发采集 + 断点续传 + 脏数据隔离 + 运行报告。

运行模型：
- 页与页之间按 wave 并行（游标是线性偏移，若发现上游游标非线性自动退回顺序模式）；
- 每页明细也并发抓取；所有请求共享一个滑动窗口限速器，整体不超 5 req/s；
- 每页在一个事务内 upsert 并推进 checkpoint，进程被 kill 后重跑从上次
  成功提交的页继续，已入库数据靠 updated_at upsert 保证不重复写；
- 明细抓取失败的单条记录进 dead_letter，不影响整轮；
- 每轮结束（含异常退出）写 run_report.json。
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from pipeline import fetch, store


def _fetch_page(limiter, stats, cursor, page_size):
    t0 = time.monotonic()
    r0 = stats.retries
    page = fetch.list_page(cursor, page_size, limiter=limiter, stats=stats)
    return page, time.monotonic() - t0, stats.retries - r0


def _fetch_detail(limiter, stats, sku):
    r0 = stats.retries
    try:
        rec = fetch.item_detail(sku, limiter=limiter, stats=stats)
        return rec, stats.retries - r0, None
    except fetch.FetchError as e:
        return None, stats.retries - r0, e


def run(db_path, page_size, max_pages=200, workers=4,
        report_path="run_report.json", base=None):
    if base:
        fetch.BASE = base
    conn = store.connect(db_path)
    run_id = datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")
    limiter = fetch.RateLimiter(max_calls=5, period=1.05)
    stats = fetch.Stats()
    started = time.monotonic()

    # 断点续传：从上次成功提交的下一页继续；没有 checkpoint 则从头开始
    resume_from = store.get_checkpoint(conn, "next_cursor")
    resume_from = 0 if resume_from is None else int(resume_from)

    report = {
        "run_id": run_id,
        "status": "ok",
        "resumed_from_cursor": resume_from,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "pages": [],
        "totals": {},
        "failures": {},
    }
    failures = collections.Counter()   # 失败分类统计
    totals = collections.Counter()
    batch_no = resume_from // page_size

    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            cursor = resume_from
            linear = cursor % page_size == 0
            done = False
            while not done and len(report["pages"]) < max_pages:
                if linear:
                    cursors = [cursor + i * page_size for i in range(workers)]
                else:
                    cursors = [cursor]
                futs = {ex.submit(_fetch_page, limiter, stats, c, page_size): c
                        for c in cursors}
                got = {}
                for fut in as_completed(futs):
                    got[futs[fut]] = fut.result()  # 页级重试耗尽会在此抛出，整轮失败

                next_cursor = None
                for c in sorted(got):
                    page, dur, retries = got[c]
                    items = page.get("items") or []
                    nxt = page.get("next_cursor")
                    if linear and nxt is not None and nxt != c + page_size:
                        linear = False  # 游标不是线性偏移，退回顺序跟随模式
                    if not items:
                        done = True
                        continue

                    # 明细并发抓取，单条失败进 dead_letter，不影响整页
                    det_futs = {ex.submit(_fetch_detail, limiter, stats, it["sku"]): it["sku"]
                                for it in items}
                    det = {}
                    for f in as_completed(det_futs):
                        det[det_futs[f]] = f.result()
                    good, d_retries, d_fail = [], 0, 0
                    for it in items:
                        rec, r, err = det[it["sku"]]
                        d_retries += r
                        if err is not None:
                            d_fail += 1
                            failures[err.category] += 1
                            store.add_dead_letter(
                                conn, run_id, it["sku"], "detail",
                                err.category, str(err))
                        else:
                            good.append(rec)

                    ins, upd, drop = store.upsert_items(conn, good, batch_no)
                    batch_no += 1
                    # 页提交成功后立即推进 checkpoint（同事务语义外的第二步，
                    # 崩溃最坏情况是重抓已提交的页，upsert 保证幂等）
                    store.set_checkpoint(conn, "next_cursor", nxt)

                    report["pages"].append({
                        "cursor": c, "items": len(items),
                        "duration_s": round(dur, 3), "retries": retries,
                        "detail_retries": d_retries, "detail_failures": d_fail,
                        "inserted": ins, "updated": upd, "dedup_discarded": drop,
                    })
                    totals["inserted"] += ins
                    totals["updated"] += upd
                    totals["dedup_discarded"] += drop
                    print("page cursor=%d got=%d ins=%d upd=%d drop=%d "
                          "retries=%d total=%d"
                          % (c, len(items), ins, upd, drop,
                             retries + d_retries, store.count(conn)),
                          flush=True)
                    if nxt is None:
                        done = True
                    else:
                        next_cursor = nxt

                if linear:
                    cursor += workers * page_size
                else:
                    cursor = next_cursor
                    if cursor is None:
                        done = True
    except Exception as e:  # 页级失败：保留 checkpoint，写报告后退出非零
        report["status"] = "failed"
        report["error"] = repr(e)
        failures["page_error"] += 1
        print("run failed: %r" % (e,), file=sys.stderr)
    else:
        # 完整跑完一轮：复位 checkpoint，下一轮从头开始新一轮采集
        store.set_checkpoint(conn, "next_cursor", None)

    report["total_duration_s"] = round(time.monotonic() - started, 3)
    report["totals"] = {
        "pages": len(report["pages"]),
        "requests": stats.requests,
        "retries": stats.retries,
        "inserted": totals["inserted"],
        "updated": totals["updated"],
        "dedup_discarded": totals["dedup_discarded"],
        "dead_lettered": sum(failures.values()),
        "items_in_db": store.count(conn),
        "dead_letter_in_db": store.count_dead_letter(conn),
    }
    report["failures"] = dict(failures)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return store.count(conn), report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="catalog.db")
    ap.add_argument("--page-size", type=int, default=10)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--max-pages", type=int, default=200)
    ap.add_argument("--report", default="run_report.json")
    ap.add_argument("--base", default=None, help="覆盖上游 base URL")
    args = ap.parse_args(argv)
    total, report = run(args.db, args.page_size, max_pages=args.max_pages,
                        workers=args.workers, report_path=args.report,
                        base=args.base)
    print("done, rows=%d status=%s duration=%.1fs"
          % (total, report["status"], report["total_duration_s"]))
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
