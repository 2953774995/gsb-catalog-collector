#!/usr/bin/env bash
# 一键启动：起 mock 门户 + 跑一轮完整采集，打印中文摘要。
# 用法：bash start.sh            跑完即退出
#       bash start.sh --hold     跑完暂停，等回车（录屏用）
set -uo pipefail
cd "$(dirname "$0")"

PORT="${PORT:-8765}"
DB="${DB:-catalog.db}"

cleanup() {
  [ -n "${PORTAL:-}" ] && kill "$PORTAL" 2>/dev/null
  wait "$PORTAL" 2>/dev/null
}
trap cleanup EXIT

echo "=============================================="
echo " catalog-collector 一键启动"
echo "=============================================="

# 清干净上一轮残留，保证可复现
pkill -f "mock_portal.py --port $PORT" 2>/dev/null
lsof -ti :"$PORT" 2>/dev/null | xargs kill -9 2>/dev/null
sleep 0.5
rm -f "$DB" "$DB-wal" "$DB-shm" run_report.json

echo "[1/3] 启动 mock 门户 http://127.0.0.1:$PORT ..."
python3 mock_portal.py --port "$PORT" >/dev/null 2>&1 &
PORTAL=$!
sleep 1.5
if ! lsof -ti :"$PORT" >/dev/null 2>&1; then
  echo "  ❌ 门户没起来，退出"
  exit 1
fi
echo "  ✅ 门户就绪"

echo "[2/3] 开始采集（限速 5 req/s + 退避重试 + 去重 + 断点续传）..."
START=$(date +%s)
python3 -m pipeline.cli --db "$DB" --page-size 10 --workers 4
RC=$?
END=$(date +%s)
echo "  exit=$RC 墙钟=$((END-START))s"

echo "[3/3] 结果校验"
python3 - "$DB" <<'PY'
import json, os, sqlite3, sys
db = sys.argv[1]
ok = True

def line(flag, text):
    print(f"  {'✅' if flag else '❌'} {text}")

if os.path.exists("run_report.json"):
    r = json.load(open("run_report.json", encoding="utf-8"))
    t = r["totals"]
    line(r.get("status") == "ok", f"run_report: 状态={r.get('status')} 请求={t['requests']} "
         f"重试={t['retries']} 去重丢弃={t['dedup_discarded']} 死信={t['dead_letter_in_db']} "
         f"耗时={r['total_duration_s']}s")
else:
    line(False, "run_report.json 不存在")
    ok = False

if os.path.exists(db):
    c = sqlite3.connect(db)
    n = c.execute("select count(*) from items").fetchone()[0]
    d = c.execute("select count(distinct sku) from items").fetchone()[0]
    try:
        dead = c.execute("select count(*) from dead_letter").fetchone()[0]
    except sqlite3.OperationalError:
        dead = None
    line(n == d and n > 0, f"入库 {n} 行 / distinct sku {d}（不重）")
    line(dead == 7, f"dead_letter {dead} 条（预期 7 条截断 JSON）")
    c.close()
else:
    line(False, f"{db} 不存在")
    ok = False

sys.exit(0 if ok else 1)
PY
echo "=============================================="

if [ "${1:-}" = "--hold" ]; then
  echo
  read -r -p "按回车结束…" _
fi
