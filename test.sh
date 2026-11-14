#!/usr/bin/env bash
# 一键自测：单测 + 端到端行为校验，逐条中文 PASS/FAIL。
# 用法：bash test.sh
set -uo pipefail
cd "$(dirname "$0")"

PORT="${PORT:-8766}"     # 避开 start.sh 的 8765
DB="$(mktemp -d)/test.db"
PASS=0
FAIL=0

cleanup() {
  [ -n "${PORTAL:-}" ] && kill "$PORTAL" 2>/dev/null
  wait "$PORTAL" 2>/dev/null
  rm -rf "$(dirname "$DB")"
}
trap cleanup EXIT

check() {  # check <描述> <0=通过>
  if [ "$2" -eq 0 ]; then echo "  ✅ $1"; PASS=$((PASS+1)); else echo "  ❌ $1"; FAIL=$((FAIL+1)); fi
}

echo "=============================================="
echo " catalog-collector 自测"
echo "=============================================="

echo "[1/2] 单元测试"
python3 -m unittest discover -s tests -q 2>&1 | tail -3
check "unittest 全过" "$?"

echo
echo "[2/2] 端到端行为校验（起 mock 门户跑一轮）"
pkill -f "mock_portal.py --port $PORT" 2>/dev/null
lsof -ti :"$PORT" 2>/dev/null | xargs kill -9 2>/dev/null
sleep 0.5
python3 mock_portal.py --port "$PORT" >/dev/null 2>&1 &
PORTAL=$!
sleep 1.5
python3 -m pipeline.cli --db "$DB" --page-size 10 --workers 4 --base "http://127.0.0.1:$PORT" >/dev/null 2>&1
check "整轮 exit=0" "$?"

python3 - "$DB" <<'PY'
import importlib.util, json, os, sqlite3, sys
db = sys.argv[1]
res = []

def add(name, ok, detail=""):
    res.append((name, bool(ok), detail))

c = sqlite3.connect(db)
n = c.execute("select count(*) from items").fetchone()[0]
d = c.execute("select count(distinct sku) from items").fetchone()[0]
dead = c.execute("select count(*) from dead_letter").fetchone()[0]
ck = c.execute("select value from checkpoint where key='next_cursor'").fetchone()
got = {r[0] for r in c.execute("select sku from items")}
c.close()
expected = {f"SKU-{i:04d}" for i in range(1, 121) if i % 17 != 0}

add("去重：items == distinct sku", n == d and n > 0, f"items={n} distinct={d}")
add("不重不漏：与期望 sku 集合一致", got == expected,
    f"missing={sorted(expected-got)[:3]} extra={sorted(got-expected)[:3]}")
add("脏数据隔离：dead_letter == 7", dead == 7, f"dead_letter={dead}")
add("断点续传：跑完 checkpoint 复位", ck is None, f"next_cursor={ck}")

r = json.load(open("run_report.json", encoding="utf-8"))
need_top = ["total_duration_s", "pages", "totals", "failures"]
need_tot = ["requests", "retries", "dedup_discarded", "dead_lettered"]
add("run_report 字段齐全",
    all(k in r for k in need_top) and all(k in r.get("totals", {}) for k in need_tot))
add("run_report 失败分类", "invalid_json" in (r.get("failures") or {}), str(r.get("failures")))
add("run_report 每页耗时/重试",
    bool(r.get("pages")) and all(k in r["pages"][0] for k in ("cursor", "duration_s", "retries")))

third = []
for root, _dirs, files in os.walk("."):
    if ".git" in root:
        continue
    for fn in files:
        if not fn.endswith(".py"):
            continue
        p = os.path.join(root, fn)
        for line in open(p, encoding="utf-8", errors="ignore"):
            line = line.strip()
            if line.startswith(("import ", "from ")):
                mod = line.split()[1].split(".")[0]
                if mod in ("pipeline", "__future__"):
                    continue
                spec = importlib.util.find_spec(mod) if mod else None
                origin = (spec.origin or "") if spec else ""
                if "site-packages" in origin or "dist-packages" in origin:
                    third.append(f"{p}: {line}")
add("无第三方依赖", not third, str(third))

for name, ok, detail in res:
    print(f"  {'✅' if ok else '❌'} {name}" + (f"  [{detail}]" if detail else ""))
sys.exit(1 if any(not ok for _n, ok, _d in res) else 0)
PY
check "端到端 7 项校验" "$?"

rm -f run_report.json
echo "=============================================="
echo " 通过 $PASS 项，失败 $FAIL 项"
echo "=============================================="
[ "$FAIL" -eq 0 ]
