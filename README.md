# catalog-collector

供应商目录采集器。上游是 `mock_portal.py`（本地模拟门户，供离线复现）。

## 现状

```
mock_portal.py        本地模拟门户（限流 / 5xx 抖动 / 跨页重复 / 脏明细）
pipeline/fetch.py     单次 GET，无重试、无限流
pipeline/store.py     SQLite 单表 items，直接 INSERT
pipeline/cli.py       从 cursor=0 翻到最后一页
tests/                现有冒烟测试
```

跑一次：

```bash
python3 mock_portal.py --port 8765 &
python3 -m pipeline.cli --db catalog.db --page-size 10
python3 -m pytest -q      # 或 python3 -m unittest discover -s tests
```

## 已知问题

- 上游 429/503 一出现就直接抛异常，整轮中断，已入库的页留在库里
- 跨页重复的 sku 会撞主键，`INSERT` 抛 `IntegrityError`
- 没有断点续传，重跑从 cursor=0 开始，重复写库
- 没有并发，120 条数据要 13 次串行请求
- 明细接口偶发非法 JSON，目前没有隔离

## 约束

- 只用 Python 标准库（评测环境不联网装包）
- 数据规模约 120 条，单机跑
- 入库结果要能被下游按 `updated_at` 做增量对比
