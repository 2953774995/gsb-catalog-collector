# catalog-collector

供应商目录采集器。上游是 `mock_portal.py`（本地模拟门户，供离线复现）。

## 现状

```
mock_portal.py        本地模拟门户（限流 / 5xx 抖动 / 跨页重复 / 脏明细）
pipeline/fetch.py     滑动窗口限速 + 429/5xx 指数退避重试（尊重 Retry-After）
pipeline/store.py     items 按 updated_at upsert 去重；dead_letter 隔离脏数据；checkpoint 断点
pipeline/cli.py       页级并行（wave）+ 明细并发，断点续传，每轮输出 run_report.json
tests/                现有冒烟测试
```

跑一次：

```bash
python3 mock_portal.py --port 8765 &
python3 -m pipeline.cli --db catalog.db --page-size 10
python3 -m pytest -q      # 或 python3 -m unittest discover -s tests
```

## 行为说明

- 限速：所有请求共享一个滑动窗口限速器（任意 1.05s 内最多 5 个），页与页之间、
  页内明细都并发，但整体不超上游 5 req/s
- 重试：429 优先按响应头 Retry-After 等待，否则与 5xx/网络错误一样指数退避
  （0.5s 起步、翻倍、封顶 10s、最多 8 次）
- 去重：同一 sku 多页重复上架时按 updated_at 保留最新一条，旧记录计入
  dedup_discarded，不会抛主键冲突
- 断点续传：每页在一个事务内 upsert 并推进 checkpoint 表；进程被 kill 后重跑
  会从上次成功提交的下一页继续，已入库数据靠 upsert 幂等，不重不漏；
  一轮完整跑完后 checkpoint 复位，下一轮是全新采集
- 脏数据隔离：明细接口的截断 JSON 等单条失败记入 dead_letter 表
  （含 stage/reason/message），不影响整轮；同一 (sku, stage) 重跑不堆叠
- 报告：每轮结束（含失败）写 run_report.json —— 总耗时、每页耗时与重试次数、
  去重丢弃数、失败分类统计、请求/重试总量

## 约束

- 只用 Python 标准库（评测环境不联网装包）
- 数据规模约 120 条，单机跑
- 入库结果要能被下游按 `updated_at` 做增量对比
