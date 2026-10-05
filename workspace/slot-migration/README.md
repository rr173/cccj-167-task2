# 逻辑复制槽跨集群迁移控制器

在不停止源集群写入的前提下，把一个逻辑复制槽（连同它的下游消费者）从源集群
安全迁移到目标集群。控制器负责：**冻结迁移计划 → 建立一致快照 → 传输增量 →
零停机校验 → 原子切换消费者 → 出具切换证明**。

```
slotmig/
  cluster.py      集群模拟：复制日志(LSN)、槽、表、模式版本、写围栏、运行校验和
  store.py        控制器持久化状态（SQLite）：计划、事件、快照、LSN 映射、消费者组、证明
  controller.py   迁移状态机（核心）：freeze/start/catch-up/verify/cutover/retry/abort
  proof.py        切换证明的生成与独立验证
  reconcile.py    位点对账报告 + HTML 页面
  api.py          HTTP API（仅标准库）
tests/            24 个端到端与边界场景测试
demo.py           全流程演示（写入线程 + 消费者线程并发）
```

## 快速开始

```bash
cd slot-migration
python3 -m unittest discover -s tests   # 运行测试（24 个）
python3 demo.py                          # 端到端演示
python3 demo.py --serve                  # 演示后保持 API 运行（打开对账页面）
python3 -m slotmig.api --port 8080       # 只启动 API 服务（含演示数据）
```

## 状态机

```
DRAFT ──freeze──▶ FROZEN ──start──▶ STREAMING ──verify──▶ VERIFIED ──cutover──▶ COMPLETED
                     ▲                 │                     │
                     │                 └── 失败 ──▶ FAILED ──┤
                     └────────── retry（attempt+1）──────────┘
任意非终态 ──abort──▶ ABORTED（终态）
```

* **freeze** 冻结：源集群/槽、目标订阅、表集合、模式版本、冻结位点、消费者组。
  冻结后规格不可变；同一槽/订阅/消费者组同时只允许一个活动计划。
* **start** 在源端锁内原子拷贝表数据 + 记录快照位点（一致快照），写入目标端并
  自检装载校验和，随后从快照位点之后续传增量——快照与增量无缝并集，不丢不重。
* **catch-up** 幂等追增量：目标端按 `(plan_id, 源LSN)` 去重，任意重试安全。
* **verify** 零停机校验：向源日志注入校验标记（marker），源端在标记落日志的
  同一瞬间取运行校验和快照；标记流式到达目标后，目标取自己的运行校验和快照；
  两侧摘要比对一致才进入 VERIFIED。校验期间源端照常写入。
* **cutover** 原子切换（见下）。
* **retry** 失败计划以新 attempt 重试（重建快照、自动清空目标表、重传增量）；
  模式变更导致的失败**不可重试**，必须新建计划。
* **release-source** 切换后源表保持围栏（防双写脑裂）；运维确认写流量已切到
  目标后调用，解除围栏。

## 原子切换协议（cutover）

1. **围栏**：对计划表集合加写围栏（fencing token），此后源端对这些表的写入
   立即收到可重试的 `WRITES_FENCED` 错误——这是切换窗口，也是唯一的停写点；
2. **排空**：增量流追平到围栏瞬间的源位点 `cutover_lsn`；
3. **最终校验**：围栏下两侧全量重算校验摘要，不一致则解除围栏、计划置 FAILED、
   消费者原地不动（可重试）；
4. **单事务切换**：在一个 SQLite 事务内完成——复核消费者组 epoch、把旧水位
   （源 LSN）经 LSN 映射表换算成目标 LSN、epoch+1、活跃槽指向新槽、写入
   切换证明、计划置 COMPLETED。任何一步失败整体回滚；
5. **集群级围栏**：源槽置 RETIRED（拒绝读取），目标槽确认位点更新为新水位。

切换后消费者从新槽的换算水位继续消费，**无间隙、无重叠**（测试
`test_consumer_handoff_is_exact_no_gap_no_overlap` 逐事件验证）。

## 关键不变量

| 需求 | 机制 |
|---|---|
| 源端继续写入不丢事件 | 快照 ≤ snapshot_lsn ∪ 增量 > snapshot_lsn；切换前排空到 cutover_lsn |
| 不重复推进 | 目标端按 (plan, 源LSN) 幂等应用；水位只单调前进，重复提交为幂等空操作 |
| 不存在两个有效消费水位 | 消费者组单行记录（唯一活跃槽）；epoch 围栏；切换为单事务 |
| 旧消费者迟到确认 | 旧 epoch → `STALE_EPOCH`；旧槽 → `SLOT_NOT_ACTIVE`；集群层 RETIRED 槽拒读 |
| 模式变更 | 计划冻结各表模式版本；每个阶段入口 + 流内 SCHEMA 事件双重检测 → 计划失败、消费者不受影响、禁止重试（须新建计划） |
| 迁移重试 | 全部构件按 attempt 隔离；重试自动清空目标重建；SCHEMA_CHANGED 拒绝重试 |

## 切换证明（proof）

切换成功即落库一份防篡改证明，可独立验证：

```json
{
  "proof_id": "proof_…", "plan_id": "plan_…", "attempt": 1,
  "plan_spec":    { "source_slot": "…", "target_slot": "…", "tables": […],
                    "schema_versions": {…}, "frozen_lsn": 250 },
  "snapshot":     { "snapshot_id": "…", "snapshot_lsn": 251, "manifest_digest": "…" },
  "verification": { "marker_lsn": 317, "digest": "…", "verified_at": "…" },
  "final_check":  { "cutover_lsn": 339, "digest": "…" },
  "handoff":      { "old": {"slot":"app_slot","epoch":1,"watermark_source_lsn":336},
                    "new": {"slot":"app_slot_replica","epoch":2,"watermark_target_lsn":85},
                    "mapping_anchor": {"source_lsn":336,"target_lsn":85} },
  "timeline_digest": "…",
  "proof_hash": "sha256(除自身外的全部字段)"
}
```

`POST /proofs/verify`（或 `slotmig.verify_proof(proof, store)`）执行 9 项检查：
哈希完整性、epoch 步进、水位映射一致性、摘要存在性、计划已完成、消费者组
已切换、库内证明哈希一致、LSN 映射重算一致。篡改任何字段都会使
`hash_integrity` 失败。

## 位点对账页面

`GET /plans/{id}/reconcile.html`（5 秒自动刷新），JSON 版本在
`/plans/{id}/reconcile`：

* 冻结规格（源/目标/表集合/模式版本）与当前状态；
* 八个关键位点：冻结位点、快照位点、源当前位点、已扫描位点、目标已应用
  位点、落后事件数、校验标记位点、切换位点；
* 各表行数与校验和（即时值，仅供参考）+ 校验标记处的权威比对结论；
* 消费者组：活跃槽、epoch、水位、两侧槽状态；
* 切换证明哈希与链接、完整事件时间线。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/plans` | 创建计划（DRAFT） |
| POST | `/plans/{id}/freeze` | 冻结规格 |
| POST | `/plans/{id}/start` | 一致快照 + 打开增量流 |
| POST | `/plans/{id}/catch-up` | 追增量（幂等） |
| POST | `/plans/{id}/verify` | 零停机校验 |
| POST | `/plans/{id}/cutover` | 原子切换（需 VERIFIED） |
| POST | `/plans/{id}/retry` `/abort` `/release-source` | 重试 / 中止 / 释放源围栏 |
| GET | `/plans/{id}` `/plans` | 计划详情 / 列表 |
| GET | `/plans/{id}/reconcile[.html]` | 位点对账（JSON / 页面） |
| GET | `/plans/{id}/proof` | 切换证明 |
| POST | `/proofs/verify` | 独立验证证明 |
| POST | `/groups/{g}/commit` | 消费者水位提交（epoch 围栏） |
| POST | `/clusters/{c}/write` `/delete` `/schema-change` | 集群模拟驱动 |
| GET | `/clusters/{c}/slots/{s}/read?after=N` | 槽消费读取（RETIRED 拒读） |

错误统一为 `{"error": 稳定错误码, "message": …}`，如 `STALE_EPOCH`、
`WRITES_FENCED`、`SCHEMA_CHANGED`、`VERIFY_MISMATCH`、`TARGET_NOT_EMPTY`。

## 设计说明与权衡

* **集群是进程内模拟**（`Cluster`）：日志、表、槽、围栏、校验和都是真实语义，
  只是没有网络与磁盘。控制器状态（SQLite）是真实持久化的，切换事务是真实的。
* **校验为何用标记而不是停写比对**：标记与日志追加原子地完成校验和快照，
  使两侧在同一个逻辑位点比对，源端全程可写；最终切换仍需一个短暂围栏窗口
  （与生产系统一致），窗口内写入收到可重试错误而非丢失。
* **目标日志只含流式事件**：快照装载不产生日志条目（同 Postgres 初始 COPY），
  因此新槽水位 = 旧水位经 LSN 映射表换算（floor map），精确衔接。
* **切换后源表保持围栏**：防止"消费者已切走、写入仍进源"的静默丢数；
  运维确认写流量切换后调用 `release-source`。
* 局限：单进程控制器（锁串行化）；一个槽同时只允许一个活动计划；快照拷贝
  在源锁内完成（真实系统用 MVCC 快照替代）。
