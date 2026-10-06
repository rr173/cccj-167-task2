# 逻辑复制槽跨集群迁移控制器

在不停止源集群写入的前提下，把一个逻辑复制槽（连同它的下游消费者）从源集群
安全迁移到目标集群。控制器负责：**冻结迁移计划 → 建立一致快照 → 传输增量 →
零停机校验 → 原子切换消费者 → 出具切换证明**。

```
slotmig/
  cluster.py      集群模拟：复制日志(LSN)、槽、表、模式版本、写围栏、运行校验和
  store.py        控制器持久化状态（SQLite）：计划、事件、快照、LSN 映射、消费者组、证明
  controller.py   迁移状态机（核心）：freeze/start/catch-up/verify/cutover/revoke/retry/abort
  proof.py        切换证明与撤回凭证的生成及独立验证
  reconcile.py    位点对账报告 + HTML 页面（含观察期撤回资格）
  api.py          HTTP API（仅标准库）
tests/            42 个端到端与边界场景测试
demo.py           全流程演示（写入线程 + 消费者线程并发 + 观察期撤回）
```

## 快速开始

```bash
cd slot-migration
python3 -m unittest discover -s tests   # 运行测试（42 个）
python3 demo.py                          # 端到端演示
python3 demo.py --serve                  # 演示后保持 API 运行（打开对账页面）
python3 -m slotmig.api --port 8080       # 只启动 API 服务（含演示数据）
```

## 状态机

```
DRAFT ──freeze──▶ FROZEN ──start──▶ STREAMING ──verify──▶ VERIFIED ──cutover──▶ COMPLETED
                     ▲                 │                     │                 │
                     │                 └── 失败 ──▶ FAILED ──┤            观察期内 revoke
                     └────────── retry（attempt+1）──────────┘                 │
                                                                               ▼
                                                                            REVOKED（终态）
任意非终态 ──abort──▶ ABORTED（终态）
COMPLETED ──观察期结束 / 接管方确认新记录 / 解除源保护──▶ 撤回被永久拒绝（计划保持 COMPLETED）
```

* **freeze** 冻结：源集群/槽、目标订阅、表集合、模式版本、冻结位点、消费者组。
  冻结后规格不可变；同一槽/订阅/消费者组同时只允许一个活动计划。
* **start** 在源端锁内原子拷贝表数据 + 记录快照位点（一致快照），写入目标端并
  自检装载校验和，随后从快照位点之后续传增量——快照与增量无缝并集，不丢不重。
* **catch-up** 幂等追增量：目标端按 `(plan_id, 源LSN)` 去重，任意重试安全。
* **verify** 零停机校验：向源日志注入校验标记（marker），源端在标记落日志的
  同一瞬间取运行校验和快照；标记流式到达目标后，目标取自己的运行校验和快照；
  两侧摘要比对一致才进入 VERIFIED。校验期间源端照常写入。
* **cutover** 原子切换（见下）；可在创建计划时以 `observation_seconds`
  配置观察期（默认 300 秒），切换成功即进入观察期。
* **revoke** 观察期撤回交接（见下）。
* **retry** 失败计划以新 attempt 重试（重建快照、自动清空目标表、重传增量）；
  模式变更导致的失败**不可重试**，必须新建计划。
* **release-source** 切换后源表保持围栏（防双写脑裂）；运维确认写流量已切到
  目标后调用，解除围栏。**注意：解除后撤回资格立即丧失。**

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

## 观察期撤回协议（revoke）

交接成功后计划进入长度可配置（`observation_seconds`，创建计划时指定，
默认 300 秒）的观察期，并在**切换事务内**保存接管方开始服务时的确认游标
（`takeover_cursor`：接管槽、epoch、组水位、槽确认位点、交接时刻）。

操作员在观察期内可调用 `POST /plans/{id}/revoke` 撤回交接。撤回**仅在三个
条件同时满足**时允许：

1. **观察期未结束** —— 当前时间 ≤ `cutover_at + observation_seconds`
   （`OBSERVATION_EXPIRED`）；
2. **接管方尚未确认任何新记录** —— 消费者组水位与接管槽确认位点都停留在
   保存的接管游标上（`TAKEOVER_PROGRESSED`）；
3. **原服务仍处于保护状态** —— 源表写围栏仍由本计划持有
   （`SOURCE_NOT_PROTECTED`）。

允许时，撤回的核心动作在**同一个 SQLite 事务**中原子完成：

1. 事务内复核：计划仍为 COMPLETED、epoch 未变、接管水位确实未推进；
2. 从持久化的切换证明锚点（`mapping_anchor`）取回**原服务归属与原游标**；
3. **fencing epoch 再 +1**，消费者组指回原槽，水位**恢复为原源 LSN 游标
   （保留，不重置）**；
4. 写入防篡改的**撤回凭证**（绑定切换证明 id/哈希、撤回时资格快照、恢复后
   的归属与 epoch、保留的双侧游标）；
5. 计划置 **REVOKED**。任一步失败整体回滚，接管保持有效。

事务提交后调整集群层：源槽 RETIRED→ACTIVE（确认位点还原为保留的源游标）、
接管槽置 RETIRED、解除源表写围栏——原服务在它停下的位点精确恢复（测试
`test_resumed_consumer_exact_no_gap_no_overlap` 逐事件验证恢复后无间隙无
重叠）。

**语义保证：**

| 需求 | 机制 |
|---|---|
| 撤回必须原子 | 归属恢复 + epoch 递增 + 凭证落库 + REVOKED 单事务；集群层操作在提交后执行 |
| 游标保留 | 恢复水位 = 切换证明锚点的源 LSN；接管侧目标 LSN 记入凭证 |
| 撤回可验真 | 撤回凭证 14 项独立校验：哈希完整性、epoch 步进、归属恢复、保留游标与切换锚点一致、计划已撤回、库内哈希一致等 |
| 重复请求只返回既有结果 | 计划已是 REVOKED 时，再次 revoke 原样返回既有凭证，不产生新事件/状态 |
| 拒绝时状态不变 | 拒绝只做只读检查并返回稳定错误码（含全部阻止条件与截止时间），不写事件、不改任何状态 |
| 旧接管方被围栏 | 撤回后 epoch=3，接管方 epoch 2 的迟到提交 → `STALE_EPOCH`；接管槽 RETIRED → `SLOT_NOT_ACTIVE` |
| 撤回后可再迁移 | REVOKED 为终态，不占用槽/组；可新建计划，epoch 继续递增 |

观察期资格不做后台定时任务，而是在查询/撤回时按保存的 `cutover_at`
**按需计算**（控制器支持注入时钟，测试可确定性跨越截止时间）。

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

`POST /proofs/verify`（或 `slotmig.verify_proof(proof, store)`；撤回凭证会
按 `kind` 字段自动分派到撤回校验）执行检查：
哈希完整性、epoch 步进、水位映射一致性、摘要存在性、计划已完成、消费者组
已切换、库内证明哈希一致、LSN 映射重算一致。篡改任何字段都会使
`hash_integrity` 失败。

### 撤回凭证（revocation credential）

撤回成功即落库一份独立的防篡改凭证（`kind: "REVOCATION"`），结构：

```json
{
  "kind": "REVOCATION", "revocation_id": "revocation_…",
  "plan_id": "plan_…", "attempt": 1,
  "cutover_ref":  { "proof_id": "proof_…", "proof_hash": "…",
                    "cutover_at": "…", "cutover_lsn": 339 },
  "observation":  { "window_seconds": 300, "deadline": "…",
                    "elapsed_seconds": 12.4, "within_window": true },
  "handoff_back": { "from":     {"cluster":"target","slot":"…","epoch":2,
                                 "watermark_target_lsn":85},
                    "restored": {"cluster":"source","slot":"…","epoch":3,
                                 "watermark_source_lsn":336},
                    "retained_cursor": {"source_lsn":336,"target_lsn":85,
                                        "cutover_proof_id":"…","cutover_proof_hash":"…"} },
  "eligibility":  { "takeover_confirmed_new_records": false,
                    "takeover_cursor": {…},
                    "source_still_protected": true, "source_fence_token": "plan:…:1" },
  "slots": { …撤回瞬间的双侧槽状态… },
  "timeline_digest": "…",
  "revocation_hash": "sha256(除自身外的全部字段)"
}
```

`verify_revocation(credential, store)` 执行 14 项检查（哈希完整性、kind、
epoch 步进 2→3、保留游标与内部字段一致、引用了切换证明、撤回发生在窗口内、
无接管确认、源当时受保护、计划已撤回且链接一致、消费者组已恢复、库内哈希
一致、切换证明哈希匹配、保留游标与切换锚点重算一致）。

## 位点对账页面

`GET /plans/{id}/reconcile.html`（5 秒自动刷新），JSON 版本在
`/plans/{id}/reconcile`：

* 冻结规格（源/目标/表集合/模式版本）与当前状态；
* 八个关键位点：冻结位点、快照位点、源当前位点、已扫描位点、目标已应用
  位点、落后事件数、校验标记位点、切换位点；
* 各表行数与校验和（即时值，仅供参考）+ 校验标记处的权威比对结论；
* 消费者组：活跃槽、epoch、水位、两侧槽状态；
* **交接观察期与撤回资格**：资格（可撤回/不可撤回/已撤回）、交接时刻与
  截止时间、剩余/已用秒数、保存的接管游标与当前游标、三条阻止条件
  （接管方已确认新记录 / 原服务已解除保护 / 观察期已结束）逐条结论、
  撤回凭证链接；
* 切换证明哈希与链接、撤回凭证哈希与链接、完整事件时间线
  （`CUTOVER_EXECUTED` 与 `HANDOVER_REVOKED` 高亮）。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/plans` | 创建计划（DRAFT） |
| POST | `/plans/{id}/freeze` | 冻结规格 |
| POST | `/plans/{id}/start` | 一致快照 + 打开增量流 |
| POST | `/plans/{id}/catch-up` | 追增量（幂等） |
| POST | `/plans/{id}/verify` | 零停机校验 |
| POST | `/plans/{id}/cutover` | 原子切换（需 VERIFIED；成功即进入观察期） |
| POST | `/plans/{id}/revoke` | 观察期内撤回交接（幂等；不符合条件返回拒绝原因） |
| POST | `/plans/{id}/retry` `/abort` `/release-source` | 重试 / 中止 / 释放源围栏 |
| GET | `/plans/{id}` `/plans` | 计划详情 / 列表 |
| GET | `/plans/{id}/reconcile[.html]` | 位点对账（JSON / 页面，含撤回资格） |
| GET | `/plans/{id}/proof` | 切换证明 |
| GET | `/plans/{id}/revocation` | 撤回凭证 |
| POST | `/proofs/verify` | 独立验证证明或撤回凭证（按 `kind` 分派） |
| POST | `/groups/{g}/commit` | 消费者水位提交（epoch 围栏） |
| POST | `/clusters/{c}/write` `/delete` `/schema-change` | 集群模拟驱动 |
| GET | `/clusters/{c}/slots/{s}/read?after=N` | 槽消费读取（RETIRED 拒读） |

错误统一为 `{"error": 稳定错误码, "message": …, "details": …}`，如
`STALE_EPOCH`、`WRITES_FENCED`、`SCHEMA_CHANGED`、`VERIFY_MISMATCH`、
`TARGET_NOT_EMPTY`。撤回被拒绝时使用 `TAKEOVER_PROGRESSED`、
`SOURCE_NOT_PROTECTED`、`OBSERVATION_EXPIRED`（details 中附完整阻止条件、
截止时间与剩余秒数）。

## 设计说明与权衡

* **集群是进程内模拟**（`Cluster`）：日志、表、槽、围栏、校验和都是真实语义，
  只是没有网络与磁盘。控制器状态（SQLite）是真实持久化的，切换事务是真实的。
* **校验为何用标记而不是停写比对**：标记与日志追加原子地完成校验和快照，
  使两侧在同一个逻辑位点比对，源端全程可写；最终切换仍需一个短暂围栏窗口
  （与生产系统一致），窗口内写入收到可重试错误而非丢失。
* **目标日志只含流式事件**：快照装载不产生日志条目（同 Postgres 初始 COPY），
  因此新槽水位 = 旧水位经 LSN 映射表换算（floor map），精确衔接。
* **切换后源表保持围栏**：防止"消费者已切走、写入仍进源"的静默丢数；
  运维确认写流量切换后调用 `release-source`。围栏同时是撤回前提之一——
  一旦解除，观察期撤回将以 `SOURCE_NOT_PROTECTED` 拒绝。
* **观察期撤回是单事务逆操作**：归属恢复、epoch 递增、游标保留、撤回凭证
  同事务落库；集群层（槽状态/围栏）在提交后调整。接管方一旦确认了新
  记录，交接即"钉死"不可撤，保证接管方已处理的效果不被回退。
* 局限：单进程控制器（锁串行化）；一个槽同时只允许一个活动计划；快照拷贝
  在源锁内完成（真实系统用 MVCC 快照替代）。
