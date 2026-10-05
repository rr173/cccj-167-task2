#!/usr/bin/env python3
"""End-to-end demo: migrate the `app_slot` replication slot (and its
consumers) from the source cluster to the target cluster — while the source
keeps writing and an old consumer keeps committing — then show the
reconciliation data and the cutover proof.

Usage:
    python3 demo.py            # run the whole flow and exit
    python3 demo.py --serve    # keep the API up afterwards (reconcile page)
"""
import argparse
import json
import threading
import time
import urllib.error
import urllib.request

from slotmig.api import build_demo_controller, create_server

HOST, PORT = "127.0.0.1", 8765


def call(method, path, payload=None, expect_error=False):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"http://{HOST}:{PORT}{path}", data=data,
                                 method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode()
            ct = resp.headers["Content-Type"]
            return resp.status, (json.loads(body)
                                 if ct.startswith("application/json") else body)
    except urllib.error.HTTPError as exc:
        if expect_error:
            return exc.code, json.loads(exc.read().decode())
        raise


def banner(text):
    print(f"\n{'=' * 66}\n  {text}\n{'=' * 66}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true",
                        help="keep the API server running after the demo")
    args = parser.parse_args()

    ctl, clusters, _store = build_demo_controller(seed=True)
    source = clusters["source"]
    server = create_server(ctl, clusters, HOST, PORT)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"API: http://{HOST}:{PORT}  (source 已预置 200 订单 + 50 客户)")

    banner("1. 创建迁移计划并冻结（源位点 / 目标订阅 / 表集合 / 模式版本）")
    _, plan = call("POST", "/plans", {
        "source_cluster": "source", "source_slot": "app_slot",
        "target_cluster": "target", "target_slot": "app_slot_replica",
        "tables": ["orders", "customers"], "consumer_group": "analytics"})
    pid = plan["plan_id"]
    _, plan = call("POST", f"/plans/{pid}/freeze")
    print(f"plan {pid}  frozen_lsn={plan['frozen_lsn']}  "
          f"schema={plan['schema_versions']}")

    banner("2. 一致快照 + 打开增量流（源端持续写入中）")
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            try:
                call("POST", "/clusters/source/write",
                     {"table": "orders", "key": f"live-{i:04d}",
                      "value": {"amount": i, "live": True}})
                i += 1
            except urllib.error.HTTPError as e:
                if json.loads(e.read())["error"] == "WRITES_FENCED":
                    print(f"  [writer] 源表已围栏，停止写入（共写入 {i} 条）")
                    return
                raise
            time.sleep(0.01)

    def consumer():
        """Old consumer: reads the old slot, commits watermarks (epoch 1)."""
        pos = 0
        while not stop.is_set():
            try:
                _, data = call("GET",
                               f"/clusters/source/slots/app_slot/read?after={pos}")
            except urllib.error.HTTPError:
                return  # slot retired after cutover
            entries = data["entries"]
            if entries:
                pos = entries[-1]["lsn"]
                call("POST", "/groups/analytics/commit",
                     {"cluster": "source", "slot": "app_slot",
                      "lsn": pos, "epoch": 1})
            time.sleep(0.05)

    threads = [threading.Thread(target=fn, daemon=True)
               for fn in (writer, consumer)]
    for t in threads:
        t.start()

    call("POST", f"/plans/{pid}/start")
    print("快照完成，增量流已打开；源端写入线程运行中…")
    time.sleep(0.4)
    _, r = call("POST", f"/plans/{pid}/catch-up")
    print(f"第一次追增量：applied={r['applied']} position={r['stream_position']}")
    time.sleep(0.4)
    call("POST", f"/plans/{pid}/catch-up")

    banner("3. 零停机校验（注入校验标记，比对两侧校验摘要）")
    _, result = call("POST", f"/plans/{pid}/verify")
    print(f"校验通过  marker_lsn={result['marker_lsn']}  "
          f"digest={result['digest'][:32]}…")

    banner("4. 原子切换（围栏→排空→最终校验→单事务切换消费者）")
    time.sleep(0.3)
    _, proof = call("POST", f"/plans/{pid}/cutover")
    h = proof["handoff"]
    print(f"cutover_lsn={proof['final_check']['cutover_lsn']}  "
          f"final_digest={proof['final_check']['digest'][:32]}…")
    print(f"消费者组 {h['group']}: "
          f"{h['old']['cluster']}/{h['old']['slot']}@epoch{h['old']['epoch']}"
          f"(水位 {h['old']['watermark_source_lsn']})  →  "
          f"{h['new']['cluster']}/{h['new']['slot']}@epoch{h['new']['epoch']}"
          f"(水位 {h['new']['watermark_target_lsn']})")
    stop.set()
    for t in threads:
        t.join(timeout=5)

    banner("5. 切换证明（防篡改，可独立验证）")
    print(f"proof_id={proof['proof_id']}")
    print(f"proof_hash={proof['proof_hash']}")
    _, verdict = call("POST", "/proofs/verify", {"proof": proof})
    for c in verdict["checks"]:
        print(f"  [{'✓' if c['ok'] else '✗'}] {c['check']}  {c.get('detail','')}")
    print(f"→ 证明{'有效' if verdict['valid'] else '无效'}")

    banner("6. 迟到确认防护（旧消费者 old epoch 提交被拒绝）")
    status, err = call("POST", "/groups/analytics/commit",
                       {"cluster": "source", "slot": "app_slot",
                        "lsn": 99999, "epoch": 1}, expect_error=True)
    print(f"旧 epoch 提交 → HTTP {status}  {err['error']}: {err['message']}")
    _, group = call("GET", "/groups/analytics")
    print(f"消费者组当前：{group['cluster']}/{group['slot']} "
          f"epoch={group['epoch']} watermark={group['watermark']}（未被扰动）")

    banner("7. 位点对账")
    _, report = call("GET", f"/plans/{pid}/reconcile")
    pos = report["positions"]
    print(f"frozen={pos['frozen_lsn']} snapshot={pos['snapshot_lsn']} "
          f"source={pos['source_current_lsn']} "
          f"applied={pos['target_applied_source_lsn']} "
          f"cutover={pos['cutover_lsn']} lag={pos['lag_events']}")
    for t in report["tables"]:
        print(f"  {t['table']:<10} 源 {t['source_rows']:>4} 行 / "
              f"目标 {t['target_rows']:>4} 行  "
              f"{'✓ 一致' if t['equal_now'] else '✗ 不一致'}")
    print(f"\n对账页面:  http://{HOST}:{PORT}/plans/{pid}/reconcile.html")
    print(f"证明 JSON: http://{HOST}:{PORT}/plans/{pid}/proof")

    banner("8. 观察期撤回（交接成功后仍可回退）")
    _, w = call("GET", f"/plans/{pid}/withdrawal")
    print(f"阶段={w['phase']}  截止={w['deadline']}  剩余={w['remaining_ms']}ms")
    print(f"接管方开服确认游标(目标LSN)={w['takeover_cursor']}  "
          f"原服务游标(源LSN)={w['takeover_source_cursor']}")
    print(f"撤回资格={w['eligible']}  阻止条件={w['blocking_conditions']}")
    _, rev = call("POST", f"/plans/{pid}/withdraw")
    print(f"已撤回  revocation_id={rev['revocation_id']}")
    print(f"revocation_hash={rev['revocation_hash']}")
    r = rev["restoration"]
    print(f"归属恢复：{r['cluster']}/{r['slot']} epoch={r['epoch']} "
          f"水位={r['watermark_source_lsn']}（游标保留，未重置）")
    _, verdict = call("POST", "/revocations/verify", {"revocation": rev})
    print(f"撤回凭证独立验证 → {'有效' if verdict['valid'] else '无效'}")
    for c in verdict["checks"]:
        print(f"  [{'✓' if c['ok'] else '✗'}] {c['check']}  {c.get('detail','')}")

    # same request again → existing result (idempotent)
    _, rev_again = call("POST", f"/plans/{pid}/withdraw")
    print(f"重复撤回请求 → 返回既有凭证 {rev_again['revocation_id'] == rev['revocation_id']}")

    _, group = call("GET", "/groups/analytics")
    print(f"消费者组当前：{group['cluster']}/{group['slot']} "
          f"epoch={group['epoch']} watermark={group['watermark']}")
    _, wrote = call("POST", "/clusters/source/write",
                    {"table": "orders", "key": "post-revoke",
                     "value": {"ok": True}})
    print(f"源围栏已解除、源槽恢复 ACTIVE：新写入被接受 lsn={wrote['lsn']}")
    _, report = call("GET", f"/plans/{pid}/reconcile")
    print(f"对账页撤回区块阶段：{report['withdrawal']['phase']}")

    if args.serve:
        print("\n按 Ctrl+C 停止服务…")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    server.shutdown()
    server.server_close()


if __name__ == "__main__":
    main()
