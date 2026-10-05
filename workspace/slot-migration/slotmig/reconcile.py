"""Position reconciliation report (位点对账) and its HTML rendering."""
from __future__ import annotations

import html

from .common import digest_tables, now_iso


def build_report(store, clusters, plan_id: str) -> dict:
    plan = store.get_plan(plan_id)
    source = clusters[plan.source_cluster]
    target = clusters[plan.target_cluster]

    stream_pos = store.get_stream_position(plan_id, plan.attempt)
    applied = target.applied_source_lsn(plan_id)
    source_lsn = source.current_lsn()
    markers = store.get_markers(plan_id, plan.attempt)
    snapshot = store.get_snapshot(plan.snapshot_id) if plan.snapshot_id else None
    proof_row = store.get_proof_for_plan(plan_id)
    events = store.get_events(plan_id)

    try:
        group = store.get_group(plan.consumer_group)
    except Exception:
        group = None

    tables = []
    for t in plan.tables:
        src_now = source.running_checksum([t])[t]
        tgt_now = target.running_checksum([t])[t]
        tables.append({
            "table": t,
            "source_rows": src_now["rows"],
            "target_rows": tgt_now["rows"],
            "source_checksum_now": src_now["checksum"],
            "target_checksum_now": tgt_now["checksum"],
            "equal_now": src_now == tgt_now,
        })

    verification = {"status": "pending", "digest": plan.verify_digest,
                    "marker_lsn": plan.verify_marker_lsn, "tables": []}
    confirmed = [m for m in markers if m["status"] == "CONFIRMED"]
    if confirmed:
        m = confirmed[-1]
        per_table = []
        ok = True
        for t in plan.tables:
            s, g = m["source_checksums"].get(t), m["target_checksums"].get(t)
            per_table.append({"table": t, "source": s, "target": g, "match": s == g})
            ok = ok and s == g
        verification = {
            "status": "passed" if ok else "failed",
            "marker_lsn": m["marker_lsn"],
            "digest": digest_tables(m["source_checksums"]),
            "confirmed_at": m["confirmed_at"],
            "tables": per_table,
        }
    if plan.state == "FAILED" and plan.error_code:
        verification["status"] = "failed"

    lag = None
    if stream_pos is not None:
        lag = max(0, source_lsn - stream_pos)

    return {
        "generated_at": now_iso(),
        "plan": plan.to_dict(),
        "positions": {
            "frozen_lsn": plan.frozen_lsn,
            "snapshot_lsn": plan.snapshot_lsn,
            "source_current_lsn": source_lsn,
            "stream_position": stream_pos,
            "target_applied_source_lsn": applied,
            "lag_events": lag,
            "verify_marker_lsn": plan.verify_marker_lsn,
            "cutover_lsn": plan.cutover_lsn,
        },
        "tables": tables,
        "verification": verification,
        "snapshot": snapshot,
        "consumer_group": group,
        "slots": {
            "source": _slot_or_none(source, plan.source_slot),
            "target": _slot_or_none(target, plan.target_slot),
        },
        "proof": ({"proof_id": proof_row["proof_id"],
                   "proof_hash": proof_row["proof_hash"]}
                  if proof_row else None),
        "timeline": events,
    }


def _slot_or_none(cluster, name):
    try:
        return cluster.get_slot(name)
    except Exception:
        return None


# --------------------------------------------------------------------- HTML
_CSS = """
body{font-family:-apple-system,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
margin:0;background:#f4f6f8;color:#1c2733}
header{background:#16222e;color:#fff;padding:18px 28px}
header h1{margin:0;font-size:20px}
header .sub{opacity:.75;font-size:13px;margin-top:4px}
main{padding:20px 28px;max-width:1180px}
section{background:#fff;border:1px solid #dde4ea;border-radius:8px;
padding:16px 20px;margin-bottom:18px}
h2{margin:0 0 12px;font-size:15px;color:#33475b}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{text-align:left;padding:6px 10px;border-bottom:1px solid #edf1f4}
th{color:#6b7c8d;font-weight:600;white-space:nowrap}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
code{background:#eef2f5;border-radius:4px;padding:1px 5px;font-size:12px}
.pill{display:inline-block;border-radius:12px;padding:2px 12px;font-size:12px;
font-weight:600;color:#fff;vertical-align:middle}
.st-DRAFT{background:#90a4ae}.st-FROZEN{background:#5c7cfa}
.st-STREAMING{background:#22b8cf}.st-VERIFIED{background:#2f9e44}
.st-COMPLETED{background:#087f5b}.st-FAILED{background:#e03131}
.st-ABORTED{background:#868e96}
.ok{color:#2f9e44;font-weight:600}.bad{color:#e03131;font-weight:600}
.muted{color:#8a97a3}.mono{font-family:ui-monospace,Menlo,Consolas,monospace}
.tl{font-size:12.5px;max-height:280px;overflow:auto}
.tl li{margin:3px 0}
a{color:#1971c2}
"""

_POSITION_LABELS = [
    ("frozen_lsn", "计划冻结位点", "计划冻结时源集群的位点（基线）"),
    ("snapshot_lsn", "一致快照位点", "快照覆盖 ≤ 此位点的全部数据"),
    ("source_current_lsn", "源当前位点", "源集群实时写入位点"),
    ("stream_position", "已扫描位点", "增量流已消费到的源位点"),
    ("target_applied_source_lsn", "目标已应用位点", "目标端确认应用的源位点"),
    ("lag_events", "落后事件数", "源位点 − 已扫描位点"),
    ("verify_marker_lsn", "校验标记位点", "零停机校验标记所在位点"),
    ("cutover_lsn", "切换位点", "切换前排空到的最终位点"),
]


def _esc(v):
    return html.escape("—" if v is None else str(v))


def render_html(report: dict) -> str:
    plan = report["plan"]
    pos = report["positions"]
    ver = report["verification"]
    group = report["consumer_group"]
    proof = report["proof"]
    pid = plan["plan_id"]

    pos_rows = "".join(
        f"<tr><td>{label}</td><td class='num mono'>{_esc(pos.get(key))}</td>"
        f"<td class='muted'>{html.escape(desc)}</td></tr>"
        for key, label, desc in _POSITION_LABELS)

    table_rows = "".join(
        "<tr><td><code>{t}</code></td><td class='num'>{sr}</td>"
        "<td class='num'>{tr}</td>"
        "<td class='mono'>{sc}</td><td class='mono'>{tc}</td>"
        "<td class='{cls}'>{mark}</td></tr>".format(
            t=html.escape(r["table"]), sr=r["source_rows"], tr=r["target_rows"],
            sc=r["source_checksum_now"][:16] + "…",
            tc=r["target_checksum_now"][:16] + "…",
            cls="ok" if r["equal_now"] else "bad",
            mark="一致" if r["equal_now"] else "不一致")
        for r in report["tables"])

    if ver["status"] == "pending":
        ver_html = "<p class='muted'>尚未执行校验（verify）。</p>"
    else:
        rows = "".join(
            "<tr><td><code>{}</code></td><td class='mono'>{}</td>"
            "<td class='mono'>{}</td><td class='{}'>{}</td></tr>".format(
                html.escape(r["table"]),
                (r["source"] or {}).get("checksum", "—")[:16] + "…",
                (r["target"] or {}).get("checksum", "—")[:16] + "…",
                "ok" if r["match"] else "bad",
                "✓ 一致" if r["match"] else "✗ 不一致")
            for r in ver.get("tables", []))
        badge = ("<span class='ok'>通过</span>" if ver["status"] == "passed"
                 else "<span class='bad'>未通过</span>")
        ver_html = (
            f"<p>状态：{badge}　标记位点：<code>{_esc(ver.get('marker_lsn'))}</code>"
            f"　校验摘要：<code class='mono'>{html.escape((ver.get('digest') or '—')[:24])}…</code></p>"
            f"<table><tr><th>表</th><th>源校验和@标记</th>"
            f"<th>目标校验和@标记</th><th>结论</th></tr>{rows}</table>")

    if group:
        slots = report["slots"]
        group_html = (
            f"<table><tr><th>消费者组</th><th>当前活跃槽</th><th>Epoch</th>"
            f"<th>消费水位</th><th>源槽状态</th><th>目标槽状态</th></tr>"
            f"<tr><td><code>{html.escape(group['group_id'])}</code></td>"
            f"<td><code>{html.escape(group['cluster'])}/{html.escape(group['slot'])}</code></td>"
            f"<td class='num'>{group['epoch']}</td>"
            f"<td class='num mono'>{group['watermark']}</td>"
            f"<td>{_esc((slots['source'] or {}).get('status'))}</td>"
            f"<td>{_esc((slots['target'] or {}).get('status'))}</td></tr></table>"
            f"<p class='muted'>旧 epoch 的迟到确认会被围栏拒绝；"
            f"水位只在当前 epoch 的活跃槽上单调推进。</p>")
    else:
        group_html = "<p class='muted'>消费者组尚未创建（freeze 后可见）。</p>"

    if proof:
        proof_html = (
            f"<p>证明 ID：<code>{html.escape(proof['proof_id'])}</code><br>"
            f"证明哈希：<code class='mono'>{html.escape(proof['proof_hash'])}</code></p>"
            f"<p><a href='/plans/{pid}/proof'>查看切换证明 JSON</a></p>")
    else:
        proof_html = "<p class='muted'>切换完成后生成。</p>"

    timeline = "".join(
        "<li><span class='muted'>{}</span> <b>{}</b> <span class='muted'>{}</span></li>".format(
            html.escape(e["ts"]), html.escape(e["kind"]),
            html.escape(str(e["detail"])[:220]))
        for e in reversed(report["timeline"][-60:]))

    err = ""
    if plan.get("error"):
        err = (f"<section><h2>错误</h2><p class='bad'>"
               f"{html.escape(plan['error']['code'])}："
               f"{html.escape(plan['error']['message'] or '')}</p></section>")

    return f"""<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>位点对账 · {html.escape(pid)}</title><style>{_CSS}</style></head>
<body><header>
<h1>位点对账 <span class="pill st-{plan['state']}">{plan['state']}</span></h1>
<div class="sub">计划 <code>{html.escape(pid)}</code> · 第 {plan['attempt']} 次尝试 ·
生成于 {html.escape(report['generated_at'])}（每 5 秒自动刷新）·
<a style="color:#9ecbff" href="/plans/{pid}/reconcile">JSON</a></div>
</header><main>
<section><h2>迁移计划（冻结规格）</h2>
<table>
<tr><th>源</th><td><code>{html.escape(plan['source_cluster'])}/{html.escape(plan['source_slot'])}</code></td>
<th>目标订阅</th><td><code>{html.escape(plan['target_cluster'])}/{html.escape(plan['target_slot'])}</code></td></tr>
<tr><th>表集合</th><td colspan="3"><code>{html.escape(', '.join(plan['tables']))}</code></td></tr>
<tr><th>模式版本</th><td colspan="3"><code class="mono">{html.escape(str(plan['schema_versions']))}</code></td></tr>
</table></section>
{err}
<section><h2>位点对账</h2>
<table><tr><th>指标</th><th class="num">位点</th><th>说明</th></tr>{pos_rows}</table></section>
<section><h2>表数据校验（即时，仅供参考）</h2>
<table><tr><th>表</th><th class="num">源行数</th><th class="num">目标行数</th>
<th>源校验和</th><th>目标校验和</th><th>即时比对</th></tr>{table_rows}</table>
<p class="muted">即时比对在增量传输期间可能短暂不一致；权威结论以「校验标记」与切换前最终校验为准。</p></section>
<section><h2>校验标记（零停机校验）</h2>{ver_html}</section>
<section><h2>消费者水位</h2>{group_html}</section>
<section><h2>切换证明</h2>{proof_html}</section>
<section><h2>事件时间线</h2><ul class="tl">{timeline}</ul></section>
</main></body></html>"""
