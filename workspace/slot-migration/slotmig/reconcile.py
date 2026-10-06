"""Position reconciliation report (位点对账) and its HTML rendering."""
from __future__ import annotations

import html

from .common import (
    OBSERVATION_EXPIRED,
    SOURCE_NOT_PROTECTED,
    TAKEOVER_PROGRESSED,
    digest_tables,
    iso_add_seconds,
    now_iso,
    seconds_between,
)


def observation_status(store, clusters, plan, *, now=None) -> dict | None:
    """Revocation eligibility for the post-cutover observation window.

    Returns None before a cutover. For COMPLETED plans it evaluates the three
    revocation conditions (window open, no takeover confirmation, source
    still protected); for REVOKED plans it returns the frozen record.
    """
    if plan.state not in ("COMPLETED", "REVOKED") or not plan.cutover_at:
        return None
    current = now or now_iso()
    deadline = iso_add_seconds(plan.cutover_at, plan.observation_seconds)
    elapsed = max(0.0, seconds_between(current, plan.cutover_at))
    remaining = seconds_between(deadline, current)

    source = clusters[plan.source_cluster]
    target = clusters[plan.target_cluster]
    try:
        group = store.get_group(plan.consumer_group)
    except Exception:
        group = None

    result = {
        "window_seconds": plan.observation_seconds,
        "cutover_at": plan.cutover_at,
        "deadline": deadline,
        "elapsed_seconds": round(elapsed, 3),
        "remaining_seconds": round(remaining, 3),
        "takeover_cursor": plan.takeover_cursor,
        "blocking_conditions": [],
        "rejection_reason": None,
        "revocation_id": plan.revocation_id,
    }

    if plan.state == "REVOKED":
        result["status"] = "revoked"
        result["eligible"] = False
        return result

    result["status"] = "expired" if remaining <= 0 else "open"

    takeover_progressed = False
    current_cursor = None
    if group is not None:
        current_cursor = {
            "cluster": group["cluster"], "slot": group["slot"],
            "epoch": group["epoch"], "group_watermark": group["watermark"],
        }
        saved = plan.takeover_cursor or {}
        if (group["watermark"] != saved.get("group_watermark")
                or group["epoch"] != saved.get("epoch")):
            takeover_progressed = True
    else:
        takeover_progressed = True
    try:
        t_slot = target.get_slot(plan.target_slot)
        confirmed_now = t_slot["confirmed_lsn"]
    except Exception:
        confirmed_now = None
        t_slot = None
    saved_confirmed = (plan.takeover_cursor or {}).get("confirmed_lsn")
    if confirmed_now is not None and saved_confirmed is not None \
            and confirmed_now > saved_confirmed:
        takeover_progressed = True
    result["current_cursor"] = current_cursor
    result["target_slot_confirmed_lsn"] = confirmed_now

    token = f"plan:{plan.plan_id}:{plan.attempt}"
    source_protected = source.tables_fenced_by(plan.tables, token)
    result["source_protected"] = source_protected
    result["fence_token"] = token if source_protected else None
    try:
        result["source_slot_status"] = source.get_slot(
            plan.source_slot)["status"]
    except Exception:
        result["source_slot_status"] = None
    result["target_slot_status"] = (t_slot or {}).get("status")

    if takeover_progressed:
        result["blocking_conditions"].append({
            "code": TAKEOVER_PROGRESSED,
            "reason": "接管方已确认新记录（消费者水位或槽确认位点已推进）"})
    if not source_protected:
        result["blocking_conditions"].append({
            "code": SOURCE_NOT_PROTECTED,
            "reason": "原服务已解除保护（源表写围栏已释放）"})
    if remaining <= 0:
        result["blocking_conditions"].append({
            "code": OBSERVATION_EXPIRED,
            "reason": f"观察期已于 {deadline} 结束"})

    result["eligible"] = not result["blocking_conditions"]
    if result["blocking_conditions"]:
        # precedence: takeover progress is the strongest bar
        result["rejection_reason"] = result["blocking_conditions"][0]["code"]
    return result


def build_report(store, clusters, plan_id: str, now=None) -> dict:
    plan = store.get_plan(plan_id)
    source = clusters[plan.source_cluster]
    target = clusters[plan.target_cluster]

    stream_pos = store.get_stream_position(plan_id, plan.attempt)
    applied = target.applied_source_lsn(plan_id)
    source_lsn = source.current_lsn()
    markers = store.get_markers(plan_id, plan.attempt)
    snapshot = store.get_snapshot(plan.snapshot_id) if plan.snapshot_id else None
    proof_row = store.get_proof_for_plan(plan_id)
    revocation_row = store.get_revocation_for_plan(plan_id)
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
        "observation": observation_status(store, clusters, plan, now=now),
        "revocation": ({"revocation_id": revocation_row["proof_id"],
                        "revocation_hash": revocation_row["proof_hash"]}
                       if revocation_row else None),
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
.st-ABORTED{background:#868e96}.st-REVOKED{background:#d9480f}
.ok{color:#2f9e44;font-weight:600}.bad{color:#e03131;font-weight:600}
.muted{color:#8a97a3}.mono{font-family:ui-monospace,Menlo,Consolas,monospace}
.tl{font-size:12.5px;max-height:280px;overflow:auto}
.tl li{margin:3px 0}
.tl .ev-CUTOVER_EXECUTED{color:#087f5b;font-weight:700}
.tl .ev-HANDOVER_REVOKED{color:#d9480f;font-weight:700}
a{color:#1971c2}
.badge-ok{display:inline-block;background:#ebfbee;color:#2f9e44;border:1px solid #b2f2bb;
border-radius:6px;padding:2px 10px;font-size:12.5px;font-weight:600}
.badge-warn{display:inline-block;background:#fff4e6;color:#d9480f;border:1px solid #ffc078;
border-radius:6px;padding:2px 10px;font-size:12.5px;font-weight:600}
.badge-off{display:inline-block;background:#f1f3f5;color:#868e96;border:1px solid #dee2e6;
border-radius:6px;padding:2px 10px;font-size:12.5px;font-weight:600}
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
            f"<p>切换证明 ID：<code>{html.escape(proof['proof_id'])}</code><br>"
            f"证明哈希：<code class='mono'>{html.escape(proof['proof_hash'])}</code></p>"
            f"<p><a href='/plans/{pid}/proof'>查看切换证明 JSON</a></p>")
    else:
        proof_html = "<p class='muted'>切换完成后生成。</p>"

    obs = report.get("observation")
    rev = report.get("revocation")
    if obs is None:
        observation_html = (
            "<p class='muted'>交接完成后进入可配置的观察期，此处在交接后显示。</p>")
    elif obs["status"] == "revoked":
        observation_html = (
            "<p><span class='badge-warn'>交接已撤回</span>　归属已恢复到原服务，"
            "fencing epoch 已再次递增，原游标保留。</p>"
            f"<table><tr><th>观察期时长</th><th>交接时间</th><th>观察截止</th>"
            f"<th>撤回凭证</th></tr><tr>"
            f"<td class='num'>{obs['window_seconds']} 秒</td>"
            f"<td class='mono'>{html.escape(obs['cutover_at'])}</td>"
            f"<td class='mono'>{html.escape(obs['deadline'])}</td>"
            f"<td><code>{html.escape(obs.get('revocation_id') or '—')}</code></td>"
            "</tr></table>"
            "<p class='muted'>重复撤回请求只返回既有撤回凭证，不产生新的状态变化。</p>")
    else:
        if obs["eligible"]:
            badge = "<span class='badge-ok'>可撤回</span>"
            hint = ("观察期内、接管方尚未确认任何新记录、原服务仍处于保护状态；"
                    "操作员可调用 <code>POST /plans/{id}/revoke</code> 撤回交接。")
        else:
            badge = "<span class='badge-warn'>不可撤回</span>"
            hint = "存在阻止条件，撤回请求会被拒绝且所有运行状态保持不变："
        blocks = "".join(
            "<li><b>{}</b>：{}</li>".format(html.escape(b["code"]),
                                           html.escape(b["reason"]))
            for b in obs["blocking_conditions"])
        blocked = {b["code"] for b in obs["blocking_conditions"]}
        progressed = "TAKEOVER_PROGRESSED" in blocked
        protected = obs["source_protected"]
        window_open = obs["status"] == "open"
        cursor = obs.get("takeover_cursor") or {}
        cur_cursor = obs.get("current_cursor") or {}
        observation_html = (
            f"<p>{badge}　{hint}</p>"
            f"<table><tr><th>观察期</th><th>交接时间</th><th>截止时间</th>"
            f"<th>剩余秒数</th><th>已用秒数</th></tr><tr>"
            f"<td class='num'>{obs['window_seconds']} 秒</td>"
            f"<td class='mono'>{html.escape(obs['cutover_at'])}</td>"
            f"<td class='mono'>{html.escape(obs['deadline'])}</td>"
            f"<td class='num'>{obs['remaining_seconds']}</td>"
            f"<td class='num'>{obs['elapsed_seconds']}</td></tr></table>"
            f"<table style='margin-top:8px'><tr><th>条件</th><th>当前值</th>"
            f"<th>结论</th></tr>"
            f"<tr><td>接管方未确认新记录</td>"
            f"<td>接管起始水位 <code>{_esc(cursor.get('group_watermark'))}</code>"
            f" / 当前水位 <code>{_esc(cur_cursor.get('group_watermark'))}</code>"
            f"；接管槽确认位点 <code>{_esc(cursor.get('confirmed_lsn'))}</code>"
            f" → <code>{_esc(obs.get('target_slot_confirmed_lsn'))}</code></td>"
            f"<td class='{'ok' if not progressed else 'bad'}'>"
            f"{'未推进' if not progressed else '已确认新记录'}</td></tr>"
            f"<tr><td>原服务仍处于保护状态</td>"
            f"<td>源槽状态 <code>{_esc(obs.get('source_slot_status'))}</code>"
            f"；围栏 token <code>{html.escape(str(obs.get('fence_token') or '—'))}</code></td>"
            f"<td class='{'ok' if protected else 'bad'}'>"
            f"{'保护中' if protected else '已解除保护'}</td></tr>"
            f"<tr><td>观察期未结束</td>"
            f"<td>状态 <code>{obs['status']}</code></td>"
            f"<td class='{'ok' if window_open else 'bad'}'>"
            f"{'进行中' if window_open else '已结束'}</td></tr>"
            f"</table>"
            + (f"<ul>{blocks}</ul>" if blocks else ""))
        if rev:
            observation_html += (
                f"<p class='muted'>撤回凭证：<code>{html.escape(rev['revocation_id'])}</code>"
                f"（<code class='mono'>{html.escape(rev['revocation_hash'])}</code>）"
                f"　<a href='/plans/{pid}/revocation'>查看撤回凭证 JSON</a></p>")

    timeline = "".join(
        "<li class='ev-{kind}'><span class='muted'>{ts}</span> <b>{kind}</b>"
        " <span class='muted'>{detail}</span></li>".format(
            ts=html.escape(e["ts"]), kind=html.escape(e["kind"]),
            detail=html.escape(str(e["detail"])[:220]))
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
<section><h2>交接观察期与撤回资格</h2>{observation_html}</section>
<section><h2>事件时间线（交接 / 撤回事件）</h2><ul class="tl">{timeline}</ul></section>
</main></body></html>"""
