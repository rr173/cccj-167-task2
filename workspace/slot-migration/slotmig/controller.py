"""Migration controller — the state machine that moves a logical replication
slot (and its consumers) from a source cluster to a target cluster.

State machine:

    DRAFT → FROZEN → STREAMING → VERIFIED → COMPLETED
                  ↘ (any failure) → FAILED ──retry──→ FROZEN (attempt+1)
                  ↘ ABORTED (terminal, operator decision)

Guarantees (enforced here + in store/cluster):

* no lost events — the snapshot covers everything ≤ snapshot_lsn, the stream
  covers everything after it, and the cutover drains up to a fenced LSN;
* no duplicate advancement — target apply is idempotent per (plan, source
  LSN); consumer watermarks only move forward;
* never two valid consumption watermarks — the group switch is one
  transaction guarded by an epoch fence; late acks from old consumers carry
  a stale epoch and are rejected;
* schema drift aborts the plan instead of silently corrupting the stream.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from . import proof as proof_mod
from .cluster import KIND_MARKER, KIND_ROW, KIND_SCHEMA
from .common import (
    BAD_REQUEST,
    CONFLICT,
    DRAIN_INCOMPLETE,
    FINAL_MISMATCH,
    FUTURE_EPOCH,
    INVALID_STATE,
    LOAD_MISMATCH,
    NO_COMPLETED_HANDOFF,
    NOT_FOUND,
    OBSERVATION_EXPIRED,
    RETRY_NOT_ALLOWED,
    SCHEMA_CHANGED,
    SLOT_NOT_ACTIVE,
    SOURCE_PROTECTION_RELEASED,
    STALE_EPOCH,
    TAKEOVER_CONFIRMED,
    VERIFY_MISMATCH,
    MigrationError,
    canonical_json,
    digest_tables,
    new_id,
    now_iso,
    sha256_hex,
    table_summary,
)

NONTERMINAL = ("DRAFT", "FROZEN", "STREAMING", "VERIFIED")

# Default length of the post-cutover observation window in which the
# operator may withdraw the handoff.
DEFAULT_OBSERVATION_WINDOW_MS = 5 * 60 * 1000


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


class MigrationController:
    def __init__(self, clusters: dict, store, *,
                 observation_window_ms: int = DEFAULT_OBSERVATION_WINDOW_MS,
                 clock=None):
        self.clusters = clusters
        self.store = store
        self.observation_window_ms = int(observation_window_ms)
        self._clock = clock or datetime.now
        self._lock = threading.RLock()

    def _now(self) -> datetime:
        return self._clock().astimezone(timezone.utc)

    def _now_iso(self) -> str:
        return self._now().isoformat(timespec="milliseconds")

    def _deadline_iso(self, window_ms: int) -> str:
        return (self._now() + timedelta(milliseconds=window_ms)).isoformat(
            timespec="milliseconds")

    # ------------------------------------------------------------ plan basics
    def create_plan(self, spec: dict) -> dict:
        with self._lock:
            for key in ("source_cluster", "source_slot", "target_cluster",
                        "target_slot", "tables"):
                if not spec.get(key):
                    raise MigrationError(BAD_REQUEST, f"missing field: {key}")
            for c in (spec["source_cluster"], spec["target_cluster"]):
                if c not in self.clusters:
                    raise MigrationError(NOT_FOUND, f"unknown cluster '{c}'",
                                         http_status=404)
            tables = sorted({t for t in spec["tables"] if isinstance(t, str) and t})
            if not tables:
                raise MigrationError(BAD_REQUEST, "tables must be a non-empty list")
            spec = dict(spec, tables=tables,
                        consumer_group=spec.get("consumer_group")
                        or f"grp_{spec['source_slot']}")
            window_ms = spec.get("observation_window_ms", self.observation_window_ms)
            try:
                window_ms = int(window_ms)
            except (TypeError, ValueError):
                raise MigrationError(BAD_REQUEST,
                                     "observation_window_ms must be an integer")
            if window_ms < 0:
                raise MigrationError(BAD_REQUEST,
                                     "observation_window_ms must be >= 0")
            plan = self.store.create_plan(spec, observation_window_ms=window_ms)
            self.store.record_event(plan.plan_id, "PLAN_CREATED",
                                    {"spec": plan.to_dict()})
            return plan.to_dict()

    def get_plan(self, plan_id: str) -> dict:
        return self.store.get_plan(plan_id).to_dict()

    def list_plans(self) -> list:
        return [p.to_dict() for p in self.store.list_plans()]

    def freeze(self, plan_id: str) -> dict:
        """Pin the plan: source position, schema versions, target subscription,
        consumer group. Immutable afterwards."""
        with self._lock:
            plan = self.store.get_plan(plan_id)
            self._require(plan, ("DRAFT",))
            source = self.clusters[plan.source_cluster]
            target = self.clusters[plan.target_cluster]
            slot = source.get_slot(plan.source_slot)  # raises if missing
            if slot["status"] != "ACTIVE":
                raise MigrationError(
                    CONFLICT, f"source slot '{plan.source_slot}' is {slot['status']}",
                    http_status=409)
            if plan.target_slot not in {s["name"] for s in target.list_slots()}:
                target.create_slot(plan.target_slot)  # provision the subscription
            group = self.store.ensure_group(plan.consumer_group,
                                            plan.source_cluster, plan.source_slot)
            if (group["cluster"], group["slot"]) != (plan.source_cluster,
                                                     plan.source_slot):
                raise MigrationError(
                    CONFLICT,
                    f"consumer group '{plan.consumer_group}' is bound to "
                    f"{group['cluster']}/{group['slot']}, not the plan's source slot",
                    http_status=409)
            frozen_lsn = source.current_lsn()
            schema_versions = source.schema_versions(plan.tables)
            plan = self.store.transition(
                plan_id, ("DRAFT",), "FROZEN",
                frozen_lsn=frozen_lsn,
                schema_versions_json=canonical_json(schema_versions))
            self.store.record_event(plan_id, "PLAN_FROZEN", {
                "frozen_lsn": frozen_lsn, "schema_versions": schema_versions,
                "tables": plan.tables})
            return plan.to_dict()

    # --------------------------------------------------------------- snapshot
    def start(self, plan_id: str) -> dict:
        """Take the consistent snapshot and open the incremental stream."""
        with self._lock:
            plan = self.store.get_plan(plan_id)
            self._require(plan, ("FROZEN",))
            self._check_schema(plan)
            source = self.clusters[plan.source_cluster]
            target = self.clusters[plan.target_cluster]

            rows, snap_lsn = source.snapshot_rows(plan.tables)
            if snap_lsn < plan.frozen_lsn:
                raise MigrationError(INVALID_STATE, "snapshot LSN moved backwards?!")
            snapshot_id = new_id("snap")
            manifest = {t: table_summary(r) for t, r in rows.items()}

            # Retry attempts truncate the target tables first; the first
            # attempt refuses to touch a non-empty target.
            target.bulk_load(plan.plan_id, rows, snap_lsn, truncate=plan.attempt > 1)
            loaded = target.running_checksum(plan.tables)
            if digest_tables(loaded) != digest_tables(manifest):
                raise MigrationError(LOAD_MISMATCH,
                                     "target state after snapshot load differs "
                                     "from the snapshot manifest")

            self.store.save_snapshot(snapshot_id, plan_id, plan.attempt,
                                     snap_lsn, manifest)
            self.store.set_stream_position(plan_id, plan.attempt, snap_lsn)
            plan = self.store.transition(
                plan_id, ("FROZEN",), "STREAMING",
                snapshot_id=snapshot_id, snapshot_lsn=snap_lsn)
            self.store.record_event(plan_id, "SNAPSHOT_TAKEN", {
                "snapshot_id": snapshot_id, "snapshot_lsn": snap_lsn,
                "manifest": manifest, "attempt": plan.attempt})
            applied = self._stream(plan)  # initial catch-up
            self.store.record_event(plan_id, "STREAM_OPENED",
                                    {"initial_applied": applied})
            return plan.to_dict()

    # -------------------------------------------------------------- streaming
    def catch_up(self, plan_id: str) -> dict:
        with self._lock:
            plan = self.store.get_plan(plan_id)
            self._require(plan, ("STREAMING", "VERIFIED"))
            self._check_schema(plan)
            applied = self._stream(plan)
            pos = self.store.get_stream_position(plan_id, plan.attempt)
            self.store.record_event(plan_id, "CATCH_UP",
                                    {"applied": applied, "position": pos})
            return {"plan_id": plan_id, "applied": applied, "stream_position": pos}

    def _stream(self, plan, through: int | None = None) -> int:
        """Apply pending source entries to the target. Idempotent; safe to
        call any number of times (this is what makes retries and the final
        cutover drain cheap)."""
        source = self.clusters[plan.source_cluster]
        target = self.clusters[plan.target_cluster]
        pos = self.store.get_stream_position(plan.plan_id, plan.attempt)
        if pos is None:
            pos = plan.snapshot_lsn or plan.frozen_lsn or 0
        applied, pairs = 0, []
        for entry in source.scan(pos):
            if through is not None and entry.lsn > through:
                break
            if entry.kind == KIND_SCHEMA and entry.table in plan.tables:
                self._fail(plan, SCHEMA_CHANGED,
                           f"schema change on '{entry.table}' at source LSN "
                           f"{entry.lsn} (plan froze schema "
                           f"{plan.schema_versions}); create a new plan")
            elif entry.kind == KIND_ROW and entry.table in plan.tables:
                target_lsn, _ = target.apply_replication_event(plan.plan_id, entry)
                if target_lsn is not None:
                    pairs.append((entry.lsn, target_lsn))
                    applied += 1
            elif entry.kind == KIND_MARKER:
                payload = entry.payload or {}
                if (payload.get("plan_id") == plan.plan_id
                        and payload.get("attempt") == plan.attempt):
                    _, confirmation = target.apply_replication_event(
                        plan.plan_id, entry)
                    if confirmation is not None:
                        self.store.confirm_marker(plan.plan_id, plan.attempt,
                                                  entry.lsn, confirmation)
            pos = entry.lsn
        self.store.put_lsn_map(plan.plan_id, plan.attempt, pairs)
        self.store.set_stream_position(plan.plan_id, plan.attempt, pos)
        target.advance_applied(plan.plan_id, pos)
        return applied

    # ----------------------------------------------------------- verification
    def verify(self, plan_id: str) -> dict:
        """Zero-downtime verification: drain, inject a marker into the source
        log, wait until the target applies it, then compare the checksum
        snapshots both sides took atomically at the marker."""
        with self._lock:
            plan = self.store.get_plan(plan_id)
            self._require(plan, ("STREAMING", "VERIFIED"))
            self._check_schema(plan)
            source = self.clusters[plan.source_cluster]

            self._stream(plan)  # drain whatever is pending
            payload = {"plan_id": plan.plan_id, "attempt": plan.attempt,
                       "kind": "verify", "tables": plan.tables}
            marker_lsn, source_sums = source.emit_marker(payload, plan.tables)
            self.store.save_marker(plan_id, plan.attempt, marker_lsn, source_sums)
            self._stream(plan)  # the marker flows to the target

            marker = next(m for m in self.store.get_markers(plan_id, plan.attempt)
                          if m["marker_lsn"] == marker_lsn)
            if marker["status"] != "CONFIRMED":
                self._fail(plan, "VERIFY_INCOMPLETE",
                           "target did not confirm the verification marker")
            src_digest = digest_tables(marker["source_checksums"])
            tgt_digest = digest_tables(marker["target_checksums"])
            if src_digest != tgt_digest:
                diff = {t: {"source": marker["source_checksums"].get(t),
                            "target": marker["target_checksums"].get(t)}
                        for t in plan.tables
                        if marker["source_checksums"].get(t)
                        != marker["target_checksums"].get(t)}
                self._fail(plan, VERIFY_MISMATCH,
                           "checksum digest mismatch at verification marker",
                           {"marker_lsn": marker_lsn, "diff": diff})
            verified_at = now_iso()
            plan = self.store.transition(
                plan_id, ("STREAMING", "VERIFIED"), "VERIFIED",
                verify_digest=src_digest, verify_marker_lsn=marker_lsn)
            self.store.record_event(plan_id, "VERIFIED", {
                "marker_lsn": marker_lsn, "digest": src_digest})
            return {"plan_id": plan_id, "state": plan.state,
                    "digest": src_digest, "marker_lsn": marker_lsn,
                    "verified_at": verified_at}

    # ---------------------------------------------------------------- cutover
    def cutover(self, plan_id: str) -> dict:
        """Atomically switch the consumer group to the new slot.

        Fence source writes → drain to the fenced LSN → final full checksum →
        one transaction that bumps the consumer epoch, repoints the group and
        persists the proof. The source fence stays ON after cutover (no
        split-brain writes) until `release_source` is called by the operator.
        """
        with self._lock:
            plan = self.store.get_plan(plan_id)
            self._require(plan, ("VERIFIED",))
            self._check_schema(plan)
            source = self.clusters[plan.source_cluster]
            target = self.clusters[plan.target_cluster]
            token = f"plan:{plan.plan_id}:{plan.attempt}"

            source.fence_tables(plan.tables, token)
            switched = False
            try:
                cutover_lsn = source.current_lsn()
                self._stream(plan, through=cutover_lsn)  # final drain
                applied = target.applied_source_lsn(plan.plan_id)
                if applied is None or applied < cutover_lsn:
                    raise MigrationError(
                        DRAIN_INCOMPLETE,
                        f"target applied through {applied}, need {cutover_lsn}")
                final_src = source.recomputed_checksum(plan.tables)
                final_tgt = target.recomputed_checksum(plan.tables)
                if digest_tables(final_src) != digest_tables(final_tgt):
                    self._fail(plan, FINAL_MISMATCH,
                               "final checksum mismatch under the cutover fence",
                               {"source": final_src, "target": final_tgt})
                final_digest = digest_tables(final_src)

                group = self.store.get_group(plan.consumer_group)
                snapshot = (self.store.get_snapshot(plan.snapshot_id)
                            if plan.snapshot_id else None)
                manifest_digest = (digest_tables(snapshot["manifest"])
                                   if snapshot else None)
                events = self.store.get_events(plan_id)
                timeline_digest = sha256_hex(canonical_json(
                    [(e["id"], e["ts"], e["kind"]) for e in events]))
                verified_at = next((e["ts"] for e in reversed(events)
                                    if e["kind"] == "VERIFIED"), None)

                def build(handoff):
                    return proof_mod.build_proof(
                        plan=plan, snapshot=snapshot,
                        manifest_digest=manifest_digest,
                        verify_digest=plan.verify_digest,
                        verify_marker_lsn=plan.verify_marker_lsn,
                        verified_at=verified_at,
                        cutover_lsn=cutover_lsn, final_digest=final_digest,
                        handoff=handoff, timeline_digest=timeline_digest)

                proof = self.store.execute_cutover(
                    plan_id=plan.plan_id, attempt=plan.attempt,
                    group_id=group["group_id"], expected_epoch=group["epoch"],
                    new_cluster=plan.target_cluster, new_slot=plan.target_slot,
                    cutover_lsn=cutover_lsn,
                    observation_window_ms=plan.observation_window_ms
                    if plan.observation_window_ms is not None
                    else self.observation_window_ms,
                    observation_deadline=self._deadline_iso(
                        plan.observation_window_ms
                        if plan.observation_window_ms is not None
                        else self.observation_window_ms),
                    build_proof_fn=build)
                switched = True
            finally:
                if not switched:
                    source.unfence_tables(plan.tables, token)

            # Cluster-level fences (defense in depth behind the epoch check).
            try:
                source.retire_slot(plan.source_slot)
            except MigrationError:
                pass
            target.update_slot_confirmed(
                plan.target_slot,
                proof["handoff"]["new"]["watermark_target_lsn"])
            done = self.store.get_plan(plan_id)
            self.store.record_event(plan_id, "CUTOVER_EXECUTED", {
                "cutover_lsn": cutover_lsn, "final_digest": final_digest,
                "proof_id": proof["proof_id"],
                "handoff": proof["handoff"],
                "observation": {
                    "window_ms": done.observation_window_ms,
                    "deadline": done.observation_deadline,
                    "takeover_source_cursor": done.takeover_source_cursor,
                    "takeover_cursor": done.takeover_cursor,
                }})
            return proof

    # -------------------------------------------------------------- lifecycle
    def retry(self, plan_id: str, auto_start: bool = True) -> dict:
        """Retry a failed plan as a new attempt. The frozen spec is unchanged;
        the snapshot/stream/verification artifacts are rebuilt per attempt.
        Schema changes are NOT retryable — a new plan must be created."""
        with self._lock:
            plan = self.store.get_plan(plan_id)
            self._require(plan, ("FAILED",))
            if plan.error_code == SCHEMA_CHANGED:
                raise MigrationError(
                    RETRY_NOT_ALLOWED,
                    "the schema changed since the plan was frozen; "
                    "create a new plan instead of retrying", http_status=409)
            plan = self.store.transition(
                plan_id, ("FAILED",), "FROZEN",
                attempt=plan.attempt + 1, error_code=None, error_message=None)
            self.store.record_event(plan_id, "RETRY", {"attempt": plan.attempt})
            if auto_start:
                return self.start(plan_id)
            return plan.to_dict()

    def abort(self, plan_id: str) -> dict:
        with self._lock:
            plan = self.store.get_plan(plan_id)
            self._require(plan, NONTERMINAL + ("FAILED",))
            plan = self.store.transition(plan_id, NONTERMINAL + ("FAILED",),
                                         "ABORTED")
            self.store.record_event(plan_id, "ABORTED", {})
            return plan.to_dict()

    def release_source(self, plan_id: str) -> dict:
        """Operator acknowledgment that write traffic has moved to the target;
        lifts the post-cutover write fence on the source tables. Once done the
        handoff can no longer be withdrawn (original service unprotected)."""
        with self._lock:
            plan = self.store.get_plan(plan_id)
            self._require(plan, ("COMPLETED",))
            source = self.clusters[plan.source_cluster]
            source.unfence_tables(plan.tables, f"plan:{plan.plan_id}:{plan.attempt}")
            first = self.store.mark_source_released(plan_id)
            self.store.record_event(plan_id, "SOURCE_RELEASED",
                                    {"first": first})
            return {"plan_id": plan_id, "source_released": True}

    # ------------------------------------------------------------- withdraw
    def withdrawal_status(self, plan_id: str) -> dict:
        """Read-only view backing the reconciliation page: eligibility, the
        observation deadline and every blocking condition. Never mutates."""
        with self._lock:
            plan = self.store.get_plan(plan_id)
            existing = self.store.get_revocation_for_plan(plan_id)
            if existing is not None:
                return {
                    "phase": "WITHDRAWN",
                    "eligible": False,
                    "window_ms": plan.observation_window_ms,
                    "deadline": plan.observation_deadline,
                    "takeover_source_cursor": plan.takeover_source_cursor,
                    "takeover_cursor": plan.takeover_cursor,
                    "blocking_conditions": [],
                    "revocation": {"revocation_id": existing["revocation_id"],
                                   "revocation_hash": existing["revocation_hash"],
                                   "created_at": existing["created_at"]},
                }
            if plan.state != "COMPLETED":
                return {
                    "phase": "PRE_HANDOFF" if plan.state != "WITHDRAWN" else "WITHDRAWN",
                    "eligible": False,
                    "window_ms": plan.observation_window_ms,
                    "deadline": plan.observation_deadline,
                    "takeover_source_cursor": plan.takeover_source_cursor,
                    "takeover_cursor": plan.takeover_cursor,
                    "blocking_conditions": [
                        {"code": NO_COMPLETED_HANDOFF,
                         "message": f"plan is {plan.state}; observation window opens "
                                    f"after cutover"}],
                    "revocation": None,
                }

            now = self._now()
            deadline = _parse_iso(plan.observation_deadline)
            blocking = []
            expired = now > deadline
            if expired:
                blocking.append({"code": OBSERVATION_EXPIRED,
                                 "message": f"observation window ended at "
                                            f"{plan.observation_deadline}"})

            group = self.store.get_group(plan.consumer_group)
            q = plan.takeover_cursor
            advanced = group["watermark"] > q
            if advanced:
                blocking.append({
                    "code": TAKEOVER_CONFIRMED,
                    "message": f"takeover side confirmed new records: group watermark "
                               f"{group['watermark']} past the takeover cursor {q}"})

            source = self.clusters[plan.source_cluster]
            protected = (plan.source_released_at is None
                         and all(source.is_fenced(t) for t in plan.tables))
            if not protected:
                detail = ("operator already called release-source"
                          if plan.source_released_at is not None
                          else "source write fence is no longer held in the cluster")
                blocking.append({"code": SOURCE_PROTECTION_RELEASED,
                                 "message": f"original service is unprotected: {detail}"})

            remaining_ms = max(0, int((deadline - now).total_seconds() * 1000))
            return {
                "phase": "OBSERVATION",
                "eligible": not blocking,
                "window_ms": plan.observation_window_ms,
                "deadline": plan.observation_deadline,
                "now": now.isoformat(timespec="milliseconds"),
                "remaining_ms": remaining_ms,
                "takeover_source_cursor": plan.takeover_source_cursor,
                "takeover_cursor": q,
                "group_watermark": group["watermark"],
                "source_protected": protected,
                "source_released_at": plan.source_released_at,
                "takeover_confirmed_new_records": advanced,
                "expired": expired,
                "blocking_conditions": blocking,
                "revocation": None,
            }

    def withdraw_handoff(self, plan_id: str) -> dict:
        """Operator management call: undo the handoff during the observation
        window.

        Eligible only while the takeover side confirmed NO new record, the
        original service is still protected (source fenced) and the window is
        open. The switch back is ONE transaction (ownership restored, epoch
        bumped, existing cursor retained, verifiable credential persisted).
        A repeated request returns the existing credential; rejection leaves
        all runtime state untouched.
        """
        with self._lock:
            plan = self.store.get_plan(plan_id)

            # Idempotency first: if a credential already exists, the same
            # request simply returns it — never a second withdrawal.
            existing = self.store.get_revocation_for_plan(plan_id)
            if existing is not None:
                return existing["revocation"]

            if plan.state != "COMPLETED":
                raise MigrationError(
                    NO_COMPLETED_HANDOFF,
                    f"plan {plan_id} is {plan.state}; withdrawal is only possible "
                    f"during the post-cutover observation window",
                    {"current": plan.state}, http_status=409)

            status = self.withdrawal_status(plan_id)
            if status["blocking_conditions"]:
                first = status["blocking_conditions"][0]
                raise MigrationError(
                    first["code"],
                    f"handoff withdrawal refused: {first['message']}",
                    {"blocking_conditions": status["blocking_conditions"],
                     "deadline": plan.observation_deadline},
                    http_status=409)

            source = self.clusters[plan.source_cluster]
            target = self.clusters[plan.target_cluster]
            token = f"plan:{plan.plan_id}:{plan.attempt}"
            q = plan.takeover_cursor
            p_old = plan.takeover_source_cursor

            cutover_row = self.store.get_proof_for_plan(plan_id)
            if cutover_row is None:
                raise MigrationError(NOT_FOUND,
                                     "cutover proof missing; cannot build revocation",
                                     http_status=404)
            cutover_proof = cutover_row["proof"]
            group = self.store.get_group(plan.consumer_group)
            events = self.store.get_events(plan_id)
            timeline_digest = sha256_hex(canonical_json(
                [(e["id"], e["ts"], e["kind"]) for e in events]))
            observed_at = self._now_iso()

            def build(restoration, from_epoch):
                return proof_mod.build_revocation(
                    plan=plan, cutover_proof=cutover_proof,
                    takeover_source_cursor=p_old, takeover_cursor=q,
                    observed_at=observed_at, revoked_at=self._now_iso(),
                    restoration=restoration, from_epoch=from_epoch,
                    timeline_digest=timeline_digest)

            revocation = self.store.execute_revocation(
                plan_id=plan_id, group_id=plan.consumer_group,
                old_cluster=plan.source_cluster, old_slot=plan.source_slot,
                new_cluster=plan.target_cluster, new_slot=plan.target_slot,
                takeover_source_cursor=p_old, takeover_cursor=q,
                observation_deadline=plan.observation_deadline,
                build_revocation_fn=build)

            # Post-commit cluster-level changes (defense in depth behind the
            # epoch fence): resume the original slot, lift the source fence so
            # writes flow again, retire the takeover slot.
            source.activate_slot(plan.source_slot)
            source.unfence_tables(plan.tables, token)
            try:
                target.retire_slot(plan.target_slot)
            except MigrationError:
                pass

            self.store.record_event(plan_id, "OBSERVATION_WITHDRAWN", {
                "revocation_id": revocation["revocation_id"],
                "restored_watermark": p_old,
                "takeover_cursor_retained": q})
            return revocation

    def get_revocation(self, plan_id: str) -> dict:
        with self._lock:
            row = self.store.get_revocation_for_plan(plan_id)
            if row is None:
                raise MigrationError(NOT_FOUND,
                                     "no handoff revocation for this plan",
                                     http_status=404)
            return row["revocation"]

    def verify_revocation(self, revocation: dict) -> dict:
        return proof_mod.verify_revocation(revocation, self.store)

    # --------------------------------------------------------------- consumers
    def commit(self, group_id: str, cluster: str, slot: str, lsn: int,
               epoch: int) -> dict:
        """Consumer watermark commit. This is where late acks die:

        * stale epoch  → rejected (an old consumer after cutover);
        * wrong slot   → rejected (not the group's active slot);
        * lsn ≤ watermark → idempotent no-op (never a duplicate advancement).
        """
        with self._lock:
            group = self.store.get_group(group_id)
            if epoch < group["epoch"]:
                raise MigrationError(
                    STALE_EPOCH,
                    f"epoch {epoch} is fenced off (current {group['epoch']}); "
                    f"this consumer is working on a retired slot",
                    http_status=409)
            if epoch > group["epoch"]:
                raise MigrationError(FUTURE_EPOCH,
                                     f"epoch {epoch} is ahead of {group['epoch']}",
                                     http_status=409)
            if (cluster, slot) != (group["cluster"], group["slot"]):
                raise MigrationError(
                    SLOT_NOT_ACTIVE,
                    f"slot {cluster}/{slot} is not the group's active slot "
                    f"({group['cluster']}/{group['slot']})", http_status=409)
            if lsn <= group["watermark"]:
                return {"group_id": group_id, "watermark": group["watermark"],
                        "advanced": False, "duplicate": True}
            group = self.store.advance_group(group_id, epoch, lsn)
            self.clusters[cluster].update_slot_confirmed(slot, lsn)
            return {"group_id": group_id, "watermark": group["watermark"],
                    "advanced": True}

    def get_group(self, group_id: str) -> dict:
        return self.store.get_group(group_id)

    def ensure_group(self, group_id: str, cluster: str, slot: str) -> dict:
        with self._lock:
            if cluster not in self.clusters:
                raise MigrationError(NOT_FOUND, f"unknown cluster '{cluster}'",
                                     http_status=404)
            self.clusters[cluster].get_slot(slot)  # must exist
            return self.store.ensure_group(group_id, cluster, slot)

    # ---------------------------------------------------------------- reports
    def reconcile(self, plan_id: str) -> dict:
        from .reconcile import build_report
        with self._lock:
            withdrawal = self.withdrawal_status(plan_id)
            return build_report(self.store, self.clusters, plan_id,
                                withdrawal=withdrawal)

    def get_proof(self, plan_id: str) -> dict:
        with self._lock:
            row = self.store.get_proof_for_plan(plan_id)
            if row is None:
                raise MigrationError(NOT_FOUND, "no cutover proof for this plan",
                                     http_status=404)
            return row["proof"]

    def verify_proof(self, proof: dict) -> dict:
        return proof_mod.verify_proof(proof, self.store)

    # ----------------------------------------------------------------- helpers
    def _get(self, plan_id: str):
        return self.store.get_plan(plan_id)

    @staticmethod
    def _require(plan, states: tuple):
        if plan.state not in states:
            raise MigrationError(
                INVALID_STATE,
                f"plan {plan.plan_id} is {plan.state}; expected one of {list(states)}",
                {"current": plan.state, "expected": list(states)}, http_status=409)

    def _check_schema(self, plan):
        if not plan.schema_versions:
            return
        source = self.clusters[plan.source_cluster]
        current = source.schema_versions(plan.tables)
        if current != plan.schema_versions:
            self._fail(plan, SCHEMA_CHANGED,
                       "schema drift detected on the source",
                       {"frozen": plan.schema_versions, "current": current})

    def _fail(self, plan, code: str, message: str, details=None):
        try:
            self.store.transition(plan.plan_id, NONTERMINAL, "FAILED",
                                  error_code=code, error_message=message)
        except MigrationError:
            pass  # already terminal — keep the original failure
        self.store.record_event(plan.plan_id, "FAILED",
                                {"code": code, "message": message,
                                 "details": details or {}})
        raise MigrationError(code, message, details)
