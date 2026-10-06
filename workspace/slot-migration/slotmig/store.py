"""Durable controller state, backed by SQLite.

The store owns the two invariants that must survive anything:

* plan state transitions are guarded compare-and-swap updates;
* the consumer cutover (epoch bump + active-slot switch + watermark remap +
  proof insert) happens inside ONE transaction, so there is never a window
  with two valid consumption watermarks.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass, field

from .common import (
    CONFLICT,
    DEFAULT_OBSERVATION_SECONDS,
    INVALID_STATE,
    NOT_FOUND,
    TAKEOVER_PROGRESSED,
    MigrationError,
    canonical_json,
    new_id,
    now_iso,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
  plan_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 1,
  source_cluster TEXT NOT NULL,
  source_slot TEXT NOT NULL,
  target_cluster TEXT NOT NULL,
  target_slot TEXT NOT NULL,
  consumer_group TEXT NOT NULL,
  tables_json TEXT NOT NULL,
  schema_versions_json TEXT,
  frozen_lsn INTEGER,
  snapshot_id TEXT,
  snapshot_lsn INTEGER,
  verify_digest TEXT,
  verify_marker_lsn INTEGER,
  cutover_lsn INTEGER,
  proof_id TEXT,
  error_code TEXT,
  error_message TEXT,
  observation_seconds INTEGER NOT NULL DEFAULT 300,
  cutover_at TEXT,
  takeover_cursor_json TEXT,
  revocation_id TEXT,
  version INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  kind TEXT NOT NULL,
  detail_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  snapshot_lsn INTEGER NOT NULL,
  manifest_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_positions (
  plan_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  position INTEGER NOT NULL,
  PRIMARY KEY (plan_id, attempt)
);
CREATE TABLE IF NOT EXISTS lsn_map (
  plan_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  source_lsn INTEGER NOT NULL,
  target_lsn INTEGER NOT NULL,
  PRIMARY KEY (plan_id, attempt, source_lsn)
);
CREATE TABLE IF NOT EXISTS markers (
  plan_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  marker_lsn INTEGER NOT NULL,
  source_checksums_json TEXT NOT NULL,
  target_checksums_json TEXT,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  confirmed_at TEXT,
  PRIMARY KEY (plan_id, attempt, marker_lsn)
);
CREATE TABLE IF NOT EXISTS groups (
  group_id TEXT PRIMARY KEY,
  cluster TEXT NOT NULL,
  slot TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  watermark INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS proofs (
  proof_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'CUTOVER',
  proof_json TEXT NOT NULL,
  proof_hash TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""

# Additive migrations for databases created by older versions.
_PLAN_COLUMN_MIGRATIONS = (
    ("observation_seconds",
     f"ALTER TABLE plans ADD COLUMN observation_seconds INTEGER NOT NULL "
     f"DEFAULT {DEFAULT_OBSERVATION_SECONDS}"),
    ("cutover_at", "ALTER TABLE plans ADD COLUMN cutover_at TEXT"),
    ("takeover_cursor_json",
     "ALTER TABLE plans ADD COLUMN takeover_cursor_json TEXT"),
    ("revocation_id", "ALTER TABLE plans ADD COLUMN revocation_id TEXT"),
)
# Plan fields that transition() is allowed to set (SQL-injection whitelist).
UPDATABLE_FIELDS = {
    "attempt", "schema_versions_json", "frozen_lsn", "snapshot_id",
    "snapshot_lsn", "verify_digest", "verify_marker_lsn", "cutover_lsn",
    "proof_id", "error_code", "error_message",
}

TERMINAL_STATES = ("COMPLETED", "ABORTED", "REVOKED")


@dataclass
class Plan:
    plan_id: str
    state: str
    attempt: int
    source_cluster: str
    source_slot: str
    target_cluster: str
    target_slot: str
    consumer_group: str
    tables: list
    schema_versions: dict | None = None
    frozen_lsn: int | None = None
    snapshot_id: str | None = None
    snapshot_lsn: int | None = None
    verify_digest: str | None = None
    verify_marker_lsn: int | None = None
    cutover_lsn: int | None = None
    proof_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    observation_seconds: int = DEFAULT_OBSERVATION_SECONDS
    cutover_at: str | None = None
    takeover_cursor: dict | None = None
    revocation_id: str | None = None
    version: int = 0
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "plan_id": self.plan_id,
            "state": self.state,
            "attempt": self.attempt,
            "source_cluster": self.source_cluster,
            "source_slot": self.source_slot,
            "target_cluster": self.target_cluster,
            "target_slot": self.target_slot,
            "consumer_group": self.consumer_group,
            "tables": list(self.tables),
            "schema_versions": self.schema_versions,
            "frozen_lsn": self.frozen_lsn,
            "snapshot_id": self.snapshot_id,
            "snapshot_lsn": self.snapshot_lsn,
            "verify_digest": self.verify_digest,
            "verify_marker_lsn": self.verify_marker_lsn,
            "cutover_lsn": self.cutover_lsn,
            "proof_id": self.proof_id,
            "observation_seconds": self.observation_seconds,
            "cutover_at": self.cutover_at,
            "takeover_cursor": self.takeover_cursor,
            "revocation_id": self.revocation_id,
            "error": {"code": self.error_code, "message": self.error_message}
                     if self.error_code else None,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class Store:
    def __init__(self, path: str = ":memory:"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock, self.conn:
            self.conn.executescript(SCHEMA)
            self._migrate()

    def _migrate(self):
        """Apply additive column migrations to pre-existing databases."""
        plan_cols = {r["name"] for r in
                     self.conn.execute("PRAGMA table_info(plans)")}
        for name, ddl in _PLAN_COLUMN_MIGRATIONS:
            if name not in plan_cols:
                self.conn.execute(ddl)
        proof_cols = {r["name"] for r in
                      self.conn.execute("PRAGMA table_info(proofs)")}
        if proof_cols and "kind" not in proof_cols:
            # existing rows are cutover proofs — backfill the discriminator
            self.conn.execute(
                "ALTER TABLE proofs ADD COLUMN kind TEXT NOT NULL "
                "DEFAULT 'CUTOVER'")

    def close(self):
        with self._lock:
            self.conn.close()

    # ------------------------------------------------------------------ plans
    def _row_to_plan(self, row: sqlite3.Row) -> Plan:
        d = dict(row)
        return Plan(
            plan_id=d["plan_id"], state=d["state"], attempt=d["attempt"],
            source_cluster=d["source_cluster"], source_slot=d["source_slot"],
            target_cluster=d["target_cluster"], target_slot=d["target_slot"],
            consumer_group=d["consumer_group"],
            tables=json.loads(d["tables_json"]),
            schema_versions=(json.loads(d["schema_versions_json"])
                             if d["schema_versions_json"] else None),
            frozen_lsn=d["frozen_lsn"], snapshot_id=d["snapshot_id"],
            snapshot_lsn=d["snapshot_lsn"], verify_digest=d["verify_digest"],
            verify_marker_lsn=d["verify_marker_lsn"], cutover_lsn=d["cutover_lsn"],
            proof_id=d["proof_id"], error_code=d["error_code"],
            error_message=d["error_message"],
            observation_seconds=d["observation_seconds"],
            cutover_at=d["cutover_at"],
            takeover_cursor=(json.loads(d["takeover_cursor_json"])
                             if d["takeover_cursor_json"] else None),
            revocation_id=d["revocation_id"],
            version=d["version"],
            created_at=d["created_at"], updated_at=d["updated_at"],
        )

    def create_plan(self, spec: dict) -> Plan:
        plan_id = new_id("plan")
        now = now_iso()
        observation_seconds = int(spec.get("observation_seconds",
                                           DEFAULT_OBSERVATION_SECONDS))
        with self._lock, self.conn:
            clash = self.conn.execute(
                "SELECT plan_id FROM plans WHERE state NOT IN ('COMPLETED','ABORTED','REVOKED') AND ("
                " (source_cluster=? AND source_slot=?) OR"
                " (target_cluster=? AND target_slot=?) OR"
                " consumer_group=? )",
                (spec["source_cluster"], spec["source_slot"],
                 spec["target_cluster"], spec["target_slot"],
                 spec["consumer_group"])).fetchone()
            if clash:
                raise MigrationError(
                    CONFLICT,
                    f"an active plan ({clash['plan_id']}) already covers this slot, "
                    f"target subscription or consumer group",
                    http_status=409)
            self.conn.execute(
                "INSERT INTO plans (plan_id, state, attempt, source_cluster, source_slot,"
                " target_cluster, target_slot, consumer_group, tables_json,"
                " observation_seconds, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (plan_id, "DRAFT", 1, spec["source_cluster"], spec["source_slot"],
                 spec["target_cluster"], spec["target_slot"], spec["consumer_group"],
                 json.dumps(spec["tables"]), observation_seconds, now, now))
        return self.get_plan(plan_id)

    def get_plan(self, plan_id: str) -> Plan:
        with self._lock:
            row = self.conn.execute("SELECT * FROM plans WHERE plan_id=?",
                                    (plan_id,)).fetchone()
        if row is None:
            raise MigrationError(NOT_FOUND, f"plan '{plan_id}' not found", http_status=404)
        return self._row_to_plan(row)

    def list_plans(self) -> list:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM plans ORDER BY created_at DESC").fetchall()
        return [self._row_to_plan(r) for r in rows]

    def transition(self, plan_id: str, expect: tuple, new_state: str, **fields) -> Plan:
        """Guarded state transition (compare-and-swap on current state)."""
        unknown = set(fields) - UPDATABLE_FIELDS
        if unknown:
            raise ValueError(f"unknown plan fields: {unknown}")
        sets, params = "state=?, version=version+1, updated_at=?", [new_state, now_iso()]
        for k, v in fields.items():
            sets += f", {k}=?"
            params.append(v)
        placeholders = ",".join("?" for _ in expect)
        with self._lock, self.conn:
            cur = self.conn.execute(
                f"UPDATE plans SET {sets} WHERE plan_id=? AND state IN ({placeholders})",
                (*params, plan_id, *expect))
            if cur.rowcount != 1:
                current = self.get_plan(plan_id)
                raise MigrationError(
                    INVALID_STATE,
                    f"plan {plan_id} is {current.state}; expected one of {expect}",
                    {"current": current.state, "expected": list(expect)},
                    http_status=409)
        return self.get_plan(plan_id)

    # ----------------------------------------------------------------- events
    def record_event(self, plan_id: str, kind: str, detail: dict):
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT INTO events (plan_id, ts, kind, detail_json) VALUES (?,?,?,?)",
                (plan_id, now_iso(), kind, canonical_json(detail)))

    def get_events(self, plan_id: str) -> list:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM events WHERE plan_id=? ORDER BY id", (plan_id,)).fetchall()
        return [{"id": r["id"], "ts": r["ts"], "kind": r["kind"],
                 "detail": json.loads(r["detail_json"])} for r in rows]

    # --------------------------------------------------------------- snapshot
    def save_snapshot(self, snapshot_id: str, plan_id: str, attempt: int,
                      snapshot_lsn: int, manifest: dict):
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT INTO snapshots VALUES (?,?,?,?,?,?)",
                (snapshot_id, plan_id, attempt, snapshot_lsn,
                 canonical_json(manifest), now_iso()))

    def get_snapshot(self, snapshot_id: str) -> dict | None:
        with self._lock:
            r = self.conn.execute("SELECT * FROM snapshots WHERE snapshot_id=?",
                                  (snapshot_id,)).fetchone()
        if r is None:
            return None
        return {"snapshot_id": r["snapshot_id"], "plan_id": r["plan_id"],
                "attempt": r["attempt"], "snapshot_lsn": r["snapshot_lsn"],
                "manifest": json.loads(r["manifest_json"]),
                "created_at": r["created_at"]}

    # ------------------------------------------------------------ stream state
    def set_stream_position(self, plan_id: str, attempt: int, position: int):
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT INTO stream_positions VALUES (?,?,?) "
                "ON CONFLICT(plan_id, attempt) DO UPDATE SET position=excluded.position",
                (plan_id, attempt, position))

    def get_stream_position(self, plan_id: str, attempt: int):
        with self._lock:
            r = self.conn.execute(
                "SELECT position FROM stream_positions WHERE plan_id=? AND attempt=?",
                (plan_id, attempt)).fetchone()
        return r["position"] if r else None

    def put_lsn_map(self, plan_id: str, attempt: int, pairs: list):
        if not pairs:
            return
        with self._lock, self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO lsn_map VALUES (?,?,?,?)",
                [(plan_id, attempt, s, t) for s, t in pairs])

    def floor_map(self, plan_id: str, attempt: int, source_lsn: int) -> int:
        """Greatest mapped target LSN whose source LSN <= source_lsn (0 if none)."""
        with self._lock:
            r = self.conn.execute(
                "SELECT target_lsn FROM lsn_map WHERE plan_id=? AND attempt=?"
                " AND source_lsn<=? ORDER BY source_lsn DESC LIMIT 1",
                (plan_id, attempt, source_lsn)).fetchone()
        return r["target_lsn"] if r else 0

    def map_size(self, plan_id: str, attempt: int) -> int:
        with self._lock:
            r = self.conn.execute(
                "SELECT COUNT(*) c FROM lsn_map WHERE plan_id=? AND attempt=?",
                (plan_id, attempt)).fetchone()
        return r["c"]

    # ----------------------------------------------------------------- markers
    def save_marker(self, plan_id: str, attempt: int, marker_lsn: int,
                    source_checksums: dict):
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT INTO markers VALUES (?,?,?,?,NULL,'PENDING',?,NULL)",
                (plan_id, attempt, marker_lsn, canonical_json(source_checksums),
                 now_iso()))

    def confirm_marker(self, plan_id: str, attempt: int, marker_lsn: int,
                       target_checksums: dict):
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE markers SET target_checksums_json=?, status='CONFIRMED',"
                " confirmed_at=? WHERE plan_id=? AND attempt=? AND marker_lsn=?",
                (canonical_json(target_checksums), now_iso(), plan_id, attempt,
                 marker_lsn))

    def get_markers(self, plan_id: str, attempt: int) -> list:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM markers WHERE plan_id=? AND attempt=? ORDER BY marker_lsn",
                (plan_id, attempt)).fetchall()
        return [{
            "marker_lsn": r["marker_lsn"],
            "source_checksums": json.loads(r["source_checksums_json"]),
            "target_checksums": (json.loads(r["target_checksums_json"])
                                 if r["target_checksums_json"] else None),
            "status": r["status"],
            "created_at": r["created_at"],
            "confirmed_at": r["confirmed_at"],
        } for r in rows]

    # ------------------------------------------------------------------ groups
    def ensure_group(self, group_id: str, cluster: str, slot: str) -> dict:
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO groups VALUES (?,?,?,?,?,?)",
                (group_id, cluster, slot, 1, 0, now_iso()))
        return self.get_group(group_id)

    def get_group(self, group_id: str) -> dict:
        with self._lock:
            r = self.conn.execute("SELECT * FROM groups WHERE group_id=?",
                                  (group_id,)).fetchone()
        if r is None:
            raise MigrationError(NOT_FOUND, f"consumer group '{group_id}' not found",
                                 http_status=404)
        return {"group_id": r["group_id"], "cluster": r["cluster"], "slot": r["slot"],
                "epoch": r["epoch"], "watermark": r["watermark"],
                "updated_at": r["updated_at"]}

    def advance_group(self, group_id: str, epoch: int, lsn: int) -> dict:
        with self._lock, self.conn:
            cur = self.conn.execute(
                "UPDATE groups SET watermark=?, updated_at=?"
                " WHERE group_id=? AND epoch=? AND watermark<?",
                (lsn, now_iso(), group_id, epoch, lsn))
            if cur.rowcount != 1:
                raise MigrationError(CONFLICT, "consumer watermark changed concurrently",
                                     http_status=409)
        return self.get_group(group_id)

    # ----------------------------------------------------------------- cutover
    def execute_cutover(self, *, plan_id: str, attempt: int, group_id: str,
                        expected_epoch: int, new_cluster: str, new_slot: str,
                        cutover_lsn: int, target_confirmed_lsn: int,
                        at_iso: str, build_proof_fn) -> dict:
        """The atomic switch. In ONE transaction:

        1. re-read the group (epoch fence check);
        2. map the old watermark (source LSN) onto the target LSN space;
        3. build + persist the cutover proof;
        4. bump the epoch, repoint the group at the new slot, set the
           remapped watermark;
        5. mark the plan COMPLETED, recording the cutover instant (which opens
           the observation window) and the takeover cursor — the new side's
           confirmed cursor at the moment service starts, the baseline against
           which later revocation eligibility is decided.

        Any failure rolls everything back — the group keeps consuming the old
        slot and the plan stays VERIFIED (the cutover is retryable).
        """
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                g = cur.execute("SELECT * FROM groups WHERE group_id=?",
                                (group_id,)).fetchone()
                if g is None:
                    raise MigrationError(NOT_FOUND, f"consumer group '{group_id}' not found",
                                         http_status=404)
                if g["epoch"] != expected_epoch:
                    raise MigrationError(CONFLICT, "consumer group epoch changed concurrently",
                                         http_status=409)
                p_old = g["watermark"]
                row = cur.execute(
                    "SELECT target_lsn FROM lsn_map WHERE plan_id=? AND attempt=?"
                    " AND source_lsn<=? ORDER BY source_lsn DESC LIMIT 1",
                    (plan_id, attempt, p_old)).fetchone()
                q = row["target_lsn"] if row else 0
                handoff = {
                    "group": group_id,
                    "old": {"cluster": g["cluster"], "slot": g["slot"],
                            "epoch": g["epoch"], "watermark_source_lsn": p_old},
                    "new": {"cluster": new_cluster, "slot": new_slot,
                            "epoch": g["epoch"] + 1, "watermark_target_lsn": q},
                    "mapping_anchor": {"source_lsn": p_old, "target_lsn": q},
                }
                proof = build_proof_fn(handoff)
                takeover_cursor = canonical_json({
                    "cluster": new_cluster, "slot": new_slot,
                    "epoch": g["epoch"] + 1, "confirmed_lsn": target_confirmed_lsn,
                    "group_watermark": q,
                    "saved_at": at_iso,
                })
                r = cur.execute(
                    "UPDATE groups SET cluster=?, slot=?, epoch=epoch+1, watermark=?,"
                    " updated_at=? WHERE group_id=? AND epoch=?",
                    (new_cluster, new_slot, q, now_iso(), group_id, expected_epoch))
                if r.rowcount != 1:
                    raise MigrationError(CONFLICT, "consumer group switched concurrently",
                                         http_status=409)
                r = cur.execute(
                    "UPDATE plans SET state='COMPLETED', cutover_lsn=?, proof_id=?,"
                    " cutover_at=?, takeover_cursor_json=?,"
                    " version=version+1, updated_at=?"
                    " WHERE plan_id=? AND state='VERIFIED'",
                    (cutover_lsn, proof["proof_id"], at_iso, takeover_cursor,
                     now_iso(), plan_id))
                if r.rowcount != 1:
                    raise MigrationError(CONFLICT, "plan state changed during cutover",
                                         http_status=409)
                cur.execute(
                    "INSERT INTO proofs (proof_id, plan_id, kind, proof_json,"
                    " proof_hash, created_at) VALUES (?,?,?,?,?,?)",
                    (proof["proof_id"], plan_id, "CUTOVER", canonical_json(proof),
                     proof["proof_hash"], now_iso()))
                self.conn.commit()
                return proof
            except Exception:
                self.conn.rollback()
                raise

    # --------------------------------------------------------------- revocation
    def execute_revocation(self, *, plan_id: str, group_id: str,
                           takeover_epoch: int, takeover_watermark: int,
                           at_iso: str, build_revocation_fn) -> dict:
        """Atomically revoke a cutover during the observation window.

        In ONE transaction:

        1. re-read the plan (must still be COMPLETED with the saved takeover
           cursor) and re-check that the takeover side has NOT confirmed any
           new record (group watermark unchanged from the takeover cursor);
        2. derive the original ownership and the retained source cursor from
           the persisted cutover proof (its mapping anchor);
        3. bump the fencing epoch again and repoint the group back to the
           original slot with its old watermark — the cursor is retained,
           never reset;
        4. build + persist the tamper-evident revocation credential;
        5. mark the plan REVOKED.

        Any failure rolls everything back: the takeover stays in effect and
        all runtime state is untouched.
        """
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                p = cur.execute("SELECT * FROM plans WHERE plan_id=?",
                                (plan_id,)).fetchone()
                if p is None:
                    raise MigrationError(NOT_FOUND, f"plan '{plan_id}' not found",
                                         http_status=404)
                if p["state"] == "REVOKED":
                    raise MigrationError(CONFLICT, "handover already revoked",
                                         http_status=409)
                if p["state"] != "COMPLETED":
                    raise MigrationError(
                        CONFLICT,
                        f"plan {plan_id} is {p['state']}; no handoff to revoke",
                        http_status=409)
                cutover_row = cur.execute(
                    "SELECT proof_json FROM proofs WHERE proof_id=? AND kind='CUTOVER'",
                    (p["proof_id"],)).fetchone()
                if cutover_row is None:
                    raise MigrationError(CONFLICT,
                                         "cutover proof missing; cannot revoke")
                cutover_proof = json.loads(cutover_row["proof_json"])
                anchor = cutover_proof["handoff"]["mapping_anchor"]
                source_watermark = anchor["source_lsn"]
                target_watermark = anchor["target_lsn"]

                g = cur.execute("SELECT * FROM groups WHERE group_id=?",
                                (group_id,)).fetchone()
                if g is None:
                    raise MigrationError(NOT_FOUND,
                                         f"consumer group '{group_id}' not found",
                                         http_status=404)
                if g["epoch"] != takeover_epoch:
                    raise MigrationError(CONFLICT,
                                         "consumer group epoch changed concurrently",
                                         http_status=409)
                # The takeover side must not have confirmed any new record.
                if g["watermark"] != takeover_watermark:
                    raise MigrationError(
                        TAKEOVER_PROGRESSED,
                        "the takeover side has confirmed new records; "
                        "the handoff can no longer be revoked",
                        {"takeover_cursor_lsn": takeover_watermark,
                         "current_lsn": g["watermark"]},
                        http_status=409)

                original = cutover_proof["handoff"]["old"]
                handoff_back = {
                    "group": group_id,
                    "from": {"cluster": g["cluster"], "slot": g["slot"],
                             "epoch": g["epoch"],
                             "watermark_target_lsn": g["watermark"]},
                    "restored": {"cluster": original["cluster"],
                                 "slot": original["slot"],
                                 "epoch": g["epoch"] + 1,
                                 "watermark_source_lsn": source_watermark},
                    "retained_cursor": {
                        "source_lsn": source_watermark,
                        "target_lsn": target_watermark,
                        "cutover_proof_id": cutover_proof["proof_id"],
                        "cutover_proof_hash": cutover_proof["proof_hash"],
                    },
                }
                credential = build_revocation_fn(handoff_back)

                r = cur.execute(
                    "UPDATE groups SET cluster=?, slot=?, epoch=epoch+1,"
                    " watermark=?, updated_at=?"
                    " WHERE group_id=? AND epoch=? AND watermark=?",
                    (original["cluster"], original["slot"], source_watermark,
                     now_iso(), group_id, takeover_epoch, takeover_watermark))
                if r.rowcount != 1:
                    raise MigrationError(TAKEOVER_PROGRESSED,
                                         "takeover progressed during revocation",
                                         http_status=409)
                r = cur.execute(
                    "UPDATE plans SET state='REVOKED', revocation_id=?,"
                    " version=version+1, updated_at=?"
                    " WHERE plan_id=? AND state='COMPLETED'",
                    (credential["revocation_id"], now_iso(), plan_id))
                if r.rowcount != 1:
                    raise MigrationError(CONFLICT,
                                         "plan state changed during revocation",
                                         http_status=409)
                cur.execute(
                    "INSERT INTO proofs (proof_id, plan_id, kind, proof_json,"
                    " proof_hash, created_at) VALUES (?,?,?,?,?,?)",
                    (credential["revocation_id"], plan_id, "REVOCATION",
                     canonical_json(credential), credential["revocation_hash"],
                     now_iso()))
                self.conn.commit()
                return credential
            except Exception:
                self.conn.rollback()
                raise

    # ------------------------------------------------------------------ proofs
    def get_proof(self, proof_id: str) -> dict | None:
        with self._lock:
            r = self.conn.execute("SELECT * FROM proofs WHERE proof_id=?",
                                  (proof_id,)).fetchone()
        return self._proof_row(r) if r else None

    def get_proof_for_plan(self, plan_id: str) -> dict | None:
        with self._lock:
            r = self.conn.execute(
                "SELECT * FROM proofs WHERE plan_id=? AND kind='CUTOVER'"
                " ORDER BY created_at DESC LIMIT 1",
                (plan_id,)).fetchone()
        return self._proof_row(r) if r else None

    def get_revocation_for_plan(self, plan_id: str) -> dict | None:
        with self._lock:
            r = self.conn.execute(
                "SELECT * FROM proofs WHERE plan_id=? AND kind='REVOCATION'"
                " ORDER BY created_at DESC LIMIT 1",
                (plan_id,)).fetchone()
        return self._proof_row(r) if r else None

    def get_all_proofs_for_plan(self, plan_id: str) -> list:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM proofs WHERE plan_id=? ORDER BY rowid",
                (plan_id,)).fetchall()
        return [self._proof_row(r) for r in rows]

    @staticmethod
    def _proof_row(r) -> dict:
        return {"proof_id": r["proof_id"], "plan_id": r["plan_id"],
                "kind": r["kind"], "proof": json.loads(r["proof_json"]),
                "proof_hash": r["proof_hash"], "created_at": r["created_at"]}
