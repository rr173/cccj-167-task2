"""Simulated database cluster with a logical replication log.

A Cluster models just enough of a database to exercise the migration
controller honestly:

* an append-only replication log with monotonically increasing LSNs;
* row state per table, mutated atomically together with the log append;
* replication slots with a confirmed flush position and a status
  (ACTIVE / RETIRED) — reading a retired slot is refused;
* per-table schema versions;
* write fencing for a table set (used during the cutover window);
* incremental (running) row checksums, so a consistent checksum snapshot
  can be taken at a marker LSN without stopping writes;
* target-side idempotent apply, keyed by (plan_id, source LSN).
"""
from __future__ import annotations

import copy
import threading
from dataclasses import dataclass

from .common import (
    CONFLICT,
    NOT_FOUND,
    SCHEMA_CHANGED,
    SLOT_NOT_ACTIVE,
    TARGET_NOT_EMPTY,
    WRITES_FENCED,
    MigrationError,
    checksum_hex,
    checksum_rows,
    now_iso,
    row_hash,
)

KIND_ROW = "row"
KIND_SCHEMA = "schema"
KIND_MARKER = "marker"


@dataclass
class LogEntry:
    lsn: int
    kind: str                 # row | schema | marker
    table: str | None
    op: str | None            # INSERT / UPDATE / DELETE for row entries
    key: str | None
    value: dict | None
    schema_version: int
    origin_lsn: int | None    # set on target entries applied from a source log
    payload: dict | None      # marker payload
    ts: str

    def to_dict(self) -> dict:
        return {
            "lsn": self.lsn,
            "kind": self.kind,
            "table": self.table,
            "op": self.op,
            "key": self.key,
            "value": self.value,
            "schema_version": self.schema_version,
            "origin_lsn": self.origin_lsn,
            "payload": self.payload,
            "ts": self.ts,
        }


class Cluster:
    def __init__(self, name: str):
        self.name = name
        self._lock = threading.RLock()
        self._lsn = 0
        self._log: list[LogEntry] = []
        self._tables: dict[str, dict[str, dict]] = {}
        self._running: dict[str, int] = {}                 # table -> xor checksum
        self._schema_versions: dict[str, int] = {}
        self._slots: dict[str, dict] = {}
        self._fences: dict[str, str] = {}                  # table -> fence token
        self._applied_meta: dict[str, int] = {}            # plan_id -> last applied source LSN (target role)
        self._marker_confirmations: dict[tuple, dict] = {} # (plan_id, marker_lsn) -> checksum snapshot

    # ------------------------------------------------------------------ reads
    def current_lsn(self) -> int:
        with self._lock:
            return self._lsn

    def schema_versions(self, tables) -> dict:
        with self._lock:
            return {t: self._schema_versions.get(t, 1) for t in tables}

    def table_rows(self, table: str) -> dict:
        with self._lock:
            return copy.deepcopy(self._tables.get(table, {}))

    def table_count(self, table: str) -> int:
        with self._lock:
            return len(self._tables.get(table, {}))

    def running_checksum(self, tables) -> dict:
        with self._lock:
            return {
                t: {
                    "rows": len(self._tables.get(t, {})),
                    "checksum": checksum_hex(self._running.get(t, 0)),
                }
                for t in tables
            }

    def recomputed_checksum(self, tables) -> dict:
        """Full recompute from row state — used to validate the running sums."""
        with self._lock:
            return {
                t: {
                    "rows": len(self._tables.get(t, {})),
                    "checksum": checksum_hex(checksum_rows(self._tables.get(t, {}))),
                }
                for t in tables
            }

    def fenced_tables(self) -> list:
        with self._lock:
            return sorted(self._fences)

    def fence_token_for(self, table: str) -> str | None:
        with self._lock:
            return self._fences.get(table)

    def tables_fenced_by(self, tables, token: str) -> bool:
        """True when every table is held by exactly this fence token."""
        with self._lock:
            return all(self._fences.get(t) == token for t in tables)

    def scan(self, after_lsn: int) -> list:
        """All log entries with lsn > after_lsn, in order."""
        with self._lock:
            return [copy.deepcopy(e) for e in self._log if e.lsn > after_lsn]

    def status(self) -> dict:
        with self._lock:
            return {
                "cluster": self.name,
                "lsn": self._lsn,
                "tables": {t: len(r) for t, r in sorted(self._tables.items())},
                "schema_versions": dict(sorted(self._schema_versions.items())),
                "slots": copy.deepcopy(self._slots),
                "fenced_tables": sorted(self._fences),
                "log_entries": len(self._log),
            }

    # ----------------------------------------------------------------- writes
    def _check_fence_locked(self, table: str):
        if table in self._fences:
            raise MigrationError(
                WRITES_FENCED,
                f"writes to table '{table}' on cluster '{self.name}' are fenced "
                f"by an in-flight migration cutover; retry after cutover",
                {"table": table, "cluster": self.name},
                http_status=423,
            )

    def _apply_row_locked(self, table: str, op: str, key: str, value):
        rows = self._tables.setdefault(table, {})
        h = self._running.get(table, 0)
        if op in ("INSERT", "UPDATE"):
            if key in rows:
                h ^= row_hash(key, rows[key])
            rows[key] = copy.deepcopy(value)
            h ^= row_hash(key, value)
        elif op == "DELETE":
            if key in rows:
                h ^= row_hash(key, rows.pop(key))
        self._running[table] = h

    def write(self, table: str, key: str, value: dict) -> int:
        """Upsert a row; appends the log entry atomically. Returns the LSN."""
        with self._lock:
            self._check_fence_locked(table)
            self._schema_versions.setdefault(table, 1)
            op = "INSERT" if key not in self._tables.get(table, {}) else "UPDATE"
            self._apply_row_locked(table, op, key, value)
            self._lsn += 1
            self._log.append(
                LogEntry(self._lsn, KIND_ROW, table, op, key, copy.deepcopy(value),
                         self._schema_versions[table], None, None, now_iso())
            )
            return self._lsn

    def delete(self, table: str, key: str):
        with self._lock:
            self._check_fence_locked(table)
            self._schema_versions.setdefault(table, 1)
            if key not in self._tables.get(table, {}):
                return None
            self._apply_row_locked(table, "DELETE", key, None)
            self._lsn += 1
            self._log.append(
                LogEntry(self._lsn, KIND_ROW, table, "DELETE", key, None,
                         self._schema_versions[table], None, None, now_iso())
            )
            return self._lsn

    def schema_change(self, table: str, note: str = "") -> int:
        with self._lock:
            self._check_fence_locked(table)
            self._schema_versions[table] = self._schema_versions.get(table, 1) + 1
            self._lsn += 1
            self._log.append(
                LogEntry(self._lsn, KIND_SCHEMA, table, "SCHEMA", None, None,
                         self._schema_versions[table], None, {"note": note}, now_iso())
            )
            return self._lsn

    # ---------------------------------------------------------------- fencing
    def fence_tables(self, tables, token: str):
        with self._lock:
            for t in tables:
                holder = self._fences.get(t)
                if holder is not None and holder != token:
                    raise MigrationError(CONFLICT, f"table '{t}' already fenced by {holder}")
                self._fences[t] = token

    def unfence_tables(self, tables, token: str):
        with self._lock:
            for t in tables:
                holder = self._fences.get(t)
                if holder is None:
                    continue
                if holder != token:
                    raise MigrationError(CONFLICT, f"table '{t}' fenced by {holder}, not {token}")
                del self._fences[t]

    # ----------------------------------------------------------------- marker
    def emit_marker(self, payload: dict, tables) -> tuple:
        """Append a verification marker; returns (lsn, checksum snapshot *now*).

        The checksum snapshot is taken atomically with the log append, so it
        exactly describes the table state as of the marker LSN — this is what
        makes zero-downtime verification possible.
        """
        with self._lock:
            self._lsn += 1
            self._log.append(
                LogEntry(self._lsn, KIND_MARKER, None, None, None, None, 0,
                         None, copy.deepcopy(payload), now_iso())
            )
            return self._lsn, self.running_checksum(tables)

    # ------------------------------------------------------------- consistent
    def snapshot_rows(self, tables) -> tuple:
        """Atomic copy of the given tables plus the LSN of that instant."""
        with self._lock:
            return {t: copy.deepcopy(self._tables.get(t, {})) for t in tables}, self._lsn

    # ------------------------------------------------------------------ slots
    def create_slot(self, name: str):
        with self._lock:
            if name in self._slots:
                raise MigrationError(CONFLICT, f"slot '{name}' already exists on {self.name}")
            self._slots[name] = {
                "name": name, "status": "ACTIVE", "confirmed_lsn": 0,
                "created_at": now_iso(),
            }

    def get_slot(self, name: str) -> dict:
        with self._lock:
            s = self._slots.get(name)
            if s is None:
                raise MigrationError(NOT_FOUND, f"slot '{name}' not found on {self.name}",
                                     http_status=404)
            return copy.deepcopy(s)

    def list_slots(self) -> list:
        with self._lock:
            return [copy.deepcopy(s) for s in self._slots.values()]

    def retire_slot(self, name: str):
        with self._lock:
            s = self._slots.get(name)
            if s is None:
                raise MigrationError(NOT_FOUND, f"slot '{name}' not found on {self.name}",
                                     http_status=404)
            s["status"] = "RETIRED"

    def activate_slot(self, name: str, confirmed_lsn: int | None = None):
        """Reactivate a RETIRED slot (revocation of a handoff). The confirmed
        cursor is restored, never reset: the resumed original consumer picks
        up exactly at the cursor it held before the switch."""
        with self._lock:
            s = self._slots.get(name)
            if s is None:
                raise MigrationError(NOT_FOUND, f"slot '{name}' not found on {self.name}",
                                     http_status=404)
            if confirmed_lsn is not None and confirmed_lsn > s["confirmed_lsn"]:
                s["confirmed_lsn"] = confirmed_lsn
            s["status"] = "ACTIVE"

    def update_slot_confirmed(self, name: str, lsn: int):
        with self._lock:
            s = self._slots.get(name)
            if s is not None and lsn > s["confirmed_lsn"]:
                s["confirmed_lsn"] = lsn

    def read_slot(self, name: str, after_lsn: int, limit: int = 1000) -> list:
        """Consumer-facing read. A RETIRED slot refuses reads — this is the
        cluster-level fence that complements the controller's epoch check."""
        with self._lock:
            s = self._slots.get(name)
            if s is None:
                raise MigrationError(NOT_FOUND, f"slot '{name}' not found on {self.name}",
                                     http_status=404)
            if s["status"] != "ACTIVE":
                raise MigrationError(SLOT_NOT_ACTIVE,
                                     f"slot '{name}' on {self.name} is {s['status']}",
                                     http_status=409)
            out = [e for e in self._log
                   if e.lsn > after_lsn and e.kind in (KIND_ROW, KIND_SCHEMA)]
            return [copy.deepcopy(e) for e in out[:limit]]

    # ------------------------------------------------------------ target role
    def bulk_load(self, plan_id: str, rows_by_table: dict, source_lsn: int,
                  truncate: bool = False):
        """Load a consistent snapshot. Does not emit log entries (the initial
        copy is not part of the change stream, exactly like Postgres' COPY
        phase of logical replication)."""
        with self._lock:
            if not truncate:
                non_empty = {t: len(self._tables.get(t, {}))
                             for t in rows_by_table if self._tables.get(t)}
                if non_empty:
                    raise MigrationError(
                        TARGET_NOT_EMPTY,
                        "target tables are not empty; clear them or retry the plan "
                        "(retries truncate automatically)",
                        non_empty, http_status=409)
            for t, rows in rows_by_table.items():
                self._tables[t] = copy.deepcopy(rows)
                self._running[t] = checksum_rows(rows)
                self._schema_versions.setdefault(t, 1)
            self._applied_meta[plan_id] = source_lsn

    def apply_replication_event(self, plan_id: str, entry: LogEntry) -> tuple:
        """Idempotently apply one replicated source entry.

        Returns (target_lsn, marker_confirmation). (None, None) means the
        entry was already applied — duplicate deliveries are absorbed here,
        which is what makes stream retries safe.
        """
        with self._lock:
            if entry.lsn <= self._applied_meta.get(plan_id, 0):
                return None, None
            if entry.kind == KIND_ROW:
                self._apply_row_locked(entry.table, entry.op, entry.key, entry.value)
            elif entry.kind == KIND_SCHEMA:
                raise MigrationError(
                    SCHEMA_CHANGED,
                    f"schema event for table '{entry.table}' reached target apply; "
                    f"the plan must be re-created against the new schema")
            confirmation = None
            self._lsn += 1
            self._log.append(
                LogEntry(self._lsn, entry.kind, entry.table, entry.op, entry.key,
                         copy.deepcopy(entry.value), entry.schema_version,
                         entry.lsn, copy.deepcopy(entry.payload), now_iso())
            )
            if entry.kind == KIND_MARKER:
                tables = (entry.payload or {}).get("tables", [])
                confirmation = self.running_checksum(tables)
                self._marker_confirmations[(plan_id, entry.lsn)] = confirmation
            self._applied_meta[plan_id] = entry.lsn
            return self._lsn, confirmation

    def advance_applied(self, plan_id: str, source_lsn: int):
        """Record that all source entries up to source_lsn have been consumed
        (entries that needed no apply — other tables, foreign markers — still
        advance the position so the drain check is exact)."""
        with self._lock:
            cur = self._applied_meta.get(plan_id, 0)
            if source_lsn > cur:
                self._applied_meta[plan_id] = source_lsn

    def applied_source_lsn(self, plan_id: str):
        with self._lock:
            return self._applied_meta.get(plan_id)
