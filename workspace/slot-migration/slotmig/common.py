"""Shared helpers: ids, timestamps, canonical JSON, checksums, error type."""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 timestamp (as produced by now_iso) into aware UTC."""
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def iso_add_seconds(value: str, seconds: int) -> str:
    return (parse_iso(value) + timedelta(seconds=seconds)).isoformat(
        timespec="milliseconds")


def seconds_between(later: str, earlier: str) -> float:
    return (parse_iso(later) - parse_iso(earlier)).total_seconds()


# Post-cutover observation window in which the operator may revoke the handoff.
DEFAULT_OBSERVATION_SECONDS = 300


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(data) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def row_hash(key: str, value) -> int:
    """Order-independent per-row hash used by the running checksums."""
    digest = hashlib.sha256(canonical_json([key, value]).encode("utf-8")).digest()
    return int.from_bytes(digest[:16], "big")


def checksum_rows(rows: dict) -> int:
    h = 0
    for k, v in rows.items():
        h ^= row_hash(k, v)
    return h


def checksum_hex(h: int) -> str:
    return f"{h:032x}"


def table_summary(rows: dict) -> dict:
    return {"rows": len(rows), "checksum": checksum_hex(checksum_rows(rows))}


def digest_tables(summaries: dict) -> str:
    """Digest over {table: {"rows": n, "checksum": hex}} — the 校验摘要."""
    return sha256_hex(canonical_json(summaries))


class MigrationError(Exception):
    """Domain error with a stable machine-readable code."""

    def __init__(self, code: str, message: str, details=None, http_status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.http_status = http_status

    def to_dict(self) -> dict:
        return {"error": self.code, "message": self.message, "details": self.details}


# Stable error codes (also part of the API contract).
NOT_FOUND = "NOT_FOUND"
BAD_REQUEST = "BAD_REQUEST"
INVALID_STATE = "INVALID_STATE"
CONFLICT = "CONFLICT"
SCHEMA_CHANGED = "SCHEMA_CHANGED"
VERIFY_MISMATCH = "VERIFY_MISMATCH"
FINAL_MISMATCH = "FINAL_MISMATCH"
WRITES_FENCED = "WRITES_FENCED"
STALE_EPOCH = "STALE_EPOCH"
FUTURE_EPOCH = "FUTURE_EPOCH"
SLOT_NOT_ACTIVE = "SLOT_NOT_ACTIVE"
TARGET_NOT_EMPTY = "TARGET_NOT_EMPTY"
RETRY_NOT_ALLOWED = "RETRY_NOT_ALLOWED"
DRAIN_INCOMPLETE = "DRAIN_INCOMPLETE"
LOAD_MISMATCH = "LOAD_MISMATCH"
TAKEOVER_PROGRESSED = "TAKEOVER_PROGRESSED"
SOURCE_NOT_PROTECTED = "SOURCE_NOT_PROTECTED"
OBSERVATION_EXPIRED = "OBSERVATION_EXPIRED"
REVOKE_NOT_ALLOWED = "REVOKE_NOT_ALLOWED"
