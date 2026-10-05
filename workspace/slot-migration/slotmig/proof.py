"""Cutover proof (切换证明): construction and independent verification.

The proof is a self-contained, tamper-evident record of the atomic switch.
`verify_proof` recomputes the hash, checks internal consistency, and — when
given a store — cross-checks the proof against the live controller state.
"""
from __future__ import annotations

from .common import canonical_json, new_id, now_iso, sha256_hex


def build_proof(*, plan, snapshot: dict | None, manifest_digest: str | None,
                verify_digest: str, verify_marker_lsn: int, verified_at: str,
                cutover_lsn: int, final_digest: str, handoff: dict,
                timeline_digest: str) -> dict:
    proof = {
        "proof_id": new_id("proof"),
        "plan_id": plan.plan_id,
        "attempt": plan.attempt,
        "generated_at": now_iso(),
        "plan_spec": {
            "source_cluster": plan.source_cluster,
            "source_slot": plan.source_slot,
            "target_cluster": plan.target_cluster,
            "target_slot": plan.target_slot,
            "tables": list(plan.tables),
            "schema_versions": plan.schema_versions,
            "frozen_lsn": plan.frozen_lsn,
            "consumer_group": plan.consumer_group,
        },
        "snapshot": {
            "snapshot_id": plan.snapshot_id,
            "snapshot_lsn": plan.snapshot_lsn,
            "manifest_digest": manifest_digest,
        },
        "verification": {
            "marker_lsn": verify_marker_lsn,
            "digest": verify_digest,
            "verified_at": verified_at,
        },
        "final_check": {
            "cutover_lsn": cutover_lsn,
            "digest": final_digest,
        },
        "handoff": handoff,
        "timeline_digest": timeline_digest,
    }
    proof["proof_hash"] = sha256_hex(canonical_json(proof))
    return proof


# ------------------------------------------------------------- revocation
def build_revocation(*, plan, cutover_proof: dict, takeover_source_cursor: int,
                     takeover_cursor: int, observed_at: str, revoked_at: str,
                     restoration: dict, from_epoch: int,
                     timeline_digest: str) -> dict:
    """The tamper-evident credential produced by a handoff withdrawal.

    It binds together: the cutover it reverses, the observation window
    (deadline + that the revocation happened inside it), the saved takeover
    confirmation cursor that never moved, and the restoration (original
    ownership, epoch bumped once more, original cursor retained).
    """
    rev = {
        "kind": "handoff_revocation",
        "revocation_id": new_id("revoke"),
        "plan_id": plan.plan_id,
        "attempt": plan.attempt,
        "generated_at": now_iso(),
        "cutover_ref": {
            "proof_id": cutover_proof["proof_id"],
            "proof_hash": cutover_proof["proof_hash"],
            "cutover_lsn": plan.cutover_lsn,
        },
        "observation": {
            "window_ms": plan.observation_window_ms,
            "deadline": plan.observation_deadline,
            "observed_at": observed_at,
            "revoked_at": revoked_at,
        },
        "takeover": {
            "cluster": plan.target_cluster,
            "slot": plan.target_slot,
            "epoch": from_epoch,
            "confirm_cursor_target_lsn": takeover_cursor,
            "new_records_confirmed": False,
        },
        "restoration": restoration,
        "cursor_retention": {
            "takeover_cursor_retained": takeover_cursor,
            "restored_source_cursor": takeover_source_cursor,
        },
        "timeline_digest": timeline_digest,
    }
    rev["revocation_hash"] = sha256_hex(canonical_json(rev))
    return rev


def verify_revocation(revocation: dict, store=None) -> dict:
    """Independent verification of a withdrawal credential. Never raises."""
    checks = []

    def check(name, ok, detail=""):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    try:
        body = {k: v for k, v in revocation.items() if k != "revocation_hash"}
        recomputed = sha256_hex(canonical_json(body))
        check("hash_integrity", recomputed == revocation.get("revocation_hash"),
              f"recomputed={recomputed[:16]}…")
    except Exception as exc:  # malformed credential
        check("hash_integrity", False, str(exc))
        return {"valid": False, "checks": checks}

    check("kind", revocation.get("kind") == "handoff_revocation",
          revocation.get("kind", ""))

    takeover = revocation.get("takeover", {})
    restoration = revocation.get("restoration", {})
    from_epoch = takeover.get("epoch")
    check("epoch_step",
          restoration.get("epoch") == from_epoch + 1,
          f"{from_epoch} → {restoration.get('epoch')}")
    retention = revocation.get("cursor_retention", {})
    check("cursor_retention",
          retention.get("takeover_cursor_retained")
          == takeover.get("confirm_cursor_target_lsn")
          and retention.get("restored_source_cursor")
          == restoration.get("watermark_source_lsn"),
          f"takeover={retention.get('takeover_cursor_retained')} "
          f"source={retention.get('restored_source_cursor')}")
    check("takeover_did_not_advance",
          takeover.get("new_records_confirmed") is False)
    obs = revocation.get("observation", {})
    check("within_window",
          bool(obs.get("deadline")) and obs.get("revoked_at") is not None
          and obs.get("revoked_at") <= obs["deadline"],
          f"{obs.get('revoked_at')} ≤ {obs.get('deadline')}")

    if store is not None:
        plan = None
        try:
            plan = store.get_plan(revocation.get("plan_id", ""))
            check("plan_withdrawn", plan.state == "WITHDRAWN", plan.state)
            check("plan_revocation_link",
                  plan.revocation_id == revocation.get("revocation_id"))
            check("window_matches",
                  plan.observation_deadline == obs.get("deadline"),
                  f"stored deadline={plan.observation_deadline}")
        except Exception as exc:
            check("plan_withdrawn", False, str(exc))
        try:
            group = store.get_group(plan.consumer_group if plan else "")
            check("group_restored",
                  group["epoch"] == restoration.get("epoch")
                  and group["cluster"] == restoration.get("cluster")
                  and group["slot"] == restoration.get("slot")
                  and group["watermark"]
                  == restoration.get("watermark_source_lsn"),
                  f"group now on {group['cluster']}/{group['slot']} "
                  f"epoch={group['epoch']} watermark={group['watermark']}")
        except Exception as exc:
            check("group_restored", False, str(exc))
        try:
            stored = store.get_revocation(revocation.get("revocation_id", ""))
            check("stored_hash_match",
                  stored is not None
                  and stored["revocation_hash"] == revocation.get("revocation_hash"))
        except Exception as exc:
            check("stored_hash_match", False, str(exc))
        try:
            ref = revocation.get("cutover_ref", {})
            cutover = store.get_proof(ref.get("proof_id", ""))
            check("cutover_ref_match",
                  cutover is not None
                  and cutover["proof_hash"] == ref.get("proof_hash"),
                  f"cutover {ref.get('proof_id')}")
        except Exception as exc:
            check("cutover_ref_match", False, str(exc))

    return {"valid": all(c["ok"] for c in checks), "checks": checks}


def verify_proof(proof: dict, store=None) -> dict:
    """Returns {"valid": bool, "checks": [...]}. Never raises."""
    checks = []

    def check(name, ok, detail=""):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    try:
        body = {k: v for k, v in proof.items() if k != "proof_hash"}
        recomputed = sha256_hex(canonical_json(body))
        check("hash_integrity", recomputed == proof.get("proof_hash"),
              f"recomputed={recomputed[:16]}…")
    except Exception as exc:  # malformed proof
        check("hash_integrity", False, str(exc))
        return {"valid": False, "checks": checks}

    handoff = proof.get("handoff", {})
    old, new = handoff.get("old", {}), handoff.get("new", {})
    check("epoch_step", new.get("epoch") == old.get("epoch", 0) + 1,
          f"{old.get('epoch')} → {new.get('epoch')}")
    anchor = handoff.get("mapping_anchor", {})
    check("watermark_mapping",
          anchor.get("source_lsn") == old.get("watermark_source_lsn")
          and anchor.get("target_lsn") == new.get("watermark_target_lsn"),
          f"{anchor.get('source_lsn')} → {anchor.get('target_lsn')}")
    check("digests_present",
          bool(proof.get("verification", {}).get("digest"))
          and bool(proof.get("final_check", {}).get("digest")))

    if store is not None:
        try:
            plan = store.get_plan(proof.get("plan_id", ""))
            check("plan_completed", plan.state == "COMPLETED", plan.state)
            check("plan_proof_link", plan.proof_id == proof.get("proof_id"))
        except Exception as exc:
            check("plan_completed", False, str(exc))
        try:
            group = store.get_group(handoff.get("group", ""))
            check("group_switched",
                  group["epoch"] == new.get("epoch")
                  and group["cluster"] == new.get("cluster")
                  and group["slot"] == new.get("slot")
                  and group["watermark"] == new.get("watermark_target_lsn"),
                  f"group now on {group['cluster']}/{group['slot']} "
                  f"epoch={group['epoch']} watermark={group['watermark']}")
        except Exception as exc:
            check("group_switched", False, str(exc))
        try:
            stored = store.get_proof(proof.get("proof_id", ""))
            check("stored_hash_match",
                  stored is not None and stored["proof_hash"] == proof.get("proof_hash"))
        except Exception as exc:
            check("stored_hash_match", False, str(exc))
        try:
            q = store.floor_map(proof.get("plan_id", ""), proof.get("attempt", 0),
                                old.get("watermark_source_lsn", 0))
            check("mapping_recomputed", q == new.get("watermark_target_lsn"),
                  f"floor_map({old.get('watermark_source_lsn')})={q}")
        except Exception as exc:
            check("mapping_recomputed", False, str(exc))

    return {"valid": all(c["ok"] for c in checks), "checks": checks}
