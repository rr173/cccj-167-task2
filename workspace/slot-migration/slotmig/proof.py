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


def build_revocation_credential(*, plan, cutover_proof: dict,
                                handoff_back: dict, observation: dict,
                                source_protected: bool, source_fence_token: str | None,
                                slots: dict, timeline_digest: str) -> dict:
    """The tamper-evident credential produced when a cutover is revoked
    inside its observation window. It binds together:

    * the original cutover (id + hash + mapping anchor) being undone;
    * the eligibility snapshot at revocation time (window open, no takeover
      confirmation, source still protected);
    * the ownership restoration with the fencing epoch bump;
    * the retained cursor (source LSN preserved, target cursor recorded).
    """
    credential = {
        "kind": "REVOCATION",
        "revocation_id": new_id("revocation"),
        "plan_id": plan.plan_id,
        "attempt": plan.attempt,
        "generated_at": now_iso(),
        "cutover_ref": {
            "proof_id": cutover_proof["proof_id"],
            "proof_hash": cutover_proof["proof_hash"],
            "cutover_at": plan.cutover_at,
            "cutover_lsn": plan.cutover_lsn,
        },
        "observation": {
            "window_seconds": plan.observation_seconds,
            "deadline": observation["deadline"],
            "elapsed_seconds": observation["elapsed_seconds"],
            "within_window": observation["status"] != "expired",
        },
        "handoff_back": handoff_back,
        "eligibility": {
            "takeover_confirmed_new_records": False,
            "takeover_cursor": plan.takeover_cursor,
            "source_still_protected": source_protected,
            "source_fence_token": source_fence_token,
        },
        "slots": slots,
        "timeline_digest": timeline_digest,
    }
    credential["revocation_hash"] = sha256_hex(canonical_json(credential))
    return credential


def verify_revocation(credential: dict, store=None) -> dict:
    """Independent verification of a handover-revocation credential.
    Returns {"valid": bool, "checks": [...]}. Never raises."""
    checks = []

    def check(name, ok, detail=""):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    try:
        body = {k: v for k, v in credential.items() if k != "revocation_hash"}
        recomputed = sha256_hex(canonical_json(body))
        check("hash_integrity", recomputed == credential.get("revocation_hash"),
              f"recomputed={recomputed[:16]}…")
    except Exception as exc:
        check("hash_integrity", False, str(exc))
        return {"valid": False, "checks": checks}

    check("kind", credential.get("kind") == "REVOCATION",
          str(credential.get("kind")))

    hb = credential.get("handoff_back", {})
    frm, restored = hb.get("from", {}), hb.get("restored", {})
    retained = hb.get("retained_cursor", {})
    check("epoch_step",
          restored.get("epoch") == frm.get("epoch", 0) + 1,
          f"{frm.get('epoch')} → {restored.get('epoch')}")

    plan_spec_cutover = credential.get("cutover_ref", {})
    check("cursor_retained",
          retained.get("source_lsn") == restored.get("watermark_source_lsn")
          and retained.get("target_lsn") == frm.get("watermark_target_lsn"),
          f"source@{retained.get('source_lsn')} / "
          f"target@{retained.get('target_lsn')}")
    check("cutover_ref_present",
          bool(plan_spec_cutover.get("proof_id"))
          and bool(plan_spec_cutover.get("proof_hash")))

    obs = credential.get("observation", {})
    elig = credential.get("eligibility", {})
    check("within_observation_window",
          obs.get("within_window") is True,
          f"deadline={obs.get('deadline')}")
    check("no_takeover_confirmation",
          elig.get("takeover_confirmed_new_records") is False)
    check("source_protected",
          elig.get("source_still_protected") is True)

    if store is not None:
        try:
            plan = store.get_plan(credential.get("plan_id", ""))
            check("plan_revoked", plan.state == "REVOKED", plan.state)
            check("revocation_link",
                  plan.revocation_id == credential.get("revocation_id"))
        except Exception as exc:
            check("plan_revoked", False, str(exc))
        try:
            group = store.get_group(hb.get("group", ""))
            check("group_restored",
                  group["epoch"] == restored.get("epoch")
                  and group["cluster"] == restored.get("cluster")
                  and group["slot"] == restored.get("slot")
                  and group["watermark"] == restored.get("watermark_source_lsn"),
                  f"group now on {group['cluster']}/{group['slot']} "
                  f"epoch={group['epoch']} watermark={group['watermark']}")
        except Exception as exc:
            check("group_restored", False, str(exc))
        try:
            stored = store.get_proof(credential.get("revocation_id", ""))
            check("stored_hash_match",
                  stored is not None
                  and stored["proof_hash"] == credential.get("revocation_hash"))
        except Exception as exc:
            check("stored_hash_match", False, str(exc))
        try:
            cutover = store.get_proof(
                plan_spec_cutover.get("proof_id", ""))
            check("cutover_proof_hash_match",
                  cutover is not None
                  and cutover["proof_hash"] == plan_spec_cutover.get("proof_hash"))
            if cutover is not None:
                anchor = cutover["proof"]["handoff"]["mapping_anchor"]
                check("retained_cursor_matches_cutover",
                      anchor.get("source_lsn") == retained.get("source_lsn")
                      and anchor.get("target_lsn") == retained.get("target_lsn"),
                      f"anchor={anchor}")
        except Exception as exc:
            check("cutover_proof_hash_match", False, str(exc))

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
