"""End-to-end and edge-case tests for the migration controller.

Covers the requirements:
  * consistent snapshot → incremental stream → verify → atomic cutover
  * source keeps writing during migration        → no lost events
  * schema change mid-migration                  → plan aborts, consumers safe
  * migration retry                              → no duplicate advancement
  * late acks from old consumers                 → fenced, watermark intact
  * never two valid consumption watermarks
"""
import threading
import time
import unittest

from slotmig import (
    Cluster,
    MigrationController,
    MigrationError,
    Store,
    verify_proof,
    verify_revocation,
)
from slotmig.reconcile import render_html
from datetime import datetime, timedelta, timezone


class Base(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")
        self.source = Cluster("source")
        self.target = Cluster("target")
        self.source.create_slot("app_slot")
        self.ctl = MigrationController(
            {"source": self.source, "target": self.target}, self.store)
        for i in range(50):
            self.source.write("orders", f"o{i:03d}", {"amount": i})
        for i in range(10):
            self.source.write("customers", f"c{i:02d}", {"tier": i % 3})

    def tearDown(self):
        self.store.close()

    # -- helpers ------------------------------------------------------
    def make_plan(self, tables=("orders",), group="analytics", ctl=None):
        ctl = ctl or self.ctl
        plan = ctl.create_plan({
            "source_cluster": "source", "source_slot": "app_slot",
            "target_cluster": "target", "target_slot": "app_slot_replica",
            "tables": list(tables), "consumer_group": group,
        })
        ctl.freeze(plan["plan_id"])
        return plan["plan_id"]

    def run_to_verified(self, pid):
        self.ctl.start(pid)
        self.ctl.catch_up(pid)
        return self.ctl.verify(pid)

    def run_to_completed(self, pid):
        self.run_to_verified(pid)
        return self.ctl.cutover(pid)

    def assert_tables_equal(self, *tables):
        for t in tables:
            self.assertEqual(self.source.table_rows(t), self.target.table_rows(t),
                             f"table {t} diverged")


class HappyPath(Base):
    def test_full_flow(self):
        pid = self.make_plan(tables=("orders", "customers"))
        self.ctl.start(pid)
        # source keeps writing during streaming
        for i in range(50, 90):
            self.source.write("orders", f"o{i:03d}", {"amount": i})
        self.source.delete("orders", "o000")
        self.ctl.catch_up(pid)
        # a consumer keeps committing on the old slot mid-migration
        self.ctl.commit("analytics", "source", "app_slot", lsn=10, epoch=1)
        result = self.ctl.verify(pid)
        self.assertEqual(result["state"], "VERIFIED")
        proof = self.ctl.cutover(pid)

        self.assert_tables_equal("orders", "customers")
        self.assertEqual(self.source.table_count("orders"), 89)  # 90 - 1 delete

        group = self.ctl.get_group("analytics")
        self.assertEqual((group["cluster"], group["slot"]),
                         ("target", "app_slot_replica"))
        self.assertEqual(group["epoch"], 2)

        # proof verifies against the store
        check = verify_proof(proof, self.store)
        self.assertTrue(check["valid"], check)
        # plan is complete, proof linked
        plan = self.ctl.get_plan(pid)
        self.assertEqual(plan["state"], "COMPLETED")
        self.assertEqual(plan["proof_id"], proof["proof_id"])

    def test_snapshot_is_consistent_under_writes(self):
        # writes interleaved tightly around the snapshot
        pid = self.make_plan()
        self.source.write("orders", "pre-snap", {"v": 1})
        self.ctl.start(pid)
        for i in range(100):
            self.source.write("orders", f"during-{i}", {"v": i})
        self.ctl.catch_up(pid)
        self.ctl.verify(pid)
        self.ctl.cutover(pid)
        self.assert_tables_equal("orders")
        self.assertEqual(self.source.table_count("orders"), 151)

    def test_stream_is_idempotent(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        for i in range(20):
            self.source.write("orders", f"dup-{i}", {"v": i})
        r1 = self.ctl.catch_up(pid)
        size1 = self.store.map_size(pid, 1)
        r2 = self.ctl.catch_up(pid)  # nothing new
        self.assertEqual(r2["applied"], 0)
        self.assertEqual(self.store.map_size(pid, 1), size1)
        self.assertGreaterEqual(r1["applied"], 20)
        self.assertEqual(self.target.table_count("orders"), 70)


class NoEventLoss(Base):
    def test_interleaved_writes_every_phase(self):
        pid = self.make_plan()
        writes = []

        def w(tag, n):
            for i in range(n):
                key = f"{tag}-{i}"
                writes.append(key)
                self.source.write("orders", key, {"tag": tag, "i": i})

        w("before", 5)
        self.ctl.start(pid)
        w("after-snap", 5)
        self.ctl.catch_up(pid)
        w("after-catchup", 5)
        self.ctl.verify(pid)
        w("after-verify", 5)          # written between verify and cutover
        self.ctl.cutover(pid)         # must be drained by the cutover
        self.assert_tables_equal("orders")
        for key in writes:
            self.assertIn(key, self.target.table_rows("orders"))

    def test_concurrent_writer_thread(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        errors, fenced = [], threading.Event()

        def writer():
            i = 0
            while True:
                try:
                    self.source.write("orders", f"w{i:05d}", {"i": i})
                    i += 1
                    time.sleep(0.0005)
                except MigrationError as exc:
                    if exc.code == "WRITES_FENCED":
                        fenced.set()
                        return
                    errors.append(exc)
                    return

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        for _ in range(5):
            self.ctl.catch_up(pid)
            time.sleep(0.005)
        self.ctl.verify(pid)
        self.ctl.cutover(pid)
        t.join(timeout=5)

        self.assertFalse(errors, errors)
        self.assertTrue(fenced.is_set(), "writer should have hit the cutover fence")
        # every accepted write survived: full equality, exact counts
        self.assert_tables_equal("orders")
        self.assertEqual(self.source.table_count("orders"),
                         self.target.table_count("orders"))

    def test_cutover_drains_inflight_writes(self):
        pid = self.make_plan()
        self.run_to_verified(pid)
        # writes after verification, right up to the fence
        last = [self.source.write("orders", f"last-{i}", {"i": i}) for i in range(7)]
        proof = self.ctl.cutover(pid)
        self.assertGreaterEqual(proof["final_check"]["cutover_lsn"], last[-1])
        applied = self.target.applied_source_lsn(pid)
        self.assertGreaterEqual(applied, proof["final_check"]["cutover_lsn"])
        self.assert_tables_equal("orders")


class SchemaChanges(Base):
    def test_schema_change_on_plan_table_aborts(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        self.source.schema_change("orders", "add column total")
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.catch_up(pid)
        self.assertEqual(ctx.exception.code, "SCHEMA_CHANGED")
        plan = self.ctl.get_plan(pid)
        self.assertEqual(plan["state"], "FAILED")
        self.assertEqual(plan["error"]["code"], "SCHEMA_CHANGED")
        # consumers are completely unaffected
        r = self.ctl.commit("analytics", "source", "app_slot", lsn=5, epoch=1)
        self.assertTrue(r["advanced"])
        # retry is refused: a new plan is required
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.retry(pid)
        self.assertEqual(ctx.exception.code, "RETRY_NOT_ALLOWED")
        # cutover impossible
        with self.assertRaises(MigrationError):
            self.ctl.cutover(pid)

    def test_schema_change_on_unrelated_table_is_fine(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        self.source.schema_change("audit_log", "unrelated table")
        self.ctl.catch_up(pid)
        self.ctl.verify(pid)
        self.ctl.cutover(pid)
        self.assertEqual(self.ctl.get_plan(pid)["state"], "COMPLETED")

    def test_schema_change_between_verify_and_cutover(self):
        pid = self.make_plan()
        self.run_to_verified(pid)
        self.source.schema_change("orders", "sneaky")
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.cutover(pid)
        self.assertEqual(ctx.exception.code, "SCHEMA_CHANGED")
        # consumers still on the old slot
        group = self.ctl.get_group("analytics")
        self.assertEqual((group["cluster"], group["slot"]), ("source", "app_slot"))


class RetrySemantics(Base):
    def corrupt_target(self):
        # out-of-band write to the replica (fault injection)
        self.target.write("orders", "evil-row", {"corrupt": True})

    def test_verify_mismatch_blocks_cutover_then_retry_succeeds(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        self.ctl.catch_up(pid)
        self.corrupt_target()
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.verify(pid)
        self.assertEqual(ctx.exception.code, "VERIFY_MISMATCH")
        self.assertIn("orders", ctx.exception.details["diff"])
        with self.assertRaises(MigrationError):
            self.ctl.cutover(pid)
        # consumer group untouched
        self.assertEqual(self.ctl.get_group("analytics")["epoch"], 1)

        # retry: new attempt re-snapshots (truncating the corrupt target)
        self.ctl.retry(pid)
        self.assertEqual(self.ctl.get_plan(pid)["attempt"], 2)
        self.ctl.verify(pid)
        proof = self.ctl.cutover(pid)
        self.assertEqual(proof["attempt"], 2)
        self.assert_tables_equal("orders")
        self.assertNotIn("evil-row", self.target.table_rows("orders"))
        # audit trail shows both attempts
        kinds = [e["kind"] for e in self.store.get_events(pid)]
        self.assertIn("FAILED", kinds)
        self.assertIn("RETRY", kinds)

    def test_retry_rejects_non_failed_plan(self):
        pid = self.make_plan()
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.retry(pid)
        self.assertEqual(ctx.exception.code, "INVALID_STATE")

    def test_target_not_empty_first_attempt(self):
        self.target.write("orders", "pre-existing", {"x": 1})
        pid = self.make_plan()
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.start(pid)
        self.assertEqual(ctx.exception.code, "TARGET_NOT_EMPTY")
        # plan is still FROZEN; clear the target and start again
        self.assertEqual(self.ctl.get_plan(pid)["state"], "FROZEN")
        self.target.bulk_load("manual-cleanup", {}, 0, truncate=True)
        self.target._tables["orders"] = {}
        self.target._running["orders"] = 0
        self.ctl.start(pid)
        self.assertEqual(self.ctl.get_plan(pid)["state"], "STREAMING")

    def test_repeated_catch_up_after_retry_no_double_apply(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        for i in range(10):
            self.source.write("orders", f"r-{i}", {"i": i})
        self.corrupt_target()
        with self.assertRaises(MigrationError):
            self.ctl.verify(pid)
        self.ctl.retry(pid)
        for _ in range(3):
            self.ctl.catch_up(pid)  # idempotent
        self.ctl.verify(pid)
        self.ctl.cutover(pid)
        self.assert_tables_equal("orders")
        self.assertEqual(self.target.table_count("orders"), 60)


class ConsumerFencing(Base):
    def test_late_ack_from_old_consumer_rejected(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        self.ctl.commit("analytics", "source", "app_slot", lsn=20, epoch=1)
        self.ctl.verify(pid)
        proof = self.ctl.cutover(pid)
        new_q = proof["handoff"]["new"]["watermark_target_lsn"]

        # late ack with the old epoch → fenced
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.commit("analytics", "source", "app_slot", lsn=999, epoch=1)
        self.assertEqual(ctx.exception.code, "STALE_EPOCH")
        # right epoch but the retired slot → rejected
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.commit("analytics", "source", "app_slot", lsn=999, epoch=2)
        self.assertEqual(ctx.exception.code, "SLOT_NOT_ACTIVE")
        # watermark unmoved
        self.assertEqual(self.ctl.get_group("analytics")["watermark"], new_q)
        # the retired slot also refuses reads at the cluster level
        with self.assertRaises(MigrationError) as ctx:
            self.source.read_slot("app_slot", 0)
        self.assertEqual(ctx.exception.code, "SLOT_NOT_ACTIVE")

    def test_watermark_never_regresses(self):
        pid = self.make_plan()
        proof = self.run_to_completed(pid)
        q = proof["handoff"]["new"]["watermark_target_lsn"]
        r = self.ctl.commit("analytics", "target", "app_slot_replica",
                            lsn=q + 5, epoch=2)
        self.assertTrue(r["advanced"])
        r = self.ctl.commit("analytics", "target", "app_slot_replica",
                            lsn=q + 2, epoch=2)  # regression attempt
        self.assertFalse(r["advanced"])
        self.assertTrue(r["duplicate"])
        self.assertEqual(self.ctl.get_group("analytics")["watermark"], q + 5)
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.commit("analytics", "target", "app_slot_replica",
                            lsn=q + 9, epoch=3)  # epoch from the future
        self.assertEqual(ctx.exception.code, "FUTURE_EPOCH")

    def test_exactly_one_active_watermark_throughout(self):
        pid = self.make_plan()
        states = []

        def observe():
            g = self.ctl.get_group("analytics")
            states.append((g["cluster"], g["slot"], g["epoch"]))

        observe()
        self.ctl.start(pid); observe()
        self.ctl.verify(pid); observe()
        self.ctl.cutover(pid); observe()
        # every observation has exactly one active slot; epochs are monotonic
        epochs = [s[2] for s in states]
        self.assertEqual(epochs, sorted(epochs))
        self.assertEqual(states[-1], ("target", "app_slot_replica", 2))
        # double cutover impossible
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.cutover(pid)
        self.assertEqual(ctx.exception.code, "INVALID_STATE")

    def test_consumer_handoff_is_exact_no_gap_no_overlap(self):
        """The new slot resumes exactly where the old watermark stopped."""
        pid = self.make_plan()
        self.ctl.start(pid)
        lsns = [self.source.write("orders", f"k{i}", {"v": i}) for i in range(10)]
        self.ctl.catch_up(pid)
        p_old = lsns[4]  # consumer has processed up to the 5th new event
        self.ctl.commit("analytics", "source", "app_slot", lsn=p_old, epoch=1)
        # noise on a non-plan table + more plan writes
        self.source.write("inventory", "sku-1", {"qty": 9})
        more = [self.source.write("orders", f"k{i}", {"v": i * 2})
                for i in range(10)]
        self.ctl.verify(pid)
        proof = self.ctl.cutover(pid)
        q = proof["handoff"]["new"]["watermark_target_lsn"]
        self.assertEqual(proof["handoff"]["mapping_anchor"],
                         {"source_lsn": p_old, "target_lsn": q})

        # consumer resumes on the new slot from the remapped watermark
        entries = self.target.read_slot("app_slot_replica", after_lsn=q)
        got = sorted(e.origin_lsn for e in entries
                     if e.kind == "row" and e.table == "orders")
        expected = sorted(e.lsn for e in self.source.scan(p_old)
                          if e.kind == "row" and e.table == "orders")
        self.assertEqual(got, expected)          # no gap
        self.assertEqual(len(got), len(set(got)))  # no duplicates

    def test_commit_during_migration_still_works(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        lsn = self.source.write("orders", "x", {"v": 1})
        r = self.ctl.commit("analytics", "source", "app_slot", lsn=lsn, epoch=1)
        self.assertTrue(r["advanced"])
        self.ctl.verify(pid)
        self.ctl.cutover(pid)


class PlanLifecycle(Base):
    def test_frozen_spec_immutable_and_conflicts(self):
        pid = self.make_plan()
        # a second plan over the same slot/group/target is refused
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.create_plan({
                "source_cluster": "source", "source_slot": "app_slot",
                "target_cluster": "target", "target_slot": "other_slot",
                "tables": ["orders"], "consumer_group": "g2"})
        self.assertEqual(ctx.exception.code, "CONFLICT")
        self.ctl.abort(pid)
        # after abort the slot is free again
        pid2 = self.make_plan(group="analytics")
        self.assertNotEqual(pid, pid2)

    def test_abort_keeps_consumers_untouched(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        self.ctl.abort(pid)
        self.assertEqual(self.ctl.get_plan(pid)["state"], "ABORTED")
        r = self.ctl.commit("analytics", "source", "app_slot", lsn=3, epoch=1)
        self.assertTrue(r["advanced"])
        with self.assertRaises(MigrationError):
            self.ctl.start(pid)

    def test_fence_token_and_release(self):
        pid = self.make_plan()
        self.run_to_completed(pid)
        # post-cutover the source tables stay fenced (no split-brain writes)
        with self.assertRaises(MigrationError) as ctx:
            self.source.write("orders", "post", {})
        self.assertEqual(ctx.exception.code, "WRITES_FENCED")
        # unrelated tables are unaffected
        self.source.write("inventory", "sku", {"qty": 1})
        # operator acknowledges the write-path move
        self.ctl.release_source(pid)
        self.source.write("orders", "post", {})  # now accepted

    def test_proof_tampering_detected(self):
        pid = self.make_plan()
        proof = self.run_to_completed(pid)
        self.assertTrue(verify_proof(proof, self.store)["valid"])
        tampered = dict(proof)
        tampered["handoff"] = dict(proof["handoff"],
                                   new=dict(proof["handoff"]["new"], epoch=99))
        result = verify_proof(tampered, self.store)
        self.assertFalse(result["valid"])
        failed = {c["check"] for c in result["checks"] if not c["ok"]}
        self.assertIn("hash_integrity", failed)

    def test_reconcile_report_shape(self):
        pid = self.make_plan()
        self.run_to_completed(pid)
        report = self.ctl.reconcile(pid)
        self.assertEqual(report["plan"]["state"], "COMPLETED")
        self.assertEqual(report["verification"]["status"], "passed")
        self.assertIsNotNone(report["proof"]["proof_hash"])
        positions = report["positions"]
        self.assertLessEqual(positions["snapshot_lsn"],
                             positions["cutover_lsn"])
        self.assertEqual(positions["lag_events"], 0)
        for t in report["tables"]:
            self.assertTrue(t["equal_now"])


class HandoffWithdrawal(Base):
    """Observation window + operator withdrawal of a completed handoff."""

    def _clock_ctl(self, window_ms):
        """A second controller over the same clusters/store with a controllable
        clock and a short observation window."""
        t = [datetime(2026, 10, 5, 12, 0, 0, tzinfo=timezone.utc)]

        def clock():
            return t[0]

        def advance(seconds):
            t[0] += timedelta(seconds=seconds)

        ctl = MigrationController(
            {"source": self.source, "target": self.target}, self.store,
            observation_window_ms=window_ms, clock=clock)
        return ctl, advance

    def test_takeover_cursors_saved_at_cutover(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        lsn = self.source.write("orders", "k0", {"v": 0})
        self.ctl.catch_up(pid)
        self.ctl.commit("analytics", "source", "app_slot", lsn=lsn, epoch=1)
        self.ctl.verify(pid)
        proof = self.ctl.cutover(pid)
        q = proof["handoff"]["new"]["watermark_target_lsn"]

        plan = self.ctl.get_plan(pid)
        self.assertEqual(plan["state"], "COMPLETED")
        obs = plan["observation"]
        self.assertIsNotNone(obs["deadline"])
        self.assertEqual(obs["takeover_source_cursor"], lsn)
        self.assertEqual(obs["takeover_cursor"], q)
        self.assertEqual(obs["source_released_at"], None)

    def test_withdraw_restores_ownership_epoch_and_cursor_one_credential(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        lsn = self.source.write("orders", "k0", {"v": 0})
        self.ctl.catch_up(pid)
        self.ctl.commit("analytics", "source", "app_slot", lsn=lsn, epoch=1)
        self.ctl.verify(pid)
        proof = self.ctl.cutover(pid)
        q = proof["handoff"]["new"]["watermark_target_lsn"]
        self.assertEqual(self.ctl.get_group("analytics")["epoch"], 2)

        rev = self.ctl.withdraw_handoff(pid)

        # group restored: original slot, epoch bumped AGAIN, old cursor kept
        g = self.ctl.get_group("analytics")
        self.assertEqual((g["cluster"], g["slot"]), ("source", "app_slot"))
        self.assertEqual(g["epoch"], 3)
        self.assertEqual(g["watermark"], lsn)          # cursor retained

        plan = self.ctl.get_plan(pid)
        self.assertEqual(plan["state"], "WITHDRAWN")
        self.assertEqual(plan["observation"]["revocation_id"],
                         rev["revocation_id"])

        # cluster-level: source slot reads again & writes flow, target retired
        self.assertEqual(self.source.get_slot("app_slot")["status"], "ACTIVE")
        self.assertEqual(self.target.get_slot("app_slot_replica")["status"],
                         "RETIRED")
        self.source.write("orders", "after-withdraw", {"v": 9})
        with self.assertRaises(MigrationError) as ctx:
            self.target.read_slot("app_slot_replica", q)
        self.assertEqual(ctx.exception.code, "SLOT_NOT_ACTIVE")

        # late takeover-side ack is fenced off at epoch 2
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.commit("analytics", "target", "app_slot_replica",
                            lsn=q + 1, epoch=2)
        self.assertEqual(ctx.exception.code, "STALE_EPOCH")

        # the credential independently verifies
        verdict = verify_revocation(rev, self.store)
        self.assertTrue(verdict["valid"], verdict)

        # original consumer resumes on the restored slot without gap/reset
        r = self.ctl.commit("analytics", "source", "app_slot",
                            lsn=lsn + 2, epoch=3)
        self.assertTrue(r["advanced"])

    def test_withdraw_is_idempotent_returns_existing_credential(self):
        pid = self.make_plan()
        self.run_to_completed(pid)
        rev1 = self.ctl.withdraw_handoff(pid)
        rev2 = self.ctl.withdraw_handoff(pid)   # same request again
        self.assertEqual(rev1["revocation_id"], rev2["revocation_id"])
        self.assertEqual(rev1["revocation_hash"], rev2["revocation_hash"])
        self.assertEqual(self.ctl.get_group("analytics")["epoch"], 3)
        kinds = [e["kind"] for e in self.store.get_events(pid)]
        self.assertEqual(kinds.count("HANDOFF_REVOKED"), 1)

    def test_blocked_after_takeover_confirms_new_record_state_untouched(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        lsns = [self.source.write("orders", f"k{i}", {"v": i})
                for i in range(6)]
        self.ctl.catch_up(pid)
        self.ctl.commit("analytics", "source", "app_slot",
                        lsn=lsns[2], epoch=1)
        self.ctl.verify(pid)
        proof = self.ctl.cutover(pid)
        q = proof["handoff"]["new"]["watermark_target_lsn"]

        # takeover consumer confirms one NEW target record
        r = self.ctl.commit("analytics", "target", "app_slot_replica",
                            lsn=q + 1, epoch=2)
        self.assertTrue(r["advanced"])
        target_slot_cursor = self.target.get_slot(
            "app_slot_replica")["confirmed_lsn"]

        with self.assertRaises(MigrationError) as ctx:
            self.ctl.withdraw_handoff(pid)
        self.assertEqual(ctx.exception.code, "TAKEOVER_CONFIRMED")
        codes = {b["code"] for b in ctx.exception.details["blocking_conditions"]}
        self.assertIn("TAKEOVER_CONFIRMED", codes)

        # everything stays as it was
        g = self.ctl.get_group("analytics")
        self.assertEqual((g["cluster"], g["slot"]),
                         ("target", "app_slot_replica"))
        self.assertEqual(g["epoch"], 2)
        self.assertEqual(self.ctl.get_plan(pid)["state"], "COMPLETED")
        self.assertEqual(self.target.get_slot("app_slot_replica")["status"],
                         "ACTIVE")
        self.assertEqual(
            self.target.get_slot("app_slot_replica")["confirmed_lsn"],
            target_slot_cursor)
        self.assertIsNone(self.store.get_revocation_for_plan(pid))

    def test_duplicate_ack_at_cursor_does_not_block_withdraw(self):
        # an idempotent commit exactly at the saved takeover cursor is not a
        # "new confirmation" — withdrawal stays available
        pid = self.make_plan()
        self.run_to_completed(pid)
        q = self.ctl.get_group("analytics")["watermark"]
        dup = self.ctl.commit("analytics", "target", "app_slot_replica",
                              lsn=q, epoch=2)
        self.assertTrue(dup["duplicate"])
        self.assertTrue(self.ctl.withdrawal_status(pid)["eligible"])
        rev = self.ctl.withdraw_handoff(pid)
        self.assertIn("revocation_hash", rev)

    def test_blocked_after_source_released_state_untouched(self):
        pid = self.make_plan()
        self.run_to_completed(pid)
        self.ctl.release_source(pid)
        self.source.write("orders", "new-write", {})

        with self.assertRaises(MigrationError) as ctx:
            self.ctl.withdraw_handoff(pid)
        self.assertEqual(ctx.exception.code, "SOURCE_PROTECTION_RELEASED")

        g = self.ctl.get_group("analytics")
        self.assertEqual((g["cluster"], g["slot"]),
                         ("target", "app_slot_replica"))
        self.assertEqual(g["epoch"], 2)
        self.assertEqual(self.ctl.get_plan(pid)["state"], "COMPLETED")
        self.assertIsNone(self.store.get_revocation_for_plan(pid))

        # releasing twice is idempotent and cannot re-arm withdrawal
        self.ctl.release_source(pid)
        with self.assertRaises(MigrationError):
            self.ctl.withdraw_handoff(pid)

    def test_blocked_after_observation_window_expires(self):
        ctl, advance = self._clock_ctl(window_ms=10_000)
        pid = self.make_plan(ctl=ctl)
        ctl.start(pid)
        ctl.catch_up(pid)
        ctl.verify(pid)
        ctl.cutover(pid)

        st = ctl.withdrawal_status(pid)
        self.assertTrue(st["eligible"])
        self.assertEqual(st["phase"], "OBSERVATION")
        self.assertGreater(st["remaining_ms"], 9_000)

        advance(11)  # window (10s) is over
        st = ctl.withdrawal_status(pid)
        self.assertFalse(st["eligible"])
        self.assertTrue(st["expired"])
        self.assertEqual(st["blocking_conditions"][0]["code"],
                         "OBSERVATION_EXPIRED")
        with self.assertRaises(MigrationError) as ctx:
            ctl.withdraw_handoff(pid)
        self.assertEqual(ctx.exception.code, "OBSERVATION_EXPIRED")
        # runtime untouched
        self.assertEqual(ctl.get_group("analytics")["epoch"], 2)
        self.assertEqual(ctl.get_plan(pid)["state"], "COMPLETED")
        # source still fenced even though the window elapsed
        self.assertTrue(self.source.is_fenced("orders"))

    def test_withdraw_just_inside_window_succeeds(self):
        ctl, advance = self._clock_ctl(window_ms=10_000)
        pid = self.make_plan(ctl=ctl)
        ctl.start(pid)
        ctl.catch_up(pid)
        ctl.verify(pid)
        ctl.cutover(pid)
        advance(10)  # exactly at the deadline — still open (<)
        rev = ctl.withdraw_handoff(pid)
        self.assertIn("revocation_hash", rev)
        self.assertEqual(ctl.get_plan(pid)["state"], "WITHDRAWN")

    def test_withdraw_before_cutover_rejected(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.withdraw_handoff(pid)
        self.assertEqual(ctx.exception.code, "NO_COMPLETED_HANDOFF")

    def test_credential_tampering_detected(self):
        pid = self.make_plan()
        self.run_to_completed(pid)
        rev = self.ctl.withdraw_handoff(pid)
        self.assertTrue(verify_revocation(rev, self.store)["valid"])
        tampered = dict(rev)
        tampered["restoration"] = dict(rev["restoration"], epoch=99)
        verdict = verify_revocation(tampered, self.store)
        self.assertFalse(verdict["valid"])
        self.assertIn("hash_integrity",
                      {c["check"] for c in verdict["checks"] if not c["ok"]})

    def test_takeover_slot_cursor_retained_after_withdraw(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        lsns = [self.source.write("orders", f"k{i}", {"v": i})
                for i in range(5)]
        self.ctl.catch_up(pid)
        self.ctl.commit("analytics", "source", "app_slot",
                        lsn=lsns[1], epoch=1)
        self.ctl.verify(pid)
        proof = self.ctl.cutover(pid)
        q = proof["handoff"]["new"]["watermark_target_lsn"]
        self.assertEqual(
            self.target.get_slot("app_slot_replica")["confirmed_lsn"], q)
        self.ctl.withdraw_handoff(pid)
        # takeover cursor preserved on the retired slot (not reset)
        self.assertEqual(
            self.target.get_slot("app_slot_replica")["confirmed_lsn"], q)

    def test_withdraw_then_new_plan_possible_and_consumer_continuous(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        lsn = self.source.write("orders", "k0", {"v": 0})
        self.ctl.catch_up(pid)
        self.ctl.commit("analytics", "source", "app_slot", lsn=lsn, epoch=1)
        self.ctl.verify(pid)
        self.ctl.cutover(pid)
        self.ctl.withdraw_handoff(pid)

        # a fresh plan over the now-free slot/group is allowed; first the
        # operator discards the (never-confirmed) replica state, as after a
        # failed cutover
        self.target._tables["orders"] = {}
        self.target._running["orders"] = 0
        pid2 = self.ctl.create_plan({
            "source_cluster": "source", "source_slot": "app_slot",
            "target_cluster": "target", "target_slot": "app_slot_replica2",
            "tables": ["orders"], "consumer_group": "analytics"})["plan_id"]
        self.ctl.freeze(pid2)
        self.ctl.start(pid2)
        self.ctl.catch_up(pid2)
        self.ctl.verify(pid2)
        p2 = self.ctl.cutover(pid2)
        self.assertEqual(p2["attempt"], 1)
        g = self.ctl.get_group("analytics")
        self.assertEqual((g["cluster"], g["slot"]),
                         ("target", "app_slot_replica2"))
        self.assertEqual(g["epoch"], 4)   # 1 → cutover → 2 → withdraw → 3 → 4

    def test_reconcile_shows_eligibility_deadline_blockers_and_credential(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        lsn = self.source.write("orders", "k0", {"v": 0})
        self.ctl.catch_up(pid)
        self.ctl.commit("analytics", "source", "app_slot", lsn=lsn, epoch=1)
        self.ctl.verify(pid)
        self.ctl.cutover(pid)

        rep = self.ctl.reconcile(pid)
        w = rep["withdrawal"]
        self.assertEqual(w["phase"], "OBSERVATION")
        self.assertTrue(w["eligible"])
        self.assertIsNotNone(w["deadline"])
        self.assertEqual(w["blocking_conditions"], [])
        self.assertFalse(w["takeover_confirmed_new_records"])
        self.assertTrue(w["source_protected"])

        page = render_html(self.ctl.reconcile(pid))
        self.assertIn("交接观察期与撤回", page)
        self.assertIn("可撤回", page)
        self.assertIn("截止时间", page)
        self.assertIn("接管方开服确认游标", page)

        # after a takeover confirmation the report lists the blocker
        q = w["takeover_cursor"]
        self.ctl.commit("analytics", "target", "app_slot_replica",
                        lsn=q + 1, epoch=2)
        rep = self.ctl.reconcile(pid)
        self.assertFalse(rep["withdrawal"]["eligible"])
        self.assertEqual(rep["withdrawal"]["blocking_conditions"][0]["code"],
                         "TAKEOVER_CONFIRMED")
        page = render_html(self.ctl.reconcile(pid))
        self.assertIn("接管方已确认新记录", page)
        self.assertIn("不可撤回", page)

    def test_reconcile_after_withdraw_shows_credential_and_event(self):
        pid = self.make_plan()
        self.run_to_completed(pid)
        rev = self.ctl.withdraw_handoff(pid)
        rep = self.ctl.reconcile(pid)
        self.assertEqual(rep["withdrawal"]["phase"], "WITHDRAWN")
        self.assertEqual(rep["withdrawal"]["revocation"]["revocation_id"],
                         rev["revocation_id"])
        kinds = [e["kind"] for e in rep["timeline"]]
        self.assertIn("HANDOFF_REVOKED", kinds)
        page = render_html(self.ctl.reconcile(pid))
        self.assertIn("交接已撤回", page)
        self.assertIn(rev["revocation_hash"], page)

    def test_configurable_window_per_plan(self):
        pid = self.ctl.create_plan({
            "source_cluster": "source", "source_slot": "app_slot",
            "target_cluster": "target", "target_slot": "app_slot_replica",
            "tables": ["orders"], "consumer_group": "analytics",
            "observation_window_ms": 1234})["plan_id"]
        self.ctl.freeze(pid)
        self.ctl.start(pid)
        self.ctl.verify(pid)
        self.ctl.cutover(pid)
        self.assertEqual(self.ctl.get_plan(pid)["observation"]["window_ms"],
                         1234)

    def test_withdraw_keeps_consuming_after_restore_exact_no_gap(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        lsns = [self.source.write("orders", f"k{i}", {"v": i})
                for i in range(8)]
        self.ctl.catch_up(pid)
        p_old = lsns[3]
        self.ctl.commit("analytics", "source", "app_slot",
                        lsn=p_old, epoch=1)
        self.ctl.verify(pid)
        proof = self.ctl.cutover(pid)
        self.assertEqual(proof["handoff"]["new"]["watermark_target_lsn"],
                         proof["handoff"]["mapping_anchor"]["target_lsn"])
        self.ctl.withdraw_handoff(pid)

        # source keeps accepting writes; consumer continues from p_old
        more = [self.source.write("orders", f"m{i}", {"v": i})
                for i in range(4)]
        entries = self.source.read_slot("app_slot", after_lsn=p_old)
        got = [e.lsn for e in entries if e.kind == "row" and e.table == "orders"]
        self.assertEqual(got, sorted(lsns[4:] + more))  # no gap, no dup

    def test_release_source_refused_after_withdraw(self):
        pid = self.make_plan()
        self.run_to_completed(pid)
        self.ctl.withdraw_handoff(pid)
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.release_source(pid)
        self.assertEqual(ctx.exception.code, "INVALID_STATE")


class WithdrawalApi(Base):
    def test_http_withdraw_flow(self):
        import json
        import urllib.request
        import urllib.error
        from slotmig.api import create_server

        pid = self.make_plan()
        self.run_to_completed(pid)

        server = create_server(self.ctl,
                               {"source": self.source, "target": self.target},
                               "127.0.0.1", 0)
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            def call(method, path, payload=None):
                data = json.dumps(payload).encode() if payload is not None else None
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}{path}", data=data, method=method,
                    headers={"Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(req) as resp:
                        return resp.status, json.loads(resp.read().decode())
                except urllib.error.HTTPError as exc:
                    return exc.code, json.loads(exc.read().decode())

            status, st = call("GET", f"/plans/{pid}/withdrawal")
            self.assertEqual(status, 200)
            self.assertTrue(st["eligible"])
            self.assertEqual(st["blocking_conditions"], [])

            status, rev = call("POST", f"/plans/{pid}/withdraw")
            self.assertEqual(status, 200)
            self.assertEqual(rev["kind"], "handoff_revocation")
            rid = rev["revocation_id"]

            # repeat request → same existing result
            status, rev2 = call("POST", f"/plans/{pid}/withdraw")
            self.assertEqual(status, 200)
            self.assertEqual(rev2["revocation_id"], rid)

            # credential fetch + independent verification endpoints
            status, fetched = call("GET", f"/plans/{pid}/revocation")
            self.assertEqual(status, 200)
            self.assertEqual(fetched["revocation_hash"], rev["revocation_hash"])
            status, verdict = call("POST", "/revocations/verify",
                                   {"revocation": fetched})
            self.assertEqual(status, 200)
            self.assertTrue(verdict["valid"], verdict)

            # reconcile HTML mentions the credential
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/plans/{pid}/reconcile.html")
            with urllib.request.urlopen(req) as resp:
                page = resp.read().decode()
            self.assertIn("交接已撤回", page)
            self.assertIn(rev["revocation_hash"], page)
        finally:
            server.shutdown()
            server.server_close()


class ApiSmoke(Base):
    def test_http_api_end_to_end(self):
        import json
        import urllib.request
        from slotmig.api import create_server

        server = create_server(self.ctl,
                               {"source": self.source, "target": self.target},
                               "127.0.0.1", 0)
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            def call(method, path, payload=None):
                data = json.dumps(payload).encode() if payload is not None else None
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}{path}", data=data, method=method,
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req) as resp:
                    raw = resp.read().decode()
                    if resp.headers["Content-Type"].startswith("application/json"):
                        return resp.status, json.loads(raw)
                    return resp.status, raw

            status, plan = call("POST", "/plans", {
                "source_cluster": "source", "source_slot": "app_slot",
                "target_cluster": "target", "target_slot": "app_slot_replica",
                "tables": ["orders"], "consumer_group": "analytics"})
            self.assertEqual(status, 201)
            pid = plan["plan_id"]
            for step in ("freeze", "start", "catch-up", "verify"):
                status, _ = call("POST", f"/plans/{pid}/{step}")
                self.assertEqual(status, 200, step)
            status, proof = call("POST", f"/plans/{pid}/cutover")
            self.assertEqual(status, 200)
            self.assertIn("proof_hash", proof)

            status, report = call("GET", f"/plans/{pid}/reconcile")
            self.assertEqual(report["plan"]["state"], "COMPLETED")
            status, page = call("GET", f"/plans/{pid}/reconcile.html")
            self.assertEqual(status, 200)
            self.assertIn("位点对账", page)
            self.assertIn("切换证明", page)

            status, fetched = call("GET", f"/plans/{pid}/proof")
            self.assertEqual(fetched["proof_hash"], proof["proof_hash"])
            status, verdict = call("POST", "/proofs/verify", {"proof": fetched})
            self.assertTrue(verdict["valid"], verdict)

            # late ack over HTTP → 409
            from urllib.error import HTTPError
            with self.assertRaises(HTTPError) as ctx:
                call("POST", "/groups/analytics/commit",
                     {"cluster": "source", "slot": "app_slot",
                      "lsn": 1, "epoch": 1})
            self.assertEqual(ctx.exception.code, 409)
            body = json.loads(ctx.exception.read().decode())
            self.assertEqual(body["error"], "STALE_EPOCH")
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
