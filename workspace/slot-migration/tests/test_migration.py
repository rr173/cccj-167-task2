"""End-to-end and edge-case tests for the migration controller.

Covers the requirements:
  * consistent snapshot → incremental stream → verify → atomic cutover
  * source keeps writing during migration        → no lost events
  * schema change mid-migration                  → plan aborts, consumers safe
  * migration retry                              → no duplicate advancement
  * late acks from old consumers                 → fenced, watermark intact
  * never two valid consumption watermarks
  * post-cutover observation window: revoke the handover atomically,
    retain cursors, verifiable revocation credential, idempotency, and the
    three rejection conditions that must leave every runtime state untouched
"""
import datetime
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


class FakeClock:
    """Deterministic UTC clock used to cross the observation deadline."""

    def __init__(self, dt=None):
        self.t = dt or datetime.datetime(2026, 10, 6,
                                         tzinfo=datetime.timezone.utc)

    def __call__(self):
        return self.t.isoformat(timespec="milliseconds")

    def advance(self, seconds):
        self.t += datetime.timedelta(seconds=seconds)


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
    def make_plan(self, tables=("orders",), group="analytics"):
        plan = self.ctl.create_plan({
            "source_cluster": "source", "source_slot": "app_slot",
            "target_cluster": "target", "target_slot": "app_slot_replica",
            "tables": list(tables), "consumer_group": group,
        })
        self.ctl.freeze(plan["plan_id"])
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


class ObservationRevocation(Base):
    OBS = 60

    def setUp(self):
        super().setUp()
        self.clock = FakeClock()  # fresh clock per test
        self.ctl = MigrationController(
            {"source": self.source, "target": self.target}, self.store,
            clock=self.clock)

    def _cutover_with_commit(self, lsn=30):
        pid = self.make_plan()
        self.run_to_verified(pid)
        self.ctl.commit("analytics", "source", "app_slot", lsn=lsn, epoch=1)
        proof = self.ctl.cutover(pid)
        return pid, proof

    # ----------------------------------------------------------- happy paths
    def make_plan(self, tables=("orders",), group="analytics"):
        plan = self.ctl.create_plan({
            "source_cluster": "source", "source_slot": "app_slot",
            "target_cluster": "target", "target_slot": "app_slot_replica",
            "tables": list(tables), "consumer_group": group,
            "observation_seconds": self.OBS,
        })
        self.ctl.freeze(plan["plan_id"])
        return plan["plan_id"]

    def test_cutover_opens_configurable_observation_and_saves_cursor(self):
        # a plan with a non-default observation window
        plan = self.ctl.create_plan({
            "source_cluster": "source", "source_slot": "app_slot",
            "target_cluster": "target", "target_slot": "app_slot_replica",
            "tables": ["orders"], "consumer_group": "windowed",
            "observation_seconds": 42})
        self.assertEqual(plan["observation_seconds"], 42)
        self.ctl.freeze(plan["plan_id"])
        self.ctl.start(plan["plan_id"])
        self.ctl.verify(plan["plan_id"])
        self.ctl.commit("windowed", "source", "app_slot", lsn=10, epoch=1)
        proof = self.ctl.cutover(plan["plan_id"])

        p = self.ctl.get_plan(plan["plan_id"])
        self.assertEqual(p["state"], "COMPLETED")
        self.assertIsNotNone(p["cutover_at"])
        q = proof["handoff"]["new"]["watermark_target_lsn"]
        self.assertEqual(p["takeover_cursor"], {
            "cluster": "target", "slot": "app_slot_replica",
            "epoch": 2, "confirmed_lsn": q, "group_watermark": q,
            "saved_at": p["cutover_at"]})

        obs = self.ctl.observation(plan["plan_id"])
        self.assertEqual(obs["status"], "open")
        self.assertTrue(obs["eligible"])
        self.assertEqual(obs["window_seconds"], 42)
        self.assertEqual(obs["remaining_seconds"], 42.0)
        self.assertEqual(obs["blocking_conditions"], [])

    def test_invalid_observation_seconds_rejected(self):
        for bad in (0, -5, 1.5, True, "60"):
            with self.assertRaises(MigrationError) as ctx:
                self.ctl.create_plan({
                    "source_cluster": "source", "source_slot": "app_slot",
                    "target_cluster": "target",
                    "target_slot": f"slot_{str(bad).replace('.', '_')}",
                    "tables": ["orders"], "consumer_group": f"g_{id(bad)}",
                    "observation_seconds": bad})
            self.assertEqual(ctx.exception.code, "BAD_REQUEST")

    def test_revoke_restores_ownership_increments_epoch_keeps_cursor(self):
        pid, proof = self._cutover_with_commit(lsn=30)
        q = proof["handoff"]["new"]["watermark_target_lsn"]
        self.assertEqual(
            (self.ctl.get_group("analytics")["cluster"],
             self.ctl.get_group("analytics")["slot"],
             self.ctl.get_group("analytics")["epoch"]),
            ("target", "app_slot_replica", 2))

        credential = self.ctl.revoke_handoff(pid)
        self.assertEqual(credential["kind"], "REVOCATION")
        self.assertTrue(credential["revocation_hash"])

        group = self.ctl.get_group("analytics")
        self.assertEqual((group["cluster"], group["slot"]),
                         ("source", "app_slot"))
        self.assertEqual(group["epoch"], 3)          # fencing epoch bumped again
        self.assertEqual(group["watermark"], 30)     # source cursor retained

        # source slot is reactivated with the retained cursor, target retired
        self.assertEqual(self.source.get_slot("app_slot")["status"], "ACTIVE")
        self.assertEqual(
            self.source.get_slot("app_slot")["confirmed_lsn"], 30)
        self.assertEqual(self.target.get_slot("app_slot_replica")["status"],
                         "RETIRED")
        # source tables are back to normal (original service resumed)
        self.assertEqual(self.source.fenced_tables(), [])
        new_lsn = self.source.write("orders", "post-revoke", {})
        self.assertGreater(new_lsn, proof["final_check"]["cutover_lsn"])

        # plan + timeline
        p = self.ctl.get_plan(pid)
        self.assertEqual(p["state"], "REVOKED")
        self.assertEqual(p["revocation_id"], credential["revocation_id"])
        kinds = [e["kind"] for e in self.store.get_events(pid)]
        self.assertEqual(kinds.count("CUTOVER_EXECUTED"), 1)
        self.assertEqual(kinds.count("HANDOVER_REVOKED"), 1)

    def test_resumed_consumer_exact_no_gap_no_overlap(self):
        pid, proof = self._cutover_with_commit(lsn=30)
        self.ctl.revoke_handoff(pid)
        # the original slot resumes exactly after the retained source cursor
        entries = self.source.read_slot("app_slot", after_lsn=30)
        lsns = sorted(e.lsn for e in entries
                      if e.kind == "row" and e.table == "orders")
        expected = sorted(e.lsn for e in self.source.scan(30)
                          if e.kind == "row" and e.table == "orders")
        self.assertEqual(lsns, expected)
        self.assertEqual(len(lsns), len(set(lsns)))
        # and commits on the restored slot work with the bumped epoch
        if lsns:
            r = self.ctl.commit("analytics", "source", "app_slot",
                                lsn=lsns[0], epoch=3)
            self.assertTrue(r["advanced"])

    def test_stale_epoch_commits_rejected_after_revoke(self):
        pid, _ = self._cutover_with_commit(lsn=30)
        self.ctl.revoke_handoff(pid)
        # takeover-side epoch (2) is now fenced by the revocation epoch (3)
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.commit("analytics", "target", "app_slot_replica",
                            lsn=999, epoch=2)
        self.assertEqual(ctx.exception.code, "STALE_EPOCH")
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.commit("analytics", "source", "app_slot",
                            lsn=999, epoch=2)
        self.assertEqual(ctx.exception.code, "STALE_EPOCH")
        self.assertEqual(self.ctl.get_group("analytics")["watermark"], 30)

    def test_repeated_revoke_returns_existing_credential_unchanged(self):
        pid, _ = self._cutover_with_commit()
        first = self.ctl.revoke_handoff(pid)
        self.clock.advance(10)  # even later in time…
        second = self.ctl.revoke_handoff(pid)
        self.assertEqual(second, first)
        # still independently verifiable
        self.assertTrue(verify_revocation(second, self.store)["valid"])
        # exactly one revocation event / credential
        kinds = [e["kind"] for e in self.store.get_events(pid)]
        self.assertEqual(kinds.count("HANDOVER_REVOKED"), 1)

    # -------------------------------------------------------------- rejection
    def assert_state_unchanged_after_failed_revoke(self, pid, *, group_tuple,
                                                   source_fenced=True):
        self.assertEqual(self.ctl.get_plan(pid)["state"], "COMPLETED")
        g = self.ctl.get_group("analytics")
        self.assertEqual((g["cluster"], g["slot"], g["epoch"], g["watermark"]),
                         group_tuple)
        self.assertEqual(self.source.get_slot("app_slot")["status"],
                         "RETIRED")
        self.assertEqual(self.target.get_slot("app_slot_replica")["status"],
                         "ACTIVE")
        if source_fenced:
            self.assertIn("orders", self.source.fenced_tables())
        kinds = [e["kind"] for e in self.store.get_events(pid)]
        self.assertNotIn("HANDOVER_REVOKED", kinds)

    def test_reject_when_takeover_confirmed_new_record(self):
        pid, proof = self._cutover_with_commit(lsn=30)
        q = self.ctl.get_group("analytics")["watermark"]
        # takeover consumer confirms a NEW record on the new slot/epoch
        self.ctl.commit("analytics", "target", "app_slot_replica",
                        lsn=q + 1, epoch=2)
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.revoke_handoff(pid)
        self.assertEqual(ctx.exception.code, "TAKEOVER_PROGRESSED")
        self.assertTrue(ctx.exception.details["blocking_conditions"])
        # "state unchanged" means unchanged by the rejected revoke itself:
        # the post-commit watermark (q+1) is still there.
        self.assert_state_unchanged_after_failed_revoke(
            pid, group_tuple=("target", "app_slot_replica", 2, q + 1))
        # the bar is sticky: even after window elapses the reason wins
        self.clock.advance(self.OBS + 1)
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.revoke_handoff(pid)
        self.assertEqual(ctx.exception.code, "TAKEOVER_PROGRESSED")

    def test_reject_when_source_unprotected(self):
        pid, _ = self._cutover_with_commit()
        group_tuple = ("target", "app_slot_replica", 2,
                       self.ctl.get_group("analytics")["watermark"])
        self.ctl.release_source(pid)
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.revoke_handoff(pid)
        self.assertEqual(ctx.exception.code, "SOURCE_NOT_PROTECTED")
        self.assert_state_unchanged_after_failed_revoke(
            pid, group_tuple=group_tuple, source_fenced=False)
        # source fence stays lifted (we must not re-fence on rejection)
        self.assertEqual(self.source.fenced_tables(), [])

    def test_reject_when_observation_window_ended(self):
        pid, _ = self._cutover_with_commit()
        group_tuple = ("target", "app_slot_replica", 2,
                       self.ctl.get_group("analytics")["watermark"])
        self.clock.advance(self.OBS + 1)
        obs = self.ctl.observation(pid)
        self.assertEqual(obs["status"], "expired")
        self.assertFalse(obs["eligible"])
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.revoke_handoff(pid)
        self.assertEqual(ctx.exception.code, "OBSERVATION_EXPIRED")
        self.assertIn("deadline", ctx.exception.details)
        self.assert_state_unchanged_after_failed_revoke(
            pid, group_tuple=group_tuple)
        # …one second before the deadline the revoke is still allowed.
        # Use a fully fresh stack (its own clusters/store/clock) so neither
        # the advanced clock nor the retired slot interferes.
        fresh_store = Store(":memory:")
        fresh_src, fresh_tgt = Cluster("source"), Cluster("target")
        fresh_src.create_slot("app_slot")
        fresh_clock = FakeClock()
        fresh_ctl = MigrationController(
            {"source": fresh_src, "target": fresh_tgt}, fresh_store,
            clock=fresh_clock)
        for i in range(50):
            fresh_src.write("orders", f"o{i:03d}", {"amount": i})
        fplan = fresh_ctl.create_plan({
            "source_cluster": "source", "source_slot": "app_slot",
            "target_cluster": "target", "target_slot": "app_slot_replica",
            "tables": ["orders"], "consumer_group": "analytics",
            "observation_seconds": self.OBS})
        fpid = fplan["plan_id"]
        fresh_ctl.freeze(fpid); fresh_ctl.start(fpid)
        fresh_ctl.verify(fpid); fresh_ctl.cutover(fpid)
        fresh_clock.advance(self.OBS - 1)
        self.assertTrue(fresh_ctl.observation(fpid)["eligible"])
        cred = fresh_ctl.revoke_handoff(fpid)
        self.assertEqual(fresh_ctl.get_plan(fpid)["state"], "REVOKED")
        self.assertTrue(cred["revocation_hash"])
        fresh_store.close()

    def test_revoke_only_allowed_on_completed_plan(self):
        pid = self.make_plan()
        self.ctl.start(pid)
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.revoke_handoff(pid)
        self.assertEqual(ctx.exception.code, "INVALID_STATE")
        with self.assertRaises(MigrationError):
            self.ctl.observation(pid)

    def test_release_source_refused_after_revoke(self):
        pid, _ = self._cutover_with_commit()
        self.ctl.revoke_handoff(pid)
        with self.assertRaises(MigrationError) as ctx:
            self.ctl.release_source(pid)
        self.assertEqual(ctx.exception.code, "INVALID_STATE")

    # ------------------------------------------------------------- credential
    def test_revocation_credential_verifies_and_tamper_evident(self):
        pid, cutover_proof = self._cutover_with_commit(lsn=30)
        cred = self.ctl.revoke_handoff(pid)
        result = verify_revocation(cred, self.store)
        self.assertTrue(result["valid"], result)
        names = {c["check"] for c in result["checks"]}
        self.assertEqual(names, {
            "hash_integrity", "kind", "epoch_step", "cursor_retained",
            "cutover_ref_present", "within_observation_window",
            "no_takeover_confirmation", "source_protected", "plan_revoked",
            "revocation_link", "group_restored", "stored_hash_match",
            "cutover_proof_hash_match", "retained_cursor_matches_cutover"})

        # tamper with any field → hash integrity fails
        tampered = dict(cred)
        tampered["handoff_back"] = dict(
            cred["handoff_back"],
            restored=dict(cred["handoff_back"]["restored"], epoch=99))
        bad = verify_revocation(tampered, self.store)
        self.assertFalse(bad["valid"])
        self.assertIn("hash_integrity",
                      {c["check"] for c in bad["checks"] if not c["ok"]})

    def test_cutover_proof_remains_available_after_revoke(self):
        pid, cutover_proof = self._cutover_with_commit()
        self.ctl.revoke_handoff(pid)
        fetched_cutover = self.ctl.get_proof(pid)
        self.assertEqual(fetched_cutover["proof_id"], cutover_proof["proof_id"])
        fetched_revocation = self.ctl.get_revocation(pid)
        self.assertEqual(fetched_revocation["cutover_ref"]["proof_id"],
                         cutover_proof["proof_id"])
        # the cutover proof is a historical artifact; the revocation credential
        # is the tamper-evident record of current truth
        self.assertEqual(fetched_revocation["kind"], "REVOCATION")
        # verify_proof dispatches by kind
        verdict = self.ctl.verify_proof(fetched_revocation)
        self.assertTrue(verdict["valid"])

    # ------------------------------------------------------------ reconcile
    def test_reconcile_shows_observation_eligibility_and_blockers(self):
        pid, _ = self._cutover_with_commit()
        report = self.ctl.reconcile(pid)
        obs = report["observation"]
        self.assertIsNone(report["revocation"])
        self.assertTrue(obs["eligible"])
        self.assertEqual(obs["status"], "open")
        self.assertIn("takeover_cursor", obs)
        from slotmig.common import iso_add_seconds
        self.assertEqual(
            obs["deadline"],
            iso_add_seconds(obs["cutover_at"], obs["window_seconds"]))

        # blocker: takeover progressed
        q = self.ctl.get_group("analytics")["watermark"]
        self.ctl.commit("analytics", "target", "app_slot_replica",
                        lsn=q + 2, epoch=2)
        report = self.ctl.reconcile(pid)
        codes = {b["code"] for b in report["observation"]["blocking_conditions"]}
        self.assertIn("TAKEOVER_PROGRESSED", codes)
        self.assertFalse(report["observation"]["eligible"])

        page = render_html_for(self.ctl, pid)
        self.assertIn("交接观察期与撤回资格", page)
        self.assertIn("TAKEOVER_PROGRESSED", page)

    def test_reconcile_after_revoke_shows_revoked_and_credential(self):
        pid, _ = self._cutover_with_commit()
        cred = self.ctl.revoke_handoff(pid)
        report = self.ctl.reconcile(pid)
        self.assertEqual(report["observation"]["status"], "revoked")
        self.assertEqual(report["revocation"]["revocation_id"],
                         cred["revocation_id"])
        self.assertEqual(report["plan"]["state"], "REVOKED")
        page = render_html_for(self.ctl, pid)
        self.assertIn("交接已撤回", page)
        self.assertIn(cred["revocation_id"], page)
        self.assertIn("HANDOVER_REVOKED", page)
        self.assertIn("CUTOVER_EXECUTED", page)

    def test_new_plan_possible_after_revoke(self):
        pid, _ = self._cutover_with_commit()
        self.ctl.revoke_handoff(pid)
        # the slot/group is free again for a fresh migration of the original
        # data: a new plan streams fresh source writes onto the target.
        self.source.write("orders", "fresh", {"v": 1})
        pid2 = self.make_plan(group="analytics")
        # first attempt refuses a non-empty target; as the operator migrating
        # again, clear the replica tables first
        self.target.bulk_load("cleanup", {}, 0, truncate=True)
        self.target._tables["orders"] = {}
        self.target._running["orders"] = 0
        self.ctl.start(pid2)
        self.ctl.verify(pid2)
        proof = self.ctl.cutover(pid2)
        self.assertEqual(proof["attempt"], 1)
        group = self.ctl.get_group("analytics")
        self.assertEqual((group["cluster"], group["slot"]),
                         ("target", "app_slot_replica"))
        self.assertEqual(group["epoch"], 4)  # 1 → 2 cutover → 3 revoke → 4

    # ------------------------------------------------------------------ HTTP
    def test_http_revoke_and_rejection(self):
        import json
        import urllib.request
        from urllib.error import HTTPError
        from slotmig.api import create_server

        server = create_server(self.ctl,
                               {"source": self.source, "target": self.target},
                               "127.0.0.1", 0)
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            def call(method, path, payload=None):
                data = json.dumps(payload).encode() if payload is not None \
                    else None
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}{path}", data=data, method=method,
                    headers={"Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(req) as resp:
                        return resp.status, json.loads(resp.read().decode())
                except HTTPError as exc:
                    return exc.code, json.loads(exc.read().decode())

            pid, _ = self._cutover_with_commit(lsn=20)
            status, report = call("GET", f"/plans/{pid}/reconcile")
            self.assertEqual(status, 200)
            self.assertTrue(report["observation"]["eligible"])

            status, cred = call("POST", f"/plans/{pid}/revoke")
            self.assertEqual(status, 200)
            self.assertEqual(cred["kind"], "REVOCATION")
            # repeated request returns the same credential, same hash
            status, cred2 = call("POST", f"/plans/{pid}/revoke")
            self.assertEqual(status, 200)
            self.assertEqual(cred2["revocation_id"], cred["revocation_id"])
            self.assertEqual(cred2["revocation_hash"], cred["revocation_hash"])

            # fetch + verify via HTTP
            status, fetched = call("GET", f"/plans/{pid}/revocation")
            self.assertEqual(status, 200)
            status, verdict = call("POST", "/proofs/verify", {"proof": fetched})
            self.assertTrue(verdict["valid"], verdict)

            # HTML page shows the revoked state
            import urllib.request as ur
            with ur.urlopen(f"http://127.0.0.1:{port}/plans/{pid}/reconcile.html") as r:
                page = r.read().decode()
            self.assertIn("交接已撤回", page)

            # rejection path on a separate fresh plan over HTTP: takeover
            # confirms a new record → revoke refused with a stable code
            b_store = Store(":memory:")
            b_src, b_tgt = Cluster("source"), Cluster("target")
            b_src.create_slot("app_slot")
            b_ctl = MigrationController(
                {"source": b_src, "target": b_tgt}, b_store, clock=self.clock)
            for i in range(30):
                b_src.write("orders", f"o{i:02d}", {"n": i})
            server2 = create_server(b_ctl,
                                    {"source": b_src, "target": b_tgt},
                                    "127.0.0.1", 0)
            port2 = server2.server_address[1]
            threading.Thread(target=server2.serve_forever, daemon=True).start()
            try:
                def call2(method, path, payload=None):
                    data = json.dumps(payload).encode() if payload is not None \
                        else None
                    req = urllib.request.Request(
                        f"http://127.0.0.1:{port2}{path}", data=data,
                        method=method,
                        headers={"Content-Type": "application/json"})
                    try:
                        with urllib.request.urlopen(req) as resp:
                            return resp.status, json.loads(resp.read().decode())
                    except HTTPError as exc:
                        return exc.code, json.loads(exc.read().decode())

                _, bp = call2("POST", "/plans", {
                    "source_cluster": "source", "source_slot": "app_slot",
                    "target_cluster": "target", "target_slot": "app_slot_replica",
                    "tables": ["orders"], "consumer_group": "analytics",
                    "observation_seconds": self.OBS})
                bpid = bp["plan_id"]
                for step in ("freeze", "start", "catch-up", "verify"):
                    call2("POST", f"/plans/{bpid}/{step}")
                call2("POST", "/groups/analytics/commit",
                      {"cluster": "source", "slot": "app_slot",
                       "lsn": 5, "epoch": 1})
                call2("POST", f"/plans/{bpid}/cutover")
                # takeover advances on the new slot/epoch
                call2("POST", "/groups/analytics/commit",
                      {"cluster": "target", "slot": "app_slot_replica",
                       "lsn": 999, "epoch": 2})
                status, err = call2("POST", f"/plans/{bpid}/revoke")
                self.assertEqual(status, 409)
                self.assertEqual(err["error"], "TAKEOVER_PROGRESSED")
                self.assertTrue(err["details"]["blocking_conditions"])
                # state untouched
                self.assertEqual(
                    call2("GET", f"/plans/{bpid}")[1]["state"], "COMPLETED")
            finally:
                server2.shutdown()
                server2.server_close()
                b_store.close()
        finally:
            server.shutdown()
            server.server_close()

    # ---------------------------------------------------------- persistence
    def test_observation_state_persists_across_controller_reopen(self):
        import os
        import tempfile
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        tmp.close()
        path = tmp.name
        try:
            store = Store(path)
            src, tgt = Cluster("source"), Cluster("target")
            src.create_slot("app_slot")
            ctl = MigrationController({"source": src, "target": tgt}, store,
                                      clock=self.clock)
            for i in range(20):
                src.write("orders", f"o{i}", {"n": i})
            plan = ctl.create_plan({
                "source_cluster": "source", "source_slot": "app_slot",
                "target_cluster": "target", "target_slot": "app_slot_replica",
                "tables": ["orders"], "consumer_group": "analytics",
                "observation_seconds": self.OBS})
            pid = plan["plan_id"]
            ctl.freeze(pid); ctl.start(pid); ctl.verify(pid)
            ctl.commit("analytics", "source", "app_slot", lsn=10, epoch=1)
            ctl.cutover(pid)
            store.close()

            # reopen with a new Store against the same DB file
            store2 = Store(path)
            ctl2 = MigrationController({"source": src, "target": tgt}, store2,
                                       clock=self.clock)
            obs = ctl2.observation(pid)
            self.assertTrue(obs["eligible"])
            self.assertEqual(obs["takeover_cursor"]["group_watermark"],
                             ctl2.get_plan(pid)["takeover_cursor"]["group_watermark"])
            cred = ctl2.revoke_handoff(pid)
            self.assertTrue(verify_revocation(cred, store2)["valid"])
            store2.close()
        finally:
            os.unlink(path)


def render_html_for(ctl, pid):
    from slotmig.reconcile import render_html
    return render_html(ctl.reconcile(pid))


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
