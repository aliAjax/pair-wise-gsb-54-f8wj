import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict, PermissionDenied, ValidationError


DISPATCHER = Actor("dispatch-1", "dispatcher")
CREATOR = Actor("creator", "noc_operator")
MANAGER = Actor("manager", "repair_manager")

FAULT_A = {'cable': 'SEA-1', 'segment': 'S1', 'start_km': 10.0, 'end_km': 25.0, 'depth_m': 1500.0, 'sea_state': 3, 'vessel_available': True, 'spare_length_km': 30.0, 'permit_valid': True, 'capacity_gbps': 400}
FAULT_B = {'cable': 'SEA-1', 'segment': 'S2', 'start_km': 40.0, 'end_km': 52.0, 'depth_m': 1200.0, 'sea_state': 2, 'vessel_available': True, 'spare_length_km': 30.0, 'permit_valid': True, 'capacity_gbps': 200}
FAULT_C = {'cable': 'SEA-2', 'segment': 'S1', 'start_km': 5.0, 'end_km': 14.0, 'depth_m': 900.0, 'sea_state': 2, 'vessel_available': True, 'spare_length_km': 30.0, 'permit_valid': True, 'capacity_gbps': 800}
WINDOW = {"window_start": "2026-10-05T00:00:00Z", "window_end": "2026-10-06T00:00:00Z"}


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp.name) / "test.db")
        self.service = build_service(self.db_path)
        self.record_a = self.service.create(CREATOR, "CABLE-A", FAULT_A)
        self.record_b = self.service.create(CREATOR, "CABLE-B", FAULT_B)
        self.record_c = self.service.create(CREATOR, "CABLE-C", FAULT_C)

    def tearDown(self):
        self.temp.cleanup()

    def enqueue(self, record, key, confirm=True, **overrides):
        data = {"record_id": record["id"], "idempotency_key": key, "confirm": confirm}
        data.update(WINDOW)
        data.update(overrides)
        return self.service.enqueue(DISPATCHER, data)

    def queue_items(self):
        return {item["record_id"]: item for item in self.service.get_queue(DISPATCHER)["items"]}

    def test_trial_shortfall_recorded_on_item(self):
        self.service.upsert_resource(DISPATCHER, {"kind": "spare_cable", "name": "MAIN", "capacity": 10})
        result = self.enqueue(self.record_a, "k-a", confirm=False)
        item = result["item"]
        self.assertEqual(item["status"], "trial")
        self.assertEqual(item["allocation"]["spare_km"], 10.0)
        self.assertEqual(item["shortfall"]["spare_km"], 5.75)
        resources = self.service.get_queue(DISPATCHER)["resources"]
        self.assertEqual(resources["spare_cable"]["remaining"], 10.0)

    def test_confirm_occupies_and_replay_not_double(self):
        created = self.enqueue(self.record_a, "k-a")
        self.assertTrue(created["created"])
        self.assertEqual(created["item"]["status"], "committed")
        resources = self.service.get_queue(DISPATCHER)["resources"]
        self.assertEqual(resources["spare_cable"]["used"], 15.75)
        self.assertFalse(resources["vessels"][0]["available"])
        replay = self.enqueue(self.record_a, "k-a")
        self.assertFalse(replay["created"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["item"]["id"], created["item"]["id"])
        after = self.service.get_queue(DISPATCHER)["resources"]
        self.assertEqual(after["spare_cable"]["used"], 15.75)
        with self.assertRaises(Conflict):
            self.enqueue(self.record_b, "k-a")

    def test_trial_then_confirm(self):
        trial = self.enqueue(self.record_a, "k-a", confirm=False)
        self.assertEqual(trial["item"]["status"], "trial")
        confirmed = self.service.confirm_item(DISPATCHER, trial["item"]["id"])
        self.assertEqual(confirmed["item"]["status"], "committed")
        with self.assertRaises(Conflict):
            self.service.confirm_item(DISPATCHER, trial["item"]["id"])

    def test_reorder_reallocates_non_departed(self):
        item_a = self.enqueue(self.record_a, "k-a")["item"]
        item_b = self.enqueue(self.record_b, "k-b")["item"]
        item_c = self.enqueue(self.record_c, "k-c")["item"]
        self.assertEqual(item_a["allocation"]["vessel"], "CS-1")
        self.assertEqual(item_c["shortfall"]["vessel"], True)
        version = self.service.get_queue(DISPATCHER)["version"]
        result = self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_c["id"], item_b["id"], item_a["id"]], "reorder_id": "r-1", "note": "高影响故障插队"})
        self.assertEqual(result["result"], "committed")
        items = self.queue_items()
        self.assertEqual(items[self.record_c["id"]]["allocation"]["vessel"], "CS-1")
        self.assertEqual(items[self.record_c["id"]]["position"], 1)
        self.assertIsNone(items[self.record_a["id"]]["allocation"]["vessel"])
        self.assertTrue(items[self.record_a["id"]]["shortfall"]["vessel"])
        self.assertEqual(items[self.record_a["id"]]["basis_version"], result["queue_version"])

    def test_departed_item_keeps_original_basis(self):
        item_a = self.enqueue(self.record_a, "k-a")["item"]
        self.service.depart_item(DISPATCHER, item_a["id"])
        item_b = self.enqueue(self.record_b, "k-b")["item"]
        item_c = self.enqueue(self.record_c, "k-c")["item"]
        before = self.queue_items()[self.record_a["id"]]
        version = self.service.get_queue(DISPATCHER)["version"]
        self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_c["id"], item_b["id"], item_a["id"]], "reorder_id": "r-2"})
        items = self.queue_items()
        after = items[self.record_a["id"]]
        self.assertEqual(after["allocation"], before["allocation"])
        self.assertEqual(after["basis_version"], before["basis_version"])
        self.assertEqual(items[self.record_c["id"]]["allocation"]["vessel"], "CS-2")
        self.assertTrue(items[self.record_b["id"]]["shortfall"]["vessel"])
        timeline = self.service.timeline(CREATOR, self.record_a["id"])
        reorder_events = [e for e in timeline if e["action"] == "queue_reorder"]
        self.assertEqual(len(reorder_events), 1)
        self.assertTrue(reorder_events[0]["details"]["kept_basis"])

    def test_concurrent_reorder_second_becomes_candidate(self):
        item_a = self.enqueue(self.record_a, "k-a")["item"]
        item_b = self.enqueue(self.record_b, "k-b")["item"]
        version = self.service.get_queue(DISPATCHER)["version"]
        first = self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_b["id"], item_a["id"]], "reorder_id": "r-1"})
        self.assertEqual(first["result"], "committed")
        second = self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_a["id"], item_b["id"]], "reorder_id": "r-2"})
        self.assertEqual(second["result"], "candidate")
        self.assertEqual(second["queue_version"], first["queue_version"])
        items = self.queue_items()
        self.assertEqual(items[self.record_b["id"]]["position"], 1)
        self.assertEqual(items[self.record_b["id"]]["allocation"]["vessel"], "CS-1")
        resources = self.service.get_queue(DISPATCHER)["resources"]
        self.assertEqual(resources["spare_cable"]["used"], 28.35)
        candidates = self.service.list_candidates(DISPATCHER)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["status"], "pending")
        self.assertEqual(candidates[0]["proposed_order"], [item_a["id"], item_b["id"]])
        applied = self.service.apply_candidate(DISPATCHER, candidates[0]["id"])
        self.assertEqual(applied["result"], "committed")
        self.assertEqual(self.queue_items()[self.record_a["id"]]["position"], 1)
        with self.assertRaises(Conflict):
            self.service.apply_candidate(DISPATCHER, candidates[0]["id"])

    def test_candidate_discard(self):
        item_a = self.enqueue(self.record_a, "k-a")["item"]
        item_b = self.enqueue(self.record_b, "k-b")["item"]
        version = self.service.get_queue(DISPATCHER)["version"]
        self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_b["id"], item_a["id"]], "reorder_id": "r-1"})
        stale = self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_a["id"], item_b["id"]], "reorder_id": "r-2"})
        discarded = self.service.discard_candidate(DISPATCHER, stale["candidate_id"])
        self.assertEqual(discarded["candidate"]["status"], "discarded")
        with self.assertRaises(Conflict):
            self.service.apply_candidate(DISPATCHER, stale["candidate_id"])

    def test_reorder_replay_not_double_apply(self):
        item_a = self.enqueue(self.record_a, "k-a")["item"]
        item_b = self.enqueue(self.record_b, "k-b")["item"]
        version = self.service.get_queue(DISPATCHER)["version"]
        first = self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_b["id"], item_a["id"]], "reorder_id": "r-1"})
        self.assertEqual(first["result"], "committed")
        replay = self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_b["id"], item_a["id"]], "reorder_id": "r-1"})
        self.assertEqual(replay["result"], "replayed")
        self.assertEqual(replay["queue_version"], first["queue_version"])
        self.assertEqual(self.service.get_queue(DISPATCHER)["version"], first["queue_version"])
        items = self.queue_items()
        self.assertEqual(items[self.record_b["id"]]["allocation"]["vessel"], "CS-1")

    def test_recover_from_latest_snapshot(self):
        self.enqueue(self.record_a, "k-a")
        self.enqueue(self.record_b, "k-b")
        version = self.service.get_queue(DISPATCHER)["version"]
        connection = sqlite3.connect(self.db_path)
        connection.execute("DELETE FROM queue_items")
        connection.execute("UPDATE queue_meta SET dirty=1 WHERE id=1")
        connection.commit()
        connection.close()
        rebuilt = build_service(self.db_path)
        queue = rebuilt.get_queue(DISPATCHER)
        self.assertEqual(queue["version"], version)
        self.assertEqual(len(queue["items"]), 2)
        connection = sqlite3.connect(self.db_path)
        connection.execute("DELETE FROM queue_items")
        connection.commit()
        connection.close()
        recovered = rebuilt.recover_queue(DISPATCHER)
        self.assertTrue(recovered["recovered"])
        self.assertEqual(len(rebuilt.get_queue(DISPATCHER)["items"]), 2)

    def test_audit_trail_and_record_detail(self):
        item_a = self.enqueue(self.record_a, "k-a")["item"]
        item_b = self.enqueue(self.record_b, "k-b")["item"]
        version = self.service.get_queue(DISPATCHER)["version"]
        self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_b["id"], item_a["id"]], "reorder_id": "r-9", "note": "插队"})
        timeline = self.service.timeline(CREATOR, self.record_a["id"])
        reorder_events = [e for e in timeline if e["action"] == "queue_reorder"]
        self.assertEqual(len(reorder_events), 1)
        details = reorder_events[0]["details"]
        self.assertEqual(details["reorder_id"], "r-9")
        self.assertEqual(details["queue_version"], self.service.get_queue(DISPATCHER)["version"])
        self.assertIn("allocation_before", details)
        self.assertIn("allocation_after", details)
        record = self.service.get_record(CREATOR, self.record_a["id"])
        self.assertIsNotNone(record["queue"])
        self.assertEqual(record["queue"]["id"], item_a["id"])

    def test_depart_requires_full_allocation(self):
        self.enqueue(self.record_a, "k-a")
        self.enqueue(self.record_b, "k-b")
        item_c = self.enqueue(self.record_c, "k-c")["item"]
        self.assertTrue(item_c["shortfall"]["vessel"])
        with self.assertRaises(Conflict):
            self.service.depart_item(DISPATCHER, item_c["id"])
        item_a = self.queue_items()[self.record_a["id"]]
        departed = self.service.depart_item(DISPATCHER, item_a["id"])
        self.assertEqual(departed["item"]["status"], "departed")

    def test_auto_release_on_cancel(self):
        self.enqueue(self.record_a, "k-a")
        self.assertEqual(self.service.get_queue(DISPATCHER)["resources"]["spare_cable"]["used"], 15.75)
        self.service.act(MANAGER, self.record_a["id"], self.record_a["version"], "cancel", {"cancel_reason": "误报"})
        self.assertEqual(self.service.get_queue(DISPATCHER)["resources"]["spare_cable"]["used"], 0.0)
        record = self.service.get_record(CREATOR, self.record_a["id"])
        self.assertIsNone(record["queue"])

    def test_queue_permission(self):
        with self.assertRaises(PermissionDenied):
            self.service.enqueue(CREATOR, {"record_id": self.record_a["id"], "idempotency_key": "k-x", **WINDOW})
        queue = self.service.get_queue(CREATOR)
        self.assertEqual(queue["items"], [])

    def test_dry_run_has_no_side_effects(self):
        item_a = self.enqueue(self.record_a, "k-a")["item"]
        item_b = self.enqueue(self.record_b, "k-b")["item"]
        version = self.service.get_queue(DISPATCHER)["version"]
        preview = self.service.reorder(DISPATCHER, {"expected_version": version, "order": [item_b["id"], item_a["id"]], "dry_run": True})
        self.assertEqual(preview["result"], "dry_run")
        self.assertEqual(preview["changes"][item_b["id"]]["position"], 1)
        self.assertEqual(self.service.get_queue(DISPATCHER)["version"], version)
        self.assertEqual(self.queue_items()[self.record_a["id"]]["position"], 1)

    def test_suggest_order_by_impact(self):
        item_a = self.enqueue(self.record_a, "k-a", confirm=False, impact_score=100)["item"]
        item_b = self.enqueue(self.record_b, "k-b", confirm=False, impact_score=900)["item"]
        item_c = self.enqueue(self.record_c, "k-c", confirm=False, impact_score=500)["item"]
        suggestion = self.service.suggest_order(DISPATCHER)
        self.assertEqual(suggestion["order"], [item_b["id"], item_c["id"], item_a["id"]])

    def test_window_validation(self):
        with self.assertRaises(ValidationError):
            self.enqueue(self.record_a, "k-a", window_start="2026-10-06T00:00:00Z", window_end="2026-10-05T00:00:00Z")

    def test_duplicate_record_in_queue(self):
        self.enqueue(self.record_a, "k-a")
        with self.assertRaises(Conflict):
            self.enqueue(self.record_a, "k-a2")


if __name__ == "__main__":
    unittest.main()
