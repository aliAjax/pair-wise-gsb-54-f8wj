import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict, NotFound, PermissionDenied, ValidationError


CREATE_DATA = {'cable': 'SEA-1', 'segment': 'S3', 'start_km': 120.0, 'end_km': 135.0, 'depth_m': 1800.0, 'sea_state': 3, 'vessel_available': True, 'spare_length_km': 20.0, 'permit_valid': True, 'capacity_gbps': 400}
FLOW = [('approve', 'repair_manager', {'repair_manager': 'RM-2'}, 'approved'), ('mobilize', 'vessel_master', {'weather_window_hours': 40, 'available_spare_km': 18, 'vessel_name': 'CS-1'}, 'mobilized'), ('survey', 'cable_engineer', {'survey_complete': True, 'fault_location_km': 128}, 'surveyed'), ('splice', 'cable_engineer', {'splice_loss_db': 0.12, 'spare_used_km': 16}, 'spliced'), ('test', 'noc_operator', {'end_to_end_loss_db': 0.3}, 'tested'), ('restore', 'noc_operator', {'traffic_restored': True, 'restore_capacity_gbps': 400}, 'restored')]
W_START = "2026-10-04T08:00:00+00:00"
W_END = "2026-10-04T20:00:00+00:00"
NOC = Actor("noc-1", "noc_operator")
DISP1 = Actor("disp-1", "dispatcher")
DISP2 = Actor("disp-2", "dispatcher")


def record_data(segment):
    data = dict(CREATE_DATA)
    data["segment"] = segment
    return data


class DispatchTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "test.db"))
        self.repository = self.service.repository

    def tearDown(self):
        self.temp.cleanup()

    def make_record(self, reference, segment):
        return self.service.create(NOC, reference, record_data(segment))

    def enqueue(self, record, impact, actor=DISP1, **extra):
        payload = {"record_id": record["id"], "impact_score": impact, "window_start": W_START, "window_end": W_END}
        payload.update(extra)
        return self.service.dispatch.enqueue(actor, payload)

    def entry_of(self, record):
        return self.service.dispatch.entry_for_record(record["id"])

    def resource(self, view, name):
        return next(item for item in view["resources"] if item["name"] == name)

    def test_enqueue_allocates_and_record_detail_traces_source(self):
        first = self.make_record("CABLE-40001", "S1")
        second = self.make_record("CABLE-40002", "S2")
        result1 = self.enqueue(first, 80, operation_id="enq-1")
        self.assertEqual(result1["entry"]["position"], 1)
        self.assertEqual(result1["entry"]["vessel"], "CS-1")
        self.assertEqual(result1["entry"]["crew"], "TEAM-A")
        self.assertEqual(result1["entry"]["spare_reserved_km"], 15.75)
        self.assertEqual(result1["entry"]["delta"], {"vessel": 0, "crew": 0, "spare_km": 0})
        result2 = self.enqueue(second, 60)
        self.assertEqual(result2["entry"]["vessel"], "CS-2")
        view = self.service.dispatch.queue_view(DISP1)
        self.assertEqual(view["version"], result2["queue_version"])
        self.assertEqual(self.resource(view, "DEPOT-MAIN")["remaining"], 18.5)
        self.assertEqual(self.resource(view, "CS-1")["remaining"], 0)
        detail = self.service.get_record(NOC, first["id"])
        self.assertEqual(detail["dispatch"]["operation_id"], "enq-1")
        self.assertEqual(detail["dispatch"]["position"], 1)
        replay = self.enqueue(first, 80, operation_id="enq-1")
        self.assertTrue(replay["replayed"])
        self.assertEqual(len(self.service.dispatch.queue_view(DISP1)["entries"]), 2)
        with self.assertRaises(Conflict):
            self.enqueue(first, 80, operation_id="enq-2")

    def test_trial_then_confirm_reorder_and_audit_trace(self):
        records = [self.make_record("CABLE-4000%s" % index, "S%s" % index) for index in (1, 2, 3)]
        entries = [self.enqueue(record, impact)["entry"] for record, impact in zip(records, (50, 90, 70))]
        trial = self.service.dispatch.reorder(DISP1, {"mode": "trial", "auto": True, "reason": "高影响插队"})
        self.assertEqual(trial["mode"], "trial")
        self.assertEqual([item["entry_id"] for item in trial["entries"]], [entries[1]["id"], entries[2]["id"], entries[0]["id"]])
        view = self.service.dispatch.queue_view(DISP1)
        self.assertEqual(view["version"], trial["queue_version"])
        self.assertEqual([entry["position"] for entry in view["entries"]], [1, 2, 3])
        confirmed = self.service.dispatch.reorder(DISP1, {"mode": "confirm", "auto": True, "operation_id": "re-1", "expected_version": trial["queue_version"], "reason": "高影响插队"})
        self.assertFalse(confirmed["replayed"])
        self.assertEqual(confirmed["queue_version"], trial["queue_version"] + 1)
        view = self.service.dispatch.queue_view(DISP1)
        positions = {entry["id"]: entry["position"] for entry in view["entries"]}
        self.assertEqual(positions[entries[1]["id"]], 1)
        self.assertEqual(positions[entries[0]["id"]], 3)
        timeline = self.service.timeline(NOC, records[1]["id"])
        reorder_events = [event for event in timeline if event["action"] == "dispatch_reorder"]
        self.assertEqual(len(reorder_events), 1)
        self.assertEqual(reorder_events[0]["details"]["operation_id"], "re-1")
        self.assertEqual(reorder_events[0]["details"]["queue_version"], confirmed["queue_version"])
        self.assertEqual(reorder_events[0]["details"]["old_position"], 2)
        self.assertEqual(reorder_events[0]["details"]["new_position"], 1)
        events = self.service.dispatch.events(DISP1)
        self.assertEqual(events[0]["event"], "reorder")
        self.assertEqual(events[0]["operation_id"], "re-1")
        detail = self.service.get_record(NOC, records[0]["id"])
        self.assertEqual(detail["dispatch"]["operation_id"], "re-1")
        self.assertEqual(detail["dispatch"]["queue_version"], confirmed["queue_version"])

    def test_shortfall_delta_is_recorded_on_entry(self):
        records = [self.make_record("CABLE-4000%s" % index, "S%s" % index) for index in (1, 2, 3)]
        entries = [self.enqueue(record, impact, spare_need_km=20)["entry"] for record, impact in zip(records, (50, 10, 90))]
        self.assertEqual(entries[2]["spare_reserved_km"], 10)
        self.assertEqual(entries[2]["delta"]["spare_km"], 10)
        version = self.service.dispatch.queue_view(DISP1)["version"]
        self.service.dispatch.reorder(DISP1, {"mode": "confirm", "auto": True, "operation_id": "re-spare", "expected_version": version})
        view = self.service.dispatch.queue_view(DISP1)
        by_id = {entry["id"]: entry for entry in view["entries"]}
        self.assertEqual(by_id[entries[2]["id"]]["delta"]["spare_km"], 0)
        self.assertEqual(by_id[entries[2]["id"]]["spare_reserved_km"], 20)
        self.assertEqual(by_id[entries[1]["id"]]["delta"]["spare_km"], 10)
        self.assertEqual(self.resource(view, "DEPOT-MAIN")["used"], 50)

    def test_departed_entry_keeps_original_allocation(self):
        records = [self.make_record("CABLE-4000%s" % index, "S%s" % index) for index in (1, 2, 3)]
        entries = [self.enqueue(record, impact)["entry"] for record, impact in zip(records, (50, 60, 70))]
        self.assertIsNone(entries[2]["vessel"])
        departed = self.service.dispatch.depart(DISP1, entries[0]["id"])
        self.assertTrue(departed["changed"])
        again = self.service.dispatch.depart(DISP1, entries[0]["id"])
        self.assertFalse(again["changed"])
        version = departed["queue_version"]
        confirmed = self.service.dispatch.reorder(DISP1, {"mode": "confirm", "order": [entries[2]["id"], entries[1]["id"]], "operation_id": "re-depart", "expected_version": version})
        self.assertEqual(confirmed["departed"][0]["entry_id"], entries[0]["id"])
        self.assertEqual(confirmed["departed"][0]["vessel"], "CS-1")
        view = self.service.dispatch.queue_view(DISP1)
        by_id = {entry["id"]: entry for entry in view["entries"]}
        self.assertEqual(by_id[entries[0]["id"]]["status"], "departed")
        self.assertEqual(by_id[entries[0]["id"]]["vessel"], "CS-1")
        self.assertEqual(by_id[entries[0]["id"]]["crew"], "TEAM-A")
        self.assertEqual(by_id[entries[0]["id"]]["position"], 1)
        self.assertEqual(by_id[entries[0]["id"]]["queue_version"], version)
        self.assertEqual(by_id[entries[2]["id"]]["vessel"], "CS-2")
        self.assertIsNone(by_id[entries[1]["id"]]["vessel"])
        self.assertEqual(by_id[entries[1]["id"]]["delta"]["vessel"], 1)

    def test_concurrent_confirm_second_becomes_candidate(self):
        records = [self.make_record("CABLE-4000%s" % index, "S%s" % index) for index in (1, 2)]
        entries = [self.enqueue(record, impact)["entry"] for record, impact in zip(records, (50, 90))]
        version = self.service.dispatch.queue_view(DISP1)["version"]
        order_a = [entries[1]["id"], entries[0]["id"]]
        order_b = [entries[0]["id"], entries[1]["id"]]
        trial_a = self.service.dispatch.reorder(DISP1, {"mode": "trial", "order": order_a})
        trial_b = self.service.dispatch.reorder(DISP2, {"mode": "trial", "order": order_b})
        self.assertEqual(trial_a["queue_version"], trial_b["queue_version"])
        self.service.dispatch.reorder(DISP1, {"mode": "confirm", "order": order_a, "operation_id": "op-a", "expected_version": version})
        with self.assertRaises(Conflict) as caught:
            self.service.dispatch.reorder(DISP2, {"mode": "confirm", "order": order_b, "operation_id": "op-b", "expected_version": version})
        self.assertIn("candidate_id", caught.exception.details)
        self.assertEqual(caught.exception.details["current_version"], version + 1)
        candidates = self.service.dispatch.candidates(DISP1)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["operation_id"], "op-b")
        self.assertEqual(candidates[0]["status"], "candidate")
        self.assertEqual(candidates[0]["payload"]["order"], order_b)
        view = self.service.dispatch.queue_view(DISP1)
        positions = {entry["id"]: entry["position"] for entry in view["entries"]}
        self.assertEqual(positions[entries[1]["id"]], 1)
        self.assertEqual(self.resource(view, "DEPOT-MAIN")["used"], 31.5)
        retried = self.service.dispatch.reorder(DISP2, {"mode": "confirm", "order": order_b, "operation_id": "op-b", "expected_version": version + 1})
        self.assertFalse(retried["replayed"])
        view = self.service.dispatch.queue_view(DISP1)
        positions = {entry["id"]: entry["position"] for entry in view["entries"]}
        self.assertEqual(positions[entries[0]["id"]], 1)
        self.assertEqual(self.resource(view, "DEPOT-MAIN")["used"], 31.5)

    def test_replay_same_operation_does_not_double_occupy(self):
        records = [self.make_record("CABLE-4000%s" % index, "S%s" % index) for index in (1, 2, 3)]
        for record, impact in zip(records, (50, 60, 70)):
            self.enqueue(record, impact, spare_need_km=20)
        version = self.service.dispatch.queue_view(DISP1)["version"]
        confirmed = self.service.dispatch.reorder(DISP1, {"mode": "confirm", "auto": True, "operation_id": "op-x", "expected_version": version})
        replayed = self.service.dispatch.reorder(DISP1, {"mode": "confirm", "auto": True, "operation_id": "op-x", "expected_version": version})
        self.assertTrue(replayed["replayed"])
        self.assertEqual(replayed["queue_version"], confirmed["queue_version"])
        view = self.service.dispatch.queue_view(DISP1)
        self.assertEqual(view["version"], confirmed["queue_version"])
        self.assertEqual(self.resource(view, "DEPOT-MAIN")["used"], 50)

    def test_write_failure_recovers_from_last_complete_snapshot(self):
        records = [self.make_record("CABLE-4000%s" % index, "S%s" % index) for index in (1, 2)]
        entries = [self.enqueue(record, impact)["entry"] for record, impact in zip(records, (50, 90))]
        version = self.service.dispatch.queue_view(DISP1)["version"]

        def boom(*args, **kwargs):
            raise RuntimeError("模拟写入失败")

        self.repository._reorder_apply = boom
        with self.assertRaises(RuntimeError):
            self.service.dispatch.reorder(DISP1, {"mode": "confirm", "order": [entries[1]["id"], entries[0]["id"]], "operation_id": "op-fail", "expected_version": version})
        del self.repository._reorder_apply
        self.assertEqual(self.repository.dispatch_meta()["dirty"], 1)
        third = self.make_record("CABLE-40003", "S3")
        with self.assertRaises(Conflict):
            self.enqueue(third, 70)
        recovered = self.service.dispatch.recover(DISP1)
        self.assertFalse(recovered["dirty"])
        self.assertEqual(recovered["queue_version"], version)
        self.assertEqual(recovered["restored_entries"], 2)
        with self.repository._connect() as connection:
            connection.execute("DELETE FROM dispatch_entries WHERE id=?", (entries[1]["id"],))
        self.assertEqual(len(self.service.dispatch.queue_view(DISP1)["entries"]), 1)
        self.service.dispatch.recover(DISP1)
        self.assertEqual(len(self.service.dispatch.queue_view(DISP1)["entries"]), 2)
        confirmed = self.service.dispatch.reorder(DISP1, {"mode": "confirm", "order": [entries[1]["id"], entries[0]["id"]], "operation_id": "op-ok", "expected_version": version})
        self.assertEqual(confirmed["queue_version"], version + 1)

    def test_record_actions_drive_depart_and_release(self):
        record = self.make_record("CABLE-40001", "S1")
        self.enqueue(record, 80)
        for action, role, data, expected_state in FLOW:
            record = self.service.act(Actor("operator", role), record["id"], record["version"], action, data)
            if action == "mobilize":
                self.assertEqual(self.entry_of(record)["status"], "departed")
        entry = self.entry_of(record)
        self.assertEqual(entry["status"], "completed")
        view = self.service.dispatch.queue_view(DISP1)
        self.assertEqual(self.resource(view, "CS-1")["remaining"], 1)
        self.assertEqual(self.resource(view, "DEPOT-MAIN")["remaining"], 50)
        actions = [event["action"] for event in self.service.timeline(NOC, record["id"])]
        self.assertIn("dispatch_enqueue", actions)
        self.assertIn("dispatch_depart", actions)
        self.assertIn("dispatch_release", actions)

    def test_permissions(self):
        record = self.make_record("CABLE-40001", "S1")
        with self.assertRaises(PermissionDenied):
            self.enqueue(record, 80, actor=NOC)
        with self.assertRaises(PermissionDenied):
            self.service.dispatch.reorder(Actor("rm-1", "repair_manager"), {"mode": "trial", "auto": True})
        with self.assertRaises(PermissionDenied):
            self.service.dispatch.queue_view(Actor("outsider", "outsider"))
        self.assertIn("entries", self.service.dispatch.queue_view(NOC))

    def test_validation(self):
        record = self.make_record("CABLE-40001", "S1")
        with self.assertRaises(ValidationError):
            self.enqueue(record, 80, window_start=W_END, window_end=W_START)
        with self.assertRaises(ValidationError):
            self.service.dispatch.enqueue(DISP1, {"record_id": "x", "impact_score": 1, "window_start": W_START, "window_end": W_END})
        with self.assertRaises(NotFound):
            self.service.dispatch.enqueue(DISP1, {"record_id": 999, "impact_score": 1, "window_start": W_START, "window_end": W_END})
        self.enqueue(record, 80)
        with self.assertRaises(ValidationError):
            self.service.dispatch.reorder(DISP1, {"mode": "trial", "order": []})
        with self.assertRaises(ValidationError):
            self.service.dispatch.reorder(DISP1, {"mode": "confirm", "auto": True, "expected_version": 1})
        for action, role, data, expected_state in FLOW:
            record = self.service.act(Actor("operator", role), record["id"], record["version"], action, data)
        with self.assertRaises(ValidationError):
            self.enqueue(record, 80)


if __name__ == "__main__":
    unittest.main()
