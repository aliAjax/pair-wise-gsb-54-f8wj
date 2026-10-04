import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path

from app import BASE_DIR, build_service
from src.http_api import create_server


FAULT_A = {'cable': 'SEA-1', 'segment': 'S1', 'start_km': 10.0, 'end_km': 25.0, 'depth_m': 1500.0, 'sea_state': 3, 'vessel_available': True, 'spare_length_km': 30.0, 'permit_valid': True, 'capacity_gbps': 400}
FAULT_B = {'cable': 'SEA-1', 'segment': 'S2', 'start_km': 40.0, 'end_km': 52.0, 'depth_m': 1200.0, 'sea_state': 2, 'vessel_available': True, 'spare_length_km': 30.0, 'permit_valid': True, 'capacity_gbps': 200}
WINDOW = {"window_start": "2026-10-05T00:00:00Z", "window_end": "2026-10-06T00:00:00Z"}
HEADERS = {"X-User-Id": "dispatch-1", "X-Role": "dispatcher", "Content-Type": "application/json"}
NOC_HEADERS = {"X-User-Id": "noc-1", "X-Role": "noc_operator", "Content-Type": "application/json"}


class QueueHttpTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "test.db"))
        self.server = create_server("127.0.0.1", 0, self.service, BASE_DIR / "static")
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        for reference, payload in (("CABLE-A", FAULT_A), ("CABLE-B", FAULT_B)):
            status, _ = self.request("POST", "/api/records", {"reference": reference, "data": payload}, NOC_HEADERS)
            self.assertEqual(status, 201)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.temp.cleanup()

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        connection.request(method, path, json.dumps(body or {}), headers or HEADERS)
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
        connection.close()
        return response.status, payload

    def test_queue_flow_status_codes(self):
        enqueue_body = {"record_id": 1, "idempotency_key": "k-a", "confirm": True, **WINDOW}
        status, created = self.request("POST", "/api/queue/items", enqueue_body)
        self.assertEqual(status, 201)
        self.assertTrue(created["created"])
        status, replay = self.request("POST", "/api/queue/items", enqueue_body)
        self.assertEqual(status, 200)
        self.assertTrue(replay["replayed"])
        status, second = self.request("POST", "/api/queue/items", {"record_id": 2, "idempotency_key": "k-b", "confirm": True, **WINDOW})
        self.assertEqual(status, 201)
        status, queue = self.request("GET", "/api/queue")
        self.assertEqual(status, 200)
        self.assertEqual(len(queue["items"]), 2)
        version = queue["version"]
        order = [queue["items"][1]["id"], queue["items"][0]["id"]]
        status, committed = self.request("POST", "/api/queue/reorder", {"expected_version": version, "order": order, "reorder_id": "r-1"})
        self.assertEqual(status, 200)
        self.assertEqual(committed["result"], "committed")
        status, candidate = self.request("POST", "/api/queue/reorder", {"expected_version": version, "order": order, "reorder_id": "r-2"})
        self.assertEqual(status, 202)
        self.assertEqual(candidate["result"], "candidate")
        status, replayed = self.request("POST", "/api/queue/reorder", {"expected_version": version, "order": order, "reorder_id": "r-1"})
        self.assertEqual(status, 200)
        self.assertEqual(replayed["result"], "replayed")
        status, candidates = self.request("GET", "/api/queue/candidates")
        self.assertEqual(status, 200)
        self.assertEqual(len(candidates["items"]), 1)
        status, applied = self.request("POST", "/api/queue/candidates/%s/apply" % candidates["items"][0]["id"])
        self.assertEqual(status, 200)
        self.assertEqual(applied["result"], "committed")
        status, record = self.request("GET", "/api/records/1", headers=NOC_HEADERS)
        self.assertEqual(status, 200)
        self.assertIsNotNone(record["queue"])
        status, timeline = self.request("GET", "/api/records/1/audit", headers=NOC_HEADERS)
        self.assertEqual(status, 200)
        self.assertTrue(any(event["action"] == "queue_reorder" for event in timeline["items"]))
        status, recovered = self.request("POST", "/api/queue/recover")
        self.assertEqual(status, 200)
        self.assertTrue(recovered["recovered"])

    def test_queue_requires_dispatcher_role(self):
        status, error = self.request("POST", "/api/queue/items", {"record_id": 1, "idempotency_key": "k-a", **WINDOW}, NOC_HEADERS)
        self.assertEqual(status, 403)
        self.assertEqual(error["error"], "permission_denied")


if __name__ == "__main__":
    unittest.main()
